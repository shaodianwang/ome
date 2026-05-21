// Package omenative implements the OMENative deployment-mode backend for
// InferenceService — direct pod management with native Kubernetes primitives
// instead of LeaderWorkerSet. It plugs into the existing
// InferenceServiceReconciler as a new dispatch case in
// components/{engine,decoder,router}.go; it is not a parallel controller.
package omenative

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ReconcileParams is the input bag the OMENative reconciler reads on each
// invocation. Constructed at the dispatch site in
// components/{engine,decoder,router}.go from the values reconcileDeployment
// already has in scope.
type ReconcileParams struct {
	// ISVC is the parent InferenceService being reconciled.
	ISVC *v1beta1.InferenceService

	// ObjectMeta is the rendered per-Component metadata (labels,
	// annotations, owner refs).
	ObjectMeta metav1.ObjectMeta

	// PodSpec is the rendered pod template for the Component's leader /
	// single-pod runner.
	PodSpec *v1.PodSpec

	// Component identifies which of router / engine / decoder is being
	// reconciled.
	Component v1beta1.ComponentType

	// ComponentExt is the merged ComponentExtensionSpec, including the
	// `omenative` sub-block populated by the mutating webhook defaulter.
	ComponentExt *v1beta1.ComponentExtensionSpec

	// WorkerSize is Worker.Size for multi-pod Instances. Zero when the
	// Component is single-pod or has no Worker block (e.g., Router).
	WorkerSize int

	// WorkerPodSpec is the rendered pod template for the Worker runner.
	// Nil when WorkerSize is 0.
	WorkerPodSpec *v1.PodSpec

	// MultiPod indicates the Component declares Leader or Worker (Size > 0),
	// i.e., each Instance materializes more than one pod. Router is always
	// single-pod (MultiPod=false). Set at the dispatch site since
	// EngineSpec/DecoderSpec/RouterSpec aren't carried through this bag.
	MultiPod bool

	// Client is the controller-runtime client used to materialize pods,
	// patch status, and observe cluster state. Set at the dispatch site
	// from BaseComponentFields.Client.
	Client client.Client
}

// ReconcileComponent is the Loop A entry point for OMENative-managed
// Components. The InferenceService controller calls it from the
// `case constants.OMENative` branch of each Component's
// reconcileDeployment switch.
//
// Loop:
//  1. Build the desired ComponentPlan from ReconcileParams.
//  2. Scale down: drive Delete for any InstanceStatus index that the
//     plan no longer asks for. Scale-down completes before scale-up
//     to avoid running excess pods alongside in-flight drains.
//  3. Per-Instance restart: detect pod loss / pod-Failed triggers and
//     dispatch Restart when the Component's restart policy allows it.
//     Held to one pass per reconcile to keep the state machine simple.
//  4. Scale up: drive Create toward every desired Instance. The
//     operation is multi-pass — pods are created, ome.io/serving is
//     flipped once runtime-ready, and Phase=Ready is recorded.
//
// Update / Migrate land in follow-up PRs.
func ReconcileComponent(ctx context.Context, p ReconcileParams) (ctrl.Result, error) {
	if p.Client == nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: nil client (component=%s)", p.Component)
	}
	plan, err := BuildPlan(p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: build plan (component=%s): %w", p.Component, err)
	}

	// Scale-down first. An ISVC that drops MinReplicas leaves
	// InstanceStatus entries above the new replica count; Delete drives
	// each to gone.
	extras := extraInstanceIndices(p.ISVC, plan.Component, plan.Replicas)
	allDeleted := true
	for _, idx := range extras {
		done, derr := Delete(ctx, p, plan, idx)
		if derr != nil {
			return ctrl.Result{}, fmt.Errorf("OMENative reconciler: delete instance %d: %w", idx, derr)
		}
		if !done {
			allDeleted = false
		}
	}
	if !allDeleted {
		// Hold off on Create until scale-down completes — running both
		// at once would create churn against in-flight drains.
		return ctrl.Result{RequeueAfter: deleteRequeueInterval}, nil
	}

	// Per-Instance restart pass. Trigger detection works on the cached
	// pod list; the actual drain+recreate sequence runs inside Restart.
	// Only RecreateInstanceOnPodRestart routes here today — None falls
	// through to Create (which will recreate any missing-by-name pods),
	// and RecreateISVCOnPodRestart is a future cross-Component slice.
	anyRestarting := false
	if plan.RestartPolicy == v1beta1.OMENativeRestartPolicyRecreateInstance {
		for _, inst := range plan.Instances {
			needsRestart, reason, derr := detectRestartTrigger(ctx, p, plan, inst)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect restart trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needsRestart {
				continue
			}
			done, rerr := Restart(ctx, p, plan, inst, reason)
			if rerr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: restart instance %d: %w", inst.Index, rerr)
			}
			if !done {
				anyRestarting = true
			}
		}
	}
	if anyRestarting {
		return ctrl.Result{RequeueAfter: restartRequeueInterval}, nil
	}

	return Create(ctx, p, plan)
}

// Package omenative implements the OMENative deployment-mode backend for
// InferenceService — direct pod management with native Kubernetes primitives
// instead of LeaderWorkerSet. It plugs into the existing
// InferenceServiceReconciler as a new dispatch case in
// components/{engine,decoder,router}.go; it is not a parallel controller.
package omenative

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
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

	// APIReader is an optional live API reader, typically mgr.GetAPIReader().
	// When set it is used for the correctness-critical reads in revision
	// bookkeeping where stale cache contents could cause spurious
	// collisions or duplicate Revision numbers. When nil the cached Client
	// is used and the caller accepts the cache-lag exposure. Callers wire
	// this in once the controller manager exposes a live reader; tests
	// leave it nil since the fake client is a live source.
	APIReader client.Reader
}

// reader returns the live API reader when APIReader is set; otherwise the
// cached Client. Callers use this for revision bookkeeping and other
// reads that must not observe a stale cache.
func (p *ReconcileParams) reader() client.Reader {
	if p.APIReader != nil {
		return p.APIReader
	}
	return p.Client
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
//  4. Per-Instance update: compute the target ControllerRevision from
//     the rendered PodSpec and drive Update on any Instance whose pods
//     don't yet match. Mode (in-place vs recreate) is resolved per
//     Instance from the UpdateStrategy and the observed diff.
//  5. Scale up: drive Create toward every desired Instance. The
//     operation is multi-pass — pods are created, ome.io/serving is
//     flipped once runtime-ready, and Phase=Ready is recorded.
//
// Migrate lands in a follow-up PR.
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

	// Per-Instance update pass. Compute the target ControllerRevision
	// from the current rendered PodSpec, then drive Update on any
	// Instance whose pods don't yet match (or that is already
	// mid-Update). RecreatePod / InPlaceIfPossible / InPlaceOnly are
	// resolved per Instance inside Update; this layer just dispatches.
	if p.PodSpec != nil {
		target, rerr := ensureRevisionWithCollisionRetry(ctx, p, plan.Component)
		if rerr != nil {
			return ctrl.Result{}, fmt.Errorf("OMENative reconciler: ensure revision (component=%s): %w", plan.Component, rerr)
		}
		anyUpdating := false
		for _, inst := range plan.Instances {
			needs, derr := detectUpdateTrigger(ctx, p, plan, inst, target, p.PodSpec)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect update trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needs {
				continue
			}
			done, uerr := Update(ctx, p, plan, inst, target, p.PodSpec)
			if uerr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: update instance %d: %w", inst.Index, uerr)
			}
			if !done {
				anyUpdating = true
			}
		}
		if anyUpdating {
			return ctrl.Result{RequeueAfter: updateRequeueInterval}, nil
		}
	}

	return Create(ctx, p, plan)
}

// ensureRevisionWithCollisionRetry computes the target ControllerRevision
// for the Component's current PodSpec, honoring the collision contract
// from revision.go: if EnsureControllerRevision reports a hash collision
// (same revision name, different Data), bump
// OMENativeComponentStatus.CollisionCount, persist the status, and retry
// the revision creation with the new salt. The retry yields a different
// hash and lands.
//
// Without this retry, ReconcileComponent would silently drive pods
// toward the conflicting CR's Data, which is not the spec the user
// wrote — exactly the bug the collision return value is designed to
// prevent.
func ensureRevisionWithCollisionRetry(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType) (*appsv1.ControllerRevision, error) {
	var collisionCount *int32
	if cs, ok := p.ISVC.Status.Components[component]; ok && cs.OMENative != nil {
		collisionCount = cs.OMENative.CollisionCount
	}
	target, collision, err := EnsureControllerRevision(ctx, p.Client, p.reader(), p.ISVC, component, p.PodSpec, collisionCount)
	if err != nil {
		return nil, err
	}
	if !collision {
		return target, nil
	}
	// Bump CollisionCount on the Component status, then retry.
	bumped, err := bumpCollisionCount(ctx, p, component)
	if err != nil {
		return nil, fmt.Errorf("bump CollisionCount after revision collision: %w", err)
	}
	target, collision, err = EnsureControllerRevision(ctx, p.Client, p.reader(), p.ISVC, component, p.PodSpec, bumped)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("revision still collides after CollisionCount bump (count=%d)", *bumped)
	}
	return target, nil
}

// bumpCollisionCount increments OMENativeComponentStatus.CollisionCount
// (initializing to 1 when nil) and persists the status. Returns the
// new pointer for the retry caller.
func bumpCollisionCount(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType) (*int32, error) {
	fresh := &v1beta1.InferenceService{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
		return nil, fmt.Errorf("re-read ISVC for CollisionCount bump: %w", err)
	}
	if fresh.Status.Components == nil {
		fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := fresh.Status.Components[component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	next := int32(1)
	if cs.OMENative.CollisionCount != nil {
		next = *cs.OMENative.CollisionCount + 1
	}
	cs.OMENative.CollisionCount = &next
	fresh.Status.Components[component] = cs
	if err := p.Client.Status().Update(ctx, fresh); err != nil {
		return nil, fmt.Errorf("persist CollisionCount: %w", err)
	}
	// Mirror onto the cached ISVC the caller is reading.
	if p.ISVC.Status.Components == nil {
		p.ISVC.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	mirror := p.ISVC.Status.Components[component]
	if mirror.OMENative == nil {
		mirror.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	mirror.OMENative.CollisionCount = &next
	p.ISVC.Status.Components[component] = mirror
	return &next, nil
}

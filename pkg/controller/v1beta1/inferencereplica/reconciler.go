// Package inferencereplica implements the controller for the
// InferenceReplica CRD.
//
// THIS IS A NO-OP SKELETON. The full reconciler — instance lifecycle,
// revision management, pod / service / podmonitor creation, drain,
// migration, status aggregation — lands in PR 2 of this work
// (.claude/specs/2026-05-27-inference-workload-crd/impl-plan.md
// commits 5 - 8). The skeleton exists to:
//
//   - prove the wiring (scheme registration, watch setup, RBAC
//     scaffolding, manager flag) end-to-end before any real logic
//     accretes,
//   - stamp status.observedGeneration so operators can confirm with
//     kubectl that the controller is running against the new CR.
//
// The controller is enabled by default; the --enable-inferencereplica-controller
// manager flag lets operators flip it off if PR 2 lands a regression.
package inferencereplica

import (
	"context"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/finalizers,verbs=update

// Reconciler is the no-op skeleton for the InferenceReplica controller.
// PR 2 will replace this with the real lifecycle reconciler ported from
// pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/.
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// Reconcile loads the InferenceReplica and stamps observedGeneration.
// No other side effects yet — pod / service / status aggregation all
// land in PR 2.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("inferencereplica", req.NamespacedName)

	ir := &v1beta1.InferenceReplica{}
	if err := r.Get(ctx, req.NamespacedName, ir); err != nil {
		if apierrors.IsNotFound(err) {
			// The IR was deleted between enqueue and reconcile. Owner-ref
			// cascade GC handles the children (pods, services, etc. — none
			// of which the skeleton creates anyway).
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get InferenceReplica")
		return ctrl.Result{}, err
	}

	if ir.Status.ObservedGeneration == ir.Generation {
		// Already in sync. Cheap shortcut so test-driven generation bumps
		// don't write the same value back unnecessarily.
		return ctrl.Result{}, nil
	}

	// Retry on conflict — the IR is owned by this controller, but the
	// fake client + status subresource separation in tests can race the
	// resourceVersion check on the first read. retry.RetryOnConflict
	// covers both production (concurrent reconciler workers, though we
	// run only one) and test paths.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1beta1.InferenceReplica{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.Status.ObservedGeneration = latest.Generation
		return r.Status().Update(ctx, latest)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to stamp observedGeneration")
		return ctrl.Result{}, err
	}

	log.V(1).Info("Skeleton reconcile complete",
		"observedGeneration", ir.Generation)
	return ctrl.Result{}, nil
}

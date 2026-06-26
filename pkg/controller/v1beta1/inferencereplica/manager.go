package inferencereplica

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/utils"
)

// SetupWithManager wires the Reconciler into the controller manager.
//
// Watches:
//   - InferenceReplica (primary): re-enqueue on spec / status changes.
//   - Pods: a custom handler that BOTH updates the Expectations cache
//     (ObservedCreate / ObservedDelete) AND re-enqueues the owning IR.
//     The expectations update is load-bearing: the workload dispatcher's
//     destructive steps (surge-pod create, old-pod delete during
//     SurgeThenDrain / RecreatePod / Migrate) gate on
//     ExpectationsCache().Satisfied(). Without an observer feeding the
//     SAME cache the dispatcher reads, those gates only release on the
//     2-minute TTL — every rollout stalls with the Instance pinned at
//     Phase=Updating and readyReplicas=0. Owns(Pod) alone would
//     re-enqueue but never feed the cache, so we use Watches with the
//     observe-then-enqueue handler instead (mirrors the legacy omenative
//     path's pod watch on the ISVC controller).
//   - ControllerRevisions (owned): re-enqueue on revision GC so the
//     workload-side retention sweep doesn't fight a manual delete.
//   - HorizontalPodAutoscaler (owned): re-enqueue when the dispatcher-
//     emitted HPA changes — the status writer mirrors
//     HPA.Status.Conditions onto the IR / ISVC status surface.
//   - ScaledObject (owned): KEDA scheme is registered unconditionally
//     so the Owns() is unconditional. Re-enqueue on
//     ScaledObject status churn for the same status mirror.
//
// The parent InferenceService is NOT watched — the IR's owner ref to
// its parent means the parent's controller already gets re-enqueued
// on IR status churn via ISVC's Owns(&InferenceReplica{}).
//
// Services / PodMonitor are NOT watched: the ISVC controller still owns
// them — the IR controller can't observe-only on resources it doesn't
// manage without confusing the owner-ref shape.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Own an Expectations cache so this controller is self-consistent:
	// the same instance is read by the workload dispatcher (via
	// Deps.Expectations below — Reconcile threads r.Expectations) and
	// written by the pod event handler. Mirrors how the ISVC controller
	// seeds omenative.NewExpectations(). When nil the workload dispatcher
	// would fall back to the global singleton, which the pod handler does
	// not feed — the bug this fixes.
	if r.Expectations == nil {
		r.Expectations = workload.NewExpectations()
	}
	// Discover the scheduler-plugins PodGroup CRD once at setup (mirrors the
	// ISVC controller). Gates per-Instance PodGroup creation for multi-pod
	// Instances; absence is a degradation surface, not a hard fail (the
	// reconciler stamps GangSchedulingUnavailable and skips PodGroup creation).
	podGroupFound, err := utils.IsCrdAvailable(mgr.GetConfig(), schedulingv1alpha1.SchemeGroupVersion.String(), constants.PodGroupKind)
	if err != nil {
		return err
	}
	r.GangSchedulingAvailable = podGroupFound
	return ctrl.NewControllerManagedBy(mgr).
		// MaxConcurrentReconciles parallelizes reconciles for distinct IRs;
		// zero (unset) falls back to controller-runtime's single-worker default.
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		For(&v1beta1.InferenceReplica{}).
		Owns(&appsv1.ControllerRevision{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&kedav1.ScaledObject{}).
		Watches(
			&corev1.Pod{},
			newPodEventHandler(r.Expectations),
			builder.WithPredicates(managedByOMENativePredicate()),
		).
		Complete(r)
}

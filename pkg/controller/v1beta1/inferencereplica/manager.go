package inferencereplica

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// SetupWithManager wires the Reconciler into the controller manager.
//
// Watches:
//   - InferenceReplica (primary): re-enqueue on spec / status changes.
//   - Pods (owned): pod-status churn → re-enqueue the owning IR so the
//     status aggregator picks up new ReadyPodCount / ServingPodCount /
//     AvailablePodCount values. Owns() composes both an enqueue handler
//     and a controller-owns predicate.
//   - ControllerRevisions (owned): re-enqueue on revision GC so the
//     workload-side retention sweep doesn't fight a manual delete.
//   - HorizontalPodAutoscaler (owned): re-enqueue when the OEP-0013
//     Slice 3 dispatch-emitted HPA changes — Slice 4's status writer
//     mirrors HPA.Status.Conditions onto the IR / ISVC status surface.
//   - ScaledObject (owned): KEDA scheme is registered unconditionally
//     per OEP-0013 Q6, so the Owns() is unconditional. Re-enqueue on
//     ScaledObject status churn for the same Slice 4 status mirror.
//
// The parent InferenceService is NOT watched — the IR's owner ref to
// its parent means the parent's controller already gets re-enqueued
// on IR status churn via ISVC's Owns(&InferenceReplica{}) (PR 3
// wires that). The IR controller only needs to react to its own
// object plus the pods/CRs it owns.
//
// Services / PodMonitor are NOT watched: the ISVC controller still
// owns them (PR 2 scope) — the IR controller can't observe-only on
// resources it doesn't manage without confusing the owner-ref shape.
// PR 2.1 (Lane B) lands the headless-Service extraction; the watch
// list will grow then.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceReplica{}).
		Owns(&corev1.Pod{}).
		Owns(&appsv1.ControllerRevision{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&kedav1.ScaledObject{}).
		Complete(r)
}

package inferencereplica

import (
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// SetupWithManager wires the Reconciler into the controller manager. PR
// 2 will add Owns() entries for pods, services, podmonitors, and
// controllerrevisions; the skeleton owns nothing.
//
// The parent InferenceService is NOT watched here (the IR's owner ref
// to its parent means the parent's controller already gets re-enqueued
// on IR status churn via Owns(&InferenceReplica{}); the IR controller
// itself only needs to react to its own object).
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceReplica{}).
		Complete(r)
}

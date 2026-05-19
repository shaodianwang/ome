// Package omenative implements the OMENative deployment-mode backend for
// InferenceService — direct pod management with native Kubernetes primitives
// instead of LeaderWorkerSet. It plugs into the existing
// InferenceServiceReconciler as a new dispatch case in
// components/{engine,decoder,router}.go; it is not a parallel controller.
//
// This package is currently a skeleton. Reconcile attempts return a clear
// "not implemented" error until the operation set lands in follow-up PRs.
package omenative

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

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
}

// ReconcileComponent is the Loop A entry point for OMENative-managed
// Components. The InferenceService controller calls it from the
// `case constants.OMENative` branch of each Component's
// reconcileDeployment switch.
//
// The skeleton returns a sentinel error so callers get a clear signal that
// they hit the OMENative path on a build that has not implemented it.
func ReconcileComponent(ctx context.Context, p ReconcileParams) (ctrl.Result, error) {
	_ = ctx
	return ctrl.Result{}, fmt.Errorf("OMENative reconciler not implemented (component=%s)", p.Component)
}

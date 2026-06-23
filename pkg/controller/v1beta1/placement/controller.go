package placement

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
)

const (
	defaultPlacementRequeue = 30 * time.Second

	// PhasePending: matched no candidate, or the chosen cluster's transport is
	// not connected yet. PhasePlaced: the derived ISVC was created on a cluster.
	PhasePending = "Pending"
	PhasePlaced  = "Placed"
)

// ClusterClients is the subset of *workloadcluster.Manager the placer needs.
// *workloadcluster.Manager satisfies it; tests inject a fake.
type ClusterClients interface {
	ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool)
}

// Reconciler is the OEP-0018 control-plane fan-out controller. For phase 3a it
// places an ISVC on a SINGLE matching WorkloadCluster (the multi-candidate
// fan-out + race is phase 3b). It runs ONLY on the control-plane cluster, where
// the normal ISVC->pods reconciler is disabled.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Clusters ClusterClients
	// Requeue is the status-refresh poll cadence; defaults to defaultPlacementRequeue.
	Requeue time.Duration
}

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=workloadclusters,verbs=get;list;watch

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, request.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !isvc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, isvc)
	}

	if controllerutil.AddFinalizer(isvc, PlacementFinalizer) {
		if err := r.Update(ctx, isvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	clusters := &v1beta1.WorkloadClusterList{}
	if err := r.List(ctx, clusters); err != nil {
		return ctrl.Result{}, err
	}
	candidates, err := MatchingClusters(isvc.Annotations[ClusterSelectorAnnotation], clusters.Items)
	if err != nil || len(candidates) == 0 {
		// Bad selector (user error) or no match: record Pending, poll.
		return r.setPlacement(ctx, isvc, "", PhasePending)
	}

	// Sticky: keep the current cluster if it is still a candidate.
	chosen := candidates[0]
	if isvc.Status.Placement != nil && isvc.Status.Placement.Cluster != "" {
		for _, c := range candidates {
			if c == isvc.Status.Placement.Cluster {
				chosen = c
				break
			}
		}
	}

	cl, ok := r.Clusters.ClientFor(chosen)
	if !ok {
		// Matched but transport not connected yet; record Pending and retry.
		return r.setPlacement(ctx, isvc, "", PhasePending)
	}

	if err := r.placeOn(ctx, cl, isvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("place %s/%s on %q: %w", isvc.Namespace, isvc.Name, chosen, err)
	}
	return r.setPlacement(ctx, isvc, chosen, PhasePlaced)
}

// placeOn creates-or-updates the derived ISVC on the target cluster.
func (r *Reconciler) placeOn(ctx context.Context, cl client.Client, src *v1beta1.InferenceService) error {
	desired := DeriveISVC(src)
	target := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, cl, target, func() error {
		target.Labels = desired.Labels
		target.Annotations = desired.Annotations
		target.Spec = desired.Spec
		return nil
	})
	return err
}

func (r *Reconciler) reconcileDelete(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(isvc, PlacementFinalizer) {
		return ctrl.Result{}, nil
	}
	if isvc.Status.Placement != nil && isvc.Status.Placement.Cluster != "" {
		if cl, ok := r.Clusters.ClientFor(isvc.Status.Placement.Cluster); ok {
			derived := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name, Namespace: isvc.Namespace}}
			if err := cl.Delete(ctx, derived); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
	}
	controllerutil.RemoveFinalizer(isvc, PlacementFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// setPlacement writes status.placement and returns a poll requeue.
func (r *Reconciler) setPlacement(ctx context.Context, isvc *v1beta1.InferenceService, cluster, phase string) (ctrl.Result, error) {
	if isvc.Status.Placement == nil {
		isvc.Status.Placement = &v1beta1.PlacementStatus{}
	}
	isvc.Status.Placement.Cluster = cluster
	isvc.Status.Placement.Phase = phase
	if err := r.Status().Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeue()}, nil
}

func (r *Reconciler) requeue() time.Duration {
	if r.Requeue > 0 {
		return r.Requeue
	}
	return defaultPlacementRequeue
}

// SetupWithManager wires the placer: reconcile ISVCs, and re-enqueue all ISVCs
// when a WorkloadCluster changes (new/removed capacity may change placement).
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceService{}).
		Watches(&v1beta1.WorkloadCluster{}, handler.EnqueueRequestsFromMapFunc(r.isvcsForClusterChange)).
		Complete(r)
}

func (r *Reconciler) isvcsForClusterChange(ctx context.Context, _ client.Object) []ctrl.Request {
	list := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: list.Items[i].Namespace, Name: list.Items[i].Name}})
	}
	return reqs
}

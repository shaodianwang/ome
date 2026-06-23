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

	// PlacementStatus.Phase values.
	PhasePending = "Pending" // no candidate, or none connected yet
	PhaseRacing  = "Racing"  // fanned out to candidates, no winner yet
	PhasePlaced  = "Placed"  // a candidate was admitted and won the race
	PhaseFailed  = "Failed"

	// CandidatePlacement.Phase values.
	candidatePhasePlaced   = "Placed"
	candidatePhaseAdmitted = "Admitted"
)

// ClusterClients is the subset of *workloadcluster.Manager the placer needs.
// *workloadcluster.Manager satisfies it; tests inject a fake.
type ClusterClients interface {
	ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool)
}

// Reconciler is the OEP-0018 control-plane fan-out controller. It clones the
// derived ISVC onto every matched candidate cluster, lets each cluster's Kueue
// gate the pods, declares the FIRST cluster to admit (pods leave the gate) the
// winner, deletes the losers, and re-places if the winner is later lost. It runs
// ONLY on the control-plane cluster, where the local ISVC->pods reconciler is
// disabled. Status refresh is poll-based (the remote watch-funnel is Phase 3c).
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
		return r.writePlacement(ctx, isvc, "", PhasePending, nil)
	}

	// Sticky winner: keep the current winner while its derived ISVC still
	// exists and it remains a candidate. Otherwise fall through to re-race.
	if winner := winnerCluster(isvc); winner != "" && contains(candidates, winner) {
		healthy, err := r.winnerHealthy(ctx, winner, isvc)
		if err != nil {
			return ctrl.Result{}, err
		}
		if healthy {
			if err := r.ensureWinnerOnly(ctx, isvc, winner); err != nil {
				return ctrl.Result{}, err
			}
			return r.writePlacement(ctx, isvc, winner, PhasePlaced,
				[]v1beta1.CandidatePlacement{{Cluster: winner, Phase: candidatePhaseAdmitted}})
		}
	}

	// Fan out to all connected candidates.
	placed, err := r.fanOut(ctx, isvc, candidates)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(placed) == 0 {
		return r.writePlacement(ctx, isvc, "", PhasePending, nil)
	}

	// Race: the first placed candidate whose derived ISVC reports an admitted
	// instance wins.
	winner, err := r.findWinner(ctx, isvc, placed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if winner == "" {
		cands := make([]v1beta1.CandidatePlacement, 0, len(placed))
		for _, c := range placed {
			cands = append(cands, v1beta1.CandidatePlacement{Cluster: c, Phase: candidatePhasePlaced})
		}
		return r.writePlacement(ctx, isvc, "", PhaseRacing, cands)
	}

	// Winner: delete the losers.
	for _, c := range placed {
		if c == winner {
			continue
		}
		if err := r.deleteDerivedOn(ctx, c, isvc); err != nil {
			return ctrl.Result{}, err
		}
	}
	return r.writePlacement(ctx, isvc, winner, PhasePlaced,
		[]v1beta1.CandidatePlacement{{Cluster: winner, Phase: candidatePhaseAdmitted}})
}

// fanOut ensures the derived ISVC exists on each connected candidate; returns
// the clusters actually placed on (those currently connected via the Manager).
func (r *Reconciler) fanOut(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) ([]string, error) {
	var placed []string
	for _, c := range candidates {
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			continue
		}
		if err := r.placeOn(ctx, cl, isvc); err != nil {
			return nil, fmt.Errorf("fan out %s/%s to %q: %w", isvc.Namespace, isvc.Name, c, err)
		}
		placed = append(placed, c)
	}
	return placed, nil
}

// findWinner returns the first placed cluster whose derived ISVC reports an
// admitted instance, or "" if none has been admitted yet.
func (r *Reconciler) findWinner(ctx context.Context, isvc *v1beta1.InferenceService, placed []string) (string, error) {
	for _, c := range placed {
		derived, ok, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			return "", err
		}
		if ok && AnyInstanceAdmitted(derived) {
			return c, nil
		}
	}
	return "", nil
}

// winnerHealthy reports whether the winner's derived ISVC still exists. (Phase
// 3b treats "derived gone" as the loss signal; richer failure detection is 3c.)
func (r *Reconciler) winnerHealthy(ctx context.Context, winner string, isvc *v1beta1.InferenceService) (bool, error) {
	_, ok, err := r.getDerived(ctx, winner, isvc)
	return ok, err
}

// ensureWinnerOnly re-applies the derived ISVC on the winner and deletes any
// derived copies left on the other clusters recorded as candidates.
func (r *Reconciler) ensureWinnerOnly(ctx context.Context, isvc *v1beta1.InferenceService, winner string) error {
	if cl, ok := r.Clusters.ClientFor(winner); ok {
		if err := r.placeOn(ctx, cl, isvc); err != nil {
			return err
		}
	}
	if isvc.Status.Placement != nil {
		for _, cand := range isvc.Status.Placement.Candidates {
			if cand.Cluster == winner {
				continue
			}
			if err := r.deleteDerivedOn(ctx, cand.Cluster, isvc); err != nil {
				return err
			}
		}
	}
	return nil
}

// getDerived fetches the derived ISVC on a cluster. Returns ok=false if the
// cluster is not connected or the object does not exist.
func (r *Reconciler) getDerived(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) (*v1beta1.InferenceService, bool, error) {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return nil, false, nil
	}
	derived := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return derived, true, nil
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

// deleteDerivedOn best-effort deletes the derived ISVC on a cluster.
func (r *Reconciler) deleteDerivedOn(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) error {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return nil
	}
	derived := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name, Namespace: isvc.Namespace}}
	if err := cl.Delete(ctx, derived); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *Reconciler) reconcileDelete(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(isvc, PlacementFinalizer) {
		return ctrl.Result{}, nil
	}
	// Delete derived ISVCs on every cluster we fanned out to (winner + stragglers).
	clusters := map[string]struct{}{}
	if isvc.Status.Placement != nil {
		if isvc.Status.Placement.Cluster != "" {
			clusters[isvc.Status.Placement.Cluster] = struct{}{}
		}
		for _, c := range isvc.Status.Placement.Candidates {
			clusters[c.Cluster] = struct{}{}
		}
	}
	for c := range clusters {
		if err := r.deleteDerivedOn(ctx, c, isvc); err != nil {
			return ctrl.Result{}, err
		}
	}
	controllerutil.RemoveFinalizer(isvc, PlacementFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// writePlacement sets status.placement and returns a poll requeue.
func (r *Reconciler) writePlacement(ctx context.Context, isvc *v1beta1.InferenceService, winner, phase string, candidates []v1beta1.CandidatePlacement) (ctrl.Result, error) {
	if isvc.Status.Placement == nil {
		isvc.Status.Placement = &v1beta1.PlacementStatus{}
	}
	isvc.Status.Placement.Cluster = winner
	isvc.Status.Placement.Phase = phase
	isvc.Status.Placement.Candidates = candidates
	if err := r.Status().Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeue()}, nil
}

func winnerCluster(isvc *v1beta1.InferenceService) string {
	if isvc.Status.Placement == nil {
		return ""
	}
	return isvc.Status.Placement.Cluster
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
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

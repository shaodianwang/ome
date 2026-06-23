package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
)

const (
	defaultPlacementRequeue = 30 * time.Second
)

// placementResult is the status the reconciler writes for one pass.
type placementResult struct {
	winner     string
	phase      v1beta1.PlacementPhase
	candidates []v1beta1.CandidatePlacement
	url        *apis.URL // published endpoint; mirrored to BOTH status.placement.endpoint and status.url
}

// ClusterClients is the subset of *workloadcluster.Manager the placer needs.
// *workloadcluster.Manager satisfies it; tests inject a fake.
type ClusterClients interface {
	ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool)
	Connected() []string
}

// Reconciler is the OEP-0018 control-plane fan-out controller. It clones the
// derived ISVC onto every matched candidate cluster, lets each cluster's Kueue
// gate the pods, declares the first candidate (by sorted name) whose Kueue admits
// the pods the winner, deletes the losers, and re-places if the winner is later
// lost. It runs ONLY on the control-plane cluster, where the local ISVC->pods
// reconciler is disabled. Status refresh is poll-based (the remote watch-funnel
// is Phase 3c).
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
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	// Sticky winner: keep the current winner while its derived ISVC still
	// exists and it remains a candidate. Otherwise fall through to re-race.
	if winner := winnerCluster(isvc); winner != "" && slices.Contains(candidates, winner) {
		derived, exists, err := r.getDerived(ctx, winner, isvc)
		if err != nil {
			return ctrl.Result{}, err
		}
		if exists {
			if IsTerminallyFailed(derived) {
				// The placement itself failed; surface it and stop. Do NOT
				// re-place into a hot fail-loop — needs human/spec action.
				u := endpointFor(derived)
				if u == nil {
					u = isvc.Status.URL
				}
				return r.writePlacement(ctx, isvc, placementResult{
					winner: winner, phase: v1beta1.PlacementPhaseFailed,
					candidates: []v1beta1.CandidatePlacement{{Cluster: winner, Phase: v1beta1.CandidatePhaseAdmitted}},
					url:        u,
				})
			}
			// Re-apply on the winner and sweep any stray derived on other clusters.
			if cl, ok := r.Clusters.ClientFor(winner); ok {
				if err := r.placeOn(ctx, cl, isvc); err != nil {
					return ctrl.Result{}, err
				}
			}
			if err := r.deleteLosers(ctx, isvc, winner); err != nil {
				return ctrl.Result{}, err
			}
			return r.writePlacement(ctx, isvc, placedResult(winner, derived, isvc))
		}
		// derived gone (deleted out from under us) -> fall through to re-race.
	}

	// Fan out to all connected candidates.
	placed, err := r.fanOut(ctx, isvc, candidates)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(placed) == 0 {
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
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
			cands = append(cands, v1beta1.CandidatePlacement{Cluster: c, Phase: v1beta1.CandidatePhasePlaced})
		}
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhaseRacing, candidates: cands})
	}

	// Winner: sweep the losers (every connected cluster except the winner).
	if err := r.deleteLosers(ctx, isvc, winner); err != nil {
		return ctrl.Result{}, err
	}
	wd, _, err := r.getDerived(ctx, winner, isvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.writePlacement(ctx, isvc, placedResult(winner, wd, isvc))
}

// fanOut ensures the derived ISVC exists on each connected candidate. A
// per-cluster failure does not abort the others — one bad cluster must not deny
// the race to the healthy ones. Returns the clusters actually placed on; returns
// an error only when connected candidates existed but ALL failed.
func (r *Reconciler) fanOut(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) ([]string, error) {
	var placed []string
	var errs []error
	for _, c := range candidates {
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			continue
		}
		if err := r.placeOn(ctx, cl, isvc); err != nil {
			r.Log.Error(err, "fan out failed on cluster", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			errs = append(errs, fmt.Errorf("%q: %w", c, err))
			continue
		}
		placed = append(placed, c)
	}
	if len(placed) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("fan out %s/%s failed on all connected candidates: %w", isvc.Namespace, isvc.Name, errors.Join(errs...))
	}
	return placed, nil
}

// findWinner returns the winning cluster: the first placed candidate, in sorted
// candidate order, whose derived ISVC reports an admitted instance, or "" if
// none has been admitted yet. When several admit near-simultaneously the
// lowest-named candidate wins — a deterministic tie-break, not literally
// "first in wall-clock time".
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

// deleteLosers deletes the derived ISVC on every currently-connected cluster
// except keep. Self-healing: it sweeps any stray derived left by a prior
// fan-out or a delete that was skipped while a cluster was disconnected, so
// loser cleanup does NOT depend on the (possibly stale) status candidate list.
func (r *Reconciler) deleteLosers(ctx context.Context, isvc *v1beta1.InferenceService, keep string) error {
	for _, c := range r.Clusters.Connected() {
		if c == keep {
			continue
		}
		if err := r.deleteDerivedOn(ctx, c, isvc); err != nil {
			return err
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

// placeOn creates-or-updates the derived ISVC on the target cluster. The control
// plane is the sole owner of the derived ISVC's spec and metadata; this overwrites
// them wholesale on each reconcile.
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
	// Best-effort cleanup on every currently-connected cluster. A cluster that
	// is disconnected at delete time is reaped later by the GC sweep (its source
	// UID is gone), so finalizer cleanup need not block on it.
	if err := r.deleteLosers(ctx, isvc, ""); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(isvc, PlacementFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// writePlacement sets status.placement (+ the mirrored status.url) for one pass,
// retrying on conflict since the local object may change during the
// cross-cluster fan-out/race. Returns a poll requeue.
func (r *Reconciler) writePlacement(ctx context.Context, isvc *v1beta1.InferenceService, res placementResult) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(isvc)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &v1beta1.InferenceService{}
		if err := r.Get(ctx, key, cur); err != nil {
			return err
		}
		if cur.Status.Placement == nil {
			cur.Status.Placement = &v1beta1.PlacementStatus{}
		}
		cur.Status.Placement.Cluster = res.winner
		cur.Status.Placement.Phase = res.phase
		cur.Status.Placement.Candidates = res.candidates
		cur.Status.Placement.Endpoint = res.url
		cur.Status.URL = res.url
		return r.Status().Update(ctx, cur)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeue()}, nil
}

// endpointFor returns the winner's externally-addressable URL from its derived
// ISVC status, or nil until the worker reports one.
func endpointFor(derived *v1beta1.InferenceService) *apis.URL {
	if derived == nil {
		return nil
	}
	return derived.Status.URL
}

// placedResult builds the Placed status for winner. The endpoint is sticky: if
// the winner's derived has no URL this pass (transient worker-status gap), the
// last-published URL (from the in-memory isvc) is kept rather than cleared, so
// an external LB watching status.url does not see a spurious deroute.
func placedResult(winner string, derived, isvc *v1beta1.InferenceService) placementResult {
	u := endpointFor(derived)
	if u == nil {
		u = isvc.Status.URL // keep last-known endpoint
	}
	return placementResult{
		winner: winner, phase: v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: winner, Phase: v1beta1.CandidatePhaseAdmitted}},
		url:        u,
	}
}

func winnerCluster(isvc *v1beta1.InferenceService) string {
	if isvc.Status.Placement == nil {
		return ""
	}
	return isvc.Status.Placement.Cluster
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

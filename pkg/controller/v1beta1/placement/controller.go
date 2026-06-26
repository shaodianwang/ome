package placement

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
)

const (
	// DefaultPlacementRequeue is the fallback status-refresh poll cadence used
	// when Reconciler.Requeue is unset. The operative value is supplied by the
	// manager flag / chart; this is the graceful-degradation default.
	DefaultPlacementRequeue = 30 * time.Second
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
	// Requeue is the status-refresh poll cadence; defaults to DefaultPlacementRequeue.
	Requeue time.Duration
	// ControlPlaneID is this control plane's identity, stamped onto every derived
	// ISVC (PlacementControlPlaneLabel) so the GC sweep only reaps deriveds THIS
	// control plane created. Config-driven (manager flag / chart); empty leaves
	// the stamp off and keeps single-control-plane behavior.
	ControlPlaneID string
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
	candidates, reason, err := MatchCandidates(isvc, clusters.Items)
	if err != nil || len(candidates) == 0 {
		// Surface WHY there are no candidates (malformed selector / no
		// requirements declared / no Ready clusters / no match) so the empty
		// set is diagnosable instead of an indistinguishable Pending.
		r.Log.Info("no placement candidates", "isvc", isvc.Namespace+"/"+isvc.Name,
			"reason", reason, "error", err)
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	// Sticky winner: keep the current winner while its derived ISVC still
	// exists and it remains a candidate. Otherwise fall through to re-race.
	if winner := winnerCluster(isvc); winner != "" && slices.Contains(candidates, winner) {
		derived, exists, err := r.getDerived(ctx, winner, isvc)
		if err != nil {
			// A transient remote GET error must NOT abort the reconcile or be
			// misread as "derived gone" (which would prematurely re-race and
			// sweep losers). Hold the existing placement and retry next poll.
			r.Log.Error(err, "sticky winner: remote get failed; holding placement", "cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
			return ctrl.Result{RequeueAfter: r.requeue()}, nil
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
			// Re-apply on the winner and sweep any stray derived on the other
			// candidates (origin-guarded, so a same-named user ISVC is safe).
			if cl, ok := r.Clusters.ClientFor(winner); ok {
				if err := r.placeOn(ctx, cl, isvc); err != nil {
					return ctrl.Result{}, err
				}
			}
			r.deleteLosers(ctx, isvc, candidates, winner)
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

	// Winner: sweep the losers (every candidate cluster except the winner;
	// each delete is origin-guarded so only our derived copies are removed).
	r.deleteLosers(ctx, isvc, candidates, winner)
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
// candidate order, whose derived ISVC reports EVERY declared component admitted,
// or "" if none is fully admitted yet. Requiring all components (not just one
// instance) prevents a PD service from being declared placed while only the
// engine has cleared Kueue and the decoder is still gated. When several clusters
// fully admit near-simultaneously the lowest-named candidate wins — a
// deterministic tie-break, not literally "first in wall-clock time".
func (r *Reconciler) findWinner(ctx context.Context, isvc *v1beta1.InferenceService, placed []string) (string, error) {
	for _, c := range placed {
		derived, ok, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			return "", err
		}
		if ok && AllComponentsAdmitted(derived) {
			return c, nil
		}
	}
	return "", nil
}

// deleteLosers best-effort deletes THIS ISVC's derived copy on each cluster in
// clusters except keep. deleteDerivedOn only ever deletes an object that carries
// this source ISVC's origin label, so a same-named ISVC a user created directly
// on a workload cluster is never touched (cross-tenant data-loss guard).
//
// Per-cluster failures are tolerated (like fanOut): one slow/unreachable cluster
// must not block loser cleanup on the healthy ones, and must NOT block recording
// the winner. Failures are logged; the next poll retries, and the GC sweep is a
// backstop. Callers that must confirm full teardown (reconcileDelete) verify the
// derived is actually gone separately rather than relying on the return here.
func (r *Reconciler) deleteLosers(ctx context.Context, isvc *v1beta1.InferenceService, clusters []string, keep string) {
	for _, c := range clusters {
		if c == keep {
			continue
		}
		if err := r.deleteDerivedOn(ctx, c, isvc); err != nil {
			r.Log.Error(err, "loser cleanup failed on cluster (will retry next poll)", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
	}
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
// plane owns the derived Spec and the metadata keys IT stamps (origin markers,
// Kueue queue/serving gating); it does NOT own the rest of the object's labels
// and annotations. The worker-cluster reconciler adds its own metadata (status
// bookkeeping annotations, generated labels) to the derived object, so this
// merges the control-plane-owned keys into the existing maps rather than
// replacing them wholesale — preserving worker-side reconciler state across the
// poll-driven re-apply.
func (r *Reconciler) placeOn(ctx context.Context, cl client.Client, src *v1beta1.InferenceService) error {
	desired := DeriveISVC(src, r.ControlPlaneID)
	target := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, cl, target, func() error {
		target.Labels = mergeOwnedKeys(target.Labels, desired.Labels)
		target.Annotations = mergeOwnedKeys(target.Annotations, desired.Annotations)
		target.Spec = desired.Spec
		return nil
	})
	return err
}

// mergeOwnedKeys overlays the control-plane-owned keys (from desired) onto the
// existing map without dropping keys the worker-cluster reconciler added. nil
// existing maps are initialized only when there is something to set.
func mergeOwnedKeys(existing, owned map[string]string) map[string]string {
	if len(owned) == 0 {
		return existing
	}
	if existing == nil {
		existing = make(map[string]string, len(owned))
	}
	for k, v := range owned {
		existing[k] = v
	}
	return existing
}

// deleteDerivedOn best-effort deletes the derived ISVC on a cluster, but ONLY
// if the object actually present there is a copy this control plane derived from
// THIS source ISVC (it carries our origin label set to the source UID). It never
// deletes by identity alone: a same-named ISVC a user created directly on the
// workload cluster, or a copy derived from a different source UID, is left
// untouched. The UID precondition on Delete closes the read-then-delete race so
// a concurrently-recreated object isn't deleted out from under its owner.
func (r *Reconciler) deleteDerivedOn(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) error {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return nil
	}
	derived := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !isOurDerived(derived, isvc) {
		// Same-named object that is NOT ours: do not delete (cross-tenant guard).
		return nil
	}
	uid := derived.UID
	opts := []client.DeleteOption{}
	if uid != "" {
		opts = append(opts, client.Preconditions{UID: &uid})
	}
	if err := cl.Delete(ctx, derived, opts...); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// isOurDerived reports whether derived is a placement copy this control plane
// created from src (its origin label/annotation equals src's UID). The empty UID
// case (e.g. a fake/source without a server-assigned UID) requires the marker to
// be present and non-empty, never matches a blank.
func isOurDerived(derived, src *v1beta1.InferenceService) bool {
	want := string(src.UID)
	if want == "" {
		return false
	}
	return derived.Labels[PlacementOriginLabel] == want ||
		derived.Annotations[PlacementOriginUIDAnnotation] == want
}

func (r *Reconciler) reconcileDelete(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(isvc, PlacementFinalizer) {
		return ctrl.Result{}, nil
	}
	// Clean up our derived copy on every currently-connected cluster. The delete
	// is origin-guarded, so it only removes copies derived from THIS source. A
	// cluster that is disconnected at delete time can't be cleaned now; it is
	// reaped later by the GC sweep (the source UID is gone once the finalizer is
	// removed), so we don't block teardown on an unreachable cluster.
	//
	// We do, however, hold the finalizer if a CONNECTED cluster errored on
	// cleanup: dropping the finalizer then would orphan a still-reachable derived
	// (and the GC only catches it on its slower cadence). Requeue and retry.
	connected := r.Clusters.Connected()
	r.deleteLosers(ctx, isvc, connected, "")
	if remaining, err := r.derivedRemainingOnConnected(ctx, isvc, connected); err != nil {
		return ctrl.Result{}, err
	} else if remaining {
		// A connected cluster still holds our derived (transient delete error
		// tolerated by deleteLosers). Keep the finalizer; retry on the next poll.
		return ctrl.Result{RequeueAfter: r.requeue()}, nil
	}
	controllerutil.RemoveFinalizer(isvc, PlacementFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// derivedRemainingOnConnected reports whether any connected cluster still holds
// our derived copy of isvc. Used during teardown to decide whether the finalizer
// can be safely removed. A transient GET error is reported as an error (caller
// requeues) rather than silently treated as "gone".
func (r *Reconciler) derivedRemainingOnConnected(ctx context.Context, isvc *v1beta1.InferenceService, connected []string) (bool, error) {
	for _, c := range connected {
		derived, exists, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			return false, err
		}
		if exists && isOurDerived(derived, isvc) {
			return true, nil
		}
	}
	return false, nil
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
	return DefaultPlacementRequeue
}

// SetupWithManager wires the placer: reconcile ISVCs, and re-enqueue all ISVCs
// when a WorkloadCluster changes in a placement-relevant way (new/removed
// capacity, readiness flip, or label change). The predicate suppresses the
// per-heartbeat status writes (the WorkloadCluster reconciler re-stamps Ready
// every health interval) so a routine heartbeat does NOT fan out to every ISVC.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceService{}).
		Watches(&v1beta1.WorkloadCluster{}, handler.EnqueueRequestsFromMapFunc(r.isvcsForClusterChange),
			builder.WithPredicates(placementRelevantClusterChange)).
		Complete(r)
}

// placementRelevantClusterChange admits WorkloadCluster events that can change
// placement decisions: create/delete (capacity appears/disappears) and updates
// that flip the Ready condition status or change labels. It drops the routine
// status heartbeat (the health-probe re-stamp of an unchanged Ready condition),
// which would otherwise re-enqueue every control-plane ISVC on each probe.
var placementRelevantClusterChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldWC, ok1 := e.ObjectOld.(*v1beta1.WorkloadCluster)
		newWC, ok2 := e.ObjectNew.(*v1beta1.WorkloadCluster)
		if !ok1 || !ok2 {
			return true // unexpected type: fail safe by re-enqueuing
		}
		if clusterReady(oldWC) != clusterReady(newWC) {
			return true
		}
		return !maps.Equal(oldWC.Labels, newWC.Labels)
	},
}

// clusterReady reports whether the WorkloadCluster's Ready condition is True.
func clusterReady(wc *v1beta1.WorkloadCluster) bool {
	return apimeta.IsStatusConditionTrue(wc.Status.Conditions, v1beta1.WorkloadClusterReady)
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

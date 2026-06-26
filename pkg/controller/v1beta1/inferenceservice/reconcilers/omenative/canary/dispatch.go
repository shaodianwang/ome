package canary

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/utils"
)

// DispatchDeps is the controller-side input to the canary executor, mirroring
// coordination.ReconcileInputs. The controller calls Dispatch once per reconcile
// when a canary group is set (spec.rollout.groups[].canary); coordination runs in
// the same pass on the non-canary Components.
type DispatchDeps struct {
	Client   client.Client
	Reader   client.Reader
	Recorder record.EventRecorder
	ISVC     *v1beta1.InferenceService
	Now      time.Time
	// Sampler is the controller's shared async analysis sampler (built once at
	// startup). nil disables metric-gated steps (they hold), which keeps a
	// controller without a configured sampler from advancing analysis steps ungated.
	Sampler *Sampler
	// BundledPrometheusAddress and QueryTimeout come from the canaryAnalysis operator
	// config (fetched per-reconcile by the controller), threaded to the sampler.
	BundledPrometheusAddress string
	QueryTimeout             time.Duration
}

// Dispatch runs the canary step machine for an InferenceService, mutating
// isvc.Status in-memory (phase, traffic, step) and returning a requeue hint. The
// step machine is ISVC-level; it is driven through the externally-routed
// (primary) Component — the router for PD, otherwise the single Component — whose
// per-revision Services carry the external traffic weight. Per-component pod
// capacity is applied separately by the controller via EffectivePartition.
//
// No-op (requeue 0) when no canary plan is set.
func Dispatch(ctx context.Context, d DispatchDeps) (time.Duration, error) {
	if d.ISVC.Spec.GetCanaryGroup() == nil {
		return 0, nil
	}
	primary := primaryComponent(d.ISVC)
	if primary == "" {
		return 0, nil
	}
	// Observe per-revision pods for EVERY configured component (not just the
	// primary): the canary stages capacity on all of them, so the gate must wait
	// on all of them before shifting traffic. perRev is TOTAL pods (used to ensure
	// per-revision Services + pick the stable revision); readyRev is the READY
	// (serving) subset — the capacity gate MUST use ready so traffic never shifts
	// onto canary pods that exist but are not serving yet.
	perRev, readyRev, err := coordination.ObservePerRevisionPods(ctx, d.Reader, d.ISVC, configuredComponents(d.ISVC))
	if err != nil {
		return 0, err
	}
	pods := perRev[primary]

	canaryHash := canaryTargetHash(ctx, d.Reader, d.ISVC, primary)

	// Ensure the per-revision Services the canary references, for EVERY Component
	// in the canary group — not just the primary. The downstream weighted-route
	// consumer and the peer-discovery revision endpoints (OME_<PEER>_REVISION_ENDPOINT)
	// reference these by name, and coordination skips canary-owned Components, so
	// the canary engine is their sole producer; ensuring only the primary's left a
	// secondary's revision endpoint dangling. Mirrors coordination's per-Component
	// ensure+GC loop.
	for _, comp := range configuredComponents(d.ISVC) {
		// Ensure runs EVERY reconcile — it is the self-heal path. A
		// per-revision Service deleted out-of-band must be recreated even
		// when the live revision-hash set is unchanged; EnsurePerRevisionServices
		// uses CreateOrUpdate, whose Gets are cache-served and cheap, and
		// recreates a missing Service. Gating ensure on revision-set
		// convergence would strand the weighted-route consumer (and the
		// peer OME_<PEER>_REVISION_ENDPOINT) on a dead backend.
		multiPod := isvcutils.IsMultiPodComponent(d.ISVC, comp)
		for hash := range perRev[comp] {
			if hash == "" {
				continue
			}
			if _, err := coordination.EnsurePerRevisionServices(ctx, d.Client, d.ISVC, comp, hash, multiPod); err != nil {
				return 0, err
			}
		}
		// GC — and only GC — may be skipped when converged. The orphan
		// sweep is a heavier namespace Service LIST; when the live
		// revision-hash set matches the set recorded on
		// Status.Components.<c>.Traffic there is nothing to delete.
		// Mirrors coordination's gate. Secondaries (whose Traffic the
		// canary never writes) always fall through, which is correct —
		// they just don't get the steady-state GC skip.
		if coordination.PerRevisionServicesConverged(d.ISVC.Name, comp, perRev[comp], d.ISVC.Status.Components[comp].Traffic) {
			continue
		}
		if err := coordination.GCOrphanedPerRevisionServices(ctx, d.Client, d.ISVC, comp, perRev[comp]); err != nil {
			return 0, err
		}
	}

	now := d.Now
	if now.IsZero() {
		now = time.Now()
	}

	res, err := Reconcile(ctx, ReconcileInputs{
		Client:                   d.Client,
		ISVC:                     d.ISVC,
		Component:                primary,
		CanaryRevisionHash:       canaryHash,
		StableRevisionHash:       otherRevision(pods, canaryHash),
		DesiredReplicas:          componentReplicas(d.ISVC, primary),
		PerRevisionPods:          readyRev[primary],
		SecondaryCapacityReady:   secondaryCapacityReady(ctx, d.Reader, d.ISVC, perRev, readyRev, primary),
		Now:                      now,
		Sampler:                  samplerOrNil(d.Sampler),
		Prometheus:               canaryPrometheus(d.ISVC),
		BundledPrometheusAddress: d.BundledPrometheusAddress,
		QueryTimeout:             d.QueryTimeout,
	})
	if err != nil {
		return 0, err
	}

	// Rollback signal: when the executor is rolling back, point EVERY group
	// Component's IR at its own stable ControllerRevision so it rolls every
	// Instance back to stable; otherwise clear the signal. (Reconcile shifted
	// traffic + set the phase; this drives the actual pod revert.) The canary
	// stages capacity on all group Components, so a PD rollback must revert all of
	// them — signaling only the primary leaves the engine/decoder serving the
	// rejected revision behind the rolled-back router.
	for _, comp := range configuredComponents(d.ISVC) {
		if err := reconcileRollbackSignal(ctx, d.Client, d.Reader, d.ISVC, comp, res.RolledBack); err != nil {
			return 0, err
		}
	}

	recordDispatch(d.Recorder, d.ISVC, primary, res)
	return res.RequeueAfter, nil
}

// reconcileRollbackSignal sets (rolledBack) or clears one Component IR's
// Spec.Pacing.RollbackToRevision. When rolling back it points at the STABLE
// pre-canary ControllerRevision so the IR reconciler overrides its desired
// template with that revision and rolls every Instance back to it. Idempotent —
// only writes on a change.
//
// The target is resolved from the IR's ControllerRevision history, NOT from
// ir.Status.CurrentRevision: CurrentRevision is promoted to the canary revision
// as soon as the Component fully rolls forward onto it (every Instance Ready on
// target). The primary always reaches that state — the router (the PD primary)
// is typically a 1-replica proxy whose every step rounds the new-pod count up to
// its full replica count (partition 0), so it rolls fully onto the canary at the
// FIRST step. Echoing CurrentRevision back would then tell the IR to "roll back"
// to the canary revision it is already on — a no-op — so the canary pods never
// drain and the rollback wedges in RollingBack forever.
func reconcileRollbackSignal(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, rolledBack bool) error {
	if c == nil || reads == nil {
		return nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irprojector.InferenceReplicaName(isvc.Name, comp)}
	if err := reads.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	var want *string
	if rolledBack {
		// The rejected revision is this Component's OWN canary target (revision
		// hashes are per-Component, so the primary's rejected hash never names a
		// secondary's revision). canaryTargetHash reads the IR's UpdateRevision,
		// which keeps naming the canary (spec) target throughout the rollback.
		stable, err := stableRevisionName(ctx, reads, isvc, comp, canaryTargetHash(ctx, reads, isvc, comp))
		if err != nil {
			return err
		}
		// Only signal a real stable target. An empty result (the stable CR was
		// GC'd, or only the canary revision exists) must NOT fall back to the
		// canary revision — that would be a no-op roll. Leaving the signal unset
		// lets the IR reconciler's NotFound path degrade gracefully.
		if stable != "" {
			want = &stable
		}
	}
	var cur *string
	if ir.Spec.Pacing != nil {
		cur = ir.Spec.Pacing.RollbackToRevision
	}
	if utils.PtrStrEqual(cur, want) {
		return nil
	}
	if ir.Spec.Pacing == nil {
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	ir.Spec.Pacing.RollbackToRevision = want
	return c.Update(ctx, ir)
}

// stableRevisionName resolves the ControllerRevision name of the stable,
// pre-canary revision for a Component: the highest-numbered ControllerRevision
// in the Component's history whose revision hash is NOT the rejected (canary)
// hash. The canary is the latest spec target so it owns the highest .Revision;
// the stable revision the canary rolled away from is the next-highest. Returns
// "" when no non-canary revision is in history (nothing to roll back to).
//
// Reading the history is what makes the rollback target survive the canary's
// forward roll — once every Instance is on the canary revision, neither the IR
// status nor the live pod set still names the stable revision, but its
// ControllerRevision is retained.
func stableRevisionName(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, rejectedHash string) (string, error) {
	// Without a known rejected (canary) hash we can't tell the stable revision
	// apart from the canary, so don't guess — better no signal than the wrong one.
	if rejectedHash == "" {
		return "", nil
	}
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(comp),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	})
	if err := reads.List(ctx, list, client.InNamespace(isvc.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return "", err
	}
	bestName := ""
	var bestRev int64
	for i := range list.Items {
		cr := &list.Items[i]
		if query.RevisionHashFromControllerRevisionName(cr.Name) == rejectedHash {
			continue
		}
		if bestName == "" || cr.Revision > bestRev {
			bestName, bestRev = cr.Name, cr.Revision
		}
	}
	return bestName, nil
}

// samplerOrNil converts a possibly-nil *Sampler into the stepSampler interface,
// returning a true nil interface (not a non-nil interface wrapping a nil pointer)
// so the executor's `Sampler == nil` guard works and never dereferences nil.
func samplerOrNil(s *Sampler) stepSampler {
	if s == nil {
		return nil
	}
	return s
}

// canaryPrometheus returns the canary group's shared metrics source (the
// connection every analysis step queries), or nil when unset (the sampler then
// falls back to the bundled Prometheus).
func canaryPrometheus(isvc *v1beta1.InferenceService) *v1beta1.AnalysisPrometheus {
	g := isvc.Spec.GetCanaryGroup()
	if g == nil || g.Canary == nil {
		return nil
	}
	return g.Canary.Prometheus
}

// configuredComponents lists the Components the canary operates on — the canary
// group's own Components. The step machine + traffic run through the primary
// (router>engine>decoder); the capacity gate observes EVERY Component in the
// group, so a PD pair stages capacity on engine+decoder together before traffic
// shifts (the secondaryCapacityReady gate).
func configuredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	g := isvc.Spec.GetCanaryGroup()
	if g == nil {
		return nil
	}
	return g.Components
}

// secondaryCapacityReady reports whether every NON-primary Component's canary
// pods have reached that Component's current-step newCount. The step machine and
// external traffic run through the primary, but traffic must not shift until
// EVERY Component's canary capacity is up — otherwise a Ready router (primary)
// can advance the step while the engine/decoder canary pods behind it are still
// coming up. True when there are no secondaries (single-component canary). The
// step index mirrors EffectivePartition so the gate matches the staged partition.
//
// Each secondary gates on ITS OWN canary target hash (revision hashes are
// per-Component, so the primary's hash never names a secondary's pods). A
// secondary that was not bumped has no distinct canary revision — its target hash
// resolves to the only revision present — so there is nothing to stage and it is
// treated as ready, rather than blocking the rollout forever.
//
// readyPerRev must be the READY (serving) per-revision counts — not total — so a
// secondary's pods that merely exist don't satisfy the gate prematurely. perRev
// (total) is used only to detect whether a secondary has a roll in progress.
func secondaryCapacityReady(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, perRev, readyPerRev map[v1beta1.ComponentType]map[string]int32, primary v1beta1.ComponentType) bool {
	g := isvc.Spec.GetCanaryGroup()
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return true
	}
	plan := g.Canary
	idx := int32(0)
	if isvc.Status.Canary != nil {
		idx = isvc.Status.Canary.CurrentStep
	}
	if int(idx) >= len(plan.Steps) {
		idx = int32(len(plan.Steps) - 1)
	}
	step := plan.Steps[idx]
	for _, c := range configuredComponents(isvc) {
		if c == primary {
			continue
		}
		secHash := canaryTargetHash(ctx, reads, isvc, c)
		// No canary revision distinct from the stable one → the secondary was not
		// bumped; nothing to stage, so don't block. otherRevision is empty when the
		// secondary has a single revision present (its canary hash == the only one).
		if secHash == "" || otherRevision(perRev[c], secHash) == "" {
			continue
		}
		if readyPerRev[c][secHash] < resolveStepNewCount(step, componentReplicas(isvc, c)) {
			return false
		}
	}
	return true
}

// canaryTargetHash returns the revision hash the canary is rolling toward for a
// Component. It reads the authoritative InferenceReplica status — the workload
// object whose RollingUpdate.Partition the canary drives — rather than the
// ISVC's aggregated lifecycle status. In the IR-managed path the projector
// refreshes that ISVC aggregate only later in the same reconcile, after Dispatch
// runs, so at dispatch time it still names the pre-roll (stable) revision;
// reading it would point the canary at the wrong revision and wedge the capacity
// gate on an empty pod bucket. Falls back to the ISVC lifecycle aggregate for
// the legacy path, where no IR exists and the in-reconcile status aggregation
// has already run by the time the canary dispatches.
func canaryTargetHash(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if reads != nil {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: irprojector.InferenceReplicaName(isvc.Name, c)}
		if err := reads.Get(ctx, key, ir); err == nil {
			if name := ir.Status.UpdateRevision; name != "" {
				return query.RevisionHashFromControllerRevisionName(name)
			}
			if name := ir.Status.CurrentRevision; name != "" {
				return query.RevisionHashFromControllerRevisionName(name)
			}
		}
	}
	return coordination.LatestRevisionHash(isvc, c)
}

// primaryComponent is the externally-routed Component the canary group drives its
// step machine + traffic weight through: router > engine > decoder, among the
// group's Components. Secondary Components (a PD pair's engine/decoder) stage
// capacity and gate the step but don't carry the external traffic weight.
func primaryComponent(isvc *v1beta1.InferenceService) v1beta1.ComponentType {
	g := isvc.Spec.GetCanaryGroup()
	if g == nil || len(g.Components) == 0 {
		return ""
	}
	for _, c := range []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if groupHasComponent(g, c) {
			return c
		}
	}
	return g.Components[0]
}

// componentReplicas is the steady replica count for a Component (MinReplicas,
// falling back to MaxReplicas, else 0).
func componentReplicas(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) int32 {
	var ext *v1beta1.ComponentExtensionSpec
	switch c {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			ext = &isvc.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			ext = &isvc.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			ext = &isvc.Spec.Router.ComponentExtensionSpec
		}
	}
	if ext == nil {
		return 0
	}
	if ext.MinReplicas != nil {
		return int32(*ext.MinReplicas)
	}
	return int32(ext.MaxReplicas)
}

// otherRevision returns the non-canary revision hash with live pods (the stable
// revision). Empty when only the canary revision is present.
func otherRevision(pods map[string]int32, canaryHash string) string {
	best, bestN := "", int32(0)
	for h, n := range pods {
		if h == canaryHash || h == "" {
			continue
		}
		if n >= bestN {
			best, bestN = h, n
		}
	}
	return best
}

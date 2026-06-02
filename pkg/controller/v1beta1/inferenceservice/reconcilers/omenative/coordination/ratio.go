package coordination

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// RatioState carries the cross-Component replica counts the
// RatioBalanced pacing math reasons about. Snapshotted at rollout
// start (Original) and refreshed every reconcile (Current, NewPods,
// Serving) while the group is in Surging / Waiting.
type RatioState struct {
	// Original maps Component → desired replica count at the moment
	// the rollout began. Used as the ratio anchor; the tolerance band
	// is computed against this.
	Original map[v1beta1.ComponentType]int32

	// Current maps Component → total live replica count (any
	// revision) as of this reconcile.
	Current map[v1beta1.ComponentType]int32

	// NewPods maps Component → number of new-revision pods (i.e.,
	// pods on the target ControllerRevision) as of this reconcile.
	// Retained for status reporting / observability; the RatioBalanced
	// math uses Serving instead.
	NewPods map[v1beta1.ComponentType]int32

	// Serving maps Component → number of pods CURRENTLY in the load
	// balancer's rotation (serving gate True AND containers Ready),
	// counting both revisions together. RatioBalanced math projects
	// against this — operators care about live traffic capacity, not
	// rollout progress fractions.
	Serving map[v1beta1.ComponentType]int32

	// InFlightUnavail maps Component → number of Instances currently
	// taken OUT of serving by an in-flight drain/recreate/in-place
	// operation, counted from Operation.Step (NOT the cache-derived
	// Serving). The dispatcher stamps Operation.Step synchronously, so
	// this is FRESH even when Serving (pod-cache derived) still
	// over-reports during a recreate burst. The discrete-granularity
	// tiebreaker reads it to bound in-flight drains to a single pod
	// across reconcile wake-ups. Optional: a nil map reads as 0 (no
	// in-flight), which is the correct default for the surge path and
	// for unit callers that don't populate it.
	InFlightUnavail map[v1beta1.ComponentType]int32
}

// RatioDecision is the answer for "may we surge new-revision pods for
// `component`?" produced by RatioBalanced pacing.
type RatioDecision struct {
	// AllowedSurgeDelta is the maximum number of new-revision pods we
	// may add to component this reconcile. Zero when no surge is
	// allowed; negative is normalized to zero.
	AllowedSurgeDelta int32

	// SkewRejected is true when the previous attempted surge for
	// component was refused to avoid drifting the cross-Component
	// ratio outside the tolerance band. Drives the
	// `ratio_skew_total` counter and the RatioSkewRejected event.
	SkewRejected bool

	// Reason is the operator-visible explanation.
	Reason string
}

// EvaluateSurge returns the per-Component delta budget that respects
// the cross-Component live-serving ratio. It is a pure function: feed
// it the pacing block, the snapshot state, the Component being
// considered, and the proposed delta — get back what the controller
// may apply.
//
// Delta semantic: +N means "the action will INCREASE serving by N
// pods for `component`" (surge a new ready pod). Negative deltas
// (drain) go through the same band check via CheckRatioGate with
// projection = current - 1.
//
// Math: the original prefill:decode (or any pair, largest original on
// top) ratio is anchored at rollout start. After the proposed action,
// the live serving ratio for every component pair must stay within
// original_ratio * (1 ± tolerance). Pairwise so 3+-way topologies
// (engine + decoder + router) are constrained against every pair.
func EvaluateSurge(pacing v1beta1.CoordinationPacing, state RatioState, component v1beta1.ComponentType, proposedDelta int32) RatioDecision {
	if proposedDelta == 0 {
		return RatioDecision{AllowedSurgeDelta: 0, Reason: "no surge requested"}
	}
	if pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "PerComponent pacing"}
	}
	if len(state.Original) <= 1 {
		// Single-Component group — ratio enforcement is trivially
		// satisfied (no peer to skew against).
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "single-component group"}
	}
	tolerance := int32(5)
	if pacing.RatioTolerance != nil {
		tolerance = *pacing.RatioTolerance
	}
	band := float64(tolerance) / 100.0

	// Project against live Serving counts. Falls back to NewPods for
	// backward compat with callers that haven't populated Serving yet.
	baseline := state.Serving
	if baseline == nil {
		baseline = state.NewPods
	}
	// Drain (proposedDelta < 0): the gate is asked "may I pull one more
	// pod from rotation right now?" The projection is single-step — no
	// reduction loop because a partial drain isn't a thing the
	// dispatcher can act on (the unit of work is one pod).
	if proposedDelta < 0 {
		projected := projectedServing(baseline, component, proposedDelta)
		if respectsBand(state.Original, projected, band) {
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				Reason:            "drain within tolerance",
			}
		}
		// Discrete-granularity tiebreaker (rounding-error only). When the
		// tolerance band is narrower than one whole pod at the current
		// operating point, NO single -1 drain can satisfy the strict band,
		// and a drain-first strategy (RecreatePod / in-place) deadlocks
		// permanently — e.g. a symmetric 4:4 pair at tol 25%: draining
		// either side projects max/min = 4/3 = 1.333 > 1.25.
		//
		// Allow exactly ONE in-flight pod through, but only when the skew it
		// introduces is rounding-error-sized:
		//   (a) NO pod of the drained Component is already out of rotation:
		//       - no in-flight drain/recreate op (state.InFlightUnavail is
		//         operation-tracked, so it is FRESH even when the cache-derived
		//         Serving lags during a recreate burst); AND
		//       - the baseline is at full original serving (CheckRatio nets
		//         THIS-wake-up drains out of the baseline, so the second pod in
		//         one wake-up sees a reduced baseline and is refused).
		//       Together these bound in-flight to one pod ACROSS wake-ups and
		//       restrict the tiebreaker to the genuine "can't even start"
		//       deadlock — a Component mid-roll isn't deadlocked, so the strict
		//       band edge stands (see the 30:10 boundary test).
		//   (b) the pair is currently balanced (baseline in band);
		//   (c) the drained Component keeps >=1 pod serving — the mass-drain
		//       guard (zero-serving side) stays intact; and
		//   (d) the projection respects a band relaxed by one extra
		//       tolerance width (2x band). A large asymmetric skew (e.g.
		//       4:2 decoder->1 = 4:1) exceeds 2x band and stays an honest
		//       deadlock the operator resolves by widening tolerance.
		if state.InFlightUnavail[component] == 0 &&
			baseline[component] >= state.Original[component] &&
			respectsBand(state.Original, baseline, band) &&
			projected[component] >= 1 &&
			respectsBand(state.Original, projected, band*2) {
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				Reason:            "drain allowed by discrete-granularity tiebreaker (1 pod, rounding-error overshoot)",
			}
		}
		return RatioDecision{
			AllowedSurgeDelta: 0,
			SkewRejected:      true,
			Reason:            "drain would skew cross-Component serving ratio past tolerance",
		}
	}
	for delta := proposedDelta; delta > 0; delta-- {
		projected := projectedServing(baseline, component, delta)
		if respectsBand(state.Original, projected, band) {
			return RatioDecision{
				AllowedSurgeDelta: delta,
				Reason:            "within tolerance",
			}
		}
	}
	// Surge-side discrete-granularity tiebreaker — the dual of the drain
	// tiebreaker above. When the band is narrower than one whole pod at the
	// current operating point, NO single +1 surge satisfies the strict band
	// and a surge-first roll deadlocks (e.g. symmetric 3:3 at tol 25%: surging
	// either side to 4 is 4:3 = 1.333 > 1.25, so BOTH deny and neither goes
	// first). A surge only ADDS a pod to `component` — it raises component's
	// serving, never lowers a peer's — so letting ONE rounding-error pod
	// through is safe: it can't starve a peer, and MaxSurge (CheckSurge) bounds
	// the magnitude. Same guard shape as the drain tiebreaker:
	//   (a) the surging Component is at full original serving (a genuine
	//       start-of-roll deadlock, not a mid-roll skew — mirrors the drain
	//       baseline>=original guard; rejects the already-skewed / mid-roll
	//       and zero-serving cases the strict band must keep gating); AND
	//   (b) the pair is currently balanced (baseline in band); AND
	//   (c) the overshoot is rounding-error-sized (within 2x band).
	// Large overshoots stay an honest deadlock the operator resolves by
	// widening tolerance — N=1 (surge 1->2 = 2:1) and a tolerance so tight the
	// single-pod step exceeds 2x band (tol 10% at N=4: 5:4 = 1.25 > 1.2) are
	// intentionally NOT cleared.
	projected := projectedServing(baseline, component, 1)
	if baseline[component] >= state.Original[component] &&
		respectsBand(state.Original, baseline, band) &&
		respectsBand(state.Original, projected, band*2) {
		return RatioDecision{
			AllowedSurgeDelta: 1,
			Reason:            "surge allowed by discrete-granularity tiebreaker (1 pod, rounding-error overshoot)",
		}
	}
	return RatioDecision{
		AllowedSurgeDelta: 0,
		SkewRejected:      true,
		Reason:            "surge would skew cross-Component serving ratio past tolerance",
	}
}

// projectedServing clones the baseline serving map with component += delta.
// Negative values are floored at zero — a drain can't take serving
// below zero, but the band check will reject that projection
// (zero-serving denominator → undefined ratio → out of band).
func projectedServing(in map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, delta int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[component] += delta
	if out[component] < 0 {
		out[component] = 0
	}
	return out
}

// respectsBand reports whether the projected live serving counts, taken
// pairwise as larger/smaller ratios, stay within `band` of the original
// pairwise ratio at rollout start.
//
// Semantic: the "RatioBalanced" name promises the live cross-Component
// capacity ratio (e.g., prefill:decode) stays in band, not per-Component
// rollout progress drift. Convention: for each pair, larger original on
// top so the ratio is always >= 1 and bands are symmetric multiplicatively.
//
// For each pair (a, b) with originals OA, OB (assume OA >= OB):
//
//	originalRatio = OA / OB
//	bandLow       = originalRatio * (1 - band)
//	bandHigh      = originalRatio * (1 + band)
//	projRatio     = max(proj[a], proj[b]) / min(proj[a], proj[b])
//	require: bandLow <= projRatio <= bandHigh
//
// A component projected to zero serving makes the ratio undefined /
// infinite → out of band → reject. This is what blocks the mass-drain
// failure mode at the gate.
//
// Pairwise so 3+-way topologies (engine + decoder + router) are
// constrained against every pair, not just the largest.
func respectsBand(original, projected map[v1beta1.ComponentType]int32, band float64) bool {
	if band < 0 {
		band = 0
	}
	type entry struct {
		comp v1beta1.ComponentType
		orig int32
		proj int32
	}
	items := make([]entry, 0, len(original))
	for c, orig := range original {
		if orig <= 0 {
			// Components with zero original replicas can't constrain ratio.
			continue
		}
		items = append(items, entry{comp: c, orig: orig, proj: projected[c]})
	}
	if len(items) <= 1 {
		return true
	}
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			a, b := items[i], items[j]
			// Larger-original on top → ratio is always >= 1.
			if a.orig < b.orig {
				a, b = b, a
			}
			origRatio := float64(a.orig) / float64(b.orig)
			bandLow := origRatio * (1 - band)
			bandHigh := origRatio * (1 + band)
			// Project pairwise live serving counts (already after the
			// proposed action — see EvaluateSurge).
			pa, pb := a.proj, b.proj
			if pa < pb {
				pa, pb = pb, pa
			}
			if pb <= 0 {
				// Smaller side dropped to zero serving → undefined ratio.
				// This is the mass-drain failure mode the gate must block.
				return false
			}
			projRatio := float64(pa) / float64(pb)
			if projRatio < bandLow || projRatio > bandHigh {
				return false
			}
		}
	}
	return true
}

// SnapshotOriginal records the desired replica counts at rollout
// start so RatioBalanced has a stable anchor. Caller passes the
// per-Component effective MinReplicas; outputs are int32 because
// ResolveGroup / status writers expect that shape.
//
// Components with MinReplicas == 0 are recorded as 1 — a zero-replica
// Component can't anchor a ratio, and treating zero as one yields the
// same "any surge allowed" behavior as the single-Component path.
func SnapshotOriginal(replicas map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(replicas))
	for c, r := range replicas {
		if r <= 0 {
			r = 1
		}
		out[c] = r
	}
	return out
}

// unavailableInFlight counts a Component's Instances currently taken OUT
// of serving rotation by an in-flight operation (Drain for recreate,
// InPlace for in-place patch). Surge / SurgeDrain ADD a pod rather than
// remove one, so they are excluded. Mirrors
// workload.CurrentUnavailableInFlight on the v1beta1 status shape.
//
// This is OPERATION-tracked (the dispatcher stamps Operation.Step
// synchronously when it starts the op), so it is fresh even when the
// pod-cache-derived ServingReplicas still over-reports during a recreate
// burst — which is what lets the discrete-granularity tiebreaker bound
// in-flight drains to a single pod across reconcile wake-ups.
func unavailableInFlight(statuses []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Operation == nil {
			continue
		}
		switch s.Operation.Step {
		case "Surge", "SurgeDrain":
			// surge adds a pod, it doesn't remove one
		default:
			n++
		}
	}
	return n
}

// CheckRatioGate is the consumer side of RatioBalanced pacing. The
// OMENative ops dispatcher calls it before kicking off a per-Instance
// Update to ask: "given the live cross-Component pod distribution, may
// this Component bump one more pod to the new revision without skewing
// the ratio past tolerance?"
//
// Returns allowed=true when:
//   - The Component is not in any RatioBalanced coord group,
//   - The component is alone in its group (no peer to skew against),
//   - The group has no ObservedRatio snapshot yet (first reconcile),
//   - OR the projected NewPods[component]+1 stays within the group's
//     RatioTolerance band against every peer Component.
//
// Returns allowed=false when EvaluateSurge rejects the +1 surge. Caller
// requeues; the peer Component will have caught up by the next reconcile.
//
// Reads exclusively from isvc.Status — no client/cache hits — so it is
// safe to call from hot paths.
//
// inFlightDelta is the number of pods the dispatcher has ALREADY pulled
// from rotation in THIS wake-up for the caller's Component.
// status.ServingReplicas reflects the prior reconcile pass — without
// subtracting in-flight, every gate decision in one wake-up shares the
// same stale snapshot answer and the dispatcher fires past the band
// before status catches up.
func CheckRatioGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (allowed bool, reason string) {
	// Drain projection (-1) is the conservative default. The strategy-aware
	// caller (coordination.EvaluateUpdateGate) uses the GateContext method
	// directly with a +1 surge projection for SurgeThenDrain.
	return ResolveGateContext(isvc, component).CheckRatio(inFlightDelta, -1)
}

// CheckRatio is the GateContext-aware variant of CheckRatioGate. The
// dispatcher calls ResolveGateContext once per per-Instance tick and
// threads the resulting context into each gate via CheckRatio /
// CheckSurge / CheckUnavailability / CheckSequential, paying the
// resolve prelude exactly once per tick instead of once per gate.
func (ctx GateContext) CheckRatio(inFlightDelta, projDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}
	group := ctx.Group
	isvc := ctx.ISVC
	component := ctx.Component
	if group.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return true, "not RatioBalanced pacing"
	}
	if len(group.Components) <= 1 {
		return true, "single-component group"
	}

	// Build RatioState from the live ISVC status. Original is read from
	// the previously-recorded ObservedRatio (which is SnapshotOriginal'd
	// at rollout start). If absent (first reconcile), let the surge
	// through — the next pass will snapshot and gate.
	state := RatioState{
		Original:        make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Current:         make(map[v1beta1.ComponentType]int32, len(group.Components)),
		NewPods:         make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Serving:         make(map[v1beta1.ComponentType]int32, len(group.Components)),
		InFlightUnavail: make(map[v1beta1.ComponentType]int32, len(group.Components)),
	}
	var groupStatus *v1beta1.RolloutCoordinationGroupStatus
	if isvc.Status.RolloutCoordination != nil {
		for i := range isvc.Status.RolloutCoordination.Groups {
			gs := &isvc.Status.RolloutCoordination.Groups[i]
			if gs.Name == group.Name {
				groupStatus = gs
				break
			}
		}
	}
	if groupStatus == nil || groupStatus.ObservedRatio == nil || len(groupStatus.ObservedRatio.Original) == 0 {
		return true, "no ObservedRatio snapshot yet (first reconcile)"
	}
	for _, c := range group.Components {
		orig := groupStatus.ObservedRatio.Original[c]
		if orig <= 0 {
			orig = 1
		}
		state.Original[c] = orig
		if cs, ok := isvc.Status.Components[c]; ok && cs.Lifecycle != nil {
			state.Current[c] = cs.Lifecycle.Replicas
			state.NewPods[c] = cs.Lifecycle.UpdatedReplicas
			// Serving is what the band check projects against — live
			// traffic capacity, not rollout progress.
			state.Serving[c] = cs.Lifecycle.ServingReplicas
			// Operation-tracked in-flight drains (fresh; see RatioState doc).
			state.InFlightUnavail[c] = unavailableInFlight(cs.Lifecycle.InstanceStatuses)
		}
	}
	// Account for pods this wake-up has already started pulling out of
	// rotation but whose status_aggregate write hasn't landed yet. Only
	// the caller's Component carries in-flight here; peer Components'
	// in-flight is implicit in their own dispatchers' separate runs and
	// will be reflected in their next status write.
	if inFlightDelta > 0 {
		state.Serving[component] -= inFlightDelta
		if state.Serving[component] < 0 {
			state.Serving[component] = 0
		}
	}

	// projDelta is the projected per-pod transient capacity change for THIS
	// update step, set by the strategy-aware caller:
	//   - SurgeThenDrain → +1: a new pod is created and goes Ready BEFORE
	//     the old one drains, so the transient is +1 capacity.
	//   - recreate / drain-first → -1: the old pod leaves rotation first,
	//     so the transient is -1 capacity.
	// The package-level CheckRatioGate wrapper defaults to -1 (drain).
	//
	// Getting this sign right is load-bearing. A hardcoded -1 for a
	// SurgeThenDrain roll mis-models the surge as a capacity loss: on a
	// symmetric ratio (e.g. 4:4, tol 25% → band [0.75, 1.25]) a single -1
	// projects max/min = 4/3 = 1.333 > 1.25, so BOTH members are denied at
	// the start and neither can go first → permanent deadlock. With +1 the
	// same roll projects 5/4 = 1.25 (in band) and proceeds.
	decision := EvaluateSurge(group.Pacing, state, component, projDelta)
	if decision.AllowedSurgeDelta != 0 {
		return true, decision.Reason
	}
	return false, decision.Reason
}

// CheckUnavailabilityGate is the consumer side of MaxUnavailable
// pacing. The OMENative ops dispatcher calls it before kicking off a
// disruptive operation (Update recreate, in-place drain, Restart) to
// ask: "given the live pod observation PLUS the in-flight count from
// this wake-up, may this Component take one more pod offline without
// exceeding the group's MaxUnavailable budget?"
//
// inFlightDelta is the number of pods the dispatcher has ALREADY
// flipped to NotServing in this wake-up. Status counters from
// isvc.Status reflect the prior reconcile pass — they don't see
// in-wake-up writes — so the dispatcher must thread the running count
// in or every gate decision in one wake-up shares the same stale
// snapshot answer (the original mass-outage bug).
//
// Returns allowed=true when:
//   - The Component is not in any coord group,
//   - The component has no observed status yet (first reconcile),
//   - OR (currentUnavailable + inFlightDelta + 1) stays within
//     MaxUnavailableBudget for the Component.
//
// Counts pods serving traffic (ServingReplicas), NOT pods with merely
// running containers (ReadyReplicas). The two diverge during in-place
// updates: kubelet still reports ContainersReady while the controller
// has flipped the serving gate to False; using ReadyReplicas would
// mislead the gate into letting every pod get pulled from traffic at
// once (the original mass-outage bug at scale).
//
// Honest semantic: with MaxUnavailable=0 and an update strategy that
// requires drain-first (in-place, recreate without surge), this gate
// will deadlock rollouts. SurgeThenDrain side-steps the gate by
// creating new pods within the surge budget instead of taking old
// ones offline.
func CheckUnavailabilityGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (allowed bool, reason string) {
	return ResolveGateContext(isvc, component).CheckUnavailability(inFlightDelta)
}

// CheckUnavailability is the GateContext-aware variant of
// CheckUnavailabilityGate. See GateContext / CheckRatio for the
// pre-resolve pattern the dispatcher uses to pay the prelude once per
// per-Instance tick.
func (ctx GateContext) CheckUnavailability(inFlightDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}
	cs, ok := ctx.ISVC.Status.Components[ctx.Component]
	if !ok || cs.Lifecycle == nil {
		return true, "no observation yet"
	}
	desired := cs.Lifecycle.Replicas
	if desired <= 0 {
		desired = 1
	}
	// ServingReplicas (in-load-balancer count) NOT ReadyReplicas
	// (containers-running count) — in-place drain flips serving=False
	// before kubelet sees a container restart, so ReadyReplicas would
	// still report all pods Ready while half are actually out of rotation.
	currentUnavailable := desired - cs.Lifecycle.ServingReplicas
	if currentUnavailable < 0 {
		currentUnavailable = 0
	}
	if inFlightDelta < 0 {
		inFlightDelta = 0
	}
	budget := MaxUnavailableBudget(ctx.Group.Pacing, desired)
	projected := currentUnavailable + inFlightDelta + 1
	if projected > budget {
		return false, fmt.Sprintf("unavailable budget %d exhausted (current %d, in-flight %d, would become %d)",
			budget, currentUnavailable, inFlightDelta, projected)
	}
	return true, "within unavailable budget"
}

// CheckSurgeGate is the surge-side dual of CheckUnavailabilityGate.
// SurgeThenDrain never takes pods OFFLINE (so the unavailability gate
// is irrelevant) but it temporarily adds an extra pod per Instance
// (alive = desired + N where N is the number of in-flight surges).
// This gate enforces the group's MaxSurge budget across the Component.
//
// The dispatcher calls this only on `startingFresh` for instances
// whose UpdateStrategy is SurgeThenDrain (other strategies go through
// CheckUnavailabilityGate). Mirrors the inFlightDelta pattern from the
// other gates: status counters can't see in-wake-up writes, so the
// dispatcher threads its running tally to avoid the stale-snapshot
// trap.
//
// Returns allowed=true when:
//   - The Component is not in any coord group,
//   - The component has no observed status yet (first reconcile),
//   - OR (currentInFlightSurge + inFlightDelta + 1) stays within
//     MaxSurgeBudget for the Component.
//
// currentInFlightSurge = count of Instances whose Operation.Step is
// the surge step (Surge or SurgeDrain) — those are the Instances that
// already have an extra pod alive from a prior wake-up.
func CheckSurgeGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (allowed bool, reason string) {
	return ResolveGateContext(isvc, component).CheckSurge(inFlightDelta)
}

// CheckSurge is the GateContext-aware variant of CheckSurgeGate. See
// GateContext / CheckRatio for the pre-resolve pattern the dispatcher
// uses to pay the prelude once per per-Instance tick.
func (ctx GateContext) CheckSurge(inFlightDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}
	cs, ok := ctx.ISVC.Status.Components[ctx.Component]
	if !ok || cs.Lifecycle == nil {
		return true, "no observation yet"
	}
	desired := cs.Lifecycle.Replicas
	if desired <= 0 {
		desired = 1
	}
	var currentInFlight int32
	for _, s := range cs.Lifecycle.InstanceStatuses {
		if s.Operation == nil {
			continue
		}
		// Surge mechanism uses two op steps: "Surge" (creating new
		// pod) and "SurgeDrain" (waiting drain on old). Both contribute
		// an extra pod alive. Other steps (Drain for recreate, InPlace
		// for in-place patch) are NOT surge — they don't add pods.
		switch s.Operation.Step {
		case "Surge", "SurgeDrain":
			currentInFlight++
		}
	}
	if inFlightDelta < 0 {
		inFlightDelta = 0
	}
	budget := MaxSurgeBudget(ctx.Group.Pacing, desired)
	projected := currentInFlight + inFlightDelta + 1
	if projected > budget {
		return false, fmt.Sprintf("surge budget %d exhausted (current %d, in-flight %d, would become %d)",
			budget, currentInFlight, inFlightDelta, projected)
	}
	return true, "within surge budget"
}

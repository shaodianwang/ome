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
// CheckRatioGate's `inFlightDelta` is the number of pods the
// dispatcher has ALREADY pulled from rotation in THIS wake-up for the
// caller's Component. status.ServingReplicas reflects the prior
// reconcile pass — without subtracting in-flight, every gate decision
// in one wake-up shares the same stale snapshot answer and the
// dispatcher fires past the band before status catches up (the load-
// scale variant of the mass-outage bug; same shape as the fix the
// unavailability gate received in OEP-0007.2 commit 12).
func CheckRatioGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (allowed bool, reason string) {
	if isvc == nil {
		return true, "nil ISVC"
	}
	if isvc.Spec.RolloutCoordination == nil || len(isvc.Spec.RolloutCoordination.Groups) == 0 {
		return true, "no coordination block"
	}
	// Find the group this component is part of.
	groups := ResolveGroups(isvc.Spec.RolloutCoordination)
	var group *ResolvedGroup
	for i := range groups {
		for _, c := range groups[i].Components {
			if c == component {
				group = &groups[i]
				break
			}
		}
		if group != nil {
			break
		}
	}
	if group == nil {
		return true, "component not in any coord group"
	}
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
		Original: make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Current:  make(map[v1beta1.ComponentType]int32, len(group.Components)),
		NewPods:  make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Serving:  make(map[v1beta1.ComponentType]int32, len(group.Components)),
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
		if cs, ok := isvc.Status.Components[c]; ok && cs.OMENative != nil {
			state.Current[c] = cs.OMENative.Replicas
			state.NewPods[c] = cs.OMENative.UpdatedReplicas
			// Serving is what the band check projects against — live
			// traffic capacity, not rollout progress.
			state.Serving[c] = cs.OMENative.ServingReplicas
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

	// Project the drain (–1): the dispatcher is about to pull one more
	// pod from serving rotation. The redesign's "projection = current –
	// 1" semantic (ratio.go header comment) applies here. Using +1
	// (surge) only makes sense once real surge-then-drain ships and the
	// dispatcher creates a new pod before draining the old; today every
	// update strategy drains first, so –1 is the immediate transient
	// the gate must keep in band.
	decision := EvaluateSurge(group.Pacing, state, component, -1)
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
	if isvc == nil {
		return true, "nil ISVC"
	}
	if isvc.Spec.RolloutCoordination == nil || len(isvc.Spec.RolloutCoordination.Groups) == 0 {
		return true, "no coordination block"
	}
	groups := ResolveGroups(isvc.Spec.RolloutCoordination)
	var group *ResolvedGroup
	for i := range groups {
		for _, c := range groups[i].Components {
			if c == component {
				group = &groups[i]
				break
			}
		}
		if group != nil {
			break
		}
	}
	if group == nil {
		return true, "component not in any coord group"
	}

	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return true, "no observation yet"
	}
	desired := cs.OMENative.Replicas
	if desired <= 0 {
		desired = 1
	}
	// ServingReplicas (in-load-balancer count) NOT ReadyReplicas
	// (containers-running count) — in-place drain flips serving=False
	// before kubelet sees a container restart, so ReadyReplicas would
	// still report all pods Ready while half are actually out of rotation.
	currentUnavailable := desired - cs.OMENative.ServingReplicas
	if currentUnavailable < 0 {
		currentUnavailable = 0
	}
	if inFlightDelta < 0 {
		inFlightDelta = 0
	}
	budget := MaxUnavailableBudget(group.Pacing, desired)
	projected := currentUnavailable + inFlightDelta + 1
	if projected > budget {
		return false, fmt.Sprintf("unavailable budget %d exhausted (current %d, in-flight %d, would become %d)",
			budget, currentUnavailable, inFlightDelta, projected)
	}
	return true, "within unavailable budget"
}

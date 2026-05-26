package coordination

import (
	"fmt"
	"math"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// RatioState carries the cross-Component replica counts the
// RatioBalanced pacing math reasons about. Snapshotted at rollout
// start and refreshed every reconcile while the group is in Surging /
// Waiting.
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
	NewPods map[v1beta1.ComponentType]int32
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

// EvaluateSurge returns the per-Component surge budget that respects
// the cross-Component ratio. It is a pure function: feed it the
// pacing block, the snapshot state, the Component being considered,
// and the proposed delta — get back what the controller may apply.
//
// The math: with the original ratio fixed at rollout start, the
// target new-pod count for each Component is proportional. A
// candidate surge is accepted only if the projected ratio between
// the largest Component's new-pod fraction and any peer's new-pod
// fraction stays within (1 ± tolerance). The check generalizes
// pairwise — every (a, b) ratio must satisfy the band — so 3-way
// 40:20:10 topologies don't degenerate to "biggest only."
func EvaluateSurge(pacing v1beta1.CoordinationPacing, state RatioState, component v1beta1.ComponentType, proposedDelta int32) RatioDecision {
	if proposedDelta <= 0 {
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

	for delta := proposedDelta; delta > 0; delta-- {
		projected := projectedNewPods(state.NewPods, component, delta)
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
		Reason:            "surge would skew cross-Component ratio past tolerance",
	}
}

// projectedNewPods clones state.NewPods with component += delta.
func projectedNewPods(in map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, delta int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[component] += delta
	return out
}

// respectsBand reports whether the projected new-pod counts, taken as a
// fraction of each Component's original replica count, fall within the
// pairwise tolerance band against every peer.
//
// The band is the operator-supplied "1 ± tolerance" window. The check
// is pairwise so that asymmetric 3+-way topologies don't degenerate to
// only comparing the largest pair.
func respectsBand(original, projected map[v1beta1.ComponentType]int32, band float64) bool {
	if band < 0 {
		band = 0
	}
	frac := make(map[v1beta1.ComponentType]float64, len(original))
	for c, orig := range original {
		if orig <= 0 {
			// Components with zero original replicas can't constrain
			// ratio. Skip them.
			continue
		}
		newPods := projected[c]
		frac[c] = float64(newPods) / float64(orig)
	}
	if len(frac) <= 1 {
		return true
	}
	// Pairwise check: for every (a, b), |frac[a] - frac[b]| <= band.
	type entry struct {
		comp v1beta1.ComponentType
		f    float64
	}
	keys := make([]entry, 0, len(frac))
	for c, f := range frac {
		keys = append(keys, entry{c, f})
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if math.Abs(keys[i].f-keys[j].f) > band {
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
func CheckRatioGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (allowed bool, reason string) {
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
		}
	}

	decision := EvaluateSurge(group.Pacing, state, component, 1)
	if decision.AllowedSurgeDelta >= 1 {
		return true, decision.Reason
	}
	return false, decision.Reason
}

// CheckUnavailabilityGate is the consumer side of MaxUnavailable
// pacing. The OMENative ops dispatcher calls it before kicking off a
// disruptive operation (Update recreate, Restart) to ask: "given the
// live pod observation, may this Component take one more pod offline
// without exceeding the group's MaxUnavailable budget?"
//
// Returns allowed=true when:
//   - The Component is not in any coord group,
//   - The component has no observed status yet (first reconcile),
//   - OR the projected (currentUnavailable + 1) stays within
//     MaxUnavailableBudget for the Component.
//
// Returns allowed=false when terminating one more pod would push the
// Component above its MaxUnavailable budget. Caller skips the op
// and requeues; once existing recreations complete (Ready replicas
// climbs back), the gate reopens.
//
// Honest semantic: with MaxUnavailable=0 and no surge-then-drain
// support in OMENative today, this gate will deadlock rollouts.
// That's the correct response to "literally zero downtime" without
// the surge capacity to back it up — operators should either allow
// non-zero MaxUnavailable or wait for surge-then-drain support.
//
// Reads exclusively from isvc.Status — no client hits — safe for
// hot paths.
func CheckUnavailabilityGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (allowed bool, reason string) {
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
	currentUnavailable := desired - cs.OMENative.ReadyReplicas
	if currentUnavailable < 0 {
		currentUnavailable = 0
	}
	budget := MaxUnavailableBudget(group.Pacing, desired)
	projected := currentUnavailable + 1
	if projected > budget {
		return false, fmt.Sprintf("unavailable budget %d exhausted (current %d, would become %d)", budget, currentUnavailable, projected)
	}
	return true, "within unavailable budget"
}

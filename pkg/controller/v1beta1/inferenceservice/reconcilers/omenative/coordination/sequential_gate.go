package coordination

import (
	"fmt"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// CheckSequentialGate is the consumer side of Sequential coordination.
// The OMENative ops dispatcher calls it before kicking off a fresh
// per-Instance Update so the Sequential invariant — "at most one
// Component rolls at a time, in the operator-declared Order" — is
// enforced on the action side, not just observed on the status side.
//
// Without this gate, both Components of a 2-Component Sequential group
// would recreate concurrently the moment the operator bumps both images:
// the Sequential state machine correctly emits `decoder.Surging` then
// `engine.Surging`, but the dispatcher fires Update on whichever
// Instance's `detectUpdateTrigger` returns true, regardless of which
// Component the state machine considers active. The composite phase
// `decoder.Idle` / `Sequential.Awaiting` never lands in status because
// engine starts surging before decoder finishes.
//
// Returns allowed=true when:
//   - The Component is not in any coordination group,
//   - The Component's group Policy is not Sequential (gate doesn't apply),
//   - The Component is the active Sequential Component (its turn), AND
//     no soak window is currently held,
//   - No Component in the group has a rollout in flight (idle group).
//
// Returns allowed=false when:
//   - A different Component in the Order is currently rolling (the
//     active selector picks that one; this Component must wait).
//   - The Component IS the active one but the operator-configured soak
//     window between the previous Component's completion and this one's
//     start is still in effect.
//
// The "already mid-update" branch in detectUpdateTrigger intentionally
// bypasses gates so in-flight updates aren't stranded; the dispatcher
// only calls this gate on startingFresh, matching CheckRatioGate /
// CheckUnavailabilityGate.
//
// Reads exclusively from isvc.Spec + isvc.Status (no client / cache
// hits) so it is safe to call from the hot dispatcher loop.
func CheckSequentialGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (allowed bool, reason string) {
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
	if group.Policy != v1beta1.CoordinationPolicySequential {
		return true, "not Sequential pacing"
	}
	if len(group.Order) == 0 {
		// Sequential without an Order is malformed (ValidateGroupShape
		// would have rejected this). Bypass rather than wedge — the
		// group already has a louder problem.
		return true, "Sequential group missing Order"
	}

	// Compute per-Component RolloutInFlight observations from live ISVC
	// status — mirrors buildComponentObservation's revision-hash check
	// but on the much smaller subset the gate needs.
	observations := observeSequentialComponentsForGate(isvc, group.Components)
	active, hasActive := activeSequentialComponent(group.Order, observations)
	if !hasActive {
		// No Component in flight at all — the group is idle (the
		// operator hasn't bumped anything yet, or the wake-up is racing
		// the very first observation). Don't block; the moment any
		// Component flips RolloutInFlight, the gate will route the
		// dispatcher to the first Order entry.
		return true, "Sequential group idle"
	}
	if active != component {
		return false, fmt.Sprintf("Sequential waiting on %s", active)
	}

	// active == component: this Component IS its turn. Honor the soak
	// window between completion of the previous Component and the start
	// of this one. The state machine's sequentialTransition holds the
	// composite phase at Sequential.Awaiting during the soak; the gate
	// mirrors that on the dispatcher side so the per-Instance Update
	// doesn't fire while the operator's soak is still elapsing.
	if group.Soak > 0 && previousCompletedComponentForGate(group.Order, observations, component) != "" {
		entered := previousPhaseEnteredAtFor(isvc, group.Name)
		if !entered.IsZero() {
			elapsed := time.Since(entered)
			if elapsed < group.Soak {
				remaining := group.Soak - elapsed
				return false, fmt.Sprintf("Sequential.Soak: %s remaining", remaining)
			}
		}
	}

	return true, "Sequential active Component"
}

// observeSequentialComponentsForGate builds the per-Component
// RolloutInFlight observations the gate needs from the live ISVC
// status. Mirrors buildComponentObservation's revision-hash compare
// but skips the pod-count / desired-replicas fields the gate doesn't
// consult. Returns one ComponentObservation per `components` entry —
// Components with no status block get a zero-valued observation
// (RolloutInFlight=false), which is exactly what the active-selector
// needs.
func observeSequentialComponentsForGate(isvc *v1beta1.InferenceService, components []v1beta1.ComponentType) map[v1beta1.ComponentType]ComponentObservation {
	out := make(map[v1beta1.ComponentType]ComponentObservation, len(components))
	for _, c := range components {
		obs := ComponentObservation{Component: c}
		if cs, ok := isvc.Status.Components[c]; ok && cs.OMENative != nil {
			currentHash := revisionHashFromName(isvc.Name, c, cs.OMENative.CurrentRevision)
			targetHash := revisionHashFromName(isvc.Name, c, cs.OMENative.UpdateRevision)
			obs.CurrentRevisionHash = currentHash
			obs.TargetRevisionHash = targetHash
			obs.RolloutInFlight = targetHash != "" && targetHash != currentHash
		}
		out[c] = obs
	}
	return out
}

// previousCompletedComponentForGate is the order-aware "what came
// before `c`" lookup that mirrors previousCompletedComponent on
// GroupObservation. Pulled out into its own helper so the gate can
// answer the soak-arming question (is there a prior Component whose
// completion started the soak window?) without synthesizing a full
// GroupObservation.
func previousCompletedComponentForGate(order []v1beta1.ComponentType, observations map[v1beta1.ComponentType]ComponentObservation, c v1beta1.ComponentType) v1beta1.ComponentType {
	for i, oc := range order {
		if oc != c {
			continue
		}
		if i == 0 {
			return ""
		}
		// The previous entry counts as "completed" only when its
		// rollout is no longer in flight. If the previous Component is
		// still rolling, soak hasn't armed yet (the state machine
		// would have routed to it instead of us).
		prev := order[i-1]
		if pobs, ok := observations[prev]; ok && pobs.RolloutInFlight {
			return ""
		}
		return prev
	}
	return ""
}

// previousPhaseEnteredAtFor reads the LastTransitionTime of the named
// group from the ISVC's status — the soak gate consumes this to time
// the "how long since the previous Component completed?" question.
// Returns zero time when no prior group status exists; callers treat
// that as "no clock running, don't gate."
func previousPhaseEnteredAtFor(isvc *v1beta1.InferenceService, groupName string) time.Time {
	if isvc.Status.RolloutCoordination == nil {
		return time.Time{}
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName && gs.LastTransitionTime != nil {
			return gs.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

package coordination

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// conditionAvailable is the Component-level condition type stamped by
// the OMENative status reconciler when AvailableReplicas==Replicas.
// Duplicated as a local const to avoid importing the OMENative status
// package (which would pull the entire pod/conditions write path into
// the coordination layer's import graph).
const conditionAvailable = "Available"

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

	group, ok := MembershipFor(ResolveGroups(isvc.Spec.RolloutCoordination), component)
	if !ok {
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
	//
	// Timestamp source: read the previous Component's Available
	// condition's LastTransitionTime. The Available condition flips to
	// True the moment AvailableReplicas==Replicas — exactly when the
	// previous Component finished its rollout. The group's
	// LastTransitionTime is the wrong source: it only updates when the
	// base Phase changes, and Sequential's soak window holds the group
	// at Phase=Idle (CompositePhase=Sequential.AwaitingNextComponent),
	// so LastTransitionTime points back at the pre-rollout Idle and
	// time.Since(entered) is always past soakDuration.
	if group.Soak > 0 {
		if prev := previousCompletedComponentForGate(group.Order, observations, component); prev != "" {
			completed := previousComponentReadyAt(isvc, prev)
			if !completed.IsZero() {
				elapsed := time.Since(completed)
				if elapsed < group.Soak {
					remaining := group.Soak - elapsed
					return false, fmt.Sprintf("Sequential.Soak: %s remaining", remaining)
				}
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
//
// RolloutInFlight is derived from TWO independent signals:
//
//  1. Revision-hash skew: `UpdateRevision != CurrentRevision` — the
//     classic "spec changed and status caught up enough to record the
//     new target name" signal. This is what fires once the deferred
//     AggregateAndWriteStatus has stamped UpdateRevision=target.Name on
//     the Component being reconciled.
//
//  2. Generation skew: `ObservedGeneration < isvc.Generation` — fires
//     at the *moment of bump*, BEFORE any deferred status write has
//     run. The top-level reconciler computes `target` via
//     ensureRevisionWithCollisionRetry and defers
//     AggregateAndWriteStatus until after the op chain returns; the
//     dispatcher's gate check is invoked between those two points. At
//     that instant UpdateRevision still names the OLD CR (so signal #1
//     is silent), but every per-Component status block is one
//     reconcile behind isvc.Generation. The generation skew turns
//     RolloutInFlight on for every Component whose status hasn't
//     caught up to the bump — and the active-selector then routes the
//     dispatcher to the first one in Order, denying the rest.
//
// Over-detection from signal #2 is harmless. A Component whose target
// PodSpec didn't actually change still gets observed as in-flight here,
// but detectUpdateTrigger in the dispatcher returns needs=false for
// that Component (its pods' RunningRevision already matches target.Name)
// and the gate is never consulted. Once AggregateAndWriteStatus runs
// and bumps ObservedGeneration, the noise clears.
func observeSequentialComponentsForGate(isvc *v1beta1.InferenceService, components []v1beta1.ComponentType) map[v1beta1.ComponentType]ComponentObservation {
	out := make(map[v1beta1.ComponentType]ComponentObservation, len(components))
	for _, c := range components {
		obs := ComponentObservation{Component: c}
		if cs, ok := isvc.Status.Components[c]; ok && cs.OMENative != nil {
			currentHash := revisionHashFromName(isvc.Name, c, cs.OMENative.CurrentRevision)
			targetHash := revisionHashFromName(isvc.Name, c, cs.OMENative.UpdateRevision)
			obs.CurrentRevisionHash = currentHash
			obs.TargetRevisionHash = targetHash
			revisionSkew := targetHash != "" && targetHash != currentHash
			generationSkew := cs.OMENative.ObservedGeneration < isvc.Generation
			obs.RolloutInFlight = revisionSkew || generationSkew
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

// previousComponentReadyAt returns the moment the named Component
// became Available (every Instance Ready) — read from the Component's
// `Available` condition's LastTransitionTime. This is the wall-clock
// anchor the Sequential soak gate compares `time.Since(...)` against.
//
// Returns the zero time when:
//   - The Component has no OMENative status block yet (first reconcile
//     before the status writer has run).
//   - The Available condition has not been written yet, OR is currently
//     False (the previous Component never finished a rollout, so there
//     is no completion timestamp to honor — the gate's caller treats
//     this as "no soak clock, allow").
//
// Why not `RolloutCoordinationGroupStatus.LastTransitionTime`?
// `LastTransitionTime` is updated only when the group's base Phase
// changes. Sequential's soak window holds the group at Phase=Idle while
// only CompositePhase advances to Sequential.AwaitingNextComponent —
// Phase stays constant, so LastTransitionTime keeps pointing at the
// initial pre-rollout Idle. `time.Since(LastTransitionTime)` is always
// well past `soak`, the gate never fires, and the soak window is a
// no-op. The Available condition tracks the actual rollout-completion
// moment because `apimeta.SetStatusCondition` only updates
// LastTransitionTime on real status transitions (False→True).
func previousComponentReadyAt(isvc *v1beta1.InferenceService, prev v1beta1.ComponentType) time.Time {
	cs, ok := isvc.Status.Components[prev]
	if !ok || cs.OMENative == nil {
		return time.Time{}
	}
	for _, cond := range cs.OMENative.Conditions {
		if cond.Type != conditionAvailable {
			continue
		}
		if cond.Status != metav1.ConditionTrue {
			return time.Time{}
		}
		return cond.LastTransitionTime.Time
	}
	return time.Time{}
}

package coordination

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ComponentObservation captures the inputs the state machine reads for
// one Component in a group: replica counts, per-revision pod counts,
// in-flight failure / pause hints. Populated by the caller from
// status_aggregate plus live pod reads.
type ComponentObservation struct {
	// Component identifies this observation.
	Component v1beta1.ComponentType

	// DesiredReplicas is the Component's MinReplicas (or 1).
	DesiredReplicas int32

	// TotalPods is the live pod count, any revision.
	TotalPods int32

	// ReadyPods is the live pod count reporting Ready=True.
	ReadyPods int32

	// NewRevisionPods is the live pod count on the target
	// ControllerRevision (UpdateRevision in OMENativeComponentStatus
	// terms). When zero, the Component is not yet surging.
	NewRevisionPods int32

	// NewRevisionReadyPods is the subset of NewRevisionPods that
	// reports Ready=True.
	NewRevisionReadyPods int32

	// TargetRevisionHash is the ControllerRevision hash the Component
	// is converging toward. Empty when no rollout is in flight (the
	// Component is already at its desired revision).
	TargetRevisionHash string

	// CurrentRevisionHash is the ControllerRevision hash the
	// Component is currently serving.
	CurrentRevisionHash string

	// RolloutInFlight is true when DesiredRevisionHash !=
	// CurrentRevisionHash. The state machine routes off this flag
	// rather than recomputing it from the two hashes so callers can
	// override (e.g., for tests).
	RolloutInFlight bool

	// Failed is true when the Component's OMENative reconciler
	// flagged a terminal failure (Ready timeout, persistent op error).
	Failed bool

	// FailureMessage is the operator-facing explanation when Failed.
	FailureMessage string

	// Paused is true when the Component's rollout is held at the
	// current phase by an annotation or step-boundary pause.
	Paused bool
}

// GroupObservation collects per-Component observations plus group-wide
// hints the state machine reads.
type GroupObservation struct {
	// Group is the resolved coordination group.
	Group ResolvedGroup

	// Components maps Component → observation. Includes every
	// Component listed in Group.Components.
	Components map[v1beta1.ComponentType]ComponentObservation

	// OriginalReplicas is the snapshot the RatioBalanced pacing math
	// reads. Snapshotted at rollout start; nil when no rollout has
	// started yet.
	OriginalReplicas map[v1beta1.ComponentType]int32

	// PausedGlobal is true when the operator paused the whole
	// rollout via ome.io/rollout-paused: "true" on the ISVC. The
	// state machine honors this by leaving every group in Paused
	// until the annotation is removed.
	PausedGlobal bool
}

// GroupTransition is the state machine's decision for one group this
// reconcile. The caller applies the decision (writes status, surges
// pods, drains, etc.) without re-reading the state.
type GroupTransition struct {
	// Phase is the next base phase.
	Phase v1beta1.CoordinationPhase

	// CompositePhase is the operator-facing composite phase string
	// (e.g., `decoder.Surging` for Sequential).
	CompositePhase string

	// CurrentComponent names the Component the group is actively
	// reconciling (Sequential only).
	CurrentComponent v1beta1.ComponentType

	// PreviousComponent names the most recently completed Component
	// (Sequential only).
	PreviousComponent v1beta1.ComponentType

	// Message is the operator-facing explanation of the phase.
	Message string
}

// ComputeTransition walks the group's state machine for this reconcile
// and returns the decision. Pure function — no I/O, no status writes.
// Status writers translate the decision; tests assert against this
// shape directly.
func ComputeTransition(obs GroupObservation) GroupTransition {
	if obs.PausedGlobal {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhasePaused,
			CompositePhase: string(v1beta1.CoordinationPhasePaused),
			Message:        "rollout paused via ome.io/rollout-paused annotation",
		}
	}
	switch obs.Group.Policy {
	case v1beta1.CoordinationPolicyAtomic:
		return atomicTransition(obs)
	case v1beta1.CoordinationPolicyPairedMixable:
		return pairedMixableTransition(obs)
	case v1beta1.CoordinationPolicySequential:
		return sequentialTransition(obs)
	case v1beta1.CoordinationPolicyIndependent:
		fallthrough
	default:
		return independentTransition(obs)
	}
}

// atomicTransition implements the gang-style state machine: surge all
// Components together, wait for all Ready, shift traffic together,
// drain, scale down. Failure in any Component fails the whole group.
func atomicTransition(obs GroupObservation) GroupTransition {
	allFailed := false
	for _, c := range obs.Components {
		if c.Failed {
			allFailed = true
			break
		}
	}
	if allFailed {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseFailed,
			CompositePhase: string(v1beta1.CoordinationPhaseFailed),
			Message:        "one or more Components in the Atomic group failed",
		}
	}

	anyPaused := false
	for _, c := range obs.Components {
		if c.Paused {
			anyPaused = true
			break
		}
	}
	if anyPaused {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhasePaused,
			CompositePhase: string(v1beta1.CoordinationPhasePaused),
			Message:        "one or more Components in the Atomic group are paused",
		}
	}

	rolling := false
	for _, c := range obs.Components {
		if c.RolloutInFlight {
			rolling = true
			break
		}
	}
	if !rolling {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "no rollout in flight",
		}
	}

	allHaveSurge := true
	allNewReady := true
	for _, c := range obs.Components {
		if c.NewRevisionPods == 0 {
			allHaveSurge = false
		}
		if c.NewRevisionReadyPods < c.NewRevisionPods || c.NewRevisionPods == 0 {
			allNewReady = false
		}
	}

	switch {
	case !allHaveSurge:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseSurging,
			CompositePhase: string(v1beta1.CoordinationPhaseSurging),
			Message:        "creating new-revision pods",
		}
	case !allNewReady:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseWaiting,
			CompositePhase: string(v1beta1.CoordinationPhaseWaiting),
			Message:        "waiting for new-revision pods to be Ready",
		}
	}

	// All Components have surge pods AND every surge pod is Ready.
	// Walk Shifting → Draining → ScalingDown. We collapse the
	// post-Ready phases into a single Shifting return here so the
	// status writer (which sees the traffic delta) can advance to
	// Draining/ScalingDown based on whether the producer has
	// already written and whether old pods still exist.
	oldStillUp := false
	for _, c := range obs.Components {
		if c.TotalPods > c.NewRevisionPods {
			oldStillUp = true
			break
		}
	}
	if oldStillUp {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseShifting,
			CompositePhase: string(v1beta1.CoordinationPhaseShifting),
			Message:        "shifting traffic; old-revision pods still serving",
		}
	}
	return GroupTransition{
		Phase:          v1beta1.CoordinationPhaseScalingDown,
		CompositePhase: string(v1beta1.CoordinationPhaseScalingDown),
		Message:        "scaling down old-revision pods",
	}
}

// pairedMixableTransition implements PairedMixable: per-Component
// state machines walk in parallel; the group phase is the union (any
// Component still surging means the group is Surging; otherwise
// Waiting → Shifting → ScalingDown → Idle).
func pairedMixableTransition(obs GroupObservation) GroupTransition {
	failed := false
	for _, c := range obs.Components {
		if c.Failed {
			failed = true
			break
		}
	}
	if failed {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseFailed,
			CompositePhase: string(v1beta1.CoordinationPhaseFailed),
			Message:        "one or more Components in the PairedMixable group failed",
		}
	}

	anyRolling := false
	for _, c := range obs.Components {
		if c.RolloutInFlight {
			anyRolling = true
			break
		}
	}
	if !anyRolling {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "no rollout in flight",
		}
	}

	anySurging := false
	anyWaiting := false
	anyScalingDown := false
	for _, c := range obs.Components {
		if !c.RolloutInFlight {
			continue
		}
		if c.NewRevisionPods == 0 {
			anySurging = true
			continue
		}
		if c.NewRevisionReadyPods < c.NewRevisionPods {
			anyWaiting = true
			continue
		}
		if c.TotalPods > c.NewRevisionPods {
			anyScalingDown = true
		}
	}
	switch {
	case anySurging:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseSurging,
			CompositePhase: string(v1beta1.CoordinationPhaseSurging),
			Message:        "at least one Component is surging",
		}
	case anyWaiting:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseWaiting,
			CompositePhase: string(v1beta1.CoordinationPhaseWaiting),
			Message:        "waiting for new-revision pods to be Ready",
		}
	case anyScalingDown:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseScalingDown,
			CompositePhase: string(v1beta1.CoordinationPhaseScalingDown),
			Message:        "scaling down old-revision pods",
		}
	default:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseShifting,
			CompositePhase: string(v1beta1.CoordinationPhaseShifting),
			Message:        "writing per-Component traffic weights",
		}
	}
}

// independentTransition collapses every rolling Component into the
// group's phase but skips the gang-readiness check. Used both for
// explicit Independent groups and as the implicit default when no
// group is declared (callers that drive an InferenceService without a
// rolloutCoordination block hit this branch).
func independentTransition(obs GroupObservation) GroupTransition {
	anyRolling := false
	failed := false
	for _, c := range obs.Components {
		if c.Failed {
			failed = true
		}
		if c.RolloutInFlight {
			anyRolling = true
		}
	}
	if failed {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseFailed,
			CompositePhase: string(v1beta1.CoordinationPhaseFailed),
			Message:        "one or more Components in the Independent group failed",
		}
	}
	if !anyRolling {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "no rollout in flight",
		}
	}
	return GroupTransition{
		Phase:          v1beta1.CoordinationPhaseShifting,
		CompositePhase: string(v1beta1.CoordinationPhaseShifting),
		Message:        "independent per-Component rollouts in flight",
	}
}

// sequentialTransition implements Sequential: walk the group's
// Order one Component at a time. The next Component cannot enter
// Surging until the previous Component is back at Idle on its new
// revision.
func sequentialTransition(obs GroupObservation) GroupTransition {
	// Failure block: if any earlier Component in the Order is in
	// Failed, the group enters Sequential.Failed and stays there until
	// the operator reverts the failed Component's spec.
	for _, c := range obs.Group.Order {
		comp, ok := obs.Components[c]
		if !ok {
			continue
		}
		if comp.Failed {
			return GroupTransition{
				Phase:             v1beta1.CoordinationPhaseFailed,
				CompositePhase:    v1beta1.CompositePhaseSequentialFailed,
				CurrentComponent:  c,
				PreviousComponent: previousCompletedComponent(obs, c),
				Message:           "Sequential group blocked by Failed Component: " + string(c),
			}
		}
	}

	// Pick the Component in the Order that's currently rolling, if any.
	// Sequential's invariant: at most one Component rolls at a time.
	for _, c := range obs.Group.Order {
		comp, ok := obs.Components[c]
		if !ok || !comp.RolloutInFlight {
			continue
		}
		// Found the active Component — drive its per-Component phase
		// as the group phase.
		sub := atomicTransition(GroupObservation{
			Group: ResolvedGroup{
				Name:       obs.Group.Name,
				Index:      obs.Group.Index,
				Components: []v1beta1.ComponentType{c},
				Policy:     v1beta1.CoordinationPolicyAtomic,
				Pacing:     obs.Group.Pacing,
			},
			Components: map[v1beta1.ComponentType]ComponentObservation{c: comp},
		})
		return GroupTransition{
			Phase:             sub.Phase,
			CompositePhase:    string(c) + "." + string(sub.Phase),
			CurrentComponent:  c,
			PreviousComponent: previousCompletedComponent(obs, c),
			Message:           sub.Message,
		}
	}

	// No Component currently rolling. Determine whether we're at Idle
	// (everything done in this order) or in the operator-controlled
	// soak window between Components.
	completed := completedSequentialComponents(obs)
	if completed >= len(obs.Group.Order) {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "Sequential group complete",
		}
	}
	if completed == 0 {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "Sequential group awaiting first Component spec bump",
		}
	}
	prev := obs.Group.Order[completed-1]
	next := obs.Group.Order[completed]
	return GroupTransition{
		Phase:             v1beta1.CoordinationPhaseIdle,
		CompositePhase:    v1beta1.CompositePhaseSequentialAwaiting,
		CurrentComponent:  next,
		PreviousComponent: prev,
		Message:           "Sequential group in soak window between Components",
	}
}

// completedSequentialComponents counts the number of leading Components
// in the Order that have finished their rollout (RolloutInFlight is
// false on them). The count drives Sequential's "which Component is
// next" decision.
func completedSequentialComponents(obs GroupObservation) int {
	count := 0
	for _, c := range obs.Group.Order {
		comp, ok := obs.Components[c]
		if !ok {
			return count
		}
		if comp.RolloutInFlight {
			return count
		}
		count++
	}
	return count
}

// previousCompletedComponent returns the Component immediately
// preceding c in the Order whose rollout has completed (or empty when
// c is the first Order entry).
func previousCompletedComponent(obs GroupObservation, c v1beta1.ComponentType) v1beta1.ComponentType {
	for i, oc := range obs.Group.Order {
		if oc != c {
			continue
		}
		if i == 0 {
			return ""
		}
		return obs.Group.Order[i-1]
	}
	return ""
}

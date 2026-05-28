package coordination

import (
	"time"

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

	// Now is the reference time the state machine compares against
	// when evaluating time-based gates (Sequential soak). Caller
	// passes time.Now() at the top of the reconcile pass so all gates
	// see the same instant.
	Now time.Time

	// PreviousPhaseEnteredAt is the timestamp the group last entered
	// its current Phase, as recorded in the previously-written status.
	// Sequential soak compares Now - PreviousPhaseEnteredAt to the
	// group's Soak duration. Zero when no prior status is available
	// (first reconcile) — soak is treated as not-yet-started in that
	// case, which means the very first reconcile after completion does
	// trip the wait correctly.
	PreviousPhaseEnteredAt time.Time
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

	// SurgeBudget reports the maximum number of new-revision pods the
	// caller may create for each Component this reconcile, computed
	// from the group's pacing block via MaxSurgeBudget AND (for
	// RatioBalanced pacing) gated by EvaluateSurge. Nil when the
	// transition's Phase doesn't authorize surge (Idle, Paused, Failed,
	// Shifting, ScalingDown). Caller plumbs these into the per-Component
	// surge logic to respect the operator-supplied MaxSurge.
	SurgeBudget map[v1beta1.ComponentType]int32

	// UnavailableBudget reports the maximum number of old-revision pods
	// the caller may terminate before the new-revision replacements are
	// Ready. Mirrors SurgeBudget — nil when not in a phase that
	// authorizes pod termination.
	UnavailableBudget map[v1beta1.ComponentType]int32

	// RatioSkewRejected is true when at least one Component's surge
	// budget was reduced to zero by RatioBalanced pacing this reconcile.
	// Surfaces the EvaluateSurge SkewRejected signal so callers can
	// emit the RatioSkewRejected event + increment the ratio_skew_total
	// counter. Always false for PerComponent pacing.
	RatioSkewRejected bool
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
	case v1beta1.CoordinationPolicyBlueGreen:
		return blueGreenTransition(obs)
	case v1beta1.CoordinationPolicyRollingUpdate:
		return rollingUpdateTransition(obs)
	case v1beta1.CoordinationPolicySequential:
		return sequentialTransition(obs)
	case v1beta1.CoordinationPolicyIndependent:
		fallthrough
	default:
		return independentTransition(obs)
	}
}

// blueGreenTransition implements the gang-style state machine: surge all
// Components together, wait for all Ready, shift traffic together,
// drain, scale down. Failure in any Component fails the whole group.
func blueGreenTransition(obs GroupObservation) GroupTransition {
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
			Message:        "one or more Components in the BlueGreen group failed",
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
			Message:        "one or more Components in the BlueGreen group are paused",
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
		budget, skew := computeSurgeBudgetWithRatio(obs)
		return GroupTransition{
			Phase:             v1beta1.CoordinationPhaseSurging,
			CompositePhase:    string(v1beta1.CoordinationPhaseSurging),
			Message:           "creating new-revision pods",
			SurgeBudget:       budget,
			UnavailableBudget: computeUnavailableBudget(obs),
			RatioSkewRejected: skew,
		}
	case !allNewReady:
		budget, skew := computeSurgeBudgetWithRatio(obs)
		return GroupTransition{
			Phase:             v1beta1.CoordinationPhaseWaiting,
			CompositePhase:    string(v1beta1.CoordinationPhaseWaiting),
			Message:           "waiting for new-revision pods to be Ready",
			SurgeBudget:       budget,
			UnavailableBudget: computeUnavailableBudget(obs),
			RatioSkewRejected: skew,
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

// rollingUpdateTransition implements RollingUpdate: per-Component
// state machines walk in parallel; the group phase is the union (any
// Component still surging means the group is Surging; otherwise
// Waiting → Shifting → ScalingDown → Idle).
//
// Pause semantics differ from BlueGreen: BlueGreen short-circuits to Paused
// when ANY Component is paused (gang-stop), but RollingUpdate lets
// each Component progress independently, so the group is Paused only
// when EVERY Component is paused.
func rollingUpdateTransition(obs GroupObservation) GroupTransition {
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
			Message:        "one or more Components in the RollingUpdate group failed",
		}
	}

	allPaused := len(obs.Components) > 0
	for _, c := range obs.Components {
		if !c.Paused {
			allPaused = false
			break
		}
	}
	if allPaused {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhasePaused,
			CompositePhase: string(v1beta1.CoordinationPhasePaused),
			Message:        "all Components in the RollingUpdate group are paused",
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
		budget, skew := computeSurgeBudgetWithRatio(obs)
		return GroupTransition{
			Phase:             v1beta1.CoordinationPhaseSurging,
			CompositePhase:    string(v1beta1.CoordinationPhaseSurging),
			Message:           "at least one Component is surging",
			SurgeBudget:       budget,
			UnavailableBudget: computeUnavailableBudget(obs),
			RatioSkewRejected: skew,
		}
	case anyWaiting:
		budget, skew := computeSurgeBudgetWithRatio(obs)
		return GroupTransition{
			Phase:             v1beta1.CoordinationPhaseWaiting,
			CompositePhase:    string(v1beta1.CoordinationPhaseWaiting),
			Message:           "waiting for new-revision pods to be Ready",
			SurgeBudget:       budget,
			UnavailableBudget: computeUnavailableBudget(obs),
			RatioSkewRejected: skew,
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
//
// Pause semantics match RollingUpdate's: Independent allows each
// Component to progress on its own, so the group is Paused only when
// EVERY Component is paused (not BlueGreen's "ANY paused").
func independentTransition(obs GroupObservation) GroupTransition {
	anyRolling := false
	failed := false
	allPaused := len(obs.Components) > 0
	for _, c := range obs.Components {
		if c.Failed {
			failed = true
		}
		if c.RolloutInFlight {
			anyRolling = true
		}
		if !c.Paused {
			allPaused = false
		}
	}
	if failed {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseFailed,
			CompositePhase: string(v1beta1.CoordinationPhaseFailed),
			Message:        "one or more Components in the Independent group failed",
		}
	}
	if allPaused {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhasePaused,
			CompositePhase: string(v1beta1.CoordinationPhasePaused),
			Message:        "all Components in the Independent group are paused",
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
	if c, ok := activeSequentialComponent(obs.Group.Order, obs.Components); ok {
		comp := obs.Components[c]
		// Soak gate: if a previous Component just completed and the
		// group is still inside its operator-configured soak window,
		// hold this Component at Sequential.Awaiting until the wait
		// elapses. The soak timer reads from the previously-written
		// status's LastTransitionTime (PreviousPhaseEnteredAt), so
		// soak only fires AFTER the prior completion was observed in
		// status — the very first time the Awaiting phase shows up.
		if obs.Group.Soak > 0 && previousCompletedComponent(obs, c) != "" {
			if !obs.PreviousPhaseEnteredAt.IsZero() {
				elapsed := obs.Now.Sub(obs.PreviousPhaseEnteredAt)
				if elapsed < obs.Group.Soak {
					return GroupTransition{
						Phase:             v1beta1.CoordinationPhaseIdle,
						CompositePhase:    v1beta1.CompositePhaseSequentialAwaiting,
						CurrentComponent:  c,
						PreviousComponent: previousCompletedComponent(obs, c),
						Message: "Sequential soak: " + (obs.Group.Soak - elapsed).String() +
							" remaining before " + string(c) + " starts",
					}
				}
			}
		}
		// Found the active Component — drive its per-Component phase
		// as the group phase.
		sub := blueGreenTransition(GroupObservation{
			Group: ResolvedGroup{
				Name:       obs.Group.Name,
				Index:      obs.Group.Index,
				Components: []v1beta1.ComponentType{c},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
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

// activeSequentialComponent returns the first Component in `order`
// whose observation has RolloutInFlight=true. The state machine routes
// Sequential's "which Component is currently rolling" decision off
// this; the dispatcher's CheckSequentialGate consumes the same helper
// so the two surfaces never disagree on what's active. Returns ok=false
// when no Component in `order` is rolling (Sequential is idle).
func activeSequentialComponent(order []v1beta1.ComponentType, components map[v1beta1.ComponentType]ComponentObservation) (v1beta1.ComponentType, bool) {
	for _, c := range order {
		comp, ok := components[c]
		if !ok || !comp.RolloutInFlight {
			continue
		}
		return c, true
	}
	return "", false
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

// computeSurgeBudget returns the per-Component MaxSurge budget for the
// group's pacing, keyed by Component. Caller plumbs each value into
// the per-Component surge logic so the operator's MaxSurge bound is
// respected end-to-end. PerComponent-pacing only — for RatioBalanced,
// use computeSurgeBudgetWithRatio.
func computeSurgeBudget(obs GroupObservation) map[v1beta1.ComponentType]int32 {
	pacing := obs.Group.Pacing
	out := make(map[v1beta1.ComponentType]int32, len(obs.Components))
	for c, comp := range obs.Components {
		replicas := comp.DesiredReplicas
		if replicas <= 0 {
			replicas = 1
		}
		out[c] = MaxSurgeBudget(pacing, replicas)
	}
	return out
}

// computeSurgeBudgetWithRatio returns SurgeBudget and a skew-rejected
// flag. For PerComponent pacing this is identical to computeSurgeBudget
// + skew=false. For RatioBalanced pacing, EvaluateSurge gates the
// MaxSurge budget — when the ratio band would be exceeded by even one
// surge, the budget is zeroed and skew=true so the caller can fire
// RatioSkewRejected metrics/events.
func computeSurgeBudgetWithRatio(obs GroupObservation) (map[v1beta1.ComponentType]int32, bool) {
	budget := computeSurgeBudget(obs)
	if obs.Group.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return budget, false
	}
	if len(obs.OriginalReplicas) == 0 {
		// No anchor yet — let MaxSurge govern alone, no skew possible
		// until the next reconcile when SnapshotOriginal has run.
		return budget, false
	}
	state := RatioState{
		Original: obs.OriginalReplicas,
		Current:  make(map[v1beta1.ComponentType]int32, len(obs.Components)),
		NewPods:  make(map[v1beta1.ComponentType]int32, len(obs.Components)),
	}
	for c, comp := range obs.Components {
		state.Current[c] = comp.TotalPods
		state.NewPods[c] = comp.NewRevisionPods
	}
	var skew bool
	for c, proposed := range budget {
		if proposed <= 0 {
			continue
		}
		d := EvaluateSurge(obs.Group.Pacing, state, c, proposed)
		budget[c] = d.AllowedSurgeDelta
		if d.SkewRejected {
			skew = true
		}
	}
	return budget, skew
}

// computeUnavailableBudget mirrors computeSurgeBudget for the
// MaxUnavailable side.
func computeUnavailableBudget(obs GroupObservation) map[v1beta1.ComponentType]int32 {
	pacing := obs.Group.Pacing
	out := make(map[v1beta1.ComponentType]int32, len(obs.Components))
	for c, comp := range obs.Components {
		replicas := comp.DesiredReplicas
		if replicas <= 0 {
			replicas = 1
		}
		out[c] = MaxUnavailableBudget(pacing, replicas)
	}
	return out
}

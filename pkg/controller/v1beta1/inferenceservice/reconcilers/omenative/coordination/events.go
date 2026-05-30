package coordination

// Event reasons emitted on the InferenceService for coordination state
// machine transitions. Operators see these in `kubectl describe isvc`
// — they are the primary surface for understanding what the
// coordination layer is doing.
const (
	// EventReasonGroupSurging fires when a group enters Surging.
	EventReasonGroupSurging = "CoordinationGroupSurging"

	// EventReasonGroupShifting fires when a group enters Shifting and
	// the Status.Components.<c>.Traffic[] writer has updated the
	// per-Component traffic weights.
	EventReasonGroupShifting = "CoordinationGroupShifting"

	// EventReasonGroupCompleted fires when a group returns to Idle
	// after a rollout cycle.
	EventReasonGroupCompleted = "CoordinationGroupCompleted"

	// EventReasonGroupFailed fires when a group enters Failed (Ready
	// timeout, persistent reconcile error, etc.).
	EventReasonGroupFailed = "CoordinationGroupFailed"

	// EventReasonRatioSkewRejected fires when RatioBalanced pacing
	// refused a surge to avoid drifting the cross-Component ratio.
	EventReasonRatioSkewRejected = "RatioSkewRejected"

	// EventReasonMixedPairingObserved fires when a RollingUpdate
	// group sees distinct (vN, vM) revision pairs in flight.
	// Informational — helps operators audit back-compat assertions.
	EventReasonMixedPairingObserved = "MixedPairingObserved"

	// EventReasonSequentialAwaitingNext fires when a Sequential group
	// enters the soak window between Components, waiting for the
	// operator to bump the next Component's spec.
	EventReasonSequentialAwaitingNext = "SequentialAwaitingNext"

	// EventReasonSequentialNextSpecBumpDeferred fires when an
	// operator bumps Component i+1's spec while Component i is still
	// mid-rollout. The bump is observed but not acted on yet.
	EventReasonSequentialNextSpecBumpDeferred = "SequentialNextSpecBumpDeferred"

	// EventReasonConsiderCoordinationGroup is an informational hint
	// emitted on multi-Component ISVCs without any declared group,
	// suggesting that operators consider declaring a coordination
	// group.
	EventReasonConsiderCoordinationGroup = "ConsiderCoordinationGroup"
)

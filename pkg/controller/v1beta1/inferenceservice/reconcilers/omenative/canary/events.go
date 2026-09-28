package canary

// Event reasons recorded on the InferenceService during a canary rollout, so the
// step-by-step progression is visible via `kubectl describe isvc` / events.
const (
	// EventReasonCanaryStepAdvanced is recorded when the canary advances to the
	// next step.
	EventReasonCanaryStepAdvanced = "CanaryStepAdvanced"
	// EventReasonCanaryCompleted is recorded when the canary reaches Stable.
	EventReasonCanaryCompleted = "CanaryCompleted"
	// EventReasonRolloutResumed is recorded when ome.io/rollout-resume clears a
	// terminal phase and the ladder re-enters at step 0.
	EventReasonRolloutResumed = "RolloutResumed"
	// EventReasonRolloutResumeRejected is recorded when an addressed
	// ome.io/rollout-resume cannot be honored (stale hash, or no pinned run).
	EventReasonRolloutResumeRejected = "RolloutResumeRejected"
)

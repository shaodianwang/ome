package types

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecordUpdateFailureInRetryBlock upserts the RetryBlock for targetRev
// after a terminal same-target attempt failure (failed update rollout,
// deadline-disposed create/update attempt). Wave counting: an existing
// Backoff block means this wave already recorded — refresh evidence
// only. Policy nil (unconfigured) or exhausted → Held; else Backoff
// with persisted NextRetryAt. No-op when the adapter did not wire
// MutateRetryBlock. Callers hold the writer-ordering invariant: the
// failed attempt's Operation is cleared in the same transition (block
// write first — a crash between the two re-enters the caller's failed
// branch, where the wave dedup refreshes without recounting).
//
// Lives in the leaf types package so both workload/ops (gang abandon)
// and the workload-root disposition share ONE implementation without
// closing the workload → workload/ops import cycle.
func RecordUpdateFailureInRetryBlock(ctx context.Context, input ReconcileInput, targetRev, reason string) error {
	if input.MutateRetryBlock == nil || targetRev == "" {
		return nil
	}
	now := metav1.NewTime(input.Now())
	// heldAttempts captures the Held transition inside the mutate; the
	// warning is emitted only after the write COMMITS so RMW conflict
	// retries cannot duplicate the event.
	var heldAttempts int32
	err := input.MutateRetryBlock(ctx, targetRev, func(b *RetryBlock) RetryBlockDisposition {
		heldAttempts = 0
		if b.FirstFailureAt == nil {
			b.FirstFailureAt = &now
		}
		b.LastFailureAt = &now
		b.Reason = reason
		switch b.State {
		case RetryBlockBackoff, RetryBlockHeld:
			// This wave already recorded (Backoff) or the subject is
			// terminally held — evidence refresh only.
			return RetryBlockPersist
		}
		b.AttemptsStarted++
		if input.UpdateRetryPolicy.Exhausted(b.AttemptsStarted) {
			b.State = RetryBlockHeld
			b.NextRetryAt = nil
			heldAttempts = b.AttemptsStarted
		} else {
			b.State = RetryBlockBackoff
			next := metav1.NewTime(now.Add(input.UpdateRetryPolicy.NextRetryDelay(b.AttemptsStarted)))
			b.NextRetryAt = &next
		}
		return RetryBlockPersist
	})
	if err == nil && heldAttempts > 0 && input.WarnRetryHeld != nil {
		input.WarnRetryHeld(targetRev, heldAttempts, reason)
	}
	return err
}

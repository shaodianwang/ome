package ops

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// recordUpdateFailureInRetryBlock upserts the RetryBlock for targetRev
// after a terminal same-target update failure. Wave counting: an
// existing Backoff block means this wave already recorded — refresh
// evidence only. Policy nil (unconfigured) or exhausted → Held; else
// Backoff with persisted NextRetryAt. No-op when the adapter did not
// wire MutateRetryBlock. Callers hold the writer-ordering invariant:
// the failed attempt's Operation is cleared in the same transition.
func recordUpdateFailureInRetryBlock(ctx context.Context, input workload.ReconcileInput, targetRev, reason string) error {
	if input.MutateRetryBlock == nil || targetRev == "" {
		return nil
	}
	now := metav1.NewTime(input.Now())
	// heldAttempts captures the Held transition inside the mutate; the
	// warning is emitted only after the write COMMITS so RMW conflict
	// retries cannot duplicate the event.
	var heldAttempts int32
	err := input.MutateRetryBlock(ctx, targetRev, func(b *workload.RetryBlock) workload.RetryBlockDisposition {
		heldAttempts = 0
		if b.FirstFailureAt == nil {
			b.FirstFailureAt = &now
		}
		b.LastFailureAt = &now
		b.Reason = reason
		switch b.State {
		case workload.RetryBlockBackoff, workload.RetryBlockHeld:
			// This wave already recorded (Backoff) or the subject is
			// terminally held — evidence refresh only.
			return workload.RetryBlockPersist
		}
		b.AttemptsStarted++
		if input.UpdateRetryPolicy.Exhausted(b.AttemptsStarted) {
			b.State = workload.RetryBlockHeld
			b.NextRetryAt = nil
			heldAttempts = b.AttemptsStarted
		} else {
			b.State = workload.RetryBlockBackoff
			next := metav1.NewTime(now.Add(input.UpdateRetryPolicy.NextRetryDelay(b.AttemptsStarted)))
			b.NextRetryAt = &next
		}
		return workload.RetryBlockPersist
	})
	if err == nil && heldAttempts > 0 && input.WarnRetryHeld != nil {
		input.WarnRetryHeld(targetRev, heldAttempts, reason)
	}
	return err
}

// pruneRetryBlockOnPromote removes the RetryBlock for rev once the
// subject converges there (RunningRevision promoted to rev) — a
// converged subject leaves no active block. No-op when the adapter did
// not wire MutateRetryBlock or rev is empty.
func pruneRetryBlockOnPromote(ctx context.Context, input workload.ReconcileInput, rev string) error {
	if input.MutateRetryBlock == nil || rev == "" {
		return nil
	}
	if err := input.MutateRetryBlock(ctx, rev, func(_ *workload.RetryBlock) workload.RetryBlockDisposition {
		return workload.RetryBlockRemove
	}); err != nil {
		return fmt.Errorf("prune retry block (rev=%s): %w", rev, err)
	}
	return nil
}

// instanceFailureReason summarizes the failure evidence the escalators
// already stamped on the instance (LastFailure) for RetryBlock.Reason:
// the kubelet detail message when present, else the compact termination
// fragment, else fallback.
func instanceFailureReason(s *workload.InstanceStatus, fallback string) string {
	if s == nil || s.LastFailure == nil {
		return fallback
	}
	if s.LastFailure.Message != "" {
		return s.LastFailure.Message
	}
	if short := s.LastFailure.ShortString(); short != "" {
		return short
	}
	return fallback
}

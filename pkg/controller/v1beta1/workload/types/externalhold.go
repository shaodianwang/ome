package types

import (
	"context"
)

// External holds — the single writer for InstanceOperation.Waiting.
//
// Several authorities outside the workload can stall an operation
// without anything being wrong with it: admission refusing a create for
// lack of quota, the scheduler finding no placement. Each records itself
// as a short token on Waiting, which is what parks the
// InstanceReadyTimeout clock (see the deadline-parking step) and what
// the escalation pass reads to leave the row alone.
//
// One writer, one precedence rule, so the authorities cannot steal from
// each other:
//
//   - A token is never overwritten by a different one. The authority
//     that got there first owns the wait being reported, and replacing
//     its token would tell an operator the row is queued behind
//     something it is not. The one ordered pair is the operator pause:
//     it reports a decision to stop rolling forward rather than a fact
//     about the Instance, so an authority with a fact to report takes
//     the row from it (see supersedes).
//   - A token is only released by the authority that set it. Clearing
//     unconditionally would let one authority's completion re-arm a
//     deadline another authority is still holding.
//   - Marking is edge-triggered on the held state, never on the
//     authority's message: quota names current usage, the scheduler
//     names current node counts, and both move continuously. Storing
//     either would rewrite status on every pass for as long as the wait
//     lasts. The message reaches operators once, through the event the
//     caller emits on entry.

// MarkExternalHold records token as the operation's Waiting token.
//
// It reports two things about the row the write landed on: entered, the
// transition INTO the hold (the edge a caller announces), and held,
// whether the row carries THIS token afterwards. held is the honest
// answer to "is my authority the one reporting this row's wait" —
// eligibility is judged on the fresh row, so a caller cannot derive it
// from its own pass-start observation, and treating a refused mark as a
// hold would suppress another authority's escalation.
//
// evidence, when supplied, lands on LastFailure in the same write so the
// authority's explanation survives for `kubectl describe`. It is
// compared by which pod is held, why, and since when — deliberately not
// by its message, for the reason above.
//
// writable, when supplied, is the caller's own eligibility rule. It is
// evaluated INSIDE the mutation, against the row the write will actually
// land on: a caller decides from the pass-start observation, which can
// be stale by the time the callback runs, and a row that has since
// entered teardown must not be given a hold — or have its own
// termination record overwritten — on the strength of a read that
// predates it. nil means the token rules alone decide.
//
// A no-op when the slot is gone, has no operation, is refused by
// writable, or already carries a different authority's token.
func MarkExternalHold(ctx context.Context, input ReconcileInput, idx int32, token string, evidence *InstanceTermination, writable func(*InstanceOperation) bool) (entered, held bool, err error) {
	err = input.MutateInstance(ctx, idx, func(s *InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil {
			return false
		}
		if writable != nil && !writable(s.Operation) {
			return false
		}
		current := s.Operation.Waiting
		if current != "" && current != token && !supersedes(token, current) {
			return false
		}
		held = true
		if current == token && sameHoldEvidence(s.LastFailure, evidence) {
			return false
		}
		op := *s.Operation
		op.Waiting = token
		s.Operation = &op
		entered = current != token
		if evidence != nil {
			captured := *evidence
			s.LastFailure = &captured
		}
		return true
	})
	return entered, held, err
}

// supersedes reports whether token may take the row from the incumbent
// current. Exactly one pair is ordered, and it is ordered because the
// two are not the same kind of statement: an operator pause reports a
// decision to stop rolling forward, while every other token reports
// something about the Instance — its quota was refused, it has no
// placement, it is serving nothing. The fact is what an operator needs
// to see, so it takes the row; the pause goes on holding the rollout
// either way. Everything else is first-writer-wins.
func supersedes(token, current string) bool {
	return current == WaitingReasonPaused && token != WaitingReasonPaused
}

// ClearExternalHold releases token once its authority reports the wait
// over. Another authority's token, or none at all, is left untouched.
//
// Edge-triggered off the observation as well as the write: a pass over a
// row that was never held reaches no mutation at all, so the common case
// — every reconcile of every healthy row — costs nothing.
func ClearExternalHold(ctx context.Context, input ReconcileInput, idx int32, token string) error {
	if !observedHoldingToken(input, idx, token) {
		return nil
	}
	return input.MutateInstance(ctx, idx, func(s *InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || s.Operation.Waiting != token {
			return false
		}
		op := *s.Operation
		op.Waiting = ""
		s.Operation = &op
		return true
	})
}

// observedHoldingToken reports whether this reconcile's observation of
// the row already carries token, which is the only state a release can
// act on.
func observedHoldingToken(input ReconcileInput, idx int32, token string) bool {
	for i := range input.ObservedState.InstanceStatuses {
		s := &input.ObservedState.InstanceStatuses[i]
		if s.Index != idx {
			continue
		}
		return s.Operation != nil && s.Operation.Waiting == token
	}
	return false
}

// sameHoldEvidence reports whether the recorded evidence already states
// this hold: the same pod, held for the same reason, since the same
// moment. Including the moment is what lets a same-name pod recreated
// later start a fresh record instead of inheriting the old episode's.
// A caller supplying no evidence has nothing to compare, so the token
// alone decides.
func sameHoldEvidence(recorded, fresh *InstanceTermination) bool {
	if fresh == nil {
		return true
	}
	return recorded != nil &&
		recorded.Reason == fresh.Reason &&
		recorded.PodName == fresh.PodName &&
		recorded.Time.Equal(&fresh.Time)
}

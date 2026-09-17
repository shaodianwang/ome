// gangschedule.go — the gang arm of the escalation pass. A gang is
// admitted as a unit, so its PodGroup carries facts no single member
// does: whether the deterministic name is usable by this owner at all,
// and whether the group itself has to be rebuilt.
//
// A name still held by an object being collected parks the
// InstanceReadyTimeout clock until that object is gone. A name held by
// ANOTHER controller ends the attempt — nothing this controller does
// frees it. A group the gang scheduler has failed is neither: the reset
// the PodGroup pass performs recovers it, so the row only withholds
// members while the replacement is built.
//
// Whether the gang can be PLACED is not read here — that reaches the
// Instance on its member pods (see unschedulable.go).
package workload

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// gangHoldToken is the Waiting token a PodGroup state parks the clock
// on, or "" for a state that is not a wait: nothing to report, or a
// terminal verdict the escalation ends outright.
func gangHoldToken(state GangState) string {
	if state == GangStateTerminating {
		return WaitingReasonPodGroupTerminating
	}
	return ""
}

// gangTerminalReason is the LastFailure.Reason for a PodGroup state
// nothing this controller does can resolve, or "" otherwise. Only a
// foreign owner qualifies: the name belongs to another controller and
// will not be handed over. A failed group is recoverable — the PodGroup
// pass resets it — so it never ends an attempt, or the reset and the
// rebuild would chase each other through Failed forever.
func gangTerminalReason(state GangState) string {
	if state == GangStateOwnershipConflict {
		return PodGroupOwnershipConflictReason
	}
	return ""
}

// gangHoldWritable reports whether this arm may own the operation's
// Waiting token: the row's outcome must be this machine's, and an
// operation parked on another authority's token keeps it so the row
// reports the wait it actually entered first.
func gangHoldWritable(op *InstanceOperation) bool {
	if op == nil || types.GangRowOwnedElsewhere(op) {
		return false
	}
	return op.Waiting == "" || OperationGangHeld(op)
}

// gangVerdictActionable reports whether a terminal PodGroup verdict may
// end this row. There must be an attempt in flight to end — the Failed
// stamp is a no-op without one, so announcing it would re-fire the same
// event every pass — and the row's outcome must not belong to another
// state machine.
func gangVerdictActionable(op *InstanceOperation) bool {
	return op != nil && !types.GangRowOwnedElsewhere(op)
}

// gangHoldObservedAt is the moment stamped on the hold's evidence: the
// one already recorded when this row holds the token, otherwise now.
//
// Nothing measures a window from it — the only gang wait is a name being
// collected, which resolves itself and never escalates. It is re-read so
// that re-observing an unchanged hold produces an identical record and
// the status no-op guard keeps the pass write-free.
func gangHoldObservedAt(s InstanceStatus, token string, now time.Time) metav1.Time {
	if s.Operation != nil && s.Operation.Waiting == token &&
		s.LastFailure != nil && s.LastFailure.Reason == token && !s.LastFailure.Time.IsZero() {
		return s.LastFailure.Time
	}
	return metav1.NewTime(now)
}

// gangTermination is the evidence record for a gang hold or verdict: the
// token or reason, the PodGroup's own explanation, and when it was seen.
func gangTermination(reason, message string, at metav1.Time) *InstanceTermination {
	return &InstanceTermination{Reason: reason, Message: message, Time: at}
}

// clearGangHold releases the gang token when the row is observed holding
// it, so the arm never leaves a stale wait behind once the old object is
// collected and the gang is announced again. Another authority's token
// is left untouched.
func clearGangHold(ctx context.Context, input ReconcileInput, s InstanceStatus) error {
	if s.Operation == nil || s.Operation.Waiting != WaitingReasonPodGroupTerminating {
		return nil
	}
	return ClearExternalHold(ctx, input, s.Index, WaitingReasonPodGroupTerminating)
}

// releaseGangHold drops the gang token once the name is usable again,
// so the arm never leaves a stale wait behind. A fully-serving pod set
// counts as released: the PodGroup bookkeeping may be stale, but a group
// reading cannot stop a gang already in rotation.
func releaseGangHold(ctx context.Context, input ReconcileInput, s InstanceStatus, gang GangObservation, serving bool) error {
	if gangHoldToken(gang.State) != "" && !serving {
		return nil
	}
	return clearGangHold(ctx, input, s)
}

// recordGangHold marks the gang wait and reports whether the row carries
// the gang token afterwards.
//
// It runs AFTER the pod-level hold has had its chance at the token, and
// that order is the precedence: a name being collected is transient and
// resolves itself, while a pod the scheduler cannot place is the wait an
// operator has configured a grace for. A row that is both should report
// the one that can end.
//
// Eligibility is judged on the FRESH row inside the writer. Only the
// other-owner exclusion is applied up front: a Delete or Migrate
// operation is durable, so the observation cannot be stale about it.
func recordGangHold(ctx context.Context, input ReconcileInput, s InstanceStatus, gang GangObservation, serving bool) (bool, error) {
	token := gangHoldToken(gang.State)
	if token == "" || serving || types.GangRowOwnedElsewhere(s.Operation) {
		return false, nil
	}
	evidence := gangTermination(token, gang.Message, gangHoldObservedAt(s, token, input.Now()))
	_, held, err := MarkExternalHold(ctx, input, s.Index, token, evidence, gangHoldWritable)
	return held, err
}

// gangFailureSummary is the operator-facing reason for a terminal gang
// verdict. Leading with the grep-stable token keeps LastFailure.Reason
// stable through the disposition's short-reason split.
func gangFailureSummary(reason, message string) string {
	if message == "" {
		return reason
	}
	return reason + ": " + message
}

// escalateGangFailure ends an attempt whose PodGroup name belongs to
// another controller. ENVIRONMENT-CAUSED: no corrected pod template
// frees a name someone else owns, so the revision keeps a clean retry
// ladder, and the Operation is preserved for the gang abandon path.
//
// Idempotent announcement: the stamp is a no-op on a row already Failed
// for this reason, so the warning is withheld there too. Nothing clears
// the collision on its own, and a row an operator resets would otherwise
// re-announce the same conflict on the very next pass.
func escalateGangFailure(input ReconcileInput, buf *failureStampBuffer, s InstanceStatus, gang GangObservation, reason string, now time.Time) {
	if s.Phase == InstancePhaseFailed && s.LastFailure != nil && s.LastFailure.Reason == reason {
		return
	}
	idx := s.Index
	summary := gangFailureSummary(reason, gang.Message)
	buf.add(idx, deadlineFailedMutation(gangTermination(reason, gang.Message, metav1.NewTime(now))), func() {
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(idx, "", summary)
		}
	})
}

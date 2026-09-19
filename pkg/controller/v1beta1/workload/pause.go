// pause.go — the operator pause's side of the shared external-hold
// contract (types/externalhold.go). A pause withholds the next operation
// and the next step; this is what makes the resulting wait visible on
// the attempt, so `kubectl describe` says who is holding it and the
// InstanceReadyTimeout clock does not run through the hold.
package workload

import (
	"context"
	"fmt"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// reconcilePauseHolds is the pause authority over InstanceOperation.Waiting:
// it records the Paused token on every attempt a paused Component is
// holding, and releases exactly that token once the pause is cleared.
//
// PRECEDENCE: the pause is the weakest authority on the row. Quota, the
// scheduler, the gang scheduler and a node that went silent each hold an
// attempt for a wait only they can end, and a pause is not a reason to
// stop reporting it; a surge reporting its own source out of rotation
// outranks it too — "this Instance is serving nothing" is the more urgent
// fact and may take the token over. When another authority speaks the
// pause reports nothing and still does its work, which is not planning
// the next step.
//
// Edge-triggered through MarkExternalHold / ClearExternalHold: a Component
// paused for hours writes on the way in and on the way out, not per pass.
func reconcilePauseHolds(ctx context.Context, input ReconcileInput, plan ComponentPlan) error {
	for i := range input.ObservedState.InstanceStatuses {
		s := &input.ObservedState.InstanceStatuses[i]
		if !plan.Paused {
			if err := ClearExternalHold(ctx, input, s.Index, WaitingReasonPaused); err != nil {
				return fmt.Errorf("release pause hold (instance=%d): %w", s.Index, err)
			}
			continue
		}
		if !pauseHolds(s, plan) {
			continue
		}
		if _, _, err := MarkExternalHold(ctx, input, s.Index, WaitingReasonPaused, nil, pauseHoldWritable(plan)); err != nil {
			return fmt.Errorf("record pause hold (instance=%d): %w", s.Index, err)
		}
	}
	return nil
}

// pauseHolds reports whether the observed row is one the pause is
// holding: an attempt in flight, in a phase the timeout backstop bounds,
// that the pause is actually what stands in front of — and not one whose
// surge has already reported its source out of rotation, which is the
// louder wait and keeps the row.
func pauseHolds(s *InstanceStatus, plan ComponentPlan) bool {
	return s != nil && isTransientPhase(s.Phase) &&
		!types.OperationSourceUnrouted(s.Operation) &&
		pauseHeldOperation(s.Operation, plan)
}

// pauseHeldOperation reports whether a pause is what holds this kind of
// attempt back, which is not true of every attempt a paused Component
// carries:
//
//   - Delete: scale-down keeps running while paused, so its clock must
//     keep running with it.
//   - Create: a committed create finishes the set it started; what the
//     pause holds is the operation that would come after it.
//   - Restart: repair keeps running under a standard pause. Only a frozen
//     pause holds it, and then only past the step under way.
//   - Update and Migrate: held.
func pauseHeldOperation(op *InstanceOperation, plan ComponentPlan) bool {
	if op == nil {
		return false
	}
	switch op.Type {
	case InstanceOperationDelete, InstanceOperationCreate:
		return false
	case InstanceOperationRestart:
		return plan.PauseFreeze
	}
	return true
}

// pauseHoldWritable is the pause's eligibility rule, evaluated against the
// row the write lands on rather than the pass-start observation: an
// attempt that concluded, or that a scale-down took over since the pass
// started, must not be given the hold.
func pauseHoldWritable(plan ComponentPlan) func(*InstanceOperation) bool {
	return func(op *InstanceOperation) bool {
		return pauseHeldOperation(op, plan)
	}
}

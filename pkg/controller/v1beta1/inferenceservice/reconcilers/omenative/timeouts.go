package omenative

import (
	"context"
	"fmt"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// timeoutReasonExceeded is stamped on Operation.Reason when a deadline
// elapses without a recovery transition. External observers
// (events.go later) lift it into an OperationStuck condition; for now
// it's a flag visible via kubectl.
const timeoutReasonExceeded = "DeadlineExceeded"

// timeoutReasonInPlaceFallback marks the InstanceStatus that just
// timed out of an in-place rollout and is about to be recreated. The
// next reconcile's detectUpdateTrigger picks the Instance up again;
// chooseUpdateMode will route it through the recreate branch since
// the recorded running revision no longer matches target.
const timeoutReasonInPlaceFallback = "InPlaceTimeoutFallback"

// ExpireOperationsIfDeadlinePassed walks every InstanceStatus and, for
// any whose Operation.Deadline has elapsed without a terminal
// transition, takes recovery action.
//
// Current recovery matrix (broader cases land alongside events.go):
//
//	in-place Update timeout -> clear Operation, restore Phase=Ready.
//	                           detectUpdateTrigger refires on the next
//	                           reconcile and recreate handles the
//	                           rollout. Reason=InPlaceTimeoutFallback.
//	any other timeout        -> stamp Reason=DeadlineExceeded so the
//	                           condition surface picks it up. The
//	                           controller does not force-progress
//	                           operations whose drain or surge steps
//	                           may have committed traffic side-effects.
//
// Called near the top of ReconcileComponent so timed-out ops are
// reset before the per-op detectors run.
func ExpireOperationsIfDeadlinePassed(ctx context.Context, p ReconcileParams, plan ComponentPlan) error {
	if p.Client == nil {
		return fmt.Errorf("ExpireOperationsIfDeadlinePassed: nil client")
	}
	if p.ISVC == nil {
		return nil
	}
	cs, ok := p.ISVC.Status.Components[plan.Component]
	if !ok || cs.OMENative == nil {
		return nil
	}

	now := time.Now()
	for _, s := range cs.OMENative.InstanceStatuses {
		if !operationDeadlinePassed(&s, now) {
			continue
		}
		if isInPlaceUpdate(s.Operation) {
			if err := fallbackInPlaceToRecreate(ctx, p, plan.Component, s.Index); err != nil {
				return fmt.Errorf("expire in-place update (instance=%d): %w", s.Index, err)
			}
			recordWarning(p, EventReasonInPlaceTimeoutFallback,
				"OMENative %s in-place update timed out; falling back to recreate",
				instanceKey(plan.Component, s.Index))
			continue
		}
		// Idempotency check: only emit the event when we actually
		// stamp the marker (i.e., it wasn't already there). Without
		// this guard every reconcile pass after the first timeout
		// would re-emit the event.
		alreadyStuck := s.Operation != nil && s.Operation.Reason == timeoutReasonExceeded
		if err := stampOperationStuck(ctx, p, plan.Component, s.Index); err != nil {
			return fmt.Errorf("stamp stuck (instance=%d): %w", s.Index, err)
		}
		if !alreadyStuck && s.Operation != nil {
			recordWarning(p, EventReasonOperationDeadlineExceeded,
				"OMENative %s operation %s/%s exceeded deadline; operator intervention required",
				instanceKey(plan.Component, s.Index), s.Operation.Type, s.Operation.Step)
		}
	}
	return nil
}

// operationDeadlinePassed reports whether s.Operation exists and has a
// Deadline in the past. Zero Deadline (the Go zero time) is treated as
// "never expires" so an unset field can't accidentally trip the
// timeout — see plan.go's instanceReadyTimeoutOrDefault for the
// default value supplier.
func operationDeadlinePassed(s *v1beta1.OMENativeInstanceStatus, now time.Time) bool {
	if s == nil || s.Operation == nil {
		return false
	}
	if s.Operation.Deadline.IsZero() {
		return false
	}
	return now.After(s.Operation.Deadline.Time)
}

// isInPlaceUpdate identifies a Type=Update, Step=InPlace operation —
// the only path with an automatic recovery (fall back to recreate
// because the spec promises a safe degradation).
func isInPlaceUpdate(op *v1beta1.InstanceOperation) bool {
	if op == nil {
		return false
	}
	return op.Type == v1beta1.InstanceOperationUpdate && op.Step == updateStepInPlace
}

// fallbackInPlaceToRecreate clears the in-flight Operation and sets
// Phase=Ready so the next reconcile's detectUpdateTrigger picks the
// Instance up again. With RunningRevision still lagging UpdateRevision,
// chooseUpdateMode then routes through recreate (eligibility check
// compares running CR vs target — they no longer match because the
// failed image patch never landed cleanly, or matches but the user's
// preference now is to bail to recreate).
//
// Reason=InPlaceTimeoutFallback is left as a breadcrumb on the next
// Operation that recreate stamps.
func fallbackInPlaceToRecreate(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.Operation = nil
		// TargetRevision is preserved on the InstanceStatus so the next
		// reconcile can still see "this Instance is heading toward X"
		// while detectUpdateTrigger refires. The recreate path will
		// overwrite Operation with Step=Drain on its next pass.
		// Tests use the InstanceStatus.Conditions surface (added with
		// events.go) to inspect the fallback breadcrumb; for now it's
		// recorded only via the absence of an Operation on a status
		// that still carries a TargetRevision != RunningRevision.
		_ = timeoutReasonInPlaceFallback
		return true
	})
}

// stampOperationStuck writes Operation.Reason=DeadlineExceeded without
// clearing the Operation itself. Drain/surge/restart steps may have
// committed traffic-routing or pod-delete side effects; force-progress
// would risk worse states than the stuck one. The condition surface
// (events.go) lifts the marker into an OperationStuck=True condition
// so operators can intervene.
func stampOperationStuck(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil {
			return false
		}
		if s.Operation.Reason == timeoutReasonExceeded {
			return false
		}
		s.Operation.Reason = timeoutReasonExceeded
		return true
	})
}

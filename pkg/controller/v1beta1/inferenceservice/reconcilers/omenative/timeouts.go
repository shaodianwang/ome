package omenative

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// timeoutReasonExceeded is stamped on Operation.Reason when a deadline
// elapses without a recovery transition. External observers
// (events.go later) lift it into an OperationStuck condition; for now
// it's a flag visible via kubectl.
const timeoutReasonExceeded = "DeadlineExceeded"

// The InPlaceTimeoutFallback breadcrumb the original PR #193 deferred
// to events.go lives in conditions.go as ReasonInPlaceTimeoutFallback
// now (lifted into the InPlaceUpdateFallback Condition by PR C4); no
// local const needed here.

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
		// MIGRATE-1: Migrate timeouts get a live surge re-check
		// rather than the blanket DeadlineExceeded stamp. If surge
		// pods are all runtime-ready the stamp stays (issue is
		// downstream of surge — drain step or kube-proxy lag);
		// operator investigates. If surge isn't ready, abandon
		// surge ("preserve old, abandon surge" per OEP §B.4) so the
		// Component doesn't sit forever in a half-finished state.
		if isMigrate(s.Operation) {
			abandoned, err := tryAbandonStuckMigration(ctx, p, plan.Component, s)
			if err != nil {
				return fmt.Errorf("expire migration (instance=%d): %w", s.Index, err)
			}
			if abandoned {
				continue
			}
			// Surge IS ready — fall through to the generic stamp.
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

// isMigrate identifies a Type=Migrate operation on the source side
// of a surge pair. Only source-side stamps carry a SurgeIndex
// pointer; the surge-side stamp has SurgeIndex pointing back at the
// source but its own Type=Migrate too. We treat both as
// migration ops for timeout-handling purposes — the abandon helper
// figures out which side it's looking at by inspecting SurgeIndex.
func isMigrate(op *v1beta1.InstanceOperation) bool {
	return op != nil && op.Type == v1beta1.InstanceOperationMigrate
}

// tryAbandonStuckMigration is the MIGRATE-1 surge re-check. Returns
// abandoned=true when the surge was found not-ready and the
// migration was rolled back. abandoned=false means surge is healthy
// and the caller should fall through to the generic stuck stamp
// (operator must investigate the downstream-of-surge wedge).
//
// The abandon path:
//
//  1. Issue best-effort Deletes on every surge pod (NotFound is OK
//     for re-runs).
//  2. RemoveNotReadyKey on source pods so the drain key set during
//     step 8 of Migrate is reversed and source pods return to
//     serving rotation.
//  3. Restore source InstanceStatus: clear Operation, Phase=Ready.
//  4. Remove the surge InstanceStatus.
//  5. Write Failed audit row + clear request annotation.
//  6. Emit MigrationSurgeAbandoned event.
//
// Re-entrant: every step uses idempotent helpers so a crash mid-
// rollback completes on the next reconcile.
func tryAbandonStuckMigration(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, source v1beta1.OMENativeInstanceStatus) (bool, error) {
	if source.Operation == nil || source.Operation.SurgeIndex == nil {
		// Source-side stamp without SurgeIndex is malformed (or
		// this is the surge side; abandon doesn't run from surge
		// side because the source side's stamp is the one that
		// carries the migration intent and times out first).
		return false, nil
	}
	surgeIdx := *source.Operation.SurgeIndex
	uuid := source.Operation.RequestUUID

	surgePods, err := liveListPodsForInstance(ctx, p, component, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge pods: %w", err)
	}
	if surgeReady(surgePods) {
		// Surge IS ready — leave the stamp for the operator.
		return false, nil
	}

	// Surge is NOT ready — abandon. Best-effort delete on remaining
	// surge pods so the next reconcile sees a clean state.
	for _, pod := range surgePods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if err := p.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete surge pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}

	// Undo the source-drain key (set during Migrate step 8). If the
	// drain key was never added (timeout fired before step 8) the
	// remove is a no-op via podreadiness's idempotency.
	sourcePods, err := liveListPodsForInstance(ctx, p, component, source.Index)
	if err != nil {
		return false, fmt.Errorf("list source pods: %w", err)
	}
	for _, pod := range sourcePods {
		if err := MarkPodServing(ctx, p.Client, pod, WriterMigrateSourceDrain, uuid); err != nil {
			return false, fmt.Errorf("undo source drain on pod %s: %w", pod.Name, err)
		}
	}

	// Restore source to Ready, clear Operation.
	if err := patchInstanceStatusReady(ctx, p, component, source.Index); err != nil {
		return false, fmt.Errorf("restore source Ready: %w", err)
	}
	// Remove surge InstanceStatus.
	if _, err := removeInstanceStatus(ctx, p, component, surgeIdx); err != nil {
		return false, fmt.Errorf("remove surge InstanceStatus: %w", err)
	}

	// Mark Failed in the audit ledger so a re-delivered request
	// with the same UUID short-circuits.
	ledger, err := loadAuditLedger(ctx, p.reader(), p.ISVC)
	if err != nil {
		return false, fmt.Errorf("load ledger for abandon: %w", err)
	}
	now := metav1.Now().UTC().Format(time.RFC3339)
	ledger.upsertEntry(AuditEntry{
		RequestUUID: uuid,
		Phase:       auditPhaseFailed,
		Component:   string(component),
		FromNode:    source.Operation.FromNode,
		StartedAt:   now,
		CompletedAt: now,
		Outcome:     "surge abandoned after deadline (surge not Ready)",
	})
	if err := persistAuditLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
		return false, fmt.Errorf("persist Failed ledger after abandon: %w", err)
	}
	if err := clearMigrationRequestAnnotation(ctx, p, uuid); err != nil {
		return false, fmt.Errorf("clear annotation after abandon: %w", err)
	}

	recordWarning(p, EventReasonMigrationSurgeAbandoned,
		"OMENative migration uuid=%s abandoned: surge instance=%d never reached Ready before deadline; source preserved",
		uuid, surgeIdx)
	return true, nil
}

// surgeReady reports whether every surge pod is runtime-ready
// (ContainersReady=True). An empty slice returns false because we
// can't claim surge is healthy without observing any pods.
func surgeReady(pods []*corev1.Pod) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if !IsContainersReady(pod) {
			return false
		}
	}
	return true
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
	now := metav1.Now()
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.Operation = nil
		// TargetRevision is preserved on the InstanceStatus so the
		// next reconcile can still see "this Instance is heading
		// toward X" while detectUpdateTrigger refires. The recreate
		// path will overwrite Operation with Step=Drain on its next
		// pass.
		//
		// Lift the in-place timeout breadcrumb (originally deferred
		// from PR #193) into the InstanceStatus.Conditions surface so
		// operators see why this Instance suddenly looks Ready but is
		// about to recreate.
		apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{
			Type:               ConditionInPlaceUpdateFallback,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonInPlaceTimeoutFallback,
			Message:            "In-place update exceeded its deadline; the next reconcile will route through recreate",
			LastTransitionTime: now,
		})
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

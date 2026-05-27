package ops

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
)

// ExpireOperationsIfDeadlinePassed walks every InstanceStatus and takes
// recovery action on any whose Operation.Deadline has elapsed:
//
//	in-place Update -> clear Operation, restore Phase=Ready (next reconcile's
//	                   detectUpdateTrigger refires and recreate takes over).
//	Migrate         -> live surge re-check; abandon surge if not ready,
//	                   else stamp DeadlineExceeded.
//	any other       -> stamp Reason=DeadlineExceeded. The controller refuses
//	                   to force-progress ops whose drain/surge steps have
//	                   committed traffic side effects.
//
// Called near the top of ReconcileComponent so timed-out ops reset before
// the per-op detectors run.
func ExpireOperationsIfDeadlinePassed(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan) error {
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
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonInPlaceTimeoutFallback,
				"OMENative %s in-place update timed out; falling back to recreate",
				status.InstanceKey(plan.Component, s.Index))
			continue
		}
		// Migrate timeouts get a live surge re-check rather than the
		// blanket stamp: if surge is runtime-ready the wedge is
		// downstream (drain / kube-proxy lag) and operator must
		// investigate; if not, abandon surge so the Component doesn't
		// sit forever in a half-finished state.
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
		alreadyStuck := s.Operation != nil && s.Operation.Reason == status.ReasonDeadlineExceeded
		if err := stampOperationStuck(ctx, p, plan.Component, s.Index); err != nil {
			return fmt.Errorf("stamp stuck (instance=%d): %w", s.Index, err)
		}
		if !alreadyStuck && s.Operation != nil {
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonOperationDeadlineExceeded,
				"OMENative %s operation %s/%s exceeded deadline; operator intervention required",
				status.InstanceKey(plan.Component, s.Index), s.Operation.Type, s.Operation.Step)
		}
		// Phase escalation: stamping OperationStuck leaves the Instance
		// in its transient Phase (Creating / Updating / Restarting /
		// Migrating). That's the right signal at the Component-level
		// OperationStuck condition, but the coordination state machine
		// reads `c.Failed` to decide whether to fail the whole group —
		// without a per-Instance terminal marker the group sits at
		// Surging / Waiting forever even though the operator can no
		// longer make progress. Escalate the Phase to Failed so
		// buildComponentObservation can lift the signal into
		// Component.Failed.
		alreadyFailed := s.Phase == v1beta1.OMENativeInstanceFailed
		if !alreadyFailed && isEscalatablePhase(s.Phase) {
			if err := stampInstanceFailed(ctx, p, plan.Component, s.Index); err != nil {
				return fmt.Errorf("stamp failed (instance=%d): %w", s.Index, err)
			}
			// Mirror the just-committed Phase onto the in-memory ISVC
			// the rest of this reconcile pass reads. dispatch.go's
			// Update / Restart passes read `p.ISVC.Status` (the
			// snapshot at reconcile start), so without this mirror
			// detectUpdateTrigger keeps seeing Phase=Updating, fires
			// recreate, and recreate's patchInstanceStatusRecreatingFor
			// Update clobbers the Phase=Failed back to Updating in the
			// apiserver — the escalation never sticks and the
			// InstanceFailed event fires on every reconcile until the
			// test times out.
			mirrorInstancePhase(p.ISVC, plan.Component, s.Index, v1beta1.OMENativeInstanceFailed)
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonInstanceFailed,
				"OMENative %s escalated to Phase=Failed after deadline; coordination layer will fail the group",
				status.InstanceKey(plan.Component, s.Index))
		}
	}
	return nil
}

// mirrorInstancePhase updates the in-memory InstanceStatus.Phase on the
// reconcile-start ISVC snapshot so downstream passes in the SAME
// reconcile (Restart / Update / Create) read the just-escalated value
// instead of the stale pre-escalation phase. Without this, the
// dispatcher's detector + patch sequence races itself: Update reads
// Phase=Updating from the snapshot, fires recreate, and recreate
// rewrites Phase=Updating in the apiserver — undoing the escalation
// the same reconcile just committed.
func mirrorInstancePhase(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, idx int32, phase v1beta1.OMENativeInstancePhase) {
	if isvc == nil {
		return
	}
	cs, ok := isvc.Status.Components[c]
	if !ok || cs.OMENative == nil {
		return
	}
	for i := range cs.OMENative.InstanceStatuses {
		if cs.OMENative.InstanceStatuses[i].Index == idx {
			cs.OMENative.InstanceStatuses[i].Phase = phase
			return
		}
	}
}

// isEscalatablePhase reports whether the Instance is currently in a
// transient phase that ExpireOperationsIfDeadlinePassed may escalate
// to Failed. The terminal phases (Ready / Failed / Deleting) are
// already settled, so escalation isn't meaningful.
func isEscalatablePhase(p v1beta1.OMENativeInstancePhase) bool {
	switch p {
	case v1beta1.OMENativeInstanceCreating,
		v1beta1.OMENativeInstanceUpdating,
		v1beta1.OMENativeInstanceRestarting,
		v1beta1.OMENativeInstanceMigrating:
		return true
	}
	return false
}

// isMigrate identifies any Type=Migrate operation. Both source and surge
// stamps use the same Type; tryAbandonStuckMigration disambiguates by
// inspecting SurgeIndex.
func isMigrate(op *v1beta1.InstanceOperation) bool {
	return op != nil && op.Type == v1beta1.InstanceOperationMigrate
}

// tryAbandonStuckMigration runs the surge re-check on a Migrate timeout.
// Returns abandoned=true when the surge was found not-ready and the
// migration was rolled back. abandoned=false means surge is healthy and
// the caller falls through to the generic stuck stamp.
//
// Rollback steps (all idempotent so a mid-crash resumes):
//  1. Delete surge pods.
//  2. RemoveNotReadyKey on source pods (reverses the source-drain key).
//  3. Restore source InstanceStatus to Phase=Ready, Operation=nil.
//  4. Remove surge InstanceStatus.
//  5. Write Failed audit row + clear the request annotation.
//  6. Emit MigrationSurgeAbandoned event.
func tryAbandonStuckMigration(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, source v1beta1.OMENativeInstanceStatus) (bool, error) {
	if source.Operation == nil || source.Operation.SurgeIndex == nil {
		// Source-side stamp without SurgeIndex is malformed (or
		// this is the surge side; abandon doesn't run from surge
		// side because the source side's stamp is the one that
		// carries the migration intent and times out first).
		return false, nil
	}
	surgeIdx := *source.Operation.SurgeIndex
	uuid := source.Operation.RequestUUID

	surgePods, err := query.LiveListPodsForInstance(ctx, p, component, surgeIdx)
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
	sourcePods, err := query.LiveListPodsForInstance(ctx, p, component, source.Index)
	if err != nil {
		return false, fmt.Errorf("list source pods: %w", err)
	}
	for _, pod := range sourcePods {
		if err := podreadiness.MarkPodServing(ctx, p.Client, pod, podreadiness.WriterMigrateSourceDrain, uuid); err != nil {
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
	ledger, err := audit.LoadLedger(ctx, p.Reader(), p.ISVC)
	if err != nil {
		return false, fmt.Errorf("load ledger for abandon: %w", err)
	}
	now := metav1.Now().UTC().Format(time.RFC3339)
	ledger.UpsertEntry(audit.Entry{
		RequestUUID: uuid,
		Phase:       audit.PhaseFailed,
		Component:   string(component),
		FromNode:    source.Operation.FromNode,
		StartedAt:   now,
		CompletedAt: now,
		Outcome:     "surge abandoned after deadline (surge not Ready)",
	})
	if err := audit.PersistLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
		return false, fmt.Errorf("persist Failed ledger after abandon: %w", err)
	}
	if err := clearMigrationRequestAnnotation(ctx, p, uuid); err != nil {
		return false, fmt.Errorf("clear annotation after abandon: %w", err)
	}

	status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonMigrationSurgeAbandoned,
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
		if !podreadiness.IsContainersReady(pod) {
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
func fallbackInPlaceToRecreate(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	now := metav1.Now()
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.Operation = nil
		// TargetRevision stays so the next reconcile still knows where
		// this Instance is heading; recreate's first pass overwrites
		// Operation with Step=Drain. The InPlaceUpdateFallback condition
		// surfaces WHY the Instance suddenly looks Ready but is about to
		// recreate.
		apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{
			Type:               status.ConditionInPlaceUpdateFallback,
			Status:             metav1.ConditionTrue,
			Reason:             status.ReasonInPlaceTimeoutFallback,
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
func stampOperationStuck(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil {
			return false
		}
		if s.Operation.Reason == status.ReasonDeadlineExceeded {
			return false
		}
		s.Operation.Reason = status.ReasonDeadlineExceeded
		return true
	})
}

// stampInstanceFailed escalates the Instance's Phase to Failed while
// preserving the Operation breadcrumb. Idempotent: a second call when
// the Phase is already Failed is a no-op (no status churn). The
// coordination layer's buildComponentObservation reads Phase==Failed
// to mark the Component as Failed, which fails the whole group on
// BlueGreen / RollingUpdate / Sequential.
//
// Per-Instance Phase=Failed is the operator-facing "we've given up
// trying to converge this Instance" signal. The Operation is kept so
// the OperationStuck condition + the per-Instance trace still tell
// operators what was in flight when the deadline elapsed.
//
// Guards against re-creating an InstanceStatus that was deliberately
// removed earlier in the same ExpireOperationsIfDeadlinePassed pass:
// MutateInstance re-fetches and appends an empty slot when the
// (component, idx) entry is gone (e.g., the migrate-abandon path's
// surge cleanup), so a missing Operation here is the unambiguous
// signal that the upstream rollback won and we must NOT resurrect
// the entry. Matches stampOperationStuck's `if s.Operation == nil`
// guard for the same reason.
func stampInstanceFailed(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil {
			return false
		}
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceFailed
		return true
	})
}

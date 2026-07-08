package ops

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// updateStep distinguishes which mode wrote the current Operation. Without
// it, a strategy change mid-rollout (InPlaceIfPossible → RecreatePod)
// would let recreate's idempotency check match the in-place state, skip
// the Incarnation bump, classify every pod as fresh, and never recreate.
const (
	updateStepInPlace = "InPlace"
	updateStepDrain   = "Drain"
	// updateStepSurge is the SurgeThenDrain entry step: surge pod
	// being created at the other ordinal slot. Transitions to
	// updateStepSurgeDrain once the surge pod is Ready and the old
	// pod's serving gate is being flipped.
	updateStepSurge = "Surge"
	// updateStepSurgeDrain is the SurgeThenDrain drain phase. Distinct
	// from updateStepDrain so the dispatcher's CheckSurgeGate can
	// recognize in-flight surge operations by step name alone (the
	// surge contributes an extra pod alive — the unavailability gate
	// doesn't apply; the surge gate does).
	updateStepSurgeDrain = "SurgeDrain"
)

// UpdateStepInPlace is the exported alias for the in-place step name.
// Used by the omenative timeouts handler to identify in-flight in-place
// updates (Type=Update, Step=InPlace) that need to fall back to
// recreate when their deadline elapses.
const UpdateStepInPlace = updateStepInPlace

// UpdateStepDrain is the exported alias for the recreate-drain step name.
const UpdateStepDrain = updateStepDrain

// UpdateStepSurge is the exported alias for the surge entry step name.
const UpdateStepSurge = updateStepSurge

// UpdateStepSurgeDrain is the exported alias for the surge-drain step name.
const UpdateStepSurgeDrain = updateStepSurgeDrain

// patchInstanceStatusUpdating idempotently stamps Phase=Updating with the
// Update operation in Step=InPlace at targetRev. Skip-write requires
// Step==InPlace so a previous recreate-step write doesn't short-circuit.
func patchInstanceStatusUpdating(ctx context.Context, input workload.ReconcileInput, idx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseUpdating &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
			s.Operation.Step == updateStepInPlace &&
			s.TargetRevision == targetRev {
			return false
		}
		s.Phase = workload.InstancePhaseUpdating
		s.TargetRevision = targetRev
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepInPlace,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusRecreatingForUpdate is the recreate entry point.
// Bumps Incarnation (like Restart) and stamps Phase=Updating with
// Step=Drain. reason is the "revision <from> → <to>" cause string recorded
// on Operation.Reason so a revision-roll recreate is distinguishable in
// status from a pod-failure Restart. Returns the post-write Incarnation.
// The idempotency guard requires Step==Drain so a prior in-place pass at
// the same target doesn't block the bump.
func patchInstanceStatusRecreatingForUpdate(ctx context.Context, input workload.ReconcileInput, idx int32, targetRev, reason string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseUpdating &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
			s.Operation.Step == updateStepDrain &&
			s.TargetRevision == targetRev && s.Incarnation > 0 {
			observedIncarnation = s.Incarnation
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = workload.InstancePhaseUpdating
		s.TargetRevision = targetRev
		now := metav1.NewTime(time.Now())
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepDrain,
			TargetRevision: targetRev,
			Reason:         reason,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
	return observedIncarnation, err
}

// (patchInstanceStatusReadyOnRevision lives in create.go — the in-place,
// recreate, and surge promote paths share it. surge variant that also
// advances ActiveOrdinal is patchInstanceStatusReadyOnRevisionWithOrdinal
// below.)

// patchInstanceStatusSurgingForUpdate is the SurgeThenDrain entry
// point. Stamps Phase=Updating + Op{Step=Surge, TargetRevision} without
// bumping Incarnation (different pod NAME — ordinal slot — keeps old
// and new distinct without reusing identity). ActiveOrdinal is not yet
// advanced; it stays on the old slot until promote completes. The
// idempotency guard requires Step in {Surge, SurgeDrain} so the
// helper does not regress an in-flight SurgeDrain back to Surge when
// surgeUpdate re-runs the entry stamp on a subsequent pass; it does
// require Type==Update so a prior recreate / in-place write at the
// same target doesn't short-circuit (those writes use the same
// TargetRevision but a different Step namespace).
func patchInstanceStatusSurgingForUpdate(ctx context.Context, input workload.ReconcileInput, idx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		// Already surging toward this target — no-op. The Phase check
		// admits BOTH an in-flight Updating instance AND one the stuck-pod
		// escalator has since flipped to Failed. Resurrecting a Failed
		// instance back to Updating (with a fresh Operation + deadline)
		// re-arms the escalator into a Failed<->Updating write storm when
		// the target revision itself is bad and there is no corrective edit
		// to drive to — the surge pod never goes Ready, so every pass
		// re-stamps Updating and the escalator re-stamps Failed. A real
		// corrective edit changes TargetRevision, so the guard falls
		// through and the re-surge proceeds (the #711 recovery path).
		if s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
			(s.Operation.Step == updateStepSurge || s.Operation.Step == updateStepSurgeDrain) &&
			s.TargetRevision == targetRev &&
			(s.Phase == workload.InstancePhaseUpdating || s.Phase == workload.InstancePhaseFailed) {
			return false
		}
		s.Phase = workload.InstancePhaseUpdating
		s.TargetRevision = targetRev
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusSurgeStepDrain transitions the surge operation
// from Step=Surge to Step=SurgeDrain once the surge pod is Ready and
// we're about to flip serving gates. Distinct from updateStepDrain
// (which recreate uses) so the dispatcher's CheckSurgeGate can
// identify in-flight surges by step name. Updates LastProgressAt for
// the stuck-operation timeout machinery. Idempotent — no-op if
// already on SurgeDrain.
func patchInstanceStatusSurgeStepDrain(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Operation == nil ||
			s.Operation.Type != workload.InstanceOperationUpdate {
			return false
		}
		if s.Operation.Step == updateStepSurgeDrain {
			return false
		}
		s.Operation.Step = updateStepSurgeDrain
		s.Operation.LastProgressAt = now
		return true
	})
}

// patchInstanceStatusReadyOnRevisionWithOrdinal is the surge-promote
// terminator: clears Operation, advances ActiveOrdinal to the new slot,
// stamps RunningRevision=rev and Phase=Ready. Distinct from
// patchInstanceStatusReadyOnRevision because surge needs to ALSO bump
// ActiveOrdinal — the old pod is gone, the surge pod is the new
// canonical pod, and future restart / scale-up paths must address that
// slot.
func patchInstanceStatusReadyOnRevisionWithOrdinal(ctx context.Context, input workload.ReconcileInput, idx int32, rev string, newOrdinal int32) error {
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" &&
			s.ActiveOrdinal == newOrdinal {
			return false
		}
		s.Phase = workload.InstancePhaseReady
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		s.ActiveOrdinal = newOrdinal
		return true
	})
}

// patchInstanceStatusGangSurging stamps the SOURCE Instance of a gang
// surge: Phase=Updating, Op{Update, Step=Surge, SurgeIndex=k}. Step=Surge
// makes CurrentSurgeInFlight / the coordination CheckSurge gate count it
// against MaxSurge; the SurgeIndex pointer distinguishes a gang surge
// (new index) from a single-pod surge (ActiveOrdinal toggle). Idempotent.
func patchInstanceStatusGangSurging(ctx context.Context, input workload.ReconcileInput, idx, surgeIdx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseUpdating && s.Operation != nil &&
			s.Operation.Type == workload.InstanceOperationUpdate &&
			s.Operation.Step == updateStepSurge && s.Operation.SurgeIndex != nil &&
			*s.Operation.SurgeIndex == surgeIdx && s.TargetRevision == targetRev {
			return false
		}
		k := surgeIdx
		s.Phase = workload.InstancePhaseUpdating
		s.TargetRevision = targetRev
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("gangsurge-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			SurgeIndex:     &k,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusGangSurgeTarget stamps the TARGET (surge) index of a
// gang surge: Phase=Creating, Op{Update, Step=GangSurgeTarget}. The
// marker pins the index in instancePlanIndices so EnsurePodGroups creates
// its PodGroup and scale-down won't delete it. Creates the InstanceStatus
// if absent. Idempotent.
func patchInstanceStatusGangSurgeTarget(ctx context.Context, input workload.ReconcileInput, surgeIdx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, surgeIdx, func(s *workload.InstanceStatus) bool {
		if s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
			s.Operation.Step == workload.UpdateStepGangSurgeTarget {
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Phase = workload.InstancePhaseCreating
		s.TargetRevision = targetRev
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("gangsurgetarget-%d-%d", surgeIdx, now.Unix()),
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

package status

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// The gang-surge stamps. A gang surge is the one update whose two rows
// are written together: the SOURCE carries the operation and its
// deadline, the TARGET index carries the marker that pins the surge slot
// in the plan. They commit in one batch so no pass can observe a claim
// with no marker behind it.

// stampGangSurging stamps the SOURCE row of a gang surge:
// Phase=Updating, Op{Update, Step=Surge, SurgeIndex=k}. Step=Surge is
// what makes surge accounting and the coordination surge gate count the
// operation against MaxSurge; the SurgeIndex pointer distinguishes a
// gang surge (a new index) from a single-pod surge (an ordinal toggle).
// Idempotent.
func stampGangSurging(ctx context.Context, input types.ReconcileInput, idx, surgeIdx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		return stampGangSurgeSource(s, idx, surgeIdx, targetRev, strategy, timeout, now)
	})
	if err != nil {
		return err
	}
	return RetryBlockAttemptStarted(ctx, input, targetRev)
}

// StampGangSurgeTarget stamps the TARGET index of a gang surge:
// Phase=Creating, Op{Update, Step=GangSurgeTarget}. The same attempt
// stampGangSurging already flipped the RetryBlock for, so there is no
// flip here — once per attempt, on the source side. The marker pins the
// index in the plan so the gang pass creates its PodGroup and scale-down
// will not delete it. Creates the row if absent. Idempotent.
func StampGangSurgeTarget(ctx context.Context, input types.ReconcileInput, surgeIdx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	return input.MutateInstance(ctx, surgeIdx, func(s *types.InstanceStatus) bool {
		return stampGangSurgeTarget(s, surgeIdx, targetRev, timeout, now)
	})
}

func stampGangSurgeSource(s *types.InstanceStatus, idx, surgeIdx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration, now metav1.Time) bool {
	if s.Phase == types.InstancePhaseUpdating && s.Operation != nil &&
		s.Operation.Type == types.InstanceOperationUpdate &&
		s.Operation.Step == types.UpdateStepSurge && s.Operation.SurgeIndex != nil &&
		*s.Operation.SurgeIndex == surgeIdx && s.TargetRevision == targetRev {
		return false
	}
	k := surgeIdx
	s.Phase = types.InstancePhaseUpdating
	s.TargetRevision = targetRev
	s.Operation = &types.InstanceOperation{
		ID:             fmt.Sprintf("gangsurge-%d-%d", idx, now.Unix()),
		Type:           types.InstanceOperationUpdate,
		Step:           types.UpdateStepSurge,
		SurgeIndex:     &k,
		TargetRevision: targetRev,
		Strategy:       strategy,
		StartedAt:      now,
		LastProgressAt: now,
		Deadline:       types.DeadlineAt(now, timeout),
	}
	return true
}

func stampGangSurgeTarget(s *types.InstanceStatus, surgeIdx int32, targetRev string, timeout time.Duration, now metav1.Time) bool {
	if GangSurgeTargetClaimMatches(s, targetRev) || !EmptyGangSurgeTargetSlot(s) {
		return false
	}
	s.Incarnation = 1
	s.Phase = types.InstancePhaseCreating
	s.TargetRevision = targetRev
	s.Operation = &types.InstanceOperation{
		ID:             fmt.Sprintf("gangsurgetarget-%d-%d", surgeIdx, now.Unix()),
		Type:           types.InstanceOperationUpdate,
		Step:           types.UpdateStepGangSurgeTarget,
		TargetRevision: targetRev,
		StartedAt:      now,
		LastProgressAt: now,
		Deadline:       types.DeadlineAt(now, timeout),
	}
	return true
}

// StartGangSurge commits the source claim and the target marker in one
// batch, fenced on the source identity the pass decided from. Reports
// whether this pass is the one that opened the surge.
func StartGangSurge(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surgeIdx int32,
	targetRev string,
	strategy types.UpdateStrategyType,
	timeout time.Duration,
) (bool, error) {
	if source == nil {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if err := stampGangSurging(ctx, input, source.Index, surgeIdx, targetRev, strategy, timeout); err != nil {
			return false, err
		}
		if err := StampGangSurgeTarget(ctx, input, surgeIdx, targetRev, timeout); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := RequireOwner(input); err != nil {
		return false, err
	}

	now := metav1.NewTime(input.Now())
	sourceIdentity := Capture(source)
	desiredSource := Clone(*source)
	stampGangSurgeSource(&desiredSource, source.Index, surgeIdx, targetRev, strategy, timeout, now)
	desiredSourceIdentity := Capture(&desiredSource)
	desiredTarget := types.InstanceStatus{Index: surgeIdx}
	stampGangSurgeTarget(&desiredTarget, surgeIdx, targetRev, timeout, now)
	desiredTargetIdentity := Capture(&desiredTarget)
	ownerUID := input.OwnerObject.GetUID()
	committed := false
	sourceMutation := types.InstanceMutation{
		Index: source.Index,
		Mutate: func(status *types.InstanceStatus) bool {
			return stampGangSurgeSource(status, source.Index, surgeIdx, targetRev, strategy, timeout, now)
		},
		BatchPrecondition: func(snapshot types.InstanceMutationSnapshot) bool {
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, found := snapshot.Instances[source.Index]
			if !found || !sourceIdentity.Matches(currentSource) {
				return false
			}
			currentTarget, found := snapshot.Instances[surgeIdx]
			return !found || EmptyGangSurgeTargetSlot(&currentTarget)
		},
		Postcondition: func(status *types.InstanceStatus) bool {
			return status != nil && desiredSourceIdentity.Matches(*status)
		},
		OnCommit: func(_, _ *types.InstanceStatus) { committed = true },
	}
	targetMutation := types.InstanceMutation{
		Index: surgeIdx,
		Mutate: func(status *types.InstanceStatus) bool {
			return stampGangSurgeTarget(status, surgeIdx, targetRev, timeout, now)
		},
		Postcondition: func(status *types.InstanceStatus) bool {
			return status != nil && desiredTargetIdentity.Matches(*status)
		},
	}
	err := input.ApplyInstanceMutationsWithRetryBlock(ctx,
		[]types.InstanceMutation{sourceMutation, targetMutation}, targetRev, RetryBlockStartAttempt)
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return committed, nil
}

// GangSurgeTargetMarker reports which marker state a target slot was
// found in.
type GangSurgeTargetMarker uint8

const (
	// GangSurgeTargetMarkerUnclaimed: no marker, and none written.
	GangSurgeTargetMarkerUnclaimed GangSurgeTargetMarker = iota
	// GangSurgeTargetMarkerActive: the live marker is already there.
	GangSurgeTargetMarkerActive
	// GangSurgeTargetMarkerRestored: this pass wrote the marker, so a
	// fresh reconcile must observe it before any external effect.
	GangSurgeTargetMarkerRestored
	// GangSurgeTargetMarkerCleanup: the marker is there, in its cleanup
	// step — the slot is being collected, not claimed.
	GangSurgeTargetMarkerCleanup
)

// RestoreGangSurgeTarget closes the resumable gap between the source's
// committed surge claim and its target marker. The result identifies the
// authoritative marker state. The strong path binds target creation to
// the exact source operation on every conflict retry.
func RestoreGangSurgeTarget(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surgeIdx int32,
	targetRev string,
	timeout time.Duration,
) (GangSurgeTargetMarker, error) {
	if source == nil || source.Operation == nil {
		return GangSurgeTargetMarkerUnclaimed, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if input.FinalizeInstanceResources != nil {
			return GangSurgeTargetMarkerUnclaimed, fmt.Errorf("gang surge target recovery requires the owner-aware atomic status adapter")
		}
		if err := StampGangSurgeTarget(ctx, input, surgeIdx, targetRev, timeout); err != nil {
			return GangSurgeTargetMarkerUnclaimed, err
		}
		return GangSurgeTargetMarkerRestored, nil
	}
	if err := RequireOwner(input); err != nil {
		return GangSurgeTargetMarkerUnclaimed, err
	}

	now := metav1.NewTime(input.Now())
	desired := types.InstanceStatus{
		Index:          surgeIdx,
		Incarnation:    1,
		Phase:          types.InstancePhaseCreating,
		TargetRevision: targetRev,
		Operation: &types.InstanceOperation{
			ID:             fmt.Sprintf("gangsurgetarget-%d-%d", surgeIdx, now.Unix()),
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepGangSurgeTarget,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
		},
	}
	sourceIdentity := Capture(source)
	ownerUID := input.OwnerObject.GetUID()
	resolution := GangSurgeTargetMarkerUnclaimed
	mutation := types.InstanceMutation{
		Index: surgeIdx,
		Mutate: func(status *types.InstanceStatus) bool {
			if GangSurgeActiveTargetClaimMatches(status, targetRev) {
				resolution = GangSurgeTargetMarkerActive
				return false
			}
			if GangSurgeCleanupTargetClaimMatches(status, targetRev) {
				resolution = GangSurgeTargetMarkerCleanup
				return false
			}
			if !EmptyGangSurgeTargetSlot(status) {
				return false
			}
			*status = Clone(desired)
			return true
		},
		BatchPrecondition: func(snapshot types.InstanceMutationSnapshot) bool {
			resolution = GangSurgeTargetMarkerUnclaimed
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, found := snapshot.Instances[source.Index]
			if !found || !sourceIdentity.Matches(currentSource) {
				return false
			}
			currentTarget, found := snapshot.Instances[surgeIdx]
			if !found {
				return true
			}
			if GangSurgeActiveTargetClaimMatches(&currentTarget, targetRev) {
				resolution = GangSurgeTargetMarkerActive
				return true
			}
			if GangSurgeCleanupTargetClaimMatches(&currentTarget, targetRev) {
				resolution = GangSurgeTargetMarkerCleanup
				return true
			}
			return false
		},
		Postcondition: func(status *types.InstanceStatus) bool {
			return sameRestoredGangSurgeTarget(status, &desired)
		},
		OnCommit: func(_, _ *types.InstanceStatus) {
			resolution = GangSurgeTargetMarkerRestored
		},
	}
	err := Apply(ctx, input, []types.InstanceMutation{mutation})
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return GangSurgeTargetMarkerUnclaimed, nil
	}
	if err != nil {
		return GangSurgeTargetMarkerUnclaimed, err
	}
	return resolution, nil
}

// EmptyGangSurgeTargetSlot reports whether a slot carries nothing at all
// — the only shape a surge may claim.
func EmptyGangSurgeTargetSlot(status *types.InstanceStatus) bool {
	return status != nil && status.Incarnation == 0 && status.Phase == types.InstancePhaseEmpty &&
		status.RunningRevision == "" && status.TargetRevision == "" && status.Operation == nil &&
		status.PodCount == 0 && status.ServingPodCount == 0 && status.AvailablePodCount == 0 &&
		!status.Admitted && len(status.Conditions) == 0 && status.LastFailure == nil
}

// gangSurgeTargetMatches reports the live marker in full: the active
// claim, on a row still in Creating at the GangSurgeTarget step.
func gangSurgeTargetMatches(status *types.InstanceStatus, targetRevision string) bool {
	return GangSurgeActiveTargetClaimMatches(status, targetRevision) &&
		status.Phase == types.InstancePhaseCreating &&
		status.Operation.Step == types.UpdateStepGangSurgeTarget
}

// GangSurgeTargetClaimMatches reports either marker for targetRevision:
// the live claim or the cleanup one the collection leaves behind.
func GangSurgeTargetClaimMatches(status *types.InstanceStatus, targetRevision string) bool {
	return GangSurgeActiveTargetClaimMatches(status, targetRevision) ||
		GangSurgeCleanupTargetClaimMatches(status, targetRevision)
}

// GangSurgeActiveTargetClaimMatches reports the live target claim.
func GangSurgeActiveTargetClaimMatches(status *types.InstanceStatus, targetRevision string) bool {
	return status != nil && status.TargetRevision == targetRevision && status.Operation != nil &&
		status.Operation.Type == types.InstanceOperationUpdate &&
		status.Operation.Step == types.UpdateStepGangSurgeTarget &&
		status.Operation.TargetRevision == targetRevision
}

// GangSurgeCleanupTargetClaimMatches reports the cleanup claim: the
// marker a collection of the surge slot is running under.
func GangSurgeCleanupTargetClaimMatches(status *types.InstanceStatus, targetRevision string) bool {
	return status != nil && status.TargetRevision == targetRevision && status.Operation != nil &&
		status.Operation.Type == types.InstanceOperationUpdate &&
		status.Operation.Step == types.UpdateStepGangSurgeTargetCleanup &&
		status.Operation.TargetRevision == targetRevision
}

func sameRestoredGangSurgeTarget(current, desired *types.InstanceStatus) bool {
	return current != nil && desired != nil && gangSurgeTargetMatches(current, desired.TargetRevision) &&
		current.Index == desired.Index && current.Incarnation == desired.Incarnation &&
		current.Operation.ID == desired.Operation.ID
}

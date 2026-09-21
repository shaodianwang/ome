package status

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// MigrationRole picks which side of the migration pair a pin is written
// for — source goes to Phase=Migrating with sibling=surge; surge goes to
// Phase=Creating with sibling=source and Incarnation seeded.
type MigrationRole int

const (
	MigrationRoleSource MigrationRole = iota
	MigrationRoleSurge
)

// StampMigrationPin writes one side of the migration pair. Idempotent on
// (Phase, Operation.Type, RequestUUID).
func StampMigrationPin(ctx context.Context, input types.ReconcileInput, idx, siblingIdx int32, role MigrationRole, uuid string, timeout time.Duration) error {
	mutation := MigrationPinMutation(input, idx, siblingIdx, role, uuid, timeout)
	return input.MutateInstance(ctx, mutation.Index, mutation.Mutate)
}

// MigrationPinMutation is the migration pin as a mutation, for the pass
// that commits both sides of the pair in one batch. The Operation it
// writes is a pin — Type + RequestUUID (+ SurgeIndex for pair
// correlation) plus the timing fields the deadline machinery reads;
// migration facts (FromNode, hints, reason) live on the owner's
// status.migrations record, not on the row.
func MigrationPinMutation(input types.ReconcileInput, idx, siblingIdx int32, role MigrationRole, uuid string, timeout time.Duration) types.InstanceMutation {
	now := metav1.NewTime(input.Now())
	siblingPtr := siblingIdx
	wantPhase := types.InstancePhaseMigrating
	idSuffix := ""
	if role == MigrationRoleSurge {
		wantPhase = types.InstancePhaseCreating
		idSuffix = "-surge"
	}
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if role == MigrationRoleSource && s.Phase == "" {
			// Fresh-empty slot from the append path: the source status
			// was removed out from under us — don't resurrect it. The
			// surge role legitimately seeds a fresh slot.
			return false
		}
		if s.Phase == wantPhase &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationMigrate &&
			s.Operation.RequestUUID == uuid {
			return false
		}
		if role == MigrationRoleSurge && s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Phase = wantPhase
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("migrate-%s%s-%d", uuid, idSuffix, now.Unix()),
			Type:           types.InstanceOperationMigrate,
			Step:           "CreateSurge",
			RequestUUID:    uuid,
			SurgeIndex:     &siblingPtr,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	}}
}

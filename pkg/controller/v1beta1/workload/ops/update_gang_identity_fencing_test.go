package ops

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

func TestPatchInstanceStatusGangSurgeTarget_DoesNotOverwriteOccupiedSlot(t *testing.T) {
	occupied := workload.InstanceStatus{
		Index:           2,
		Incarnation:     8,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "unrelated-revision",
	}
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, mutate func(*workload.InstanceStatus) bool) error {
			if mutate(&occupied) {
				t.Fatal("occupied target slot was mutated")
			}
			return nil
		},
	}

	if err := patchInstanceStatusGangSurgeTarget(context.Background(), input, occupied.Index, "new-revision", time.Minute); err != nil {
		t.Fatal(err)
	}
	if occupied.Phase != workload.InstancePhaseReady || occupied.RunningRevision != "unrelated-revision" || occupied.Operation != nil {
		t.Fatalf("occupied target slot changed: %+v", occupied)
	}
}

func TestClaimGangSurgeDrain_RejectsAuthoritativePairDrift(t *testing.T) {
	const targetRevision = "pair-guard-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	target := gangSurgeActiveTarget(surgeIndex, targetRevision)
	tests := []struct {
		name   string
		mutate func(map[int32]workload.InstanceStatus)
	}{
		{
			name: "source lifecycle",
			mutate: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Operation.ID = "replacement-source"
				statuses[source.Index] = changed
			},
		},
		{
			name: "target lifecycle",
			mutate: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[target.Index]
				changed.Operation.ID = "replacement-target"
				statuses[target.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]workload.InstanceStatus{
				source.Index: cloneTerminalStatus(source),
				target.Index: cloneTerminalStatus(target),
			}
			test.mutate(statuses)
			store := &terminalMutationStore{ownerUID: "owner-a", statuses: statuses}
			input := gangSurgeRecoveryInput("owner-a", "pair-guard", "test-ns", store,
				cloneTerminalStatus(source), cloneTerminalStatus(target))

			claimed, err := claimGangSurgeDrain(context.Background(), input, &source, &target)
			if err != nil || claimed || store.writes != 0 {
				t.Fatalf("claim with drift: claimed=%v writes=%d err=%v", claimed, store.writes, err)
			}
		})
	}
}

func TestPromoteGangSurgeTarget_RejectsAuthoritativePairDrift(t *testing.T) {
	const targetRevision = "promotion-guard-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	source.Operation.Step = updateStepSurgeDrain
	target := gangSurgeActiveTarget(surgeIndex, targetRevision)
	tests := []struct {
		name   string
		mutate func(map[int32]workload.InstanceStatus)
	}{
		{
			name: "source lifecycle",
			mutate: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Operation.ID = "replacement-source"
				statuses[source.Index] = changed
			},
		},
		{
			name: "target lifecycle",
			mutate: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[target.Index]
				changed.Operation.ID = "replacement-target"
				statuses[target.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]workload.InstanceStatus{
				source.Index: cloneTerminalStatus(source),
				target.Index: cloneTerminalStatus(target),
			}
			test.mutate(statuses)
			store := &terminalMutationStore{ownerUID: "owner-a", statuses: statuses}
			input := gangSurgeRecoveryInput("owner-a", "promotion-guard", "test-ns", store,
				cloneTerminalStatus(source), cloneTerminalStatus(target))

			promoted, err := promoteGangSurgeTarget(context.Background(), input, &source, &target, targetRevision)
			if err != nil || promoted || store.writes != 0 {
				t.Fatalf("promotion with drift: promoted=%v writes=%d err=%v", promoted, store.writes, err)
			}
			if current := store.statuses[target.Index]; current.Operation == nil || current.Operation.ID != "replacement-target" && test.name == "target lifecycle" {
				t.Fatalf("promotion changed authoritative target: %+v", current)
			}
		})
	}
}

func TestGangSurge_TargetConflictRollbackGuardsAuthoritativePair(t *testing.T) {
	surgeIndex := int32(2)
	const targetRevision = "gang-pair-guard-engine-newrev"
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	occupied := workload.InstanceStatus{
		Index:           surgeIndex,
		Incarnation:     8,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "unrelated-revision",
	}
	tests := []struct {
		name       string
		ownerUID   types.UID
		storeUID   types.UID
		mutateLive func(map[int32]workload.InstanceStatus)
	}{
		{
			name:     "owner changed",
			ownerUID: "owner-a",
			storeUID: "owner-b",
		},
		{
			name:     "source lifecycle changed",
			ownerUID: "owner-a",
			storeUID: "owner-a",
			mutateLive: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Operation.Step = updateStepSurgeDrain
				statuses[source.Index] = changed
			},
		},
		{
			name:     "target lifecycle changed",
			ownerUID: "owner-a",
			storeUID: "owner-a",
			mutateLive: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[occupied.Index]
				changed.Incarnation++
				statuses[occupied.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]workload.InstanceStatus{
				source.Index:   cloneTerminalStatus(source),
				occupied.Index: cloneTerminalStatus(occupied),
			}
			if test.mutateLive != nil {
				test.mutateLive(statuses)
			}
			store := &terminalMutationStore{ownerUID: test.storeUID, statuses: statuses}
			input := gangSurgeRecoveryInput(test.ownerUID, "gang-pair-guard", "test-ns", store,
				cloneTerminalStatus(source), cloneTerminalStatus(occupied))

			reset, err := resetGangSurgeSourceAfterTargetConflict(context.Background(), input, &source, &occupied)
			if err != nil || reset {
				t.Fatalf("guarded rollback: reset=%v err=%v", reset, err)
			}
			if store.writes != 0 {
				t.Fatalf("guarded rollback status writes=%d want 0", store.writes)
			}
		})
	}
}

package status

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

func TestRestoreGangSurgeTargetMarker_StrongOwnershipGuard(t *testing.T) {
	source := terminalStatusFixture(3)
	surgeIndex := *source.Operation.SurgeIndex
	targetRevision := source.Operation.TargetRevision
	tests := []struct {
		name           string
		prepare        func(*terminalMutationStore)
		wantResolution GangSurgeTargetMarker
		wantWrites     int
	}{
		{
			name:           "exact source restores marker",
			wantResolution: GangSurgeTargetMarkerRestored,
			wantWrites:     1,
		},
		{
			name: "wire-normalized source restores marker",
			prepare: func(store *terminalMutationStore) {
				current := store.statuses[source.Index]
				current.Operation.StartedAt = metav1.NewTime(current.Operation.StartedAt.Truncate(time.Second))
				current.Operation.LastProgressAt = metav1.NewTime(current.Operation.LastProgressAt.Truncate(time.Second))
				store.statuses[source.Index] = current
			},
			wantResolution: GangSurgeTargetMarkerRestored,
			wantWrites:     1,
		},
		{
			name: "existing matching marker is confirmed",
			prepare: func(store *terminalMutationStore) {
				store.statuses[surgeIndex] = types.InstanceStatus{
					Index:          surgeIndex,
					Incarnation:    1,
					Phase:          types.InstancePhaseCreating,
					TargetRevision: targetRevision,
					Operation: &types.InstanceOperation{
						ID:             "existing-target",
						Type:           types.InstanceOperationUpdate,
						Step:           types.UpdateStepGangSurgeTarget,
						TargetRevision: targetRevision,
					},
				}
			},
			wantResolution: GangSurgeTargetMarkerActive,
		},
		{
			name: "existing cleanup marker is reported",
			prepare: func(store *terminalMutationStore) {
				marker := types.InstanceStatus{
					Index:          surgeIndex,
					Incarnation:    1,
					Phase:          types.InstancePhaseCreating,
					TargetRevision: targetRevision,
					Operation: &types.InstanceOperation{
						ID:             "existing-target",
						Type:           types.InstanceOperationUpdate,
						Step:           types.UpdateStepGangSurgeTargetCleanup,
						TargetRevision: targetRevision,
					},
				}
				store.statuses[surgeIndex] = marker
			},
			wantResolution: GangSurgeTargetMarkerCleanup,
		},
		{
			name: "source ownership drift rejects marker",
			prepare: func(store *terminalMutationStore) {
				current := store.statuses[source.Index]
				current.Operation.ID = "replacement-source"
				store.statuses[source.Index] = current
			},
		},
		{
			name: "owner recreation rejects marker",
			prepare: func(store *terminalMutationStore) {
				store.ownerUID = "owner-b"
			},
		},
		{
			name: "occupied target rejects marker",
			prepare: func(store *terminalMutationStore) {
				store.statuses[surgeIndex] = types.InstanceStatus{
					Index: surgeIndex, Incarnation: 4, Phase: types.InstancePhaseReady,
					RunningRevision: "unrelated",
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &terminalMutationStore{
				ownerUID: "owner-a",
				statuses: map[int32]types.InstanceStatus{source.Index: cloneTerminalStatus(source)},
			}
			if test.prepare != nil {
				test.prepare(store)
			}
			input := types.ReconcileInput{
				OwnerObject:                          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
				FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
				ApplyInstanceMutationsWithRetryBlock: store.apply,
			}

			resolution, err := RestoreGangSurgeTarget(
				context.Background(), input, &source, surgeIndex, targetRevision, time.Minute,
			)
			if err != nil || resolution != test.wantResolution {
				t.Fatalf("result: resolution=%v err=%v", resolution, err)
			}
			if store.writes != test.wantWrites {
				t.Fatalf("row writes=%d want %d", store.writes, test.wantWrites)
			}
			if test.wantResolution == GangSurgeTargetMarkerRestored || test.wantResolution == GangSurgeTargetMarkerActive {
				marker := store.statuses[surgeIndex]
				if !gangSurgeTargetMatches(&marker, targetRevision) {
					t.Fatalf("target marker=%+v", marker)
				}
			}
		})
	}
}

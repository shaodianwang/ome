package v1beta1

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOMENativeComponentStatus_JSONRoundTrip(t *testing.T) {
	requestedAt := metav1.NewTime(time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC))
	startedAt := metav1.NewTime(time.Date(2026, 5, 15, 12, 1, 0, 0, time.UTC))

	original := InferenceServiceStatus{
		Components: map[ComponentType]ComponentStatusSpec{
			EngineComponent: {
				OMENative: &OMENativeComponentStatus{
					ObservedGeneration: 42,
					CurrentRevision:    "llama-70b-engine-rev-abc123",
					UpdateRevision:     "llama-70b-engine-rev-abc123",
					LabelSelector:      "ome.io/inferenceservice=llama-70b,ome.io/component=engine",
					Replicas:           4,
					ReadyReplicas:      4,
					AvailableReplicas:  4,
					InstanceStatuses: []OMENativeInstanceStatus{
						{
							Index:             0,
							Incarnation:       3,
							Phase:             OMENativeInstanceReady,
							RunningRevision:   "llama-70b-engine-rev-abc123",
							TargetRevision:    "llama-70b-engine-rev-abc123",
							PodCount:          4,
							ReadyPodCount:     4,
							AvailablePodCount: 4,
							ScheduledPodCount: 4,
							NodesOccupied:     []string{"node3", "node7", "node9", "node12"},
						},
					},
				},
			},
		},
		MigrationHistory: []MigrationHistoryEntry{
			{
				ID:                  "fake-uuid-1",
				Component:           EngineComponent,
				Instance:            0,
				ReplacementInstance: 2,
				Mode:                MigrationModeSurge,
				Phase:               MigrationPhaseInProgress,
				RequestedAt:         requestedAt,
				StartedAt:           &startedAt,
				Reason:              "fragmentation",
				RequestedBy:         "alfred-controller",
				Events: []MigrationEvent{
					{At: startedAt, Message: "surge Instance created on node3"},
				},
			},
		},
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded InferenceServiceStatus
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if diff := cmp.Diff(original, decoded); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestOMENativeInstancePhase_Values(t *testing.T) {
	want := []OMENativeInstancePhase{
		OMENativeInstancePending,
		OMENativeInstanceCreating,
		OMENativeInstanceReady,
		OMENativeInstanceUpdating,
		OMENativeInstanceRestarting,
		OMENativeInstanceMigrating,
		OMENativeInstanceFailed,
		OMENativeInstanceDeleting,
	}
	wantStrings := []string{"Pending", "Creating", "Ready", "Updating", "Restarting", "Migrating", "Failed", "Deleting"}
	for i, p := range want {
		if string(p) != wantStrings[i] {
			t.Errorf("phase[%d]: got %q want %q", i, string(p), wantStrings[i])
		}
	}
}

func TestMigrationPhase_Values(t *testing.T) {
	pairs := map[MigrationPhase]string{
		MigrationPhasePending:    "Pending",
		MigrationPhaseInProgress: "InProgress",
		MigrationPhaseCompleted:  "Completed",
		MigrationPhaseFailed:     "Failed",
	}
	for got, want := range pairs {
		if string(got) != want {
			t.Errorf("got %q want %q", string(got), want)
		}
	}
}

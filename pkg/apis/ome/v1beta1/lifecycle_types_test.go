package v1beta1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func int32Ptr(v int32) *int32 { return &v }

func TestOMENativeComponentStatus_JSONRoundTrip(t *testing.T) {
	requestedAt := metav1.NewTime(time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC))
	startedAt := metav1.NewTime(time.Date(2026, 5, 15, 12, 1, 0, 0, time.UTC))
	deadline := metav1.NewTime(time.Date(2026, 5, 15, 12, 31, 0, 0, time.UTC))

	original := InferenceServiceStatus{
		Components: map[ComponentType]ComponentStatusSpec{
			EngineComponent: {
				OMENative: &OMENativeComponentStatus{
					ObservedGeneration:   42,
					CurrentRevision:      "llama-70b-engine-rev-abc123",
					UpdateRevision:       "llama-70b-engine-rev-def456",
					CollisionCount:       int32Ptr(1),
					LabelSelector:        "ome.io/inferenceservice=llama-70b,ome.io/component=engine",
					Replicas:             4,
					ReadyReplicas:        3,
					AvailableReplicas:    3,
					UpdatedReplicas:      2,
					UpdatedReadyReplicas: 1,
					Conditions: []metav1.Condition{
						{Type: "Available", Status: metav1.ConditionTrue, LastTransitionTime: startedAt, Reason: "InstancesReady"},
						{Type: "Progressing", Status: metav1.ConditionTrue, LastTransitionTime: startedAt, Reason: "RolloutInProgress"},
					},
					InstanceStatuses: []OMENativeInstanceStatus{
						{
							Index:             0,
							Incarnation:       3,
							Phase:             OMENativeInstanceUpdating,
							RunningRevision:   "llama-70b-engine-rev-abc123",
							TargetRevision:    "llama-70b-engine-rev-def456",
							PodCount:          4,
							ReadyPodCount:     2,
							AvailablePodCount: 2,
							ScheduledPodCount: 4,
							NodesOccupied:     []string{"node3", "node7", "node9", "node12"},
							Conditions: []metav1.Condition{
								{Type: "AllPodsScheduled", Status: metav1.ConditionTrue, LastTransitionTime: startedAt, Reason: "Scheduled"},
							},
							Operation: &InstanceOperation{
								ID:             "op-uuid-1",
								Type:           InstanceOperationUpdate,
								Step:           "Drain",
								StartedAt:      startedAt,
								LastProgressAt: startedAt,
								Deadline:       deadline,
								RetryCount:     0,
								TargetRevision: "llama-70b-engine-rev-def456",
								Reason:         "ImageBump",
							},
						},
						{
							Index:           1,
							Incarnation:     1,
							Phase:           OMENativeInstanceMigrating,
							RunningRevision: "llama-70b-engine-rev-abc123",
							Operation: &InstanceOperation{
								ID:              "op-uuid-2",
								Type:            InstanceOperationMigrate,
								Step:            "WaitReady",
								StartedAt:       startedAt,
								LastProgressAt:  startedAt,
								Deadline:        deadline,
								SurgeIndex:      int32Ptr(2),
								FromNode:        "node1",
								HintTargetNodes: []string{"node3", "node7"},
								RequestUUID:     "migration-uuid-1",
							},
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
				ReplacementInstance: int32Ptr(2),
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
			{
				// Rejected-before-allocation case: ReplacementInstance is nil,
				// distinct from a legitimate replacement index of 0.
				ID:          "fake-uuid-2",
				Component:   EngineComponent,
				Instance:    0,
				Mode:        MigrationModeSurge,
				Phase:       MigrationPhaseFailed,
				RequestedAt: requestedAt,
				Reason:      "fragmentation",
				RequestedBy: "alfred-controller",
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

func TestInstanceOperationType_Values(t *testing.T) {
	pairs := map[InstanceOperationType]string{
		InstanceOperationCreate:  "Create",
		InstanceOperationUpdate:  "Update",
		InstanceOperationRestart: "Restart",
		InstanceOperationMigrate: "Migrate",
		InstanceOperationDelete:  "Delete",
	}
	for got, want := range pairs {
		if string(got) != want {
			t.Errorf("got %q want %q", string(got), want)
		}
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

func TestLifecycleSpec_JSONRoundTrip(t *testing.T) {
	restartPolicy := InstanceRestartPolicyRecreateInstance
	updateType := UpdateStrategyInPlaceIfPossible
	readyPolicy := InstanceReadyPolicyAllPodReady
	timeout := metav1.Duration{Duration: 30 * time.Minute}
	graceSecs := int32(30)
	markNotReady := true
	partition := int32(0)
	maxUnavail := int32(1)

	original := EngineSpec{
		ComponentExtensionSpec: ComponentExtensionSpec{
			Lifecycle: &LifecycleSpec{
				RestartPolicy: &restartPolicy,
				UpdateStrategy: &UpdateStrategy{
					Type: updateType,
					RollingUpdate: &RollingUpdate{
						Partition:      &partition,
						MaxUnavailable: &maxUnavail,
					},
					InPlaceUpdateStrategy: &InPlaceUpdateStrategy{
						GracePeriodSeconds:          &graceSecs,
						MarkNotReadyDuringLifecycle: &markNotReady,
					},
				},
				ReadyPolicy:          &readyPolicy,
				InstanceReadyTimeout: &timeout,
				MigrationPolicy: &MigrationPolicy{
					Mode: MigrationPolicyModeSurge,
				},
			},
		},
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded EngineSpec
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if diff := cmp.Diff(original, decoded); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestLifecycleSpec_JSONKeyIsNestedUnderLifecycle(t *testing.T) {
	// Confirm the OMENative restart policy serializes under
	// `lifecycle.restartPolicy`, not at the top level where it would collide
	// with the inlined corev1.PodSpec.RestartPolicy.
	policy := InstanceRestartPolicyRecreateInstance
	spec := EngineSpec{
		ComponentExtensionSpec: ComponentExtensionSpec{
			Lifecycle: &LifecycleSpec{RestartPolicy: &policy},
		},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(encoded)
	if !strings.Contains(got, `"lifecycle":{"restartPolicy":"RecreateInstanceOnPodRestart"}`) {
		t.Fatalf("expected nested lifecycle.restartPolicy, got: %s", got)
	}
}

func TestInstanceRestartPolicy_Values(t *testing.T) {
	pairs := map[InstanceRestartPolicy]string{
		InstanceRestartPolicyNone:             "None",
		InstanceRestartPolicyRecreateInstance: "RecreateInstanceOnPodRestart",
		InstanceRestartPolicyRecreateISVC:     "RecreateISVCOnPodRestart",
	}
	for got, want := range pairs {
		if string(got) != want {
			t.Errorf("got %q want %q", string(got), want)
		}
	}
}

func TestUpdateStrategyType_Values(t *testing.T) {
	pairs := map[UpdateStrategyType]string{
		UpdateStrategyRecreatePod:       "RecreatePod",
		UpdateStrategyInPlaceIfPossible: "InPlaceIfPossible",
		UpdateStrategyInPlaceOnly:       "InPlaceOnly",
	}
	for got, want := range pairs {
		if string(got) != want {
			t.Errorf("got %q want %q", string(got), want)
		}
	}
}

func TestMigrationPolicyMode_Values(t *testing.T) {
	pairs := map[MigrationPolicyMode]string{
		MigrationPolicyModeAuto:  "auto",
		MigrationPolicyModeSurge: "surge",
		MigrationPolicyModeNever: "never",
	}
	for got, want := range pairs {
		if string(got) != want {
			t.Errorf("got %q want %q", string(got), want)
		}
	}
}

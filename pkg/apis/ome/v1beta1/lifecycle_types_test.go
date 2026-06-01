package v1beta1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func int32Ptr(v int32) *int32 { return &v }

func TestLifecycleStatus_JSONRoundTrip(t *testing.T) {
	requestedAt := metav1.NewTime(time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC))
	startedAt := metav1.NewTime(time.Date(2026, 5, 15, 12, 1, 0, 0, time.UTC))
	deadline := metav1.NewTime(time.Date(2026, 5, 15, 12, 31, 0, 0, time.UTC))

	original := InferenceServiceStatus{
		Components: map[ComponentType]ComponentStatusSpec{
			EngineComponent: {
				Lifecycle: &LifecycleStatus{
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
	maxUnavail := intstr.FromInt(1)
	maxSurge := intstr.FromString("25%")

	original := EngineSpec{
		ComponentExtensionSpec: ComponentExtensionSpec{
			Lifecycle: &LifecycleSpec{
				RestartPolicy: &restartPolicy,
				UpdateStrategy: &UpdateStrategy{
					Type: updateType,
					RollingUpdate: &RollingUpdate{
						Partition:      &partition,
						MaxUnavailable: &maxUnavail,
						MaxSurge:       &maxSurge,
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
		MigrationPolicyModeAuto:  "Auto",
		MigrationPolicyModeSurge: "Surge",
		MigrationPolicyModeNever: "Never",
	}
	for got, want := range pairs {
		if string(got) != want {
			t.Errorf("got %q want %q", string(got), want)
		}
	}
}

// TestRollingUpdate_MaxUnavailable_AcceptsIntAndPercent exercises the
// IntOrString-typed MaxUnavailable field over the three shapes the
// CRD validator permits: nil (default), integer count, and percent
// string. Mirrors the corresponding MaxSurge case below — both fields
// share the same encoder so the parity check guards against future
// refactors that drift one shape out of sync.
func TestRollingUpdate_MaxUnavailable_AcceptsIntAndPercent(t *testing.T) {
	intVal := intstr.FromInt(2)
	pctVal := intstr.FromString("25%")
	cases := []struct {
		name string
		in   *intstr.IntOrString
		want string // empty when MaxUnavailable is nil (field omitted)
	}{
		{"nil-omitted", nil, ""},
		{"integer-count", &intVal, `"maxUnavailable":2`},
		{"percent-string", &pctVal, `"maxUnavailable":"25%"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ru := RollingUpdate{MaxUnavailable: tc.in}
			b, err := json.Marshal(ru)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got := string(b)
			if tc.want == "" {
				if strings.Contains(got, "maxUnavailable") {
					t.Fatalf("expected maxUnavailable omitted, got %s", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Fatalf("expected %q in %s", tc.want, got)
			}
			var decoded RollingUpdate
			if err := json.Unmarshal(b, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if diff := cmp.Diff(ru, decoded); diff != "" {
				t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestRollingUpdate_MaxSurge_AcceptsIntAndPercent mirrors the
// MaxUnavailable matrix above. MaxSurge is a new field (Gap 5) so the
// nil case is the default-empty shape; the integer and percent forms
// give callers the same flexibility as upstream
// appsv1.Deployment.Strategy.RollingUpdate.MaxSurge.
func TestRollingUpdate_MaxSurge_AcceptsIntAndPercent(t *testing.T) {
	intVal := intstr.FromInt(1)
	pctVal := intstr.FromString("50%")
	cases := []struct {
		name string
		in   *intstr.IntOrString
		want string
	}{
		{"nil-omitted", nil, ""},
		{"integer-count", &intVal, `"maxSurge":1`},
		{"percent-string", &pctVal, `"maxSurge":"50%"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ru := RollingUpdate{MaxSurge: tc.in}
			b, err := json.Marshal(ru)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got := string(b)
			if tc.want == "" {
				if strings.Contains(got, "maxSurge") {
					t.Fatalf("expected maxSurge omitted, got %s", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Fatalf("expected %q in %s", tc.want, got)
			}
			var decoded RollingUpdate
			if err := json.Unmarshal(b, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if diff := cmp.Diff(ru, decoded); diff != "" {
				t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestRollingUpdate_DeepCopy_PreservesIntOrString verifies the
// generated DeepCopy reproduces both MaxUnavailable and MaxSurge as
// independent IntOrString pointers — mutating the copy must not change
// the original. This guards the zz_generated.deepcopy.go output after
// the int32 → IntOrString widening in Gap 4 / Gap 5.
func TestRollingUpdate_DeepCopy_PreservesIntOrString(t *testing.T) {
	mu := intstr.FromString("30%")
	ms := intstr.FromInt(3)
	original := &RollingUpdate{
		MaxUnavailable: &mu,
		MaxSurge:       &ms,
	}
	clone := original.DeepCopy()
	if clone.MaxUnavailable == original.MaxUnavailable {
		t.Fatalf("DeepCopy MaxUnavailable must allocate a new pointer")
	}
	if clone.MaxSurge == original.MaxSurge {
		t.Fatalf("DeepCopy MaxSurge must allocate a new pointer")
	}
	if diff := cmp.Diff(original, clone); diff != "" {
		t.Fatalf("DeepCopy value mismatch (-want +got):\n%s", diff)
	}
	// Mutate clone; original must stay intact.
	clone.MaxUnavailable.StrVal = "99%"
	clone.MaxSurge.IntVal = 99
	if original.MaxUnavailable.StrVal != "30%" {
		t.Fatalf("DeepCopy aliased MaxUnavailable: original mutated to %q", original.MaxUnavailable.StrVal)
	}
	if original.MaxSurge.IntVal != 3 {
		t.Fatalf("DeepCopy aliased MaxSurge: original mutated to %d", original.MaxSurge.IntVal)
	}
}

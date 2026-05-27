package core

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func intPtr(v int) *int       { return &v }
func int32Ptr(v int32) *int32 { return &v }
func boolPtr(v bool) *bool    { return &v }
func restartPolicyPtr(v v1beta1.OMENativeRestartPolicy) *v1beta1.OMENativeRestartPolicy {
	return &v
}
func readyPolicyPtr(v v1beta1.OMENativeReadyPolicy) *v1beta1.OMENativeReadyPolicy {
	return &v
}

func TestBuildPlan_NilComponentExt(t *testing.T) {
	_, err := BuildPlan(ReconcileParams{Component: v1beta1.EngineComponent})
	if err == nil {
		t.Fatal("expected error for nil ComponentExt, got nil")
	}
}

// TestBuildPlan_MultiPodEmitsLeaderWorkerRunners pins the multi-pod layout:
// one leader runner of size 1 plus one worker runner of size WorkerSize.
// Replaces the previous TestBuildPlan_MultiPodRejected, which guarded the
// pre-multi-node ErrMultiPodNotYetSupported short-circuit.
func TestBuildPlan_MultiPodEmitsLeaderWorkerRunners(t *testing.T) {
	plan, err := BuildPlan(ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2)},
		MultiPod:     true,
		WorkerSize:   3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := plan.Replicas, int32(2); got != want {
		t.Errorf("Replicas: got %d want %d", got, want)
	}
	if got, want := len(plan.Instances), 2; got != want {
		t.Fatalf("len(Instances): got %d want %d", got, want)
	}
	for i, inst := range plan.Instances {
		if int(inst.Index) != i {
			t.Errorf("Instances[%d].Index: got %d want %d", i, inst.Index, i)
		}
		want := []RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 3},
		}
		if diff := cmp.Diff(want, inst.Runners); diff != "" {
			t.Errorf("Instances[%d].Runners mismatch (-want +got):\n%s", i, diff)
		}
	}
}

// TestBuildPlan_MultiPodDefaultsMatchOEP pins the OEP-mandated defaults for
// multi-pod Instances: RestartPolicy=RecreateInstance, ReadyPolicy=AllPodReady.
// Single-pod defaults stay None for both (verified elsewhere).
func TestBuildPlan_MultiPodDefaultsMatchOEP(t *testing.T) {
	plan, err := BuildPlan(ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(1)},
		MultiPod:     true,
		WorkerSize:   1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.RestartPolicy != v1beta1.OMENativeRestartPolicyRecreateInstance {
		t.Errorf("RestartPolicy: got %q want RecreateInstanceOnPodRestart", plan.RestartPolicy)
	}
	if plan.ReadyPolicy != v1beta1.OMENativeReadyPolicyAllPodReady {
		t.Errorf("ReadyPolicy: got %q want AllPodReady", plan.ReadyPolicy)
	}
}

// TestBuildPlan_MultiPodWithZeroWorkerSize confirms WorkerSize=0 still
// produces leader+worker entries; the webhook validator rejects orphan
// leader without worker.size>0, but BuildPlan stays defensive — it would
// produce a leader-only Instance (worker runner of size 0). Multi-pod
// without worker is a misconfiguration callers should never reach.
func TestBuildPlan_MultiPodWithZeroWorkerSize(t *testing.T) {
	plan, err := BuildPlan(ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(1)},
		MultiPod:     true,
		WorkerSize:   0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := len(plan.Instances[0].Runners), 2; got != want {
		t.Fatalf("Runners length: got %d want %d (leader + worker even if size=0)", got, want)
	}
	if plan.Instances[0].Runners[0].Size != 1 || plan.Instances[0].Runners[0].Name != "leader" {
		t.Errorf("leader runner shape: got %+v", plan.Instances[0].Runners[0])
	}
	if plan.Instances[0].Runners[1].Size != 0 || plan.Instances[0].Runners[1].Name != "worker" {
		t.Errorf("worker runner shape: got %+v", plan.Instances[0].Runners[1])
	}
}

func TestBuildPlan_SinglePod_DefaultsFromDefaulter(t *testing.T) {
	// Simulates an InferenceService that went through the mutating webhook
	// defaulter (#152) — omenative.* is fully populated.
	plan, err := BuildPlan(ReconcileParams{
		Component: v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{
			MinReplicas: intPtr(2),
			OMENative: &v1beta1.OMENativeComponentSpec{
				RestartPolicy: restartPolicyPtr(v1beta1.OMENativeRestartPolicyNone),
				ReadyPolicy:   readyPolicyPtr(v1beta1.OMENativeReadyPolicyNone),
				UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
					Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
					InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
						GracePeriodSeconds:          int32Ptr(30),
						MarkNotReadyDuringLifecycle: boolPtr(true),
					},
				},
				InstanceReadyTimeout: &metav1.Duration{Duration: 30 * time.Minute},
				MigrationPolicy:      &v1beta1.OMENativeMigrationPolicy{Mode: v1beta1.OMENativeMigrationPolicyModeAuto},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  2,
		Instances: []InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []RunnerPlan{{Name: "default", Size: 1}}},
		},
		RestartPolicy: v1beta1.OMENativeRestartPolicyNone,
		UpdateStrategy: v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.OMENativeInPlaceUpdateStrategy{
				GracePeriodSeconds:          int32Ptr(30),
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
		ReadyPolicy:          v1beta1.OMENativeReadyPolicyNone,
		InstanceReadyTimeout: 30 * time.Minute,
		MigrationMode:        v1beta1.OMENativeMigrationPolicyModeAuto,
	}
	if diff := cmp.Diff(want, plan); diff != "" {
		t.Fatalf("plan mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildPlan_SinglePod_InlineDefaultsWhenWebhookSkipped(t *testing.T) {
	// Simulates an ISVC that never went through the defaulter (e.g.,
	// pre-#152 object on disk). BuildPlan applies the same defaults inline.
	plan, err := BuildPlan(ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Replicas != 1 {
		t.Errorf("Replicas: got %d want 1", plan.Replicas)
	}
	if plan.RestartPolicy != v1beta1.OMENativeRestartPolicyNone {
		t.Errorf("RestartPolicy: got %q want None", plan.RestartPolicy)
	}
	if plan.ReadyPolicy != v1beta1.OMENativeReadyPolicyNone {
		t.Errorf("ReadyPolicy: got %q want None", plan.ReadyPolicy)
	}
	if plan.UpdateStrategy.Type != v1beta1.OMENativeUpdateStrategySurgeThenDrain {
		t.Errorf("UpdateStrategy.Type: got %q want SurgeThenDrain (default)", plan.UpdateStrategy.Type)
	}
	if plan.UpdateStrategy.InPlaceUpdateStrategy == nil ||
		plan.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds == nil ||
		*plan.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds != 30 {
		t.Errorf("gracePeriodSeconds: got %+v want 30", plan.UpdateStrategy.InPlaceUpdateStrategy)
	}
	if plan.InstanceReadyTimeout != 30*time.Minute {
		t.Errorf("InstanceReadyTimeout: got %v want 30m", plan.InstanceReadyTimeout)
	}
	if plan.MigrationMode != v1beta1.OMENativeMigrationPolicyModeAuto {
		t.Errorf("MigrationMode: got %q want auto", plan.MigrationMode)
	}
}

func TestBuildPlan_ReplicaCount(t *testing.T) {
	tests := []struct {
		name   string
		minRep *int
		want   int32
	}{
		{name: "nil MinReplicas defaults to 1", minRep: nil, want: 1},
		{name: "zero MinReplicas defaults to 1", minRep: intPtr(0), want: 1},
		{name: "explicit 1", minRep: intPtr(1), want: 1},
		{name: "explicit 4", minRep: intPtr(4), want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := BuildPlan(ReconcileParams{
				Component: v1beta1.EngineComponent,
				ComponentExt: &v1beta1.ComponentExtensionSpec{
					MinReplicas: tt.minRep,
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Replicas != tt.want {
				t.Errorf("Replicas: got %d want %d", plan.Replicas, tt.want)
			}
			if int32(len(plan.Instances)) != tt.want {
				t.Errorf("len(Instances): got %d want %d", len(plan.Instances), tt.want)
			}
			for i, inst := range plan.Instances {
				if inst.Index != int32(i) {
					t.Errorf("Instances[%d].Index: got %d", i, inst.Index)
				}
				if len(inst.Runners) != 1 ||
					inst.Runners[0].Name != "default" ||
					inst.Runners[0].Size != 1 {
					t.Errorf("Instances[%d].Runners: got %+v want [{default 1}]", i, inst.Runners)
				}
			}
		})
	}
}

func TestBuildPlan_IncarnationDefaultsToOneOnFirstReconcile(t *testing.T) {
	// No Status.Components yet — every Instance gets Incarnation=1.
	plan, err := BuildPlan(ReconcileParams{
		Component: v1beta1.EngineComponent,
		ISVC:      &v1beta1.InferenceService{},
		ComponentExt: &v1beta1.ComponentExtensionSpec{
			MinReplicas: intPtr(3),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, inst := range plan.Instances {
		if inst.Incarnation != 1 {
			t.Errorf("Instances[%d].Incarnation: got %d want 1", inst.Index, inst.Incarnation)
		}
	}
}

func TestBuildPlan_IncarnationPreservedFromStatus(t *testing.T) {
	// ISVC carries InstanceStatuses with explicit Incarnations — BuildPlan
	// reads them back instead of resetting to 1.
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Incarnation: 4},
							{Index: 1, Incarnation: 2},
						},
					},
				},
			},
		},
	}
	plan, err := BuildPlan(ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ISVC:         isvc,
		ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[int32]int64{0: 4, 1: 2}
	for _, inst := range plan.Instances {
		if got := inst.Incarnation; got != want[inst.Index] {
			t.Errorf("Instances[%d].Incarnation: got %d want %d", inst.Index, got, want[inst.Index])
		}
	}
}

func TestBuildPlan_IncarnationScopedToComponent(t *testing.T) {
	// Status entry for Decoder must not influence Engine plan.
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.DecoderComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Incarnation: 9},
						},
					},
				},
			},
		},
	}
	plan, err := BuildPlan(ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ISVC:         isvc,
		ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(1)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Instances[0].Incarnation != 1 {
		t.Errorf("Engine[0].Incarnation: got %d want 1 (decoder status must not leak)",
			plan.Instances[0].Incarnation)
	}
}

func TestBuildPlan_RestartPolicyExplicitWinsOverDefault(t *testing.T) {
	plan, err := BuildPlan(ReconcileParams{
		Component: v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{
			OMENative: &v1beta1.OMENativeComponentSpec{
				RestartPolicy: restartPolicyPtr(v1beta1.OMENativeRestartPolicyRecreateISVC),
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.RestartPolicy != v1beta1.OMENativeRestartPolicyRecreateISVC {
		t.Errorf("RestartPolicy: got %q want RecreateISVCOnPodRestart", plan.RestartPolicy)
	}
}

func TestLowestUnusedIndex(t *testing.T) {
	used := map[int32]struct{}{0: {}, 1: {}, 3: {}}
	if got := lowestUnusedIndex(used); got != 2 {
		t.Errorf("got %d want 2", got)
	}
	if got := lowestUnusedIndex(map[int32]struct{}{}); got != 0 {
		t.Errorf("empty set: got %d want 0", got)
	}
}

func TestInstancePlanIndices_MultiReplicaMigrationPreservesUnrelatedInstance(t *testing.T) {
	// Regression: with replicas=2 and statuses {0 Ready, 1 Ready},
	// migrating instance 0 (surge at 2) must yield plan {0, 1, 2}.
	// Counting migration-protected indices against the replica cap
	// would push instance 1 out of the plan and into extraInstanceIndices,
	// where Delete would silently tear it down.
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Phase: v1beta1.OMENativeInstanceMigrating},
							{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
							{Index: 2, Phase: v1beta1.OMENativeInstanceCreating, Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationMigrate}},
						},
					},
				},
			},
		},
	}
	got := instancePlanIndices(isvc, v1beta1.EngineComponent, 2)
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[1] || !hit[2] {
		t.Errorf("plan must include {0,1,2}; got %v", got)
	}
}

func TestInstancePlanIndices_PreservesSparseMigrationLayout(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Phase: v1beta1.OMENativeInstanceMigrating},
							{Index: 2, Phase: v1beta1.OMENativeInstanceCreating, Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationMigrate}},
						},
					},
				},
			},
		},
	}
	got := instancePlanIndices(isvc, v1beta1.EngineComponent, 1)
	// Both migration-in-flight indices must be in the plan even though
	// replicas=1.
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[2] {
		t.Errorf("plan must include both migration indices; got %v", got)
	}
}

func TestAllocateSurgeIndex(t *testing.T) {
	// Smoke: with statuses {0, 1, 3}, surge should land at 2.
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0}, {Index: 1}, {Index: 3},
						},
					},
				},
			},
		},
	}
	if got := AllocateSurgeIndex(isvc, v1beta1.EngineComponent); got != 2 {
		t.Errorf("got %d want 2", got)
	}
	// Empty status returns 0.
	empty := &v1beta1.InferenceService{}
	if got := AllocateSurgeIndex(empty, v1beta1.EngineComponent); got != 0 {
		t.Errorf("empty: got %d want 0", got)
	}
}

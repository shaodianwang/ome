package workload

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

// singlePodDesired builds a single-pod WorkloadDesiredSpec for tests.
// replicas <= 0 mirrors the production path where MinReplicas=nil/0
// defaults to 1.
func singlePodDesired(replicas int32, lifecycle v1beta1.OMENativeComponentSpec) WorkloadDesiredSpec {
	return WorkloadDesiredSpec{
		Replicas:  replicas,
		Runners:   []Runner{{Name: "default", Size: 1}},
		Lifecycle: lifecycle,
	}
}

func multiPodDesired(replicas, workerSize int32, lifecycle v1beta1.OMENativeComponentSpec) WorkloadDesiredSpec {
	return WorkloadDesiredSpec{
		Replicas: replicas,
		MultiPod: true,
		Runners: []Runner{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: workerSize},
		},
		Lifecycle: lifecycle,
	}
}

// TestBuildPlan_MultiPodEmitsLeaderWorkerRunners pins the multi-pod
// layout: one leader runner of size 1 plus one worker runner of size
// WorkerSize.
func TestBuildPlan_MultiPodEmitsLeaderWorkerRunners(t *testing.T) {
	desired := multiPodDesired(2, 3, v1beta1.OMENativeComponentSpec{})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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

// TestBuildPlan_MultiPodDefaultsMatchOEP pins the OEP-mandated defaults
// for multi-pod Instances: RestartPolicy=RecreateInstance,
// ReadyPolicy=AllPodReady. Single-pod defaults stay None for both.
func TestBuildPlan_MultiPodDefaultsMatchOEP(t *testing.T) {
	desired := multiPodDesired(1, 1, v1beta1.OMENativeComponentSpec{})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
// leader without worker.size>0, but BuildPlan stays defensive.
func TestBuildPlan_MultiPodWithZeroWorkerSize(t *testing.T) {
	desired := multiPodDesired(1, 0, v1beta1.OMENativeComponentSpec{})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
	// Simulates a WorkloadDesiredSpec produced by an adapter whose source
	// went through the mutating webhook defaulter — lifecycle is fully
	// populated.
	desired := singlePodDesired(2, v1beta1.OMENativeComponentSpec{
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
	})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
	// Simulates a pre-defaulter object — adapter projected an empty
	// lifecycle. BuildPlan applies the same defaults inline.
	desired := singlePodDesired(1, v1beta1.OMENativeComponentSpec{})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
			var rep int32
			if tt.minRep != nil {
				rep = int32(*tt.minRep)
			}
			desired := singlePodDesired(rep, v1beta1.OMENativeComponentSpec{})
			plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
	// Observed state has no InstanceStatuses — every Instance gets
	// Incarnation=1.
	desired := singlePodDesired(3, v1beta1.OMENativeComponentSpec{})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
	// Observed state carries InstanceStatuses with explicit Incarnations —
	// BuildPlan reads them back instead of resetting to 1.
	desired := singlePodDesired(2, v1beta1.OMENativeComponentSpec{})
	observed := WorkloadObservedState{
		InstanceStatuses: []InstanceStatus{
			{Index: 0, Incarnation: 4},
			{Index: 1, Incarnation: 2},
		},
	}
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, observed)
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

func TestBuildPlan_IncarnationScopedToWorkload(t *testing.T) {
	// The observed state is per-workload now (Source.ObservedState
	// returns only this Component's statuses), so cross-Component leakage
	// is impossible at the call site. The test pins the BuildPlan contract:
	// no InstanceStatuses → default to 1 for every Instance.
	desired := singlePodDesired(1, v1beta1.OMENativeComponentSpec{})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Instances[0].Incarnation != 1 {
		t.Errorf("Engine[0].Incarnation: got %d want 1", plan.Instances[0].Incarnation)
	}
}

func TestBuildPlan_RestartPolicyExplicitWinsOverDefault(t *testing.T) {
	desired := singlePodDesired(0, v1beta1.OMENativeComponentSpec{
		RestartPolicy: restartPolicyPtr(v1beta1.OMENativeRestartPolicyRecreateISVC),
	})
	plan, err := BuildPlan(v1beta1.EngineComponent, desired, WorkloadObservedState{})
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
	instances := []InstanceStatus{
		{Index: 0, Phase: InstancePhaseMigrating},
		{Index: 1, Phase: InstancePhaseReady},
		{Index: 2, Phase: InstancePhaseCreating, Operation: &InstanceOperation{Type: InstanceOperationMigrate}},
	}
	got := instancePlanIndices(instances, 2)
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[1] || !hit[2] {
		t.Errorf("plan must include {0,1,2}; got %v", got)
	}
}

func TestInstancePlanIndices_PreservesSparseMigrationLayout(t *testing.T) {
	instances := []InstanceStatus{
		{Index: 0, Phase: InstancePhaseMigrating},
		{Index: 2, Phase: InstancePhaseCreating, Operation: &InstanceOperation{Type: InstanceOperationMigrate}},
	}
	got := instancePlanIndices(instances, 1)
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
	instances := []InstanceStatus{
		{Index: 0}, {Index: 1}, {Index: 3},
	}
	if got := AllocateSurgeIndex(instances); got != 2 {
		t.Errorf("got %d want 2", got)
	}
	// Empty status returns 0.
	if got := AllocateSurgeIndex(nil); got != 0 {
		t.Errorf("empty: got %d want 0", got)
	}
}

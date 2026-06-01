package workload_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// Per-Component RollingUpdate.MaxSurge / MaxUnavailable dispatcher
// integration. The budget calculations themselves are covered in
// budget_test.go; these tests pin the dispatcher behavior when the
// per-Component layer denies, allows, and composes with the group-
// level UpdateGate. The end-to-end pod surge behavior at scale is
// covered in the KIND suite (tests/integration/.../coordination_kind/).

// TestReconcile_PerComponentMaxSurge_DeniesAtBudget pins the
// per-Component gate refusing to start a fresh surge when the layer's
// budget is already exhausted by a prior-pass in-flight surge.
// Setup: 3 plan instances, all needing update; budget=1; one Instance
// already in Phase=Updating + Step=Surge from a prior wake-up. The
// dispatcher MUST NOT start a fresh surge — it should requeue at the
// gate interval (Requeue / RequeueAfter > 0) and leave the in-flight
// Instance alone.
func TestReconcile_PerComponentMaxSurge_DeniesAtBudget(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 3
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		// Index 0: already mid-surge from a prior pass (charged to
		// priorSurgeInFlight=1). The dispatcher's Update call on this
		// one is allowed (startingFresh=false).
		{
			Index: 0, Incarnation: 1,
			Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev",
			Operation: &workload.InstanceOperation{
				Type: workload.InstanceOperationUpdate, Step: "Surge",
			},
		},
		// Indices 1 and 2: Ready on prior-rev. Should be gated because
		// the per-Component MaxSurge=1 is already consumed by index 0.
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	// Track which Instance indices the dispatcher attempted to start.
	// MutateInstance is the cheapest signal — any Update call mutates
	// status. Index 0 is allowed (already in progress); indices 1 / 2
	// must NOT mutate when the gate denies.
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  3,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{
			Type: workload.UpdateStrategySurgeThenDrain,
			RollingUpdate: &workload.RollingUpdate{
				MaxSurge: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		// Per-op machines can error on the empty fake client (no live
		// pods to drain). The contract under test is the gate behavior
		// — not the per-op success.
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing assertions:
	// 1) Indices 1 and 2 must NOT have been started — the per-Component
	//    MaxSurge=1 is fully consumed by the prior-pass index 0.
	if mutatedIndices[1] > 0 {
		t.Errorf("index 1 must be gated by per-Component MaxSurge=1; got %d mutations", mutatedIndices[1])
	}
	if mutatedIndices[2] > 0 {
		t.Errorf("index 2 must be gated by per-Component MaxSurge=1; got %d mutations", mutatedIndices[2])
	}
	// 2) The dispatcher must requeue — at least one Instance was gated.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when any Instance was gated; got %+v", result)
	}
}

// TestReconcile_PerComponentMaxSurge_AllowsWithinBudget is the dual:
// when the per-Component MaxSurge has headroom, the dispatcher allows
// at least one fresh start this wake-up. Setup: 3 plan instances all
// Ready on prior-rev (no prior-pass in-flight), budget=1, no group-
// level UpdateGate. Exactly one Instance should be started this pass.
func TestReconcile_PerComponentMaxSurge_AllowsWithinBudget(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 3
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  3,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{
			Type: workload.UpdateStrategySurgeThenDrain,
			RollingUpdate: &workload.RollingUpdate{
				MaxSurge: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing: exactly one Instance was started (other two gated).
	// Count distinct indices that mutated at least once.
	started := 0
	for _, count := range mutatedIndices {
		if count > 0 {
			started++
		}
	}
	if started != 1 {
		t.Errorf("with MaxSurge=1 and 3 Ready instances, expected exactly 1 fresh start; got %d (mutations=%v)", started, mutatedIndices)
	}
}

// TestReconcile_PerComponentMaxSurge_NilDefersToGroupGate verifies the
// composition rule when the per-Component layer is unset: the
// dispatcher MUST defer to whatever the group-level UpdateGate
// decides (the cited "BudgetNoLimit means no cap" behavior). We wire
// an UpdateGate that always denies and assert nothing starts.
func TestReconcile_PerComponentMaxSurge_NilDefersToGroupGate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 2
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	// Per-Component RollingUpdate left nil (BudgetNoLimit). The group
	// gate should be the only constraint.
	in.UpdateGate = func(_ workload.UpdateStrategyType, _, _ int32) (bool, string) {
		return false, "group gate denies"
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
		// RollingUpdate nil → per-Component layer doesn't cap.
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing: NO fresh starts (group gate said deny, per-Component
	// didn't cap so deferred to the group gate).
	for idx, count := range mutatedIndices {
		if count > 0 {
			t.Errorf("group gate denied yet index %d still mutated %d times", idx, count)
		}
	}
	// And the dispatcher must requeue.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when all Instances are gated; got %+v", result)
	}
}

// TestReconcile_PerComponentMaxUnavailable_DeniesAtBudget pins the
// non-surge strategy branch. Setup: RecreatePod strategy,
// MaxUnavailable=1, one Instance already in Phase=Updating + Step=Drain
// (charged to priorUnavailInFlight=1). The other Instance must be
// gated.
func TestReconcile_PerComponentMaxUnavailable_DeniesAtBudget(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 2
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		// Index 0: already draining (Step=Drain takes pod offline).
		{
			Index: 0, Incarnation: 2,
			Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev",
			Operation: &workload.InstanceOperation{
				Type: workload.InstanceOperationUpdate, Step: "Drain",
			},
		},
		// Index 1: Ready, needs update.
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{
			Type: workload.UpdateStrategyRecreatePod,
			RollingUpdate: &workload.RollingUpdate{
				MaxUnavailable: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Index 1 must be gated (budget=1 consumed by index 0).
	if mutatedIndices[1] > 0 {
		t.Errorf("index 1 must be gated by per-Component MaxUnavailable=1; got %d mutations", mutatedIndices[1])
	}
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when Instance is gated; got %+v", result)
	}
}

// TestReconcile_PerComponentBudget_PercentForm exercises the percent
// resolver through the dispatcher: 25% on 4 replicas → budget=1, so
// only one fresh start is allowed. This is the load-bearing
// integration assertion for percent expressions matching the
// budget_test.go ceil-rounding case.
func TestReconcile_PerComponentBudget_PercentForm(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 4
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 3, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  4,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 3, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{
			Type: workload.UpdateStrategySurgeThenDrain,
			RollingUpdate: &workload.RollingUpdate{
				MaxSurge: intOrStringStr("25%"), // 25% of 4 = ceil(1.0) = 1
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	started := 0
	for _, count := range mutatedIndices {
		if count > 0 {
			started++
		}
	}
	if started != 1 {
		t.Errorf("MaxSurge=25%% on 4 replicas yields budget=1; expected exactly 1 fresh start, got %d (mutations=%v)", started, mutatedIndices)
	}
}

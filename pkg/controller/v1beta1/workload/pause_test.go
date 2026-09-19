package workload_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// The pause holds a rollout at STEP boundaries: what it withholds is the
// next operation and the next step, never the step already under way.
// The tests below pin the two halves of that rule — which passes a
// paused plan still selects, and what a held attempt reports while it
// waits.

// TestPlan_Paused_AdvancesInFlightAttemptsOnly: a paused plan selects the
// attempts already in flight (a recreate mid-gap, a committed create) and
// nothing else — an Instance at a prior revision with no attempt open is
// not started, and escalation stays suspended. Unpaused, the same
// observation selects the fresh start too.
//
// Cells: instance/Update/Drain/spec.pauseTrue/before, instance/Create/CreatePods/spec.pauseTrue/before, instance/Ready/spec.pauseTrue/before, instance/Ready/spec.unpause/before
func TestPlan_Paused_AdvancesInFlightAttemptsOnly(t *testing.T) {
	build := func(t *testing.T) (workload.ReconcileInput, workload.ComponentPlan) {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
			// Recreate in flight: the old pods are gone and the new ones
			// are not created yet.
			{Index: 0, Incarnation: 2, Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev",
				Operation: &workload.InstanceOperation{
					ID: "update-0", Type: workload.InstanceOperationUpdate, Step: "Drain",
					TargetRevision: updateTarget().Name,
				}},
			// Off-target but idle: a fresh start.
			{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
			// Create committed to this target: the set is half materialized.
			{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseCreating,
				Operation: &workload.InstanceOperation{
					ID: "create-2", Type: workload.InstanceOperationCreate, Step: "CreatePods",
					TargetRevision: updateTarget().Name,
				}},
		}
		plan := minimalPlan()
		plan.Replicas = 3
		plan.Instances = append(plan.Instances,
			workload.InstancePlan{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			workload.InstancePlan{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		)
		return in, plan
	}
	// Index 1 is backed, so the truth pass has nothing to correct and the
	// decision is exactly the lifecycle selection under test.
	pods := func(in workload.ReconcileInput) map[int32][]*corev1.Pod {
		return map[int32][]*corev1.Pod{1: {enginePod(in.Key.OwnerName, in.Key.Namespace, 1)}}
	}

	t.Run("paused selects the open attempts only", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused = true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		if !kindsEqual(actionKinds(d), []workload.ActionKind{workload.ActionUpdate, workload.ActionCreate}) {
			t.Fatalf("paused decision = %v, want [Update Create]", actionKinds(d))
		}
		ua := findAction(d, workload.ActionUpdate)
		if len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 0 {
			t.Errorf("update selection = %+v, want the in-flight index 0 only", ua.Update.Items)
		}
		if ua.Update.Items[0].StartingFresh {
			t.Errorf("a paused selection must never start fresh: %+v", ua.Update.Items[0])
		}
		if d.Escalate {
			t.Errorf("paused decision must suspend escalation")
		}
	})

	t.Run("no committed create means no Create pass", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused = true
		in.ObservedState.InstanceStatuses = in.ObservedState.InstanceStatuses[:2]
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		if findAction(d, workload.ActionCreate) != nil {
			t.Errorf("paused decision with nothing committed must not plan Create, got %v", actionKinds(d))
		}
	})

	t.Run("unpause restores the fresh start", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		ua := findAction(d, workload.ActionUpdate)
		if ua == nil || len(ua.Update.Items) != 2 {
			t.Fatalf("unpaused update selection = %+v, want both indices", ua)
		}
		if !ua.Update.Items[1].StartingFresh {
			t.Errorf("index 1 = %+v, want a fresh start once the pause is cleared", ua.Update.Items[1])
		}
	})
}

// TestPlan_PauseFreeze_AdvancesOpenRepairOnly: a frozen pause suspends
// the repair pass — a fresh pod-loss trigger is not selected — but a
// repair already under way is still driven, so the pods it deleted are
// recreated instead of staying missing for the length of the hold. A
// standard pause keeps starting repairs.
//
// Cells: instance/Restart/Drain/spec.pauseFreeze/before, instance/Restart/Drain/spec.pauseTrue/before, instance/Ready/spec.pauseFreeze/before
func TestPlan_PauseFreeze_AdvancesOpenRepairOnly(t *testing.T) {
	build := func(t *testing.T) (workload.ReconcileInput, workload.ComponentPlan) {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
			{Index: 0, Incarnation: 2, Phase: workload.InstancePhaseRestarting,
				Operation: &workload.InstanceOperation{
					ID: "restart-0", Type: workload.InstanceOperationRestart, Step: "Drain",
				}},
			// Ready with no pods: a fresh pod-loss repair trigger.
			{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady},
		}
		plan := minimalPlan()
		plan.Replicas = 2
		plan.RestartPolicy = workload.RestartPolicyRecreateInstance
		plan.Paused = true
		plan.Instances = append(plan.Instances,
			workload.InstancePlan{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		)
		return in, plan
	}

	t.Run("frozen pause drives the open repair only", func(t *testing.T) {
		in, plan := build(t)
		plan.PauseFreeze = true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		ra := findAction(d, workload.ActionRestart)
		if ra == nil || len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 0 {
			t.Fatalf("frozen restart selection = %+v, want the open repair on index 0 only", ra)
		}
	})

	t.Run("frozen pause with nothing open plans no repair", func(t *testing.T) {
		in, plan := build(t)
		plan.PauseFreeze = true
		in.ObservedState.InstanceStatuses = in.ObservedState.InstanceStatuses[1:]
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		if findAction(d, workload.ActionRestart) != nil {
			t.Errorf("frozen decision must not start a repair, got %v", actionKinds(d))
		}
	})

	t.Run("standard pause keeps repairing", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		ra := findAction(d, workload.ActionRestart)
		if ra == nil || len(ra.Restarts) != 2 {
			t.Fatalf("paused restart selection = %+v, want both the open repair and the fresh one", ra)
		}
	})
}

// TestReconcile_PausedCompletesTheOpenRecreate: a pause taken between a
// recreate's two phases — the old pods deleted, the new ones not created
// yet — still recreates them. Holding there would leave the Instance
// dark for the whole pause, which is the one thing a pause may not do.
//
// Cells: instance/Update/Drain/spec.pauseTrue/before
func TestReconcile_PausedCompletesTheOpenRecreate(t *testing.T) {
	name := "paused-recreate"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 1, true, workload.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workload.InstancePhaseUpdating,
		RunningRevision: targetName,
		Operation: &workload.InstanceOperation{
			ID: "update-0", Type: workload.InstanceOperationUpdate, Step: "Drain",
			TargetRevision: targetName,
		},
	}, []workload.RunnerPlan{{Name: "default", Size: 1}})
	f.plan.RestartPolicy = ""
	f.plan.UpdateStrategy = workload.UpdateStrategy{Type: workload.UpdateStrategyRecreatePod}

	if _, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	want := query.PodName(f.isvc.Name, workload.ComponentEngine, 0, "default", 0)
	if !names[want] {
		t.Errorf("paused reconcile left the recreate half-done; pods = %v", names)
	}
}

// TestReconcile_PausedCompletesTheCommittedCreate: a create that already
// committed finishes the set it started materializing, while an index
// that never opened one stays empty until the pause is cleared. Holding
// a half-built set would leave the Instance with nothing serving.
//
// Cells: instance/Create/CreatePods/spec.pauseTrue/before, instance/Empty/spec.pauseTrue/before
func TestReconcile_PausedCompletesTheCommittedCreate(t *testing.T) {
	name := "paused-create"
	targetName := name + "-engine-target"
	member := enginePod(name, "prod", 0)
	f := newRestartScaleTestFixture(t, name, 2, true, workload.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: workload.InstancePhaseCreating, PodCount: 1,
		RunningRevision: targetName,
		Operation: &workload.InstanceOperation{
			ID: "create-0", Type: workload.InstanceOperationCreate, Step: "CreatePods",
			TargetRevision: targetName,
		},
	}, []workload.RunnerPlan{{Name: "default", Size: 2}}, member)
	// The gang-member-loss repair owns a partial set under the recreate
	// policy; this test is about the Create pass finishing its own.
	f.plan.RestartPolicy = ""

	if _, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	missing := query.PodName(f.isvc.Name, workload.ComponentEngine, 0, "default", 1)
	if !names[missing] {
		t.Errorf("paused reconcile left the committed gang half-built; pods = %v", names)
	}
	absent := query.PodName(f.isvc.Name, workload.ComponentEngine, 1, "default", 0)
	if names[absent] {
		t.Errorf("paused reconcile materialized an index that never committed a create; pods = %v", names)
	}
	if s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1); s != nil {
		t.Errorf("paused reconcile allocated status for an uncommitted index: %+v", s)
	}
}

// TestReconcile_PausedLeavesASupersededCreateAlone: a revision edit
// arriving mid-pause does not reach a half-built set. Finishing it is
// not what the Create pass would do — it retires the attempt and opens a
// fresh one at the new revision, with a new identity, a new deadline and
// re-rendered pods — and that is the new operation a pause withholds.
// The retirement runs on the unpause.
//
// Cells: instance/Create/CreatePods/spec.pauseTrue/before, instance/Create/CreatePods/spec.unpause/before
func TestReconcile_PausedLeavesASupersededCreateAlone(t *testing.T) {
	name := "paused-superseded-create"
	pinned := name + "-engine-pinned"
	member := enginePod(name, "prod", 0)
	build := func(t *testing.T, paused bool) *restartScaleTestFixture {
		// RunningRevision empty: a first materialization, which is the
		// shape whose pods the attempt owns and can hand to a successor.
		f := newRestartScaleTestFixture(t, name, 1, paused, workload.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workload.InstancePhaseCreating, PodCount: 1,
			Operation: &workload.InstanceOperation{
				ID: "create-0", Type: workload.InstanceOperationCreate, Step: "CreatePods",
				TargetRevision: pinned,
			},
		}, []workload.RunnerPlan{{Name: "default", Size: 2}}, member)
		f.plan.RestartPolicy = ""
		// The edit: the pass now converges to a revision the in-flight
		// attempt was not opened for.
		f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: name + "-engine-edited", Namespace: f.isvc.Namespace,
		}}
		return f
	}

	f := build(t, true)
	if _, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile paused: %v", err)
	}
	got := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if got == nil || got.Operation == nil || got.Operation.ID != "create-0" ||
		got.Operation.TargetRevision != pinned {
		t.Errorf("paused reconcile replaced the in-flight attempt: %+v", got.Operation)
	}
	if names := podNameSet(t, f.client, f.isvc.Namespace); len(names) != 1 {
		t.Errorf("paused reconcile built pods for the edited revision; pods = %v", names)
	}

	f = build(t, false)
	if _, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile unpaused: %v", err)
	}
	got = instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if got == nil || got.Operation == nil || got.Operation.ID == "create-0" {
		t.Errorf("unpaused reconcile did not retire the superseded attempt: %+v", got.Operation)
	}
}

// TestExecute_PausedRecordsAndReleasesTheHold: a paused pass reports the
// hold on the attempts the pause is actually holding, so the wait is
// visible on the operation and the deadline-parking step can stop its
// clock. The attempts a pause does not hold report nothing: scale-down
// runs while paused, a committed create finishes the set it started, and
// repair keeps running unless the pause is frozen. A row with no attempt
// has nothing to report. Clearing the pause releases the token.
//
// Cells: instance/Update/InPlace/spec.pauseTrue/before, instance/Update/InPlace/spec.unpause/before, instance/Delete/Drain/spec.pauseTrue/before, instance/Create/CreatePods/spec.pauseTrue/before, instance/Restart/Drain/spec.pauseTrue/before, instance/Restart/Drain/spec.pauseFreeze/before
func TestExecute_PausedRecordsAndReleasesTheHold(t *testing.T) {
	build := func(t *testing.T, waiting string) (workload.ReconcileInput, *testAtomicMutationStore) {
		in := minimalInput(t)
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating,
				Operation: &workload.InstanceOperation{
					ID: "update-0", Type: workload.InstanceOperationUpdate, Step: "InPlace",
					Waiting: waiting,
				}},
			{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseDeleting,
				Operation: &workload.InstanceOperation{
					ID: "delete-1", Type: workload.InstanceOperationDelete, Step: "Drain",
				}},
			{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady},
			{Index: 3, Incarnation: 1, Phase: workload.InstancePhaseCreating,
				Operation: &workload.InstanceOperation{
					ID: "create-3", Type: workload.InstanceOperationCreate, Step: "CreatePods",
				}},
			{Index: 4, Incarnation: 2, Phase: workload.InstancePhaseRestarting,
				Operation: &workload.InstanceOperation{
					ID: "restart-4", Type: workload.InstanceOperationRestart, Step: "Drain",
				}},
		}
		store := installTestAtomicMutationStore(&in)
		in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			return store.apply(ctx, []workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
		}
		return in, store
	}
	deps := workload.Deps{Client: fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()}

	t.Run("paused records the hold", func(t *testing.T) {
		in, store := build(t, "")
		plan := minimalPlan()
		plan.Paused = true
		if _, err := workload.Execute(context.Background(), deps, in, plan, nil,
			workload.SnapshotWithPodsForTest(in, nil), workload.Decision{}); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if got := store.status(0); got == nil || got.Operation.Waiting != workload.WaitingReasonPaused {
			t.Errorf("held attempt = %+v, want Waiting=%s", got, workload.WaitingReasonPaused)
		}
		if got := store.status(1); got == nil || got.Operation.Waiting != "" {
			t.Errorf("Delete attempt = %+v, want no hold (scale-down runs while paused)", got)
		}
		if got := store.status(2); got == nil || got.Operation != nil {
			t.Errorf("idle row = %+v, want untouched", got)
		}
		if got := store.status(3); got == nil || got.Operation.Waiting != "" {
			t.Errorf("Create attempt = %+v, want no hold (it finishes the set it started)", got)
		}
		if got := store.status(4); got == nil || got.Operation.Waiting != "" {
			t.Errorf("Restart attempt = %+v, want no hold (repair runs under a standard pause)", got)
		}
	})

	t.Run("a frozen pause holds the repair too", func(t *testing.T) {
		in, store := build(t, "")
		plan := minimalPlan()
		plan.Paused, plan.PauseFreeze = true, true
		if _, err := workload.Execute(context.Background(), deps, in, plan, nil,
			workload.SnapshotWithPodsForTest(in, nil), workload.Decision{}); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if got := store.status(4); got == nil || got.Operation.Waiting != workload.WaitingReasonPaused {
			t.Errorf("frozen Restart attempt = %+v, want Waiting=%s", got, workload.WaitingReasonPaused)
		}
		if got := store.status(3); got == nil || got.Operation.Waiting != "" {
			t.Errorf("Create attempt under freeze = %+v, want no hold", got)
		}
	})

	t.Run("another authority keeps the row", func(t *testing.T) {
		in, store := build(t, workload.RejectionReasonQuotaExceeded)
		plan := minimalPlan()
		plan.Paused = true
		if _, err := workload.Execute(context.Background(), deps, in, plan, nil,
			workload.SnapshotWithPodsForTest(in, nil), workload.Decision{}); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if got := store.status(0); got == nil || got.Operation.Waiting != workload.RejectionReasonQuotaExceeded {
			t.Errorf("quota-held attempt = %+v, want the quota token kept", got)
		}
	})

	t.Run("unpause releases the hold", func(t *testing.T) {
		in, store := build(t, workload.WaitingReasonPaused)
		if _, err := workload.Execute(context.Background(), deps, in, minimalPlan(), nil,
			workload.SnapshotWithPodsForTest(in, nil), workload.Decision{}); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if got := store.status(0); got == nil || got.Operation.Waiting != "" {
			t.Errorf("released attempt = %+v, want the pause token cleared", got)
		}
	})
}

// TestReconcileGatedDeadlines_PausedHoldParksAndRearms: the pause token
// is an external hold like any other, so the InstanceReadyTimeout clock
// parks for as long as it is set and re-arms with a FULL window once the
// pause authority releases it — the hold does not eat the attempt's
// budget.
//
// Cells: instance/Update/Surge/spec.pauseTrue/before, instance/Update/Surge/spec.unpause/before
func TestReconcileGatedDeadlines_PausedHoldParksAndRearms(t *testing.T) {
	now := time.Now()
	held := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationUpdate,
			Step:     workload.UpdateStepSurge,
			Waiting:  workload.WaitingReasonPaused,
			Deadline: metav1.NewTime(now.Add(3 * time.Minute)),
		},
	}}
	input, store, _ := expireFixture(held)
	if err := workload.ReconcileGatedDeadlines(context.Background(), input, held, nil, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines (held): %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("held Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}

	released := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{
			Type: workload.InstanceOperationUpdate,
			Step: workload.UpdateStepSurge,
		},
	}}
	input, store, _ = expireFixture(released)
	if err := workload.ReconcileGatedDeadlines(context.Background(), input, released, nil, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines (released): %v", err)
	}
	d := (*store)[0].Operation.Deadline
	if d.IsZero() || !d.Time.After(now.Add(25*time.Minute)) {
		t.Errorf("released Deadline %v: want a full window from the unpause", d)
	}
}

// TestEscalation_PausedHoldSkipsStuckPodFastPath: a pod wedged in a
// terminal waiting state under a pause is held by an operator, not
// stuck on a verdict this controller can reach, so the fast path leaves
// the row alone. The same evidence without the hold escalates.
//
// Cells: instance/Update/Surge/spec.pauseTrue/before
func TestEscalation_PausedHoldSkipsStuckPodFastPath(t *testing.T) {
	now := time.Now()
	insts := func(waiting string) []workload.InstanceStatus {
		return []workload.InstanceStatus{{
			Index:    0,
			Phase:    workload.InstancePhaseUpdating,
			PodCount: 1,
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepSurge,
				TargetRevision: "own-engine-badhash",
				Waiting:        waiting,
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		}}
	}
	stuck := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "engine-0-default-1",
				CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)),
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "main",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
				}},
			},
		}
	}

	input, rec := escalationFixture(insts(workload.WaitingReasonPaused))
	input.StuckPodGrace = time.Second
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck()}}); err != nil {
		t.Fatalf("escalation pass (held): %v", err)
	}
	if rec.store[0].Phase != workload.InstancePhaseUpdating {
		t.Errorf("held Phase: got %q want Updating (an operator holds the row)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}

	input, rec = escalationFixture(insts(""))
	input.StuckPodGrace = time.Second
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck()}}); err != nil {
		t.Fatalf("escalation pass (released): %v", err)
	}
	if rec.store[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("released Phase: got %q want Failed", rec.store[0].Phase)
	}

	// The guard is scoped to the pause token, so a row whose surge took
	// the token over to report its source out of rotation is still the
	// pass's business once the pause is cleared.
	input, rec = escalationFixture(insts(workload.WaitingReasonSourceUnrouted))
	input.StuckPodGrace = time.Second
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck()}}); err != nil {
		t.Fatalf("escalation pass (unrouted): %v", err)
	}
	if rec.store[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("unrouted Phase: got %q want Failed (the pause guard must not cover it)", rec.store[0].Phase)
	}
}

// TestReconcile_PausedFinishesACreateItWouldNotRetire: the pause skips a
// committed create only where finishing it would mean replacing it. An
// Instance that was serving before is never retired, so the pass adds
// the member it lost under the same operation even after the target has
// moved; with no target at all there is nothing to retarget to and the
// same holds. Either way the Instance would otherwise stay short of its
// set for the length of the hold.
//
// Cells: instance/Create/CreatePods/spec.pauseTrue/before
func TestReconcile_PausedFinishesACreateItWouldNotRetire(t *testing.T) {
	name := "paused-lost-member"
	pinned := name + "-engine-pinned"
	missing := query.PodName(name, workload.ComponentEngine, 0, "default", 1)

	build := func(t *testing.T, runningRevision string) *restartScaleTestFixture {
		f := newRestartScaleTestFixture(t, name, 1, true, workload.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workload.InstancePhaseCreating, PodCount: 1,
			RunningRevision: runningRevision,
			Operation: &workload.InstanceOperation{
				ID: "create-0", Type: workload.InstanceOperationCreate, Step: "CreatePods",
				TargetRevision: pinned,
			},
		}, []workload.RunnerPlan{{Name: "default", Size: 2}}, enginePod(name, "prod", 0))
		// The gang-member-loss repair owns a partial set under the
		// recreate policy; this is about the Create pass finishing its own.
		f.plan.RestartPolicy = ""
		return f
	}

	t.Run("a serving Instance rebuilding a lost member", func(t *testing.T) {
		// RunningRevision set: the Instance was promoted once, so the
		// retirement rule leaves its attempt in place whatever the target
		// says.
		f := build(t, pinned)
		f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: name + "-engine-edited", Namespace: f.isvc.Namespace,
		}}
		recorder := record.NewFakeRecorder(8)

		if _, err := workload.Reconcile(context.Background(), workload.Deps{
			Client: f.client, APIReader: f.client, Recorder: recorder,
			Expectations: workload.NewExpectations(),
		}, f.input, f.plan, f.target); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if names := podNameSet(t, f.client, f.isvc.Namespace); !names[missing] {
			t.Errorf("paused reconcile left the Instance short of its set; pods = %v", names)
		}
		got := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
		if got == nil || got.Operation == nil || got.Operation.ID != "create-0" ||
			got.Operation.TargetRevision != pinned {
			t.Errorf("paused reconcile replaced the attempt: %+v", got.Operation)
		}
		select {
		case ev := <-recorder.Events:
			t.Errorf("paused reconcile announced %q; finishing a set is not an event", ev)
		default:
		}
	})

	t.Run("no target to retarget to", func(t *testing.T) {
		f := build(t, "")
		if _, err := workload.Reconcile(context.Background(), workload.Deps{
			Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
		}, f.input, f.plan, nil); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if names := podNameSet(t, f.client, f.isvc.Namespace); !names[missing] {
			t.Errorf("paused reconcile with no target left the set half-built; pods = %v", names)
		}
	})
}

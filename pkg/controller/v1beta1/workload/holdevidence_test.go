package workload_test

// Pause freezes repair, not observation. A paused Component runs the
// escalation pass's evidence half alone: the fact-bearing hold
// authorities still record and release their tokens — and still take a
// paused row per the external-hold precedence rule — while every repair
// the action half would decide stays withheld for the length of the
// pause.

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// pausedUpdate is a single-pod Update attempt already carrying the
// operator's pause token — an attempt held at a step boundary, with its
// deadline parked by the adapter.
func pausedUpdate(waiting string) []workload.InstanceStatus {
	return []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workload.InstanceOperation{
			Type:    workload.InstanceOperationUpdate,
			Step:    workload.UpdateStepSurge,
			Waiting: waiting,
		},
	}}
}

// pausedRecreate is the recreate roll's Phase B row under the same
// pause — the shape the gang arm judges, since a surge SOURCE is never
// read against its own group.
func pausedRecreate(waiting string) []workload.InstanceStatus {
	rows := pausedUpdate(waiting)
	rows[0].Operation.Step = "Drain"
	return rows
}

// runHoldEvidence drives the evidence half on its own, the way Execute
// invokes it for a paused Component.
func runHoldEvidence(t *testing.T, input workload.ReconcileInput, plan workload.ComponentPlan, byIdx map[int32][]*corev1.Pod) error {
	t.Helper()
	return workload.ReconcileExternalHoldsForTest(context.Background(), input, plan,
		workload.SnapshotWithPodsForTest(input, byIdx))
}

// TestHoldEvidence_PausedRowRecordsTheSchedulerHold: a pod the scheduler
// cannot place while the Component is paused still gets its hold
// recorded. The scheduler reports a FACT about the Instance and the
// pause reports a decision, so the fact takes the row; the operator sees
// which constraint has no placement instead of a pause token that hides
// it.
//
// Cells: instance/Update/Surge/pod.unschedulable[target]/before, instance/Update/Surge/spec.pauseTrue/before
func TestHoldEvidence_PausedRowRecordsTheSchedulerHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workload.WaitingReasonPaused))

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q (the fact takes the paused row)", got.Operation, workload.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
		t.Errorf("LastFailure: got %+v want the scheduler message %q", got.LastFailure, schedulerMessage)
	}
	if got.Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (the row is queued, not failed)", got.Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestHoldEvidence_PausedRowTakesNoRepairAction: the same row with the
// operator's placement grace long elapsed. Unpaused that is a terminal,
// environment-caused failure; paused it is only a hold, because the half
// that decides repair does not run.
//
// Cells: instance/Update/Surge/pod.unschedulable[target]/before, instance/Update/Surge/spec.pauseTrue/before
func TestHoldEvidence_PausedRowTakesNoRepairAction(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workload.WaitingReasonPaused))
	input.UnschedulableGrace = time.Minute

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase == workload.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want Updating (a pause withholds the escalation)")
	}
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workload.WaitingReasonUnschedulable)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %v want none", rec.blocks)
	}
}

// TestHoldEvidence_PausedRowReleasesTheSchedulerHold: the release runs
// under a pause too. A hold that outlives the placement that ended it
// would keep naming a wait that is over, and the row would report it
// until the operator unpaused.
//
// Cells: instance/Update/Surge/pod.scheduled[target]/before, instance/Update/Surge/spec.pauseTrue/before
func TestHoldEvidence_PausedRowReleasesTheSchedulerHold(t *testing.T) {
	input, rec := escalationFixture(pausedUpdate(workload.WaitingReasonUnschedulable))

	placed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}
	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {placed}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want the scheduler token cleared", got.Operation)
	}
	if got.Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating", got.Phase)
	}
}

// TestHoldEvidence_PausedRowRecordsTheGangHold: the gang arm runs under
// a pause on the same terms. A deterministic name still being collected
// is a fact about the Instance, so it takes the paused row and parks the
// clock on the wait that is real.
//
// Cells: instance/Update/Drain/gang.podGroup[Terminating]/before, instance/Update/Drain/spec.pauseTrue/before
func TestHoldEvidence_PausedRowRecordsTheGangHold(t *testing.T) {
	input, rec := escalationFixture(pausedRecreate(workload.WaitingReasonPaused))
	input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1), nil); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workload.WaitingReasonPodGroupTerminating)
	}
	if got.Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating", got.Phase)
	}
}

// TestHoldEvidence_PausedRowKeepsTheIncumbentFact: precedence between
// two facts is unchanged by the pause. A row already reporting that its
// source is out of rotation keeps that token — first writer wins between
// authorities that both state something about the Instance, and only the
// pause is ordered below them.
//
// Cells: instance/Update/Surge/pod.unschedulable[target]/before, instance/Update/Surge/spec.pauseTrue/before
func TestHoldEvidence_PausedRowKeepsTheIncumbentFact(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workload.WaitingReasonSourceUnrouted))

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonSourceUnrouted {
		t.Errorf("Operation.Waiting: got %+v want %q kept", got.Operation, workload.WaitingReasonSourceUnrouted)
	}
}

package workload_test

// The non-surge update mechanisms — the in-place image patch and the
// recreate roll — reach the passes in this package through three seams:
// what a paused plan still selects, what the escalation pass records
// about a pod it cannot place, and what it makes of the Instance's
// PodGroup. These tests pin each of those for a roll in flight.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// rollInFlight is one Instance with an update already stamped on it, at
// the step the caller names.
func rollInFlight(idx int32, step string, incarnation int64, deadline time.Time) workload.InstanceStatus {
	return workload.InstanceStatus{
		Index:           idx,
		Incarnation:     incarnation,
		Phase:           workload.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "prior-rev",
		Operation: &workload.InstanceOperation{
			ID:             "update-" + step,
			Type:           workload.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: updateTarget().Name,
			Deadline:       metav1.NewTime(deadline),
		},
	}
}

// TestPlan_PauseFreeze_AdvancesTheOpenRollOnly: a freeze suspends repair
// on top of what a standard pause withholds, but neither withholds a
// step already under way. A recreate mid-gap and an in-place patch
// mid-flight are both still selected as continuations — never as fresh
// starts — and clearing the pause adds the fresh start back without
// disturbing either of them.
//
// Cells: instance/Update/Drain/spec.pauseFreeze/before, instance/Update/InPlace/spec.pauseFreeze/before, instance/Update/Drain/spec.unpause/before
func TestPlan_PauseFreeze_AdvancesTheOpenRollOnly(t *testing.T) {
	build := func(t *testing.T) (workload.ReconcileInput, workload.ComponentPlan) {
		t.Helper()
		now := time.Now()
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
			rollInFlight(0, "Drain", 2, now.Add(30*time.Minute)),
			rollInFlight(1, "InPlace", 1, now.Add(30*time.Minute)),
			// Off-target with no attempt open: the fresh start a pause
			// of either kind withholds.
			{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		}
		plan := minimalPlan()
		plan.Replicas = 3
		plan.Instances = append(plan.Instances,
			workload.InstancePlan{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			workload.InstancePlan{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		)
		return in, plan
	}
	// Index 2 is backed, so the truth pass has nothing to correct and the
	// decision is exactly the lifecycle selection under test.
	pods := func(in workload.ReconcileInput) map[int32][]*corev1.Pod {
		return map[int32][]*corev1.Pod{2: {enginePod(in.Key.OwnerName, in.Key.Namespace, 2)}}
	}

	openIndices := func(t *testing.T, d workload.Decision) []int32 {
		t.Helper()
		ua := findAction(d, workload.ActionUpdate)
		if ua == nil {
			t.Fatalf("no Update action selected; got %v", actionKinds(d))
		}
		var out []int32
		for _, item := range ua.Update.Items {
			out = append(out, item.Instance.Index)
		}
		return out
	}

	t.Run("frozen pause drives both open rolls and starts nothing", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused, plan.PauseFreeze = true, true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 2 || got[0] != 0 || got[1] != 1 {
			t.Fatalf("frozen update selection = %v, want the two open rolls [0 1]", got)
		}
		for _, item := range findAction(d, workload.ActionUpdate).Update.Items {
			if item.StartingFresh {
				t.Errorf("index %d = %+v, want a continuation under a freeze", item.Instance.Index, item)
			}
		}
		if d.Escalate {
			t.Errorf("a frozen decision must suspend escalation")
		}
	})

	t.Run("unpause adds the fresh start and leaves the open rolls alone", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 3 {
			t.Fatalf("unpaused update selection = %v, want all three indices", got)
		}
		for _, item := range findAction(d, workload.ActionUpdate).Update.Items {
			fresh := item.Instance.Index == 2
			if item.StartingFresh != fresh {
				t.Errorf("index %d StartingFresh = %v, want %v (the unpause restamps no open roll)",
					item.Instance.Index, item.StartingFresh, fresh)
			}
		}
	})
}

// TestEscalation_UnschedulableRollHoldsThenFailsPastGrace: an update
// whose pod the scheduler cannot place is waiting on cluster capacity.
// Both mechanisms answer it the same way — the hold takes the operation's
// waiting token, which parks the InstanceReadyTimeout clock so a
// shortage of nodes is not charged to the roll, and the scheduler's own
// message reaches LastFailure. Past a configured grace the attempt ends
// environment-caused, so the revision keeps a clean retry ladder.
//
// Cells: instance/Update/InPlace/pod.unschedulable[source]/before, instance/Update/Drain/pod.unschedulable[source]/before
func TestEscalation_UnschedulableRollHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()
	for _, step := range []string{"InPlace", "Drain"} {
		t.Run(step+" held while the grace is unconfigured", func(t *testing.T) {
			input, rec := escalationFixture([]workload.InstanceStatus{
				rollInFlight(0, step, 1, now.Add(30*time.Minute)),
			})
			if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != workload.InstancePhaseUpdating {
				t.Errorf("Phase: got %q want Updating (queued for placement, not failed)", got.Phase)
			}
			if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonUnschedulable {
				t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workload.WaitingReasonUnschedulable)
			}
			if !workload.OperationExternallyHeld(got.Operation) {
				t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
			}
			if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
				t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none", rec.blocks)
			}
		})

		t.Run(step+" failed past the configured grace", func(t *testing.T) {
			input, rec := escalationFixture([]workload.InstanceStatus{
				rollInFlight(0, step, 1, now.Add(30*time.Minute)),
			})
			input.UnschedulableGrace = 10 * time.Minute
			input.ObservedState.UpdateRevision = updateTarget().Name
			if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != workload.InstancePhaseFailed {
				t.Errorf("Phase: got %q want Failed", got.Phase)
			}
			if got.LastFailure == nil || got.LastFailure.Reason != workload.WaitingReasonUnschedulable {
				t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, workload.WaitingReasonUnschedulable)
			}
			if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
				t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
			}
		})
	}
}

// TestEscalation_GangReadingsOnARecreateRow: a gang recreate meets the
// same three PodGroup readings a create does and answers them the same
// way — a name being collected is a wait that resolves itself and parks
// the clock, a name held by another controller ends the roll with the
// operation preserved and no revision blamed, and the scheduler's Failed
// phase is the PodGroup pass's business and leaves the row untouched.
//
// Cells: instance/Update/Drain/gang.podGroup[Terminating]/before, instance/Update/Drain/gang.podGroup[OwnershipConflict]/before, instance/Update/Drain/gang.podGroup[PhaseFailed]/before
func TestEscalation_GangReadingsOnARecreateRow(t *testing.T) {
	now := time.Now()
	recreatingGang := func() []workload.InstanceStatus {
		s := rollInFlight(0, "Drain", 2, now.Add(30*time.Minute))
		s.PodCount = 2
		return []workload.InstanceStatus{s}
	}

	t.Run("terminating parks the recreate", func(t *testing.T) {
		input, rec := escalationFixture(recreatingGang())
		input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)
		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != workload.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonPodGroupTerminating {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workload.WaitingReasonPodGroupTerminating)
		}
		if !workload.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if len(rec.warns) != 0 {
			t.Errorf("WarnInstanceFailed: got %v want none (the wait resolves itself)", rec.warns)
		}
	})

	t.Run("ownership conflict ends the recreate", func(t *testing.T) {
		input, rec := escalationFixture(recreatingGang())
		input.Gangs = gangObservations(0, workload.GangStateOwnershipConflict, gangConflictMessage)
		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != workload.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != workload.PodGroupOwnershipConflictReason {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, workload.PodGroupOwnershipConflictReason)
		}
		if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, "StatefulSet/legacy") {
			t.Errorf("LastFailure.Message: got %+v want the foreign controller named", got.LastFailure)
		}
		if got.Operation == nil {
			t.Errorf("Operation: got none want it preserved (nothing this controller does frees the name)")
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})

	t.Run("the scheduler's failed phase leaves the row alone", func(t *testing.T) {
		input, rec := escalationFixture(recreatingGang())
		input.Gangs = gangObservations(0, workload.GangStateFailed, gangFailedMessage)
		for pass := 0; pass < 2; pass++ {
			if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass %d: %v", pass, err)
			}
			input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), rec.store...)
		}
		got := rec.store[0]
		if got.Phase != workload.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating (the group is reset, not the row)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != "" {
			t.Errorf("Operation.Waiting: got %+v want none (a reset is not a wait)", got.Operation)
		}
		if len(rec.warns) != 0 || len(rec.blocks) != 0 {
			t.Errorf("warns=%v blocks=%v want neither across two passes", rec.warns, rec.blocks)
		}
	})
}

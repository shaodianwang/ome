package workload_test

// A SurgeThenDrain cycle reaches the passes in this package through three
// seams: what a paused or migration-carrying plan still selects, what the
// escalation pass makes of the source's PodGroup and of a pod nobody can
// place, and what it leaves behind for a corrective revision to pick up.
// These tests pin each of those for a surge in flight.

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// surgeSourceRow is the source half of a gang surge at step: Updating,
// pinned to the replacement gang under SurgeIndex, with the target
// revision the attempt is committed to.
func surgeSourceRow(now time.Time, step string, markerIndex int32) []workload.InstanceStatus {
	rows := gangSurgePair(now, markerIndex, workload.UpdateStepGangSurgeTarget)
	rows[0].Operation.Step = step
	return rows
}

// TestEscalation_GangReadingsOnASurgeSource: a gang-surge source is never
// judged on its own group. Its serving pods already belong to that group;
// the attempt in flight builds members under the SURGE index, which
// carries the reading and the hold that matter. Holding or ending the
// source on its own group would act on the wrong half of the pair — so
// whichever reading arrives, the source keeps its phase, its step and an
// unparked clock at both surge steps.
//
// Re-ensuring the name and withholding the members it would place there
// is the top-level PodGroup pass's business, not this row's.
//
// Cells: instance/Update/Surge/gang.podGroup[Terminating]/before, instance/Update/Surge/gang.podGroup[OwnershipConflict]/before, instance/Update/Surge/gang.podGroup[PhaseFailed]/before, instance/Update/SurgeDrain/gang.podGroup[Terminating]/before, instance/Update/SurgeDrain/gang.podGroup[OwnershipConflict]/before, instance/Update/SurgeDrain/gang.podGroup[PhaseFailed]/before
func TestEscalation_GangReadingsOnASurgeSource(t *testing.T) {
	now := time.Now()
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{
		{Index: 0, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	for _, step := range []string{workload.UpdateStepSurge, workload.UpdateStepSurgeDrain} {
		for _, tc := range []struct {
			name    string
			state   workload.GangState
			message string
		}{
			{"the name is being collected", workload.GangStateTerminating, gangTerminatingMessage},
			{"the name is held by another controller", workload.GangStateOwnershipConflict, gangConflictMessage},
			{"the scheduler failed the group", workload.GangStateFailed, gangFailedMessage},
		} {
			t.Run(step+"/"+tc.name, func(t *testing.T) {
				input, rec := escalationFixture(surgeSourceRow(now, step, 1))
				input.Gangs = gangObservations(0, tc.state, tc.message)

				if err := runEscalationPass(t, workload.Deps{}, input, plan, map[int32][]*corev1.Pod{
					0: {servingPod("engine-0-default-0"), servingPod("engine-0-default-1")},
				}); err != nil {
					t.Fatalf("escalation pass: %v", err)
				}

				got := rec.store[0]
				if got.Phase != workload.InstancePhaseUpdating {
					t.Errorf("Phase: got %q want Updating (the source's own group does not judge it)", got.Phase)
				}
				if got.Operation == nil || got.Operation.Step != step {
					t.Errorf("Operation: got %+v want the surge still at %s", got.Operation, step)
				}
				if got.Operation == nil || got.Operation.Waiting != "" {
					t.Errorf("Operation.Waiting: got %+v want none; the replacement's reading owns the hold", got.Operation)
				}
				if len(rec.warns) != 0 || len(rec.blocks) != 0 {
					t.Errorf("warns=%v blocks=%v want neither", rec.warns, rec.blocks)
				}
			})
		}
	}
}

// TestEscalation_UnschedulableDrainHoldsThenFailsPastGrace: past the
// surge step the source is already out of rotation, so a replacement the
// scheduler cannot place leaves the Instance serving nothing — and it is
// still a wait on cluster capacity, not a fault of the roll. The hold
// takes the operation's waiting token, which parks the shared deadline so
// the drain is not failed for a shortage of nodes, and the scheduler's
// message reaches LastFailure. Past a configured grace the attempt ends
// environment-caused, with the revision's retry ladder untouched.
//
// Cells: instance/Update/SurgeDrain/pod.unschedulable[source]/before
func TestEscalation_UnschedulableDrainHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()

	t.Run("held while the grace is unconfigured", func(t *testing.T) {
		input, rec := escalationFixture([]workload.InstanceStatus{
			rollInFlight(0, workload.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
		})
		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-1", now.Add(-5*time.Minute))}}); err != nil {
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

	t.Run("failed past the configured grace", func(t *testing.T) {
		input, rec := escalationFixture([]workload.InstanceStatus{
			rollInFlight(0, workload.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
		})
		input.UnschedulableGrace = 10 * time.Minute
		input.ObservedState.UpdateRevision = updateTarget().Name
		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-1", now.Add(-time.Hour))}}); err != nil {
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

// TestPlan_PauseFreeze_AdvancesTheOpenDrain: a drain step is past the
// point where a hold is free — the source is already out of rotation and
// the replacement is the Instance's only capacity — so neither a standard
// pause nor a freeze withholds it. It is selected as a continuation, and
// clearing the pause adds back the fresh start it was withholding without
// disturbing the drain.
//
// Cells: instance/Update/SurgeDrain/spec.pauseFreeze/before, instance/Update/SurgeDrain/spec.unpause/before
func TestPlan_PauseFreeze_AdvancesTheOpenDrain(t *testing.T) {
	build := func(t *testing.T) (workload.ReconcileInput, workload.ComponentPlan) {
		t.Helper()
		now := time.Now()
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
			rollInFlight(0, workload.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
			// Off-target with no attempt open: the fresh start a pause of
			// either kind withholds.
			{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
		}
		plan := minimalPlan()
		plan.Replicas = 2
		plan.Instances = append(plan.Instances,
			workload.InstancePlan{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}})
		return in, plan
	}
	pods := func(in workload.ReconcileInput) map[int32][]*corev1.Pod {
		return map[int32][]*corev1.Pod{1: {enginePod(in.Key.OwnerName, in.Key.Namespace, 1)}}
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

	t.Run("frozen pause drives the open drain and starts nothing", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused, plan.PauseFreeze = true, true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 1 || got[0] != 0 {
			t.Fatalf("frozen update selection = %v, want the open drain [0]", got)
		}
		if findAction(d, workload.ActionUpdate).Update.Items[0].StartingFresh {
			t.Errorf("the open drain was restamped as a fresh attempt under a freeze")
		}
		if d.Escalate {
			t.Errorf("a frozen decision must suspend escalation")
		}
	})

	t.Run("unpause adds the fresh start and leaves the drain alone", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 2 {
			t.Fatalf("unpaused update selection = %v, want both indices", got)
		}
		for _, item := range findAction(d, workload.ActionUpdate).Update.Items {
			fresh := item.Instance.Index == 1
			if item.StartingFresh != fresh {
				t.Errorf("index %d StartingFresh = %v, want %v (the unpause restamps no open roll)",
					item.Instance.Index, item.StartingFresh, fresh)
			}
		}
	})
}

// TestPlan_MigrationRequestWaitsForASurgeCycleToResolve: a fresh
// migration adds one more surge Instance, so it is not driven while a
// SurgeThenDrain cycle is already counted — at either of its steps. The
// request waits in its record rather than stacking a second lot of extra
// capacity on the Component.
//
// Cells: instance/Update/Surge/spec.migrateRequest/before, instance/Update/SurgeDrain/spec.migrateRequest/before
func TestPlan_MigrationRequestWaitsForASurgeCycleToResolve(t *testing.T) {
	for _, step := range []string{workload.UpdateStepSurge, workload.UpdateStepSurgeDrain} {
		t.Run(step, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = surgeSourceRow(time.Now(), step, 1)
			in.ObservedState.Migrations = []workload.MigrationRecord{{
				RequestUUID: "request-a",
				Trigger:     workload.MigrationTriggerManual,
				Phase:       workload.MigrationPhaseAccepted,
			}}
			plan, err := workload.BuildPlan(workload.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			plan.MigrationMode = workload.MigrationModeSurge

			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: gangPairTarget,
			}}
			sourcePod := enginePod("llama-70b", "prod", 0)
			surgePod := enginePod("llama-70b", "prod", 1)
			surgePod.Labels[query.LabelRevisionHash] = query.RevisionFromName(gangPairTarget).Hash()
			d := planTargetOrFail(t, in, plan, target,
				planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}, 1: {surgePod}}))

			if a := findAction(d, workload.ActionMigrate); a != nil {
				t.Fatalf("a migration was driven while the surge is in flight: %+v", a.Migration)
			}
		})
	}
}

// TestEscalation_CorrectiveRevisionDoesNotRideTheFailingSurge: a
// revision edit that lands in the window where the attempt is being ended
// — by its operation deadline or by the stuck-pod fast path — does not
// retarget it. The escalation commits against the revision the surge was
// pinned to, so the blame lands where the evidence is, and the row that
// comes out of it is Failed. From there the corrective revision is a
// different subject: the Failed row owns the decision to re-drive, and
// what it drives is a fresh attempt rather than a continuation.
//
// Cells: instance/Update/Surge/spec.revision/boundary@T-surge-deadline, instance/Update/Surge/spec.revision/after@T-surge-deadline, instance/Update/Surge/spec.revision/boundary@T-surge-stuck, instance/Update/Surge/spec.revision/after@T-surge-stuck
func TestEscalation_CorrectiveRevisionDoesNotRideTheFailingSurge(t *testing.T) {
	const pinned = "llama-70b-engine-pinnedrv"
	now := time.Now()

	// failingSurge is a single-pod surge committed to pinned, with the
	// operator's corrective revision already observed.
	failingSurge := func(deadline time.Time) []workload.InstanceStatus {
		return []workload.InstanceStatus{{
			Index:           0,
			Incarnation:     1,
			Phase:           workload.InstancePhaseUpdating,
			PodCount:        1,
			RunningRevision: "llama-70b-engine-priorrev",
			TargetRevision:  pinned,
			Operation: &workload.InstanceOperation{
				ID:             "surge-0",
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepSurge,
				TargetRevision: pinned,
				StartedAt:      metav1.NewTime(now.Add(-time.Hour)),
				Deadline:       metav1.NewTime(deadline),
			},
		}}
	}

	for _, tc := range []struct {
		name     string
		deadline time.Time
		pods     map[int32][]*corev1.Pod
		grace    time.Duration
	}{
		{
			name:     "the operation deadline ends it",
			deadline: now.Add(-time.Minute),
			pods:     map[int32][]*corev1.Pod{0: {waitingPod("engine-0-default-1", "ContainerCreating", "node-a", now.Add(-time.Hour))}},
		},
		{
			name:     "the stuck-pod fast path ends it",
			deadline: now.Add(30 * time.Minute),
			pods:     map[int32][]*corev1.Pod{0: {waitingPod("engine-0-default-1", "ImagePullBackOff", "node-a", now.Add(-time.Hour))}},
			grace:    time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Boundary: the corrective revision is observed on the very
			// pass that ends the attempt.
			input, rec := escalationFixture(failingSurge(tc.deadline))
			input.StuckPodGrace = tc.grace
			input.ObservedState.UpdateRevision = updateTarget().Name
			if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1), tc.pods); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != workload.InstancePhaseFailed {
				t.Fatalf("Phase: got %q want Failed (the stamp is what this window commits)", got.Phase)
			}
			for _, b := range rec.blocks {
				if b.TargetRevision != pinned {
					t.Errorf("RetryBlock subject: got %q want the pinned %q (the edit is not what failed)",
						b.TargetRevision, pinned)
				}
			}

			// After: the row is Failed and the corrective revision drives a
			// fresh attempt, not the surge that just ended.
			in := minimalInput(t)
			forbidMutations(t, &in)
			// The row the escalation just wrote, carried forward.
			failed := rec.store[0]
			failed.RunningRevision = "prior-rev"
			in.ObservedState.InstanceStatuses = []workload.InstanceStatus{failed}
			d := planTargetOrFail(t, in, minimalPlan(), updateTarget(),
				planSnapshot(in, map[int32][]*corev1.Pod{0: {enginePod(in.Key.OwnerName, in.Key.Namespace, 0)}}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("the corrective revision drove nothing from Failed; got %v", actionKinds(d))
			}
			if !ua.Update.Items[0].StartingFresh {
				t.Errorf("update item: got %+v want a fresh attempt, not the ended surge continued", ua.Update.Items[0])
			}
		})
	}
}

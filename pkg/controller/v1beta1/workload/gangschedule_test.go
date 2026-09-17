package workload_test

// Gang contract: the Instance's PodGroup says whether the deterministic
// name is usable by this owner and whether the group has to be rebuilt.
// A name being collected parks the InstanceReadyTimeout clock; a name
// held by another controller ends the attempt, because nothing this
// controller does frees it; a group the scheduler has failed ends
// nothing, because the PodGroup pass resets it.
//
// Whether a gang can be PLACED is not read off the PodGroup: the gang
// scheduler writes that verdict onto the member pods, where the
// pod-level scheduler hold picks it up.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

const (
	gangFailedMessage      = "PodGroup svc-engine-0 reported phase Failed: 2 of 2 members failed"
	gangConflictMessage    = "PodGroup svc-engine-0 is controlled by StatefulSet/legacy, not by this owner"
	gangTerminatingMessage = "PodGroup svc-engine-0 is terminating; the gang is announced again once it is collected"
	// coschedulingMessage is the shape the gang scheduler writes onto a
	// member it rejects because the whole gang cannot be placed.
	coschedulingMessage = "0/3 nodes are available: cannot find enough sibling pods for PodGroup svc-engine-0"
)

// gangObservations wires one Instance's PodGroup classification into the
// reconcile input, the way the PodGroup pass does before the dispatcher.
func gangObservations(idx int32, state workload.GangState, message string) *workload.GangObservations {
	g := workload.NewGangObservations()
	g.Record(idx, workload.GangObservation{Name: "svc-engine-0", State: state, Message: message})
	return g
}

// creatingGang is a two-pod Create attempt with a live deadline — the
// shape a first gang rollout presents while the scheduler decides.
func creatingGang(deadline time.Time) []workload.InstanceStatus {
	return []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseCreating,
		PodCount: 2,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationCreate,
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// pendingGangPods are gang members that exist but are not yet placed,
// with no verdict of their own — so only the PodGroup can explain why
// the gang is not starting.
func pendingGangPods() map[int32][]*corev1.Pod {
	return map[int32][]*corev1.Pod{0: {
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
			Status: corev1.PodStatus{Phase: corev1.PodPending}},
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-worker-0"},
			Status: corev1.PodStatus{Phase: corev1.PodPending}},
	}}
}

// TestEscalation_GangTerminatingParksWithoutEscalating: a name still
// held by an object being collected is the one gang wait, and it
// resolves itself — so it records the token and the PodGroup's message
// and never escalates, however long it lasts.
//
// Cells: instance/Create/CreatePods/gang.podGroup[Terminating]/before
func TestEscalation_GangTerminatingParksWithoutEscalating(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)
	input.UnschedulableGrace = time.Minute

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the predecessor object is still being collected)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workload.WaitingReasonPodGroupTerminating)
	}
	if got.LastFailure == nil || got.LastFailure.Message != gangTerminatingMessage {
		t.Errorf("LastFailure: got %+v want the PodGroup message", got.LastFailure)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_GangHoldIsEdgeTriggered: a gang waiting on a name for
// hours re-derives the same token, message and moment every pass, so
// only the first pass writes.
//
// Cells: instance/Create/CreatePods/gang.podGroup[Terminating]/before
func TestEscalation_GangHoldIsEdgeTriggered(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *workload.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}

	for pass := 0; pass < 3; pass++ {
		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across three passes: got %d want 1 (edge-triggered)", writes)
	}
}

// TestEscalation_GangHoldClearedOnceCollected: with the old object gone
// the ensure pass announces the gang again, so the token is released by
// the authority that took it and the deadline clock restarts.
//
// Cells: instance/Create/CreatePods/gang.podGroup[Deleted]/before
func TestEscalation_GangHoldClearedOnceCollected(t *testing.T) {
	now := time.Now()
	insts := creatingGang(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = workload.WaitingReasonPodGroupTerminating
	input, rec := escalationFixture(insts)
	// The old object is gone, which is what an absent reading means.
	input.Gangs = gangObservations(0, workload.GangStateNone, "")

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want cleared", got)
	}
	if rec.store[0].Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating", rec.store[0].Phase)
	}
}

// TestEscalation_GangHoldTakesOverFromClearedPodToken: the pod-level
// hold ends and the gang's name goes into collection in the SAME pass.
// Eligibility is judged on the fresh row, so the gang token lands
// immediately — refusing on the pass-start copy would leave the row
// unheld and restart a whole InstanceReadyTimeout window next pass.
//
// Cells: instance/Create/CreatePods/gang.podGroup[Terminating]/before
func TestEscalation_GangHoldTakesOverFromClearedPodToken(t *testing.T) {
	now := time.Now()
	insts := creatingGang(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = workload.WaitingReasonUnschedulable
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)

	// Every member is placed now, so the pod-level hold releases its
	// token on this same pass.
	scheduled := map[int32][]*corev1.Pod{0: {
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
			Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			}}},
	}}
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), scheduled); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q on the same pass the pod token cleared",
			got.Operation, workload.WaitingReasonPodGroupTerminating)
	}
	if !workload.OperationExternallyHeld(got.Operation) {
		t.Errorf("Operation: got %+v want still externally held so the deadline stays parked", got.Operation)
	}
}

// TestEscalation_GangOwnershipConflictFailsOnlyItsRow: a deterministic
// name held by another controller is terminal for the Instance that
// wanted it and invisible to every other Instance in the same pass.
//
// Cells: instance/Create/CreatePods/gang.podGroup[OwnershipConflict]/before
func TestEscalation_GangOwnershipConflictFailsOnlyItsRow(t *testing.T) {
	now := time.Now()
	insts := append(creatingGang(now.Add(30*time.Minute)), workload.InstanceStatus{
		Index:    1,
		Phase:    workload.InstancePhaseCreating,
		PodCount: 2,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	})
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(0, workload.GangStateOwnershipConflict, gangConflictMessage)
	plan := singleInstancePlan(0, 2)
	plan.Instances = append(plan.Instances, workload.InstancePlan{
		Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}},
	})

	pods := pendingGangPods()
	pods[1] = pods[0]
	if err := runEscalationPass(t, workload.Deps{}, input, plan, pods); err != nil {
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
	if rec.store[1].Phase != workload.InstancePhaseCreating {
		t.Errorf("sibling Phase: got %q want Creating (the conflict is scoped to one row)", rec.store[1].Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
}

// TestEscalation_GangHoldKeepsOffTeardowns: DeleteBatch owns a teardown's
// progress and may pace it for longer than any wait, and a gang on its
// way out has nothing left to announce.
//
// Cells: instance/Delete/Drain/gang.podGroup[Terminating]/before
func TestEscalation_GangHoldKeepsOffTeardowns(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseDeleting,
		PodCount: 2,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationDelete,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseDeleting {
		t.Errorf("Phase: got %q want Deleting", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want none (DeleteBatch owns the teardown)", got.Operation)
	}
}

// TestEscalation_GangUnschedulableReachesUsOnMemberPods: the gang
// scheduler reports an unplaceable gang by rejecting its MEMBERS, so the
// Instance is held on the pod-level scheduler token with the
// coscheduling explanation preserved — nothing has to read it off the
// PodGroup, whose status cannot express it.
//
// Cells: instance/Create/CreatePods/pod.unschedulable[source]/before
func TestEscalation_GangUnschedulableReachesUsOnMemberPods(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = workload.NewGangObservations()

	rejected := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             workload.WaitingReasonUnschedulable,
			Message:            coschedulingMessage,
			LastTransitionTime: metav1.NewTime(now.Add(-5 * time.Minute)),
		}}},
	}
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {rejected}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workload.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != coschedulingMessage {
		t.Errorf("LastFailure: got %+v want the gang scheduler's own explanation", got.LastFailure)
	}
	if got.Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (queued for placement, not failed)", got.Phase)
	}
}

// TestReconcileGatedDeadlines_ParksGangSurgeSourceOnMemberRejection: a
// gang surge creates its members under the SURGE index while the
// governing operation and its deadline live on the SOURCE, so a member
// the gang scheduler rejected parks the source's clock too.
//
// Cells: instance/Update/GangSurgeTarget/pod.unschedulable[target]/before
func TestReconcileGatedDeadlines_ParksGangSurgeSourceOnMemberRejection(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []workload.InstanceStatus{
		{
			Index: 0,
			Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{
				Type:       workload.InstanceOperationUpdate,
				Step:       workload.UpdateStepSurge,
				SurgeIndex: &surge,
				Deadline:   metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
		{
			Index: 1,
			Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				Type:    workload.InstanceOperationUpdate,
				Step:    workload.UpdateStepGangSurgeTarget,
				Waiting: workload.WaitingReasonUnschedulable,
			},
		},
	}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source Deadline: got %v want zero (parked on the surge row's hold)", (*store)[0].Operation.Deadline)
	}
}

// TestReconcileGatedDeadlines_ParksGangTerminatingSurgeSource: the same
// SurgeIndex hop for the gang's own wait — a surge whose PodGroup name
// is still occupied must not burn the source's clock.
//
// Cells: instance/Update/GangSurgeTarget/gang.podGroup[Terminating]/before
func TestReconcileGatedDeadlines_ParksGangTerminatingSurgeSource(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []workload.InstanceStatus{
		{
			Index: 0,
			Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{
				Type:       workload.InstanceOperationUpdate,
				Step:       workload.UpdateStepSurge,
				SurgeIndex: &surge,
				Deadline:   metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
		{
			Index: 1,
			Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				Type:    workload.InstanceOperationUpdate,
				Step:    workload.UpdateStepGangSurgeTarget,
				Waiting: workload.WaitingReasonPodGroupTerminating,
			},
		},
	}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source Deadline: got %v want zero (parked on the surge row's gang hold)", (*store)[0].Operation.Deadline)
	}
}

// TestEscalation_GangFailedNeitherHoldsNorEnds: a failed group is
// recoverable — the PodGroup pass deletes it and builds a fresh one — so
// the row takes no token, keeps its phase, and announces nothing. Ending
// the attempt here would fail it again on every rebuild, because the
// replacement is created under the same deterministic name.
//
// Cells: instance/Create/CreatePods/gang.podGroup[PhaseFailed]/before
func TestEscalation_GangFailedNeitherHoldsNorEnds(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, workload.GangStateFailed, gangFailedMessage)

	for pass := 0; pass < 2; pass++ {
		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), rec.store...)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the group is reset, not the row)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want none (a reset is not a wait)", got.Operation)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none across two passes", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
	// The deadline the broad backstop relies on must keep running: a reset
	// loop that never converges still has to end somewhere.
	if workload.OperationExternallyHeld(got.Operation) {
		t.Errorf("Operation: got %+v want not externally held", got.Operation)
	}
}

// TestEscalation_GangTerminalKeepsOffRowsOwnedElsewhere: a teardown
// belongs to DeleteBatch and a migration to its record, and both stay
// planned while they run — so a foreign-owned PodGroup name reaches
// those rows and must not flip a phase out from under the machine
// driving them.
//
// Cells: instance/Delete/Drain/gang.podGroup[OwnershipConflict]/before, instance/Migrate/CreateSurge/gang.podGroup[OwnershipConflict]/before
func TestEscalation_GangTerminalKeepsOffRowsOwnedElsewhere(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase workload.InstancePhase
		op    *workload.InstanceOperation
	}{
		{"teardown", workload.InstancePhaseDeleting, &workload.InstanceOperation{
			Type: workload.InstanceOperationDelete, Step: "Drain"}},
		{"migration", workload.InstancePhaseMigrating, &workload.InstanceOperation{
			Type: workload.InstanceOperationMigrate, Step: "CreateSurge"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			tc.op.Deadline = metav1.NewTime(now.Add(-time.Hour))
			insts := []workload.InstanceStatus{{
				Index: 0, Phase: tc.phase, PodCount: 2, Operation: tc.op,
			}}
			input, rec := escalationFixture(insts)
			input.Gangs = gangObservations(0, workload.GangStateOwnershipConflict, gangConflictMessage)

			if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			got := rec.store[0]
			if got.Phase != tc.phase {
				t.Errorf("Phase: got %q want %q (another machine owns this row)", got.Phase, tc.phase)
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
		})
	}
}

// TestEscalation_GangReadingSkipsSurgeSource: a gang-surge SOURCE is
// serving its own group while the attempt in flight builds members under
// the SURGE index. Its own group going into collection says nothing
// about that attempt, so the source is untouched and only the surge row
// carries a wait.
//
// Cells: instance/Update/GangSurgeTarget/gang.podGroup[Terminating]/before
func TestEscalation_GangReadingSkipsSurgeSource(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []workload.InstanceStatus{
		{
			Index: 0, Phase: workload.InstancePhaseUpdating, PodCount: 2,
			Operation: &workload.InstanceOperation{
				Type:       workload.InstanceOperationUpdate,
				Step:       workload.UpdateStepSurge,
				SurgeIndex: &surge,
				Deadline:   metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
		{
			Index: 1, Phase: workload.InstancePhaseCreating, PodCount: 2,
			Operation: &workload.InstanceOperation{
				Type:     workload.InstanceOperationUpdate,
				Step:     workload.UpdateStepGangSurgeTarget,
				Deadline: metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
	}
	input, rec := escalationFixture(insts)
	g := workload.NewGangObservations()
	g.Record(0, workload.GangObservation{Name: "svc-engine-0", State: workload.GangStateTerminating, Message: gangTerminatingMessage})
	g.Record(1, workload.GangObservation{Name: "svc-engine-1", State: workload.GangStateTerminating, Message: gangTerminatingMessage})
	input.Gangs = g
	plan := singleInstancePlan(0, 2)
	plan.Instances = append(plan.Instances, workload.InstancePlan{
		Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}},
	})

	if err := runEscalationPass(t, workload.Deps{}, input, plan, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("source Operation.Waiting: got %+v want none (its replacement carries the wait)", got)
	}
	if got := rec.store[1].Operation; got == nil || got.Waiting != workload.WaitingReasonPodGroupTerminating {
		t.Errorf("surge Operation.Waiting: got %+v want %q", got, workload.WaitingReasonPodGroupTerminating)
	}
}

// TestEscalation_PodHoldWinsOverGangHold: a row whose members the
// scheduler refuses AND whose group name is being collected reports the
// pod hold. Both waits are live and the token is free, so the order the
// marks run in decides — and it favours the wait that can end: the
// collection resolves itself with nobody acting, while only the pod hold
// carries an operator-configured grace.
//
// Cells: instance/Create/CreatePods/pod.unschedulable[source]/before
func TestEscalation_PodHoldWinsOverGangHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)
	input.UnschedulableGrace = 10 * time.Minute

	rejected := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             workload.WaitingReasonUnschedulable,
			Message:            coschedulingMessage,
			LastTransitionTime: metav1.NewTime(now.Add(-time.Minute)),
		}}},
	}
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {rejected}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q, the wait a grace can end",
			got.Operation, workload.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != coschedulingMessage {
		t.Errorf("LastFailure: got %+v want the scheduler's own explanation", got.LastFailure)
	}
	if !workload.OperationExternallyHeld(got.Operation) {
		t.Errorf("Operation: got %+v want externally held so the deadline stays parked", got.Operation)
	}
	if got.Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (inside the grace)", got.Phase)
	}
}

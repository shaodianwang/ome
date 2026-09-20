package workload_test

// A migration pair is owned by its record: the record's Deadline is the
// timeout authority for both rows, and the escalation pass is the place
// that has to respect it. These tests pin the two readings that reach a
// pinned row from this package — the Instance operation deadline, which
// is stamped but never consumed, and the Instance's PodGroup.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// TestEscalation_MigrationPairIgnoresTheInstanceOperationDeadline: both
// rows of a pair carry an operation with a deadline, and both deadlines
// are long past here. Neither ends anything: the fast and the broad
// escalation paths skip the Migrate pin, because failing a healthy
// serving source on a clock the migration does not own would cost
// capacity for a move the record is still driving.
//
// Cells: instance/Migrate/CreateSurge/timer.operationDeadline/before, instance/Migrate/SurgeTarget/timer.operationDeadline/before
func TestEscalation_MigrationPairIgnoresTheInstanceOperationDeadline(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(migrationPair(now.Add(-time.Hour)))
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{
		{Index: 0, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		{Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
	}}

	// The source is serving and the replacement has not come up: exactly
	// the shape the broad backstop would end on any unpinned row.
	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-1-default-0"},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	if err := runEscalationPass(t, workload.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-default-0")},
		1: {pending},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	for _, s := range rec.store {
		if s.Phase == workload.InstancePhaseFailed {
			t.Errorf("instance %d: got Failed want the record left in charge of the move", s.Index)
		}
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// TestEscalation_GangReadingsOnAMigrationPair: a pinned row takes no gang
// waiting token and receives no terminal verdict, whichever reading
// arrives — the record's Deadline is the one clock that ends the move, so
// a token here would park a deadline nothing consumes and a verdict here
// would end a row the record still owns. Withholding the members while
// the name is unusable, and resetting a failed group, are the PodGroup
// pass's business rather than the row's.
//
// Cells: instance/Migrate/CreateSurge/gang.podGroup[PhaseFailed]/before, instance/Migrate/SurgeTarget/gang.podGroup[Terminating]/before, instance/Migrate/SurgeTarget/gang.podGroup[OwnershipConflict]/before, instance/Migrate/SurgeTarget/gang.podGroup[PhaseFailed]/before
func TestEscalation_GangReadingsOnAMigrationPair(t *testing.T) {
	now := time.Now()
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{
		{Index: 0, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	for _, tc := range []struct {
		name string
		// row is the pair index the reading lands on: 0 is the pinned
		// source, 1 the replacement.
		row     int32
		state   workload.GangState
		message string
	}{
		{"source group failed", 0, workload.GangStateFailed, gangFailedMessage},
		{"replacement name being collected", 1, workload.GangStateTerminating, gangTerminatingMessage},
		{"replacement name held by another controller", 1, workload.GangStateOwnershipConflict, gangConflictMessage},
		{"replacement group failed", 1, workload.GangStateFailed, gangFailedMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture(migrationPair(now.Add(30 * time.Minute)))
			input.Gangs = gangObservations(tc.row, tc.state, tc.message)

			if err := runEscalationPass(t, workload.Deps{}, input, plan, map[int32][]*corev1.Pod{
				0: {servingPod("engine-0-default-0")},
			}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			for _, s := range rec.store {
				if s.Phase == workload.InstancePhaseFailed {
					t.Errorf("instance %d: got Failed want the record left in charge of the move", s.Index)
				}
				if s.Operation == nil || s.Operation.Waiting != "" {
					t.Errorf("instance %d Operation: got %+v want no waiting token on a pinned row", s.Index, s.Operation)
				}
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none", rec.blocks)
			}
		})
	}
}

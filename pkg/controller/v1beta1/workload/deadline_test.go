package workload_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// expireFixture wires a ReconcileInput whose MutateInstance applies the
// callback to the local insts slice (mirroring the per-Instance status
// the adapter would persist) and whose WarnInstanceFailed records the
// last event + a fire count. Returns the input and pointers the test
// asserts on.
func expireFixture(insts []workload.InstanceStatus) (workload.ReconcileInput, *[]workload.InstanceStatus, *struct {
	idx    int32
	reason string
	count  int
}) {
	store := append([]workload.InstanceStatus(nil), insts...)
	var event struct {
		idx    int32
		reason string
		count  int
	}
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			for i := range store {
				if store[i].Index == idx {
					mutate(&store[i])
					return nil
				}
			}
			return nil
		},
		WarnInstanceFailed: func(idx int32, _, reason string) {
			event.idx = idx
			event.reason = reason
			event.count++
		},
	}
	return input, &store, &event
}

// TestExpireOperations_ExpiredDeadlineFailsInstance pins the core
// contract: a transient-phase Instance whose Operation.Deadline is in
// the past flips to Phase=Failed via MutateInstance, fires
// WarnInstanceFailed once, and records a DeadlineExceeded LastFailure.
func TestExpireOperations_ExpiredDeadlineFailsInstance(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationUpdate,
			Step:     "Surge",
			Deadline: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := workload.ExpireOperationsIfDeadlinePassed(context.Background(), input, insts); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	got := (*store)[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != workload.DeadlineExceededReason {
		t.Errorf("LastFailure: got %+v want Reason=%s", got.LastFailure, workload.DeadlineExceededReason)
	}
	// Operation preserved so operators see what was in flight.
	if got.Operation == nil {
		t.Errorf("Operation should be preserved on the failed Instance")
	}
	if event.count != 1 {
		t.Errorf("event count: got %d want 1", event.count)
	}
}

// TestExpireOperations_NotYetExpiredUntouched pins the negative case: a
// transient-phase Instance whose Deadline is still in the future is left
// alone (no Phase change, no event).
func TestExpireOperations_NotYetExpiredUntouched(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationUpdate,
			Step:     "Surge",
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := workload.ExpireOperationsIfDeadlinePassed(context.Background(), input, insts); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	if (*store)[0].Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (untouched)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0 (not yet expired)", event.count)
	}
}

// TestExpireOperations_ZeroDeadlineNeverExpires pins that an unset
// (zero) Deadline is treated as "never expires" so an Instance whose
// per-op writer didn't stamp a deadline can't accidentally trip.
func TestExpireOperations_ZeroDeadlineNeverExpires(t *testing.T) {
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type: workload.InstanceOperationCreate,
			Step: "Create",
			// Deadline left as the metav1.Time zero value.
		},
	}}
	input, store, event := expireFixture(insts)

	if err := workload.ExpireOperationsIfDeadlinePassed(context.Background(), input, insts); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	if (*store)[0].Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (zero deadline never expires)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}

// TestExpireOperations_AlreadyFailedNoOp pins idempotency: an already-
// Failed Instance must not re-fire the event even with an expired
// deadline still in front of it.
func TestExpireOperations_AlreadyFailedNoOp(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseFailed,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationUpdate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
	}}
	mutated := false
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
			mutated = true
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {
			t.Errorf("WarnInstanceFailed must not fire on an already-Failed Instance")
		},
	}

	if err := workload.ExpireOperationsIfDeadlinePassed(context.Background(), input, insts); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}
	if mutated {
		t.Errorf("MutateInstance must not be called for an already-Failed Instance")
	}
}

// TestExpireOperations_TerminalPhaseUntouched pins that Instances in
// terminal / non-recovery phases (Ready, Deleting) are skipped even with
// an expired deadline — the timeout only bounds in-flight transient ops.
func TestExpireOperations_TerminalPhaseUntouched(t *testing.T) {
	now := time.Now()
	expired := metav1.NewTime(now.Add(-1 * time.Minute))
	for _, phase := range []workload.InstancePhase{workload.InstancePhaseReady, workload.InstancePhaseDeleting} {
		insts := []workload.InstanceStatus{{
			Index:     0,
			Phase:     phase,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationDelete, Deadline: expired},
		}}
		input, store, event := expireFixture(insts)
		if err := workload.ExpireOperationsIfDeadlinePassed(context.Background(), input, insts); err != nil {
			t.Fatalf("ExpireOperationsIfDeadlinePassed(%s): %v", phase, err)
		}
		if (*store)[0].Phase != phase {
			t.Errorf("phase %s: got %q want untouched", phase, (*store)[0].Phase)
		}
		if event.count != 0 {
			t.Errorf("phase %s: event count got %d want 0", phase, event.count)
		}
	}
}

// TestExpireOperations_NoOperationUntouched pins that an Instance with no
// in-flight Operation is skipped (nothing to time out).
func TestExpireOperations_NoOperationUntouched(t *testing.T) {
	insts := []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseUpdating}}
	input, store, event := expireFixture(insts)
	if err := workload.ExpireOperationsIfDeadlinePassed(context.Background(), input, insts); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}
	if (*store)[0].Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (no operation)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}

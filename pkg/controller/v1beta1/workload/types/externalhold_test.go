package types_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	types "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// holdFixture wires a ReconcileInput whose MutateInstance applies the
// callback to a single in-memory row that the observation shares,
// mirroring how the adapters fold a committed mutation back into the
// state the rest of the pass reads.
func holdFixture(op *types.InstanceOperation) (types.ReconcileInput, *types.InstanceStatus) {
	statuses := []types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseCreating, Operation: op}}
	row := &statuses[0]
	return types.ReconcileInput{
		ObservedState: types.WorkloadObservedState{InstanceStatuses: statuses},
		MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(row)
			return nil
		},
	}, row
}

// TestExternalHold_OneOwnerPerToken pins the precedence every external
// hold shares: a token is never overwritten by a different authority,
// and it is only released by the authority that set it. Without that,
// the quota path and the scheduler hold silently steal and drop each
// other's token, and the row reports a wait nobody is actually in.
func TestExternalHold_OneOwnerPerToken(t *testing.T) {
	ctx := context.Background()
	input, row := holdFixture(&types.InstanceOperation{Type: types.InstanceOperationCreate})

	entered, held, err := types.MarkExternalHold(ctx, input, 0, types.WaitingReasonUnschedulable, nil, nil)
	if err != nil || !entered || !held {
		t.Fatalf("mark unschedulable: entered=%v held=%v err=%v want true, true, nil", entered, held, err)
	}
	if row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Fatalf("Waiting: got %q want %q", row.Operation.Waiting, types.WaitingReasonUnschedulable)
	}

	// A quota refusal arrives while the scheduler hold stands.
	entered, held, err = types.MarkExternalHold(ctx, input, 0, types.RejectionReasonQuotaExceeded, nil, nil)
	if err != nil {
		t.Fatalf("mark quota: %v", err)
	}
	if entered || held {
		t.Errorf("mark quota: entered=%v held=%v want false, false (the token has an owner)", entered, held)
	}
	if row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Waiting: got %q want the scheduler hold untouched", row.Operation.Waiting)
	}

	// The quota path completing must not release someone else's hold.
	if err := types.ClearExternalHold(ctx, input, 0, types.RejectionReasonQuotaExceeded); err != nil {
		t.Fatalf("clear quota: %v", err)
	}
	if row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Waiting: got %q want the scheduler hold still set", row.Operation.Waiting)
	}

	// Its own owner releases it.
	if err := types.ClearExternalHold(ctx, input, 0, types.WaitingReasonUnschedulable); err != nil {
		t.Fatalf("clear unschedulable: %v", err)
	}
	if row.Operation.Waiting != "" {
		t.Errorf("Waiting: got %q want released", row.Operation.Waiting)
	}
}

// TestExternalHold_EdgeTriggered: re-marking a hold already in place
// reports no entry and writes nothing, so a wait that lasts for hours
// costs one status write. Evidence is compared by which pod is held, why
// and since when — never by the authority's message, which it
// recomputes continuously.
func TestExternalHold_EdgeTriggered(t *testing.T) {
	ctx := context.Background()
	input, row := holdFixture(&types.InstanceOperation{Type: types.InstanceOperationCreate})
	at := metav1.Now()
	evidence := func(message string) *types.InstanceTermination {
		return &types.InstanceTermination{
			PodName: "engine-0-default-0",
			Reason:  types.WaitingReasonUnschedulable,
			Message: message,
			Time:    at,
		}
	}

	if entered, held, err := types.MarkExternalHold(ctx, input, 0, types.WaitingReasonUnschedulable, evidence("0/3 nodes"), nil); err != nil || !entered || !held {
		t.Fatalf("first mark: entered=%v held=%v err=%v want true, true, nil", entered, held, err)
	}
	if entered, held, err := types.MarkExternalHold(ctx, input, 0, types.WaitingReasonUnschedulable, evidence("0/9 nodes"), nil); err != nil || entered || !held {
		t.Fatalf("re-mark with a churned message: entered=%v held=%v err=%v want false, true, nil", entered, held, err)
	}
	if got := row.LastFailure; got == nil || got.Message != "0/3 nodes" {
		t.Errorf("LastFailure: got %+v want the message from hold start", got)
	}

	// A same-name pod recreated later starts a fresh hold: the condition
	// transition moved, so this is a new episode.
	fresh := evidence("0/3 nodes")
	fresh.Time = metav1.NewTime(at.Add(1))
	if entered, held, err := types.MarkExternalHold(ctx, input, 0, types.WaitingReasonUnschedulable, fresh, nil); err != nil || entered || !held {
		t.Fatalf("re-mark at a new transition: entered=%v held=%v err=%v want false, true, nil", entered, held, err)
	}
	if got := row.LastFailure; got == nil || !got.Time.Equal(&fresh.Time) {
		t.Errorf("LastFailure.Time: got %+v want the new hold start", got)
	}
}

// TestExternalHold_WritableIsRecheckedOnTheFreshRow: the caller's
// eligibility rules are evaluated against the pass-start observation,
// which can be stale by the time the mutation runs. A row that has since
// entered teardown must not have a hold — or an overwritten LastFailure
// — stamped onto it, so the predicate is re-run inside the callback on
// the row the write actually sees.
func TestExternalHold_WritableIsRecheckedOnTheFreshRow(t *testing.T) {
	ctx := context.Background()
	// Observed as an Update in flight; by the time the write lands the row
	// is a teardown.
	observed := types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate},
	}
	fresh := &types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseDeleting,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationDelete},
		LastFailure: &types.InstanceTermination{
			PodName: "engine-0-default-0", Reason: "PodFailed",
		},
	}
	input := types.ReconcileInput{
		ObservedState: types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{observed}},
		MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(fresh)
			return nil
		},
	}
	notDeleting := func(op *types.InstanceOperation) bool {
		return op != nil && op.Type != types.InstanceOperationDelete
	}

	entered, held, err := types.MarkExternalHold(ctx, input, 0, types.WaitingReasonUnschedulable,
		&types.InstanceTermination{PodName: "engine-0-default-0", Reason: types.WaitingReasonUnschedulable},
		notDeleting)
	if err != nil {
		t.Fatalf("MarkExternalHold: %v", err)
	}
	if entered || held {
		t.Errorf("entered=%v held=%v want false, false (the fresh row is a teardown)", entered, held)
	}
	if fresh.Operation.Waiting != "" {
		t.Errorf("Waiting: got %q want unset on a teardown", fresh.Operation.Waiting)
	}
	if fresh.LastFailure == nil || fresh.LastFailure.Reason != "PodFailed" {
		t.Errorf("LastFailure: got %+v want the teardown's own record untouched", fresh.LastFailure)
	}
}

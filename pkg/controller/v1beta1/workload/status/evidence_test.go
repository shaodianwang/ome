package status

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// The waiting token is written only for an owner the authority may speak
// for, and only when its precedence rule takes the row.
func TestRecordWaiting_IncumbentTokenIsKeptUnlessOutranked(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	anyOwner := func(types.RowOwner) bool { return true }
	firstWriterWins := func(token, current string) bool { return current == "" || current == token }

	w := newRowWriter(openRow(0, types.InstancePhaseUpdating))
	entered, held, err := RecordWaiting(context.Background(), w.input(now), 0, "Unschedulable", anyOwner, firstWriterWins, nil)
	if err != nil || !entered || !held {
		t.Fatalf("mark = entered:%v held:%v err:%v", entered, held, err)
	}
	entered, held, err = RecordWaiting(context.Background(), w.input(now), 0, "Unschedulable", anyOwner, firstWriterWins, nil)
	if err != nil || entered || !held {
		t.Fatalf("re-mark = entered:%v held:%v err:%v — the edge fires once, the hold stands", entered, held, err)
	}

	noOwner := func(types.RowOwner) bool { return false }
	_, held, err = RecordWaiting(context.Background(), w.input(now), 0, "QuotaExceeded", noOwner, firstWriterWins, nil)
	if err != nil || held {
		t.Fatalf("an authority refused by its own owner rule must not hold: held=%v err=%v", held, err)
	}

	if err := ReleaseWaiting(context.Background(), w.input(now), 0, "QuotaExceeded"); err != nil {
		t.Fatal(err)
	}
	if w.rows[0].Operation.Waiting != "Unschedulable" {
		t.Fatal("releasing another authority's token must leave the incumbent alone")
	}
	if err := ReleaseWaiting(context.Background(), w.input(now), 0, "Unschedulable"); err != nil {
		t.Fatal(err)
	}
	if w.rows[0].Operation.Waiting != "" {
		t.Fatalf("waiting = %q, want released", w.rows[0].Operation.Waiting)
	}
}

// Evidence is compared by which pod is held, why, and since when — never
// by its message, which moves on every pass.
func TestSameEvidence_KeysOnTheMoment(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	recorded := &types.InstanceTermination{PodName: "engine-0", Reason: "Unschedulable", Message: "3 nodes", Time: at}
	moved := &types.InstanceTermination{PodName: "engine-0", Reason: "Unschedulable", Message: "7 nodes", Time: at}
	later := &types.InstanceTermination{PodName: "engine-0", Reason: "Unschedulable", Time: metav1.NewTime(at.Add(time.Minute))}

	if !sameEvidence(recorded, moved) {
		t.Fatal("a changed message is the same hold")
	}
	if sameEvidence(recorded, later) {
		t.Fatal("a fresh moment is a fresh episode")
	}
	if !sameEvidence(nil, nil) {
		t.Fatal("no evidence to compare leaves the token to decide")
	}
	if sameEvidence(nil, recorded) {
		t.Fatal("an unrecorded hold is not the recorded one")
	}
}

// The refusal carrier records the moment and nothing else, and both
// edges are idempotent.
func TestRecordCapacityRefusal_ReportsOnlyTheFirstRefusal(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	w := newRowWriter(openRow(0, types.InstancePhaseCreating))

	w.rows[0].Operation.Waiting = types.WaitingReasonUnschedulable
	recorded, incumbent, err := RecordCapacityRefusal(context.Background(), w.input(now), 0)
	if err != nil || !recorded {
		t.Fatalf("record = %v, %v", recorded, err)
	}
	if incumbent != types.WaitingReasonUnschedulable {
		t.Fatalf("incumbent = %q, want the token the row reported when the refusal landed", incumbent)
	}
	if at := w.rows[0].Operation.CapacityRefusedAt; at == nil || !at.Time.Equal(now) {
		t.Fatalf("refusedAt = %v, want %v", at, now)
	}
	recorded, _, err = RecordCapacityRefusal(context.Background(), w.input(now), 0)
	if err != nil || recorded {
		t.Fatalf("re-record = %v, %v — the edge fires once", recorded, err)
	}

	if err := ClearCapacityRefusal(context.Background(), w.input(now), 0); err != nil {
		t.Fatal(err)
	}
	if w.rows[0].Operation.CapacityRefusedAt != nil {
		t.Fatal("completion must retire the record")
	}
	before := w.writes
	if err := ClearCapacityRefusal(context.Background(), w.input(now), 0); err != nil {
		t.Fatal(err)
	}
	if w.writes != before {
		t.Fatal("clearing an absent record must write nothing")
	}
}

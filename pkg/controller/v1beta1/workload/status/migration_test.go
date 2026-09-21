package status

import (
	"context"
	"testing"
	"time"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// The pin writes one side of the pair: the source goes Migrating and the
// surge Creating, each naming the other as SurgeIndex under the
// request's UUID with the deadline the timeout gives. A second pin of
// the same request is a no-op, and a source slot the writer had to
// append is never resurrected.
func TestStampMigrationPin_PinsEachSideOnceAndResurrectsNoSource(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	w := newRowWriter(types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, Incarnation: 2})

	if err := StampMigrationPin(ctx, w.input(now), 0, 1, MigrationRoleSource, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := StampMigrationPin(ctx, w.input(now), 1, 0, MigrationRoleSurge, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	source, surge := w.rows[0], w.rows[1]
	if source.Phase != types.InstancePhaseMigrating || source.Incarnation != 2 ||
		source.Operation == nil || source.Operation.Type != types.InstanceOperationMigrate ||
		source.Operation.RequestUUID != "req-1" ||
		source.Operation.SurgeIndex == nil || *source.Operation.SurgeIndex != 1 {
		t.Fatalf("source = %+v, want Migrating pinned to surge 1 under req-1", source)
	}
	if surge.Phase != types.InstancePhaseCreating || surge.Incarnation != 1 ||
		surge.Operation == nil || surge.Operation.SurgeIndex == nil || *surge.Operation.SurgeIndex != 0 {
		t.Fatalf("surge = %+v, want Creating at Incarnation 1 pinned to source 0", surge)
	}
	if !source.Operation.Deadline.Time.Equal(now.Add(time.Hour)) {
		t.Fatalf("deadline = %v, want now+timeout", source.Operation.Deadline.Time)
	}

	writes := w.writes
	if err := StampMigrationPin(ctx, w.input(now), 0, 1, MigrationRoleSource, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := StampMigrationPin(ctx, w.input(now), 1, 0, MigrationRoleSurge, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if w.writes != writes {
		t.Fatalf("writes = %d, want the same request pinned again to be a no-op (%d)", w.writes, writes)
	}

	empty := newRowWriter()
	if err := StampMigrationPin(ctx, empty.input(now), 4, 5, MigrationRoleSource, "req-2", time.Hour); err != nil {
		t.Fatal(err)
	}
	if empty.writes != 0 {
		t.Fatal("a source slot with no phase must not be resurrected")
	}
}

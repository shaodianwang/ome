package ops

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// Force-delete records (Reason=ForceDelete) are terminal audit rows the
// Delete op's escalation writes after force-deleting a pod wedged on a
// dead node. Like AutoRecover directives they are records, never work
// orders — the migration detector's ledger source must never hand one
// to the Migrate copier, even a (never-written-today) Started row.
func TestNextInFlightFromLedger_SkipsForceDeleteEntries(t *testing.T) {
	ledger := &audit.Ledger{Entries: []audit.Entry{
		// The shape the escalation actually writes: terminal, pod UID
		// as RequestUUID.
		{RequestUUID: "pod-uid-1", Component: "engine", SourceInstance: 0,
			Phase: audit.PhaseCompleted, Reason: audit.ReasonForceDelete,
			Outcome: audit.OutcomeForceDeleteUnreachable, FromNode: "node-a",
			StartedAt: "2026-07-23T00:00:00Z", CompletedAt: "2026-07-23T00:00:00Z"},
		// Defensive: a hypothetical Started ForceDelete row must not
		// resume as a phantom migration.
		{RequestUUID: "pod-uid-2", Component: "engine", SourceInstance: 1,
			Phase: audit.PhaseStarted, Reason: audit.ReasonForceDelete,
			FromNode: "node-b", StartedAt: "2026-07-23T00:01:00Z"},
	}}

	if uuid, _, _ := nextInFlightFromLedger(ledger, v1beta1.EngineComponent); uuid != "" {
		t.Fatalf("ForceDelete entries must be skipped; got uuid %q", uuid)
	}

	// A genuine in-flight operator migration is still picked up.
	ledger.Entries = append(ledger.Entries, audit.Entry{
		RequestUUID: "op1", Component: "engine", SourceInstance: 2,
		Phase: audit.PhaseStarted, Reason: "fragmentation",
		FromNode: "node-c", StartedAt: "2026-07-23T00:02:00Z",
	})
	uuid, req, srcIdx := nextInFlightFromLedger(ledger, v1beta1.EngineComponent)
	if uuid != "op1" || req == nil || srcIdx != 2 {
		t.Fatalf("operator migration must still be detected: got uuid=%q req=%+v src=%d", uuid, req, srcIdx)
	}
}

package ops

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// IR teardown closes dangling Started migration entries terminal
// (Phase=Failed, Outcome=owner-torn-down): the instances died with the
// IR, so the row is a record, never a work order. This pins the resume
// path against phantom migrations — when the parent ISVC outlives the
// IR and the projector recreates a fresh one, the ledger source must
// NOT hand the closed entry back to the Migrate copier.
func TestNextInFlightFromLedger_SkipsOwnerTornDownClosedEntries(t *testing.T) {
	started := audit.Entry{
		RequestUUID: "mig-1", Component: "engine", SourceInstance: 0,
		SurgeInstance: 7, Phase: audit.PhaseStarted, Reason: "fragmentation",
		FromNode: "node-a", StartedAt: "2026-07-23T00:00:00Z",
	}
	ledger := &audit.Ledger{Entries: []audit.Entry{started}}

	// Sanity: while Started, the entry IS the in-flight resume anchor.
	if uuid, _, _ := nextInFlightFromLedger(ledger, v1beta1.EngineComponent); uuid != "mig-1" {
		t.Fatalf("Started entry must resume pre-close; got uuid %q", uuid)
	}

	// The exact close the IR teardown performs.
	ledger.Entries[0] = audit.NewTerminalEntry(started, audit.PhaseFailed, audit.OutcomeOwnerTornDown)

	if uuid, _, _ := nextInFlightFromLedger(ledger, v1beta1.EngineComponent); uuid != "" {
		t.Fatalf("owner-torn-down-closed entry must not resume as a phantom migration; got uuid %q", uuid)
	}
	if ledger.InFlightEntry("mig-1") != nil {
		t.Fatalf("closed entry must not report in-flight")
	}
	if !ledger.HasCompletedOrFailedRequest("mig-1") {
		t.Fatalf("closed entry must dedup a re-delivered request with the same UUID")
	}
	if ok, reason := audit.ValidateCapacity(ledger, mustParseRFC3339(t, "2026-07-23T02:00:00Z")); !ok {
		t.Fatalf("closed entry must not count against the in-flight cap: %s", reason)
	}
}

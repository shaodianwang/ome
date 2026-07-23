package ops

import (
	"testing"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

func mustParseRFC3339(t *testing.T, raw string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return ts
}

// Relocation directives (Reason=AutoRecover) are records, not work
// orders: the migration detector's ledger source must never hand one to
// the Migrate copier — including LEGACY Started AutoRecover entries the
// old PDS-keyed detector wrote (resuming those against a broken source
// was the B1 wedge family). Real operator requests are unaffected.
func TestNextInFlightFromLedger_SkipsAutoRecoverEntries(t *testing.T) {
	ledger := &audit.Ledger{Entries: []audit.Entry{
		// Terminal directive — the current disposition shape.
		{RequestUUID: "d1", Component: "engine", SourceInstance: 0,
			Phase: audit.PhaseCompleted, Reason: audit.ReasonAutoRecover,
			Outcome: audit.OutcomeRelocateRecreate, FromNode: "node-a",
			StartedAt: "2026-07-22T00:00:00Z"},
		// LEGACY Started AutoRecover entry (old detector) — must NOT
		// resume as a phantom migration.
		{RequestUUID: "d2", Component: "engine", SourceInstance: 1,
			Phase: audit.PhaseStarted, Reason: audit.ReasonAutoRecover,
			FromNode: "node-b", StartedAt: "2026-07-22T00:01:00Z"},
	}}

	if uuid, _, _ := nextInFlightFromLedger(ledger, v1beta1.EngineComponent); uuid != "" {
		t.Fatalf("AutoRecover entries must be skipped; got uuid %q", uuid)
	}

	// A genuine in-flight operator migration is still picked up.
	ledger.Entries = append(ledger.Entries, audit.Entry{
		RequestUUID: "op1", Component: "engine", SourceInstance: 2,
		Phase: audit.PhaseStarted, Reason: "fragmentation",
		FromNode: "node-c", StartedAt: "2026-07-22T00:02:00Z",
	})
	uuid, req, srcIdx := nextInFlightFromLedger(ledger, v1beta1.EngineComponent)
	if uuid != "op1" || req == nil || srcIdx != 2 {
		t.Fatalf("operator migration must still be detected: got uuid=%q req=%+v src=%d", uuid, req, srcIdx)
	}
}

// Terminal directives must not consume the migration in-flight capacity
// (ValidateCapacity counts Phase=Started only). They do count toward the
// trailing-hour rate window — acceptable, bounded by maxAttempts.
func TestValidateCapacity_TerminalDirectivesNotInFlight(t *testing.T) {
	ledger := &audit.Ledger{}
	for _, uid := range []string{"d1", "d2", "d3"} {
		ledger.UpsertEntry(audit.Entry{
			RequestUUID: uid, Component: "engine", SourceInstance: 0,
			Phase: audit.PhaseCompleted, Reason: audit.ReasonAutoRecover,
			Outcome: audit.OutcomeRelocateRecreate,
			// Old timestamps so the per-hour window isn't the limiter.
			StartedAt: "2026-07-20T00:00:00Z",
		})
	}
	if ok, reason := audit.ValidateCapacity(ledger, mustParseRFC3339(t, "2026-07-22T00:00:00Z")); !ok {
		t.Fatalf("terminal directives must not trip the in-flight cap: %s", reason)
	}
}

package audit

import (
	"testing"
	"time"
)

// forceDeleteEntry builds a terminal ForceDelete record shaped exactly
// as the Delete op's escalation writes it.
func forceDeleteEntry(podUID, component string, idx int32, node, startedAt string) Entry {
	return Entry{
		RequestUUID:    podUID,
		Component:      component,
		SourceInstance: idx,
		Phase:          PhaseCompleted,
		Reason:         ReasonForceDelete,
		Outcome:        OutcomeForceDeleteUnreachable,
		FromNode:       node,
		StartedAt:      startedAt,
		CompletedAt:    startedAt,
	}
}

// ForceDelete rows are terminal audit records — inert for every
// AutoRecover reader: they never count toward the relocation budget,
// never feed the node-exclusion overlay, never anchor the disposition
// replay guard, and never read as an in-flight migration.
func TestForceDeleteEntries_InertForAutoRecoverReaders(t *testing.T) {
	ledger := &Ledger{Entries: []Entry{
		forceDeleteEntry("uid-1", "engine", 0, "node-a", "2026-07-23T00:00:00Z"),
		forceDeleteEntry("uid-2", "engine", 0, "node-b", "2026-07-23T00:01:00Z"),
		{RequestUUID: "uid-3", Component: "engine", SourceInstance: 0,
			Phase: PhaseCompleted, Reason: ReasonForceDelete,
			Outcome: OutcomeForceDeleteFinalizerReport, FromNode: "node-c",
			StartedAt: "2026-07-23T00:02:00Z", CompletedAt: "2026-07-23T00:02:00Z"},
	}}

	if n := CountAutoRecoverAttempts(ledger, "engine", 0); n != 0 {
		t.Errorf("CountAutoRecoverAttempts: got %d want 0 (force-deletes must not burn relocation budget)", n)
	}
	if nodes := RecentAutoRecoverFromNodes(ledger, "engine", 0, 5); len(nodes) != 0 {
		t.Errorf("RecentAutoRecoverFromNodes: got %v want none (force-deletes must not steer placement)", nodes)
	}
	if e := NewestAutoRecoverEntry(ledger, "engine", 0); e != nil {
		t.Errorf("NewestAutoRecoverEntry: got %+v want nil", e)
	}
	if RemoveAutoRecoverEntries(ledger, "engine", 0) {
		t.Errorf("RemoveAutoRecoverEntries: force-delete records must not be pruned as directives")
	}
	if HasInFlightMigrationForInstance(ledger, "engine", 0) {
		t.Errorf("HasInFlightMigrationForInstance: terminal force-delete rows are not in-flight migrations")
	}
}

// ForceDelete rows key on the pod UID in the same RequestUUID keyspace
// as migrations; a migration request reusing a force-deleted pod's UID
// must not be swallowed as already-terminal.
func TestHasCompletedOrFailedRequest_IgnoresForceDeleteRows(t *testing.T) {
	ledger := &Ledger{Entries: []Entry{
		forceDeleteEntry("uid-x", "engine", 0, "node-a", "2026-07-23T00:00:00Z"),
	}}
	if ledger.HasCompletedOrFailedRequest("uid-x") {
		t.Errorf("ForceDelete row must not satisfy HasCompletedOrFailedRequest")
	}
	// A genuine terminal migration row for the same UUID (Reason is the
	// requester's free-form string, often empty) still matches.
	ledger.Entries = append(ledger.Entries, Entry{
		RequestUUID: "uid-x", Component: "engine", Phase: PhaseCompleted,
		StartedAt: "2026-07-23T00:01:00Z", CompletedAt: "2026-07-23T00:01:00Z",
	})
	if !ledger.HasCompletedOrFailedRequest("uid-x") {
		t.Errorf("terminal migration row must still satisfy HasCompletedOrFailedRequest")
	}
}

// A dead node's pod sweep writes one ForceDelete row per pod; none of
// them may consume the migration admission window (in-flight cap or
// trailing-hour cap).
func TestValidateCapacity_IgnoresForceDeleteEntries(t *testing.T) {
	now, err := time.Parse(time.RFC3339, "2026-07-23T12:00:00Z")
	if err != nil {
		t.Fatalf("parse now: %v", err)
	}
	recent := now.Add(-time.Minute).UTC().Format(time.RFC3339)

	ledger := &Ledger{}
	// Well past DefaultPerHourCap, all inside the trailing hour.
	for i := 0; i < DefaultPerHourCap+5; i++ {
		ledger.UpsertEntry(forceDeleteEntry(
			// Distinct UUIDs so UpsertEntry appends.
			"pod-uid-"+string(rune('a'+i)), "engine", int32(i), "node-x", recent))
	}
	if ok, reason := ValidateCapacity(ledger, now); !ok {
		t.Fatalf("force-delete rows tripped migration capacity: %s", reason)
	}

	// A genuine migration burst still trips the trailing-hour cap.
	for i := 0; i < DefaultPerHourCap; i++ {
		ledger.UpsertEntry(Entry{
			RequestUUID: "mig-" + string(rune('a'+i)), Component: "engine",
			Phase: PhaseCompleted, StartedAt: recent, CompletedAt: recent,
		})
	}
	if ok, _ := ValidateCapacity(ledger, now); ok {
		t.Fatalf("real migrations must still count toward the per-hour cap")
	}
}

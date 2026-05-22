package omenative

import (
	"testing"
	"time"
)

// fixedNow gives the validateMigrationCapacity tests a deterministic
// "now" so the trailing-hour calculation is reproducible.
var fixedNow = time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

func mkEntry(uuid, phase string, startedOffset time.Duration) AuditEntry {
	return AuditEntry{
		RequestUUID: uuid,
		Phase:       phase,
		StartedAt:   fixedNow.Add(startedOffset).Format(time.RFC3339),
	}
}

func TestValidateMigrationCapacity_NilLedgerAllowed(t *testing.T) {
	ok, reason := validateMigrationCapacity(nil, fixedNow)
	if !ok || reason != "" {
		t.Errorf("nil ledger should admit; got ok=%v reason=%q", ok, reason)
	}
}

func TestValidateMigrationCapacity_EmptyLedgerAllowed(t *testing.T) {
	ok, reason := validateMigrationCapacity(&auditLedger{}, fixedNow)
	if !ok || reason != "" {
		t.Errorf("empty ledger should admit; got ok=%v reason=%q", ok, reason)
	}
}

func TestValidateMigrationCapacity_InFlightCapTrips(t *testing.T) {
	// Three Started entries == in-flight cap. Fourth request is rejected.
	ledger := &auditLedger{Entries: []AuditEntry{
		mkEntry("u1", auditPhaseStarted, -10*time.Minute),
		mkEntry("u2", auditPhaseStarted, -5*time.Minute),
		mkEntry("u3", auditPhaseStarted, -1*time.Minute),
	}}
	ok, reason := validateMigrationCapacity(ledger, fixedNow)
	if ok {
		t.Fatalf("expected reject when in-flight cap reached; got ok=true")
	}
	if reason == "" {
		t.Errorf("rejection reason must be non-empty")
	}
}

func TestValidateMigrationCapacity_CompletedDoesNotCountAsInFlight(t *testing.T) {
	// Three Completed + zero Started: in-flight is 0, admission allowed.
	ledger := &auditLedger{Entries: []AuditEntry{
		mkEntry("u1", auditPhaseCompleted, -30*time.Minute),
		mkEntry("u2", auditPhaseCompleted, -25*time.Minute),
		mkEntry("u3", auditPhaseCompleted, -20*time.Minute),
	}}
	ok, _ := validateMigrationCapacity(ledger, fixedNow)
	if !ok {
		t.Errorf("Completed entries should not consume in-flight capacity")
	}
}

func TestValidateMigrationCapacity_PerHourCapTrips(t *testing.T) {
	// 10 Completed entries all within the last hour: per-hour cap
	// reached even though no entries are Started.
	entries := make([]AuditEntry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, mkEntry("u-recent-"+itoa(i), auditPhaseCompleted, -time.Duration(i+1)*time.Minute))
	}
	ledger := &auditLedger{Entries: entries}
	ok, reason := validateMigrationCapacity(ledger, fixedNow)
	if ok {
		t.Fatalf("expected reject when per-hour cap reached; got ok=true")
	}
	if reason == "" {
		t.Errorf("rejection reason must be non-empty")
	}
}

func TestValidateMigrationCapacity_OlderThanHourDoesNotCount(t *testing.T) {
	// 10 entries from 90 minutes ago: trailing-hour window walks past
	// all of them, so the per-hour count is 0 and the request admits.
	entries := make([]AuditEntry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, mkEntry("u-old-"+itoa(i), auditPhaseCompleted, -(90+time.Duration(i))*time.Minute))
	}
	ledger := &auditLedger{Entries: entries}
	ok, _ := validateMigrationCapacity(ledger, fixedNow)
	if !ok {
		t.Errorf("entries older than 1h should not consume per-hour capacity")
	}
}

func TestValidateMigrationCapacity_MalformedStartedAtIsSkipped(t *testing.T) {
	// Garbage StartedAt strings parseAuditTime can't decode are skipped
	// rather than treated as "very recent" or "very old". An empty
	// ledger after skipping admits.
	ledger := &auditLedger{Entries: []AuditEntry{
		{RequestUUID: "u-bad", Phase: auditPhaseCompleted, StartedAt: "not-a-timestamp"},
	}}
	ok, _ := validateMigrationCapacity(ledger, fixedNow)
	if !ok {
		t.Errorf("malformed StartedAt should be skipped, not block admission")
	}
}

func TestParseAuditTime_RFC3339(t *testing.T) {
	in := "2026-05-21T12:00:00Z"
	got, ok := parseAuditTime(in)
	if !ok {
		t.Fatalf("RFC3339 parse failed for %q", in)
	}
	if !got.Equal(fixedNow) {
		t.Errorf("got %v want %v", got, fixedNow)
	}
}

func TestParseAuditTime_RFC3339Nano(t *testing.T) {
	in := "2026-05-21T12:00:00.123456789Z"
	got, ok := parseAuditTime(in)
	if !ok {
		t.Fatalf("RFC3339Nano parse failed for %q", in)
	}
	if got.Year() != 2026 || got.Month() != 5 || got.Day() != 21 {
		t.Errorf("got %v want 2026-05-21", got)
	}
}

func TestParseAuditTime_EmptyOrInvalid(t *testing.T) {
	for _, in := range []string{"", "not-a-timestamp", "2026/05/21"} {
		if _, ok := parseAuditTime(in); ok {
			t.Errorf("parse of %q should fail", in)
		}
	}
}

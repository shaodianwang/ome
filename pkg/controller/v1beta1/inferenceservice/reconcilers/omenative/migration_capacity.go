package omenative

import (
	"time"
)

// Migration capacity defaults from OEP §5.4. Cluster-wide caps that
// gate Alfred or any other automated migration caller from flooding
// the controller's destructive-action pipeline with too many
// concurrent or hourly requests.
const (
	// defaultInFlightMigrationCap is the maximum number of Started
	// (not yet terminal) migration UUIDs in the audit ledger before
	// new requests are RateLimited. Same as OEP §5.4 default of 3.
	defaultInFlightMigrationCap = 3

	// defaultMaxMigrationsPerHour is the maximum number of NEWLY
	// Started migration UUIDs in the trailing hour. Older ledger
	// entries (Started or terminal) are still counted by their
	// StartedAt timestamp. OEP §5.4 default of 10.
	defaultMaxMigrationsPerHour = 10
)

// validateMigrationCapacity reports whether a new migration request
// is admissible against the current audit ledger. Returns ok=true
// when admission is allowed; ok=false with a short reason string
// when the request must be rejected.
//
// The two caps are independent — either tripping rejects the
// request. The cluster-wide in-flight cap bounds peak destructive
// work; the per-hour cap bounds sustained churn against the same
// ISVC.
//
// `now` is injected so tests can pin a deterministic time and walk
// the trailing-hour window past the relevant entries.
func validateMigrationCapacity(ledger *auditLedger, now time.Time) (ok bool, reason string) {
	if ledger == nil {
		return true, ""
	}

	inFlight := 0
	startedInLastHour := 0
	hourAgo := now.Add(-time.Hour)

	for _, entry := range ledger.Entries {
		if entry.Phase == auditPhaseStarted {
			inFlight++
		}
		if startedAt, ok := parseAuditTime(entry.StartedAt); ok && startedAt.After(hourAgo) {
			startedInLastHour++
		}
	}

	if inFlight >= defaultInFlightMigrationCap {
		return false, "in-flight migration cap reached (" +
			itoa(inFlight) + "/" + itoa(defaultInFlightMigrationCap) + ")"
	}
	if startedInLastHour >= defaultMaxMigrationsPerHour {
		return false, "per-hour migration cap reached (" +
			itoa(startedInLastHour) + "/" + itoa(defaultMaxMigrationsPerHour) + " in last hour)"
	}
	return true, ""
}

// parseAuditTime parses the RFC3339-formatted StartedAt /
// CompletedAt strings the audit ledger uses. Returns (zero, false)
// on parse error so the caller can choose to skip rather than treat
// a malformed timestamp as "very old" or "very recent".
func parseAuditTime(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, true
	}
	return time.Time{}, false
}

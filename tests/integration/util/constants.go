// Package util holds reusable helpers for OME integration tests:
// timeouts, namespace lifecycle, polling sugar, custom matchers, and a
// per-CRD cleanup registry.
package util

import "time"

// Time constants used by Eventually wrappers. Three tiers — short for
// fast in-cache observations, medium for status reconciles that
// involve a single API write, long for chains that span 3+
// reconciles.
const (
	// ShortTimeout is for things that should happen within one
	// controller cycle (e.g., a status condition appearing after a
	// resource is created).
	ShortTimeout = 10 * time.Second

	// MediumTimeout covers a resource being created → reconciled →
	// status updated. Default for most assertions.
	MediumTimeout = 30 * time.Second

	// LongTimeout covers multi-controller chains (e.g., BaseModel →
	// metadata Job → ConfigMap → Status=Ready). Used sparingly; if a
	// test needs longer than this it likely has a real bug.
	LongTimeout = 90 * time.Second

	// PollInterval is the default polling frequency for Eventually.
	// 200ms is snappy enough for humans waiting on `go test` while
	// still bounding apiserver QPS during long chains.
	PollInterval = 200 * time.Millisecond
)

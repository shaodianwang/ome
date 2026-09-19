package omenative_coordination_kind

import (
	"time"

	"github.com/ome-projects/ome/tests/integration/kindfix"
)

// KIND-suite timeouts — local aliases of the shared kindfix tier so every KIND
// suite shares one source of truth (specs in this package reference these local
// names heavily). See kindfix.{Short,Medium,Long}Timeout / PollInterval for the
// rationale (real kubelet latency, not envtest microseconds).
const (
	kindShortTimeout  = kindfix.ShortTimeout
	kindMediumTimeout = kindfix.MediumTimeout
	kindLongTimeout   = kindfix.LongTimeout
	kindPollInterval  = kindfix.PollInterval

	// kindSteadyStateWindow spans two dispatcher operation requeues at the
	// cadence the suite's inferenceservice-config seeds, so a Consistently
	// block observes at least two full passes deciding to do nothing.
	kindSteadyStateWindow = 10 * time.Second
)

package coordination_kind

import "time"

// KIND-suite timeouts. Bumped vs the envtest sibling (which uses
// util.{Medium,Long}Timeout = 30s / 90s) because real kubelets
// pull and start containers in seconds, not microseconds. A 90s
// LongTimeout in envtest covers a ~13s simulated rollout with
// headroom; on KIND the same rollout cycle (image build, pod
// schedule, container start, readiness probe) is 30-60s before any
// actual rollout work begins.
//
// PollInterval stays at 100ms — that's the cap on how fast the
// behavior_sampling helpers can see state transitions, and real
// kubelet status updates lag enough that finer polling produces
// no extra signal.
const (
	kindShortTimeout  = 30 * time.Second
	kindMediumTimeout = 2 * time.Minute
	kindLongTimeout   = 5 * time.Minute
	kindPollInterval  = 100 * time.Millisecond
)

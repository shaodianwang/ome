package coordination_kind

import "time"

// KIND-suite timeouts. Real kubelets pull and start containers in
// seconds, not microseconds — the rollout cycle (image build, pod
// schedule, container start, readiness probe) is 30-60s before any
// actual rollout work begins, so timeouts are generous.
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

package workload

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// InstanceEvidence is the per-instance observed failure evidence for one
// reconcile: the first workload-caused/terminal stuck pod (if any) and
// whether the in-flight Operation's deadline has elapsed. It is EVIDENCE
// ONLY — building it never writes Phase; deciding the Failed transition
// from it belongs to the reconcile.
type InstanceEvidence struct {
	// StuckPod is the first live, non-deleting pod parked in a
	// workload-caused/terminal waiting state past the grace window
	// (FirstStuckPodForInstance). Nil when no pod is stuck.
	StuckPod *corev1.Pod
	// StuckReason is the kubelet waiting reason of StuckPod ("" when none).
	StuckReason string
	// DeadlinePassed reports whether the instance is in a transient phase
	// with an in-flight Operation whose Deadline lies in the past.
	DeadlinePassed bool
}

// ObservedSnapshot is the single observation for one reconcile. Pods are
// read per source (live = APIReader for destructive planning; cached =
// informer for non-destructive planning) LAZILY and MEMOIZED, so the
// snapshot preserves the current pipeline's read count and read source
// exactly — a source is Listed at most once, and only when a pass actually
// needs it (never eagerly, which would add a live List to every reconcile
// and load the apiserver on large clusters). Evidence is derived on demand
// from the cached-read pods — evidence informs a status transition, not a
// pod-destructive action, and the grace/deadline windows it feeds dwarf
// informer lag, so the cache-served read keeps the escalation pass off the
// apiserver hot path.
//
// Not safe for concurrent use — one snapshot per reconcile goroutine.
type ObservedSnapshot struct {
	deps      Deps
	input     ReconcileInput
	component ComponentType
	insts     []InstanceStatus

	live   memoBuckets
	cached memoBuckets
}

// memoBuckets memoizes one pod-List-and-bucket result (value + error).
type memoBuckets struct {
	done    bool
	buckets map[int32][]*corev1.Pod
	err     error
}

// NewObservedSnapshot builds the snapshot; pods are materialized lazily on
// first access.
func NewObservedSnapshot(deps Deps, input ReconcileInput, component ComponentType, insts []InstanceStatus) *ObservedSnapshot {
	return &ObservedSnapshot{deps: deps, input: input, component: component, insts: insts}
}

// LivePods returns the Component's pods bucketed by instance from the LIVE
// (APIReader) source — the destructive-safe read the restart pass uses.
// Memoized: at most one live List per reconcile.
func (s *ObservedSnapshot) LivePods(ctx context.Context) (map[int32][]*corev1.Pod, error) {
	if !s.live.done {
		if s.input.AuthoritativePods != nil {
			s.live.buckets = s.input.AuthoritativePods.ByInstance
		} else {
			s.live.buckets, s.live.err = liveBucketPods(ctx, s.deps, s.input, s.component)
		}
		s.live.done = true
	}
	return s.live.buckets, s.live.err
}

// CachedPods returns the Component's pods bucketed by instance from the
// CACHED (informer) source — the non-destructive read the update pass uses.
// Memoized: at most one cached List per reconcile.
func (s *ObservedSnapshot) CachedPods(ctx context.Context) (map[int32][]*corev1.Pod, error) {
	if !s.cached.done {
		s.cached.buckets, s.cached.err = cachedBucketPods(ctx, s.deps, s.input, s.component)
		s.cached.done = true
	}
	return s.cached.buckets, s.cached.err
}

// EvidenceFor returns the per-instance failure evidence for idx, derived
// from the cached-read pods + the instance's Operation deadline. Evidence
// only — no Phase write. Stuck detection attributes pods the same way the
// escalation pass blames them (podsForStuckCheck: a gang Update surge is
// inspected through its replacement gang, a Migrate pair through
// own-plus-sibling). A non-positive grace disables stuck-pod evidence
// entirely (fast escalation off; the deadline backstop still reports). A
// pod-read error yields deadline-only evidence, so a transient read
// failure never fabricates a stuck signal. now/grace are supplied by the
// caller (clock seam).
func (s *ObservedSnapshot) EvidenceFor(ctx context.Context, idx int32, now time.Time, grace time.Duration) InstanceEvidence {
	var ev InstanceEvidence
	var inst *InstanceStatus
	for i := range s.insts {
		if s.insts[i].Index == idx {
			inst = &s.insts[i]
			break
		}
	}
	if inst != nil {
		ev.DeadlinePassed = operationDeadlinePassed(inst, now)
	}
	if grace <= 0 {
		return ev
	}
	cached, err := s.CachedPods(ctx)
	if err != nil {
		return ev
	}
	pods := cached[idx]
	if inst != nil {
		pods = podsForStuckCheck(*inst, cached)
	}
	if pod, reason := FirstStuckPodForInstance(pods, now, grace); pod != nil {
		ev.StuckPod = pod
		ev.StuckReason = reason
	}
	return ev
}

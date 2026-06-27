package executor

import (
	"sync"
	"time"
)

// seenClock records the first instant the executor observed each
// Deployment (keyed by namespaced-name string) and answers "how long
// since". The simulated ready delay is measured from first-observation
// so a class's recorded time-to-Ready reflects the reconcile latency to
// produce the Deployment plus the class's configured delay — both of
// which is what the harness wants to track.
//
// Concurrency: the executor runs with MaxConcurrentReconciles > 1, so
// multiple workers may touch the map at once. A plain mutex is fine — the
// map operations are tiny and uncontended relative to apiserver round
// trips.
type seenClock struct {
	now func() time.Time

	mu    sync.Mutex
	first map[string]time.Time
}

func newSeenClock(now func() time.Time) *seenClock {
	return &seenClock{now: now, first: map[string]time.Time{}}
}

// since returns the elapsed time since key was first observed, recording
// "now" as the first-observation instant on the initial call.
func (s *seenClock) since(key string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.first[key]
	if !ok {
		t = s.now()
		s.first[key] = t
	}
	return s.now().Sub(t)
}

// forget drops key's first-observation record so a deleted Deployment's
// entry doesn't linger for the life of the run.
func (s *seenClock) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.first, key)
}

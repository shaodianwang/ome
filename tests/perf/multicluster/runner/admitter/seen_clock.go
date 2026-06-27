package admitter

import (
	"sync"
	"time"
)

// seenClock records the first instant a member observed each derived ISVC
// (keyed by namespaced-name string) and answers "how long since". The
// simulated admit delay is measured from first-observation so the
// convergence latency the harness records reflects the configured admission
// time rather than when an unrelated event happened to fire.
//
// Concurrency: the admitter runs with MaxConcurrentReconciles > 1, so multiple
// workers may touch the map at once. A plain mutex is fine — the map operations
// are tiny and uncontended relative to apiserver round trips.
type seenClock struct {
	now func() time.Time

	mu    sync.Mutex
	first map[string]time.Time
}

func newSeenClock(now func() time.Time) *seenClock {
	return &seenClock{now: now, first: map[string]time.Time{}}
}

// since returns the elapsed time since key was first observed, recording "now"
// as the first-observation instant on the initial call.
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

// forget drops key's first-observation record so a swept derived's entry
// doesn't linger for the life of the run.
func (s *seenClock) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.first, key)
}

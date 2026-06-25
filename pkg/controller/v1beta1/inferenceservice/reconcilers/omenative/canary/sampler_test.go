package canary

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

func passEval(*int32) evalFunc {
	return func(context.Context, SampleRequest) analysis.Result {
		return analysis.Result{Outcome: analysis.Pass}
	}
}

// TestSampler_GetKicksThenCaches: first Get misses and kicks a background query;
// completion emits an event for the right ISVC; the next Get serves the cached
// result; the query ran exactly once.
func TestSampler_GetKicksThenCaches(t *testing.T) {
	events := make(chan event.GenericEvent, 1)
	var calls int32
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		return analysis.Result{Outcome: analysis.Fail}
	}, events, 4, time.Minute)

	req := SampleRequest{Key: SampleKey{Namespace: "ns", ISVCName: "svc"}}
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("first Get should miss")
	}
	select {
	case ev := <-events:
		if ev.Object.GetNamespace() != "ns" || ev.Object.GetName() != "svc" {
			t.Fatalf("event for wrong object: %s/%s", ev.Object.GetNamespace(), ev.Object.GetName())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a completion event")
	}
	res, at, ok := s.Get(req, time.Time{})
	if !ok || res.Outcome != analysis.Fail || at.IsZero() {
		t.Fatalf("second Get should serve cached Fail, got ok=%v outcome=%v", ok, res.Outcome)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("eval should run once, ran %d", got)
	}
}

// TestSampler_DedupsInflight: many Gets for the same key while a query is in
// flight kick only ONE background query.
func TestSampler_DedupsInflight(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	release := make(chan struct{})
	var calls int32
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		<-release
		return analysis.Result{Outcome: analysis.Pass}
	}, events, 4, time.Minute)

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	for i := 0; i < 5; i++ {
		if _, _, ok := s.Get(req, time.Time{}); ok {
			t.Fatal("Gets during an in-flight query should miss")
		}
	}
	close(release)
	<-events
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("dedup failed: %d evals, want 1", got)
	}
}

// TestSampler_FreshnessSince: a result is served only to a caller whose `since`
// predates it; a caller that already consumed it (since == producedAt) misses.
func TestSampler_FreshnessSince(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	s := NewSampler(passEval(nil), events, 4, time.Minute)
	t0 := time.Unix(1000, 0)
	s.now = func() time.Time { return t0 }

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("first Get should miss")
	}
	<-events // entry now cached at t0

	if _, at, ok := s.Get(req, t0.Add(-time.Second)); !ok || !at.Equal(t0) {
		t.Fatalf("Get(since<t0) should hit at t0, got ok=%v at=%v", ok, at)
	}
	if _, _, ok := s.Get(req, t0); ok {
		t.Fatal("Get(since==producedAt) should miss — that sample was already consumed")
	}
}

// TestSampler_EvictsStale: a cached entry older than the TTL is evicted on access,
// so a subsequent Get re-queries.
func TestSampler_EvictsStale(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	var calls int32
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		return analysis.Result{Outcome: analysis.Pass}
	}, events, 4, time.Minute)
	t0 := time.Unix(1000, 0)
	s.now = func() time.Time { return t0 }

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	s.Get(req, time.Time{})
	<-events // entry at t0

	s.now = func() time.Time { return t0.Add(2 * time.Minute) } // past the 1m TTL
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("stale entry must be evicted, so Get misses")
	}
	<-events // the miss kicked a fresh query
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 evals (initial + after evict), got %d", got)
	}
}

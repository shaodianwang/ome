package recorder

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func key(n string) types.NamespacedName { return types.NamespacedName{Namespace: "ns", Name: n} }

func waitRunning(t *testing.T, r *Recorder) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.running.Load() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("recorder did not start running")
}

// TestRecorderLatencies feeds a full lifecycle for two sources and checks the
// derived latency distributions: placement (create->placed), fan-out
// (create->derived), race-to-win (derived->admitted), and convergence
// (admitted->placed).
func TestRecorderLatencies(t *testing.T) {
	r := New(5*time.Second, 2, 64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitRunning(t, r)

	base := time.Unix(2000, 0)
	// source a: created@0, fanout@100ms, admitted@600ms, placed@700ms
	r.RecordCreated(key("a"), "small", base)
	r.RecordFanOut(key("a"), 3, base.Add(100*time.Millisecond))
	r.RecordAdmitted(key("a"), base.Add(600*time.Millisecond))
	r.RecordPlaced(key("a"), base.Add(700*time.Millisecond))
	// source b: created@0, fanout@200ms, admitted@1200ms, placed@1400ms
	r.RecordCreated(key("b"), "large", base)
	r.RecordFanOut(key("b"), 3, base.Add(200*time.Millisecond))
	r.RecordAdmitted(key("b"), base.Add(1200*time.Millisecond))
	r.RecordPlaced(key("b"), base.Add(1400*time.Millisecond))

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %s", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not converge")
	}

	res := r.Result()
	if res.TotalPlaced != 2 {
		t.Fatalf("TotalPlaced = %d, want 2", res.TotalPlaced)
	}
	place := statsOf(res.Placement)
	if place.Max != 1400 || place.Count != 2 {
		t.Fatalf("placement stats = %+v, want max 1400 count 2", place)
	}
	conv := statsOf(res.Convergence)
	// convergence: a=100ms, b=200ms
	if conv.Max != 200 || conv.Count != 2 {
		t.Fatalf("convergence stats = %+v, want max 200 count 2", conv)
	}
	if got := statsOf(res.ByClass["small"]); got.Max != 700 {
		t.Fatalf("small class max = %d, want 700", got.Max)
	}
	if got := res.ByCandidates[3]; len(got) != 2 {
		t.Fatalf("candidate bucket 3 = %v, want 2 samples", got)
	}
}

// TestBuildSummaryDelta checks the headline convergence delta: poll-only
// convergence minus funnel convergence.
func TestBuildSummaryDelta(t *testing.T) {
	funnel := PassResult{ExpectedTotal: 1, TotalPlaced: 1, Convergence: []int64{100}, Placement: []int64{500}}
	poll := PassResult{ExpectedTotal: 1, TotalPlaced: 1, Convergence: []int64{2000}, Placement: []int64{2400}}

	s := BuildSummary(3, "AllAtOnce",
		funnel, PassOps{Remote: map[string]int64{"create/InferenceService": 3}, RemoteTotal: 3, LocalTotal: 10},
		poll, PassOps{Remote: map[string]int64{"create/InferenceService": 3}, RemoteTotal: 3, LocalTotal: 40},
		0)

	if s.ConvergenceDeltaMeanMs != 1900 {
		t.Fatalf("delta mean = %d, want 1900", s.ConvergenceDeltaMeanMs)
	}
	if !s.FunnelEnabled.FunnelEnabled || s.FunnelDisabled.FunnelEnabled {
		t.Fatalf("pass flags wrong: enabled=%v disabled=%v", s.FunnelEnabled.FunnelEnabled, s.FunnelDisabled.FunnelEnabled)
	}
	if s.Members != 3 || s.DispatcherMode != "AllAtOnce" {
		t.Fatalf("summary header wrong: %+v", s)
	}
}

// TestCreatesPerSource checks the dispatcher blast-radius proxy: remote creates
// divided by placed sources.
func TestCreatesPerSource(t *testing.T) {
	// AllAtOnce-shaped: 30 creates over 10 placed = 3.0 (one per candidate member).
	if got := createsPerSource(map[string]int64{derivedCreateOpKey: 30}, 10); got != 3.0 {
		t.Fatalf("createsPerSource = %v, want 3.0", got)
	}
	// Incremental-shaped: fewer creates than members*sources.
	if got := createsPerSource(map[string]int64{derivedCreateOpKey: 20}, 10); got != 2.0 {
		t.Fatalf("createsPerSource = %v, want 2.0", got)
	}
	// No placed sources: 0, no division by zero.
	if got := createsPerSource(map[string]int64{derivedCreateOpKey: 5}, 0); got != 0 {
		t.Fatalf("createsPerSource(placed=0) = %v, want 0", got)
	}
}

// TestPercentile spot-checks the nearest-rank percentile.
func TestPercentile(t *testing.T) {
	s := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if got := percentile(s, 0.50); got != 5 {
		t.Fatalf("p50 = %d, want 5", got)
	}
	if got := percentile(s, 0.95); got != 10 {
		t.Fatalf("p95 = %d, want 10", got)
	}
	if got := percentile(s, 0.99); got != 10 {
		t.Fatalf("p99 = %d, want 10", got)
	}
}

// TestRecorderTimeout verifies Run returns an error when the expected total
// never converges within maxRecording.
func TestRecorderTimeout(t *testing.T) {
	r := New(200*time.Millisecond, 5, 8)
	if err := r.Run(context.Background()); err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}

// TestRecorderDropsWhenFull verifies the bounded channel drops (and counts)
// events instead of blocking once full and the loop is not draining.
func TestRecorderDropsWhenFull(t *testing.T) {
	r := New(time.Second, 10, 1)
	r.running.Store(true) // simulate "running" so enqueue uses the channel path

	r.RecordPlaced(key("x-0"), time.Now())
	r.RecordPlaced(key("x-1"), time.Now())
	r.RecordPlaced(key("x-2"), time.Now())

	if got := r.DroppedEvents(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
}

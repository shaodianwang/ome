package recorder

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func key(n string) types.NamespacedName { return types.NamespacedName{Namespace: "ns", Name: n} }

// drainRun starts the recorder loop, feeds it the supplied events, and
// returns once the run converges (all expected objects Ready) or the
// short test timeout trips.
func TestRecorderTimeToReady(t *testing.T) {
	r := New(5*time.Second, 2, 64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Give Run a moment to flip running=true so events go through the
	// channel path rather than the pre-start buffer path.
	waitRunning(t, r)

	base := time.Unix(2000, 0)
	r.RecordCreated(key("small-0"), "small", base)
	r.RecordReady(key("small-0"), "small", base.Add(500*time.Millisecond))
	r.RecordCreated(key("large-0"), "large", base)
	r.RecordReady(key("large-0"), "large", base.Add(4*time.Second))

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %s", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not converge")
	}

	sum := r.Summarize()
	if sum.TotalReady != 2 {
		t.Fatalf("TotalReady = %d, want 2", sum.TotalReady)
	}
	if sum.ExpectedTotal != 2 {
		t.Fatalf("ExpectedTotal = %d, want 2", sum.ExpectedTotal)
	}
	if got := sum.Classes["small"]; got == nil || got.AverageTimeToReadyMs != 500 {
		t.Fatalf("small avg = %+v, want 500ms", got)
	}
	if got := sum.Classes["large"]; got == nil || got.AverageTimeToReadyMs != 4000 {
		t.Fatalf("large avg = %+v, want 4000ms", got)
	}
	if sum.DroppedEvents != 0 {
		t.Fatalf("DroppedEvents = %d, want 0", sum.DroppedEvents)
	}
}

// TestRecorderDropsWhenFull verifies the bounded channel drops (and
// counts) events instead of blocking once full and the loop is not
// draining.
func TestRecorderDropsWhenFull(t *testing.T) {
	// Buffer of 1, and never start Run so nothing drains the channel.
	r := New(time.Second, 10, 1)
	r.running.Store(true) // simulate "running" so enqueue uses the channel path

	// First ready event fits the buffer; subsequent ones drop.
	r.RecordReady(key("x-0"), "small", time.Now())
	r.RecordReady(key("x-1"), "small", time.Now())
	r.RecordReady(key("x-2"), "small", time.Now())

	if got := r.dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
}

// TestRecorderCreatedBeforeRun verifies creation stamps that arrive
// before Run starts are buffered (not dropped), so no time-to-Ready
// baseline is lost to a startup race.
func TestRecorderCreatedBeforeRun(t *testing.T) {
	r := New(time.Second, 1, 8)

	base := time.Unix(3000, 0)
	// running is false here → eventCreated takes the direct-apply path.
	r.RecordCreated(key("pre-0"), "small", base)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitRunning(t, r)

	r.RecordReady(key("pre-0"), "small", base.Add(750*time.Millisecond))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not converge")
	}

	sum := r.Summarize()
	if got := sum.Classes["small"]; got == nil || got.AverageTimeToReadyMs != 750 {
		t.Fatalf("small avg = %+v, want 750ms (created stamp must survive pre-Run race)", got)
	}
}

// TestRecorderTimeout verifies Run returns an error when the expected
// total never converges within maxRecording.
func TestRecorderTimeout(t *testing.T) {
	r := New(300*time.Millisecond, 5 /*expected*/, 8)
	err := r.Run(context.Background())
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}

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

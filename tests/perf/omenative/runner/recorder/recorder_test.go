package recorder

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func key(n string) types.NamespacedName { return types.NamespacedName{Namespace: "ns", Name: n} }

func podsPer() map[string]int32 { return map[string]int32{"single": 1, "gang": 4} }

// TestRecorderTimeToReady feeds create+ready events for two ISVC classes
// and verifies the recorder converges and computes per-class averages.
func TestRecorderTimeToReady(t *testing.T) {
	r := New(5*time.Second, 2, 5, 64, podsPer())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Give Run a moment to flip running=true so events go through the
	// channel path rather than the pre-start buffer path.
	waitRunning(t, r)

	base := time.Unix(2000, 0)
	r.RecordISVCCreated(key("single-0"), "single", base)
	r.RecordISVCReady(key("single-0"), "single", base.Add(500*time.Millisecond))
	r.RecordISVCCreated(key("gang-0"), "gang", base)
	r.RecordISVCReady(key("gang-0"), "gang", base.Add(4*time.Second))

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
	if sum.ExpectedPods != 5 {
		t.Fatalf("ExpectedPods = %d, want 5", sum.ExpectedPods)
	}
	if got := sum.Classes["single"]; got == nil || got.AverageTimeToReadyMs != 500 {
		t.Fatalf("single avg = %+v, want 500ms", got)
	}
	if got := sum.Classes["single"]; got == nil || got.PodsPerISVC != 1 {
		t.Fatalf("single podsPerISVC = %+v, want 1", got)
	}
	if got := sum.Classes["gang"]; got == nil || got.AverageTimeToReadyMs != 4000 {
		t.Fatalf("gang avg = %+v, want 4000ms", got)
	}
	if got := sum.Classes["gang"]; got == nil || got.PodsPerISVC != 4 {
		t.Fatalf("gang podsPerISVC = %+v, want 4", got)
	}
	if sum.DroppedEvents != 0 {
		t.Fatalf("DroppedEvents = %d, want 0", sum.DroppedEvents)
	}
}

// TestRecorderPodLatency verifies the pod create-to-Ready roll-up is
// computed independently of the per-ISVC class roll-up.
func TestRecorderPodLatency(t *testing.T) {
	r := New(5*time.Second, 1, 2, 64, podsPer())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitRunning(t, r)

	base := time.Unix(5000, 0)
	r.RecordPodCreated(key("pod-a"), base)
	r.RecordPodReady(key("pod-a"), base.Add(200*time.Millisecond))
	r.RecordPodCreated(key("pod-b"), base)
	r.RecordPodReady(key("pod-b"), base.Add(400*time.Millisecond))

	// Drive convergence on the ISVC stream so Run returns.
	r.RecordISVCCreated(key("single-0"), "single", base)
	r.RecordISVCReady(key("single-0"), "single", base.Add(400*time.Millisecond))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not converge")
	}

	sum := r.Summarize()
	if sum.PodCreateToReady.Count != 2 {
		t.Fatalf("pod count = %d, want 2", sum.PodCreateToReady.Count)
	}
	if sum.PodCreateToReady.AverageMs != 300 {
		t.Fatalf("pod avg = %d, want 300ms", sum.PodCreateToReady.AverageMs)
	}
	if sum.PodCreateToReady.MaxMs != 400 {
		t.Fatalf("pod max = %d, want 400ms", sum.PodCreateToReady.MaxMs)
	}
}

// TestRecorderInjectedMetrics verifies the With* setters land in the
// summary and survive marshaling.
func TestRecorderInjectedMetrics(t *testing.T) {
	r := New(time.Second, 1, 1, 8, podsPer())
	r.WithAPIOps(map[string]int64{"list/Pod": 12, "status-update/InferenceReplica": 7}, 19)
	r.WithStatusUpdates(map[string]*StatusUpdateSummary{
		"ir": {Total: 30, Conflict: 2},
	})
	r.WithChurn(map[string]*ChurnSummary{
		"inferencereplica": {ReconcileTotal: 40, WorkqueueAdds: 55, WorkqueueDepth: 0},
	})
	r.WithIdleReconcileRate(0.5)

	sum := r.Summarize()
	if sum.APIOpsTotal != 19 {
		t.Fatalf("APIOpsTotal = %d, want 19", sum.APIOpsTotal)
	}
	if sum.APIOps["list/Pod"] != 12 {
		t.Fatalf("APIOps[list/Pod] = %d, want 12", sum.APIOps["list/Pod"])
	}
	if su := sum.StatusUpdates["ir"]; su == nil || su.Conflict != 2 || su.Total != 30 {
		t.Fatalf("statusUpdates[ir] = %+v, want {30,2}", su)
	}
	if ch := sum.Churn["inferencereplica"]; ch == nil || ch.ReconcileTotal != 40 {
		t.Fatalf("churn[inferencereplica] = %+v", ch)
	}
	if sum.IdleReconcileRatePerSec != 0.5 {
		t.Fatalf("idle rate = %v, want 0.5", sum.IdleReconcileRatePerSec)
	}
}

// TestRecorderDropsWhenFull verifies the bounded channel drops (and
// counts) events instead of blocking once full and the loop is not
// draining.
func TestRecorderDropsWhenFull(t *testing.T) {
	// Buffer of 1, and never start Run so nothing drains the channel.
	r := New(time.Second, 10, 10, 1, podsPer())
	r.running.Store(true) // simulate "running" so enqueue uses the channel path

	// First ready event fits the buffer; subsequent ones drop.
	r.RecordISVCReady(key("x-0"), "single", time.Now())
	r.RecordISVCReady(key("x-1"), "single", time.Now())
	r.RecordISVCReady(key("x-2"), "single", time.Now())

	if got := r.dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
}

// TestRecorderCreatedBeforeRun verifies creation stamps that arrive before
// Run starts are buffered (not dropped), so no time-to-Ready baseline is
// lost to a startup race.
func TestRecorderCreatedBeforeRun(t *testing.T) {
	r := New(time.Second, 1, 1, 8, podsPer())

	base := time.Unix(3000, 0)
	// running is false here -> eventISVCCreated takes the direct-apply path.
	r.RecordISVCCreated(key("pre-0"), "single", base)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitRunning(t, r)

	r.RecordISVCReady(key("pre-0"), "single", base.Add(750*time.Millisecond))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not converge")
	}

	sum := r.Summarize()
	if got := sum.Classes["single"]; got == nil || got.AverageTimeToReadyMs != 750 {
		t.Fatalf("single avg = %+v, want 750ms (created stamp must survive pre-Run race)", got)
	}
}

// TestRecorderTimeout verifies Run returns an error when the expected
// total never converges within maxRecording.
func TestRecorderTimeout(t *testing.T) {
	r := New(300*time.Millisecond, 5 /*expected*/, 5, 8, podsPer())
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

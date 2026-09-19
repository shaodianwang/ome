package workload_test

// Pause circuit breaker, status truth: pause suspends lifecycle work, never
// the truth about what is running. An Instance that loses every pod while
// paused must stop claiming a phase its pods no longer back, and must not be
// left parked in one the paused passes can never leave.

import (
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// TestPause_UnbackedReadyInstanceDemotesToPending: the pod of a converged
// Instance is deleted while the Component is paused. No pass may recreate it,
// and the row must fall back to Pending rather than keep claiming Ready.
//
// Cells: instance/Ready/pod.deleted[source]/before, instance/Pending/spec.pauseTrue/before
func TestPause_UnbackedReadyInstanceDemotesToPending(t *testing.T) {
	h := newRecoveryHarness(t, false)
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged")
	}

	h.desired.Paused = true
	h.losePods(0)
	h.step()

	if s := h.instance(0); s == nil || s.Phase != workload.InstancePhasePending || s.Operation != nil {
		h.dumpState("paused with no pods")
		t.Fatalf("demotion: got %+v, want Phase=Pending with no operation", s)
	}
	h.step()
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("paused pass recreated %d pod(s)", len(pods))
	}
	if s := h.instance(0); s == nil || s.Phase != workload.InstancePhasePending {
		t.Fatalf("demotion did not hold across passes: %+v", s)
	}

	h.desired.Paused = false
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("after unpause")
		t.Fatalf("Instance never recovered after unpause")
	}
}

package gangpack

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// fakeWaitingPod is a minimal framework.WaitingPod for tests: it records whether
// Allow/Reject was called. The embedded nil interface satisfies the methods we
// don't exercise (they panic if called, which surfaces accidental use).
type fakeWaitingPod struct {
	framework.WaitingPod
	pod      *v1.Pod
	allowed  bool
	rejected bool
}

func (w *fakeWaitingPod) GetPod() *v1.Pod       { return w.pod }
func (w *fakeWaitingPod) Allow(string)          { w.allowed = true }
func (w *fakeWaitingPod) Reject(string, string) { w.rejected = true }

// fakeHandle is a minimal framework.Handle exposing only the waiting-pod
// iteration the gate/unwind use. The embedded nil Handle satisfies the rest.
type fakeHandle struct {
	framework.Handle
	waiting []framework.WaitingPod
}

func (h *fakeHandle) IterateOverWaitingPods(f func(framework.WaitingPod)) {
	for _, wp := range h.waiting {
		f(wp)
	}
}

package gangpack

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// gateDecision reports whether enough gang members are now present to release the
// whole gang: the members already waiting at the gate, plus the one currently
// arriving, reach minMember.
func gateDecision(waitingSiblings, minMember int) bool {
	return waitingSiblings+1 >= minMember
}

// sameGang reports whether a pod belongs to the given gang key (namespace/name).
func sameGang(pod *v1.Pod, gangKey string) bool {
	ns, name, ok := podGroupNameOf(pod)
	return ok && ns+"/"+name == gangKey
}

// Permit is the all-or-nothing gang gate. Each gang member waits here instead of
// binding immediately; when the arriving member makes the count reach minMember,
// every waiting sibling is released together and this member proceeds. Members
// wait up to the gang's timeout (its PodGroup ScheduleTimeoutSeconds); on timeout
// the framework rejects the pod and Unreserve unwinds the rest. A pod
// that is not a domain-pinned gang member is not gated.
func (g *GangPack) Permit(_ context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) (*framework.Status, time.Duration) {
	if g.pgReader == nil {
		return framework.NewStatus(framework.Success), 0
	}
	gang, ok := resolveGang(pod, g.pgReader)
	if !ok {
		return framework.NewStatus(framework.Success), 0
	}

	waiting := 0
	g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		if sameGang(wp.GetPod(), gang.key) {
			waiting++
		}
	})

	if !gateDecision(waiting, gang.minMember) {
		// Not enough yet: hold this member at the gate for the gang's timeout.
		gangGateTotal.WithLabelValues("wait").Inc()
		return framework.NewStatus(framework.Wait), gang.timeout
	}

	// The gang is complete: release every waiting sibling; this member proceeds.
	g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		if sameGang(wp.GetPod(), gang.key) {
			wp.Allow(Name)
		}
	})
	gangGateTotal.WithLabelValues("admit").Inc()
	return framework.NewStatus(framework.Success), 0
}

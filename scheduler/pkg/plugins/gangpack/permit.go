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

// boundSiblings counts the pod's gang members already placed in the cluster, from
// the live node snapshot. Used only on the gate's adopted-gang slow path.
func (g *GangPack) boundSiblings(pod *v1.Pod, topologyKey string) int {
	nodes, err := g.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0
	}
	_, count := boundGangMembers(nodes, pod, topologyKey)
	return count
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
		// The waiting members alone don't complete the gang. Normally that means
		// wait — but for a pin ADOPTED after a restart, some members are already
		// bound (not in the waiting set), and they count toward minMember too.
		// Only adopted gangs pay this snapshot scan, so a fresh gang forming
		// normally never does (its members bind all-or-nothing, so none are bound
		// while others gate).
		if g.pins.Adopted(gang.key) && g.boundSiblings(pod, gang.topologyKey)+waiting+1 >= gang.minMember {
			// fall through to admit
		} else {
			gangGateTotal.WithLabelValues("wait").Inc()
			return framework.NewStatus(framework.Wait), gang.timeout
		}
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

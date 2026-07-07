package gangpack

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/topology"
)

// The scheduler bakes in no workload conventions and no fabric values. Gang
// membership + size come from the standard scheduler-plugins PodGroup; the domain
// label is declared per-workload on the PodGroup (see gang.go); and "is a node
// free for this gang" is inferred from the GANG POD'S OWN resource requests — the
// pod already declares what it needs, so the scheduler is never told an
// accelerator resource name.

// domainOf returns the node's domain — the value of the given topology label — or
// "" when the node has no such label (not part of any domain).
func domainOf(node *v1.Node, topologyKey string) string {
	if node == nil {
		return ""
	}
	return node.Labels[topologyKey]
}

// podScalarRequests sums a pod's requested extended (scalar) resources across its
// containers — the accelerator(s) it needs (e.g. nvidia.com/gpu, a TPU resource).
// framework.NewResource splits standard resources (cpu/memory) from scalar ones;
// the whole-node gang model binds on the scalar accelerator, so that is what
// placement counts.
func podScalarRequests(pod *v1.Pod) map[v1.ResourceName]int64 {
	total := v1.ResourceList{}
	if pod != nil {
		for i := range pod.Spec.Containers {
			for name, q := range pod.Spec.Containers[i].Resources.Requests {
				cur := total[name]
				cur.Add(q)
				total[name] = cur
			}
		}
	}
	return framework.NewResource(total).ScalarResources
}

// nodeRelevant reports whether a node could ever hold this gang pod — it has
// allocatable capacity for at least one resource the pod requests. This is what
// makes a node part of an accelerator domain (a labeled node with none of the
// gang's resource is not a member).
func nodeRelevant(ni *framework.NodeInfo, req map[v1.ResourceName]int64) bool {
	if ni == nil || ni.Allocatable == nil {
		return false
	}
	for name := range req {
		if ni.Allocatable.ScalarResources[name] > 0 {
			return true
		}
	}
	return false
}

// nodeFits reports whether the gang pod's scalar requests fit the node's
// remaining capacity (allocatable - requested) for every requested resource.
// Whole-node model: a node already running the accelerator is not free.
func nodeFits(ni *framework.NodeInfo, req map[v1.ResourceName]int64) bool {
	for name, q := range req {
		var requested int64
		if ni.Requested != nil {
			requested = ni.Requested.ScalarResources[name]
		}
		if ni.Allocatable.ScalarResources[name]-requested < q {
			return false
		}
	}
	return true
}

// freeByDomain projects a framework node snapshot onto the pure accounting type:
// free nodes per domain for the given gang pod. A node participates only if it
// carries a domain label AND could hold the pod (nodeRelevant); it is counted
// free when the pod fits (nodeFits). A domain with no free node still appears at
// 0 so "full" is distinguishable from "absent".
func freeByDomain(nodeInfos []*framework.NodeInfo, topologyKey string, pod *v1.Pod) topology.FreeByDomain {
	req := podScalarRequests(pod)
	out := topology.FreeByDomain{}
	for _, info := range nodeInfos {
		dom := domainOf(info.Node(), topologyKey)
		if dom == "" {
			continue
		}
		if !nodeRelevant(info, req) {
			continue
		}
		out[dom] += 0 // register the domain even if all nodes are occupied
		if nodeFits(info, req) {
			out[dom]++
		}
	}
	return out
}

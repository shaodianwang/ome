package gangpack

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/topology"
)

// The scheduler bakes in no workload conventions. Gang membership comes from the
// standard scheduler-plugins PodGroup (its minMember + the standard pod-group
// label) — the scheduler reads that directly, no OME config. The only inputs it
// must be told are fabric facts, supplied as plugin args: which label denotes a
// domain (topologyKey — e.g. nvidia.com/gpu.clique or a TPU slice label) and
// which resource is the accelerator. Those are infrastructure config, not OME
// conventions, and there is deliberately no in-code default.
//
// Cross-gang co-location (prefill+decode in one domain, R2) is the one thing that
// can't be inferred — two independent PodGroups being "one service" only exists
// if the workload declares it. Its mechanism (standard podAffinity with a
// topologyKey, or a link on the PodGroup) is decided when R2 lands; deliberately
// not a bespoke per-pod label baked in now.

// domainOf returns the node's domain — the value of the configured topology
// label — or "" when the node has no such label (not part of any domain).
func domainOf(node *v1.Node, topologyKey string) string {
	if node == nil {
		return ""
	}
	return node.Labels[topologyKey]
}

// nodeFree reports, for an accelerator node, whether it is available for a
// whole-node gang pod. isAccel is true only when the node actually has the
// accelerator (allocatable > 0); free is true when none of that accelerator is
// currently requested. Whole-node model: any accelerator pod on the node makes
// it occupied for a gang (OEP-0022 § Placement).
func nodeFree(ni *framework.NodeInfo, accel v1.ResourceName) (isAccel, free bool) {
	if ni == nil || ni.Allocatable == nil {
		return false, false
	}
	if ni.Allocatable.ScalarResources[accel] <= 0 {
		return false, false
	}
	var requested int64
	if ni.Requested != nil {
		requested = ni.Requested.ScalarResources[accel]
	}
	return true, requested == 0
}

// freeByDomain projects a framework node snapshot onto the pure accounting type:
// free whole accelerator nodes per domain. Only accelerator nodes carrying a
// domain label participate; an accelerator domain with no free node still
// appears at 0 so "full" is distinguishable from "absent" (see
// topology.CountFree). Non-accelerator and unlabeled nodes are ignored.
func freeByDomain(nodeInfos []*framework.NodeInfo, topologyKey string, accel v1.ResourceName) topology.FreeByDomain {
	out := topology.FreeByDomain{}
	for _, info := range nodeInfos {
		node := info.Node()
		dom := domainOf(node, topologyKey)
		if dom == "" {
			continue
		}
		isAccel, free := nodeFree(info, accel)
		if !isAccel {
			continue
		}
		out[dom] += 0 // register the domain even if all nodes are occupied
		if free {
			out[dom]++
		}
	}
	return out
}

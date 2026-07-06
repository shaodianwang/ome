// Package gangpack is the OME placement plugin: topology-aware gang placement and
// bin-packing (OEP-0022, Phase A). This is the walking skeleton — the plugin
// registers, names itself, and passes every node through Filter as a no-op. The
// real domain-choice, pinning, gang gate, and packing logic land on top of this
// scaffold; the point of the skeleton is to prove the kube-scheduler-as-library
// build and the plugin registration wiring end to end.
package gangpack

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Name is the plugin name referenced from the scheduler ConfigMap.
const Name = "OMEGangPack"

// GangPack is the OME placement plugin. It holds the framework handle so later
// phases can read the shared pod/node lister and post events.
type GangPack struct {
	handle framework.Handle
}

// Interface assertions: the skeleton implements Filter today; PreFilter, Score,
// Reserve, Permit, and PostBind are added as their phases land.
var _ framework.FilterPlugin = &GangPack{}

// New is the plugin factory registered via app.WithPlugin. The signature is the
// framework's PluginFactory contract (ctx, plugin args, handle).
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &GangPack{handle: h}, nil
}

// Name returns the plugin's registered name.
func (g *GangPack) Name() string { return Name }

// Filter enforces the gang's pinned domain: once PreFilter has recorded a pin in
// CycleState, only nodes in that domain pass; every other node is rejected as
// UnschedulableAndUnresolvable (a wrong-domain node cannot be made right by
// preempting on it, so the scheduler must not attempt preemption there). When no
// pin is recorded — the pod is not a pinned gang member — Filter imposes no
// constraint and admits the node.
func (g *GangPack) Filter(_ context.Context, state *framework.CycleState, _ *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	pin := readPin(state)
	if pin == nil {
		return nil // no pinned domain this cycle -> no constraint
	}
	if domainOf(nodeInfo.Node(), pin.topologyKey) != pin.domain {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"node not in gang's pinned domain "+pin.domain)
	}
	return nil
}

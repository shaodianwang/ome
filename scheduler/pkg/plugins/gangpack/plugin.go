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

// Filter is a per-node yes/no check. The skeleton admits every node (nil Status
// == Success); domain pinning — rejecting nodes outside the gang's chosen
// domain — is the first real behavior to land here.
func (g *GangPack) Filter(_ context.Context, _ *framework.CycleState, _ *v1.Pod, _ *framework.NodeInfo) *framework.Status {
	return nil
}

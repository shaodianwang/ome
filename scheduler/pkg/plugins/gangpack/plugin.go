// Package gangpack is the OME placement plugin: topology-aware gang placement and
// bin-packing (OEP-0022, Phase A). PreFilter chooses and pins a gang's domain;
// Filter enforces it. The intellectual core (domain accounting, best-fit, pins)
// lives in the pure topology/placement packages; this package wires the live
// scheduler state onto them.
package gangpack

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
)

// Name is the plugin name referenced from the scheduler ConfigMap.
const Name = "OMEGangPack"

// GangPack is the OME placement plugin.
type GangPack struct {
	handle framework.Handle
	// pins records, per placement group, the domain its gangs are committed to.
	pins *placement.Pins
	// pgReader resolves a pod's PodGroup facts (minMember + declared topology
	// key). nil until the informer-backed reader is wired, in which case the
	// plugin is inert (PreFilter returns Skip).
	pgReader podGroupReader
}

// Interface assertions.
var (
	_ framework.PreFilterPlugin   = &GangPack{}
	_ framework.FilterPlugin      = &GangPack{}
	_ framework.PermitPlugin      = &GangPack{}
	_ framework.ReservePlugin     = &GangPack{}
	_ framework.EnqueueExtensions = &GangPack{}
)

// New is the plugin factory registered via app.WithPlugin. It stands up the
// PodGroup informer from the scheduler's shared kubeconfig, so PreFilter can
// resolve gang facts (minMember + declared topology key). Wiring pgReader here is
// what activates the plugin (before this, PreFilter returned Skip).
func New(ctx context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	reader, err := newInformerReader(ctx, h.KubeConfig())
	if err != nil {
		return nil, err
	}
	return &GangPack{handle: h, pins: placement.New(), pgReader: reader}, nil
}

// Name returns the plugin's registered name.
func (g *GangPack) Name() string { return Name }

// PreFilter chooses and pins the gang's domain. For a resolvable gang it best-fits
// a domain over the live node snapshot, records the pin (which Filter enforces),
// and narrows downstream candidates to that domain's nodes. For anything that is
// not a domain-pinned gang — no PodGroup reader wired yet, or a pod that is not a
// gang member — it returns Skip, so Filter is skipped and no domain constraint is
// imposed. Unschedulable when a real gang fits in no domain.
func (g *GangPack) PreFilter(_ context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	if g.pgReader == nil {
		return nil, framework.NewStatus(framework.Skip)
	}
	gang, ok := resolveGang(pod, g.pgReader)
	if !ok {
		return nil, framework.NewStatus(framework.Skip)
	}
	nodes, err := g.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	return g.pinGang(state, nodes, gang, pod)
}

// pinGang is the decision core, split out so it is unit-testable with an explicit
// node slice (no Handle): best-fit a domain for the gang, record the pin, and
// return the domain's nodes as the downstream candidate set.
//
// Only the first member of a gang runs best-fit; once pinned, later members read
// the committed domain and skip the O(nodes) freeByDomain projection entirely —
// the expensive per-node resource accounting is paid once per gang, not once per
// pod.
//
// When the in-memory pin is absent but the gang already has members placed in the
// cluster (scheduler restart / leader failover lost the pin), the gang's domain
// is adopted from where those members already sit rather than best-fit afresh —
// re-planning a live gang into a different domain would strand its bound members.
func (g *GangPack) pinGang(state *framework.CycleState, nodes []*framework.NodeInfo, gang gangInfo, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	domain, pinned := g.pins.Get(gang.key)
	if !pinned {
		if d, count := boundGangMembers(nodes, pod, gang.topologyKey); count > 0 {
			// Rebuild the lost pin from ground truth: the gang lives where its
			// members already are.
			g.pins.Set(gang.key, d)
			gangPinTotal.WithLabelValues("adopted").Inc()
			pinnedGroups.Set(float64(g.pins.Len()))
			domain = d
		} else {
			free := freeByDomain(nodes, gang.topologyKey, pod)
			d, fits := g.pins.Choose(gang.key, free, gang.minMember)
			if !fits {
				gangPinTotal.WithLabelValues("no_fit").Inc()
				return nil, framework.NewStatus(framework.Unschedulable, "no domain has room for gang "+gang.key)
			}
			gangPinTotal.WithLabelValues("pinned").Inc()
			pinnedGroups.Set(float64(g.pins.Len()))
			domain = d
		}
	}
	writePin(state, domain, gang.topologyKey)
	return &framework.PreFilterResult{NodeNames: nodesInDomain(nodes, gang.topologyKey, domain)}, nil
}

// PreFilterExtensions returns nil: the plugin does not incrementally re-evaluate
// on pod add/remove during preemption.
func (g *GangPack) PreFilterExtensions() framework.PreFilterExtensions { return nil }

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

// nodesInDomain returns the names of nodes whose domain matches — the candidate
// set PreFilter narrows to.
func nodesInDomain(nodes []*framework.NodeInfo, topologyKey, domain string) sets.Set[string] {
	out := sets.New[string]()
	for _, ni := range nodes {
		n := ni.Node()
		if n != nil && domainOf(n, topologyKey) == domain {
			out.Insert(n.Name)
		}
	}
	return out
}

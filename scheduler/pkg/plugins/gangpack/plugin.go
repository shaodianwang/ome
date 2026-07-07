// Package gangpack is the OME placement plugin: topology-aware gang placement and
// bin-packing. PreFilter chooses and pins a gang's domain; Filter enforces it.
// The core (domain accounting, best-fit, pins) lives in the pure
// topology/placement packages; this package wires the live scheduler state onto
// them.
package gangpack

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
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
	// podLister enumerates live gang pods for the pin garbage collector. nil in
	// unit tests that don't exercise GC.
	podLister gangPodLister
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
// PodGroup informer from the scheduler's shared kubeconfig (so PreFilter can
// resolve gang facts), wires a gang-pod lister from the scheduler's shared
// informer, and starts the pin garbage collector. Wiring pgReader here is what
// activates the plugin (before this, PreFilter returned Skip).
func New(ctx context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	reader, err := newInformerReader(ctx, h.KubeConfig())
	if err != nil {
		return nil, err
	}
	g := &GangPack{
		handle:    h,
		pins:      placement.New(),
		pgReader:  reader,
		podLister: newInformerPodLister(h),
	}
	go g.runPinGC(ctx)
	return g, nil
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
	ns, name, isMember := podGroupNameOf(pod)
	if !isMember {
		// Not a gang member: impose no constraint, skip Filter.
		return nil, framework.NewStatus(framework.Skip)
	}
	minMember, topologyKey, timeout, found := g.pgReader.get(ns, name)
	if !found {
		// The pod is labelled into a PodGroup the reader can't resolve yet — most
		// often informer lag right after the PodGroup was created (pod and group
		// created together). Skipping here would let a lone member bind and break
		// all-or-nothing, so hold the pod instead; it requeues automatically when
		// the PodGroup appears (a registered enqueue event). Plain Unschedulable so
		// the requeue hints stay in effect.
		return nil, framework.NewStatus(framework.Unschedulable, "PodGroup "+ns+"/"+name+" not resolvable yet")
	}
	if minMember <= 0 || topologyKey == "" {
		// PodGroup exists but declares no domain gang (no positive minMember, or no
		// topology key to pin to): leave placement unconstrained.
		return nil, framework.NewStatus(framework.Skip)
	}
	gang := gangInfo{key: ns + "/" + name, minMember: minMember, topologyKey: topologyKey, timeout: timeout}
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

	// Trace the pin decision at V(4): the domain choice, the pinStale inputs, and
	// the narrowed candidate set are otherwise invisible, and they are exactly what
	// is needed to debug a gang that won't place (e.g. pinned to a domain whose
	// nodes are all filtered out while other domains sit free). Off at the default
	// verbosity, so it costs nothing in normal operation.
	if klog.V(4).Enabled() {
		bd, bc := boundGangMembers(nodes, pod, gang.topologyKey)
		free := freeByDomain(nodes, gang.topologyKey, pod)
		klog.V(4).InfoS("gangpack.pinGang.enter", "gang", gang.key, "minMember", gang.minMember,
			"topologyKey", gang.topologyKey, "pinned", pinned, "pinnedDomain", domain,
			"nodesInPinnedDomain", nodesInDomain(nodes, gang.topologyKey, domain).Len(),
			"boundDomain", bd, "boundCount", bc, "freePinnedDomain", free[domain], "freeByDomain", fmt.Sprintf("%v", free))
	}

	// A stale pin must be dropped and re-planned, else the gang wedges on it
	// forever. Releasing also returns the pin's leaked capacity reservation. Only
	// safe when no member has landed in the domain yet (see pinStale).
	if pinned && pinStale(nodes, gang, pod, domain) {
		g.pins.Release(gang.key)
		gangPinTotal.WithLabelValues("stale_replan").Inc()
		pinnedGroups.Set(float64(g.pins.Len()))
		pinned = false
		klog.V(4).InfoS("gangpack.pinGang.stale_replan", "gang", gang.key, "releasedDomain", domain)
	}

	if !pinned {
		d, status := g.planDomain(nodes, gang, pod)
		if status != nil {
			klog.V(4).InfoS("gangpack.pinGang.no_fit", "gang", gang.key, "status", status.Message())
			return nil, status
		}
		domain = d
	}
	writePin(state, domain, gang.topologyKey)
	narrowed := nodesInDomain(nodes, gang.topologyKey, domain)
	klog.V(4).InfoS("gangpack.pinGang.result", "gang", gang.key, "domain", domain, "narrowedNodes", narrowed.Len())
	return &framework.PreFilterResult{NodeNames: narrowed}, nil
}

// planDomain commits an unpinned gang to a domain: it adopts the domain of any
// already-placed members (failover / lost pin), else best-fits a fresh one. It
// records the pin and the placement metric. status is non-nil (Unschedulable)
// only when the gang fits in no domain.
func (g *GangPack) planDomain(nodes []*framework.NodeInfo, gang gangInfo, pod *v1.Pod) (string, *framework.Status) {
	if d, count := boundGangMembers(nodes, pod, gang.topologyKey); count > 0 {
		// Rebuild from ground truth: the gang lives where its members already are.
		g.pins.Set(gang.key, d)
		gangPinTotal.WithLabelValues("adopted").Inc()
		pinnedGroups.Set(float64(g.pins.Len()))
		return d, nil
	}
	free := freeByDomain(nodes, gang.topologyKey, pod)
	d, fits := g.pins.Choose(gang.key, free, gang.minMember)
	if !fits {
		gangPinTotal.WithLabelValues("no_fit").Inc()
		return "", framework.NewStatus(framework.Unschedulable, "no domain has room for gang "+gang.key)
	}
	gangPinTotal.WithLabelValues("pinned").Inc()
	pinnedGroups.Set(float64(g.pins.Len()))
	return d, nil
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
	nodeDomain := domainOf(nodeInfo.Node(), pin.topologyKey)
	if nodeDomain != pin.domain {
		// Trace at V(4): the pin (domain + topology key) Filter actually reads from
		// CycleState, versus the candidate node's domain. Pairs with pinGang's trace
		// to catch a PreFilter->Filter pin mismatch (a pinned domain's own nodes
		// getting rejected here while it looks free in PreFilter).
		klog.V(4).InfoS("gangpack.Filter.reject", "node", nodeInfo.Node().GetName(),
			"nodeDomain", nodeDomain, "pinDomain", pin.domain, "pinTopologyKey", pin.topologyKey)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"node not in gang's pinned domain "+pin.domain)
	}
	return nil
}

// pinStale reports whether a gang's existing pin must be dropped and re-planned.
// Two cases wedge a gang that keeps its pin, and both are safe to re-plan because
// no member has landed in the pinned domain yet:
//
//   - the domain vanished — every node drained/scaled away, or the PodGroup's
//     topology key changed so the old domain value matches no node. Reusing it
//     narrows every candidate away.
//   - the domain filled — another gang won the race for it after this gang pinned,
//     so it can no longer fit this gang's minMember. Without a re-plan the gang
//     retries the full domain forever (Filter rejects every occupied node) while
//     other domains sit free, and its Choose reservation leaks until re-plan
//     releases it.
//
// A domain that already holds any of this gang's members is NEVER stale: those
// members are bound (or assumed) there, so re-planning would strand them. Such a
// gang keeps its pin and waits for capacity in its own domain. The full-domain
// projection is only paid while a gang is unplaced (a transient formation window);
// once a member lands, the early return keeps steady-state PreFilter cheap.
func pinStale(nodes []*framework.NodeInfo, gang gangInfo, pod *v1.Pod, domain string) bool {
	if nodesInDomain(nodes, gang.topologyKey, domain).Len() == 0 {
		return true // domain vanished
	}
	if _, bound := boundGangMembers(nodes, pod, gang.topologyKey); bound > 0 {
		return false // has members placed here already; never strand them
	}
	return freeByDomain(nodes, gang.topologyKey, pod)[domain] < gang.minMember
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

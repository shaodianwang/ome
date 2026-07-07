package gangpack

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// gcInterval is how often the pin garbage collector reconciles. It is internal
// housekeeping cadence — not a scheduling-behavior value — so it is a constant,
// not config: a pin left behind by a torn-down gang is harmless until reclaimed,
// and the reconcile is cheap (a cache List plus an O(pins) sweep).
const gcInterval = 60 * time.Second

// gangPodLister yields the set of gang keys (namespace/podgroup) that still have
// at least one live pod. An interface so the GC is unit-testable without an
// informer.
type gangPodLister interface {
	liveGangKeys() (map[string]bool, error)
}

// runPinGC periodically reconciles the pin store against live gangs, releasing
// pins whose pods are all gone. It runs for the scheduler's lifetime (bounded by
// ctx) and is a no-op if no pod lister was wired.
func (g *GangPack) runPinGC(ctx context.Context) {
	if g.podLister == nil {
		return
	}
	t := time.NewTicker(gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.gcPins()
		}
	}
}

// gcPins reconciles the pin store against ground truth on each tick. It first
// releases pins for gangs that no longer have any live pod, then reconciles the
// surviving pins' reservations against the live snapshot — reclaiming any
// reservation left dangling by a gang that pinned a domain but whose scheduling
// cycle failed before a member was assigned (Filter rejected it, so no
// Reserve/Unreserve ran to return the reservation). A transient lister error skips
// the whole sweep — the next tick retries — rather than acting on a partial view.
func (g *GangPack) gcPins() {
	live, err := g.podLister.liveGangKeys()
	if err != nil {
		return
	}
	if released := g.pins.RetainOnly(live); released > 0 {
		pinnedGroups.Set(float64(g.pins.Len()))
	}
	g.reconcileReservations()
}

// reconcileReservations resets each pinned gang's reservation to what the live
// snapshot still justifies: gangSize minus the members already occupying nodes in
// its pinned domain. A reserved node is one committed but not yet filled, so once
// a gang's cycle has failed without placing anyone the reservation should have
// drained but did not — this returns it. Needs both the reader (for gangSize) and
// the snapshot (for placed members); when either is unavailable it is a no-op.
func (g *GangPack) reconcileReservations() {
	if g.pgReader == nil {
		return
	}
	domains := g.pins.Domains()
	if len(domains) == 0 {
		return
	}
	nodes, err := g.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return
	}
	want := make(map[string]int, len(domains))
	for group, domain := range domains {
		ns, name, ok := splitGangKey(group)
		if !ok {
			continue
		}
		minMember, topologyKey, _, found := g.pgReader.get(ns, name)
		if !found || minMember <= 0 || topologyKey == "" {
			continue
		}
		placed := placedInDomain(nodes, ns, name, topologyKey, domain)
		want[group] = minMember - placed
	}
	g.pins.Reconcile(want)
}

// informerPodLister is the live gangPodLister, backed by the scheduler's shared
// pod informer cache and filtered to pods that carry the pod-group label.
type informerPodLister struct {
	list func() ([]*v1.Pod, error)
}

var _ gangPodLister = &informerPodLister{}

func (l *informerPodLister) liveGangKeys() (map[string]bool, error) {
	pods, err := l.list()
	if err != nil {
		return nil, err
	}
	keys := make(map[string]bool, len(pods))
	for _, p := range pods {
		if ns, name, ok := podGroupNameOf(p); ok {
			keys[ns+"/"+name] = true
		}
	}
	return keys, nil
}

// newInformerPodLister builds a gang-pod lister from the scheduler's shared
// informer factory, selecting only pods that carry the pod-group label.
func newInformerPodLister(h framework.Handle) *informerPodLister {
	lister := h.SharedInformerFactory().Core().V1().Pods().Lister()
	req, _ := labels.NewRequirement(podGroupLabel, selection.Exists, nil)
	sel := labels.NewSelector().Add(*req)
	return &informerPodLister{
		list: func() ([]*v1.Pod, error) { return lister.List(sel) },
	}
}

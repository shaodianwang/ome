package gangpack

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
	"github.com/ome-projects/ome/scheduler/pkg/topology"
)

func TestReserveIsNoop(t *testing.T) {
	g := &GangPack{}
	if s := g.Reserve(context.Background(), nil, &v1.Pod{}, "n1"); !s.IsSuccess() {
		t.Fatalf("Reserve = %v, want Success", s)
	}
}

// TestReserveDrainsReservation: reserving a gang member (it just got a real node)
// drains one whole node from its domain reservation, so the capacity another gang
// sees frees up in step — the hand-off from reserved to placed.
func TestReserveDrainsReservation(t *testing.T) {
	g := &GangPack{
		pins:     placement.New(),
		pgReader: fakeReader{"team/pf": {min: 2, topo: "clique"}},
	}
	// PreFilter would have pinned pf into domain d over 2 free nodes, reserving 2.
	g.pins.Choose("team/pf", topology.FreeByDomain{"d": 2}, 2)
	// A second gang can't fit: d is fully reserved (2 raw - 2 reserved = 0).
	if _, ok := g.pins.Choose("team/other", topology.FreeByDomain{"d": 2}, 1); ok {
		t.Fatal("precondition: d should be fully reserved before Reserve")
	}

	// One pf member lands: Reserve drains its slot.
	if s := g.Reserve(context.Background(), nil, gangPod("team", "pf"), "node1"); !s.IsSuccess() {
		t.Fatalf("Reserve = %v, want Success", s)
	}

	// Now a gang of 1 fits the freed node (2 raw - 1 reserved = 1).
	if d, ok := g.pins.Choose("team/other", topology.FreeByDomain{"d": 2}, 1); !ok || d != "d" {
		t.Fatalf("after Reserve, other Choose = %q,%v want d,true", d, ok)
	}
}

// TestUnreserveUnwindsGang: unreserving one member releases the domain pin and
// rejects the siblings still waiting at the gate (but not pods of other gangs).
func TestUnreserveUnwindsGang(t *testing.T) {
	sibling := &fakeWaitingPod{pod: gangPod("team", "pf")}
	other := &fakeWaitingPod{pod: gangPod("team", "decode")}
	h := &fakeHandle{waiting: []framework.WaitingPod{sibling, other}}
	g := &GangPack{
		handle:   h,
		pins:     placement.New(),
		pgReader: fakeReader{"team/pf": {min: 3, topo: "clique"}},
	}
	g.pins.Set("team/pf", "d1")

	g.Unreserve(context.Background(), nil, gangPod("team", "pf"), "node1")

	if _, pinned := g.pins.Get("team/pf"); pinned {
		t.Fatal("pin should be released so the retry re-plans")
	}
	if !sibling.rejected {
		t.Fatal("waiting sibling of the same gang should be rejected")
	}
	if other.rejected {
		t.Fatal("a pod of a different gang must not be rejected")
	}
}

// TestUnreserveNonGangNoop: a pod with no pod-group label is not a gang member,
// so Unreserve does nothing.
func TestUnreserveNonGangNoop(t *testing.T) {
	victim := &fakeWaitingPod{pod: gangPod("team", "pf")}
	h := &fakeHandle{waiting: []framework.WaitingPod{victim}}
	g := &GangPack{handle: h, pins: placement.New(), pgReader: fakeReader{}}

	g.Unreserve(context.Background(), nil, gangPod("team", ""), "node1") // no label

	if victim.rejected {
		t.Fatal("non-gang Unreserve must not reject anyone")
	}
}

// TestUnreserveReleasesWhenPodGroupGone: if the PodGroup was deleted mid-flight
// (the reader can no longer resolve it), Unreserve must STILL release the pin and
// reservation — the key comes from the pod's own label, not a PodGroup lookup —
// so nothing leaks.
func TestUnreserveReleasesWhenPodGroupGone(t *testing.T) {
	g := &GangPack{
		handle:   &fakeHandle{},
		pins:     placement.New(),
		pgReader: fakeReader{}, // empty: the PodGroup is gone
	}
	g.pins.Set("team/pf", "d1") // pinned before the PodGroup vanished

	g.Unreserve(context.Background(), nil, gangPod("team", "pf"), "node1")

	if _, pinned := g.pins.Get("team/pf"); pinned {
		t.Fatal("pin must be released even though the PodGroup is gone (else it leaks)")
	}
}

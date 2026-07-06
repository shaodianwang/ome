package gangpack

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
)

func TestReserveIsNoop(t *testing.T) {
	g := &GangPack{}
	if s := g.Reserve(context.Background(), nil, &v1.Pod{}, "n1"); !s.IsSuccess() {
		t.Fatalf("Reserve = %v, want Success", s)
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

// TestUnreserveNonGangNoop: a pod that is not a gang member unwinds nothing.
func TestUnreserveNonGangNoop(t *testing.T) {
	victim := &fakeWaitingPod{pod: gangPod("team", "pf")}
	h := &fakeHandle{waiting: []framework.WaitingPod{victim}}
	g := &GangPack{handle: h, pins: placement.New(), pgReader: fakeReader{}} // empty reader -> not resolvable

	g.Unreserve(context.Background(), nil, gangPod("team", "x"), "node1")

	if victim.rejected {
		t.Fatal("non-gang Unreserve must not reject anyone")
	}
}

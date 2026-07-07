package gangpack

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
)

// TestPinGang is the PreFilter decision core (node slice in, pin + narrowed set
// out — no fake Handle): best-fit a domain for the gang, record the pin, and
// narrow candidates to that domain's nodes. A gang that fits nowhere is
// Unschedulable.
func TestPinGang(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	nodes := []*framework.NodeInfo{
		nodeInfo(gpuNode("n1", "a", "4")), nodeInfo(gpuNode("n2", "a", "4")), // a: 2 free
		nodeInfo(gpuNode("n3", "b", "4")), nodeInfo(gpuNode("n4", "b", "4")), nodeInfo(gpuNode("n5", "b", "4")), // b: 3 free
	}
	pod := gpuPod("4")
	st := framework.NewCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, pod)
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success", status)
	}
	// Best-fit: a (2 free) is fuller than b (3) and still fits a gang of 2.
	if res == nil || !res.NodeNames.Equal(sets.New("n1", "n2")) {
		t.Fatalf("narrowed to %v, want {n1,n2}", res.NodeNames)
	}
	if pin := readPin(st); pin == nil || pin.domain != "a" || pin.topologyKey != testKey {
		t.Fatalf("pin = %+v, want domain a", pin)
	}

	// A gang larger than any domain cannot be placed.
	_, s2 := g.pinGang(framework.NewCycleState(), nodes, gangInfo{key: "team/big", minMember: 10, topologyKey: testKey}, pod)
	if s2.IsSuccess() || s2.Code() != framework.Unschedulable {
		t.Fatalf("oversized gang status = %v, want Unschedulable", s2)
	}
}

// TestPinGangAdoptsBoundMembers: with the in-memory pin lost (restart) but a gang
// member already placed in a domain, pinGang adopts that domain instead of
// best-fitting fresh — even when a fresh best-fit would fail. Here domain "a" has
// one member already bound and one free node (2 nodes total), so a fresh best-fit
// for a gang of 3 fits nowhere; adoption pins "a" so the remaining members can
// still land with their sibling.
func TestPinGangAdoptsBoundMembers(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	placed := gangPod("team", "pf") // a member already bound in domain a
	nodes := []*framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), placed),
		nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	st := framework.NewCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangPod("team", "pf"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (adopt bound member's domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "a" {
		t.Fatalf("adopted pin = %q,%v, want a,true", d, ok)
	}
	if !g.pins.Adopted("team/pf") {
		t.Fatal("pin should be marked adopted")
	}
	if pin := readPin(st); pin == nil || pin.domain != "a" {
		t.Fatalf("cycle pin = %+v, want domain a", pin)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("a1", "a2")) {
		t.Fatalf("narrowed to %v, want {a1,a2}", res.NodeNames)
	}
}

// TestPreFilterSkips: PreFilter returns Skip (no constraint, Filter skipped) when
// the plugin has no PodGroup reader wired yet, or when the pod is not a gang
// member. Neither path touches the node snapshot, so no Handle is needed.
func TestPreFilterSkips(t *testing.T) {
	ctx := context.Background()

	nilReader := &GangPack{pins: placement.New()} // pgReader nil -> inert
	if _, st := nilReader.PreFilter(ctx, framework.NewCycleState(), gangPod("team", "x")); st.Code() != framework.Skip {
		t.Fatalf("nil-reader PreFilter = %v, want Skip", st)
	}

	nonGang := &GangPack{pins: placement.New(), pgReader: fakeReader{}} // reader present, pod not resolvable
	res, st := nonGang.PreFilter(ctx, framework.NewCycleState(), gangPod("team", "x"))
	if res != nil || st.Code() != framework.Skip {
		t.Fatalf("non-gang PreFilter = %v,%v, want nil,Skip", res, st)
	}
}

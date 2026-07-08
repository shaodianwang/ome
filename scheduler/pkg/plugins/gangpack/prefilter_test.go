package gangpack

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
	"github.com/ome-projects/ome/scheduler/pkg/topology"
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
	if !g.pins.Committed("team/pf") {
		t.Fatal("adopted pin should be committed (remaining==0)")
	}
	if pin := readPin(st); pin == nil || pin.domain != "a" {
		t.Fatalf("cycle pin = %+v, want domain a", pin)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("a1", "a2")) {
		t.Fatalf("narrowed to %v, want {a1,a2}", res.NodeNames)
	}
}

// TestPinGangReplansStalePin: a pin to a domain that no longer has any node (its
// nodes drained/scaled away, or the topology key changed so the old domain value
// matches nothing) is dropped and re-planned, instead of narrowing every
// candidate away and wedging the gang forever.
func TestPinGangReplansStalePin(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	g.pins.Set("team/pf", "gone") // pinned to a domain that no longer exists
	nodes := []*framework.NodeInfo{
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	st := framework.NewCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gpuPod("4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (re-plan off the stale domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("narrowed to %v, want {b1,b2}", res.NodeNames)
	}
}

// TestPinGangReplansOnFullDomain: a gang pinned (with a capacity reservation) to a
// domain that then FILLS — another gang won the race for it before this gang placed
// any member — must drop the pin, re-plan onto a free domain, AND release the
// reservation it leaked on the full domain. Without this the gang retries the full
// domain forever while free domains sit idle, and the leaked reservation
// permanently under-counts that domain for every other gang.
func TestPinGangReplansOnFullDomain(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	// pf pinned to "a" with a reservation, exactly as a first-member Choose would
	// when "a" and "b" both looked free.
	if d, ok := g.pins.Choose("team/pf", topology.FreeByDomain{"a": 2, "b": 2}, 2); !ok || d != "a" {
		t.Fatalf("precondition: Choose = %q,%v, want a,true", d, ok)
	}
	// Snapshot now: "a" is full (its node occupied by another gang's pod), "b" free.
	nodes := []*framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gpuPod("4")), // occupied by a non-pf pod
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	st := framework.NewCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (re-plan off the full domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("narrowed to %v, want {b1,b2}", res.NodeNames)
	}
	// The reservation pf leaked on "a" must be gone: a fresh gang can pin "a" once
	// it frees. If the reservation leaked, "a"'s effective free would be 0 here.
	if d, ok := g.pins.Choose("team/other", topology.FreeByDomain{"a": 2}, 2); !ok || d != "a" {
		t.Fatalf("reservation on 'a' leaked after re-plan: other gang Choose = %q,%v, want a,true", d, ok)
	}
}

// TestPinGangReplansOffCordonedDomain: a gang pinned to a domain whose nodes have
// become unschedulable (cordoned) — capacity intact but the framework's Filter
// would reject them — must re-plan onto a schedulable domain, not wedge there. This
// is the field wedge: freeByDomain now excludes unschedulable nodes, so pinStale
// sees the pinned domain as unusable and drops it.
func TestPinGangReplansOffCordonedDomain(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	g.pins.Set("team/pf", "a") // pinned to a
	nodes := []*framework.NodeInfo{
		nodeInfo(cordoned(gpuNode("a1", "a", "4"))), // a: cordoned (has gpu, unschedulable)
		nodeInfo(cordoned(gpuNode("a2", "a", "4"))),
		nodeInfo(gpuNode("b1", "b", "4")), // b: schedulable
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	_, status := g.pinGang(framework.NewCycleState(), nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (re-plan off the cordoned domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
}

// TestPinGangKeepsPinWhenMemberBound: a pinned domain that is full but already
// holds one of the gang's own members is NOT stale — re-planning would strand that
// member. The gang keeps its pin and waits for capacity in its own domain.
func TestPinGangKeepsPinWhenMemberBound(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	g.pins.Set("team/pf", "a")
	// Domain "a": one node holds a pf member (bound), a second node is full with
	// another gang's pod — so "a" cannot fit a fresh member, but pf lives here.
	nodes := []*framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")), // pf's own bound member
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")),           // occupied by another gang
		nodeInfo(gpuNode("b1", "b", "4")),                        // free elsewhere
	}
	st := framework.NewCycleState()

	_, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (keep pin, don't strand bound member)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "a" {
		t.Fatalf("pin = %q,%v, want a,true (must not re-plan away from a bound member)", d, ok)
	}
}

// TestPreFilterSkips: PreFilter returns Skip (no constraint, Filter skipped) when
// the plugin has no PodGroup reader wired yet, when the pod carries no pod-group
// label (not a gang member), or when its PodGroup is present but declares no domain
// gang (non-positive minMember, or no topology key). None of these touch the node
// snapshot, so no Handle is needed.
func TestPreFilterSkips(t *testing.T) {
	ctx := context.Background()

	nilReader := &GangPack{pins: placement.New()} // pgReader nil -> inert
	if _, st := nilReader.PreFilter(ctx, framework.NewCycleState(), gangPod("team", "x")); st.Code() != framework.Skip {
		t.Fatalf("nil-reader PreFilter = %v, want Skip", st)
	}

	// No pod-group label -> not a gang member -> Skip.
	nonMember := &GangPack{pins: placement.New(), pgReader: fakeReader{}}
	res, st := nonMember.PreFilter(ctx, framework.NewCycleState(), gangPod("team", ""))
	if res != nil || st.Code() != framework.Skip {
		t.Fatalf("non-member PreFilter = %v,%v, want nil,Skip", res, st)
	}

	// PodGroup present but declares no topology key -> nothing to pin -> Skip.
	noTopo := &GangPack{pins: placement.New(), pgReader: fakeReader{"team/x": {min: 3, topo: ""}}}
	res, st = noTopo.PreFilter(ctx, framework.NewCycleState(), gangPod("team", "x"))
	if res != nil || st.Code() != framework.Skip {
		t.Fatalf("no-topology-key PreFilter = %v,%v, want nil,Skip", res, st)
	}
}

// TestPreFilterHoldsWhenPodGroupMissing: a pod that IS a gang member (carries the
// pod-group label) but whose PodGroup the reader can't resolve yet — the common
// informer-lag race when a workload's pods and PodGroup are created together — must
// be held (Unschedulable), NOT skipped. Skipping would let a lone member bind and
// break all-or-nothing; holding requeues it until the PodGroup appears.
func TestPreFilterHoldsWhenPodGroupMissing(t *testing.T) {
	g := &GangPack{pins: placement.New(), pgReader: fakeReader{}} // reader present, group absent
	res, st := g.PreFilter(context.Background(), framework.NewCycleState(), gangPod("team", "pf"))
	if res != nil {
		t.Fatalf("PreFilter result = %v, want nil", res)
	}
	if st.Code() != framework.Unschedulable {
		t.Fatalf("PreFilter for uncached PodGroup = %v, want Unschedulable (hold, not Skip)", st)
	}
}

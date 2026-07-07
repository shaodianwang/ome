package gangpack

import (
	"context"
	"testing"

	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
	"github.com/ome-projects/ome/scheduler/pkg/topology"
)

// TestPermitCountsBoundSiblingsWhenCommitted: once a gang is fully committed
// (remaining==0), the gate counts already-BOUND members (not in the waiting set)
// toward minMember. This admits a member arriving into an already-running gang —
// controller churn/recreate, a pod beyond minMember, a preempted member
// re-gating, or a straggler after restart — instead of wedging it at the gate.
func TestPermitCountsBoundSiblingsWhenCommitted(t *testing.T) {
	// A 3-member gang, fully committed in domain a with 2 members already bound.
	pins := placement.New()
	pins.Set("team/pf", "a") // committed: remaining==0
	snapshot := []*framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")),
		nodeInfo(gpuNode("a2", "a", "4"), gangPod("team", "pf")),
	}
	g := &GangPack{
		handle:   &fakeHandle{snapshot: snapshot}, // no waiting siblings
		pins:     pins,
		pgReader: fakeReader{"team/pf": {min: 3, topo: testKey}},
	}

	// The arriving member has 0 waiting siblings but 2 bound; 2+0+1 = 3 = minMember.
	status, _ := g.Permit(context.Background(), framework.NewCycleState(), gangPod("team", "pf"), "a3")
	if status.IsWait() {
		t.Fatal("gate wedged: committed gang with 2 bound siblings + arriving member = 3 should admit, not wait")
	}
	if !status.IsSuccess() {
		t.Fatalf("Permit = %v, want Success", status)
	}
}

// TestPermitFormingGangDoesNotScanBound: a still-forming gang (remaining>0) never
// counts bound siblings — it holds until enough members wait — so all-or-nothing
// is preserved and the gate never pays the snapshot scan during normal formation.
func TestPermitFormingGangDoesNotScanBound(t *testing.T) {
	pins := placement.New()
	pins.Choose("team/pf", topology.FreeByDomain{"a": 5}, 3) // pinned, remaining==3, not committed
	// A snapshot that WOULD satisfy the gate if (wrongly) counted.
	snapshot := []*framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")),
		nodeInfo(gpuNode("a2", "a", "4"), gangPod("team", "pf")),
	}
	g := &GangPack{
		handle:   &fakeHandle{snapshot: snapshot},
		pins:     pins,
		pgReader: fakeReader{"team/pf": {min: 3, topo: testKey}},
	}
	status, _ := g.Permit(context.Background(), framework.NewCycleState(), gangPod("team", "pf"), "a3")
	if !status.IsWait() {
		t.Fatalf("forming gang (remaining>0) must wait, not count bound siblings; got %v", status)
	}
}

// TestGateDecision: the gang releases when the members already waiting plus the
// arriving one reach minMember — not before.
func TestGateDecision(t *testing.T) {
	cases := []struct {
		waiting, minMember int
		release            bool
	}{
		{0, 1, true},  // singleton gang: the first (only) member completes it
		{0, 3, false}, // 1st of 3 -> wait
		{1, 3, false}, // 2nd of 3 -> wait
		{2, 3, true},  // 3rd of 3 -> release all
		{5, 3, true},  // already over (flexible/extra) -> release
	}
	for _, c := range cases {
		if got := gateDecision(c.waiting, c.minMember); got != c.release {
			t.Fatalf("gateDecision(%d,%d) = %v, want %v", c.waiting, c.minMember, got, c.release)
		}
	}
}

func TestSameGang(t *testing.T) {
	if !sameGang(gangPod("team", "pf"), "team/pf") {
		t.Fatal("pod in team/pf should match gang key team/pf")
	}
	if sameGang(gangPod("team", "pf"), "team/decode") {
		t.Fatal("pod in team/pf must not match a different gang")
	}
	if sameGang(gangPod("other", "pf"), "team/pf") {
		t.Fatal("namespace must be part of the gang key")
	}
	if sameGang(gangPod("team", ""), "team/pf") {
		t.Fatal("non-gang pod matches nothing")
	}
}

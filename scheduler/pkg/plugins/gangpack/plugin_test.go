package gangpack

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// TestFilterAdmitsEveryNode is the walking-skeleton unit test: it proves the
// plugin implements the framework contract and that the unit-test tier (calling
// hooks directly against real framework types — the tier where the domain-choice
// logic will be TDD'd) works end to end. The assertion is trivial by design; it
// gets real once Filter starts rejecting nodes outside a gang's pinned domain.
func TestFilterAdmitsEveryNode(t *testing.T) {
	p := &GangPack{}

	if got := p.Name(); got != Name {
		t.Fatalf("Name() = %q, want %q", got, Name)
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}})
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1"}}

	if status := p.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter() = %v, want Success (skeleton admits every node)", status)
	}
}

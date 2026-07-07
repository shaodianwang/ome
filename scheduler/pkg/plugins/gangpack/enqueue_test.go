package gangpack

import (
	"context"
	"testing"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// TestEventsToRegister: the plugin must register the cluster events that unblock
// a rejected gang — Pod (siblings / capacity), Node (capacity), and the PodGroup
// CR (minMember). Without these, QueueingHints leaves a rejected member waiting
// out the scheduler's slow periodic flush.
func TestEventsToRegister(t *testing.T) {
	g := &GangPack{}
	events, err := g.EventsToRegister(context.Background())
	if err != nil {
		t.Fatalf("EventsToRegister error: %v", err)
	}

	byResource := map[framework.EventResource]framework.ActionType{}
	for _, e := range events {
		byResource[e.Event.Resource] = e.Event.ActionType
	}

	// Pod: a sibling arriving (Add) completes a gated gang; Delete frees capacity.
	if got := byResource[framework.Pod]; got&framework.Add == 0 || got&framework.Delete == 0 {
		t.Errorf("Pod event = %v, want at least Add|Delete", got)
	}
	// Node: new/updated capacity may let a placement-blocked gang fit.
	if got, ok := byResource[framework.Node]; !ok || got&framework.Add == 0 {
		t.Errorf("Node event = %v (present=%v), want at least Add", got, ok)
	}
	// PodGroup CR: a minMember change may make a gang admissible.
	pgGVK := framework.EventResource("podgroups.v1alpha1.scheduling.x-k8s.io")
	if got, ok := byResource[pgGVK]; !ok || got&framework.Update == 0 {
		t.Errorf("PodGroup event = %v (present=%v), want at least Update", got, ok)
	}
}

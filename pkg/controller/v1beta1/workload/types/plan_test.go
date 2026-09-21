package types

import "testing"

// A per-Instance topology key overrides the Component's; a row with none
// falls back so a plan need only name the exceptions.
func TestComponentPlanTopologyKeyForInstance(t *testing.T) {
	plan := ComponentPlan{
		TopologyKey:          "rack",
		InstanceTopologyKeys: map[int32]string{1: "island-a"},
	}
	if got := plan.TopologyKeyForInstance(1); got != "island-a" {
		t.Errorf("per-Instance key: got %q want %q", got, "island-a")
	}
	if got := plan.TopologyKeyForInstance(2); got != "rack" {
		t.Errorf("fallback: got %q want %q", got, "rack")
	}
}

func TestInstancePlanTotalPodsSumsEveryRunner(t *testing.T) {
	plan := InstancePlan{Runners: []RunnerPlan{{Size: 1}, {Size: 4}}}
	if got := plan.TotalPods(); got != 5 {
		t.Errorf("total pods: got %d want 5", got)
	}
	if got := (InstancePlan{}).TotalPods(); got != 0 {
		t.Errorf("no runners: got %d want 0", got)
	}
}

// The surge slot must avoid both the steady-state indices and the slots
// siblings claimed this pass but have not materialized yet: sharing one
// releases every source at once when the shared surge turns Ready.
func TestAllocateSurgeIndexExcludesClaimedSlots(t *testing.T) {
	claimed := int32(1)
	instances := []InstanceStatus{
		{Index: 0, Operation: &InstanceOperation{SurgeIndex: &claimed}},
		{Index: 2},
	}
	if got := AllocateSurgeIndex(instances); got != 3 {
		t.Errorf("lowest slot free of indices and claims: got %d want 3", got)
	}
	if got := AllocateSurgeIndex(nil); got != 0 {
		t.Errorf("an empty Component starts at 0: got %d", got)
	}
	if got := AllocateSurgeIndex([]InstanceStatus{{Index: 1}}); got != 0 {
		t.Errorf("a hole below the highest index is still the lowest free: got %d want 0", got)
	}
}

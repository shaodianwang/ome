package replay_test

import (
	"context"
	"sort"
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/escalation"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/replay"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// surgeCeilingScenarios are the scenarios that walk a corrected push over
// a replacement an earlier attempt left behind: one disposed with the pod
// left alive, one still open with the pod deleted under it. Each is held
// to the surge ceiling on every tick.
var surgeCeilingScenarios = []string{
	"surge-corrected-push-over-abandoned-replacement",
	"surge-corrected-push-abandons-crashed-replacement",
}

// TestCorrectedPushHoldsTheSurgeCeiling replays each ceiling scenario with
// the observer on and checks, after every pass, that the Component lists
// at most replicas plus maxSurge pods that are alive, and at most that many
// inside their deletion grace. A surge that opened beside a leftover
// replacement would show as a sixth pod.
func TestCorrectedPushHoldsTheSurgeCeiling(t *testing.T) {
	byName := map[string]*replay.Scenario{}
	for _, tc := range loadScenarios(t) {
		byName[tc.name] = tc.scenario
	}
	for _, name := range surgeCeilingScenarios {
		scenario, ok := byName[name]
		if !ok {
			t.Fatalf("scenario %s not found", name)
		}
		t.Run(name, func(t *testing.T) {
			checkSurgeCeiling(t, scenario)
		})
	}
}

// checkSurgeCeiling runs one scenario and fails on the first tick whose pod
// count, alive or inside the deletion grace, exceeds replicas plus maxSurge.
func checkSurgeCeiling(t *testing.T, scenario *replay.Scenario) {
	t.Helper()
	observe := func(view replay.TickView) {
		rolling := &types.RollingUpdate{MaxSurge: view.Spec.MaxSurge}
		budget := escalation.PerComponentMaxSurgeBudget(rolling, view.Spec.Replicas)
		if budget == escalation.BudgetNoLimit {
			t.Fatalf("tick %d: the scenario must cap maxSurge for the ceiling to mean anything", view.Tick)
		}
		ceiling := view.Spec.Replicas + budget
		var alive, withinGrace int32
		var names []string
		for _, pods := range view.Pods {
			for _, pod := range pods {
				names = append(names, pod.Name)
				if pod.DeletionTimestamp == nil {
					alive++
					withinGrace++
					continue
				}
				if view.Now.Before(pod.DeletionTimestamp.Time) {
					withinGrace++
				}
			}
		}
		sort.Strings(names)
		t.Logf("tick %d: alive=%d withinGrace=%d ceiling=%d pods=%v", view.Tick, alive, withinGrace, ceiling, names)
		if alive > ceiling {
			t.Errorf("tick %d: %d pods alive, ceiling %d (replicas %d plus maxSurge %d): %v",
				view.Tick, alive, ceiling, view.Spec.Replicas, budget, names)
		}
		if withinGrace > ceiling {
			t.Errorf("tick %d: %d pods inside their deletion grace, ceiling %d: %v", view.Tick, withinGrace, ceiling, names)
		}
	}
	if _, err := replay.Run(context.Background(), scenario, replay.Options{Observer: observe}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

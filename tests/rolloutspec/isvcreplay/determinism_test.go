package isvcreplay_test

import "testing"

// determinismRuns is how many times one scenario is replayed before its
// trace is believed: enough to catch a map-order or wall-clock dependence.
const determinismRuns = 10

// TestReplayIsDeterministic replays every scenario ten times and requires
// identical traces. A difference is a defect in an engine or the driver,
// never something to normalize away.
func TestReplayIsDeterministic(t *testing.T) {
	for _, tc := range loadScenarios(t) {
		t.Run(tc.name, func(t *testing.T) {
			first := runScenario(t, tc.scenario)
			for run := 2; run <= determinismRuns; run++ {
				got := runScenario(t, tc.scenario)
				if got != first {
					t.Fatalf("run %d differs from run 1\n--- run 1 ---\n%s--- run %d ---\n%s", run, first, run, got)
				}
			}
		})
	}
}

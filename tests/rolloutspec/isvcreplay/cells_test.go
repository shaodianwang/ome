package isvcreplay_test

import (
	"strings"
	"testing"
)

// TestCellCitationsResolve pins the join between a scenario and the four
// tables: every cell id a scenario claims to drive is a key of its
// machine's expanded grid. The count per machine is the coverage the lane
// reports.
func TestCellCitationsResolve(t *testing.T) {
	vocab := vocabularyOf(t)
	cited := map[string]int{}
	total := map[string]int{}
	for cell := range vocab.Cells {
		total[strings.SplitN(cell, "/", 2)[0]]++
	}
	for _, tc := range loadScenarios(t) {
		for _, cell := range tc.scenario.Cells {
			machine := strings.SplitN(cell, "/", 2)[0]
			if _, ok := vocab.Cells[cell]; !ok {
				t.Errorf("%s: cell %q is not a key of the expanded %s grid", tc.path, cell, machine)
				continue
			}
			cited[machine]++
		}
	}
	for _, machine := range machines {
		t.Logf("%s: cells cited %d of %d", machine, cited[machine], total[machine])
	}
}

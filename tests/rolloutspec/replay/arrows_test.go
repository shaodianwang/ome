package replay_test

import (
	"sort"
	"testing"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

// TestArrowCoverage joins the scenario headers with the machine's
// transitions and reports how much of the table the replay lane drives.
//
// It fails on a scenario naming an arrow the table does not declare —
// caught at load, asserted here so the failure names coverage rather than
// parsing. The exhaustive target ("every arrow driven") is the Part 1 exit
// criterion and is skipped, with the standing count printed, until the
// scenario set reaches it: a hard failure while the set is deliberately
// partial would make the lane red for its whole construction.
func TestArrowCoverage(t *testing.T) {
	machine, err := rolloutspec.Load(machineName)
	if err != nil {
		t.Fatalf("load %s machine: %v", machineName, err)
	}
	declared := make(map[string]struct{}, len(machine.Transitions))
	for _, transition := range machine.Transitions {
		declared[transition.ID] = struct{}{}
	}

	driven := map[string][]string{}
	for _, tc := range loadScenarios(t) {
		for _, arrow := range tc.scenario.Arrows {
			if _, ok := declared[arrow]; !ok {
				t.Errorf("%s: arrow %q is not declared in the %s table", tc.path, arrow, machineName)
				continue
			}
			driven[arrow] = append(driven[arrow], tc.name)
		}
	}

	missing := make([]string, 0, len(declared))
	for arrow := range declared {
		if len(driven[arrow]) == 0 {
			missing = append(missing, arrow)
		}
	}
	sort.Strings(missing)
	t.Logf("arrows driven: %d of %d", len(driven), len(declared))
	if len(missing) > 0 {
		t.Skipf("%d arrows are not driven yet; the first undriven ones are %s",
			len(missing), head(missing, 10))
	}
}

// TestCellCitationsResolve pins the other half of the join: a cell id a
// scenario claims to drive has to exist in the expanded grid.
func TestCellCitationsResolve(t *testing.T) {
	vocab := vocabularyOf(t)
	cited := 0
	for _, tc := range loadScenarios(t) {
		for _, cell := range tc.scenario.Cells {
			if _, ok := vocab.Cells[cell]; !ok {
				t.Errorf("%s: cell %q is not a key of the expanded %s grid", tc.path, cell, machineName)
				continue
			}
			cited++
		}
	}
	t.Logf("cells cited: %d of %d", cited, len(vocab.Cells))
}

func head(values []string, n int) []string {
	if len(values) <= n {
		return values
	}
	return values[:n]
}

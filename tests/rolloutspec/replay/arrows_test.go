package replay_test

import (
	"sort"
	"testing"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

// TestArrowCoverage joins the scenario headers with the machine's
// transitions and reports how much of the table the replay lane drives.
//
// Only workload-lane arrows count toward the exit: an adapter-lane arrow's
// guard reads something the adapter produces and workload.Reconcile never
// writes, so no scenario here can reach it honestly. Those are covered by
// tagged tests in the adapter package and are reported separately.
//
// It fails on a scenario naming an arrow the table does not declare —
// caught at load, asserted here so the failure names coverage rather than
// parsing — and on one claiming an adapter-lane arrow, which the driver
// cannot have reached. The exhaustive target ("every workload-lane arrow
// driven") is the Part 1 exit criterion and is skipped, naming what is
// still missing, until the scenario set reaches it.
func TestArrowCoverage(t *testing.T) {
	machine, err := rolloutspec.Load(machineName)
	if err != nil {
		t.Fatalf("load %s machine: %v", machineName, err)
	}
	lane := make(map[string]string, len(machine.Transitions))
	adapterLane := 0
	for _, transition := range machine.Transitions {
		lane[transition.ID] = transition.LaneOf()
		if transition.LaneOf() == rolloutspec.LaneAdapter {
			adapterLane++
		}
	}

	driven := map[string][]string{}
	for _, tc := range loadScenarios(t) {
		for _, arrow := range tc.scenario.Arrows {
			switch lane[arrow] {
			case "":
				t.Errorf("%s: arrow %q is not declared in the %s table", tc.path, arrow, machineName)
			case rolloutspec.LaneAdapter:
				t.Errorf("%s: arrow %q is adapter-lane; the replay driver does not run the adapter pass that decides it",
					tc.path, arrow)
			default:
				driven[arrow] = append(driven[arrow], tc.name)
			}
		}
	}

	missing := make([]string, 0, len(lane))
	for arrow, l := range lane {
		if l == rolloutspec.LaneWorkload && len(driven[arrow]) == 0 {
			missing = append(missing, arrow)
		}
	}
	sort.Strings(missing)
	workloadLane := len(lane) - adapterLane
	t.Logf("workload-lane arrows driven: %d of %d (%d adapter-lane arrows are covered in the adapter package)",
		len(driven), workloadLane, adapterLane)
	if len(missing) > 0 {
		t.Skipf("%d workload-lane arrows are not driven: %s", len(missing), missing)
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

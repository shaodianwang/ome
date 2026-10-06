package rolloutspec

import (
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// coverageFloorSuffix names a machine's floor file, which sits beside the
// machine directory: machines/<name>.coverage-floor.yaml. It lives outside
// the directory because the loader treats every file inside as a fragment.
const coverageFloorSuffix = ".coverage-floor.yaml"

// CoverageFloor is the checked-in high-water mark for a machine's open rows.
// Each number may go down, never up, so the table only moves toward covered
// and decided.
type CoverageFloor struct {
	// Uncovered bounds the authored rows with an outcome other than n/a
	// that name no check in coverage.
	Uncovered int `json:"uncovered"`
	// Undecided bounds the authored rows whose status is not decided.
	Undecided int `json:"undecided"`
	// SetOn is the date the numbers were last lowered.
	SetOn string `json:"set_on"`
}

// CoverageCount is what a machine's table holds today: the authored rows
// behind each floor number, in selector order, so a breach can name them.
type CoverageCount struct {
	Uncovered []*Cell
	Undecided []*Cell
}

// CountCoverage tallies m's authored rows with the validator's own notion
// of coverage: a row counts as uncovered when its outcome is not n/a and
// hasCoverage is false, and as undecided when its status is not decided.
func CountCoverage(m *Machine) CoverageCount {
	var out CoverageCount
	for i := range m.Cells {
		c := &m.Cells[i]
		if c.Outcome != OutcomeNA && !hasCoverage(c.Coverage) {
			out.Uncovered = append(out.Uncovered, c)
		}
		if c.Status != StatusDecided {
			out.Undecided = append(out.Undecided, c)
		}
	}
	sortCells(out.Uncovered)
	sortCells(out.Undecided)
	return out
}

func sortCells(cells []*Cell) {
	sort.Slice(cells, func(i, j int) bool { return cellSelector(cells[i]) < cellSelector(cells[j]) })
}

// CoverageFloors returns the names of the machines that carry a floor file,
// in name order.
func CoverageFloors() ([]string, error) {
	entries, err := fs.ReadDir(machinesFS, machinesDir)
	if err != nil {
		return nil, fmt.Errorf("rolloutspec: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), coverageFloorSuffix) {
			names = append(names, strings.TrimSuffix(e.Name(), coverageFloorSuffix))
		}
	}
	return names, nil
}

// LoadCoverageFloor reads machines/<name>.coverage-floor.yaml.
func LoadCoverageFloor(name string) (*CoverageFloor, error) {
	data, err := fs.ReadFile(machinesFS, path.Join(machinesDir, name+coverageFloorSuffix))
	if err != nil {
		return nil, fmt.Errorf("rolloutspec: %s: %w", name, err)
	}
	f, err := parseCoverageFloor(data)
	if err != nil {
		return nil, fmt.Errorf("rolloutspec: %s: coverage floor: %w", name, err)
	}
	return f, nil
}

func parseCoverageFloor(data []byte) (*CoverageFloor, error) {
	var f CoverageFloor
	if err := yaml.UnmarshalStrict(data, &f); err != nil {
		return nil, err
	}
	if f.Uncovered < 0 || f.Undecided < 0 {
		return nil, fmt.Errorf("uncovered and undecided must not be negative")
	}
	if f.SetOn == "" {
		return nil, fmt.Errorf("set_on is required")
	}
	return &f, nil
}

// Breaches compares a count with its floor and returns one message per
// number that rose above it, each naming every row behind the number so
// the rows a change added can be found among them. Equal or lower is clean.
func (f *CoverageFloor) Breaches(count CoverageCount) []string {
	var out []string
	if n := len(count.Uncovered); n > f.Uncovered {
		out = append(out, breach("uncovered", n, f.Uncovered, count.Uncovered))
	}
	if n := len(count.Undecided); n > f.Undecided {
		out = append(out, breach("undecided", n, f.Undecided, count.Undecided))
	}
	return out
}

func breach(what string, got, floor int, rows []*Cell) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %s rows, floor %d (+%d); the floor only goes down, so cover or decide the rows this change added. By state:\n", got, what, floor, got-floor)
	byState := map[string]int{}
	for _, c := range rows {
		byState[c.State]++
	}
	states := make([]string, 0, len(byState))
	for s := range byState {
		states = append(states, s)
	}
	sort.Strings(states)
	for _, s := range states {
		fmt.Fprintf(&b, "  %s: %d\n", s, byState[s])
	}
	b.WriteString("Rows:\n")
	for _, c := range rows {
		b.WriteString("  " + cellSelector(c) + "\n")
	}
	return b.String()
}

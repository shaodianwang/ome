package replay_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/replay"
	"github.com/ome-projects/ome/tests/rolloutspec"
)

// machineName is the behavior table the replay scenarios are written
// against.
const machineName = "instance"

var update = flag.Bool("update", false, "rewrite the golden traces from this run")

// loaded is one scenario file, parsed against the table.
type loaded struct {
	name     string
	path     string
	golden   string
	scenario *replay.Scenario
}

// vocabularyOf projects the machine's events, transitions and expanded
// grid into the check the scenario loader applies. Going through the
// repository's own loader is what keeps a scenario and the table from
// drifting: an event, variant, arrow or cell the table does not declare
// cannot appear in a scenario.
func vocabularyOf(t *testing.T) replay.Vocabulary {
	t.Helper()
	machine, err := rolloutspec.Load(machineName)
	if err != nil {
		t.Fatalf("load %s machine: %v", machineName, err)
	}
	grid, err := machine.Expand()
	if err != nil {
		t.Fatalf("expand %s machine: %v", machineName, err)
	}
	vocab := replay.Vocabulary{
		Events: make(map[string][]string, len(machine.Events)),
		Arrows: make(map[string]struct{}, len(machine.Transitions)),
		Cells:  make(map[string]struct{}, len(grid.Cells)),
	}
	for _, event := range machine.Events {
		vocab.Events[event.ID] = event.Variants
	}
	for _, transition := range machine.Transitions {
		vocab.Arrows[transition.ID] = struct{}{}
	}
	for _, cell := range grid.Cells {
		vocab.Cells[machine.Machine+"/"+cell.Key()] = struct{}{}
	}
	return vocab
}

// loadScenarios parses every scenario file, in name order.
func loadScenarios(t *testing.T) []loaded {
	t.Helper()
	vocab := vocabularyOf(t)
	paths, err := filepath.Glob(filepath.Join("scenarios", "*.yaml"))
	if err != nil {
		t.Fatalf("glob scenarios: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("no scenarios found; the replay lane proves nothing without them")
	}
	sort.Strings(paths)
	out := make([]loaded, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		scenario, err := replay.Parse(data, vocab)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		base := strings.TrimSuffix(filepath.Base(path), ".yaml")
		if base != scenario.Scenario {
			t.Fatalf("%s: declares scenario %q; the file name is the scenario name", path, scenario.Scenario)
		}
		out = append(out, loaded{
			name:     base,
			path:     path,
			golden:   filepath.Join("golden", base+".trace"),
			scenario: scenario,
		})
	}
	return out
}

func runScenario(t *testing.T, s *replay.Scenario) string {
	t.Helper()
	trace, err := replay.Run(context.Background(), s, replay.Options{})
	if err != nil {
		t.Fatalf("%s: run: %v\npartial trace:\n%s", s.Scenario, err, trace)
	}
	return trace
}

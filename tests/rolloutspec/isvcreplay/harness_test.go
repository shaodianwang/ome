package isvcreplay_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/replay"
	"github.com/ome-projects/ome/tests/rolloutspec"
)

// machines are the behavior tables the InferenceService pass drives in one
// tick, and the only ones a scenario here may cite.
var machines = []string{"run", "coordination", "canary", "revision"}

var update = flag.Bool("update", false, "rewrite the golden traces from this run")

// loaded is one scenario file, parsed against the tables.
type loaded struct {
	name     string
	path     string
	golden   string
	scenario *replay.Scenario
}

// vocabularyOf projects the four machines' events and expanded grids into
// the check the scenario loader applies. An event's variants are the union
// of what the machines declaring it declare; a cell id carries its
// machine's prefix.
func vocabularyOf(t *testing.T) replay.Vocabulary {
	t.Helper()
	vocab := replay.Vocabulary{Events: map[string][]string{}, Cells: map[string]struct{}{}}
	for _, name := range machines {
		machine, err := rolloutspec.Load(name)
		if err != nil {
			t.Fatalf("load %s machine: %v", name, err)
		}
		grid, err := machine.Expand()
		if err != nil {
			t.Fatalf("expand %s machine: %v", name, err)
		}
		for _, event := range machine.Events {
			declared := vocab.Events[event.ID]
			for _, variant := range event.Variants {
				if !containsString(declared, variant) {
					declared = append(declared, variant)
				}
			}
			if declared == nil {
				declared = []string{}
			}
			vocab.Events[event.ID] = declared
		}
		for _, cell := range grid.Cells {
			vocab.Cells[machine.Machine+"/"+cell.Key()] = struct{}{}
		}
	}
	return vocab
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
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

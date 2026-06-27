// Package checker holds the `go test` gate that asserts a multi-cluster
// placement harness run's artifacts (summary.yaml + cmd.stats.yaml) against the
// checked-in rangespec.yaml. It is run as a separate step AFTER the runner
// produces the artifacts:
//
//	go test ./tests/perf/multicluster/checker/... \
//	  -args -summary <out>/summary.yaml \
//	        -cmd-stats <out>/cmd.stats.yaml \
//	        -range tests/perf/multicluster/configs/baseline/rangespec.yaml
//
// Splitting the gate from the runner (rather than asserting inline) mirrors the
// InferenceService perf harness: the runner is a plain binary that can be run
// for profiling without a pass/fail verdict, and the checker is a standard Go
// test that CI and humans invoke the same way.
package checker

import (
	"flag"
	"os"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/tests/perf/multicluster/runner/recorder"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/stats"
)

var (
	summaryFile  = flag.String("summary", "", "path to the runner's summary.yaml")
	cmdStatsFile = flag.String("cmd-stats", "", "path to the runner's cmd.stats.yaml")
	rangeFile    = flag.String("range", "", "path to the expectations rangespec.yaml")
)

// ClassRange is the per-class slice of the rangespec.
type ClassRange struct {
	// MaxP95PlacementMs is the ceiling on the class's p95 placement latency
	// (create -> Placed). Calibrated as (observed p95) * 1.2.
	MaxP95PlacementMs int64 `json:"maxP95PlacementMs"`
}

// RangeSpec is the checked-in expectations file. See the YAML header for the
// calibration recipe behind each number.
type RangeSpec struct {
	Cmd struct {
		MaxWallMs int64 `json:"maxWallMs"`
	} `json:"cmd"`

	// MinPlacedFraction is the convergence floor: totalPlaced/expectedTotal must
	// be at least this in BOTH passes. Calibrated as (observed fraction) * 0.93.
	MinPlacedFraction float64 `json:"minPlacedFraction"`

	// MaxFunnelConvergenceP95Ms bounds the funnel pass's p95 status-convergence
	// latency (member admits winner -> source status reflects it).
	MaxFunnelConvergenceP95Ms int64 `json:"maxFunnelConvergenceP95Ms"`

	// MinConvergenceDeltaMeanMs is the floor on the headline funnel win: the
	// poll-only convergence mean MUST exceed the funnel convergence mean by at
	// least this much, proving the funnel is meaningfully faster. A
	// non-positive calibrated value would mean the funnel showed no benefit on
	// the target hardware, which is itself a signal worth surfacing.
	MinConvergenceDeltaMeanMs int64 `json:"minConvergenceDeltaMeanMs"`

	Classes map[string]ClassRange `json:"classes"`
}

func TestScalability(t *testing.T) {
	if *summaryFile == "" || *cmdStatsFile == "" || *rangeFile == "" {
		t.Skip("checker requires -summary, -cmd-stats and -range; skipping (no artifacts to check)")
	}

	summary := readYAML[recorder.Summary](t, *summaryFile, "summary")
	cmdStats := readYAML[stats.CmdStats](t, *cmdStatsFile, "cmd stats")
	rangeSpec := readYAML[RangeSpec](t, *rangeFile, "range spec")

	t.Run("CommandWallTime", func(t *testing.T) {
		if rangeSpec.Cmd.MaxWallMs <= 0 {
			t.Fatalf("rangespec cmd.maxWallMs must be > 0 (got %d)", rangeSpec.Cmd.MaxWallMs)
		}
		if cmdStats.WallMs > rangeSpec.Cmd.MaxWallMs {
			t.Errorf("wall time %dms exceeds max %dms", cmdStats.WallMs, rangeSpec.Cmd.MaxWallMs)
		}
	})

	t.Run("NoDroppedEvents", func(t *testing.T) {
		// A non-zero drop count means the recorder was saturated and the latency
		// numbers may understate reality — never let that pass as a clean baseline.
		if summary.DroppedEvents > 0 {
			t.Errorf("recorder dropped %d events; results are unreliable, raise -recorder-buffer",
				summary.DroppedEvents)
		}
	})

	t.Run("Convergence", func(t *testing.T) {
		if summary.ExpectedTotalPerPass <= 0 {
			t.Fatalf("summary expectedTotalPerPass must be > 0 (got %d)", summary.ExpectedTotalPerPass)
		}
		for _, p := range []struct {
			name   string
			placed int
		}{
			{"funnel", summary.TotalPlacedFunnel},
			{"poll", summary.TotalPlacedPoll},
		} {
			frac := float64(p.placed) / float64(summary.ExpectedTotalPerPass)
			if frac < rangeSpec.MinPlacedFraction {
				t.Errorf("%s pass: only %d/%d (%.3f) sources reached Placed; below floor %.3f",
					p.name, p.placed, summary.ExpectedTotalPerPass, frac, rangeSpec.MinPlacedFraction)
			}
		}
	})

	t.Run("FunnelConvergenceLatency", func(t *testing.T) {
		if rangeSpec.MaxFunnelConvergenceP95Ms <= 0 {
			t.Fatalf("rangespec maxFunnelConvergenceP95Ms must be > 0")
		}
		got := summary.FunnelEnabled.Convergence
		if got.Count == 0 {
			t.Fatalf("funnel pass recorded zero convergence samples")
		}
		if got.P95 > rangeSpec.MaxFunnelConvergenceP95Ms {
			t.Errorf("funnel convergence p95 %dms exceeds max %dms (mean %dms over %d samples)",
				got.P95, rangeSpec.MaxFunnelConvergenceP95Ms, got.Mean, got.Count)
		}
	})

	t.Run("FunnelBeatsPoll", func(t *testing.T) {
		// The headline assertion: the funnel's event-driven convergence must be
		// meaningfully faster than the poll-only backstop. This is the number the
		// whole harness exists to prove.
		if rangeSpec.MinConvergenceDeltaMeanMs <= 0 {
			t.Fatalf("rangespec minConvergenceDeltaMeanMs must be > 0 (the funnel must show a win)")
		}
		if summary.FunnelDisabled.Convergence.Count == 0 {
			t.Fatalf("poll pass recorded zero convergence samples")
		}
		if summary.ConvergenceDeltaMeanMs < rangeSpec.MinConvergenceDeltaMeanMs {
			t.Errorf("funnel convergence win too small: delta mean %dms below floor %dms (funnel %dms vs poll %dms)",
				summary.ConvergenceDeltaMeanMs, rangeSpec.MinConvergenceDeltaMeanMs,
				summary.FunnelEnabled.Convergence.Mean, summary.FunnelDisabled.Convergence.Mean)
		}
	})

	t.Run("ClassPlacementLatency", func(t *testing.T) {
		// Every recorded class must have a threshold, and vice versa — a missing
		// entry on either side is a calibration drift bug, not a silently-ignored
		// class.
		for name, cs := range summary.Classes {
			t.Run(name, func(t *testing.T) {
				rng, ok := rangeSpec.Classes[name]
				if !ok {
					t.Fatalf("class %q present in summary but absent from rangespec", name)
				}
				if rng.MaxP95PlacementMs <= 0 {
					t.Fatalf("rangespec class %q maxP95PlacementMs must be > 0", name)
				}
				if cs.Placement.Count == 0 {
					t.Fatalf("class %q has zero recorded placements", name)
				}
				if cs.Placement.P95 > rng.MaxP95PlacementMs {
					t.Errorf("class %q placement p95 %dms exceeds max %dms (max single %dms over %d sources)",
						name, cs.Placement.P95, rng.MaxP95PlacementMs, cs.Placement.Max, cs.Placement.Count)
				}
			})
		}
		for name := range rangeSpec.Classes {
			if _, ok := summary.Classes[name]; !ok {
				t.Errorf("class %q present in rangespec but absent from summary (did the run produce it?)", name)
			}
		}
	})
}

// readYAML strict-unmarshals a YAML file into T, failing the test on any read or
// parse error. Strict parsing catches a stale field name in either artifact or
// the rangespec before it can mask a real regression.
func readYAML[T any](t *testing.T, path, what string) T {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s %q: %s", what, path, err)
	}
	var out T
	if err := yaml.UnmarshalStrict(b, &out); err != nil {
		t.Fatalf("parsing %s %q: %s", what, path, err)
	}
	return out
}

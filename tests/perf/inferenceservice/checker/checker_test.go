// Package checker holds the `go test` gate that asserts a harness run's
// artifacts (summary.yaml + cmd.stats.yaml) against the checked-in
// rangespec.yaml. It is run as a separate step AFTER the runner produces
// the artifacts:
//
//	go test ./tests/perf/inferenceservice/checker/... \
//	  -args -summary <out>/summary.yaml \
//	        -cmd-stats <out>/cmd.stats.yaml \
//	        -range tests/perf/inferenceservice/configs/baseline/rangespec.yaml
//
// Splitting the gate from the runner (rather than asserting inline) mirrors
// kueue's performance harness: the runner is a plain binary that can be
// run for profiling without a pass/fail verdict, and the checker is a
// standard Go test that CI and humans invoke the same way.
package checker

import (
	"flag"
	"os"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/recorder"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/stats"
)

var (
	summaryFile  = flag.String("summary", "", "path to the runner's summary.yaml")
	cmdStatsFile = flag.String("cmd-stats", "", "path to the runner's cmd.stats.yaml")
	rangeFile    = flag.String("range", "", "path to the expectations rangespec.yaml")
)

// ClassRange is the per-class slice of the rangespec.
type ClassRange struct {
	// MaxAvgTimeToReadyMs is the ceiling on the class's mean
	// time-to-Ready. Calibrated as (observed mean) * 1.2.
	MaxAvgTimeToReadyMs int64 `json:"maxAvgTimeToReadyMs"`
}

// RangeSpec is the checked-in expectations file. See the YAML header for
// the calibration recipe behind each number.
type RangeSpec struct {
	Cmd struct {
		MaxWallMs int64 `json:"maxWallMs"`
	} `json:"cmd"`
	// MinReadyFraction is the convergence floor: totalReady/expectedTotal
	// must be at least this. Calibrated as (observed mean) * 0.93.
	MinReadyFraction float64               `json:"minReadyFraction"`
	Classes          map[string]ClassRange `json:"classes"`
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
		// A non-zero drop count means the recorder was saturated and the
		// latency numbers may understate reality — never let that pass as a
		// clean baseline.
		if summary.DroppedEvents > 0 {
			t.Errorf("recorder dropped %d readiness events; results are unreliable, raise -recorder-buffer",
				summary.DroppedEvents)
		}
	})

	t.Run("Convergence", func(t *testing.T) {
		if summary.ExpectedTotal <= 0 {
			t.Fatalf("summary expectedTotal must be > 0 (got %d)", summary.ExpectedTotal)
		}
		frac := float64(summary.TotalReady) / float64(summary.ExpectedTotal)
		if frac < rangeSpec.MinReadyFraction {
			t.Errorf("only %d/%d (%.3f) InferenceServices reached Ready; below floor %.3f",
				summary.TotalReady, summary.ExpectedTotal, frac, rangeSpec.MinReadyFraction)
		}
	})

	t.Run("ClassTimeToReady", func(t *testing.T) {
		// Every recorded class must have a threshold, and vice versa — a
		// missing entry on either side is a calibration drift bug, not a
		// silently-ignored class.
		for name, cs := range summary.Classes {
			t.Run(name, func(t *testing.T) {
				rng, ok := rangeSpec.Classes[name]
				if !ok {
					t.Fatalf("class %q present in summary but absent from rangespec", name)
				}
				if rng.MaxAvgTimeToReadyMs <= 0 {
					t.Fatalf("rangespec class %q maxAvgTimeToReadyMs must be > 0", name)
				}
				if cs.Count == 0 {
					t.Fatalf("class %q has zero recorded services", name)
				}
				if cs.AverageTimeToReadyMs > rng.MaxAvgTimeToReadyMs {
					t.Errorf("class %q avg time-to-Ready %dms exceeds max %dms (max single %dms over %d services)",
						name, cs.AverageTimeToReadyMs, rng.MaxAvgTimeToReadyMs, cs.MaxTimeToReadyMs, cs.Count)
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

// readYAML strict-unmarshals a YAML file into T, failing the test on any
// read or parse error. Strict parsing catches a stale field name in
// either artifact or the rangespec before it can mask a real regression.
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

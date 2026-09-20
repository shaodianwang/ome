package replay_test

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReplayMatchesGolden is the behavior lock itself: every scenario is
// replayed against the engine at this commit and its trace compared
// byte-for-byte with the recorded one. A diff means the engine writes
// something different than it did when the golden was captured — which is
// either the point of the change, and the golden is updated with a table
// cell change or an `intentional:` note in the same commit, or a
// regression.
//
// Run with -update to rewrite the goldens from the current engine.
func TestReplayMatchesGolden(t *testing.T) {
	for _, tc := range loadScenarios(t) {
		t.Run(tc.name, func(t *testing.T) {
			got := runScenario(t, tc.scenario)
			if *update {
				if err := os.MkdirAll(filepath.Dir(tc.golden), 0o755); err != nil {
					t.Fatalf("create golden dir: %v", err)
				}
				if err := os.WriteFile(tc.golden, []byte(got), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}
			want, err := os.ReadFile(tc.golden)
			if err != nil {
				t.Fatalf("read golden (re-run with -update to record it): %v\ntrace:\n%s", err, got)
			}
			if got != string(want) {
				t.Fatalf("trace differs from %s\n--- want ---\n%s--- got ---\n%s", tc.golden, want, got)
			}
		})
	}
}

// TestNoOrphanGoldens keeps a deleted or renamed scenario from leaving a
// golden behind. An orphan proves nothing and reads like coverage, which
// is worse than no file at all.
func TestNoOrphanGoldens(t *testing.T) {
	expected := map[string]struct{}{}
	for _, tc := range loadScenarios(t) {
		expected[filepath.Base(tc.golden)] = struct{}{}
	}
	found, err := filepath.Glob(filepath.Join("golden", "*.trace"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	for _, path := range found {
		if _, ok := expected[filepath.Base(path)]; !ok {
			t.Errorf("%s has no scenario; delete it or restore the scenario it belongs to", path)
		}
	}
	if len(found) != len(expected) {
		t.Errorf("golden count %d does not match scenario count %d", len(found), len(expected))
	}
}

// Package stats defines the process-level command statistics the runner
// writes and the checker reads. Kept separate from the recorder's
// placement-latency summary so the two artifacts (cmd.stats.yaml and
// summary.yaml) have independent, narrow schemas.
package stats

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// CmdStats captures process-level resource usage of the runner. The
// checker gates on WallMs so a regression that inflates total run time is
// caught even when per-class placement latencies stay individually within
// budget. Peak RSS is informational and especially relevant here: the
// dominant memory cost of this harness is the N in-process member
// apiservers, so a regression that adds members or leaks remote caches
// shows up in Maxrss.
type CmdStats struct {
	WallMs int64 `json:"wallMs"`
	UserMs int64 `json:"userMs"`
	SysMs  int64 `json:"sysMs"`
	// Maxrss is peak resident set size; unit is platform-dependent (KiB
	// on Linux, bytes on Darwin) and -1 when unavailable. Informational
	// only — the checker does not gate on it because of the unit split.
	Maxrss int64 `json:"maxrss"`
}

// Write marshals cs to path as YAML.
func Write(path string, cs CmdStats) error {
	b, err := yaml.Marshal(cs)
	if err != nil {
		return fmt.Errorf("marshal cmd stats: %w", err)
	}
	return os.WriteFile(path, b, 0o644)
}

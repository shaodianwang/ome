// Package stats defines the process-level command statistics the runner
// writes and the checker reads. Kept separate from the recorder's
// time-to-Ready summary so the two artifacts (cmd.stats.yaml and
// summary.yaml) have independent, narrow schemas.
package stats

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// CmdStats captures process-level resource usage of the runner. The
// checker gates on WallMs so a reconcile-throughput regression that
// inflates total run time is caught even when per-class latencies stay
// individually within budget.
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

//go:build !darwin && !linux

package main

import "github.com/ome-projects/ome/tests/perf/inferenceservice/runner/stats"

// cmdResourceUsage falls back to wall-time-only stats on platforms
// without a getrusage binding. WallMs is filled in by the caller.
func cmdResourceUsage() stats.CmdStats {
	return stats.CmdStats{Maxrss: -1}
}

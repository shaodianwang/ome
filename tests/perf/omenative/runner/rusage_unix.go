//go:build darwin || linux

package main

import (
	"syscall"

	"github.com/ome-projects/ome/tests/perf/omenative/runner/stats"
)

// cmdResourceUsage reports the process's user/sys CPU time and peak RSS
// via getrusage. WallMs is filled in by the caller. Maxrss's unit is
// platform-dependent (KiB on Linux, bytes on Darwin); the checker treats
// it as informational only.
func cmdResourceUsage() stats.CmdStats {
	cs := stats.CmdStats{Maxrss: -1}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return cs
	}
	cs.UserMs = timevalMs(ru.Utime)
	cs.SysMs = timevalMs(ru.Stime)
	cs.Maxrss = int64(ru.Maxrss)
	return cs
}

func timevalMs(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1000 + int64(tv.Usec)/1000
}

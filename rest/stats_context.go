package rest

import (
	"time"
	"os"
	"github.com/elastic/gosigar"
)

// Group the stats related context that is associated w/ a ServerContext into a struct
type statsContext struct {
	statsLoggingTicker *time.Ticker
	cpuStatsSnapshot   *cpuStatsSnapshot
}

// A snapshot of the cpu stats that are used for stats calculation
type cpuStatsSnapshot struct {

	// The cumulative CPU time that's been used in various categories, in units of jiffies (clock ticks).
	// This spans all processes on the system.
	totalTimeJiffies uint64

	// CPU time spent in user code, measured in clock ticks.  Only applies to this process.
	procUserTimeJiffies uint64

	// CPU time spent in kernel code, measured in clock ticks.  Only applies to this process.
	procSystemTimeJiffies uint64
}

// Create a new cpu stats snapshot based on calling gosigar
func newCpuStatsSnapshot() (snapshot *cpuStatsSnapshot, err error) {

	snapshot = &cpuStatsSnapshot{}

	// Get the PID of this process
	pid := os.Getpid()

	// Find the total CPU time in jiffies for the machine
	cpu := gosigar.Cpu{}
	if err := cpu.Get(); err != nil {
		return nil, err
	}
	snapshot.totalTimeJiffies = cpu.Total()

	// Find the per-process CPU stats: user time and system time
	procTime := gosigar.ProcTime{}
	if err := procTime.Get(pid); err != nil {
		return nil, err
	}
	snapshot.procUserTimeJiffies = procTime.User
	snapshot.procSystemTimeJiffies = procTime.Sys

	return snapshot, nil

}
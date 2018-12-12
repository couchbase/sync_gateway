package rest

import (
	"time"
	"os"
	"github.com/elastic/gosigar"
)

type statsContext struct {
	statsLoggingTicker *time.Ticker
	cpuStatsSnapshot   *cpuStatsSnapshot
}

type cpuStatsSnapshot struct {

	// The cumulative CPU time that's been used in various categories, in units of jiffies (clock ticks).
	// This spans all processes on the system.
	totalTimeJiffies uint64

	// CPU time spent in user code, measured in clock ticks.  Only applies to this process.
	procUserTimeJiffies uint64

	// CPU time spent in kernel code, measured in clock ticks.  Only applies to this process.
	procSystemTimeJiffies uint64
}

func newCpuStatsSnapshot() (snapshot *cpuStatsSnapshot, err error) {

	snapshot = &cpuStatsSnapshot{}

	pid := os.Getpid()

	// ----------------- Sample 1

	pids := gosigar.ProcList{}
	if err := pids.Get(); err != nil {
		return nil, err
	}

	// Find the sync gateway PID  (/proc/pid/self/?)

	// Invoke func (self *ProcTime) Get(pid int) error {

	cpu := gosigar.Cpu{}
	if err := cpu.Get(); err != nil {
		return nil, err
	}
	snapshot.totalTimeJiffies = cpu.Total()

	procTime := gosigar.ProcTime{}
	if err := procTime.Get(pid); err != nil {
		return nil, err
	}
	snapshot.procUserTimeJiffies = procTime.User
	snapshot.procSystemTimeJiffies = procTime.Sys

	return snapshot, nil

}
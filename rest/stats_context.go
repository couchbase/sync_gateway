package rest

import (
	"expvar"
	"github.com/couchbase/sync_gateway/base"
	"runtime"
	"sync/atomic"
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


// Calculate the percentage of CPU used by this process over the sampling time specified in statsLogFrequencySecs
//
// Based on the accepted answer by "caf" in: https://stackoverflow.com/questions/1420426/how-to-calculate-the-cpu-usage-of-a-process-by-pid-in-linux-from-c
//
// This has a minor variation, and rather than directly:
//
// - Collect stats sample
// - Sleep for sample time (1s in the SO post, but that's fairly arbitrary)
// - Collect stats sample again
// - Calculate process cpu percentage over sample period
//
// It uses the same time.Ticker as for the other stats collection, and stores a previous stats sample and compares
// the current against the previous to calcuate the cpu percentage.  If it's the first time it's invoked, there
// won't be a previous value and so it will record 0.0 as the cpu percentage in that case.
func (statsContext *statsContext) calculateProcessCpuPercentage() (cpuPercentUtilization float64, err error) {

	// Get current value
	currentSnapshot, err := newCpuStatsSnapshot()
	if err != nil {
		return 0, err
	}

	// Is there a previous value?  If not, store current value as previous value and don't log a stat
	if statsContext.cpuStatsSnapshot == nil {
		statsContext.cpuStatsSnapshot = currentSnapshot
		return 0, nil
	}

	// Otherwise calculate the cpu percentage based on current vs previous
	prevSnapshot := statsContext.cpuStatsSnapshot

	// The delta in user time for the process
	deltaUserTimeJiffies := float64(currentSnapshot.procUserTimeJiffies - prevSnapshot.procUserTimeJiffies)

	// The delta in system time for the process
	deltaSystemTimeJiffies := float64(currentSnapshot.procSystemTimeJiffies - prevSnapshot.procSystemTimeJiffies)

	// The combined delta of user + system time for the process
	deltaSgProcessTimeJiffies := deltaUserTimeJiffies + deltaSystemTimeJiffies

	// The delta in total time for the machine
	deltaTotalTimeJiffies := float64(currentSnapshot.totalTimeJiffies - prevSnapshot.totalTimeJiffies)

	// Calculate the CPU usage percentage for the SG process
	cpuPercentUtilization = 100 * deltaSgProcessTimeJiffies / deltaTotalTimeJiffies

	// Store the current values as the previous values for the next time this function is called
	statsContext.cpuStatsSnapshot = currentSnapshot

	return cpuPercentUtilization, nil

}


func (statsContext *statsContext) addProcessCpuPercentage() error {

	statsResourceUtilization := base.StatsResourceUtilization()

	// Calculate the cpu percentage for the process
	cpuPercentUtilization, err := statsContext.calculateProcessCpuPercentage()
	if err != nil {
		return err
	}

	// Record stat
	statsResourceUtilization.Set(base.StatKeyProcessCpuPercentUtilization, base.ExpvarFloatVal(cpuPercentUtilization))

	return nil

}

func (statsContext *statsContext) addProcessMemoryPercentage() error {

	statsResourceUtilization := base.StatsResourceUtilization()

	pid := os.Getpid()

	procMem := gosigar.ProcMem{}
	if err := procMem.Get(pid); err != nil {
		return err
	}

	totalMem := gosigar.Mem{}
	if err := totalMem.Get(); err != nil {
		return err
	}

	// Record stats
	statsResourceUtilization.Set(base.StatKeyProcessMemoryResident, base.ExpvarFloatVal(float64(procMem.Resident)))
	statsResourceUtilization.Set(base.StatKeySystemMemoryTotal, base.ExpvarFloatVal(float64(totalMem.Total)))

	return nil

}


func (statsContext *statsContext) addGoSigarStats() error {

	if err := statsContext.addProcessCpuPercentage(); err != nil {
		return err
	}

	if err := statsContext.addProcessMemoryPercentage(); err != nil {
		return err
	}

	return nil

}

func AddGoRuntimeStats() {

	statsResourceUtilization := base.StatsResourceUtilization()

	// Num goroutines
	statsResourceUtilization.Set(base.StatKeyNumGoroutines, base.ExpvarIntVal(runtime.NumGoroutine()))

	recordGoroutineHighwaterMark(statsResourceUtilization, uint64(runtime.NumGoroutine()))

	// Read memstats (relatively expensive)
	memstats := runtime.MemStats{}
	runtime.ReadMemStats(&memstats)

	// Sys
	statsResourceUtilization.Set(base.StatKeyGoMemstatsSys, base.ExpvarUInt64Val(memstats.Sys))

	// HeapAlloc
	statsResourceUtilization.Set(base.StatKeyGoMemstatsHeapAlloc, base.ExpvarUInt64Val(memstats.HeapAlloc))

	// HeapIdle
	statsResourceUtilization.Set(base.StatKeyGoMemstatsHeapIdle, base.ExpvarUInt64Val(memstats.HeapIdle))

	// HeapInuse
	statsResourceUtilization.Set(base.StatKeyGoMemstatsHeapInUse, base.ExpvarUInt64Val(memstats.HeapInuse))

	// HeapReleased
	statsResourceUtilization.Set(base.StatKeyGoMemstatsHeapReleased, base.ExpvarUInt64Val(memstats.HeapReleased))

	// StackInuse
	statsResourceUtilization.Set(base.StatKeyGoMemstatsStackInUse, base.ExpvarUInt64Val(memstats.StackInuse))

	// StackSys
	statsResourceUtilization.Set(base.StatKeyGoMemstatsStackSys, base.ExpvarUInt64Val(memstats.StackSys))

	// PauseTotalNs
	statsResourceUtilization.Set(base.StatKeyGoMemstatsPauseTotalNs, base.ExpvarUInt64Val(memstats.PauseTotalNs))

}


// Record Goroutines high watermark into expvars
func recordGoroutineHighwaterMark(stats *expvar.Map, numGoroutines uint64) (maxGoroutinesSeen uint64) {

	maxGoroutinesSeen = atomic.LoadUint64(&base.MaxGoroutinesSeen)

	if numGoroutines > maxGoroutinesSeen {

		// Clobber existing values rather than attempt a CAS loop. This stat can be considered a "best effort".
		atomic.StoreUint64(&base.MaxGoroutinesSeen, numGoroutines)

		if stats != nil {
			stats.Set(base.StatKeyGoroutinesHighWatermark, base.ExpvarUInt64Val(maxGoroutinesSeen))
		}

		return numGoroutines
	}

	return maxGoroutinesSeen

}
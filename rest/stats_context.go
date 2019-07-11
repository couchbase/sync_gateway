package rest

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/elastic/gosigar"
	gopsutilnet "github.com/shirou/gopsutil/net"
	"github.com/shirou/gopsutil/process"
)

// Group the stats related context that is associated w/ a ServerContext into a struct
type statsContext struct {
	statsLoggingTicker *time.Ticker
	cpuStatsSnapshot   *cpuStatsSnapshot
	process            *process.Process
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
	// proc, _ := process.NewProcess(int32(os.Getpid()))
	// percent, _ := proc.CPUPercent()

	if statsContext.process == nil {
		statsContext.process, _ = process.NewProcess(int32(os.Getpid()))
	}

	percent, _ := statsContext.process.Percent(0)

	return percent, nil

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

func (statsContext *statsContext) addPublicNetworkInterfaceStatsForHostnamePort(hostPort string) error {

	iocountersStats, err := networkInterfaceStatsForHostnamePort(hostPort)
	if err != nil {
		return err
	}

	base.StatsResourceUtilization().Set(base.StatKeyPubNetworkInterfaceBytesSent, base.ExpvarUInt64Val(iocountersStats.BytesSent))
	base.StatsResourceUtilization().Set(base.StatKeyPubNetworkInterfaceBytesRecv, base.ExpvarUInt64Val(iocountersStats.BytesRecv))

	return nil
}

func (statsContext *statsContext) addAdminNetworkInterfaceStatsForHostnamePort(hostPort string) error {

	iocountersStats, err := networkInterfaceStatsForHostnamePort(hostPort)
	if err != nil {
		return err
	}

	base.StatsResourceUtilization().Set(base.StatKeyAdminNetworkInterfaceBytesSent, base.ExpvarUInt64Val(iocountersStats.BytesSent))
	base.StatsResourceUtilization().Set(base.StatKeyAdminNetworkInterfaceBytesRecv, base.ExpvarUInt64Val(iocountersStats.BytesRecv))

	return nil
}

func AddGoRuntimeStats() {

	statsResourceUtilization := base.StatsResourceUtilization()

	// Num goroutines
	numGoroutine := runtime.NumGoroutine()

	statsResourceUtilization.Set(base.StatKeyNumGoroutines, base.ExpvarIntVal(numGoroutine))

	statsResourceUtilization.Set(base.StatKeyGoroutinesHighWatermark, base.ExpvarUInt64Val(goroutineHighwaterMark(uint64(numGoroutine))))

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
func goroutineHighwaterMark(numGoroutines uint64) (maxGoroutinesSeen uint64) {

	maxGoroutinesSeen = atomic.LoadUint64(&base.MaxGoroutinesSeen)

	if numGoroutines > maxGoroutinesSeen {

		// Clobber existing values rather than attempt a CAS loop. This stat can be considered a "best effort".
		atomic.StoreUint64(&base.MaxGoroutinesSeen, numGoroutines)

		return numGoroutines
	}

	return maxGoroutinesSeen

}

func networkInterfaceStatsForHostnamePort(hostPort string) (iocountersStats gopsutilnet.IOCountersStat, err error) {

	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return iocountersStats, err
	}

	allInterfaces := false
	if host == "" || host == "0.0.0.0" {
		allInterfaces = true
	}

	// "localhost" -> "127.0.0.1", since this code only works with IP addresses
	if host == "localhost" {
		host = "127.0.0.1"
	}

	interfaceName := ""
	if !allInterfaces {
		interfaceName, err = discoverInterfaceName(host)
		if err != nil {
			return iocountersStats, err
		}
	}

	// Only get interface stats on a "Per Nic (network interface card)" basis if we aren't
	// listening on all interfaces, in which case we want the combined stats across all NICs.
	perNic := !allInterfaces

	iocountersStatsSet, err := gopsutilnet.IOCounters(perNic)
	if err != nil {
		return iocountersStats, err
	}

	if !allInterfaces {
		iocountersStatsSet = filterIOCountersByNic(iocountersStatsSet, interfaceName)
	}

	if len(iocountersStatsSet) == 0 {
		return iocountersStats, fmt.Errorf("Unable to find any network interface stats: %v", err)
	}

	// At this point we should only have one set of stats, either the stats for the NIC we care
	// about or the special "all" NIC which combines the stats
	iocountersStats = iocountersStatsSet[0]

	return iocountersStats, nil

}

func filterIOCountersByNic(iocountersStatsSet []gopsutilnet.IOCountersStat, interfaceName string) (filtered []gopsutilnet.IOCountersStat) {

	filtered = []gopsutilnet.IOCountersStat{}
	for _, iocountersStats := range iocountersStatsSet {
		if iocountersStats.Name == interfaceName {
			filtered = append(filtered, iocountersStats)
			return filtered
		}
	}

	return filtered
}

func discoverInterfaceName(host string) (interfaceName string, err error) {

	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		ifaceAddresses, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, ifaceCIDRAddr := range ifaceAddresses {

			ipAddr, _, err := net.ParseCIDR(ifaceCIDRAddr.String())
			if err != nil {
				return "", err
			}

			if ipAddr.String() == host {
				return iface.Name, nil
			}
		}
	}

	return "", fmt.Errorf("Unable to find interface for host: %v", host)

}

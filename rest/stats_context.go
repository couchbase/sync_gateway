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
)

// Group the stats related context that is associated w/ a ServerContext into a struct
type statsContext struct {
	statsLoggingTicker *time.Ticker
	cpuStatsSnapshot   *cpuStatsSnapshot
}

// The peak number of goroutines observed during lifetime of program
var MaxGoroutinesSeen uint64

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

	// statsResourceUtilization := base.StatsResourceUtilization()

	// Calculate the cpu percentage for the process
	cpuPercentUtilization, err := statsContext.calculateProcessCpuPercentage()
	if err != nil {
		return err
	}

	// Record stat
	// statsResourceUtilization.Set(base.StatKeyProcessCpuPercentUtilization, base.ExpvarFloatVal(cpuPercentUtilization))
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.CpuPercentUtil.Set(cpuPercentUtilization)

	return nil

}

func (statsContext *statsContext) addProcessMemoryPercentage() error {
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
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.ProcessMemoryResident.Set(int64(procMem.Resident))
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.SystemMemoryTotal.Set(int64(totalMem.Total))

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

	base.SyncGatewayStats.GlobalStats.ResourceUtilization.PublicNetworkInterfaceBytesSent.Set(int64(iocountersStats.BytesSent))
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.PublicNetworkInterfaceBytesReceived.Set(int64(iocountersStats.BytesRecv))

	return nil
}

func (statsContext *statsContext) addAdminNetworkInterfaceStatsForHostnamePort(hostPort string) error {

	iocountersStats, err := networkInterfaceStatsForHostnamePort(hostPort)
	if err != nil {
		return err
	}

	base.SyncGatewayStats.GlobalStats.ResourceUtilization.AdminNetworkInterfaceBytesSent.Set(int64(iocountersStats.BytesSent))
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.AdminNetworkInterfaceBytesReceived.Set(int64(iocountersStats.BytesRecv))

	return nil
}

func AddGoRuntimeStats() {
	// Num goroutines
	numGoroutine := runtime.NumGoroutine()

	base.SyncGatewayStats.GlobalStats.ResourceUtilization.NumGoroutines.Set(int64(numGoroutine))
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoroutinesHighWatermark.Set(int64(goroutineHighwaterMark(uint64(numGoroutine))))

	// Read memstats (relatively expensive)
	memstats := runtime.MemStats{}
	runtime.ReadMemStats(&memstats)

	// Sys
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsSys.Set(int64(memstats.Sys))

	// HeapAlloc
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsHeapAlloc.Set(int64(memstats.HeapAlloc))

	// HeapIdle
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsHeapIdle.Set(int64(memstats.HeapIdle))

	// HeapInuse
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsHeapInUse.Set(int64(memstats.HeapInuse))

	// HeapReleased
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsHeapReleased.Set(int64(memstats.HeapReleased))

	// StackInuse
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsStackInUse.Set(int64(memstats.StackInuse))

	// StackSys
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsStackSys.Set(int64(memstats.StackSys))

	// PauseTotalNs
	base.SyncGatewayStats.GlobalStats.ResourceUtilization.GoMemstatsPauseTotalNS.Set(int64(memstats.PauseTotalNs))
}

// Record Goroutines high watermark into expvars
func goroutineHighwaterMark(numGoroutines uint64) (maxGoroutinesSeen uint64) {

	maxGoroutinesSeen = atomic.LoadUint64(&MaxGoroutinesSeen)

	if numGoroutines > maxGoroutinesSeen {

		// Clobber existing values rather than attempt a CAS loop. This stat can be considered a "best effort".
		atomic.StoreUint64(&MaxGoroutinesSeen, numGoroutines)

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

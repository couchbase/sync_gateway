/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
	terminator         chan struct{} // Used to stop the goroutine handling the stats logging
	cpuStatsSnapshot   *cpuStatsSnapshot
	doneChan           chan struct{} // doneChan is closed when the stats logger goroutine finishes.
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
	// Calculate the cpu percentage for the process
	cpuPercentUtilization, err := statsContext.calculateProcessCpuPercentage()
	if err != nil {
		return err
	}

	// Record stat
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().CpuPercentUtil.Set(cpuPercentUtilization)

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
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ProcessMemoryResident.Set(int64(procMem.Resident))
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().SystemMemoryTotal.Set(int64(totalMem.Total))

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

	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().PublicNetworkInterfaceBytesSent.Set(int64(iocountersStats.BytesSent))
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().PublicNetworkInterfaceBytesReceived.Set(int64(iocountersStats.BytesRecv))

	return nil
}

func (statsContext *statsContext) addAdminNetworkInterfaceStatsForHostnamePort(hostPort string) error {

	iocountersStats, err := networkInterfaceStatsForHostnamePort(hostPort)
	if err != nil {
		return err
	}

	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().AdminNetworkInterfaceBytesSent.Set(int64(iocountersStats.BytesSent))
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().AdminNetworkInterfaceBytesReceived.Set(int64(iocountersStats.BytesRecv))

	return nil
}

func AddGoRuntimeStats() {
	// Num goroutines
	numGoroutine := runtime.NumGoroutine()

	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumGoroutines.Set(int64(numGoroutine))
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoroutinesHighWatermark.Set(int64(goroutineHighwaterMark(uint64(numGoroutine))))

	// Read memstats (relatively expensive)
	memstats := runtime.MemStats{}
	runtime.ReadMemStats(&memstats)

	// Sys
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsSys.Set(int64(memstats.Sys))

	// HeapAlloc
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsHeapAlloc.Set(int64(memstats.HeapAlloc))

	// HeapIdle
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsHeapIdle.Set(int64(memstats.HeapIdle))

	// HeapInuse
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsHeapInUse.Set(int64(memstats.HeapInuse))

	// HeapReleased
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsHeapReleased.Set(int64(memstats.HeapReleased))

	// StackInuse
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsStackInUse.Set(int64(memstats.StackInuse))

	// StackSys
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsStackSys.Set(int64(memstats.StackSys))

	// PauseTotalNs
	base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().GoMemstatsPauseTotalNS.Set(int64(memstats.PauseTotalNs))
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

func networkInterfaceStatsForHostnamePort(hostPort string) (*gopsutilnet.IOCountersStat, error) {

	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, err
	}

	// Only get interface stats on a "Per Nic (network interface card)" basis if we aren't
	// listening on all interfaces, in which case we want the combined stats across all NICs.
	perNic := true
	if host == "" || host == "0.0.0.0" {
		perNic = false
	}

	iocountersStatsSet, err := gopsutilnet.IOCounters(perNic)
	if err != nil {
		return nil, err
	}

	// filter to the interface we care about
	if perNic {
		interfaceName, err := discoverInterfaceName(host)
		if err != nil {
			return nil, err
		}
		iocountersStatsSet = filterIOCountersByNic(iocountersStatsSet, interfaceName)
	}

	if len(iocountersStatsSet) == 0 {
		return nil, fmt.Errorf("unable to find any network interface stats: %v", err)
	}

	// At this point we should only have one set of stats, either the stats for the NIC we care
	// about or the special "all" NIC which combines the stats
	return &iocountersStatsSet[0], nil
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

// discoverInterfaceName returns the network interface's name (e.g. en0, lo0, bridge0) associated with the given hostname/IP address.
func discoverInterfaceName(hostnameOrIP string) (interfaceName string, err error) {

	hosts := make(map[string]struct{})
	if net.ParseIP(hostnameOrIP) != nil {
		// Was an IP, don't need to resolve
		hosts[hostnameOrIP] = struct{}{}
	} else {
		// Was a hostname, resolve it to find address(es)
		ips, err := net.LookupHost(hostnameOrIP)
		if err != nil {
			return "", err
		}
		for _, ip := range ips {
			hosts[ip] = struct{}{}
		}
	}

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

			if _, ok := hosts[ipAddr.String()]; ok {
				return iface.Name, nil
			}
		}
	}

	return "", fmt.Errorf("unable to find matching interface for %s", hostnameOrIP)

}

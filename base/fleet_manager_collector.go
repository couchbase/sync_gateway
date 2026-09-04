/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/elastic/gosigar"
	"github.com/shirou/gopsutil/mem"
)

const ProductInfoName = "sync_gateway"

// TelemetrySettingsEndpoint is the ns_server management API path exposing the fleet manager
// collector settings (enabled flag + reporting interval).
const TelemetrySettingsEndpoint = "/internal/settings/telemetry"

// TelemetryIngestURI builds the ns_server management API path for POSTing collected metrics for a
// given instance to the fleet manager collector.
func TelemetryIngestURI(instanceID string) string {
	return fmt.Sprintf("/_telemetryCollector/ingest?product_name=%s&instance_id=%s", ProductInfoName, url.QueryEscape(instanceID))
}

type SyncGatewayFleetManagerMetrics struct {
	InstanceID    string                  `json:"instanceId"`
	CpuCores      int                     `json:"cpuCores"`
	RamBytesTotal string                  `json:"ramBytesTotal"`
	RamBytesUsed  string                  `json:"ramBytesUsed"`
	ProductInfo   FleetManagerProductInfo `json:"product"`
	OSVersion     string                  `json:"osVersion"`
	Hostname      string                  `json:"hostname"`
	UptimeSeconds int64                   `json:"uptimeSeconds"`
}

type FleetManagerProductInfo struct {
	Edition string `json:"edition"`
	Version string `json:"version"`
	Name    string `json:"name"`
}

type FleetManagerCollectorSettings struct {
	ReportingInterval int  `json:"reportIntervalHours"`
	Enabled           bool `json:"enabled"`
}

// defaultFleetManagerReportingInterval is used when the server does not supply a
// reporting interval (e.g. an older server that predates the telemetry settings endpoint).
const defaultFleetManagerReportingInterval = time.Hour

// maxReportingIntervalHours is the largest hour count that can be converted to a time.Duration
// (nanoseconds) without overflowing int64. A larger value would wrap to a negative Duration, which
// panics time.NewTicker/Reset, so Interval falls back to the default for anything beyond it.
const maxReportingIntervalHours = int(math.MaxInt64 / int64(time.Hour))

// Interval returns the reporting interval as a Duration, falling back to the default when the
// server hasn't supplied a positive value or supplies one large enough to overflow a Duration.
func (s FleetManagerCollectorSettings) Interval() time.Duration {
	if s.ReportingInterval <= 0 || s.ReportingInterval > maxReportingIntervalHours {
		return defaultFleetManagerReportingInterval
	}
	return time.Duration(s.ReportingInterval) * time.Hour
}

func CollectSGWFleetManagerMetrics(ctx context.Context, nodeUID, hostname string) SyncGatewayFleetManagerMetrics {
	edition := "Enterprise"
	if !IsEnterpriseEdition() {
		edition = "Community"
	}
	productInfo := FleetManagerProductInfo{
		Edition: edition,
		Version: ProductVersion.ReleaseVersionString(),
		Name:    ProductInfoName,
	}

	sysInfo := getSystemInfo(ctx)

	return SyncGatewayFleetManagerMetrics{
		InstanceID:    nodeUID,
		CpuCores:      sysInfo.cpuCores,
		RamBytesTotal: sysInfo.ramBytesTotal,
		RamBytesUsed:  sysInfo.ramBytesUsed,
		ProductInfo:   productInfo,
		OSVersion:     sysInfo.osVersion,
		Hostname:      hostname,
		UptimeSeconds: sysInfo.uptimeSeconds,
	}
}

type systemInfo struct {
	cpuCores      int
	ramBytesTotal string
	ramBytesUsed  string
	osVersion     string
	uptimeSeconds int64
}

func getSystemInfo(ctx context.Context) systemInfo {
	var residentBytes int64

	// Sample process/system memory directly here rather than reading the ResourceUtilizationStats
	// expvars: those are only populated by the stats-logger ticker, so at startup (before its first
	// tick) and whenever the stats logger is disabled they would still read zero when this report is
	// sent, yielding a phone-home record with ramBytesUsed/ramBytesTotal of 0.
	procMem := gosigar.ProcMem{}
	if err := procMem.Get(os.Getpid()); err != nil {
		WarnfCtx(ctx, "Could not read process memory for fleet manager metrics: %v", err)
	} else {
		residentBytes = int64(procMem.Resident)
	}

	totalRam := GetTotalMemory(ctx, false)
	return systemInfo{
		uptimeSeconds: SyncGatewayStats.GlobalStats.ResourceUtilizationStats().Uptime.ToSeconds(),
		cpuCores:      runtime.NumCPU(),
		ramBytesUsed:  strconv.FormatInt(residentBytes, 10),
		ramBytesTotal: strconv.FormatInt(int64(totalRam), 10),
		osVersion:     runtime.GOOS + "-" + runtime.GOARCH,
	}
}

// getTotalMemory returns the total memory available on the system. If a cgroup is detected, it will use the cgroup memory max.
func GetTotalMemory(ctx context.Context, virtualMem bool) uint64 {
	memoryTotal, err := memlimit.FromCgroup()
	if err == nil {
		return memoryTotal
	}
	TracefCtx(ctx, KeyAll, "Did not detect a cgroup for a memory limit")
	var totalRam uint64
	if virtualMem {
		memory, err := mem.VirtualMemory()
		if err != nil {
			WarnfCtx(ctx, "Error getting total memory from gopsutil: %v", err)
			return 0
		}
		totalRam = memory.Total
	} else {
		sysMem := gosigar.Mem{}
		if err := sysMem.Get(); err != nil {
			WarnfCtx(ctx, "Error getting total memory from gosigar: %v", err)
		} else {
			totalRam = sysMem.Total
		}
	}
	return totalRam
}

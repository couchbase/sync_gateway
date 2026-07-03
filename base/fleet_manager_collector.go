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
	"os"
	"runtime"
	"strconv"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/elastic/gosigar"
)

const ProductInfoName = "sync_gateway"

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
	var residentBytes, totalRam int64

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

	if cgroupLimit, err := memlimit.FromCgroup(); err == nil {
		// A cgroup memory limit, when present, is the meaningful "total" for this instance.
		totalRam = int64(cgroupLimit)
	} else {
		sysMem := gosigar.Mem{}
		if err := sysMem.Get(); err != nil {
			WarnfCtx(ctx, "Could not read system memory for fleet manager metrics: %v", err)
		} else {
			totalRam = int64(sysMem.Total)
		}
	}

	return systemInfo{
		uptimeSeconds: SyncGatewayStats.GlobalStats.ResourceUtilizationStats().Uptime.ToSeconds(),
		cpuCores:      runtime.NumCPU(),
		ramBytesUsed:  strconv.FormatInt(residentBytes, 10),
		ramBytesTotal: strconv.FormatInt(totalRam, 10),
		osVersion:     runtime.GOOS + "-" + runtime.GOARCH,
	}
}

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
	"runtime"
	"strconv"

	"github.com/KimMachineGun/automemlimit/memlimit"
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
	IntervalSeconds int  `json:"reportIntervalHours"`
	Enabled         bool `json:"enabled"`
}

func CollectSGWFleetManagerMetrics(nodeUID, hostname string) SyncGatewayFleetManagerMetrics {
	edition := "Enterprise"
	if !IsEnterpriseEdition() {
		edition = "Community"
	}
	productInfo := FleetManagerProductInfo{
		Edition: edition,
		Version: ProductVersion.ReleaseVersionString(),
		Name:    "sync_gateway",
	}

	sysInfo := getSystemInfo()

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

func getSystemInfo() systemInfo {
	totalRam := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().SystemMemoryTotal.Value()
	cgroupLimit, err := memlimit.FromCgroup()
	if err == nil {
		// cgroup in place report this instead
		totalRam = int64(cgroupLimit)
	}
	goArch := runtime.GOARCH
	goOS := runtime.GOOS
	osVersion := goOS + "-" + goArch

	return systemInfo{
		uptimeSeconds: SyncGatewayStats.GlobalStats.ResourceUtilizationStats().Uptime.ToSeconds(),
		cpuCores:      runtime.NumCPU(),
		ramBytesUsed:  strconv.FormatInt(SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ProcessMemoryResident.Value(), 10),
		ramBytesTotal: strconv.FormatInt(totalRam, 10),
		osVersion:     osVersion,
	}
}

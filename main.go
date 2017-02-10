//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/dustin/go-humanize"
	"runtime"
	"time"
	"github.com/stackimpact/stackimpact-go"
)

type AbbreviatedMemStats struct {

	// General statistics.
	Alloc      string // bytes allocated and not yet freed
	TotalAlloc string // bytes allocated (even if freed)
	Sys        string // bytes obtained from system (sum of XxxSys below)
	Lookups    uint64 // number of pointer lookups
	Mallocs    uint64 // number of mallocs
	Frees      uint64 // number of frees

	// Main allocation heap statistics.
	HeapAlloc    string // bytes allocated and not yet freed (same as Alloc above)
	HeapSys      string // bytes obtained from system
	HeapIdle     string // bytes in idle spans
	HeapInuse    string // bytes in non-idle span
	HeapReleased string // bytes released to the OS
	HeapObjects  uint64 // total number of allocated objects

	// Low-level fixed-size structure allocator statistics.
	//	Inuse is bytes used now.
	//	Sys is bytes obtained from system.
	StackInuse  string // bytes used by stack allocator
	StackSys    string
	MSpanInuse  uint64 // mspan structures
	MSpanSys    uint64
	MCacheInuse uint64 // mcache structures
	MCacheSys   uint64
	BuckHashSys uint64 // profiling bucket hash table
	GCSys       uint64 // GC metadata
	OtherSys    uint64 // other system allocations

	// Garbage collector statistics.
	NextGC        uint64 // next collection will happen when HeapAlloc ≥ this amount
	LastGC        uint64 // end time of last collection (nanoseconds since 1970)
	PauseTotalNs  uint64
	NumGC         uint32
	GCCPUFraction float64 // fraction of CPU time used by GC
	EnableGC      bool
	DebugGC       bool
}

func NewAbbreviatedMemStats(memstats runtime.MemStats) *AbbreviatedMemStats {
	m := AbbreviatedMemStats{
		Alloc: humanize.Bytes(memstats.Alloc),
		TotalAlloc: humanize.Bytes(memstats.TotalAlloc),
		Sys: humanize.Bytes(memstats.Sys),
		Mallocs: memstats.Mallocs,
		Frees: memstats.Frees,
		HeapAlloc: humanize.Bytes(memstats.HeapAlloc),
		HeapSys: humanize.Bytes(memstats.HeapSys),
		HeapIdle: humanize.Bytes(memstats.HeapIdle),
		HeapInuse: humanize.Bytes(memstats.HeapInuse),
		HeapReleased: humanize.Bytes(memstats.HeapReleased),
		HeapObjects: memstats.HeapObjects,
		StackInuse: humanize.Bytes(memstats.StackInuse),
		StackSys: humanize.Bytes(memstats.StackSys),
		MSpanInuse: memstats.MSpanInuse,
		MSpanSys: memstats.MSpanSys,
		MCacheInuse: memstats.MCacheInuse,
		MCacheSys: memstats.MCacheSys,
		BuckHashSys: memstats.BuckHashSys,
		GCSys: memstats.GCSys,
		OtherSys: memstats.OtherSys,
		NextGC: memstats.NextGC,
		LastGC: memstats.LastGC,
		PauseTotalNs: memstats.PauseTotalNs,
		GCCPUFraction: memstats.GCCPUFraction,
		EnableGC: memstats.EnableGC,
		DebugGC: memstats.DebugGC,
		NumGC: memstats.NumGC,
	}
	return &m
}

// Simple Sync Gateway launcher tool.
func main() {

	signalchannel := make(chan os.Signal, 1)
	signal.Notify(signalchannel, syscall.SIGHUP)

	go func() {
		for range signalchannel {
			base.Logf("Handling SIGHUP signal.\n")
			rest.HandleSighup()
		}
	}()

	agent := stackimpact.NewAgent();
	agent.Start(stackimpact.Options{
		AgentKey: "0efbff50031f0ec4d75d7389355fa00ab394f4c2",
		AppName: "SyncGateway",
	})


	go func() {
		for {
			var memstats runtime.MemStats
			runtime.ReadMemStats(&memstats)
			memStatsAbbreviated := NewAbbreviatedMemStats(memstats)
			if memstats.HeapSys > 3000000000 && memstats.StackSys > 5000000000 && memstats.HeapIdle < 1000000000 {
				base.Logf("Memory stats: %+v", memStatsAbbreviated)
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()

	rest.ServerMain(rest.SyncGatewayRunModeNormal)
}

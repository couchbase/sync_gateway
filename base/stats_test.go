/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkExpvarString(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = expvarMap.String()
	}
}

func BenchmarkExpvarAdd(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map).Add("error_count", 1)
	}
}

func BenchmarkExpvarSet(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map).Get("error_count").(*expvar.Int).Set(1)
	}
}

func BenchmarkExpvarGet(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map).Get("error_count").(*expvar.Int).Value()
	}
}

func BenchmarkExpvarAddParallel(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()
	res := expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map).Get("error_count").(*expvar.Int)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res.Add(1)
		}
	})
}

func BenchmarkNewStatsMarshal(b *testing.B) {
	sgwStats := NewSyncGatewayStats()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = sgwStats.String()
	}
}

func BenchmarkNewStatAdd(b *testing.B) {
	sgwStats := NewSyncGatewayStats()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Add(1)
	}
}

func BenchmarkNewStatSet(b *testing.B) {
	sgwStats := NewSyncGatewayStats()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Set(1)
	}
}

func BenchmarkNewStatGet(b *testing.B) {
	sgwStats := NewSyncGatewayStats()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value()
	}
}

func BenchmarkNewStatAddParallel(b *testing.B) {
	sgwStats := NewSyncGatewayStats()

	test := sgwStats.GlobalStats.ResourceUtilizationStats()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			test.ErrorCount.Add(1)
		}
	})
}

func TestSetIfMax(t *testing.T) {
	sgwStats := NewSyncGatewayStats()

	// Test an integer
	sgwStats.GlobalStats.ResourceUtilization.ErrorCount.Set(10)
	sgwStats.GlobalStats.ResourceUtilization.ErrorCount.SetIfMax(100)
	assert.Equal(t, int64(100), sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value())
	sgwStats.GlobalStats.ResourceUtilization.ErrorCount.SetIfMax(50)
	assert.Equal(t, int64(100), sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value())

	// Test a float
	sgwStats.GlobalStats.ResourceUtilization.CpuPercentUtil.Set(10)
	sgwStats.GlobalStats.ResourceUtilization.CpuPercentUtil.SetIfMax(100)
	assert.Equal(t, float64(100), sgwStats.GlobalStats.ResourceUtilizationStats().CpuPercentUtil.Value())
	sgwStats.GlobalStats.ResourceUtilization.CpuPercentUtil.SetIfMax(50)
	assert.Equal(t, float64(100), sgwStats.GlobalStats.ResourceUtilizationStats().CpuPercentUtil.Value())
}

func initExpvarBaseEquivalent() *expvar.Map {
	expvarMap := new(expvar.Map).Init()
	expvarMap.Set("global", new(expvar.Map).Init())
	expvarMap.Get("global").(*expvar.Map).Set("resource_utilization", new(expvar.Map).Init())

	resourceUtilization := expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map)
	resourceUtilization.Set("process_cpu_percent_utilization", ExpvarFloatVal(0))
	resourceUtilization.Set("process_memory_resident", ExpvarInt64Val(0))
	resourceUtilization.Set("system_memory_total", ExpvarInt64Val(0))
	resourceUtilization.Set("pub_net_bytes_sent", ExpvarInt64Val(0))
	resourceUtilization.Set("pub_net_bytes_recv", ExpvarInt64Val(0))
	resourceUtilization.Set("admin_net_bytes_sent", ExpvarInt64Val(0))
	resourceUtilization.Set("admin_net_bytes_recv", ExpvarInt64Val(0))
	resourceUtilization.Set("num_goroutines", ExpvarInt64Val(0))
	resourceUtilization.Set("goroutines_high_watermark", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_sys", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_heapalloc", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_heapidle", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_heapinuse", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_heapreleased", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_stackinuse", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_stacksys", ExpvarInt64Val(0))
	resourceUtilization.Set("go_memstats_pausetotalns", ExpvarInt64Val(0))
	resourceUtilization.Set("error_count", ExpvarInt64Val(0))
	resourceUtilization.Set("warn_count", ExpvarInt64Val(0))

	expvarMap.Set("per_db", new(expvar.Map).Init())
	expvarMap.Set("per_replication", new(expvar.Map).Init())

	return expvarMap
}

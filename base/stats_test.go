package base

import (
	"expvar"
	"testing"
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

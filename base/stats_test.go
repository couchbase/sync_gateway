package base

import (
	"expvar"
	"testing"
)

func BenchmarkExpvarString(b *testing.B) {
	expvarMap := new(expvar.Map).Init()
	expvarMap.Set("global", new(expvar.Map).Init())
	expvarMap.Get("global").(*expvar.Map).Set("resource_utilization", new(expvar.Map).Init())

	resourceUtilization := expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map)
	resourceUtilization.Set("process_cpu_percent_utilization", ExpvarFloatVal(0))
	resourceUtilization.Set("process_memory_resident", ExpvarFloatVal(0))
	resourceUtilization.Set("system_memory_total", ExpvarFloatVal(0))
	resourceUtilization.Set("pub_net_bytes_sent", ExpvarFloatVal(0))
	resourceUtilization.Set("pub_net_bytes_recv", ExpvarFloatVal(0))
	resourceUtilization.Set("admin_net_bytes_sent", ExpvarFloatVal(0))
	resourceUtilization.Set("admin_net_bytes_recv", ExpvarFloatVal(0))
	resourceUtilization.Set("num_goroutines", ExpvarFloatVal(0))
	resourceUtilization.Set("goroutines_high_watermark", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_sys", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_heapalloc", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_heapidle", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_heapinuse", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_heapreleased", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_stackinuse", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_stacksys", ExpvarFloatVal(0))
	resourceUtilization.Set("go_memstats_pausetotalns", ExpvarFloatVal(0))
	resourceUtilization.Set("error_count", ExpvarFloatVal(0))
	resourceUtilization.Set("warn_count", ExpvarFloatVal(0))

	expvarMap.Set("per_db", new(expvar.Map).Init())
	expvarMap.Set("per_replication", new(expvar.Map).Init())

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = expvarMap.String()
	}
}

func BenchmarkNewStatsMarshal(b *testing.B) {
	sgwStats := NewSyncGatewayStats()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = sgwStats.String()
	}
}

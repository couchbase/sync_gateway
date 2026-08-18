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
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/require"

	"github.com/couchbase/sync_gateway/testing/assert"
)

func BenchmarkExpvarString(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	for b.Loop() {
		_ = expvarMap.String()
	}
}

func BenchmarkExpvarAdd(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	for b.Loop() {
		expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map).Add("error_count", 1)
	}
}

func BenchmarkExpvarSet(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	for b.Loop() {
		expvarMap.Get("global").(*expvar.Map).Get("resource_utilization").(*expvar.Map).Get("error_count").(*expvar.Int).Set(1)
	}
}

func BenchmarkExpvarGet(b *testing.B) {
	expvarMap := initExpvarBaseEquivalent()

	for b.Loop() {
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
	sgwStats, err := NewSyncGatewayStats()
	require.NoError(b, err)

	for b.Loop() {
		_ = sgwStats.String()
	}
}

func BenchmarkNewStatAdd(b *testing.B) {
	sgwStats, err := NewSyncGatewayStats()
	require.NoError(b, err)

	for b.Loop() {
		sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Add(1)
	}
}

func BenchmarkNewStatSet(b *testing.B) {
	sgwStats, err := NewSyncGatewayStats()
	require.NoError(b, err)

	for b.Loop() {
		sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Set(1)
	}
}

func BenchmarkNewStatGet(b *testing.B) {
	sgwStats, err := NewSyncGatewayStats()
	require.NoError(b, err)

	for b.Loop() {
		_ = sgwStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value()
	}
}

func BenchmarkNewStatAddParallel(b *testing.B) {
	sgwStats, err := NewSyncGatewayStats()
	require.NoError(b, err)

	test := sgwStats.GlobalStats.ResourceUtilizationStats()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			test.ErrorCount.Add(1)
		}
	})
}

func TestSetIfMax(t *testing.T) {
	sgwStats, err := NewSyncGatewayStats()
	require.NoError(t, err)

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

// TestSgwFloatStatMarshalNonFinite is a regression test for CBG-3658: a non-finite float value
// (e.g. +Inf produced by a divide-by-zero in the process CPU percentage calculation) must not
// corrupt stats serialization. Previously MarshalJSON emitted the raw bytes "+Inf"/"NaN", which the
// JSON encoder rejected ("invalid character '+' looking for beginning of value"), collapsing the
// entire SgwStats blob to "null".
func TestSgwFloatStatMarshalNonFinite(t *testing.T) {
	for _, test := range []struct {
		name  string
		value float64
	}{
		{"positive_infinity", math.Inf(1)},
		{"negative_infinity", math.Inf(-1)},
		{"nan", math.NaN()},
	} {
		t.Run(test.name, func(t *testing.T) {
			stat := &SgwFloatStat{}
			stat.Set(test.value)

			// Marshal the stat inside a larger structure: this is the exact path that failed via
			// SgwStats.String -> JSONMarshalCanonical.
			marshalled, err := JSONMarshalCanonical(map[string]*SgwFloatStat{"stat": stat})
			require.NoError(t, err)

			// The output must be valid, parseable JSON, and non-finite values must fall back to 0.
			var roundTripped map[string]float64
			require.NoError(t, JSONUnmarshal(marshalled, &roundTripped))
			require.Equal(t, 0.0, roundTripped["stat"])

			// String() satisfies expvar.Var, which likewise requires a valid JSON value.
			var viaString float64
			require.NoError(t, JSONUnmarshal([]byte(stat.String()), &viaString))
			require.Equal(t, 0.0, viaString)
		})
	}
}

// TestDbReplicatorStatsUnsynchronisedAccess covers concurrent access to DbStats.DbReplicatorStats:
// creating a replication's stats while something else walks the stats tree.  Run with -race.
//
// The expvar case is the one reachable in a running Sync Gateway: any metrics or expvar read that
// lands while a replication is initialising.  The teardown case needs two live DatabaseContexts
// sharing a database name, since DbStats is keyed by name.
func TestDbReplicatorStatsUnsynchronisedAccess(t *testing.T) {
	const iterations = 200

	t.Run("expvar read while creating", func(t *testing.T) {
		const dbName = "statsRaceExpvarDb"
		dbStats, err := SyncGatewayStats.NewDBStats(dbName, false, false, false, false, []string{}, []string{})
		require.NoError(t, err)
		defer SyncGatewayStats.ClearDBStats(dbName)

		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range iterations {
				_, err := dbStats.DBReplicatorStats(fmt.Sprintf("replication%d", i))
				assert.NoError(t, err)
			}
		})
		// SgwStats.String marshals the whole tree, including DbReplicatorStats.
		wg.Go(func() {
			for range iterations {
				_ = SyncGatewayStats.String()
			}
		})
		WaitWithTimeout(t, &wg, time.Minute)
	})

	t.Run("stats teardown while creating", func(t *testing.T) {
		const dbName = "statsRaceTeardownDb"
		// One Clear per iteration: it deletes the map entry, so repeated calls on the same entry return
		// early and never reach the unregister loop.  Replication IDs are unique per iteration so stats
		// created after a Clear has walked the map cannot collide with the next iteration's registration.
		for outer := range iterations {
			dbStats, err := SyncGatewayStats.NewDBStats(dbName, false, false, false, false, []string{}, []string{})
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Go(func() {
				for i := range 20 {
					_, err := dbStats.DBReplicatorStats(fmt.Sprintf("replication%d-%d", outer, i))
					assert.NoError(t, err)
				}
			})
			// ClearDBStats iterates DbReplicatorStats to unregister them.
			wg.Go(func() {
				SyncGatewayStats.ClearDBStats(dbName)
			})
			WaitWithTimeout(t, &wg, time.Minute)

			SyncGatewayStats.ClearDBStats(dbName)
		}
	})
}

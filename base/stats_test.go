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
	"maps"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
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

// newTestSgwStats returns an empty stats tree, separate from the package-level SyncGatewayStats.
func newTestSgwStats() *SgwStats {
	return &SgwStats{sgwStatsFields: sgwStatsFields{DbStats: map[string]*DbStats{}}}
}

// TestSgwStatsSerializedFieldsAreEmbedded fails when an exported field is declared on SgwStats
// instead of sgwStatsFields, because String() copies only sgwStatsFields and would drop it.
func TestSgwStatsSerializedFieldsAreEmbedded(t *testing.T) {
	for _, field := range reflect.VisibleFields(reflect.TypeFor[SgwStats]()) {
		if !field.IsExported() {
			continue
		}
		// Promoted fields have an index path through the embedded struct, declared fields do not.
		assert.Greater(t, len(field.Index), 1,
			"SgwStats.%s must be declared in sgwStatsFields so String() serializes it", field.Name)
	}
}

// TestClearDBStatsRemovesFromSerialization checks that a database removed by ClearDBStats no longer
// appears in the expvar/stats log output.
func TestClearDBStatsRemovesFromSerialization(t *testing.T) {
	stats := newTestSgwStats()
	const dbName = "TestClearDBStatsRemovesFromSerialization_db"

	_, err := stats.NewDBStats(dbName, false, false, false, false, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, stats.String(), dbName)

	stats.ClearDBStats(dbName)
	require.NotContains(t, stats.String(), dbName)
}

// TestStatsSerializationConcurrentWithDBRegistration is a -race guard on the narrowed
// dbStatsMapMutex section (CBG-5472): String() marshals a shallow copy of the map outside the lock,
// so it can read a *DbStats that ClearDBStats is unregistering from Prometheus. This is not a
// reproducer for the stats logger stall - that was on _databasesLock, see
// rest.TestStatsLoggerIndependentOfDatabaseLock.
func TestStatsSerializationConcurrentWithDBRegistration(t *testing.T) {
	// TestMain sets this true, which would skip the Prometheus path this test wants to exercise.
	defer func(skip bool) { SkipPrometheusStatsRegistration = skip }(SkipPrometheusStatsRegistration)
	SkipPrometheusStatsRegistration = false

	stats := newTestSgwStats()

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 1000 {
				_ = stats.String()
			}
		})
	}
	for i := range 10 {
		wg.Go(func() {
			// Distinct db name per goroutine so concurrent registrations never collide in Prometheus.
			name := fmt.Sprintf("TestStatsSerializationConcurrentWithDBRegistration_db_%d", i)
			for range 100 {
				_, err := stats.NewDBStats(name, false, false, false, false, nil, nil)
				assert.NoError(t, err)
				stats.ClearDBStats(name)
			}
		})
	}
	WaitWithTimeout(t, &wg, time.Minute)
}

// TestClearDBStatsConcurrentWithReplicatorRegistration is a regression test for ClearDBStats
// iterating DbReplicatorStats without dbReplicatorStatsMutex, which panicked with "concurrent map
// read and map write" when a replication registered during teardown. Fails without -race too.
func TestClearDBStatsConcurrentWithReplicatorRegistration(t *testing.T) {
	stats := newTestSgwStats()
	const dbName = "TestClearDBStatsConcurrentWithReplicatorRegistration_db"

	dbStats, err := stats.NewDBStats(dbName, false, false, false, false, nil, nil)
	require.NoError(t, err)

	// Pre-populate the map so ClearDBStats takes longer to iterate, widening the race window.
	for i := range 100 {
		_, err := dbStats.DBReplicatorStats(fmt.Sprintf("seed_%d", i))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	defer WaitWithTimeout(t, &wg, time.Minute)
	wg.Go(func() {
		for i := range 100 {
			_, err := dbStats.DBReplicatorStats(fmt.Sprintf("repl_%d", i))
			assert.NoError(t, err)
		}
	})

	// Iterates DbReplicatorStats concurrently with the registrations above.
	stats.ClearDBStats(dbName)
}

// newTestDbStats returns a DbStats holding only the replicator map, bypassing NewDBStats so a test
// can use the same dbName twice.
func newTestDbStats(dbName string) *DbStats {
	return &DbStats{dbName: dbName, DbReplicatorStats: map[string]*DbReplicatorStats{}}
}

// TestDBReplicatorStatsFailedRegistrationNotCached checks that a DBReplicatorStats call which fails
// partway through registration caches nothing, so a caller can never be handed a half-populated
// entry - the failure is reported again instead.
func TestDBReplicatorStatsFailedRegistrationNotCached(t *testing.T) {
	// TestMain sets this true, which would skip registration and so never produce a failure.
	defer func(skip bool) { SkipPrometheusStatsRegistration = skip }(SkipPrometheusStatsRegistration)
	SkipPrometheusStatsRegistration = false

	const dbName = "TestDBReplicatorStatsFailedRegistrationNotCached_db"
	const replicationID = "repl"

	// Two DbStats sharing a dbName describe identical Prometheus metrics, so the second
	// registration of the same replication ID collides and fails.
	first, second := newTestDbStats(dbName), newTestDbStats(dbName)

	firstStats, err := first.DBReplicatorStats(replicationID)
	require.NoError(t, err)
	require.NotNil(t, firstStats.NumAttachmentBytesPushed)
	defer first.unregisterReplicationStats(replicationID)

	_, err = second.DBReplicatorStats(replicationID)
	require.Error(t, err, "expected the duplicate Prometheus registration to fail")

	require.NotContains(t, maps.Keys(second.DbReplicatorStats), replicationID,
		"a failed registration was cached")

	_, err = second.DBReplicatorStats(replicationID)
	require.Error(t, err, "expected the retry to report the registration failure again")
}

// TestDBReplicatorStatsConcurrentSameID checks that concurrent callers for one replication ID never
// see the entry before its stats are populated.
func TestDBReplicatorStatsConcurrentSameID(t *testing.T) {
	dbStats := newTestDbStats("TestDBReplicatorStatsConcurrentSameID_db")

	var partial atomic.Int64
	var wg sync.WaitGroup
	// Each replication ID is one chance to observe the window, so use plenty.
	for i := range 50 {
		replicationID := fmt.Sprintf("repl_%d", i)
		start := make(chan struct{})
		for range 4 {
			wg.Go(func() {
				<-start
				replicatorStats, err := dbStats.DBReplicatorStats(replicationID)
				if !assert.NoError(t, err) {
					return
				}
				// The last field DBReplicatorStats populates, so it is the one left nil by an
				// entry published before it was finished.
				if replicatorStats.ProcessedSequenceLenPostCleanup == nil {
					partial.Add(1)
				}
			})
		}
		close(start)
	}
	WaitWithTimeout(t, &wg, time.Minute)

	require.Zero(t, partial.Load(),
		"%d callers received an entry before its stats were populated", partial.Load())
}

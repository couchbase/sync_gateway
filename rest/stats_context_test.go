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
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestProcessCpuPercentageZeroTotalDelta is a regression test for CBG-3658: when two CPU snapshots
// report the same total system jiffies (which happens when stats are sampled very frequently, or on
// platforms with coarse CPU accounting) the percentage calculation must not divide by zero and
// produce +Inf/NaN - which then breaks stats JSON serialization. It must report 0 instead.
func TestProcessCpuPercentageZeroTotalDelta(t *testing.T) {
	previous := &cpuStatsSnapshot{totalTimeJiffies: 1000, procUserTimeJiffies: 10, procSystemTimeJiffies: 5}
	// Same total jiffies as previous, but the process consumed some time: without a guard this is
	// 100 * positiveDelta / 0 = +Inf.
	current := &cpuStatsSnapshot{totalTimeJiffies: 1000, procUserTimeJiffies: 20, procSystemTimeJiffies: 8}

	got := current.cpuPercentageSince(previous)
	require.False(t, math.IsInf(got, 0), "expected a finite percentage, got +/-Inf")
	require.False(t, math.IsNaN(got), "expected a finite percentage, got NaN")
	require.Equal(t, 0.0, got)
}

// TestProcessCpuPercentageNormal verifies the CPU percentage math over a normal (non-degenerate)
// sampling interval is unaffected by the CBG-3658 divide-by-zero guard.
func TestProcessCpuPercentageNormal(t *testing.T) {
	previous := &cpuStatsSnapshot{totalTimeJiffies: 1000, procUserTimeJiffies: 10, procSystemTimeJiffies: 5}
	current := &cpuStatsSnapshot{totalTimeJiffies: 1200, procUserTimeJiffies: 40, procSystemTimeJiffies: 15}
	// deltaProcess = (40-10) + (15-5) = 40, deltaTotal = 200 -> 100 * 40 / 200 = 20.0
	require.Equal(t, 20.0, current.cpuPercentageSince(previous))
}

func TestNetworkInterfaceStatsForHostnamePort(t *testing.T) {

	_, err := networkInterfaceStatsForHostnamePort("127.0.0.1:4984")
	assert.NoError(t, err, "Unexpected Error")

	_, err = networkInterfaceStatsForHostnamePort("localhost:4984")
	assert.NoError(t, err, "Unexpected Error")

	_, err = networkInterfaceStatsForHostnamePort("0.0.0.0:4984")
	assert.NoError(t, err, "Unexpected Error")

	_, err = networkInterfaceStatsForHostnamePort(":4984")
	assert.NoError(t, err, "Unexpected Error")

}

func TestDescriptionPopulation(t *testing.T) {
	base.SkipPrometheusStatsRegistration = false
	defer func() {
		base.SkipPrometheusStatsRegistration = true
	}()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	srv := httptest.NewServer(rt.TestMetricsHandler())
	defer srv.Close()

	// Ensure metrics endpoint is accessible and that db database has entries
	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, resp.Body.Close())
	}()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyString, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// assert on a HELP description
	assert.Contains(t, string(bodyString), `HELP sgw_cache_high_seq_stable The highest contiguous sequence number that has been cached.`)
}

func TestMemoryProfile(t *testing.T) {
	base.LongRunningTest(t)

	stats := statsContext{heapProfileCollectionThreshold: 1, heapProfileEnabled: true} // set to a very low value to ensure collection

	outputDir := t.TempDir()
	ctx := base.TestCtx(t)

	// make sure go memory stats are set once
	AddGoRuntimeStats()

	// collect single profile
	startTime := "01"
	require.NoError(t, stats.collectMemoryProfile(ctx, outputDir, startTime))
	require.Equal(t, []string{"pprof_heap_high_01.pb.gz"}, getFilenames(t, outputDir))

	// collect enough profiles to trigger rotation
	expectedFilenames := make([]string, 0, 10)
	for i := 2; i < 12; i++ {
		// reset heap profile time time to ensure we create new heap profiles
		stats.lastHeapProfile = time.Time{}
		expectedFilenames = append(expectedFilenames, fmt.Sprintf("pprof_heap_high_%02d.pb.gz", i))
		require.NoError(t, stats.collectMemoryProfile(ctx, outputDir, fmt.Sprintf("%02d", i)))
	}
	require.ElementsMatch(t, expectedFilenames, getFilenames(t, outputDir))

	// ask for another profile, this should not be collected. Since the last profile collection (11) set lastHeapProfile, we do not collect another profile for 5 minutes.
	require.NoError(t, stats.collectMemoryProfile(ctx, outputDir, "12"))
	require.Equal(t, expectedFilenames, getFilenames(t, outputDir))
}

func getFilenames(t *testing.T, dir string) []string {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	filenames := make([]string, 0, len(files))
	for _, file := range files {
		filenames = append(filenames, file.Name())
	}
	slices.Sort(filenames)
	return filenames
}

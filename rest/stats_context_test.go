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
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	resp, err := http.Get(srv.URL + "/_metrics")
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
	stats := statsContext{}

	outputDir := t.TempDir()
	ctx := base.TestCtx(t)

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
	slices.Sort(expectedFilenames)
	require.Equal(t, expectedFilenames, getFilenames(t, outputDir))

	// ask for another profile, this should not be collected because lastHeapProfile was not set.
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

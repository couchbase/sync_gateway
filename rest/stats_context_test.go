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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

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

	httpClient := http.DefaultClient

	// Ensure metrics endpoint is accessible and that db database has entries
	resp, err := httpClient.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyString, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// assert on a HELP description
	assert.Contains(t, string(bodyString), `HELP sgw_cache_high_seq_stable The highest contiguous sequence number that has been cached.`)
}

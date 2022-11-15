/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveReplicatorBlipsync uses an ActiveReplicator with another RestTester instance to connect and cleanly disconnect.
func TestActiveReplicatorBlipsync(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		}},
	})
	defer rt.Close()
	ctx := rt.Context()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")
	stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx, &db.ActiveReplicatorConfig{
		ID:                  t.Name(),
		Direction:           db.ActiveReplicatorTypePushAndPull,
		ActiveDB:            &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:         passiveDBURL,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
	})

	startNumReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	startNumReplicationsActive := rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx))

	// Check total stat
	numReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)

	// Check active stat
	assert.Equal(t, startNumReplicationsActive+2, rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value())

	// Close the replicator (implicit disconnect)
	assert.NoError(t, ar.Stop())

	// Wait for active stat to drop to original value
	numReplicationsActive, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()
	}, startNumReplicationsActive)
	assert.True(t, ok)
	assert.Equal(t, startNumReplicationsActive, numReplicationsActive)

	// Verify total stat has not been decremented
	numReplicationsTotal = rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)
}

func TestBlipSyncNonUpgradableConnection(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		}},
	})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	server := httptest.NewServer(rt.TestPublicHandler())
	defer server.Close()
	dbURL, err := url.Parse(server.URL + "/db/_blipsync")
	require.NoError(t, err)

	// Add basic auth credentials to target db URL
	dbURL.User = url.UserPassword("alice", "pass")
	request, err := http.NewRequest(http.MethodGet, dbURL.String(), nil)
	require.NoError(t, err, "Error creating new request")

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err, "Error sending request")
	require.Equal(t, http.StatusUpgradeRequired, response.StatusCode)
}

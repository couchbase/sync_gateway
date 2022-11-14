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
	"fmt"
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

// move it to revocation
func TestReplicatorSwitchPurgeNoReset(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	defer db.SuspendSequenceBatching()()

	base.RequireNumTestBuckets(t, 2)

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", `{"name": "user", "password": "letmein", "admin_channels": ["A", "B"]}`)
	RequireStatus(t, resp, http.StatusOK)

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", "letmein")
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ReplicationStatsMap: dbstats,
	})

	for i := 0; i < 10; i++ {
		_ = rt2.CreateDocReturnRev(t, fmt.Sprintf("docA%d", i), "", map[string][]string{"channels": []string{"A"}})
	}

	for i := 0; i < 7; i++ {
		_ = rt2.CreateDocReturnRev(t, fmt.Sprintf("docB%d", i), "", map[string][]string{"channels": []string{"B"}})
	}

	err = rt2.WaitForPendingChanges()
	require.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))

	changesResults, err := rt1.WaitForChanges(17, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 17)

	// Going to stop & start replication between these actions to make out of order seq no's more likely. More likely
	// to hit CBG-1591
	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", `{"name": "user", "password": "letmein", "admin_channels": ["B"]}`)
	RequireStatus(t, resp, http.StatusOK)

	// Add another few docs to 'bump' rt1's seq no. Otherwise it'll end up revoking next time as the above user PUT is
	// not processed by the rt1 receiver.
	for i := 7; i < 15; i++ {
		_ = rt2.CreateDocReturnRev(t, fmt.Sprintf("docB%d", i), "", map[string][]string{"channels": []string{"B"}})
	}

	err = rt2.WaitForPendingChanges()
	assert.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	changesResults, err = rt1.WaitForChanges(8, fmt.Sprintf("/db/_changes?since=%v", changesResults.Last_Seq), "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 8)

	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
	sgwStats, err = base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
	require.NoError(t, err)
	dbstats, err = sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar = db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
	})

	// Send a doc to act as a 'marker' so we know when replication has completed
	_ = rt2.CreateDocReturnRev(t, "docMarker", "", map[string][]string{"channels": []string{"B"}})

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	// Validate none of the documents are purged after flipping option
	err = rt2.WaitForPendingChanges()
	assert.NoError(t, err)

	changesResults, err = rt1.WaitForChanges(1, fmt.Sprintf("/db/_changes?since=%v", changesResults.Last_Seq), "", true)
	assert.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

	for i := 0; i < 10; i++ {
		resp = rt1.SendAdminRequest("GET", fmt.Sprintf("/db/docA%d", i), "")
		RequireStatus(t, resp, http.StatusOK)
	}

	for i := 0; i < 7; i++ {
		resp = rt1.SendAdminRequest("GET", fmt.Sprintf("/db/docB%d", i), "")
		RequireStatus(t, resp, http.StatusOK)
	}

	// Shutdown replicator to close out
	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
}

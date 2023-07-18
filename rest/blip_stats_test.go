// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func sendGetCheckpointRequest(bt *BlipTester) {
	t := bt.restTester.TB
	rq := bt.newRequest()
	rq.SetProfile("getCheckpoint")
	require.True(t, bt.sender.Send(rq))
	errorCode, exists := rq.Response().Properties["Error-Code"]
	require.True(t, exists)
	require.Equal(t, "404", errorCode)
}

// waitForStatGreaterThan will retry for up to 20 seconds until the result of getStatFunc is equal to the expected value.
func waitForStatGreaterThan(t *testing.T, getStatFunc func() int64, expected int64) {
	workerFunc := func() (shouldRetry bool, err error, val interface{}) {
		val = getStatFunc()
		stat, ok := val.(int64)
		require.True(t, ok)
		return stat <= expected, nil, val
	}
	// wait for up to 20 seconds for the stat to meet the expected value
	err, val := base.RetryLoop(base.TestCtx(t), "waitForStatGreaterThan retry loop", workerFunc, base.CreateSleeperFunc(200, 100))
	require.NoError(t, err)
	valInt64, ok := val.(int64)
	require.True(t, ok)
	require.Greater(t, valInt64, expected)
}

func TestBlipStatsBasic(t *testing.T) {
	bt, err := NewBlipTester(t)
	require.NoError(t, err)
	defer bt.Close()

	// make sure requests have not incremented stats.
	/// Note: there is a blip call in NewBlipTester to initialize collections
	dbStats := bt.restTester.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), dbStats.ReplicationBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.ReplicationBytesSent.Value())

	// send a request, close BlipSyncContext and make sure stats are incremented
	sendGetCheckpointRequest(bt)

	// requests shouldn't be implemented as part of handler
	require.Equal(t, int64(0), dbStats.ReplicationBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.ReplicationBytesSent.Value())

	bt.sender.Close()

	waitForStatGreaterThan(t, dbStats.ReplicationBytesReceived.Value, 1)
	waitForStatGreaterThan(t, dbStats.ReplicationBytesSent.Value, 1)

}

func TestBlipStatsFastReport(t *testing.T) {
	bt, err := NewBlipTester(t)
	require.NoError(t, err)
	defer bt.Close()
	sendRequest := func() {
		rq := bt.newRequest()
		rq.SetProfile("getCheckpoint")
		require.True(t, bt.sender.Send(rq))
		errorCode, exists := rq.Response().Properties["Error-Code"]
		require.True(t, exists)
		require.Equal(t, "404", errorCode)
	}

	dbStats := bt.restTester.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), dbStats.ReplicationBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.ReplicationBytesSent.Value())

	sendRequest()

	require.Equal(t, int64(0), dbStats.ReplicationBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.ReplicationBytesSent.Value())

	// set reporting interval to update stats immediately
	bt.restTester.GetDatabase().Options.BlipStatsReportingInterval = 0
	sendRequest()
	require.Less(t, int64(0), dbStats.ReplicationBytesReceived.Value())
	require.Less(t, int64(0), dbStats.ReplicationBytesSent.Value())
}

// TestBlipStatsISGRComputePush:
//   - Put docs on activeRT
//   - Create push replication
//   - Wait for docs to replicate
//   - assert on stats after docs replicated
func TestBlipStatsISGRComputePush(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeySync)
	activeRT, passiveRT, remoteURL, teardown := SetupSGRPeers(t)
	defer teardown()
	const repName = "replication1"
	var resp *TestResponse

	activeSyncStartStat := activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
	passiveSyncStartStat := passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()

	for i := 0; i < 100; i++ {
		resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "activeRT"}`)
		RequireStatus(t, resp, http.StatusCreated)
	}

	activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

	_, err := passiveRT.WaitForChanges(100, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	activeSyncStat := activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
	passiveSyncStat := passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
	require.Greater(t, activeSyncStat, activeSyncStartStat)
	require.Greater(t, passiveSyncStat, passiveSyncStartStat)

}

// TestBlipStatsISGRComputePull:
//   - Put docs on passiveRT
//   - Create pull replication
//   - Wait for docs to replicate
//   - assert on stats after docs replicated
func TestBlipStatsISGRComputePull(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync)
	activeRT, passiveRT, remoteURL, teardown := SetupSGRPeers(t)
	defer teardown()
	const repName = "replication1"
	var resp *TestResponse

	activeSyncStat := activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
	passiveSyncStat := passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()

	for i := 0; i < 50; i++ {
		resp = passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "activeRT"}`)
		RequireStatus(t, resp, http.StatusCreated)
	}

	activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

	_, err := activeRT.WaitForChanges(50, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), activeSyncStat)
	require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), passiveSyncStat)

}

// TestBlipStatAttachmentComputeISGR:
//   - Two test cases (one with push replication and one with pull replication)
//   - Creat doc with attachment
//   - Create replication
//   - Wait for doc to be replicated
//   - Assert on expected value for stats
func TestBlipStatAttachmentComputeISGR(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync)
	testCases := []struct {
		name      string
		direction string
	}{
		{
			name:      "pushISGR",
			direction: "push",
		},
		{
			name:      "pullISGR",
			direction: "pull",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			activeRT, passiveRT, remoteURL, teardown := SetupSGRPeers(t)
			defer teardown()
			const repName = "replication1"
			var resp *TestResponse

			if test.direction == "push" {
				bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
				resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", bodyText)
				RequireStatus(t, resp, http.StatusCreated)
				// grab stats values before replication takes place
				syncComputeStartActive := activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
				syncComputeStartPassive := passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()

				// create replication
				activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
				activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

				// wait for changes
				_, err := passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
				require.NoError(t, err)

				// assert the stats increment/do not increment as expected
				require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncComputeStartActive)
				require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncComputeStartPassive)
			} else {
				bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
				resp = passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", bodyText)
				RequireStatus(t, resp, http.StatusCreated)
				// grab stats values before replication takes place
				syncComputeStartActive := activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
				syncComputeStartPassive := passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()

				// create replication
				activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault)
				activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

				// wait for changes
				_, err := activeRT.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
				require.NoError(t, err)

				// assert the stats increment/do not increment as expected
				require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncComputeStartActive)
				require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncComputeStartPassive)
			}

		})
	}

}

// TestComputeStatAfterContextTeardown:
//   - Add doc to passiveRT
//   - Create replication oneshot to pull this doc
//   - Assert that the stats are incremented
//   - Add another doc and start the replication again
//   - Assert the starts are incremented from their last value
//
// Objective of test is to test stats resume from correct value after a blipContext cancellation
func TestComputeStatAfterContextTeardown(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync)
	activeRT, passiveRT, remoteURL, teardown := SetupSGRPeers(t)
	defer teardown()
	const repName = "replication1"

	resp := passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"source": "activeRT"}`)
	RequireStatus(t, resp, http.StatusCreated)

	activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault)

	_, err := activeRT.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	// wait for replication teardown
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateStopped)

	syncProcessComputeActive := activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()
	syncProcessComputePassive := passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()

	// assert that sync compute stats are greater than 0
	require.Greater(t, syncProcessComputeActive, int64(0))
	require.Greater(t, syncProcessComputePassive, int64(0))

	// add new doc and start replication again
	resp = passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc2", `{"source": "activeRT"}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/"+repName+"?action=start", "")
	RequireStatus(t, resp, http.StatusOK)

	_, err = activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)
	// wait for replication teardown
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateStopped)

	// assert that the stats have increased from what they were before showing stats don't start from 0 again after replication teardown
	require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncProcessComputePassive)
	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncProcessComputeActive)

}

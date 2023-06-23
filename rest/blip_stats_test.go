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

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
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
	err, val := base.RetryLoop("waitForStatGreaterThan retry loop", workerFunc, base.CreateSleeperFunc(200, 100))
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
	activeRT.GetDatabase().Options.BlipStatsReportingInterval = 1
	passiveRT.GetDatabase().Options.BlipStatsReportingInterval = 1
	var resp *TestResponse

	docWriteCompute := activeRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value()
	docReadCompute := passiveRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value()
	docCheckPassive := passiveRT.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value()

	for i := 0; i < 100; i++ {
		resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "activeRT"}`)
		RequireStatus(t, resp, http.StatusCreated)
	}

	activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

	_, err := passiveRT.WaitForChanges(100, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	// assert active has written docs
	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value(), docWriteCompute)
	// assert that active has niot read docs along replicatiuon
	require.Equal(t, int64(0), activeRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value())
	// assert passtive has read docs along replication
	require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value(), docReadCompute)
	// assert passtive has not wrotten docs alongbg replication
	require.Equal(t, int64(0), passiveRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value())

	// for handling the set of changes sent by active
	require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value(), docCheckPassive)

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
	activeRT.GetDatabase().Options.BlipStatsReportingInterval = 1
	passiveRT.GetDatabase().Options.BlipStatsReportingInterval = 1
	var resp *TestResponse

	docWriteCompute := passiveRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value()
	docReadCompute := activeRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value()
	docCheckActive := activeRT.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value()

	for i := 0; i < 50; i++ {
		resp = passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "activeRT"}`)
		RequireStatus(t, resp, http.StatusCreated)
	}

	activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

	_, err := activeRT.WaitForChanges(50, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	// assert that passive peer wrote
	require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value(), docWriteCompute)
	// assert passive peer doc read compute stat is still 0
	require.Equal(t, int64(0), passiveRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value())
	// assert active peer doc read compute stat increased
	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value(), docReadCompute)
	// assert that active peer doc write compute stat is still 0
	require.Equal(t, int64(0), activeRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value())
	// assert active peer doc check compute stat incremented
	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value(), docCheckActive)

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
			activeRT.GetDatabase().Options.BlipStatsReportingInterval = 1
			passiveRT.GetDatabase().Options.BlipStatsReportingInterval = 1
			var resp *TestResponse

			if test.direction == "push" {
				bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
				resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", bodyText)
				RequireStatus(t, resp, http.StatusCreated)
				// grab stats values before replication takes place
				computeWriteAttachment := activeRT.GetDatabase().DbStats.DatabaseStats.WriteAttachmentComputeUnit.Value()
				computeReadAttachment := activeRT.GetDatabase().DbStats.DatabaseStats.ReadAttachmentComputeUnit.Value()

				// create replication
				activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
				activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

				// wait for changes
				_, err := passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
				require.NoError(t, err)

				// assert the stats increment/do not increment as expected
				require.Equal(t, int64(0), activeRT.GetDatabase().DbStats.DatabaseStats.ReadAttachmentComputeUnit.Value())
				require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.WriteAttachmentComputeUnit.Value(), computeWriteAttachment)
				require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.ReadAttachmentComputeUnit.Value(), computeReadAttachment)
				require.Equal(t, int64(0), passiveRT.GetDatabase().DbStats.DatabaseStats.WriteAttachmentComputeUnit.Value())
			} else {
				bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
				resp = passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", bodyText)
				RequireStatus(t, resp, http.StatusCreated)
				// grab stats values before replication takes place
				computeWriteAttachment := activeRT.GetDatabase().DbStats.DatabaseStats.WriteAttachmentComputeUnit.Value()
				computeReadAttachment := activeRT.GetDatabase().DbStats.DatabaseStats.ReadAttachmentComputeUnit.Value()

				// create replication
				activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault)
				activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

				// wait for changes
				_, err := activeRT.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
				require.NoError(t, err)

				// assert the stats increment/do not increment as expected
				require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.ReadAttachmentComputeUnit.Value(), computeReadAttachment)
				require.Equal(t, int64(0), activeRT.GetDatabase().DbStats.DatabaseStats.WriteAttachmentComputeUnit.Value())
				require.Equal(t, int64(0), passiveRT.GetDatabase().DbStats.DatabaseStats.ReadAttachmentComputeUnit.Value())
				require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.WriteAttachmentComputeUnit.Value(), computeWriteAttachment)
			}

		})
	}

}

// TestBlipStatsChangesComputation:
//   - Create Blip tester
//   - Send blip changes request
//   - assert on DocCheckedCompute stat
//   - send rev assert stat increased
//   - send proposeChanges request and assert the DocCheckedCompute stat increased
func TestBlipStatsChangesComputation(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync)
	bt, err := NewBlipTester(t)
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()
	bt.restTester.GetDatabase().Options.BlipStatsReportingInterval = 1

	// test changes request increments the DocCheckComputeStat
	changesRequest := bt.newRequest()
	changesRequest.SetProfile("changes")
	changesBody := `[["1", "foo", "1-abc", false]]`
	changesRequest.SetBody([]byte(changesBody))
	sent := bt.sender.Send(changesRequest)
	assert.True(t, sent)
	changesResponse := changesRequest.Response()
	assert.Equal(t, changesRequest.SerialNumber(), changesResponse.SerialNumber())

	require.Greater(t, bt.restTester.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value(), int64(0))
	changesCompute := bt.restTester.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value()

	// put a rev to increment the read compute stat
	_, _, revResponse, err := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	assert.NoError(t, err)

	_, err = revResponse.Body()
	assert.NoError(t, err, "Error unmarshalling response body")
	require.Greater(t, bt.restTester.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value(), int64(0))

	// test proposeChanges increments the DocCheckCompute stat
	proposeChangesRequest := bt.newRequest()
	proposeChangesRequest.SetProfile("proposeChanges")
	proposeChangesRequest.SetCompressed(true)

	// define changes body
	changesBody = `[["foo", "1-abc"],["foo2", "1-abc"]]`
	proposeChangesRequest.SetBody([]byte(changesBody))
	sent = bt.sender.Send(proposeChangesRequest)
	assert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	_, err = proposeChangesResponse.Body()
	assert.NoError(t, err, "Error getting changes response body")

	// assert the doc check compute stat has increased
	require.Greater(t, bt.restTester.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value(), changesCompute)

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
	activeRT.GetDatabase().Options.BlipStatsReportingInterval = 1
	passiveRT.GetDatabase().Options.BlipStatsReportingInterval = 1

	resp := passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"source": "activeRT"}`)
	RequireStatus(t, resp, http.StatusCreated)

	activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

	_, err := activeRT.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateStopped)

	docWriteCompute := passiveRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value()
	docReadCompute := activeRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value()
	docCheckActive := activeRT.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value()

	// assert that expected stats are greater than 0
	require.Greater(t, docWriteCompute, int64(0))
	require.Greater(t, docReadCompute, int64(0))
	require.Greater(t, docCheckActive, int64(0))

	// add new doc and start replication again
	resp = passiveRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc2", `{"source": "activeRT"}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/"+repName+"?action=start", "")
	RequireStatus(t, resp, http.StatusOK)

	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

	_, err = activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)
	activeRT.WaitForReplicationStatus(repName, db.ReplicationStateStopped)

	// assert that the stats have increased from what they were before
	require.Greater(t, passiveRT.GetDatabase().DbStats.DatabaseStats.DocWriteComputeUnit.Value(), docReadCompute)
	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.DocReadComputeUnit.Value(), docReadCompute)
	require.Greater(t, activeRT.GetDatabase().DbStats.DatabaseStats.DocCheckComputeUnit.Value(), docCheckActive)

}

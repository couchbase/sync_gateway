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
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRequestPlusPull tests that a one-shot pull replication waits for pending changes when request plus is set on the replication.
func TestRequestPlusPull(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyDCP, base.KeyChanges, base.KeyHTTP)()
	defer db.SuspendSequenceBatching()() // Required for slow sequence simulation

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	database := rt.GetDatabase()

	// Initialize blip tester client (will create user)
	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username: "bernard",
	})
	require.NoError(t, err)
	defer client.Close()

	// Put a doc in channel PBS
	response := rt.SendAdminRequest("PUT", "/db/pbs-1", `{"channel":["PBS"]}`)
	RequireStatus(t, response, 201)

	// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
	slowSequence, seqErr := db.AllocateTestSequence(database)
	require.NoError(t, seqErr)

	// Write a document granting user 'bernard' access to PBS
	response = rt.SendAdminRequest("PUT", "/db/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
	RequireStatus(t, response, 201)

	caughtUpStart := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()

	// Start a regular one-shot pull
	err = client.StartOneshotPullRequestPlus()
	assert.NoError(t, err)

	// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
	require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+1))

	// Release the slow sequence
	releaseErr := db.ReleaseTestSequence(database, slowSequence)
	require.NoError(t, releaseErr)

	// The one-shot pull should unblock and replicate the document in the granted channel
	data, ok := client.WaitForDoc("pbs-1")
	assert.True(t, ok)
	assert.Equal(t, `{"channel":["PBS"]}`, string(data))

}

// TestRequestPlusPull tests that a one-shot pull replication waits for pending changes when request plus is set on the db config.
func TestRequestPlusPullDbConfig(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyDCP, base.KeyChanges, base.KeyHTTP)()
	defer db.SuspendSequenceBatching()() // Required for slow sequence simulation

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				ChangesRequestPlus: base.BoolPtr(true),
			},
		},
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	database := rt.GetDatabase()

	// Initialize blip tester client (will create user)
	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username: "bernard",
	})
	require.NoError(t, err)
	defer client.Close()

	// Put a doc in channel PBS
	response := rt.SendAdminRequest("PUT", "/db/pbs-1", `{"channel":["PBS"]}`)
	RequireStatus(t, response, 201)

	// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
	slowSequence, seqErr := db.AllocateTestSequence(database)
	require.NoError(t, seqErr)

	// Write a document granting user 'bernard' access to PBS
	response = rt.SendAdminRequest("PUT", "/db/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
	RequireStatus(t, response, 201)

	caughtUpStart := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()

	// Start a regular one-shot pull
	err = client.StartOneshotPull()
	assert.NoError(t, err)

	// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
	require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+1))

	// Release the slow sequence
	releaseErr := db.ReleaseTestSequence(database, slowSequence)
	require.NoError(t, releaseErr)

	// The one-shot pull should unblock and replicate the document in the granted channel
	data, ok := client.WaitForDoc("pbs-1")
	assert.True(t, ok)
	assert.Equal(t, `{"channel":["PBS"]}`, string(data))

}

// TestBlipRefreshUser makes sure there is no panic if a user gets deleted during a replication
func TestBlipRefreshUser(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	const username = "bernard"
	// Initialize blip tester client (will create user)
	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username: "bernard",
		Channels: []string{"chan1"},
	})

	require.NoError(t, err)
	defer btc.Close()

	// add chan1 explicitly
	response := rt.SendAdminRequest(http.MethodPut, "/db/_user/"+username, `{"name": "bernard", "password" : "letmein", "admin_channels" : ["chan1"]}`)

	docID := "doc1"

	RequireStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"channels":["chan1"]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendUserRequest(http.MethodGet, "/db/"+docID, "", username)
	RequireStatus(t, response, http.StatusOK)

	// Start a regular one-shot pull
	err = btc.StartPullSince("true", "0", "false", "", "")
	require.NoError(t, err)

	_, ok := btc.WaitForDoc(docID)
	require.True(t, ok)

	_, ok = btc.GetRev(docID, "1-78211b5eedea356c9693e08bc68b93ce")
	require.True(t, ok)

	// delete user with an active blip connection
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/"+username, "")
	RequireStatus(t, response, http.StatusOK)

	err = rt.WaitForPendingChanges()
	require.NoError(t, err)
	msg := blip.NewRequest()
	msg.SetProfile(db.MessageRev)
	msg.Properties[db.RevMessageId] = "doesnotexist"
	msg.Properties[db.RevMessageRev] = "1-fakerev"
	msg.SetBody([]byte(`{"foo": "bar"}`))

	err = btc.pullReplication.sendMsg(msg)
	require.NoError(t, err)

	testResponse := msg.Response()
	body, err := testResponse.Body()
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(db.CBLReconnectErrorCode), testResponse.Properties[db.BlipErrorCode])
	require.NotContains(t, string(body), "Panic:")
}

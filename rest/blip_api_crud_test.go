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
	"testing"

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

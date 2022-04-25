//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
//go:build !race
// +build !race

package rest

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

// TestPushReplicationAPIUpdateDatabase starts a push replication and updates the passive database underneath the replication.
// Expect to see the connection closed with an error, instead of continuously panicking.
// This is the ISGR version of TestBlipPusherUpdateDatabase
//
// This test causes the race detector to flag the bucket=nil operation and any in-flight requests being made using that bucket, prior to the replication being reset.
// TODO CBG-1903: Can be fixed by draining in-flight requests before fully closing the database.
func TestPushReplicationAPIUpdateDatabase(t *testing.T) {

	t.Skip("Skipping test - revisit in CBG-1908")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test does not support Walrus - depends on closing and re-opening persistent bucket")
	}

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	rt1, rt2, remoteURLString, teardown := setupSGRPeers(t)
	defer teardown()

	// Create initial doc on rt1
	docID := t.Name() + "rt1doc"
	_ = rt1.putDoc(docID, `{"source":"rt1","channels":["alice"]}`)

	// Create push replication, verify running
	replicationID := t.Name()
	rt1.createReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	rt1.waitForReplicationStatus(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults := rt2.RequireWaitChanges(1, "0")
	require.Equal(t, docID, changesResults.Results[0].ID)

	var lastDocID atomic.Value

	// Wait for the background updates to finish at the end of the test
	shouldCreateDocs := base.NewAtomicBool(true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer func() {
		shouldCreateDocs.Set(false)
		wg.Wait()
	}()

	// Start creating documents in the background on rt1 for the replicator to push to rt2
	go func() {
		// for i := 0; i < 10; i++ {
		for i := 0; shouldCreateDocs.IsTrue(); i++ {
			resp := rt1.putDoc(fmt.Sprintf("%s-doc%d", t.Name(), i), fmt.Sprintf(`{"i":%d,"channels":["alice"]}`, i))
			lastDocID.Store(resp.ID)
		}
		_ = rt1.WaitForPendingChanges()
		wg.Done()
	}()

	// and wait for a few to be done before we proceed with updating database config underneath replication
	_, err := rt2.WaitForChanges(5, "/db/_changes", "", true)
	require.NoError(t, err)

	// just change the sync function to cause the database to reload
	dbConfig := *rt2.ServerContext().GetDbConfig("db")
	dbConfig.Sync = base.StringPtr(`function(doc){channel(doc.channels);}`)
	resp, err := rt2.ReplaceDbConfig("db", dbConfig)
	require.NoError(t, err)
	assertStatus(t, resp, http.StatusCreated)

	shouldCreateDocs.Set(false)

	lastDocIDString, ok := lastDocID.Load().(string)
	require.True(t, ok)

	// wait for the last document written to rt1 to arrive at rt2
	waitAndAssertCondition(t, func() bool {
		_, err := rt2.GetDatabase().GetDocument(base.TestCtx(t), lastDocIDString, db.DocUnmarshalNone)
		return err == nil
	})
}

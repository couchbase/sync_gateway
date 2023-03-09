// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package replicatortest

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/document"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbaselabs/walrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions for SGR testing

func reduceTestCheckpointInterval(interval time.Duration) func() {
	previousInterval := db.DefaultCheckpointInterval
	db.DefaultCheckpointInterval = interval
	return func() {
		db.DefaultCheckpointInterval = previousInterval
	}

}

// AddActiveRT returns a new RestTester backed by a no-close clone of TestBucket
func addActiveRT(t *testing.T, dbName string, testBucket *base.TestBucket) (activeRT *rest.RestTester) {

	// CBG-2766 adding a new RestTester on the same database will cause stats to be broken, when fixing change to `NewRestTester`
	// Create a new rest tester, using a NoCloseClone of testBucket, which disables the TestBucketPool teardown
	activeRT = rest.NewRestTesterDefaultCollection(t,
		&rest.RestTesterConfig{
			CustomTestBucket:   testBucket.NoCloseClone(),
			SgReplicateEnabled: true,
			SyncFn:             channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{
				DbConfig: rest.DbConfig{
					Name: dbName,
				},
			},
		})

	// If this is a walrus bucket, we need to jump through some hoops to ensure the shared in-memory walrus bucket isn't
	// deleted when bucket.Close() is called during DatabaseContext.Close().
	// Using IgnoreClose in leakyBucket to no-op the close operation.
	// Because RestTester has Sync Gateway create the database context and bucket based on the bucketSpec, we can't
	// set up the leakyBucket wrapper prior to bucket creation.
	// Instead, we need to modify the leaky bucket config (created for vbno handling) after the fact.
	leakyBucket, ok := base.AsLeakyBucket(activeRT.GetDatabase().Bucket)
	if ok {
		ub := leakyBucket.GetUnderlyingBucket()
		_, isWalrusBucket := ub.(*walrus.WalrusBucket)
		_, isWalrusCollectionBucket := ub.(*walrus.CollectionBucket)
		if isWalrusBucket || isWalrusCollectionBucket {
			leakyBucket.SetIgnoreClose(true)
		}
	}

	// Trigger the lazy load of bucket for RestTester startup
	_ = activeRT.Bucket()

	return activeRT
}

// requireRevID asserts that the specified document revision is written to the
// underlying bucket backed by the given RestTester instance.
func requireRevID(t *testing.T, rt *rest.RestTester, docID, revID string) {
	doc, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	require.NoError(t, err, "Error reading document from bucket")
	require.Equal(t, revID, doc.SyncData.CurrentRev)
}
func requireErrorKeyNotFound(t *testing.T, rt *rest.RestTester, docID string) {
	var body []byte
	_, err := rt.Bucket().DefaultDataStore().Get(docID, &body)
	require.Error(t, err)
	require.True(t, base.IsKeyNotFoundError(rt.Bucket().DefaultDataStore(), err))
}

// waitForTombstone waits until the specified tombstone revision is available
// in the bucket backed by the specified RestTester instance.
func waitForTombstone(t *testing.T, rt *rest.RestTester, docID string) {
	require.NoError(t, rt.WaitForPendingChanges())
	require.NoError(t, rt.WaitForCondition(func() bool {
		doc, _ := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		return doc.IsDeleted() && len(doc.Body()) == 0
	}))
}

// createOrUpdateDoc creates a new document or update an existing document with the
// specified document id, revision id and body value in a channel named "alice".
func createOrUpdateDoc(t *testing.T, rt *rest.RestTester, docID, revID, bodyValue string) string {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	dbURL := "/{{.keyspace}}/" + docID
	if revID != "" {
		dbURL = "/{{.keyspace}}/" + docID + "?rev=" + revID
	}
	resp := rt.SendAdminRequest(http.MethodPut, dbURL, body)
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())
	return rest.RespRevID(t, resp)
}
func getTestRevpos(t *testing.T, doc db.Body, attachmentKey string) (revpos int) {
	attachments := document.GetBodyAttachments(doc)
	if attachments == nil {
		return 0
	}
	attachment, ok := attachments[attachmentKey].(map[string]interface{})
	assert.True(t, ok)
	if !ok {
		return 0
	}
	revposInt64, ok := base.ToInt64(attachment["revpos"])
	assert.True(t, ok)
	return int(revposInt64)
}

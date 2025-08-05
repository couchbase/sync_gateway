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
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbaselabs/rosmar"
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

	// Create a new rest tester, using a NoCloseClone of testBucket, which disables the TestBucketPool teardown
	activeRT = rest.NewRestTester(t,
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
		_, isWalrusBucket := ub.(*rosmar.Bucket)
		if isWalrusBucket {
			leakyBucket.SetIgnoreClose(true)
		}
	}

	// Trigger the lazy load of bucket for RestTester startup
	_ = activeRT.Bucket()

	return activeRT
}

// requireDocumentVersion asserts that the given document has the expected version
func requireDocumentVersion(t testing.TB, expected rest.DocVersion, doc *db.Document, checkCV bool) {
	if checkCV {
		require.Equal(t, expected.CV, *doc.SyncData.HLV.ExtractCurrentVersionFromHLV())
	}
	require.Equal(t, expected.RevTreeID, doc.SyncData.CurrentRev)
}

// createOrUpdateDoc creates a new document the specified document id, and body value in a channel named "alice".
func createDoc(rt *rest.RestTester, docID string, bodyValue string) rest.DocVersion {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	updatedVersion := rt.PutDoc(docID, body)
	// make sure doc is available to changes feed
	rt.WaitForPendingChanges()
	return updatedVersion
}

// updateDoc update an existing document with the specified document id, version and body value in a channel named "alice".
func updateDoc(rt *rest.RestTester, docID string, version rest.DocVersion, bodyValue string) rest.DocVersion {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	updatedVersion := rt.UpdateDoc(docID, version, body)
	// make sure doc is available to changes feed
	rt.WaitForPendingChanges()
	return updatedVersion
}

func getTestRevpos(t *testing.T, doc db.Body, attachmentKey string) (revpos int) {
	attachments := db.GetBodyAttachments(doc)
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

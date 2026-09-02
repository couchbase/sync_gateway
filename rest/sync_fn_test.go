//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestSyncFnBodyProperties puts a document into channels based on which properties are present on the document.
// This can be used to introspect what properties ended up in the body passed to the sync function.
func TestSyncFnBodyProperties(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must EXACTLY match the top-level properties seen in the sync function body.
	// Properties not present in this list, but present in the sync function body will be caught.
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
		db.BodyRev,
	}

	// This sync function routes into channels based on top-level properties contained in doc
	syncFn := `function(doc) {
		console.log("full doc: "+JSON.stringify(doc));
		for (var p in doc) {
			console.log("doc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Ptr(uint32(0))}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+testDocID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnBodyPropertiesTombstone puts a document into channels based on which properties are present on the document.
// It creates a doc, and then tombstones it to see what properties are present in the body of the tombstone.
func TestSyncFnBodyPropertiesTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must be present in the sync function body for a tombstone
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
		db.BodyRev,
		db.BodyDeleted,
	}

	// This sync function routes into channels based on top-level properties contained in doc
	syncFn := `function(doc) {
		console.log("full doc: "+JSON.stringify(doc));
		for (var p in doc) {
			console.log("doc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+testDocID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, revID), `{}`)
	RequireStatus(t, response, 200)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnOldDocBodyProperties puts a document into channels based on which properties are present in the 'oldDoc' body.
// It creates a doc, and updates it to inspect what properties are present on the oldDoc body.
func TestSyncFnOldDocBodyProperties(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must be present in the sync function oldDoc body for a regular PUT containing testdataKey
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
	}

	// This sync function routes into channels based on top-level properties contained in oldDoc
	syncFn := `function(doc, oldDoc) {
		console.log("full doc: "+JSON.stringify(doc));
		console.log("full oldDoc: "+JSON.stringify(oldDoc));
		for (var p in oldDoc) {
			console.log("oldDoc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+testDocID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, revID), `{"`+testdataKey+`":true,"update":2}`)
	RequireStatus(t, response, 201)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn oldDoc body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnOldDocBodyPropertiesTombstoneResurrect puts a document into channels based on which properties are present in the 'oldDoc' body.
// It creates a doc, tombstones it, and then resurrects it to inspect oldDoc properties on the tombstone.
func TestSyncFnOldDocBodyPropertiesTombstoneResurrect(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must be present in the sync function body for a regular PUT containing testdataKey
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
		db.BodyDeleted,
	}

	// This sync function routes into channels based on top-level properties contained in oldDoc
	syncFn := `function(doc, oldDoc) {
		console.log("full doc: "+JSON.stringify(doc));
		console.log("full oldDoc: "+JSON.stringify(oldDoc));
		for (var p in oldDoc) {
			console.log("oldDoc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+testDocID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, revID), `{}`)
	RequireStatus(t, response, 200)
	body = nil
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID = body["rev"].(string)

	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, revID), `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn oldDoc body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnDocBodyPropertiesSwitchActiveTombstone creates a branched revtree, where the first tombstone created becomes active again after the shorter b branch is tombstoned.
// The test makes sure that in this scenario, the "doc" body of the sync function when switching from (T) 3-b to (T) 4-a contains a _deleted property (stamped by getAvailable1xRev)
//
//	1-a
//	├── 2-a
//	│   └── 3-a
//	│       └──────── (T) 4-a
//	└──────────── 2-b
//	              └────────────── (T) 3-b
func TestSyncFnDocBodyPropertiesSwitchActiveTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// This sync function logs a warning for each revision pushed through the sync function, and an error when it sees _deleted inside doc, when oldDoc contains syncOldDocBodyCheck=true
	//
	// These are then asserted by looking at the expvar stats for warn and error counts.
	// We can't rely on channels to get information out of the sync function environment, because we'd need an active doc, which this test does not allow for.
	syncFn := `function(doc, oldDoc) {
		console.log("full doc: "+JSON.stringify(doc));
		console.log("full oldDoc: "+JSON.stringify(oldDoc));

		if (doc.testdata == 1 || (oldDoc != null && !oldDoc.syncOldDocBodyCheck)) {
			console.log("skipping oldDoc property checks for this rev")
			return
		}

		if (doc != null && doc._deleted) {
			console.error("doc contained _deleted")
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// rev 1-a
	version1a := rt.PutDoc(testDocID, `{"`+testdataKey+`":1}`)
	// rev 2-a
	version2a := rt.UpdateDoc(testDocID, version1a, `{"`+testdataKey+`":2}`)
	// rev 3-a
	version3a := rt.UpdateDoc(testDocID, version2a, `{"`+testdataKey+`":3,"syncOldDocBodyCheck":true}`)

	// rev 2-b
	version2b := rt.PutNewEditsFalse(testDocID, NewDocVersionFromFakeRev("2-b"), &version1a, `{}`)

	// tombstone at 4-a
	rt.DeleteDoc(testDocID, version3a)

	numErrorsBefore, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)
	// tombstone at 3-b
	rt.DeleteDoc(testDocID, *version2b)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, 1, numErrorsAfter-numErrorsBefore, "expecting to see only only 1 error logged")
}

// TestSyncFnLiveBranchPromotedWithUnreadableBody tombstones the winning revision of a conflicted
// document so that a live branch is promoted, while reads of that branch's externalised body fail
// with a transient bucket error.
//
//	1-a
//	├── 2-a                     live, body externalised to a _sync:rb: doc
//	└── 2-fff... (winner)
//	    └── (T) 3-fff...        tombstoned by this test, promoting 2-a
//
// recalculateSyncFnForActiveRev can't distinguish that failure from a body that has genuinely
// expired, because getAvailableRev reports both as a 404. Swallowing it would run the promoted live
// revision through updateChannels(nil), removing the document from every channel it is in and
// dropping its access grants, so the write must fail and be retried instead.
func TestSyncFnLiveBranchPromotedWithUnreadableBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const (
		testDocID = "testdoc"
		branchB   = "2-ffffffffffffffffffffffffffffffff"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:      true,
		LeakyBucketConfig: &base.LeakyBucketConfig{},
		SyncFn:            `function(doc){ if (!doc._deleted) { channel(doc.channels); } }`,
	})
	defer rt.Close()
	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// Bodies need to be over db.MaximumInlineBodySize so that a non-winning body is held in a
	// _sync:rb: document rather than inline in the rev tree, and so requires a bucket read.
	padding := strings.Repeat("x", 500)
	docBody := func(branch string) string {
		return fmt.Sprintf(`{"channels":["chanA"],"branch":%q,"padding":%q}`, branch, padding)
	}

	version1a := rt.PutDoc(testDocID, docBody("base"))
	version2a := rt.UpdateDoc(testDocID, version1a, docBody("a"))
	// branchB beats 2-a in the revid comparison, so branch b becomes the winner and branch a's body
	// is moved out of the document into a _sync:rb: doc.
	version2b := rt.PutNewEditsFalse(testDocID, NewDocVersionFromFakeRev(branchB), &version1a, docBody("b"))

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, version2b.RevTreeID, doc.GetRevTreeID(), "branch b should be the winning revision")
	bodyKey := doc.History[version2a.RevTreeID].BodyKey
	require.NotEmpty(t, bodyKey, "branch a's body should have been externalised")

	// Fail every backup body read for the duration of the tombstoning write.
	leakyDataStore, ok := base.AsLeakyDataStore(rt.GetSingleDataStore())
	require.True(t, ok)
	leakyDataStore.SetGetRawCallback(func(key string) error {
		if strings.HasPrefix(key, base.RevBodyPrefix) || strings.HasPrefix(key, base.RevPrefix) {
			return gocb.ErrTimeout
		}
		return nil
	})

	// Tombstone branch b, which promotes branch a to winning revision.
	response := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, version2b.RevTreeID), "")
	RequireStatus(t, response, http.StatusServiceUnavailable)

	leakyDataStore.SetGetRawCallback(nil)

	// The refused write must have left the document, and branch a's externalised body, untouched.
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version2b.RevTreeID, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
	removal, inChannel := doc.Channels["chanA"]
	assert.True(t, inChannel && removal == nil, "the live document should still be in chanA, got %+v", doc.Channels)
	_, _, err = rt.GetSingleDataStore().GetRaw(ctx, bodyKey)
	assert.NoError(t, err, "branch a's externalised body should not have been deleted")

	// Once the bucket is healthy again the same write succeeds, and branch a keeps its channel.
	rt.DeleteDoc(testDocID, *version2b)
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, version2a.RevTreeID, doc.GetRevTreeID(), "branch a should now be the winning revision")
	require.False(t, doc.IsDeleted())
	removal, inChannel = doc.Channels["chanA"]
	assert.True(t, inChannel && removal == nil, "the promoted live revision should be in chanA, got %+v", doc.Channels)
}

// setUpPromotedTombstoneBranch builds the rev tree an ISGR pull leaves behind when conflict
// resolution keeps the local branch: the local body is re-parented onto the remote branch as the
// live winner, and the original local branch is tombstoned at a higher generation. That tombstone
// holds no body of its own, so once the winner is tombstoned it is promoted and only its ancestors'
// backup bodies can supply a body for the sync function.
//
//	1-a ... 6-a                     local branch
//	└── (T) 7-a                     tombstoned by conflict resolution, no body of its own
//	1-remote ... 3-remote
//	└── 4-x                         live winner, minted from the local body
//
// The document is in chanA. Returns the live winning revision and the tombstoned leaf.
func setUpPromotedTombstoneBranch(t *testing.T, rt *RestTester, docID string) (winnerRev, tombstoneRev string) {
	t.Helper()

	const (
		localBranchGen  = 6
		remoteBranchGen = 3
	)

	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	version := rt.PutDoc(docID, `{"channels":["chanA"],"gen":1}`)
	for gen := 2; gen <= localBranchGen; gen++ {
		version = rt.UpdateDoc(docID, version, fmt.Sprintf(`{"channels":["chanA"],"gen":%d}`, gen))
	}

	// Drive PutExistingRevWithConflictResolution directly rather than standing up a second cluster -
	// it is the same function an ISGR active pull calls. The default resolver sees both sides live and
	// picks the higher generation, which is the local branch, so resolveDocLocalWins runs.
	remoteHistory := make([]string, 0, remoteBranchGen)
	for gen := remoteBranchGen; gen >= 1; gen-- {
		remoteHistory = append(remoteHistory, fmt.Sprintf("%d-%s", gen, strings.Repeat("a", 32)))
	}
	remoteDoc := &db.Document{ID: docID, RevID: remoteHistory[0]}
	remoteDoc.UpdateBody(db.Body{"branch": "remote", "channels": []string{"chanA"}})

	_, rawBucketDoc, err := collection.GetDocumentWithRaw(ctx, docID, db.DocUnmarshalSync)
	require.NoError(t, err)
	_, _, err = collection.PutExistingRevWithConflictResolution(ctx, db.PutDocOptions{
		NewDoc:           remoteDoc,
		RevTreeHistory:   remoteHistory,
		NoConflicts:      true,
		ConflictResolver: db.NewConflictResolver(db.DefaultConflictResolver, nil),
		ExistingDoc:      rawBucketDoc,
	})
	require.NoError(t, err, "ISGR conflict resolution should succeed")

	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.False(t, doc.IsDeleted(), "the live winner should beat the tombstone")
	require.Len(t, doc.History.GetLeaves(), 2)

	winnerRev = doc.GetRevTreeID()
	for _, leaf := range doc.History.GetLeaves() {
		if doc.History[leaf].Deleted {
			tombstoneRev = leaf
		}
	}
	require.NotEmpty(t, tombstoneRev, "expected a tombstoned leaf")
	require.Empty(t, doc.History[tombstoneRev].Body, "the tombstone should hold no inline body")
	require.Empty(t, doc.History[tombstoneRev].BodyKey, "the tombstone should hold no body key")
	return winnerRev, tombstoneRev
}

// TestSyncFnPromotedTombstoneWithUnreadableBackupBody is the tombstone twin of
// TestSyncFnLiveBranchPromotedWithUnreadableBody: the promoted branch's backup bodies are still in the
// bucket, and the only reason the read fails is a transient bucket error.
//
// getAvailableRev discards getRevision's error and reports every failure as a 404, so
// recalculateSyncFnForActiveRev cannot tell that apart from a body that has genuinely expired. The
// document's channels and access grants must not be dropped on the strength of an error that says
// nothing about whether the body exists - the write has to fail and be retried instead.
func TestSyncFnPromotedTombstoneWithUnreadableBackupBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	testCases := []struct {
		name             string
		failBackupReads  bool
		expectedStatus   int
		expectTombstoned bool
	}{
		{
			// The backup body is readable, so the sync function runs for the promoted tombstone and its
			// grants are recalculated as normal.
			name:             "readable backup body",
			failBackupReads:  false,
			expectedStatus:   http.StatusOK,
			expectTombstoned: true,
		},
		{
			// The backup body is present but unreadable, so there is no basis for stripping the grants.
			name:             "transient backup body read error",
			failBackupReads:  true,
			expectedStatus:   http.StatusServiceUnavailable,
			expectTombstoned: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				GuestEnabled:      true,
				LeakyBucketConfig: &base.LeakyBucketConfig{},
				SyncFn:            `function(doc){ channel(doc.channels); access("bob", "grantedChan"); }`,
			})
			defer rt.Close()

			winnerRev, tombstoneRev := setUpPromotedTombstoneBranch(t, rt, testDocID)
			collection, ctx := rt.GetSingleTestDatabaseCollection()

			doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
			require.NoError(t, err)
			_, bobGranted := doc.Access["bob"]
			require.True(t, bobGranted, "the live document should have granted bob access, got %+v", doc.Access)

			// Unlike TestSyncFnPromotedTombstoneWithExpiredBackupBody the backup bodies are left in place -
			// a transient bucket error is the only thing standing between the write and the body.
			if testCase.failBackupReads {
				leakyDataStore, ok := base.AsLeakyDataStore(rt.GetSingleDataStore())
				require.True(t, ok)
				leakyDataStore.SetGetRawCallback(func(key string) error {
					if strings.HasPrefix(key, base.RevBodyPrefix) || strings.HasPrefix(key, base.RevPrefix) {
						return gocb.ErrTimeout
					}
					return nil
				})
				defer leakyDataStore.SetGetRawCallback(nil)
			}

			// Tombstoning the winner promotes the tombstone branch, whose body only its ancestors' backups
			// can supply.
			response := rt.SendAdminRequest(http.MethodDelete,
				fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, winnerRev), "")
			AssertStatus(t, response, testCase.expectedStatus)

			doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
			require.NoError(t, err)
			if testCase.expectTombstoned {
				assert.Equal(t, tombstoneRev, doc.GetRevTreeID(), "the tombstone branch should have been promoted")
				assert.True(t, doc.IsDeleted())
			} else {
				assert.Equal(t, winnerRev, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
				assert.False(t, doc.IsDeleted())
			}

			// Either way the sync function's output for the promoted branch is intact - it either ran, or the
			// write was refused before anything was recalculated.
			_, bobGranted = doc.Access["bob"]
			assert.True(t, bobGranted, "bob's access grant should not have been revoked, got %+v", doc.Access)
		})
	}
}

// TestSyncFnPromotedTombstoneWithExpiredBackupBody pins what recalculateSyncFnForActiveRev does when
// a promoted tombstone branch has no body left anywhere: the sync function can't be run for it, so
// the document ends up in no channels and with no access grants, rather than the write failing.
func TestSyncFnPromotedTombstoneWithExpiredBackupBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		SyncFn:       `function(doc){ channel(doc.channels); access("bob", "grantedChan"); }`,
	})
	defer rt.Close()

	winnerRev, tombstoneRev := setUpPromotedTombstoneBranch(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	_, bobGranted := doc.Access["bob"]
	require.True(t, bobGranted, "the live document should have granted bob access, got %+v", doc.Access)

	// Model the ancestors' backup bodies ageing out after old_rev_expiry_seconds.
	deleteBackupRevisionBodies(t, rt, testDocID)

	// Tombstoning the winner promotes the tombstone branch, whose body is now unavailable.
	response := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, winnerRev), "")
	RequireStatus(t, response, http.StatusOK)

	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, tombstoneRev, doc.GetRevTreeID(), "the tombstone branch should have been promoted")
	require.True(t, doc.IsDeleted())

	removal, inChannel := doc.Channels["chanA"]
	require.True(t, inChannel, "chanA should still be recorded on the document")
	require.NotNil(t, removal, "chanA should be marked as a removal, not left active")
	assert.True(t, removal.Deleted, "the removal should be flagged as a deletion so pull replications send a tombstone")
	assert.Empty(t, doc.Access, "the promoted tombstone's access grants should have been revoked")
}

// TestSyncFnPromotedTombstoneWithCorruptBackupBody checks that only a genuinely missing body is
// tolerated when a tombstone branch is promoted. A backup body that is readable but unusable is an
// error we have no answer for, so it must fail the write rather than quietly dropping the
// document's channels and access grants.
func TestSyncFnPromotedTombstoneWithCorruptBackupBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		SyncFn:       `function(doc){ channel(doc.channels); }`,
	})
	defer rt.Close()

	winnerRev, _ := setUpPromotedTombstoneBranch(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)

	// Rewrite every backup rev body as a JSON array. getAvailableRev hands it back without complaint,
	// but it can't be used as a document body. The leading byte is nonJSONPrefixKindRevBody, which
	// marks the remainder as a plain JSON body.
	corruptBody := append([]byte{1}, []byte(`[1,2,3]`)...)
	dataStore := rt.GetSingleDataStore()
	corrupted := 0
	for revID := range doc.History {
		key := fmt.Sprintf("%s%s:%d:%s", base.RevPrefix, testDocID, len(revID), revID)
		if _, _, getErr := dataStore.GetRaw(ctx, key); getErr != nil {
			require.True(t, base.IsDocNotFoundError(getErr), "unexpected error reading %s: %v", key, getErr)
			continue
		}
		require.NoError(t, dataStore.SetRaw(ctx, key, 0, nil, corruptBody))
		corrupted++
	}
	require.NotZero(t, corrupted, "expected at least one backup rev body to corrupt")

	// Tombstoning the winner promotes the tombstone branch, which walks back to a corrupt body.
	response := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, winnerRev), "")
	RequireStatus(t, response, http.StatusInternalServerError)

	// The refused write must have left the document alone.
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, winnerRev, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
	assert.False(t, doc.IsDeleted())
	removal, inChannel := doc.Channels["chanA"]
	assert.True(t, inChannel && removal == nil, "the live document should still be in chanA, got %+v", doc.Channels)
}

func TestSyncFunctionErrorLogging(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	rtConfig := RestTesterConfig{SyncFn: `
		function(doc) {
			console.error("Error");
			console.log("Log");
			channel(doc.channel);
		}`}

	rt := NewRestTester(t, &rtConfig)

	defer rt.Close()

	// Wait for the DB to be ready before attempting to get initial error count
	rt.WaitForDBOnline()

	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo": "bar"}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, numErrors+1, numErrorsAfter)
}

func TestSyncFunctionException(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	rtConfig := RestTesterConfig{
		SyncFn: `
		function(doc) {
			if (doc.throwException) {
				channel(undefinedvariable);
			}
			if (doc.throwExplicit) {
				throw("Explicit exception");
			}
			if (doc.throwForbidden) {
				throw({forbidden: "read only!"})
			}
			if (doc.require) {
				requireAdmin();
			}
		}`,
		GuestEnabled: true,
	}

	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Wait for the DB to be ready before attempting to get initial error count
	rt.WaitForDBOnline()

	numDBSyncExceptionsStart := rt.GetDatabase().DbStats.Database().SyncFunctionExceptionCount.Value()

	// runtime error
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"throwException":true}`)
	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Contains(t, response.Body.String(), "Exception in JS sync function")

	numDBSyncExceptions := rt.GetDatabase().DbStats.Database().SyncFunctionExceptionCount.Value()
	assert.Equal(t, numDBSyncExceptionsStart+1, numDBSyncExceptions)
	numDBSyncExceptionsStart = numDBSyncExceptions

	// explicit throws should cause an exception
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"throwExplicit":true}`)
	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Contains(t, response.Body.String(), "Exception in JS sync function")

	numDBSyncExceptions = rt.GetDatabase().DbStats.Database().SyncFunctionExceptionCount.Value()
	assert.Equal(t, numDBSyncExceptionsStart+1, numDBSyncExceptions)
	numDBSyncExceptionsStart = numDBSyncExceptions
	numDBSyncRejected := rt.GetDatabase().DbStats.Security().NumDocsRejected.Value()
	assert.Equal(t, int64(0), numDBSyncRejected)

	// throw with a forbidden property shouldn't cause a true exception
	response = rt.SendRequest("PUT", "/{{.keyspace}}/doc3", `{"throwForbidden":true}`)
	assert.Equal(t, http.StatusForbidden, response.Code)
	assert.Contains(t, response.Body.String(), "read only!")
	numDBSyncExceptions = rt.GetDatabase().DbStats.Database().SyncFunctionExceptionCount.Value()
	assert.Equal(t, numDBSyncExceptionsStart, numDBSyncExceptions)
	numDBSyncRejected = rt.GetDatabase().DbStats.Security().NumDocsRejected.Value()
	assert.Equal(t, int64(1), numDBSyncRejected)

	// require methods shouldn't cause a true exception
	response = rt.SendRequest("PUT", "/{{.keyspace}}/doc4", `{"require":true}`)
	assert.Equal(t, http.StatusForbidden, response.Code)
	assert.Contains(t, response.Body.String(), "sg admin required")
	numDBSyncExceptions = rt.GetDatabase().DbStats.Database().SyncFunctionExceptionCount.Value()
	assert.Equal(t, numDBSyncExceptionsStart, numDBSyncExceptions)
	numDBSyncRejected = rt.GetDatabase().DbStats.Security().NumDocsRejected.Value()
	assert.Equal(t, int64(2), numDBSyncRejected)
}

func TestSyncFnTimeout(t *testing.T) {
	base.LongRunningTest(t)

	syncFn := `function(doc) { while(true) {} }`

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Ptr(uint32(1))}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	syncFnFinishedWG := sync.WaitGroup{}
	defer base.WaitWithTimeout(t, &syncFnFinishedWG, time.Second*15)
	syncFnFinishedWG.Go(func() {
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"foo": "bar"}`)
		AssertHTTPErrorReason(t, response, 500, "JS sync function timed out")
	})
}

func TestResyncRegenerateSequences(t *testing.T) {
	ctx := base.TestCtx(t)
	syncFn := `
	function(doc) {
		if (doc.userdoc){
			channel("channel_1")
		}
	}`

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn: syncFn,
		},
	)
	defer rt.Close()

	var response *TestResponse
	var docSeqArr []uint64
	var body db.Body
	var rawDocResponse RawDocResponse

	for i := range 10 {
		docID := fmt.Sprintf("doc%d", i)
		rt.CreateTestDoc(docID)

		response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
		require.Equal(t, http.StatusOK, response.Code)

		err := json.Unmarshal(response.BodyBytes(), &rawDocResponse)
		require.NoError(t, err)

		docSeqArr = append(docSeqArr, rawDocResponse.Xattrs.Sync.Sequence)
	}

	ds := rt.GetSingleDataStore()
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_role/role1", GetRolePayload(t, "role1", ds, []string{"channel_1"}))
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_user/user1", GetUserPayload(t, "user1", "letmein", "", ds, []string{"channel_1"}, []string{"role1"}))
	RequireStatus(t, response, http.StatusCreated)

	_, err := rt.MetadataStore().Get(ctx, rt.GetDatabase().MetadataKeys.RoleKey("role1"), &body)
	assert.NoError(t, err)
	role1SeqBefore := body["sequence"].(float64)

	_, err = rt.MetadataStore().Get(ctx, rt.GetDatabase().MetadataKeys.UserKey("user1"), &body)
	assert.NoError(t, err)
	user1SeqBefore := body["sequence"].(float64)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/userdoc", `{"userdoc": true}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/userdoc2", `{"userdoc": true}`)
	RequireStatus(t, response, http.StatusCreated)

	// Let everything catch up before opening changes feed
	rt.WaitForPendingChanges()

	changesRespContains := func(changesResp ChangesResults, docid string) bool {
		for _, resp := range changesResp.Results {
			if resp.ID == docid {
				return true
			}
		}
		return false
	}

	changesResp := rt.GetChanges("/{{.keyspace}}/_changes", "user1")
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))

	response = rt.SendAdminRequest("GET", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start&regenerate_sequences=true", "")
	RequireStatus(t, response, http.StatusOK)

	resyncStatus := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	_, err = rt.MetadataStore().Get(ctx, rt.GetDatabase().MetadataKeys.RoleKey("role1"), &body)
	assert.NoError(t, err)
	role1SeqAfter := body["sequence"].(float64)

	_, err = rt.MetadataStore().Get(ctx, rt.GetDatabase().MetadataKeys.UserKey("user1"), &body)
	assert.NoError(t, err)
	user1SeqAfter := body["sequence"].(float64)

	assert.True(t, role1SeqAfter > role1SeqBefore)
	assert.True(t, user1SeqAfter > user1SeqBefore)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	for i := range 10 {
		docID := fmt.Sprintf("doc%d", i)

		doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		assert.True(t, doc.Sequence > docSeqArr[i])
	}

	assert.Equal(t, int64(12), resyncStatus.DocsChanged)
	if !base.UnitTestUrlIsWalrus() && !base.TestsDisableGSI() {
		// It is possible for Couchbase Server GSI runs which use DCP purge to two DCP events from a previous
		// test.
		// 1. doc1 mutation
		// 2. doc1 deletion
		//
		// In a test, these will not be resynced but docsProcessed is incremented. Relax
		// the assertion to greater than the number of documents.
		assert.GreaterOrEqual(t, resyncStatus.DocsProcessed, int64(12))
	} else {
		assert.Equal(t, int64(12), resyncStatus.DocsProcessed)
	}

	rt.TakeDbOnline()

	changesResp = rt.GetChanges("/{{.keyspace}}/_changes", "user1")
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))
}

// CBG-2150: Tests that resync status is cluster aware
func TestResyncPersistence(t *testing.T) {
	tb := base.GetTestBucket(t)
	noCloseTB := tb.NoCloseClone()

	rt1 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: noCloseTB,
	})

	rt2 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb,
	})

	defer rt2.Close()
	defer rt1.Close()

	// Create a document to process through resync
	rt1.CreateTestDoc("doc1")

	// Start resync
	rt1.TakeDbOffline()

	resp := rt1.SendAdminRequest("POST", "/{{.db}}/_resync?action=start", "")
	RequireStatus(t, resp, http.StatusOK)

	// Wait for resync to complete
	rt1Status := rt1.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	rt2Status := rt2.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
	require.Equal(t, rt1Status, rt2Status)
}

func TestExpiryUpdateSyncFunction(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	const docID = "doc1"
	version := rt.CreateTestDoc(docID)
	exp, err := rt.GetSingleDataStore().GetExpiry(rt.Context(), docID)
	require.NoError(t, err)
	require.Equal(t, 0, int(exp))

	// have sync function turn on expiry, make sure new revision has an expiry
	RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/sync", `function(olddoc, doc){ expiry(1000); }`), http.StatusOK)
	version = rt.UpdateDoc(docID, version, `{"foo": "bar"}`)
	exp, err = rt.GetSingleDataStore().GetExpiry(rt.Context(), docID)
	require.NoError(t, err)
	require.NotEqual(t, 0, int(exp))

	// have sync function not set expiry, make sure no expiry is on doc
	RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/sync", `function(olddoc, doc){}`), http.StatusOK)
	_ = rt.UpdateDoc(docID, version, `{"foo": "baz"}`)
	exp, err = rt.GetSingleDataStore().GetExpiry(rt.Context(), docID)
	require.NoError(t, err)
	require.Equal(t, 0, int(exp))

}

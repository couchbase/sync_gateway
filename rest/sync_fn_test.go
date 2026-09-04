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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// setUpPromotedLiveBranch builds a conflicted document whose losing branch is live and holds a body
// of its own, externalised to a _sync:rb: doc because it is over db.MaximumInlineBodySize.
// Tombstoning the winner promotes that branch, and the _sync:rb: doc is the only place a body for it
// can come from.
//
//	1-a                         chan-base
//	├── 2-a                     chan-a, live, body externalised to a _sync:rb: doc
//	└── 2-fff... (winner)       chan-b
//
// Each revision is in a channel of its own, so that a recalculation that runs the sync function on
// the wrong revision's body shows up in the document's channels. Returns the three revisions and
// the key of branch a's externalised body.
func setUpPromotedLiveBranch(t *testing.T, rt *RestTester, docID string) (version1a, version2a, version2b DocVersion, bodyKey string) {
	t.Helper()

	const branchB = "2-ffffffffffffffffffffffffffffffff"

	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// Bodies need to be over db.MaximumInlineBodySize so that a non-winning body is held in a
	// _sync:rb: document rather than inline in the rev tree, and so requires a bucket read.
	padding := strings.Repeat("x", db.MaximumInlineBodySize*2)
	docBody := func(branch string) string {
		return fmt.Sprintf(`{"channels":["chan-%s"],"branch":%q,"padding":%q}`, branch, branch, padding)
	}

	version1a = rt.PutDoc(docID, docBody("base"))
	version2a = rt.UpdateDoc(docID, version1a, docBody("a"))
	// branchB beats 2-a in the revid comparison, so branch b becomes the winner and branch a's body is
	// moved out of the document into a _sync:rb: doc.
	version2b = *rt.PutNewEditsFalse(docID, NewDocVersionFromFakeRev(branchB), &version1a, docBody("b"))

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, version2b.RevTreeID, doc.GetRevTreeID(), "branch b should be the winning revision")
	require.Empty(t, doc.History[version2a.RevTreeID].Body, "branch a's body should not be inline in the rev tree")
	bodyKey = doc.History[version2a.RevTreeID].BodyKey
	require.NotEmpty(t, bodyKey, "branch a's body should have been externalised")
	return version1a, version2a, version2b, bodyKey
}

// TestSyncFnLiveBranchPromotedWithUnreadableBody tombstones the winning revision of a conflicted
// document so that a live branch is promoted, while every read of a backup body fails with a
// transient bucket error.
//
// recalculateSyncFnForActiveRev can't distinguish that failure from a body that has genuinely
// expired, because getAvailableRev reports both as a 404. Swallowing it would run the promoted live
// revision through updateChannels(nil), removing the document from every channel it is in and
// dropping its access grants, so the write must fail and be retried instead.
func TestSyncFnLiveBranchPromotedWithUnreadableBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:      true,
		LeakyBucketConfig: &base.LeakyBucketConfig{},
		SyncFn:            `function(doc){ if (!doc._deleted) { channel(doc.channels); } }`,
	})
	defer rt.Close()

	_, version2a, version2b, bodyKey := setUpPromotedLiveBranch(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()

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
	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version2b.RevTreeID, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
	removal, inChannel := doc.Channels["chan-b"]
	assert.True(t, inChannel && removal == nil, "the live document should still be in chan-b, got %+v", doc.Channels)
	_, _, err = rt.GetSingleDataStore().GetRaw(bodyKey)
	assert.NoError(t, err, "branch a's externalised body should not have been deleted")

	// Once the bucket is healthy again the same write succeeds, and branch a keeps its channel.
	rt.DeleteDoc(testDocID, version2b)
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, version2a.RevTreeID, doc.GetRevTreeID(), "branch a should now be the winning revision")
	require.False(t, doc.IsDeleted())
	removal, inChannel = doc.Channels["chan-a"]
	assert.True(t, inChannel && removal == nil, "the promoted live revision should be in chan-a, got %+v", doc.Channels)
}

// TestSyncFnLiveBranchPromotedWithUnreadableExternalisedBody is the other half of
// TestSyncFnLiveBranchPromotedWithUnreadableBody. There every backup read fails, so the failure
// surfaces from the _sync:rev: lookup in getAvailableRev. Here only the _sync:rb: doc holding the
// promoted branch's body is unreadable, which is the read promoteNonWinningRevisionBody makes.
//
// Unlike a _sync:rev: backup, a _sync:rb: doc has no expiry and is referenced by the rev tree, so a
// failed read of one says nothing about whether the body exists. Treating it as an absent body has
// promoteNonWinningRevisionBody put no body in the document and queue the _sync:rb: doc for
// deletion, and committing that write then deletes the only copy of the promoted revision's body,
// leaves the previous winner's body in the bucket under the promoted revision's rev ID, and
// recalculates the document's channels from whichever ancestor backup is still within
// old_rev_expiry_seconds. The write has to fail and be retried instead.
func TestSyncFnLiveBranchPromotedWithUnreadableExternalisedBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:      true,
		LeakyBucketConfig: &base.LeakyBucketConfig{},
		SyncFn:            `function(doc){ if (!doc._deleted) { channel(doc.channels); } }`,
	})
	defer rt.Close()

	_, version2a, version2b, bodyKey := setUpPromotedLiveBranch(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	dataStore := rt.GetSingleDataStore()

	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	channelsBeforeWrite := doc.Channels

	// Fail only the reads of the externalised body - branch a's body is in the bucket and readable in
	// principle, and a transient bucket error is the only thing standing between the write and it.
	leakyDataStore, ok := base.AsLeakyDataStore(dataStore)
	require.True(t, ok)
	leakyDataStore.SetGetRawCallback(func(key string) error {
		if strings.HasPrefix(key, base.RevBodyPrefix) {
			return gocb.ErrTimeout
		}
		return nil
	})

	// Tombstone branch b, which promotes branch a to winning revision.
	response := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, version2b.RevTreeID), "")
	RequireStatus(t, response, http.StatusServiceUnavailable)

	// Cleared before the assertions below, which need to read the bucket themselves.
	leakyDataStore.SetGetRawCallback(nil)

	// The refused write must have left the document, and branch a's body, exactly as they were.
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version2b.RevTreeID, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
	assert.Equal(t, bodyKey, doc.History[version2a.RevTreeID].BodyKey, "branch a should still reference its externalised body")
	assert.Equal(t, channelsBeforeWrite, doc.Channels, "the refused write should not have recalculated the document's channels")
	_, _, err = dataStore.GetRaw(bodyKey)
	assert.NoError(t, err, "branch a's externalised body should not have been deleted")

	// The document's body must still be the one its rev ID names.
	response = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+testDocID, "")
	RequireStatus(t, response, http.StatusOK)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, version2b.RevTreeID, body[db.BodyRev].(string))
	assert.Equal(t, "b", body["branch"].(string))

	// Nothing about the refused write is sticky - once the bucket is healthy the same tombstone
	// promotes branch a, with branch a's own body and channel.
	rt.DeleteDoc(testDocID, version2b)

	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, version2a.RevTreeID, doc.GetRevTreeID(), "branch a should now be the winning revision")
	require.False(t, doc.IsDeleted())
	removal, inChannel := doc.Channels["chan-a"]
	assert.True(t, inChannel && removal == nil, "the promoted live revision should be in chan-a, got %+v", doc.Channels)

	response = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+testDocID, "")
	RequireStatus(t, response, http.StatusOK)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, version2a.RevTreeID, body[db.BodyRev].(string))
	assert.Equal(t, "a", body["branch"].(string), "the promoted revision's own body should have been promoted into the document")
}

// TestSyncFnLiveBranchPromotedWithMissingBody promotes a live branch whose body isn't in the bucket
// at all: its _sync:rb: doc is gone, and so are the transient backups of every ancestor. A read that
// found nothing is the one failure that does tell us the body isn't there, so it can't be retried -
// but a live revision still can't be left with no channels the way a tombstone can, so the write has
// to fail rather than drop the document out of the channels it is in.
func TestSyncFnLiveBranchPromotedWithMissingBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		SyncFn:       `function(doc){ if (!doc._deleted) { channel(doc.channels); access("bob", "grantedChan"); } }`,
	})
	defer rt.Close()

	_, _, version2b, bodyKey := setUpPromotedLiveBranch(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	dataStore := rt.GetSingleDataStore()

	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	_, bobGranted := doc.Access["bob"]
	require.True(t, bobGranted, "the live document should have granted bob access, got %+v", doc.Access)
	channelsBeforeWrite := doc.Channels

	// Model the loss of branch a's externalised body, and the ageing out of every transient backup,
	// leaving no body for branch a anywhere in the bucket.
	require.NoError(t, dataStore.Delete(bodyKey))
	deleteBackupRevisionBodies(t, rt, testDocID)

	// Tombstoning branch b promotes branch a, which has no body to run the sync function on.
	response := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, version2b.RevTreeID), "")
	// getAvailableRev reports the missing body as db.ErrMissing, which is a 404.
	RequireStatus(t, response, http.StatusNotFound)

	// The refused write must have left the document in its channels, with its grants.
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version2b.RevTreeID, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
	assert.False(t, doc.IsDeleted())
	assert.Equal(t, channelsBeforeWrite, doc.Channels, "the refused write should not have recalculated the document's channels")
	_, bobGranted = doc.Access["bob"]
	assert.True(t, bobGranted, "bob's access grant should not have been revoked, got %+v", doc.Access)
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
		if _, _, getErr := dataStore.GetRaw(key); getErr != nil {
			require.True(t, base.IsDocNotFoundError(getErr), "unexpected error reading %s: %v", key, getErr)
			continue
		}
		require.NoError(t, dataStore.SetRaw(key, 0, nil, corruptBody))
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

// setUpPromotedTombstoneWithExternalisedBody builds a document whose tombstoned leaf holds a body of
// its own, externalised to a _sync:rb: doc because it is over db.MaximumInlineBodySize. Tombstoning
// the live winner promotes that leaf, and the _sync:rb: doc is the only place a body for it can come
// from.
//
//	1-a
//	└── 2-a                     live winner
//	4-x
//	└── (T) 5-fff...            tombstone leaf, body held in a _sync:rb: doc
//
// PutNewEditsFalse derives each generation in a history from "start" downwards, so the tombstone's
// parent is a bodiless 4-x rather than 1-a - the shape a push of a revs_limit-truncated history
// leaves behind. That is deliberate here: the branch holds no ancestor bodies for getAvailableRev to
// fall back on, so the leaf's own externalised body is the only source of one.
//
// The document is in chanA and grants bob access. Returns the live winner, the tombstoned leaf, and
// the key of the leaf's externalised body.
func setUpPromotedTombstoneWithExternalisedBody(t *testing.T, rt *RestTester, docID string) (winnerRev, tombstoneRev, bodyKey string) {
	t.Helper()

	tombstoneRev = "5-ffffffffffffffffffffffffffffffff"

	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// Bodies need to be over db.MaximumInlineBodySize so that the tombstone leaf's body is held in a
	// _sync:rb: doc rather than inline in the rev tree, and so requires a bucket read.
	padding := strings.Repeat("x", db.MaximumInlineBodySize*2)
	version1a := rt.PutDoc(docID, fmt.Sprintf(`{"channels":["chanA"],"gen":1,"padding":%q}`, padding))
	rt.PutNewEditsFalse(docID, NewDocVersionFromFakeRev(tombstoneRev), &version1a,
		fmt.Sprintf(`{"_deleted":true,"channels":["chanA"],"branch":"tombstone","padding":%q}`, padding))
	// a live revision always beats a tombstone, so 2-a is the winner and the tombstone leaf is not
	winnerRev = rt.UpdateDoc(docID, version1a, fmt.Sprintf(`{"channels":["chanA"],"gen":2,"padding":%q}`, padding)).RevTreeID

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, winnerRev, doc.GetRevTreeID(), "the live revision should be the winning revision")
	require.True(t, doc.History[tombstoneRev].Deleted)
	require.Empty(t, doc.History[tombstoneRev].Body, "the tombstone's body should not be inline in the rev tree")
	bodyKey = doc.History[tombstoneRev].BodyKey
	require.NotEmpty(t, bodyKey, "the tombstone's body should have been externalised")

	parentRev := doc.History[tombstoneRev].Parent
	require.NotEmpty(t, parentRev)
	require.Empty(t, doc.History[parentRev].Body, "the tombstone's parent should hold no body")
	require.Empty(t, doc.History[parentRev].BodyKey, "the tombstone's parent should hold no body")

	// Model the live branch's transient backups ageing out, leaving the tombstone's externalised body
	// as the only body in the bucket besides the winner's own.
	deleteBackupRevisionBodies(t, rt, docID)
	_, _, err = rt.GetSingleDataStore().GetRaw(bodyKey)
	require.NoError(t, err, "the externalised body should still be in the bucket")
	return winnerRev, tombstoneRev, bodyKey
}

// TestSyncFnPromotedTombstoneWithUnreadableExternalisedBody is the _sync:rb: twin of
// TestSyncFnPromotedTombstoneWithUnreadableBackupBody: the promoted tombstone has a body of its own
// rather than relying on an ancestor's backup, and the only reason the read of it fails is a
// transient bucket error.
//
// A tombstone with no body left anywhere is allowed to end up in no channels, so a promoted
// tombstone is the case where a failed read is most likely to be waved through as an absent body.
// It must not be: the read says nothing about whether the body exists, and by the time
// recalculateSyncFnForActiveRev sees a 404 promoteNonWinningRevisionBody has already queued the
// _sync:rb: doc for deletion, so committing the write revokes the document's grants and destroys the
// body that would have prevented that.
func TestSyncFnPromotedTombstoneWithUnreadableExternalisedBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:      true,
		LeakyBucketConfig: &base.LeakyBucketConfig{},
		SyncFn:            `function(doc){ channel(doc.channels); access("bob", "grantedChan"); }`,
	})
	defer rt.Close()

	winnerRev, tombstoneRev, bodyKey := setUpPromotedTombstoneWithExternalisedBody(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	dataStore := rt.GetSingleDataStore()

	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	_, bobGranted := doc.Access["bob"]
	require.True(t, bobGranted, "the live document should have granted bob access, got %+v", doc.Access)
	channelsBeforeWrite := doc.Channels

	// Fail only the reads of the externalised body - it is in the bucket and readable in principle.
	leakyDataStore, ok := base.AsLeakyDataStore(dataStore)
	require.True(t, ok)
	leakyDataStore.SetGetRawCallback(func(key string) error {
		if strings.HasPrefix(key, base.RevBodyPrefix) {
			return gocb.ErrTimeout
		}
		return nil
	})

	// Tombstoning the winner promotes the tombstone leaf, whose body only the _sync:rb: doc can supply.
	response := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, winnerRev), "")
	RequireStatus(t, response, http.StatusServiceUnavailable)

	// Cleared before the assertions below, which need to read the bucket themselves.
	leakyDataStore.SetGetRawCallback(nil)

	// The refused write must have left the document, and the tombstone's body, exactly as they were.
	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, winnerRev, doc.GetRevTreeID(), "the refused write should not have changed the winning revision")
	assert.False(t, doc.IsDeleted())
	assert.Equal(t, bodyKey, doc.History[tombstoneRev].BodyKey, "the tombstone should still reference its externalised body")
	assert.Equal(t, channelsBeforeWrite, doc.Channels, "the refused write should not have recalculated the document's channels")
	_, bobGranted = doc.Access["bob"]
	assert.True(t, bobGranted, "bob's access grant should not have been revoked, got %+v", doc.Access)
	_, _, err = dataStore.GetRaw(bodyKey)
	assert.NoError(t, err, "the externalised body must not be deleted on a failed read")

	// Once the bucket is healthy the same write succeeds: the sync function runs for the promoted
	// tombstone using its own body, so the document keeps chanA and bob keeps his grant, and the
	// now-redundant _sync:rb: doc is cleaned up.
	rt.DeleteDoc(testDocID, NewDocVersionFromFakeRev(winnerRev))

	doc, err = collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, tombstoneRev, doc.GetRevTreeID(), "the tombstone branch should have been promoted")
	require.True(t, doc.IsDeleted())
	removal, inChannel := doc.Channels["chanA"]
	assert.True(t, inChannel && removal == nil, "the promoted tombstone should still be in chanA, got %+v", doc.Channels)
	_, bobGranted = doc.Access["bob"]
	assert.True(t, bobGranted, "bob's access grant should have been recalculated from the tombstone's own body, got %+v", doc.Access)
	_, _, err = dataStore.GetRaw(bodyKey)
	assert.True(t, base.IsDocNotFoundError(err),
		"the externalised body should have been cleaned up once promoted into the document, got %v", err)
}

// TestSyncFnPromotedTombstoneWithMissingExternalisedBody is the other side of the split that
// TestSyncFnPromotedTombstoneWithUnreadableExternalisedBody pins. A read that found nothing does
// tell us the body isn't there, and no retry can bring it back, so - as with an ancestor's expired
// backup in TestSyncFnPromotedTombstoneWithExpiredBackupBody - the write is allowed to proceed and
// leave the promoted tombstone in no channels rather than failing every write to the document from
// here on.
func TestSyncFnPromotedTombstoneWithMissingExternalisedBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	const testDocID = "testdoc"

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		SyncFn:       `function(doc){ channel(doc.channels); access("bob", "grantedChan"); }`,
	})
	defer rt.Close()

	winnerRev, tombstoneRev, bodyKey := setUpPromotedTombstoneWithExternalisedBody(t, rt, testDocID)
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	doc, err := collection.GetDocument(ctx, testDocID, db.DocUnmarshalAll)
	require.NoError(t, err)
	_, bobGranted := doc.Access["bob"]
	require.True(t, bobGranted, "the live document should have granted bob access, got %+v", doc.Access)

	// The rev tree still points at the externalised body, but it is no longer in the bucket.
	require.NoError(t, rt.GetSingleDataStore().Delete(bodyKey))

	// Tombstoning the winner promotes the tombstone leaf, which now has no body anywhere.
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
	syncFn := `function(doc) { while(true) {} }`

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Ptr(uint32(1))}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	syncFnFinishedWG := sync.WaitGroup{}
	syncFnFinishedWG.Add(1)
	go func() {
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"foo": "bar"}`)
		AssertHTTPErrorReason(t, response, 500, "JS sync function timed out")
		syncFnFinishedWG.Done()
	}()
	timeoutErr := WaitWithTimeout(&syncFnFinishedWG, time.Second*15)
	assert.NoError(t, timeoutErr)
}

func TestResyncErrorScenariosUsingDCPStream(t *testing.T) {
	base.TestRequiresDCPResync(t)

	syncFn := `
	function(doc) {
		channel("x")
	}`

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn: syncFn,
		},
	)
	defer rt.Close()

	for i := 0; i < 1000; i++ {
		rt.CreateTestDoc(fmt.Sprintf("doc%d", i))
	}

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusBadRequest)

	rt.TakeDbOffline()

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
}

func TestResyncStopUsingDCPStream(t *testing.T) {
	base.TestRequiresDCPResync(t)

	syncFn := `
	function(doc) {
		channel("x")
	}`

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn: syncFn,
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				QueryPaginationLimit: base.Ptr(10),
			}},
		},
	)
	defer rt.Close()

	for i := 0; i < 1000; i++ {
		rt.CreateTestDoc(fmt.Sprintf("doc%d", i))
	}

	err := rt.WaitForCondition(func() bool {
		return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == 1000
	})
	assert.NoError(t, err)

	rt.TakeDbOffline()

	response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)
	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusOK)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateStopped)

	syncFnCount := int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.Less(t, syncFnCount, 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

func TestResyncRegenerateSequences(t *testing.T) {
	base.TestRequiresDCPResync(t)
	base.LongRunningTest(t)
	syncFn := `
	function(doc) {
		if (doc.userdoc){
			channel("channel_1")
		}
	}`

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

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

	for i := 0; i < 10; i++ {
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

	_, err := rt.MetadataStore().Get(rt.GetDatabase().MetadataKeys.RoleKey("role1"), &body)
	assert.NoError(t, err)
	role1SeqBefore := body["sequence"].(float64)

	_, err = rt.MetadataStore().Get(rt.GetDatabase().MetadataKeys.UserKey("user1"), &body)
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

	_, err = rt.MetadataStore().Get(rt.GetDatabase().MetadataKeys.RoleKey("role1"), &body)
	assert.NoError(t, err)
	role1SeqAfter := body["sequence"].(float64)

	_, err = rt.MetadataStore().Get(rt.GetDatabase().MetadataKeys.UserKey("user1"), &body)
	assert.NoError(t, err)
	user1SeqAfter := body["sequence"].(float64)

	assert.True(t, role1SeqAfter > role1SeqBefore)
	assert.True(t, user1SeqAfter > user1SeqBefore)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	for i := 0; i < 10; i++ {
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
	base.TestRequiresDCPResync(t)

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

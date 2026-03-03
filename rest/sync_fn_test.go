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
	"sync"
	"testing"
	"time"

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
	syncFnFinishedWG.Add(1)
	go func() {
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"foo": "bar"}`)
		AssertHTTPErrorReason(t, response, 500, "JS sync function timed out")
		syncFnFinishedWG.Done()
	}()
	WaitWithTimeout(t, &syncFnFinishedWG, time.Second*15)
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

	for i := range 1000 {
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

	for i := range 1000 {
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
	base.LongRunningTest(t)

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

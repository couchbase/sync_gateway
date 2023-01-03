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
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
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

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Uint32Ptr(0)}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+testDocID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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

	// rev 1-a
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+testDocID, `{"`+testdataKey+`":1}`)
	RequireStatus(t, resp, 201)
	rev1ID := RespRevID(t, resp)

	// rev 2-a
	resp = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, rev1ID), `{"`+testdataKey+`":2}`)
	RequireStatus(t, resp, 201)
	rev2aID := RespRevID(t, resp)

	// rev 3-a
	resp = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, rev2aID), `{"`+testdataKey+`":3,"syncOldDocBodyCheck":true}`)
	RequireStatus(t, resp, 201)
	rev3aID := RespRevID(t, resp)

	// rev 2-b
	_, rev1Hash := db.ParseRevID(rev1ID)
	resp = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?new_edits=false", testDocID), `{"`+db.BodyRevisions+`":{"start":2,"ids":["b", "`+rev1Hash+`"]}}`)
	RequireStatus(t, resp, 201)
	rev2bID := RespRevID(t, resp)

	// tombstone at 4-a
	resp = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, rev3aID), `{}`)
	RequireStatus(t, resp, 200)

	numErrorsBefore, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)
	// tombstone at 3-b
	resp = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", testDocID, rev2bID), `{}`)
	RequireStatus(t, resp, 200)

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
	assert.NoError(t, rt.WaitForDBOnline())

	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo": "bar"}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, numErrors+1, numErrorsAfter)
}

func TestSyncFnTimeout(t *testing.T) {
	syncFn := `function(doc) { while(true) {} }`

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Uint32Ptr(1)}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	syncFnFinishedWG := sync.WaitGroup{}
	syncFnFinishedWG.Add(1)
	go func() {
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"foo": "bar"}`)
		assertHTTPErrorReason(t, response, 500, "JS sync function timed out")
		syncFnFinishedWG.Done()
	}()
	timeoutErr := WaitWithTimeout(&syncFnFinishedWG, time.Second*15)
	assert.NoError(t, timeoutErr)
}

// Take DB offline and ensure can post _resync
func TestDBOfflinePostResync(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	_, isDCPResync := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if isDCPResync && base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't work with Walrus when ResyncManagerDCP is used")
	}

	if isDCPResync {
		base.TemporarilyDisableTestUsingDCPWithCollections(t)
	}

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	RequireStatus(t, rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", ""), 200)
	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.MetadataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)
}

// Take DB offline and ensure only one _resync can be in progress
func TestDBOfflineSingleResync(t *testing.T) {

	syncFn := `
	function(doc) {
		channel("x")
	}`
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: syncFn})
	defer rt.Close()

	_, isDCPResync := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if isDCPResync && base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't work with Walrus when ResyncManagerDCP is used")
	}
	if isDCPResync {
		base.TemporarilyDisableTestUsingDCPWithCollections(t)
	}

	// create documents in DB to cause resync to take a few seconds
	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(1000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	// Send a second _resync request.  This must return a 400 since the first one is blocked processing
	RequireStatus(t, rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", ""), 503)

	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.MetadataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(2000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
}

func TestResync(t *testing.T) {
	base.LongRunningTest(t)

	testCases := []struct {
		name               string
		docsCreated        int
		expectedSyncFnRuns int
		expectedQueryCount int
		queryLimit         int
	}{
		{
			name:               "Docs 0, Limit Default",
			docsCreated:        0,
			expectedSyncFnRuns: 0,
			expectedQueryCount: 1,
			queryLimit:         db.DefaultQueryPaginationLimit,
		},
		{
			name:               "Docs 1000, Limit Default",
			docsCreated:        1000,
			expectedSyncFnRuns: 2000,
			expectedQueryCount: 1,
			queryLimit:         db.DefaultQueryPaginationLimit,
		},
		{
			name:               "Docs 1000, Limit 10",
			docsCreated:        1000,
			expectedSyncFnRuns: 2000,
			expectedQueryCount: 101,
			queryLimit:         10,
		},
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rt := NewRestTester(t,
				&RestTesterConfig{
					DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
						QueryPaginationLimit: &testCase.queryLimit,
					}},
					SyncFn: syncFn,
				},
			)
			defer rt.Close()

			_, isDCPResync := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
			if isDCPResync && base.UnitTestUrlIsWalrus() {
				t.Skip("This test doesn't work with Walrus when ResyncManagerDCP is used")
			}
			if isDCPResync {
				base.TemporarilyDisableTestUsingDCPWithCollections(t)
			}

			for i := 0; i < testCase.docsCreated; i++ {
				rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
			}

			response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
			RequireStatus(t, response, http.StatusServiceUnavailable)

			response = rt.SendAdminRequest("POST", "/db/_offline", "")
			RequireStatus(t, response, http.StatusOK)

			WaitAndAssertCondition(t, func() bool {
				state := atomic.LoadUint32(&rt.GetDatabase().State)
				return state == db.DBOffline
			})

			response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
			RequireStatus(t, response, http.StatusOK)

			var resyncManagerStatus db.ResyncManagerResponse
			err := rt.WaitForCondition(func() bool {
				response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_resync", "")
				err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
				assert.NoError(t, err)

				var val interface{}
				_, err = rt.MetadataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

				if resyncManagerStatus.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err) {
					return true
				} else {
					t.Logf("resyncManagerStatus.State != %v: %v - err:%v", db.BackgroundProcessStateCompleted, resyncManagerStatus.State, err)
					return false
				}
			})
			assert.NoError(t, err)

			assert.Equal(t, testCase.expectedSyncFnRuns, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

			if !isDCPResync {
				var queryName string
				if base.TestsDisableGSI() {
					queryName = fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncGateway(), db.ViewChannels)
				} else {
					queryName = db.QueryTypeChannels
				}
				assert.Equal(t, testCase.expectedQueryCount, int(rt.GetDatabase().DbStats.Query(queryName).QueryCount.Value()))
			}

			assert.Equal(t, testCase.docsCreated, resyncManagerStatus.DocsProcessed)
			assert.Equal(t, 0, resyncManagerStatus.DocsChanged)
		})
	}
}

func TestResyncErrorScenarios(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		// Limitation of setting LeakyBucket on RestTester
		t.Skip("This test only works with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	leakyTestBucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{})

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		t.Skip("This test only works when ResyncManager is used")
	}

	leakyDataStore, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore())
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyDataStore.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
				RequireStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	} else {
		leakyDataStore.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
				RequireStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
	RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=invalid", "")
	RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.True(t, callbackFired, "expecting callback to be fired")
}

func TestResyncErrorScenariosUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't work with walrus")
	}

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

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		t.Skip("This test only works when ResyncManagerDCP is used")
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)
}

func TestResyncStop(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		// Limitation of setting LeakyBucket on RestTester
		t.Skip("This test only works with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	leakyTestBucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{})

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn: syncFn,
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(10),
			}},
			CustomTestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		t.Skip("This test only works when ResyncManager is used")
	}

	leakyDataStore, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore())
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyDataStore.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=stop", "")
				RequireStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	} else {
		leakyDataStore.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=stop", "")
				RequireStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	err := rt.WaitForCondition(func() bool {
		return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == 1000
	})
	assert.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateStopped,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.True(t, callbackFired, "expecting callback to be fired")

	syncFnCount := int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.True(t, syncFnCount < 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

func TestResyncStopUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't work with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn: syncFn,
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(10),
			}},
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		t.Skip("This test only works when ResyncManagerDCP is used")
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	err := rt.WaitForCondition(func() bool {
		return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == 1000
	})
	assert.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateRunning,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})

	time.Sleep(500 * time.Microsecond)
	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateStopped,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	syncFnCount := int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.Less(t, syncFnCount, 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

func TestResyncRegenerateSequences(t *testing.T) {

	// FIXME: PersistentWalrusBucket doesn't support collections yet
	t.Skip("PersistentWalrusBucket doesn't support collections yet")

	base.LongRunningTest(t)
	syncFn := `
	function(doc) {
		if (doc.userdoc){
			channel("channel_1")
		}
	}`

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	var testBucket *base.TestBucket

	if base.UnitTestUrlIsWalrus() {
		var closeFn func()
		testBucket, closeFn = base.GetPersistentWalrusBucket(t)
		defer closeFn()
	} else {
		testBucket = base.GetTestBucket(t)
	}

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: testBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if ok && base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't works with Walrus when ResyncManagerDCP is used")
	}

	var response *TestResponse
	var docSeqArr []float64
	var body db.Body

	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("doc%d", i)
		rt.CreateDoc(t, docID)

		response = rt.SendAdminRequest("GET", "/db/_raw/"+docID, "")
		require.Equal(t, http.StatusOK, response.Code)

		err := json.Unmarshal(response.BodyBytes(), &body)
		require.NoError(t, err)

		docSeqArr = append(docSeqArr, body["_sync"].(map[string]interface{})["sequence"].(float64))
	}

	role := "role1"
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_role/%s", role), fmt.Sprintf(`{"name":"%s", "admin_channels":["channel_1"]}`, role))
	RequireStatus(t, response, http.StatusCreated)

	username := "user1"
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"], "admin_roles": ["%s"]}`, username, role))
	RequireStatus(t, response, http.StatusCreated)

	_, err := rt.MetadataStore().Get(base.RolePrefix+"role1", &body)
	assert.NoError(t, err)
	role1SeqBefore := body["sequence"].(float64)

	_, err = rt.MetadataStore().Get(base.UserPrefix+"user1", &body)
	assert.NoError(t, err)
	user1SeqBefore := body["sequence"].(float64)

	response = rt.SendAdminRequest("PUT", "/db/userdoc", `{"userdoc": true}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/userdoc2", `{"userdoc": true}`)
	RequireStatus(t, response, http.StatusCreated)

	// Let everything catch up before opening changes feed
	require.NoError(t, rt.WaitForPendingChanges())

	type ChangesResp struct {
		Results []struct {
			ID  string `json:"id"`
			Seq int    `json:"seq"`
		} `json:"results"`
		LastSeq string `json:"last_seq"`
	}

	changesRespContains := func(changesResp ChangesResp, docid string) bool {
		for _, resp := range changesResp.Results {
			if resp.ID == docid {
				return true
			}
		}
		return false
	}

	var changesResp ChangesResp
	request, _ := http.NewRequest("GET", "/db/_changes", nil)
	request.SetBasicAuth("user1", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &changesResp)
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))

	response = rt.SendAdminRequest("GET", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start&regenerate_sequences=true", "")
	RequireStatus(t, response, http.StatusOK)

	WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	_, err = rt.MetadataStore().Get(base.RolePrefix+"role1", &body)
	assert.NoError(t, err)
	role1SeqAfter := body["sequence"].(float64)

	_, err = rt.MetadataStore().Get(base.UserPrefix+"user1", &body)
	assert.NoError(t, err)
	user1SeqAfter := body["sequence"].(float64)

	assert.True(t, role1SeqAfter > role1SeqBefore)
	assert.True(t, user1SeqAfter > user1SeqBefore)

	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("doc%d", i)

		doc, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		assert.True(t, float64(doc.Sequence) > docSeqArr[i])
	}

	response = rt.SendAdminRequest("GET", "/db/_resync", "")
	RequireStatus(t, response, http.StatusOK)
	var resyncStatus db.ResyncManagerResponse
	err = base.JSONUnmarshal(response.BodyBytes(), &resyncStatus)
	assert.NoError(t, err)
	assert.Equal(t, 12, resyncStatus.DocsChanged)
	assert.Equal(t, 12, resyncStatus.DocsProcessed)

	response = rt.SendAdminRequest("POST", "/db/_online", "")
	RequireStatus(t, response, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOnline
	})
	assert.NoError(t, err)

	// Data is wiped from walrus when brought back online
	request, err = http.NewRequest("GET", "/db/_changes?since="+changesResp.LastSeq, nil)
	require.NoError(t, err)
	request.SetBasicAuth("user1", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &changesResp)
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))
}

// CBG-2150: Tests that resync status is cluster aware
func TestResyncPersistence(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.TemporarilyDisableTestUsingDCPWithCollections(t)

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
	rt1.CreateDoc(t, "doc1")

	// Start resync
	resp := rt1.SendAdminRequest("POST", "/db/_offline", "")
	RequireStatus(t, resp, http.StatusOK)

	WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt1.GetDatabase().State)
		return state == db.DBOffline
	})

	resp = rt1.SendAdminRequest("POST", "/{{.keyspace}}/_resync?action=start", "")
	RequireStatus(t, resp, http.StatusOK)

	// Wait for resync to complete
	var resyncManagerStatus db.ResyncManagerResponse
	err := rt1.WaitForCondition(func() bool {
		resp = rt1.SendAdminRequest("GET", "/{{.keyspace}}/_resync", "")
		err := json.Unmarshal(resp.BodyBytes(), &resyncManagerStatus)
		assert.NoError(t, err)

		if resyncManagerStatus.State == db.BackgroundProcessStateCompleted {
			return true
		} else {
			t.Logf("resyncManagerStatus.State != %v: %v", db.BackgroundProcessStateCompleted, resyncManagerStatus.State)
			return false
		}
	})
	require.NoError(t, err)

	// Check statuses match
	resp2 := rt2.SendAdminRequest("GET", "/{{.keyspace}}/_resync", "")
	RequireStatus(t, resp, http.StatusOK)
	fmt.Printf("RT1 Resync Status: %s\n", resp.BodyBytes())
	fmt.Printf("RT2 Resync Status: %s\n", resp2.BodyBytes())
	assert.Equal(t, resp.BodyBytes(), resp2.BodyBytes())
}

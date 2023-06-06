//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package adminapitest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

// Reproduces CBG-1412 - JSON strings in some responses not being correctly escaped
func TestPutDocSpecialChar(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	testCases := []struct {
		name         string
		pathDocID    string
		method       string
		body         string
		expectedResp int
		eeOnly       bool
	}{
		{
			name:         "Double quote PUT",
			pathDocID:    `doc"55"`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated,
			eeOnly:       false,
		},
		{
			name:         "Double quote PUT for replicator2",
			pathDocID:    `doc"77"?replicator2=true`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated,
			eeOnly:       true,
		},
		{
			name:         "Local double quote PUT",
			pathDocID:    `_local/doc"57"`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated,
			eeOnly:       false,
		},
		{
			name:         "Double quote PUT with attachment",
			pathDocID:    `doc"59"/attachMe`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated, // Admin Docs expected response http.StatusOK
			eeOnly:       false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.eeOnly && !base.IsEnterpriseEdition() {
				t.Skipf("Skipping enterprise-only test")
			}
			tr := rt.SendAdminRequest(testCase.method, "/{{.keyspace}}/"+testCase.pathDocID, testCase.body)
			rest.RequireStatus(t, tr, testCase.expectedResp)
			var body map[string]interface{}
			err := json.Unmarshal(tr.BodyBytes(), &body)
			assert.NoError(t, err)
		})
	}

	t.Run("Delete Double quote Doc ID", func(t *testing.T) { // Should be done for Local Document deletion when it returns response
		tr := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+`del"ete"Me`, "{}") // Create the doc to delete
		rest.RequireStatus(t, tr, http.StatusCreated)
		var putBody struct {
			Rev string `json:"rev"`
		}
		err := json.Unmarshal(tr.BodyBytes(), &putBody)
		assert.NoError(t, err)

		tr = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", `del"ete"Me`, putBody.Rev), "{}")
		rest.RequireStatus(t, tr, http.StatusOK)
		var body map[string]interface{}
		err = json.Unmarshal(tr.BodyBytes(), &body)
		assert.NoError(t, err)
	})
}

// Reproduces #3048 Panic when attempting to make invalid update to a conflicting document
func TestNoPanicInvalidUpdate(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	docId := "conflictTest"

	// Create doc
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docId, `{"value":"initial"}`)
	response.DumpBody()

	rest.RequireStatus(t, response, http.StatusCreated)

	// Discover revision ID
	// TODO: The schema for SG responses should be defined in our code somewhere to avoid this clunky approach
	var responseDoc map[string]interface{}
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId := responseDoc["rev"].(string)
	revGeneration, revIdHash := db.ParseRevID(revId)
	assert.Equal(t, 1, revGeneration)

	// Update doc (normal update, no conflicting revisions added)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docId, fmt.Sprintf(`{"value":"secondval", db.BodyRev:"%s"}`, revId))
	response.DumpBody()

	// Create conflict
	input := fmt.Sprintf(`
                  {"value": "conflictval",
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "%s"]}}`, revIdHash)

	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(revId)
	assert.Equal(t, 2, revGeneration)

	// Create conflict again, should be a no-op and return the same response as previous attempt
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(revId)
	assert.Equal(t, 2, revGeneration)

}

func TestLoggingKeys(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Assert default log channels are enabled
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]interface{}{}, logKeys)

	// Set logKeys, Changes+ should enable Changes (PUT replaces any existing log keys)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{"Changes+":true, "Cache":true, "HTTP":true}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var updatedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &updatedLogKeys))
	assert.Equal(t, map[string]interface{}{"Changes": true, "Cache": true, "HTTP": true}, updatedLogKeys)

	// Disable Changes logKey which should also disable Changes+
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var deletedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &deletedLogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, deletedLogKeys)

	// Enable Changes++, which should enable Changes (POST append logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var appendedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &appendedLogKeys))
	assert.Equal(t, map[string]interface{}{"Changes": true, "Cache": true, "HTTP": true}, appendedLogKeys)

	// Disable Changes++ (POST modifies logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabledLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabledLogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, disabledLogKeys)

	// Re-Enable Changes++, which should enable Changes (POST append logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	// Disable Changes+ which should disable Changes (POST modifies logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes+":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabled2LogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabled2LogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, disabled2LogKeys)

	// Re-Enable Changes++, which should enable Changes (POST append logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	// Disable Changes (POST modifies logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabled3LogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabled3LogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, disabled3LogKeys)

	// Disable all logKeys by using PUT with an empty channel list
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var noLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &noLogKeys))
	assert.Equal(t, map[string]interface{}{}, noLogKeys)
}

func TestServerlessChangesEndpointLimit(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg, base.KeyChanges)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channel);}`,
	})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPut, "/_config", `{"max_concurrent_replications" : 2}`)
	rest.RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendAdminRequest("PUT", "/db/_user/alice", rest.GetUserPayload(t, "alice", "letmein", "", rt.GetSingleTestDatabaseCollection(), []string{"ABC"}, nil))
	rest.RequireStatus(t, resp, 201)

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3", `{"value":3, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	changesJSON := `{"style":"all_docs", 
					 "heartbeat":300000, 
					 "feed":"longpoll", 
					 "limit":50, 
					 "since":"1",
					 "filter":"` + base.ByChannelFilter + `",
					 "channels":"ABC,PBS"}`
	var wg sync.WaitGroup
	wg.Add(2)

	// send some changes requests in go routines to run concurrently along with test
	go func() {
		defer wg.Done()
		resp1 := rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", changesJSON, "alice")
		rest.RequireStatus(t, resp1, http.StatusOK)
	}()

	go func() {
		defer wg.Done()
		resp2 := rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", changesJSON, "alice")
		rest.RequireStatus(t, resp2, http.StatusOK)
	}()

	// assert count for replicators is correct according to changes request made above
	rt.WaitForActiveReplicatorCount(2)

	// assert this request is rejected due to this request taking us over the limit
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_changes?feed=longpoll&since=999999&timeout=100000", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)
	// put doc to end changes feeds
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc1", `{"value":3, "channel":["ABC"]}`)
	rest.RequireStatus(t, resp, 201)
	wg.Wait()
}

func TestLoggingLevels(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Log keys should be blank
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]bool
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]bool{}, logKeys)

	// Set log level via logLevel query parameter
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=error", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=invalidLogLevel", ``), http.StatusBadRequest)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=", ``), http.StatusBadRequest)

	// Set log level via old level query parameter
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=1", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=2", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=3", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=10", ``), http.StatusOK) // Value is clamped to acceptable range, without returning an error

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=0", ``), http.StatusBadRequest) // Zero-value is ignored and body is to be parsed
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=0", `{}`), http.StatusOK)       // Zero-value is ignored and body is to be parsed

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=invalidLogLevel", ``), http.StatusBadRequest)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=", ``), http.StatusBadRequest)

	// Trying to set log level via the body will not work (the endpoint expects a log key map)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{"logLevel": "debug"}`), http.StatusBadRequest)
}

func TestLoggingCombined(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Log keys should be blank
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]bool
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]bool{}, logKeys)

	// Set log keys and log level in a single request
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=trace", `{"Changes":true, "Cache":true, "HTTP":true}`), http.StatusOK)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]bool{"Changes": true, "Cache": true, "HTTP": true}, logKeys)
}

func TestGetStatus(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendRequest("GET", "/_status", "")
	rest.RequireStatus(t, response, 404)

	response = rt.SendAdminRequest("GET", "/_status", "")
	rest.RequireStatus(t, response, 200)
	var responseBody rest.Status
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)

	assert.Equal(t, base.LongVersionString, responseBody.Version)

	response = rt.SendAdminRequest("OPTIONS", "/_status", "")
	rest.RequireStatus(t, response, 204)
	assert.Equal(t, "GET", response.Header().Get("Allow"))
}

func TestFlush(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("sgbucket.DeleteableBucket inteface only supported by Walrus")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateDoc(t, "doc1")
	rt.CreateDoc(t, "doc2")
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/{{.keyspace}}/doc2", ""), 200)

	log.Printf("Flushing db...")
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_flush", ""), 200)
	require.NoError(t, rt.SetAdminParty(true)) // needs to be re-enabled after flush since guest user got wiped

	// After the flush, the db exists but the documents are gone:
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/", ""), 200)

	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", ""), 404)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/{{.keyspace}}/doc2", ""), 404)
}

// Test a single call to take DB offline
func TestDBOfflineSingle(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")
}

// Make two concurrent calls to take DB offline
// Ensure both calls succeed and that DB is offline
// when both calls return
func TestDBOfflineConcurrent(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	// Take DB offline concurrently using two goroutines
	// Both should return success and DB should be offline
	// once both goroutines return
	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *rest.TestResponse
	go func() {
		goroutineresponse1 = rt.SendAdminRequest("POST", "/db/_offline", "")
		wg.Done()
	}()

	var goroutineresponse2 *rest.TestResponse
	go func() {
		goroutineresponse2 = rt.SendAdminRequest("POST", "/db/_offline", "")
		wg.Done()
	}()

	err := rest.WaitWithTimeout(&wg, time.Second*30)
	assert.NoError(t, err, "Error waiting for waitgroup")
	rest.RequireStatus(t, goroutineresponse1, http.StatusOK)
	rest.RequireStatus(t, goroutineresponse2, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

}

// Test that a DB can be created offline
func TestStartDBOffline(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")
}

// Take DB offline and ensure that normal REST calls
// fail with status 503
func TestDBOffline503Response(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", "{}")
	rest.RequireStatus(t, response, http.StatusCreated)
	log.Printf("Taking DB offline")
	response = rt.SendAdminRequest("GET", "/{{.db}}/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/{{.db}}/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/{{.db}}/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendRequest("GET", "/{{.keyspace}}/doc1", ""), 503)
}

// Take DB offline and ensure can put db config
func TestDBOfflinePutDbConfig(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendRequest("PUT", "/db/_config", ""), 404)
}

// Tests that the users returned in the config endpoint have the correct names
// Reproduces #2223
func TestDBGetConfigNames(t *testing.T) {

	p := "password"
	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					"alice": &auth.PrincipalConfig{Password: &p},
					"bob":   &auth.PrincipalConfig{Password: &p},
				},
			},
			},
		},
	)

	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/_config?include_runtime=true", "")
	var body rest.DbConfig
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	assert.Equal(t, len(rt.DatabaseConfig.Users), len(body.Users))

	for k, v := range body.Users {
		assert.Equal(t, k, *v.Name)
	}

}

// Take DB offline and ensure can post _resync
func TestDBOfflinePostResync(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		t.Skip("This test only works when ResyncManager is used")
	}

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 200)
	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)
}

func TestDBOfflinePostResyncUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires gocb buckets")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		t.Skip("This test only works when ResyncManagerDCP is used")
	}

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 200)
	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

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
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: syncFn})
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		t.Skip("This test only works when ResyncManager is used")
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
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Send a second _resync request.  This must return a 400 since the first one is blocked processing
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 503)

	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(2000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
}

func TestDBOfflineSingleResyncUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't works with walrus")
	}
	syncFn := `
	function(doc) {
		channel("x")
	}`
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: syncFn})
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		t.Skip("This test only works when ResyncManagerDCP is used")
	}

	// create documents in DB to cause resync to take a few seconds
	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(1000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Send a second _resync request.  This must return a 400 since the first one is blocked processing
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 503)

	err := rt.WaitForConditionWithOptions(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	}, 200, 200)
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
			rt := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						QueryPaginationLimit: &testCase.queryLimit,
					}},
					SyncFn: syncFn,
				},
			)
			defer rt.Close()

			_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
			if !ok {
				rt.GetDatabase().ResyncManager = db.NewResyncManager(rt.GetSingleDataStore(), rt.GetDatabase().MetadataKeys)
			}

			for i := 0; i < testCase.docsCreated; i++ {
				rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
			}
			err := rt.WaitForCondition(func() bool {
				return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == testCase.docsCreated
			})
			assert.NoError(t, err)

			response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusServiceUnavailable)

			response = rt.SendAdminRequest("POST", "/db/_offline", "")
			rest.RequireStatus(t, response, http.StatusOK)

			rest.WaitAndAssertCondition(t, func() bool {
				state := atomic.LoadUint32(&rt.GetDatabase().State)
				return state == db.DBOffline
			})

			response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusOK)

			var resyncManagerStatus db.ResyncManagerResponseDCP
			err = rt.WaitForCondition(func() bool {
				response := rt.SendAdminRequest("GET", "/db/_resync", "")
				err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
				assert.NoError(t, err)

				var val interface{}
				_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

				if resyncManagerStatus.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err) {
					return true
				} else {
					t.Logf("resyncManagerStatus.State != %v: %v - err:%v", db.BackgroundProcessStateCompleted, resyncManagerStatus.State, err)
					return false
				}
			})
			assert.NoError(t, err)

			assert.Equal(t, testCase.expectedSyncFnRuns, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

			assert.Equal(t, testCase.docsCreated, int(resyncManagerStatus.DocsProcessed))
			assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
		})
	}

}

func TestResyncUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't works with walrus")
	}
	base.LongRunningTest(t)

	testCases := []struct {
		docsCreated int
	}{
		{
			docsCreated: 0,
		},
		{
			docsCreated: 1000,
		},
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("Docs %d", testCase.docsCreated), func(t *testing.T) {
			rt := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: syncFn,
				},
			)
			defer rt.Close()

			_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
			if !ok {
				rt.GetDatabase().ResyncManager = db.NewResyncManagerDCP(rt.GetSingleDataStore(), base.TestUseXattrs(), rt.GetDatabase().MetadataKeys)
			}

			for i := 0; i < testCase.docsCreated; i++ {
				rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
			}

			err := rt.WaitForCondition(func() bool {
				return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == testCase.docsCreated
			})
			assert.NoError(t, err)
			rt.GetDatabase().DbStats.Database().SyncFunctionCount.Set(0)

			response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusServiceUnavailable)

			response = rt.SendAdminRequest("POST", "/db/_offline", "")
			rest.RequireStatus(t, response, http.StatusOK)

			rest.WaitAndAssertCondition(t, func() bool {
				state := atomic.LoadUint32(&rt.GetDatabase().State)
				return state == db.DBOffline
			})

			response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusOK)

			var resyncManagerStatus db.ResyncManagerResponseDCP
			err = rt.WaitForConditionWithOptions(func() bool {
				response := rt.SendAdminRequest("GET", "/db/_resync", "")
				err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
				assert.NoError(t, err)

				var val interface{}
				_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

				if resyncManagerStatus.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err) {
					return true
				} else {
					t.Logf("resyncManagerStatus.State != %v: %v - err:%v", db.BackgroundProcessStateCompleted, resyncManagerStatus.State, err)
					return false
				}
			}, 200, 200)
			assert.NoError(t, err)

			assert.Equal(t, testCase.docsCreated, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

			assert.Equal(t, testCase.docsCreated, int(resyncManagerStatus.DocsProcessed))
			assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
		})
	}
}

func TestResyncForNamedCollection(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("DCP client doesn't work with walrus. Waiting on CBG-2661")
	}
	base.TestRequiresCollections(t)

	base.RequireNumTestDataStores(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)
	serverErr := make(chan error)

	syncFn := `
	function(doc) {
		channel("x")
	}`

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: syncFn,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		t.Skip("This test only works when ResyncManager is used")
	}
	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and add new scopes and collections to it.
	tb := base.GetTestBucket(t)
	defer func() {
		log.Println("closing test bucket")
		tb.Close()
	}()

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore1Name, ok := base.AsDataStoreName(dataStore1)
	require.True(t, ok)
	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	dataStore2Name, ok := base.AsDataStoreName(dataStore2)
	require.True(t, ok)

	keyspace1 := fmt.Sprintf("db.%s.%s", dataStore1Name.ScopeName(),
		dataStore1Name.CollectionName())
	keyspace2 := fmt.Sprintf("db.%s.%s", dataStore2Name.ScopeName(),
		dataStore2Name.CollectionName())

	resp := rt.SendAdminRequest(http.MethodPost, "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "scopes": {"%s": {"collections": {"%s": {}, "%s":{}}}}, "num_index_replicas": 0, "enable_shared_bucket_access": true, "use_views": false}`,
		tb.GetName(), dataStore1Name.ScopeName(), dataStore1Name.CollectionName(), dataStore2Name.CollectionName(),
	))
	rest.RequireStatus(t, resp, http.StatusCreated)

	// put a docs in both collections
	for i := 1; i <= 10; i++ {
		resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/1000%d", keyspace1, i), `{"type":"test_doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/1000%d", keyspace2, i), `{"type":"test_doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	resp = rt.SendAdminRequest(http.MethodPost, "/db/_offline", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	// Run resync for single collection // Request body {"scopes": "scopeName": ["collection1Name", "collection2Name"]}}
	body := fmt.Sprintf(`{
			"scopes" :{
				"%s": ["%s"]
			}
		}`, dataStore1Name.ScopeName(), dataStore1Name.CollectionName())
	resp = rt.SendAdminRequest("POST", "/db/_resync?action=start", body)
	rest.RequireStatus(t, resp, http.StatusOK)

	var resyncManagerStatus db.ResyncManagerResponse
	err = rt.WaitForConditionWithOptions(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
		assert.NoError(t, err)

		if resyncManagerStatus.State == db.BackgroundProcessStateCompleted {
			return true
		} else {
			return false
		}
	}, 200, 200)
	require.NoError(t, err)

	assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
	assert.Equal(t, 10, int(resyncManagerStatus.DocsProcessed))

	// Run resync for all collections
	resp = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	resyncManagerStatus = db.ResyncManagerResponse{}
	err = rt.WaitForConditionWithOptions(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
		assert.NoError(t, err)

		if resyncManagerStatus.State == db.BackgroundProcessStateCompleted {
			return true
		} else {
			return false
		}
	}, 200, 200)
	require.NoError(t, err)

	assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
	assert.Equal(t, 20, int(resyncManagerStatus.DocsProcessed))
}

func TestResyncUsingDCPStreamForNamedCollection(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("DCP client doesn't work with walrus. Waiting on CBG-2661")
	}
	base.TestRequiresCollections(t)

	base.RequireNumTestDataStores(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)
	serverErr := make(chan error)

	syncFn := `
	function(doc) {
		channel("x")
	}`

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: syncFn,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		t.Skip("This test only works when ResyncManagerDCP is used")
	}
	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and add new scopes and collections to it.
	tb := base.GetTestBucket(t)
	defer func() {
		log.Println("closing test bucket")
		tb.Close()
	}()

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore1Name, ok := base.AsDataStoreName(dataStore1)
	require.True(t, ok)
	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	dataStore2Name, ok := base.AsDataStoreName(dataStore2)
	require.True(t, ok)

	keyspace1 := fmt.Sprintf("db.%s.%s", dataStore1Name.ScopeName(),
		dataStore1Name.CollectionName())
	keyspace2 := fmt.Sprintf("db.%s.%s", dataStore2Name.ScopeName(),
		dataStore2Name.CollectionName())

	resp := rt.SendAdminRequest(http.MethodPost, "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "scopes": {"%s": {"collections": {"%s": {}, "%s":{}}}}, "num_index_replicas": 0, "enable_shared_bucket_access": true, "use_views": false}`,
		tb.GetName(), dataStore1Name.ScopeName(), dataStore1Name.CollectionName(), dataStore2Name.CollectionName(),
	))
	rest.RequireStatus(t, resp, http.StatusCreated)

	// put a docs in both collections
	for i := 1; i <= 10; i++ {
		resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/1000%d", keyspace1, i), `{"type":"test_doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/1000%d", keyspace2, i), `{"type":"test_doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	resp = rt.SendAdminRequest(http.MethodPost, "/db/_offline", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	// Run resync for single collection // Request body {"scopes": "scopeName": ["collection1Name", "collection2Name"]}}
	body := fmt.Sprintf(`{
			"scopes" :{
				"%s": ["%s"]
			}
		}`, dataStore1Name.ScopeName(), dataStore1Name.CollectionName())
	resp = rt.SendAdminRequest("POST", "/db/_resync?action=start", body)
	rest.RequireStatus(t, resp, http.StatusOK)

	var resyncManagerStatus db.ResyncManagerResponseDCP
	err = rt.WaitForConditionWithOptions(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
		assert.NoError(t, err)

		if resyncManagerStatus.State == db.BackgroundProcessStateCompleted {
			return true
		} else {
			return false
		}
	}, 200, 200)
	require.NoError(t, err)

	assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
	assert.LessOrEqual(t, 10, int(resyncManagerStatus.DocsProcessed))

	// Run resync for all collections
	resp = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	resyncManagerStatus = db.ResyncManagerResponseDCP{}
	err = rt.WaitForConditionWithOptions(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
		assert.NoError(t, err)

		if resyncManagerStatus.State == db.BackgroundProcessStateCompleted {
			return true
		} else {
			return false
		}
	}, 200, 200)
	require.NoError(t, err)

	assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
	assert.LessOrEqual(t, 20, int(resyncManagerStatus.DocsProcessed))
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

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		rt.GetDatabase().ResyncManager = db.NewResyncManager(rt.GetSingleDataStore(), rt.GetDatabase().MetadataKeys)
	}

	leakyDataStore, ok := base.AsLeakyDataStore(rt.TestBucket.GetSingleDataStore())
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyDataStore.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				rest.RequireStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	} else {
		leakyDataStore.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				rest.RequireStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.True(t, callbackFired, "expecting callback to be fired")
}

func TestResyncErrorScenariosUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't works with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	testBucket := base.GetTestBucket(t)

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: testBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		rt.GetDatabase().ResyncManager = db.NewResyncManagerDCP(rt.GetSingleDataStore(), base.TestUseXattrs(), rt.GetDatabase().MetadataKeys)
	}

	numOfDocs := 1000
	for i := 0; i < numOfDocs; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	assert.Equal(t, numOfDocs, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))
	rt.GetDatabase().DbStats.Database().SyncFunctionCount.Set(0)

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// If processor is running, another start request should throw error
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusServiceUnavailable)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.Equal(t, numOfDocs, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)
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

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: syncFn,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				QueryPaginationLimit: base.IntPtr(10),
			}},
			CustomTestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManager)
	if !ok {
		rt.GetDatabase().ResyncManager = db.NewResyncManager(rt.GetSingleDataStore(), rt.GetDatabase().MetadataKeys)
	}

	leakyDataStore, ok := base.AsLeakyDataStore(rt.TestBucket.GetSingleDataStore())
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyDataStore.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
				rest.RequireStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	} else {
		leakyDataStore.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
				rest.RequireStatus(t, response, http.StatusOK)
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
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateStopped,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.True(t, callbackFired, "expecting callback to be fired")

	syncFnCount := int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.True(t, syncFnCount < 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

func TestResyncStopUsingDCPStream(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		// Walrus doesn't support Collections which is required to create DCP stream
		t.Skip("This test doesn't works with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	testBucket := base.GetTestBucket(t)

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: testBucket,
		},
	)
	defer rt.Close()

	_, ok := (rt.GetDatabase().ResyncManager.Process).(*db.ResyncManagerDCP)
	if !ok {
		rt.GetDatabase().ResyncManager = db.NewResyncManagerDCP(rt.GetSingleDataStore(), base.TestUseXattrs(), rt.GetDatabase().MetadataKeys)
	}

	numOfDocs := 1000
	for i := 0; i < numOfDocs; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	err := rt.WaitForCondition(func() bool {
		return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == numOfDocs
	})
	assert.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)
	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateRunning,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateStopped,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	syncFnCount := int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.True(t, syncFnCount < 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

// Single threaded bring DB online
func TestDBOnlineSingle(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rt.SendAdminRequest("POST", "/db/_online", "")
	rest.RequireStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")
}

// Take DB online concurrently using two goroutines
// Both should return success and DB should be online
// once both goroutines return
func TestDBOnlineConcurrent(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *rest.TestResponse
	go func(rt *rest.RestTester) {
		defer wg.Done()
		goroutineresponse1 = rt.SendAdminRequest("POST", "/db/_online", "")
		rest.RequireStatus(t, goroutineresponse1, 200)
	}(rt)

	var goroutineresponse2 *rest.TestResponse
	go func(rt *rest.RestTester) {
		defer wg.Done()
		goroutineresponse2 = rt.SendAdminRequest("POST", "/db/_online", "")
		rest.RequireStatus(t, goroutineresponse2, 200)
	}(rt)

	// This only waits until both _online requests have been posted
	// They may not have been processed at this point
	wg.Wait()

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online with delay of 1 second
func TestSingleDBOnlineWithDelay(t *testing.T) {

	t.Skip("Use case covered by TestDBOnlineWithTwoDelays, skipping due to slow test")

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	// Wait until after the 1 second delay, since the online request explicitly asked for a delay
	time.Sleep(1500 * time.Millisecond)

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online with delay of 2 seconds
// But bring DB online immediately in separate call
// DB should should only be brought online once
// there should be no errors
func TestDBOnlineWithDelayAndImmediate(t *testing.T) {

	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyAll)

	// CBG-1513: This test is prone to panicing when the walrus bucket was closed and still used
	assert.NotPanicsf(t, func() {
		rt := rest.NewRestTester(t, nil)
		defer rt.Close()

		var response *rest.TestResponse
		var errDBState error

		log.Printf("Taking DB offline")
		require.Equal(t, "Online", rt.GetDBState())

		response = rt.SendAdminRequest("POST", "/db/_offline", "")
		rest.RequireStatus(t, response, 200)

		// Bring DB online with delay of two seconds
		response = rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
		rest.RequireStatus(t, response, 200)

		require.Equal(t, "Offline", rt.GetDBState())

		// Bring DB online immediately
		response = rt.SendAdminRequest("POST", "/db/_online", "")
		rest.RequireStatus(t, response, 200)

		// Wait for DB to come online (retry loop)
		errDBState = rt.WaitForDBOnline()
		assert.NoError(t, errDBState)

		// Wait until after the 1 second delay, since the online request explicitly asked for a delay
		time.Sleep(1500 * time.Millisecond)

		// Wait for DB to come online (retry loop)
		errDBState = rt.WaitForDBOnline()
		assert.NoError(t, errDBState)
	}, "CBG-1513: panicked when the walrus bucket was closed and still used")
}

// Test bring DB online concurrently with delay of 1 second
// and delay of 2 seconds
// BD should should only be brought online once
// there should be no errors
func TestDBOnlineWithTwoDelays(t *testing.T) {

	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	// Bring DB online with delay of one seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	rest.RequireStatus(t, response, 200)

	// Bring DB online with delay of two seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":2}")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	time.Sleep(1500 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	time.Sleep(600 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")
}

func TestPurgeWithBadJsonPayload(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", "foo")
	rest.RequireStatus(t, response, 400)
}

func TestPurgeWithNonArrayRevisionList(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"foo":"list"}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithEmptyRevisionList(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"foo":[]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithGreaterThanOneRevision(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"foo":["rev1","rev2"]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithNonStarRevision(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"foo":["rev1"]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithStarRevision(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar"}`), 201)

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"doc1":["*"]}`)
	rest.RequireStatus(t, response, 200)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}}, body)

	// Create new versions of the doc1 without conflicts
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar"}`), 201)
}

func TestPurgeWithMultipleValidDocs(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar"}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"moo":"car"}`), 201)

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"doc1":["*"],"doc2":["*"]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}, "doc2": []interface{}{"*"}}}, body)

	// Create new versions of the docs without conflicts
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar"}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"moo":"car"}`), 201)
}

// TestPurgeWithChannelCache will make sure thant upon calling _purge, the channel caches are also cleaned
// This was fixed in #3765, previously channel caches were not cleaned up
func TestPurgeWithChannelCache(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar", "channels": ["abc", "def"]}`), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"moo":"car", "channels": ["abc"]}`), http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
	require.NoError(t, err, "Error waiting for changes")
	base.RequireAllAssertions(t,
		assert.Equal(t, "doc1", changes.Results[0].ID),
		assert.Equal(t, "doc2", changes.Results[1].ID),
	)

	// Purge "doc1"
	resp := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"doc1":["*"]}`)
	rest.RequireStatus(t, resp, http.StatusOK)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}}, body)

	changes, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
	require.NoError(t, err, "Error waiting for changes")
	assert.Equal(t, "doc2", changes.Results[0].ID)

}

func TestPurgeWithSomeInvalidDocs(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar"}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"moo":"car"}`), 201)

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"doc1":["*"],"doc2":["1-123"]}`)
	rest.RequireStatus(t, response, 200)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}}, body)

	// Create new versions of the doc1 without conflicts
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar"}`), 201)

	// Create new versions of the doc2 fails because it already exists
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"moo":"car"}`), 409)
}

func TestRawRedaction(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	res := rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc", `{"foo":"bar", "channels": ["achannel"]}`)
	rest.RequireStatus(t, res, http.StatusCreated)

	// Test redact being disabled by default
	res = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/testdoc", ``)
	var body map[string]interface{}
	err := base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData := body[base.SyncPropertyName]
	assert.Equal(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.Equal(t, []interface{}([]interface{}{[]interface{}{"achannel"}}), syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test redacted
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/testdoc?redact=true&include_doc=false", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData = body[base.SyncPropertyName]
	require.NotNil(t, syncData)
	assert.NotEqual(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.NotEqual(t, []interface{}([]interface{}{[]interface{}{"achannel"}}), syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test include doc false doesn't return doc
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/testdoc?include_doc=false", ``)
	assert.NotContains(t, res.Body.String(), "foo")

	// Test doc is returned by default
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/testdoc", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.Equal(t, body["foo"], "bar")

	// Test that you can't use include_doc and redact at the same time
	res = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/testdoc?include_doc=true&redact=true", ``)
	rest.RequireStatus(t, res, http.StatusBadRequest)
}

func TestRawTombstone(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	const docID = "testdoc"

	// Create a doc
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"foo":"bar"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+revID+`"`)
	assert.Contains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_deleted":true`)

	// Delete the doc
	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, revID), ``)
	rest.RequireStatus(t, resp, http.StatusOK)
	revID = rest.RespRevID(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+revID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.Contains(t, string(resp.BodyBytes()), `"_deleted":true`)
}

func TestHandleCreateDB(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	server := "walrus:"
	bucket := "albums"
	kvTLSPort := 11207
	resource := fmt.Sprintf("/%s/", bucket)

	bucketConfig := rest.BucketConfig{Server: &server, Bucket: &bucket, KvTLSPort: kvTLSPort}
	dbConfig := &rest.DbConfig{BucketConfig: bucketConfig, SGReplicateEnabled: base.BoolPtr(false)}
	var respBody db.Body

	reqBody, err := base.JSONMarshal(dbConfig)
	assert.NoError(t, err, "Error unmarshalling changes response")

	resp := rt.SendAdminRequest(http.MethodPut, resource, string(reqBody))
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, resource, string(reqBody))
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Equal(t, bucket, respBody["db_name"].(string))
	assert.Equal(t, "Online", respBody["state"].(string))

	// Try to create database with bad JSON request body and simulate JSON
	// parsing error from the handler; handleCreateDB.
	reqBodyJson := `"server":"walrus:","pool":"default","bucket":"albums","kv_tls_port":11207`
	resp = rt.SendAdminRequest(http.MethodPut, "/photos/", reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
}

func TestHandleCreateDBJsonName(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testCases := []struct {
		name        string
		JSONname    string
		expectError bool
	}{
		{
			name:        "Name match",
			JSONname:    "db1",
			expectError: false,
		},
		{
			name:        "Name mismatch",
			JSONname:    "dummy",
			expectError: true,
		},
		{
			name:        "No JSON Name",
			JSONname:    "",
			expectError: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			tb := base.GetTestBucket(t)
			defer tb.Close()

			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				CustomTestBucket: tb,
				PersistentConfig: true,
			})
			defer rt.Close()

			var resp *rest.TestResponse
			DbConfigJson := ""
			if test.JSONname != "" {
				DbConfigJson = `"name": "` + test.JSONname + `",`

			}
			resp = rt.SendAdminRequest(http.MethodPut, "/db1/",
				fmt.Sprintf(
					`{"bucket": "%s", %s "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
					tb.GetName(), DbConfigJson, base.TestUseXattrs(), base.TestsDisableGSI(),
				),
			)
			if test.expectError {
				rest.RequireStatus(t, resp, http.StatusBadRequest)
			} else {
				rest.RequireStatus(t, resp, http.StatusCreated)
				resp = rt.SendAdminRequest(http.MethodGet, "/db1/", "")
				rest.RequireStatus(t, resp, http.StatusOK)

				var dbStatus map[string]any
				err := json.Unmarshal(resp.BodyBytes(), &dbStatus)
				require.NoError(t, err)
				assert.EqualValues(t, dbStatus["db_name"], "db1")
			}
		})
	}
}

func TestHandlePutDbConfigWithBackticks(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Get database info before putting config.
	resp := rt.SendAdminRequest(http.MethodGet, "/backticks/", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)

	// Create database with valid JSON config that contains sync function enclosed in backticks.
	syncFunc := `function(doc, oldDoc) { console.log("foo");}`
	reqBodyWithBackticks := `{
        "server": "walrus:",
        "bucket": "backticks",
        "sync": ` + "`" + syncFunc + "`" + `
	}`
	resp = rt.SendAdminRequest(http.MethodPut, "/backticks/", reqBodyWithBackticks)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Get database config after putting config.
	resp = rt.SendAdminRequest(http.MethodGet, "/backticks/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var respBody db.Body
	require.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Equal(t, "walrus:", respBody["server"].(string))
	assert.Equal(t, syncFunc, respBody["sync"].(string))
}

func TestHandlePutDbConfigWithBackticksCollections(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Get database info before putting config.
	resp := rt.SendAdminRequest(http.MethodGet, "/backticks/", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)

	// Create database with valid JSON config that contains sync function enclosed in backticks.
	syncFunc := `function(doc, oldDoc) { console.log("foo");}`
	// ` + "`" + syncFunc + "`" + `
	reqBodyWithBackticks := `{
        "server": "walrus:",
        "bucket": "backticks",
		"enable_shared_bucket_access":false,
		"scopes": {
			"scope1": {
			  "collections" : {
				"collection1":{
        			"sync": ` + "`" + syncFunc + "`" + `
 				}
   			  }
			}
  		}
	}`
	resp = rt.SendAdminRequest(http.MethodPut, "/backticks/", reqBodyWithBackticks)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Get database config after putting config.
	resp = rt.SendAdminRequest(http.MethodGet, "/backticks/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var respConfig rest.DbConfig
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respConfig))
	assert.Equal(t, "walrus:", *respConfig.Server)
	assert.Equal(t, syncFunc, *respConfig.Scopes["scope1"].Collections["collection1"].SyncFn)
}

func TestHandleDBConfig(t *testing.T) {
	tb := base.GetTestBucket(t)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{CustomTestBucket: tb})
	defer rt.Close()

	bucket := tb.GetName()
	dbname := rt.GetDatabase().Name
	resource := fmt.Sprintf("/%s/", dbname)

	// Get database config before putting any config.
	resp := rt.SendAdminRequest(http.MethodGet, resource, "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var respBody db.Body
	assert.NoError(t, respBody.Unmarshal(resp.Body.Bytes()))
	assert.Nil(t, respBody["bucket"])
	assert.Equal(t, dbname, respBody["db_name"].(string))
	assert.Equal(t, "Online", respBody["state"].(string))

	// Put database config
	resource = resource + "_config"

	// change cache size so we can see the update being reflected in the API response
	dbConfig := rt.NewDbConfig()
	dbConfig.CacheConfig = &rest.CacheConfig{
		RevCacheConfig: &rest.RevCacheConfig{
			Size: base.Uint32Ptr(1337), ShardCount: base.Uint16Ptr(7),
		},
	}

	resp = rt.ReplaceDbConfig(rt.GetDatabase().Name, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Get database config after putting valid database config
	resp = rt.SendAdminRequest(http.MethodGet, resource, "")
	rest.RequireStatus(t, resp, http.StatusOK)
	respBody = nil
	assert.NoError(t, respBody.Unmarshal(resp.Body.Bytes()))

	gotcache, ok := respBody["cache"].(map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, gotcache)

	gotRevcache, ok := gotcache["rev_cache"].(map[string]interface{})
	require.True(t, ok)
	gotRevcacheSize, ok := gotRevcache["size"].(json.Number)
	require.True(t, ok)
	gotRevcacheSizeInt, err := gotRevcacheSize.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1337), gotRevcacheSizeInt)

	gotRevcacheNumShards, ok := gotRevcache["shard_count"].(json.Number)
	require.True(t, ok)
	gotRevcacheNumShardsInt, err := gotRevcacheNumShards.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(7), gotRevcacheNumShardsInt)

	gotbucket, ok := respBody["bucket"].(string)
	require.True(t, ok)
	assert.Equal(t, bucket, gotbucket)

	gotName, ok := respBody["name"].(string)
	require.True(t, ok)
	assert.Equal(t, dbname, gotName)

	un, _, _ := tb.BucketSpec.Auth.GetCredentials()
	gotusername, ok := respBody["username"].(string)
	require.True(t, ok)
	assert.Equal(t, un, gotusername)
	gotpassword, ok := respBody["password"].(string)
	require.True(t, ok)
	assert.Equal(t, base.RedactedStr, gotpassword)

	_, ok = respBody["certpath"]
	require.False(t, ok)
	_, ok = respBody["keypath"]
	require.False(t, ok)
	_, ok = respBody["cacertpath"]
	require.False(t, ok)
}

func TestHandleDeleteDB(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Try to delete the database which doesn't exists
	resp := rt.SendAdminRequest(http.MethodDelete, "/albums/", "{}")
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, string(resp.BodyBytes()), "no such database")
	var v map[string]interface{}
	assert.NoError(t, json.Unmarshal(resp.BodyBytes(), &v), "couldn't unmarshal %s", string(resp.BodyBytes()))

	// Create the database
	resp = rt.SendAdminRequest(http.MethodPut, "/albums/", `{"server":"walrus:"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Delete the database
	resp = rt.SendAdminRequest(http.MethodDelete, "/albums/", "{}")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "{}")
}

func TestHandleGetConfig(t *testing.T) {
	syncFunc := `function(doc) {throw({forbidden: "read only!"})}`
	conf := rest.RestTesterConfig{SyncFn: syncFunc}
	rt := rest.NewRestTester(t, &conf)
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodGet, "/_config", "{}")
	rest.RequireStatus(t, resp, http.StatusOK)

	var respBody rest.StartupConfig
	assert.NoError(t, base.JSONUnmarshal([]byte(resp.Body.String()), &respBody))

	assert.Equal(t, "127.0.0.1:4985", respBody.API.AdminInterface)
}

func TestHandleGetRevTree(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create three revisions of the user foo with different status and updated_at values;
	reqBodyJson := `{"new_edits": false, "docs": [
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-24T17:37:49.715Z", "status": "online", "_rev": "1-123"},
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-26T17:37:49.715Z", "status": "offline", "_rev": "1-456"},
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-25T17:37:49.715Z", "status": "offline", "_rev": "1-789"}]}`

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/_bulk_docs", reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusCreated)
	respBodyExpected := `[{"id":"foo","rev":"1-123"},{"id":"foo","rev":"1-456"},{"id":"foo","rev":"1-789"}]`
	assert.Equal(t, respBodyExpected, resp.Body.String())

	// Get the revision tree  of the user foo
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_revtree/foo", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "1-123")
	assert.Contains(t, resp.Body.String(), "1-456")
	assert.Contains(t, resp.Body.String(), "1-789")
	assert.True(t, strings.HasPrefix(resp.Body.String(), "digraph"))
}

func TestHandleSGCollect(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	reqBodyJson := "invalidjson"
	resource := "/_sgcollect_info"

	// Check SGCollect status before triggering it; status should be stopped if no process is running.
	resp := rt.SendAdminRequest(http.MethodGet, resource, reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, resp.Body.String(), `{"status":"stopped"}`)

	// Try to cancel SGCollect before triggering it; Error stopping sgcollect_info: not running
	resp = rt.SendAdminRequest(http.MethodDelete, resource, reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "Error stopping sgcollect_info: not running")

	// Try to start SGCollect with invalid body; It should throw with unexpected end of JSON input error
	resp = rt.SendAdminRequest(http.MethodPost, resource, reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
}

func TestConfigRedaction(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{Users: map[string]*auth.PrincipalConfig{"alice": {Password: base.StringPtr("password")}}}}})
	defer rt.Close()

	// Test default db config redaction
	var unmarshaledConfig rest.DbConfig
	response := rt.SendAdminRequest("GET", "/db/_config?include_runtime=true", "")
	err := json.Unmarshal(response.BodyBytes(), &unmarshaledConfig)
	require.NoError(t, err)

	assert.Equal(t, base.RedactedStr, unmarshaledConfig.Password)
	assert.Equal(t, base.RedactedStr, *unmarshaledConfig.Users["alice"].Password)

	// Test default server config redaction
	var unmarshaledServerConfig rest.StartupConfig
	response = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	err = json.Unmarshal(response.BodyBytes(), &unmarshaledServerConfig)
	require.NoError(t, err)

	assert.Equal(t, base.RedactedStr, unmarshaledServerConfig.Bootstrap.Password)
}

func TestSoftDeleteCasMismatch(t *testing.T) {
	// FIXME: LeakyBucket not supported for metadata collection
	t.Skip("LeakyBucket not supported for metadata collection")

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create role
	resp := rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["channel"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	leakyDataStore, ok := base.AsLeakyDataStore(rt.TestBucket.GetSingleDataStore())
	require.True(t, ok)

	// Set callback to trigger a DELETE AFTER an update. This will trigger a CAS mismatch.
	// Update is done on a GetRole operation so this delete is done between a GET and save operation.
	triggerCallback := true
	leakyDataStore.SetPostUpdateCallback(func(key string) {
		if triggerCallback {
			triggerCallback = false
			resp = rt.SendAdminRequest("DELETE", "/db/_role/role", ``)
			rest.RequireStatus(t, resp, http.StatusOK)
		}
	})

	resp = rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["chan"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
}

// Test warnings being issued when a new channel is created with over 250 characters - CBG-1475
func TestChannelNameSizeWarningBoundaries(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"

	testCases := []struct {
		name                string
		warnThresholdLength uint32
		channelLength       int
		expectWarn          bool
	}{
		{
			name:                "Over max default channel length",
			warnThresholdLength: base.DefaultWarnThresholdChannelNameSize,
			channelLength:       int(base.DefaultWarnThresholdChannelNameSize) + 1,
			expectWarn:          true,
		},
		{
			name:                "Equal to max default channel length",
			warnThresholdLength: base.DefaultWarnThresholdChannelNameSize,
			channelLength:       int(base.DefaultWarnThresholdChannelNameSize),
			expectWarn:          false,
		},
		{
			name:                "Under max default channel length",
			warnThresholdLength: base.DefaultWarnThresholdChannelNameSize,
			channelLength:       int(base.DefaultWarnThresholdChannelNameSize) - 1,
			expectWarn:          false,
		},
		{
			name:                "Over max configured channel length",
			warnThresholdLength: 500,
			channelLength:       501,
			expectWarn:          true,
		},
		{
			name:                "Equal to max configured channel length",
			warnThresholdLength: 500,
			channelLength:       500,
			expectWarn:          false,
		},
		{
			name:                "Under max configured channel length",
			warnThresholdLength: 500,
			channelLength:       499,
			expectWarn:          false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var thresholdConfig *db.WarningThresholds
			// If threshold is not default then configure it
			if test.warnThresholdLength != base.DefaultWarnThresholdChannelNameSize {
				thresholdConfig = &db.WarningThresholds{ChannelNameSize: &test.warnThresholdLength}
			}
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: syncFn,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Unsupported: &db.UnsupportedOptions{
						WarningThresholds: thresholdConfig,
					}},
				},
			})
			defer rt.Close()

			ctx := rt.Context()
			chanNameWarnCountBefore := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()

			docId := fmt.Sprintf("doc%v", test.channelLength)
			chanName := strings.Repeat("A", test.channelLength)
			tr := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docId, `{"chan":"`+chanName+`"}`)

			rest.RequireStatus(t, tr, http.StatusCreated)
			chanNameWarnCountAfter := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
			if test.expectWarn {
				assert.Equal(t, chanNameWarnCountBefore+1, chanNameWarnCountAfter)
			} else {
				assert.Equal(t, chanNameWarnCountBefore, chanNameWarnCountAfter)
			}
		})
	}
}
func TestChannelNameSizeWarningUpdateExistingDoc(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: syncFn,
	})
	defer rt.Close()

	// Update doc - should warn
	chanName := strings.Repeat("B", int(base.DefaultWarnThresholdChannelNameSize)+5)
	t.Run("Update doc without changing channel", func(t *testing.T) {
		tr := rt.SendAdminRequest("PUT", "/{{.keyspace}}/replace", `{"chan":"`+chanName+`"}`) // init doc
		rest.RequireStatus(t, tr, http.StatusCreated)

		ctx := rt.Context()
		before := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		revId := rest.RespRevID(t, tr)
		tr = rt.SendAdminRequest("PUT", "/{{.keyspace}}/replace?rev="+revId, `{"chan":"`+chanName+`", "data":"test"}`)
		rest.RequireStatus(t, tr, http.StatusCreated)
		after := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		assert.Equal(t, before+1, after)
	})
}
func TestChannelNameSizeWarningDocChannelUpdate(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: syncFn,
	})
	defer rt.Close()

	channelLength := int(base.DefaultWarnThresholdChannelNameSize) + 5
	// Update doc channel with creation of a new channel
	t.Run("Update doc with new channel", func(t *testing.T) {

		chanName := strings.Repeat("C", channelLength)
		tr := rt.SendAdminRequest("PUT", "/{{.keyspace}}/replaceNewChannel", `{"chan":"`+chanName+`"}`) // init doc
		rest.RequireStatus(t, tr, http.StatusCreated)

		ctx := rt.Context()
		before := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		revId := rest.RespRevID(t, tr)
		chanName = strings.Repeat("D", channelLength+5)
		tr = rt.SendAdminRequest("PUT", "/{{.keyspace}}/replaceNewChannel?rev="+revId, fmt.Sprintf(`{"chan":"`+chanName+`", "data":"test"}`))
		rest.RequireStatus(t, tr, http.StatusCreated)
		after := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		assert.Equal(t, before+1, after)
	})
}
func TestChannelNameSizeWarningDeleteChannel(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: syncFn,
	})
	defer rt.Close()

	channelLength := int(base.DefaultWarnThresholdChannelNameSize) + 5
	// Delete channel over max len - no warning
	t.Run("Delete channel over max length", func(t *testing.T) {
		chanName := strings.Repeat("F", channelLength)
		tr := rt.SendAdminRequest("PUT", "/{{.keyspace}}/deleteme", `{"chan":"`+chanName+`"}`) // init channel
		rest.RequireStatus(t, tr, http.StatusCreated)

		ctx := rt.Context()
		before := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		revId := rest.RespRevID(t, tr)
		tr = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/deleteme?rev="+revId, "")
		rest.RequireStatus(t, tr, http.StatusOK)
		after := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		assert.Equal(t, before, after)
	})
}

func TestConfigEndpoint(t *testing.T) {
	testCases := []struct {
		Name              string
		Config            string
		ConsoleLevel      base.LogLevel
		ConsoleLogKeys    []string
		ExpectError       bool
		FileLoggerCheckFn func() bool
	}{
		{
			Name: "Set LogLevel and LogKeys",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "trace",
						"log_keys": ["Config"]
					}
				}
			}`,
			ConsoleLevel:   base.LevelTrace,
			ConsoleLogKeys: []string{"Config"},
			ExpectError:    false,
		},
		{
			Name: "Set LogLevel and multiple LogKeys",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					}
				}
			}`,
			ConsoleLevel:   base.LevelInfo,
			ConsoleLogKeys: []string{"Config", "HTTP"},
			ExpectError:    false,
		},
		{
			Name: "Set Invalid Fields",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					},
					"fake": {}
				}
			}`,
			ConsoleLevel:   base.LevelTrace,
			ConsoleLogKeys: []string{"Config"},
			ExpectError:    true,
		},
		{
			Name: "Set non-runtime configurable Fields",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					}
				},
				"bootstrap": {
					"server": "couchbase://0.0.0.0"
				}
			}`,
			ConsoleLevel:   base.LevelTrace,
			ConsoleLogKeys: []string{"Config"},
			ExpectError:    true,
		},
		{
			Name: "Enable Error Logger",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					},
					"error": {
						"enabled": true
					}
				}
			}`,
			ConsoleLevel:   base.LevelInfo,
			ConsoleLogKeys: []string{"Config", "HTTP"},
			ExpectError:    false,
			FileLoggerCheckFn: func() bool {
				return base.ErrorLoggerIsEnabled()
			},
		},
		{
			Name: "Enable All File Loggers",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["*"]
					},
					"error": {
						"enabled": true
					},
					"warn": {
						"enabled": true
					},
					"info": {
						"enabled": true
					},
					"debug": {
						"enabled": true
					},
					"trace": {
						"enabled": true
					},
					"stats": {
						"enabled": true
					}
				}
			}`,
			ConsoleLevel:   base.LevelInfo,
			ConsoleLogKeys: []string{"*"},
			ExpectError:    false,
			FileLoggerCheckFn: func() bool {
				return base.ErrorLoggerIsEnabled() && base.WarnLoggerIsEnabled() && base.InfoLoggerIsEnabled() &&
					base.DebugLoggerIsEnabled() && base.TraceLoggerIsEnabled() && base.StatsLoggerIsEnabled()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

			base.InitializeMemoryLoggers()
			tempDir := os.TempDir()
			test := rest.DefaultStartupConfig(tempDir)
			err := test.SetupAndValidateLogging()
			assert.NoError(t, err)

			rt := rest.NewRestTester(t, nil)
			defer rt.Close()

			// By default disable all loggers
			base.EnableErrorLogger(false)
			base.EnableWarnLogger(false)
			base.EnableInfoLogger(false)
			base.EnableDebugLogger(false)
			base.EnableTraceLogger(false)
			base.EnableStatsLogger(false)

			// Request to _config
			resp := rt.SendAdminRequest("PUT", "/_config", testCase.Config)
			if testCase.ExpectError {
				rest.RequireStatus(t, resp, http.StatusBadRequest)
				t.Logf("got response: %s", resp.BodyBytes())
				return
			}

			rest.RequireStatus(t, resp, http.StatusOK)

			assert.Equal(t, testCase.ConsoleLevel, *base.ConsoleLogLevel())
			assert.Equal(t, testCase.ConsoleLogKeys, base.ConsoleLogKey().EnabledLogKeys())

			if testCase.FileLoggerCheckFn != nil {
				assert.True(t, testCase.FileLoggerCheckFn())
			}
		})
	}
}

func TestLoggingDeprecationWarning(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create doc just to startup server and force any initial warnings
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)

	warnCountBefore := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()

	resp = rt.SendAdminRequest("GET", "/_logging", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	warnCountAfter := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	assert.Equal(t, int64(1), warnCountAfter-warnCountBefore)

	resp = rt.SendAdminRequest("PUT", "/_logging", "{}")
	rest.RequireStatus(t, resp, http.StatusOK)

	warnCountAfter2 := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	assert.Equal(t, int64(1), warnCountAfter2-warnCountAfter)

}

func TestInitialStartupConfig(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Get config
	resp := rt.SendAdminRequest("GET", "/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var initialStartupConfig rest.StartupConfig
	err := json.Unmarshal(resp.BodyBytes(), &initialStartupConfig)
	require.NoError(t, err)

	// Assert on a couple values to make sure they are set
	assert.Equal(t, base.TestClusterUsername(), initialStartupConfig.Bootstrap.Username)
	assert.Equal(t, base.RedactedStr, initialStartupConfig.Bootstrap.Password)

	// Assert error logging is nil
	assert.Nil(t, initialStartupConfig.Logging.Error)

	// Set logging running config
	rt.ServerContext().Config.Logging.Error = &base.FileLoggerConfig{}

	// Get config
	resp = rt.SendAdminRequest("GET", "/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	initialStartupConfig = rest.StartupConfig{}
	err = json.Unmarshal(resp.BodyBytes(), &initialStartupConfig)
	require.NoError(t, err)

	// Assert that error logging is still nil, that the above running config didn't change anything
	assert.Nil(t, initialStartupConfig.Logging.Error)
}

func TestIncludeRuntimeStartupConfig(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	base.InitializeMemoryLoggers()
	tempDir := os.TempDir()
	test := rest.DefaultStartupConfig(tempDir)
	err := test.SetupAndValidateLogging()
	assert.NoError(t, err)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	base.EnableErrorLogger(false)
	base.EnableWarnLogger(false)
	base.EnableInfoLogger(false)
	base.EnableDebugLogger(false)
	base.EnableTraceLogger(false)
	base.EnableStatsLogger(false)

	// Get config
	resp := rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var runtimeServerConfigResponse rest.RunTimeServerConfigResponse
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	assert.Contains(t, runtimeServerConfigResponse.Databases, "db")
	assert.Equal(t, base.UnitTestUrl(), runtimeServerConfigResponse.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), runtimeServerConfigResponse.Bootstrap.Username)
	assert.Equal(t, base.RedactedStr, runtimeServerConfigResponse.Bootstrap.Password)

	// Make request to enable error logger
	resp = rt.SendAdminRequest("PUT", "/_config", `
	{
		"logging": {
			"console": {
				"log_level": "debug",
				"log_keys": ["*"]
			},
			"error": {
				"enabled": true
			}
		}
	}
	`)
	rest.RequireStatus(t, resp, http.StatusOK)

	// Update revs limit too so we can check db config
	dbConfig := rt.ServerContext().GetDatabaseConfig("db")
	dbConfig.RevsLimit = base.Uint32Ptr(100)

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	// Check that db revs limit is there now and error logging config
	assert.Contains(t, runtimeServerConfigResponse.Databases, "db")
	assert.Equal(t, base.Uint32Ptr(100), runtimeServerConfigResponse.Databases["db"].RevsLimit)

	assert.NotNil(t, runtimeServerConfigResponse.Logging.Error)
	assert.Equal(t, "debug", runtimeServerConfigResponse.Logging.Console.LogLevel.String())

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true&redact=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	assert.Equal(t, base.RedactedStr, runtimeServerConfigResponse.Bootstrap.Password)

	// Setup replication to ensure it is visible in returned config
	replicationConfig := `{
		"replication_id": "repl",
		"remote": "http://remote:4985/db",
		"direction":"` + db.ActiveReplicatorTypePushAndPull + `",
		"conflict_resolution_type":"default",
		"max_backoff":100
	}`

	response := rt.SendAdminRequest("PUT", "/db/_replication/repl", string(replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db")
	require.Contains(t, runtimeServerConfigResponse.Databases["db"].Replications, "repl")
	replCfg := runtimeServerConfigResponse.Databases["db"].Replications["repl"]
	assert.Equal(t, "repl", replCfg.ID)
	assert.Equal(t, "http://remote:4985/db", replCfg.Remote)
	assert.Equal(t, db.ActiveReplicatorTypePushAndPull, replCfg.Direction)

}

func TestPersistentConfigConcurrency(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyDCP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	config := rest.BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Get config
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	eTag := resp.Header.Get("ETag")
	unquoteETag := strings.Trim(eTag, `"`)
	assert.NotEqual(t, "", unquoteETag)

	resp = rest.BootstrapAdminRequestWithHeaders(t, http.MethodPost, "/db/_config", "{}", map[string]string{"If-Match": eTag})
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_config", "{}")
	resp.RequireStatus(http.StatusCreated)
	putETag := resp.Header.Get("ETag")
	assert.NotEqual(t, "", putETag)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	getETag := resp.Header.Get("ETag")
	assert.Equal(t, putETag, getETag)

	quotedStr := `"x"`
	resp = rest.BootstrapAdminRequestWithHeaders(t, http.MethodPost, "/db/_config", "{}", map[string]string{"If-Match": quotedStr})
	resp.RequireStatus(http.StatusPreconditionFailed)
}

func TestDbConfigCredentials(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	var dbConfig rest.DatabaseConfig

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	require.NoError(t, err)

	// non-runtime config, we don't expect to see any credentials present
	assert.Equal(t, "", dbConfig.Username)
	assert.Equal(t, "", dbConfig.Password)
	assert.Equal(t, "", dbConfig.CACertPath)
	assert.Equal(t, "", dbConfig.CertPath)
	assert.Equal(t, "", dbConfig.KeyPath)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	require.NoError(t, err)

	// runtime config, we expect to see the credentials used by the database (either bootstrap or per-db - but in this case, bootstrap)
	assert.Equal(t, base.TestClusterUsername(), dbConfig.Username)
	assert.Equal(t, base.RedactedStr, dbConfig.Password)
	assert.Equal(t, "", dbConfig.CACertPath)
	assert.Equal(t, "", dbConfig.CertPath)
	assert.Equal(t, "", dbConfig.KeyPath)
}

func TestInvalidDBConfig(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Put db config with invalid sync fn
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", `{"sync": "function(){"}`)
	resp.RequireStatus(http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body, "invalid javascript syntax"))

	// Put invalid sync fn via sync specific endpoint
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/sync", `function(){`)
	resp.RequireStatus(http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body, "invalid javascript syntax"))

	// Put invalid import fn via import specific endpoint
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/import_filter", `function(){`)
	resp.RequireStatus(http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body, "invalid javascript syntax"))
}

func TestCreateDbOnNonExistentBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", `{"bucket": "nonexistentbucket"}`)
	resp.RequireStatus(http.StatusForbidden)
	assert.Contains(t, resp.Body, "Provided credentials do not have access to specified bucket/scope/collection")
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/nonexistentbucket/", `{}`)
	resp.RequireStatus(http.StatusForbidden)
	assert.Contains(t, resp.Body, "Provided credentials do not have access to specified bucket/scope/collection")
}

func TestPutDbConfigChangeName(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", `{"name": "test"}`)
	resp.RequireStatus(http.StatusBadRequest)
}

func TestSwitchDbConfigCollectionName(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("can not create new buckets and scopes in walrus")
	}
	base.TestRequiresCollections(t)

	base.RequireNumTestDataStores(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)
	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and add new scopes and collections to it.
	tb := base.GetTestBucket(t)
	defer func() {
		log.Println("closing test bucket")
		tb.Close()
	}()

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore1Name, ok := base.AsDataStoreName(dataStore1)
	require.True(t, ok)
	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	dataStore2Name, ok := base.AsDataStoreName(dataStore2)
	require.True(t, ok)

	keyspace1 := fmt.Sprintf("db.%s.%s", dataStore1Name.ScopeName(),
		dataStore1Name.CollectionName())
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", fmt.Sprintf(
		`{"bucket": "%s", "scopes": {"%s": {"collections": {"%s": {}}}}, "num_index_replicas": 0, "enable_shared_bucket_access": true, "use_views": false}`,
		tb.GetName(), dataStore1Name.ScopeName(), dataStore1Name.CollectionName(),
	))
	resp.RequireStatus(http.StatusCreated)

	// put a doc in db
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/"+keyspace1+"/10001", `{"type":"test_doc"}`)
	resp.RequireStatus(http.StatusCreated)

	// update config to another collection
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "scopes": {"%s": {"collections": {"%s": {}}}}, "num_index_replicas": 0, "enable_shared_bucket_access": true, "use_views": false}`,
		tb.GetName(), dataStore2Name.ScopeName(), dataStore2Name.CollectionName(),
	))
	resp.RequireStatus(http.StatusCreated)

	// put doc in new collection
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, fmt.Sprintf("/db.%s.%s/10001", dataStore2Name.ScopeName(), dataStore2Name.CollectionName()), `{"type":"test_doc1"}`)
	resp.RequireStatus(http.StatusCreated)

	// update back to original collection config
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "scopes": {"%s": {"collections": {"%s": {}}}}, "num_index_replicas": 0, "enable_shared_bucket_access": true, "use_views": false}`,
		tb.GetName(), dataStore1Name.ScopeName(), dataStore1Name.CollectionName(),
	))
	resp.RequireStatus(http.StatusCreated)

	// put doc in original collection name
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, fmt.Sprintf("/%s/100", keyspace1), `{"type":"test_doc1"}`)
	resp.RequireStatus(http.StatusCreated)
}

func TestPutDBConfigOIDC(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Attempt to update the config with an invalid OIDC issuer - should fail
	invalidOIDCConfig := fmt.Sprintf(
		`{
			"bucket": "%s",
			"num_index_replicas": 0,
			"enable_shared_bucket_access": %t,
			"use_views": %t,
			"oidc": {
				"providers": {
					"test": {
						"issuer": "https://test.invalid",
						"client_id": "test"
					}
				}
			}
		}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
	)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", invalidOIDCConfig)
	resp.RequireStatus(http.StatusBadRequest)

	// Now pass the parameter to skip the validation
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config?disable_oidc_validation=true", invalidOIDCConfig)
	resp.RequireStatus(http.StatusCreated)

	// Now check with a valid OIDC issuer
	validOIDCConfig := fmt.Sprintf(
		`{
			"bucket": "%s",
			"num_index_replicas": 0,
			"enable_shared_bucket_access": %t,
			"use_views": %t,
			"unsupported": {
				"oidc_test_provider": {
					"enabled": true
				}
			},
			"oidc": {
				"providers": {
					"test": {
						"issuer": "http://localhost:%d/db/_oidc_testing",
						"client_id": "sync_gateway"
					}
				}
			}
		}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(), 4984+rest.BootstrapTestPortOffset,
	)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", validOIDCConfig)
	resp.RequireStatus(http.StatusCreated)
}

func TestNotExistentDBRequest(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{AdminInterfaceAuthentication: true})
	defer rt.Close()

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	rest.MakeUser(t, httpClient, eps[0], "random", "password", nil)
	defer rest.DeleteUser(t, httpClient, eps[0], "random")

	// Request to non-existent db with valid credentials
	resp := rt.SendAdminRequestWithAuth("PUT", "/dbx/_config", "", "random", "password")
	rest.RequireStatus(t, resp, http.StatusForbidden)

	// Request to non-existent db with invalid credentials
	resp = rt.SendAdminRequestWithAuth("PUT", "/dbx/_config", "", "random", "passwordx")
	rest.RequireStatus(t, resp, http.StatusUnauthorized)
	require.Contains(t, resp.Body.String(), rest.ErrInvalidLogin.Message)
}

func TestConfigsIncludeDefaults(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Get a test bucket, to use to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	var dbConfig rest.DatabaseConfig
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	assert.NoError(t, err)

	// Validate a few default values to ensure they are set
	assert.Equal(t, channels.DocChannelsSyncFunction, *dbConfig.Sync)
	assert.Equal(t, db.DefaultChannelCacheMaxNumber, *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber)
	assert.Equal(t, base.DefaultOldRevExpirySeconds, *dbConfig.OldRevExpirySeconds)
	assert.Equal(t, false, *dbConfig.StartOffline)
	assert.Equal(t, db.DefaultCompactInterval, uint32(*dbConfig.CompactIntervalDays))

	var runtimeServerConfigResponse rest.RunTimeServerConfigResponse
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &runtimeServerConfigResponse)
	assert.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db")
	runtimeServerConfigDatabase := runtimeServerConfigResponse.Databases["db"]
	assert.Equal(t, channels.DocChannelsSyncFunction, *runtimeServerConfigDatabase.Sync)
	assert.Equal(t, db.DefaultChannelCacheMaxNumber, *runtimeServerConfigDatabase.CacheConfig.ChannelCacheConfig.MaxNumber)
	assert.Equal(t, base.DefaultOldRevExpirySeconds, *runtimeServerConfigDatabase.OldRevExpirySeconds)
	assert.Equal(t, false, *runtimeServerConfigDatabase.StartOffline)
	assert.Equal(t, db.DefaultCompactInterval, uint32(*runtimeServerConfigDatabase.CompactIntervalDays))

	// Test unsupported options
	tb2 := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket 2")
		tb2.Close()
	}()
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db2/",
		`{"bucket": "`+tb2.GetName()+`", "num_index_replicas": 0, "unsupported": {"disable_clean_skipped_query": true}}`,
	)
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &runtimeServerConfigResponse)
	assert.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db2")
	runtimeServerConfigDatabase = runtimeServerConfigResponse.Databases["db2"]
	assert.True(t, runtimeServerConfigDatabase.Unsupported.DisableCleanSkippedQuery)
}

func TestLegacyCredentialInheritance(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, false)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	// No credentials should fail
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusForbidden)

	// Wrong credentials should fail
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db2/",
		`{"bucket": "`+tb.GetName()+`", "username": "test", "password": "invalid_password"}`,
	)
	resp.RequireStatus(http.StatusForbidden)

	// Proper credentials should pass
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db3/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "username": "%s", "password": "%s"}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(), base.TestClusterUsername(), base.TestClusterPassword(),
		),
	)
	resp.RequireStatus(http.StatusCreated)
}

func TestDbOfflineConfigLegacy(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.StartOffline = base.BoolPtr(true)

	// Persist config
	resp := rt.UpsertDbConfig(rt.GetDatabase().Name, dbConfig)
	require.Equal(t, http.StatusCreated, resp.Code)

	// Get config values before taking db offline
	resp = rt.SendAdminRequest("GET", "/db/_config", "")
	require.Equal(t, http.StatusOK, resp.Code)
	dbConfigBeforeOffline := string(resp.BodyBytes())

	// Take DB offline
	resp = rt.SendAdminRequest("POST", "/db/_offline", "")
	require.Equal(t, http.StatusOK, resp.Code)

	// Check offline config matches online config
	resp = rt.SendAdminRequest("GET", "/db/_config", "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, dbConfigBeforeOffline, string(resp.BodyBytes()))
}

func TestDbOfflineConfigPersistent(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := `{
	"bucket": "%s",
	"name": "db",
	"sync": "%s",
	"import_filter": "%s",
	"import_docs": false,
	"offline": false,
	"enable_shared_bucket_access": %t,
	"use_views": %t,
	"num_index_replicas": 0 }`
	dbConfig = fmt.Sprintf(dbConfig, tb.GetName(), syncFunc, importFilter, base.TestUseXattrs(), base.TestsDisableGSI())

	// Persist config
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Get config values before taking db offline
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	dbConfigBeforeOffline := resp.Body

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	// Check offline config matches online config
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireResponse(http.StatusOK, dbConfigBeforeOffline)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)
}

// TestDbConfigPersistentSGVersions ensures that cluster-wide config updates are not applied to older nodes to avoid pushing invalid configuration.
func TestDbConfigPersistentSGVersions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyConfig)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	config := rest.BootstrapStartupConfigForTest(t)

	// enable the background update worker for this test only
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Millisecond * 250)

	ctx := base.TestCtx(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	ctx = sc.SetContextLogID(ctx, "initial")

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	dbName := "db"
	dbConfig := rest.DatabaseConfig{
		SGVersion: "", // leave empty to emulate what 3.0.0 would've written to the bucket
		DbConfig: rest.DbConfig{
			BucketConfig: rest.BucketConfig{
				Bucket: base.StringPtr(tb.GetName()),
			},
			Name:             dbName,
			EnableXattrs:     base.BoolPtr(base.TestUseXattrs()),
			UseViews:         base.BoolPtr(base.TestsDisableGSI()),
			NumIndexReplicas: base.UintPtr(0),
			RevsLimit:        base.Uint32Ptr(123), // use RevsLimit to detect config changes
		},
	}
	dbConfig.Version, err = rest.GenerateDatabaseConfigVersionID("", &dbConfig.DbConfig)
	require.NoError(t, err)

	// initialise with db config
	_, err = sc.BootstrapContext.InsertConfig(ctx, tb.GetName(), t.Name(), &dbConfig)
	require.NoError(t, err)

	assertRevsLimit := func(sc *rest.ServerContext, revsLimit uint32) {
		rest.WaitAndAssertCondition(t, func() bool {
			dbc, err := sc.GetDatabase(ctx, dbName)
			if err != nil {
				t.Logf("expected database with RevsLimit=%v but got err=%v", revsLimit, err)
				return false
			}
			if dbc.RevsLimit != revsLimit {
				t.Logf("expected database with RevsLimit=%v but got %v", revsLimit, dbc.RevsLimit)
				return false
			}
			return true
		}, "expected database with RevsLimit=%v", revsLimit)
	}

	assertRevsLimit(sc, 123)

	writeRevsLimitConfigWithVersion := func(sc *rest.ServerContext, version string, revsLimit uint32) error {
		_, err = sc.BootstrapContext.UpdateConfig(base.TestCtx(t), tb.GetName(), t.Name(), dbName, func(db *rest.DatabaseConfig) (updatedConfig *rest.DatabaseConfig, err error) {

			db.SGVersion = version
			db.DbConfig.RevsLimit = base.Uint32Ptr(revsLimit)
			db.Version, err = rest.GenerateDatabaseConfigVersionID(db.Version, &db.DbConfig)
			if err != nil {
				return nil, err
			}
			return db, nil
		})
		return err
	}

	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "", 456))
	assertRevsLimit(sc, 456)

	// should be allowed (as of writing current version is 3.1.0)
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "3.0.1", 789))
	assertRevsLimit(sc, 789)

	// shouldn't be applied to the already started node (as "5.4.3" is newer)
	warnsStart := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "5.4.3", 654))
	rest.WaitAndAssertConditionTimeout(t, time.Second*10, func() bool {
		warns := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
		return warns-warnsStart > 3
	}, "expected some warnings from trying to apply newer config")
	assertRevsLimit(sc, 789)

	// Shut down the first SG node
	sc.Close(ctx)
	require.NoError(t, <-serverErr)

	// Start a new SG node and ensure we *can* load the "newer" config version on initial startup, to support downgrade
	sc, err = rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	ctx = sc.SetContextLogID(ctx, "newerconfig")
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	assertRevsLimit(sc, 654)

	// overwrite new config back to current version post-downgrade
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "3.1.0", 321))
	assertRevsLimit(sc, 321)
}

func TestDeleteFunctionsWhileDbOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	// Start SG with bootstrap credentials filled
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	// Get a test bucket, and use it to create the database.
	// FIXME: CBG-2266 this test reads in persistent config
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	// Initial DB config
	dbConfig := `{
	"bucket": "` + tb.GetName() + `",
	"name": "db",
	"sync": "function(doc){ throw({forbidden : \"Rejected document\"}) }",
	"offline": false,
	"import_filter": "function(doc) { return false }",
	"import_docs": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
	"num_index_replicas": 0 }`

	// Create initial database
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Make sure import and sync fail
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/TestSyncDoc", "{}")
	resp.RequireStatus(http.StatusForbidden)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/db/_config/sync", "")
	resp.RequireStatus(http.StatusOK)

	// Take DB online
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_online", "")
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, "")

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/TestSyncDoc", "{}")
	resp.RequireStatus(http.StatusCreated)

	if base.TestUseXattrs() {
		// default data store - we're not using a named scope/collection in this test
		add, err := tb.DefaultDataStore().Add("TestImportDoc", 0, db.Document{ID: "TestImportDoc", RevID: "1-abc"})
		require.NoError(t, err)
		require.Equal(t, true, add)

		// On-demand import - rejected doc
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/TestImportDoc", "")
		resp.RequireStatus(http.StatusNotFound)

		// Persist configs
		resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/db/_config/import_filter", "")
		resp.RequireStatus(http.StatusOK)

		// Check configs match
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
		resp.RequireResponse(http.StatusOK, "")

		// On-demand import - allowed doc after restored default import filter
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/TestImportDoc", "")
		resp.RequireStatus(http.StatusOK)
	}
}

func TestSetFunctionsWhileDbOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	// Start SG with bootstrap credentials filled
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	importFilter := "function(doc){ return true; }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	// Initial DB config
	dbConfig := `{
	"bucket": "` + tb.GetName() + `",
	"name": "db",
	"offline": false,
	"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
	"num_index_replicas": 0 }`

	// Create initial database
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	// Persist configs
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/import_filter", importFilter)
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/sync", syncFunc)
	resp.RequireStatus(http.StatusOK)

	// Take DB online
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_online", "")
	resp.RequireStatus(http.StatusOK)

	// Check configs match
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)
}

func TestCollectionSyncFnWithBackticks(t *testing.T) {
	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: true})
	_ = rt.Bucket()
	defer rt.Close()

	scopesConfig := rest.GetCollectionsConfig(t, rt.TestBucket, 1)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName, collectionName := dataStoreNames[0].ScopeName(), dataStoreNames[0].CollectionName()
	// Initial DB config
	dbConfig := fmt.Sprintf(`{
    "num_index_replicas": 0,
    	"bucket": "%s",
    		"scopes": {
			  	"%s": {
					"collections": {
            			"%s": {
				   		"sync":`, rt.Bucket().GetName(), scopeName, collectionName) + "`" +
		`function(doc, oldDoc, meta) {
							var owner = doc._deleted ? oldDoc.owner : doc.owner;
							requireUser(owner);
							var listChannel = 'lists.' + doc._id;
							var contributorRole = 'role:' + listChannel + '.contributor';
							role(owner, contributorRole);
							access(contributorRole, listChannel);
							channel(listChannel);
						}` + "`" + `
         			}
				}
      		}
		}
 	}`

	// Create initial database
	resp := rt.SendAdminRequest(http.MethodPut, "/db/", dbConfig)
	assert.Equal(t, resp.Code, http.StatusCreated)

	// Update database config
	resp = rt.SendAdminRequest(http.MethodPut, "/db/_config", dbConfig)
	assert.Equal(t, resp.Code, http.StatusCreated)
}

func TestEmptyStringJavascriptFunctions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()

	// db put with empty sync func and import filter
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// db config put with empty sync func and import filter
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// db config post, with empty sync func and import filter
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_config",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)
}

// Regression test for CBG-2119 - ensure that the disable_password_auth bool field is handled correctly both when set as true and as false
func TestDisablePasswordAuthThroughAdminAPI(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	// Create a database
	config := rt.NewDbConfig()
	dbName := "db"
	res := rt.CreateDatabase(dbName, config)
	rest.RequireStatus(t, res, http.StatusCreated)

	config.DisablePasswordAuth = base.BoolPtr(true)
	res = rt.UpsertDbConfig(dbName, config)
	rest.RequireStatus(t, res, http.StatusCreated)
	assert.True(t, rt.GetDatabase().Options.DisablePasswordAuthentication)

	config.DisablePasswordAuth = base.BoolPtr(false)
	res = rt.UpsertDbConfig(dbName, config)
	rest.RequireStatus(t, res, http.StatusCreated)
	assert.False(t, rt.GetDatabase().Options.DisablePasswordAuthentication)
}

// CBG-1790: Deleting a database that targets the same bucket as another causes a panic in legacy
func TestDeleteDatabasePointingAtSameBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works against Couchbase Server with xattrs")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)
	tb := base.GetTestBucket(t)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{CustomTestBucket: tb})
	defer rt.Close()
	resp := rt.SendAdminRequest(http.MethodDelete, "/db/", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	// Make another database that uses import in-order to trigger the panic instantly instead of having to time.Sleep
	resp = rt.SendAdminRequest(http.MethodPut, "/db1/", fmt.Sprintf(`{
		"bucket": "%s",
		"username": "%s",
		"password": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb.GetName(), base.TestClusterUsername(), base.TestClusterPassword(), base.TestsDisableGSI()))
	rest.RequireStatus(t, resp, http.StatusCreated)
}

func TestDeleteDatabasePointingAtSameBucketPersistent(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works against Couchbase Server with xattrs")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)
	// Start SG with no databases in bucket(s)
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()

	dbConfig := `{
   "bucket": "` + tb.GetName() + `",
   "name": "%s",
   "import_docs": true,
   "enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
   "use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
   "num_index_replicas": 0 }`

	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db1/", fmt.Sprintf(dbConfig, "db1"))
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/db1/", "")
	resp.RequireStatus(http.StatusOK)

	// Make another database that uses import in-order to trigger the panic instantly instead of having to time.Sleep
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db2/", fmt.Sprintf(dbConfig, "db2"))
	resp.RequireStatus(http.StatusCreated)

	scopeName := ""
	collectionNames := []string{}
	// Validate that deleted database is no longer in dest factory set
	_, fetchDb1DestErr := base.FetchDestFactory(base.ImportDestKey("db1", scopeName, collectionNames))
	assert.Equal(t, base.ErrNotFound, fetchDb1DestErr)
	_, fetchDb2DestErr := base.FetchDestFactory(base.ImportDestKey("db2", scopeName, collectionNames))
	assert.NoError(t, fetchDb2DestErr)
}

func TestApiInternalPropertiesHandling(t *testing.T) {
	testCases := []struct {
		name                        string
		inputBody                   map[string]interface{}
		expectedErrorStatus         *int // If nil, will check for 201 Status Created
		skipDocContentsVerification *bool
	}{
		{
			name:      "Valid document with special prop",
			inputBody: map[string]interface{}{"_cookie": "is valid"},
		},
		{
			name:                "Invalid _sync",
			inputBody:           map[string]interface{}{"_sync": true},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                "Valid _sync",
			inputBody:           map[string]interface{}{"_sync": db.SyncData{}},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                        "Valid _deleted",
			inputBody:                   map[string]interface{}{"_deleted": false},
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                        "Valid _revisions",
			inputBody:                   map[string]interface{}{"_revisions": map[string]interface{}{"ids": "1-abc"}},
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                        "Valid _exp",
			inputBody:                   map[string]interface{}{"_exp": "123"},
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                "Invalid _exp",
			inputBody:           map[string]interface{}{"_exp": "abc"},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                "_purged",
			inputBody:           map[string]interface{}{"_purged": false},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                "_removed",
			inputBody:           map[string]interface{}{"_removed": false},
			expectedErrorStatus: base.IntPtr(http.StatusNotFound),
		},
		{
			name:                "_sync_cookies",
			inputBody:           map[string]interface{}{"_sync_cookies": true},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name: "Valid user defined uppercase properties", // Uses internal properties names but in upper case
			// Known issue: _SYNC causes unmarshal error when not using xattrs
			inputBody: map[string]interface{}{
				"_ID": true, "_REV": true, "_DELETED": true, "_ATTACHMENTS": true, "_REVISIONS": true,
				"_EXP": true, "_PURGED": true, "_REMOVED": true, "_SYNC_COOKIES": true,
			},
		},
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	for i, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := fmt.Sprintf("test%d", i)
			rawBody, err := json.Marshal(test.inputBody)
			require.NoError(t, err)

			resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docID, string(rawBody))
			if test.expectedErrorStatus != nil {
				rest.RequireStatus(t, resp, *test.expectedErrorStatus)
				return
			}
			rest.RequireStatus(t, resp, http.StatusCreated)

			var bucketDoc map[string]interface{}
			_, err = rt.GetSingleDataStore().Get(docID, &bucketDoc)
			assert.NoError(t, err)
			body := rt.GetDoc(docID)
			// Confirm input body is in the bucket doc
			if test.skipDocContentsVerification == nil || !*test.skipDocContentsVerification {
				for k, v := range test.inputBody {
					assert.Equal(t, v, bucketDoc[k])
					assert.Equal(t, v, body[k])
				}
			}
		})
	}
}

func TestPutIDRevMatchBody(t *testing.T) {
	// [REV] is replaced with the most recent revision of document "doc"
	testCases := []struct {
		name        string
		docBody     string
		docID       string
		rev         string
		expectError bool
	}{
		{
			name:        "ID match",
			docBody:     `{"_id": "id_match"}`,
			docID:       "id_match",
			expectError: false,
		},
		{
			name:        "ID mismatch",
			docBody:     `{"_id": "id_mismatch"}`,
			docID:       "completely_different_id",
			expectError: true,
		},
		{
			name:        "ID in URL only",
			docBody:     `{}`,
			docID:       "id_in_url",
			expectError: false,
		},
		{
			name:        "Rev match",
			docBody:     `{"_rev": "[REV]", "nonce": "1"}`,
			rev:         "[REV]",
			expectError: false,
		},
		{
			name:        "Rev mismatch",
			docBody:     `{"_rev": "[REV]", "nonce": "2"}`,
			rev:         "1-abc",
			expectError: true,
		},
		{
			name:        "Rev in body only",
			docBody:     `{"_rev": "[REV]", "nonce": "3"}`,
			expectError: false,
		},
		{
			name:        "Rev in URL only",
			docBody:     `{"nonce": "4"}`,
			rev:         "[REV]",
			expectError: false,
		},
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	// Create document to create rev from
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", "{}")
	rest.RequireStatus(t, resp, 201)
	rev := rest.RespRevID(t, resp)

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := test.docID
			docRev := test.rev
			docBody := test.docBody
			if test.docID == "" {
				docID = "doc" // Used for the rev tests to branch off of
				docBody = strings.ReplaceAll(docBody, "[REV]", rev)
				docRev = strings.ReplaceAll(docRev, "[REV]", rev)
			}

			resp = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, docRev), docBody)
			if test.expectError {
				rest.RequireStatus(t, resp, 400)
				return
			}
			rest.RequireStatus(t, resp, 201)
			if test.docID == "" {
				// Update rev to branch off for next test
				rev = rest.RespRevID(t, resp)
			}
		})
	}
}

func setServerPurgeInterval(t *testing.T, rt *rest.RestTester, newPurgeInterval string) {
	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	settings := url.Values{}
	settings.Add("purgeInterval", newPurgeInterval)
	settings.Add("autoCompactionDefined", "true")
	settings.Add("parallelDBAndViewCompaction", "false")

	req, err := http.NewRequest("POST", eps[0]+"/pools/default/buckets/"+rt.Bucket().GetName(), strings.NewReader(settings.Encode()))
	require.NoError(t, err)
	req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	require.NoError(t, err)

	require.Equal(t, resp.StatusCode, http.StatusOK)
}

func TestTombstoneCompactionPurgeInterval(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Server compaction metadata purge interval can only be changed on Couchbase Server")
	}
	testCases := []struct {
		name                              string
		newServerInterval                 string
		dbPurgeInterval                   time.Duration
		expectedPurgeIntervalAfterCompact time.Duration
	}{
		{
			name:                              "Default purge interval updated after db creation",
			newServerInterval:                 "1",
			dbPurgeInterval:                   db.DefaultPurgeInterval,
			expectedPurgeIntervalAfterCompact: time.Hour * 24,
		},
		{
			name:                              "Ignore server interval",
			newServerInterval:                 "1",
			dbPurgeInterval:                   0,
			expectedPurgeIntervalAfterCompact: 0,
		},
		{
			name:                              "Purge interval updated after db creation",
			newServerInterval:                 "1",
			dbPurgeInterval:                   time.Hour,
			expectedPurgeIntervalAfterCompact: time.Hour * 24,
		},
	}
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	dbc := rt.GetDatabase()
	ctx := rt.Context()

	cbStore, _ := base.AsCouchbaseBucketStore(rt.Bucket())
	serverPurgeInterval, err := cbStore.MetadataPurgeInterval()
	require.NoError(t, err)
	// Set server purge interval back to what it was for bucket reuse
	defer setServerPurgeInterval(t, rt, fmt.Sprintf("%.2f", serverPurgeInterval.Hours()/24))
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Set intervals on server and client
			dbc.PurgeInterval = test.dbPurgeInterval
			setServerPurgeInterval(t, rt, test.newServerInterval)

			// Start compact to modify purge interval
			database, _ := db.GetDatabase(dbc, nil)
			_, err = database.Compact(ctx, false, func(purgedDocCount *int) {}, base.NewSafeTerminator())
			require.NoError(t, err)

			// Check purge interval is as expected
			if !base.TestUseXattrs() {
				// Not using xattrs should cause compaction to not run therefore not changing purge interval
				assert.EqualValues(t, test.dbPurgeInterval, dbc.PurgeInterval)
				return
			}
			assert.EqualValues(t, test.expectedPurgeIntervalAfterCompact, dbc.PurgeInterval)
		})
	}
}

// Make sure per DB credentials override per bucket credentials
func TestPerDBCredsOverride(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// Get test bucket
	tb1 := base.GetTestBucket(t)
	defer tb1.Close()

	config := rest.BootstrapStartupConfigForTest(t)
	config.BucketCredentials = map[string]*base.CredentialsConfig{
		tb1.GetName(): {
			Username: base.TestClusterUsername(),
			Password: base.TestClusterPassword(),
		},
	}
	config.DatabaseCredentials = map[string]*base.CredentialsConfig{
		"db": {
			Username: "invalid",
			Password: "invalid",
		},
	}

	ctx := base.TestCtx(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)

	serverErr := make(chan error, 0)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	couchbaseCluster, err := rest.CreateCouchbaseClusterFromStartupConfig(sc.Config, base.PerUseClusterConnections)
	require.NoError(t, err)
	sc.BootstrapContext.Connection = couchbaseCluster

	dbConfig := `{
		"bucket": "` + tb1.GetName() + `",
		"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
		"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
		"num_index_replicas": 0
	}`

	res := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	// Make sure request failed as it could authenticate with the bucket
	assert.Equal(t, http.StatusForbidden, res.StatusCode)

	// Allow database to be created sucessfully
	sc.Config.DatabaseCredentials = map[string]*base.CredentialsConfig{}
	res = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	assert.Equal(t, http.StatusCreated, res.StatusCode)

	// Confirm fetch configs causes bucket credentials to be overrode
	sc.Config.DatabaseCredentials = map[string]*base.CredentialsConfig{
		"db": {
			Username: "invalidUsername",
			Password: "invalidPassword",
		},
	}
	configs, err := sc.FetchConfigs(ctx, false)
	require.NoError(t, err)
	require.NotNil(t, configs["db"])
	assert.Equal(t, "invalidUsername", configs["db"].BucketConfig.Username)
	assert.Equal(t, "invalidPassword", configs["db"].BucketConfig.Password)
}

// Can be used to reproduce connections left open after database close.  Manually deleting the bucket used by the test
// once the test reaches the sleep loop will log connection errors for unclosed connections.
func TestDeleteDatabaseCBGTTeardown(t *testing.T) {
	t.Skip("Dev-time test used to repro agent connections being left open after database close")
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeyImport)

	rtConfig := rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{AutoImport: true}}}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	// Initialize database
	_ = rt.GetDatabase()

	for i := 0; i < 1; i++ {
		time.Sleep(1 * time.Second) // some time for polling
	}

	resp := rt.SendAdminRequest(http.MethodDelete, "/db/", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	for i := 0; i < 1000; i++ {
		time.Sleep(1 * time.Second) // some time for polling
	}
}

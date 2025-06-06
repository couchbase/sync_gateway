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
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
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
	revGeneration, revIdHash := db.ParseRevID(rt.Context(), revId)
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
	revGeneration, _ = db.ParseRevID(rt.Context(), revId)
	assert.Equal(t, 2, revGeneration)

	// Create conflict again, should be a no-op and return the same response as previous attempt
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(rt.Context(), revId)
	assert.Equal(t, 2, revGeneration)

}

func TestLoggingKeys(t *testing.T) {
	base.ResetGlobalTestLogging(t)
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
	rt.CreateUser("alice", []string{"ABC"})

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

	rt.CreateTestDoc("doc1")
	rt.CreateTestDoc("doc2")
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
func TestDBGetConfigNamesAndDefaultLogging(t *testing.T) {

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
	emptyCnf := rest.DefaultPerDBLogging(rt.ServerContext().Config.Logging)
	assert.Equal(t, body.Logging, emptyCnf)

	for k, v := range body.Users {
		assert.Equal(t, k, *v.Name)
	}
}

func TestDBGetConfigCustomLogging(t *testing.T) {
	logKeys := []string{base.KeyAccess.String(), base.KeyHTTP.String()}
	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Logging: &rest.DbLoggingConfig{
					Console: &rest.DbConsoleLoggingConfig{
						LogLevel: base.Ptr(base.LevelError),
						LogKeys:  logKeys,
					},
				},
			},
			},
		},
	)

	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/_config?include_runtime=true", "")
	var body rest.DbConfig
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	assert.Equal(t, body.Logging.Console.LogLevel, base.Ptr(base.LevelError))
	assert.Equal(t, body.Logging.Console.LogKeys, logKeys)
}

func TestDBOfflineSingleResyncUsingDCPStream(t *testing.T) {
	base.TestRequiresDCPResync(t)
	syncFn := `
	function(doc) {
		channel("x")
	}`
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: syncFn})
	defer rt.Close()

	// create documents in DB to cause resync to take a few seconds
	for i := 0; i < 1000; i++ {
		rt.CreateTestDoc(fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(1000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	rt.TakeDbOffline()

	response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Send a second _resync request.  This must return a 400 since the first one is blocked processing
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 503)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
	assert.Equal(t, int64(2000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
}

func TestDCPResyncCollectionsStatus(t *testing.T) {
	base.TestRequiresDCPResync(t)
	base.TestRequiresCollections(t)

	testCases := []struct {
		name              string
		collectionNames   []string
		expectedResult    map[string][]string
		specifyCollection bool
	}{
		{
			name:            "collections_specified",
			collectionNames: []string{"sg_test_0"},
			expectedResult: map[string][]string{
				"sg_test_0": {"sg_test_0"},
			},
			specifyCollection: true,
		},
		{
			name: "collections_not_specified",
			expectedResult: map[string][]string{
				"sg_test_0": {"sg_test_0", "sg_test_1", "sg_test_2"},
			},
			collectionNames: nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rt := rest.NewRestTesterMultipleCollections(t, nil, 3)
			defer rt.Close()
			scopeName := "sg_test_0"

			// create documents in DB to cause resync to take a few seconds
			for i := 0; i < 1000; i++ {
				resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace1}}/"+fmt.Sprint(i), `{"value":1}`)
				rest.RequireStatus(t, resp, http.StatusCreated)
			}

			rt.TakeDbOffline()

			if !testCase.specifyCollection {
				resp := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				rest.RequireStatus(t, resp, http.StatusOK)
			} else {
				payload := `{"scopes": {"sg_test_0":["sg_test_0"]}}`
				resp := rt.SendAdminRequest("POST", "/db/_resync?action=start", payload)
				rest.RequireStatus(t, resp, http.StatusOK)
			}
			statusResponse := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)
			assert.ElementsMatch(t, statusResponse.CollectionsProcessing[scopeName], testCase.expectedResult[scopeName])

			statusResponse = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
			assert.ElementsMatch(t, statusResponse.CollectionsProcessing[scopeName], testCase.expectedResult[scopeName])
		})
	}
}

func TestResyncUsingDCPStream(t *testing.T) {
	base.TestRequiresDCPResync(t)
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

			for i := 0; i < testCase.docsCreated; i++ {
				rt.CreateTestDoc(fmt.Sprintf("doc%d", i))
			}

			err := rt.WaitForCondition(func() bool {
				return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == testCase.docsCreated
			})
			assert.NoError(t, err)
			rt.GetDatabase().DbStats.Database().SyncFunctionCount.Set(0)

			response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusServiceUnavailable)

			rt.TakeDbOffline()

			response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusOK)

			resyncManagerStatus := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

			assert.Equal(t, testCase.docsCreated, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

			assert.Equal(t, testCase.docsCreated, int(resyncManagerStatus.DocsProcessed))
			assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
		})
	}
}

func TestResyncUsingDCPStreamReset(t *testing.T) {
	base.TestRequiresDCPResync(t)
	base.LongRunningTest(t)

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

	const numDocs = 1000

	// create some docs
	for i := 0; i < numDocs; i++ {
		rt.CreateTestDoc(fmt.Sprintf("doc%d", i))
	}

	rt.TakeDbOffline()

	// start a resync run
	response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	resyncManagerStatus := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)
	resyncID := resyncManagerStatus.ResyncID

	// stop resync before it completes, assert it has been aborted
	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	_ = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateStopped)

	// reset the resync process through the endpoint
	response = rt.SendAdminRequest("POST", "/db/_resync?reset=true", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// grab new resync status from rest run, assert the resync if is not the same as the first
	// run and that the docs processed is equal to number of docs we have created
	resyncManagerStatus = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)
	assert.NotEqual(t, resyncID, resyncManagerStatus.ResyncID)

	resyncManagerStatus = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
	assert.Equal(t, int64(numDocs), resyncManagerStatus.DocsProcessed)

}

func TestResyncUsingDCPStreamForNamedCollection(t *testing.T) {
	base.TestRequiresDCPResync(t)
	base.TestRequiresCollections(t)

	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	syncFn := `
	function(doc) {
		channel("x")
	}`

	rt := rest.NewRestTesterMultipleCollections(t,
		&rest.RestTesterConfig{
			SyncFn: syncFn,
		},
		numCollections,
	)
	defer rt.Close()

	// put a docs in both collections
	for i := 1; i <= 10; i++ {
		resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace1}}/1000%d", i), `{"type":"test_doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace2}}/1000%d", i), `{"type":"test_doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	rt.TakeDbOffline()

	dataStore1, err := rt.TestBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	// Run resync for single collection // Request body {"scopes": "scopeName": ["collection1Name", "collection2Name"]}}
	body := fmt.Sprintf(`{
			"scopes" :{
				"%s": ["%s"]
			}
		}`, dataStore1.ScopeName(), dataStore1.CollectionName())
	resp := rt.SendAdminRequest("POST", "/db/_resync?action=start", body)
	rest.RequireStatus(t, resp, http.StatusOK)
	resyncManagerStatus := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
	assert.LessOrEqual(t, 10, int(resyncManagerStatus.DocsProcessed))

	// Run resync for all collections
	resp = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	resyncManagerStatus = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	assert.Equal(t, 0, int(resyncManagerStatus.DocsChanged))
	assert.LessOrEqual(t, 20, int(resyncManagerStatus.DocsProcessed))
}

func TestResyncErrorScenariosUsingDCPStream(t *testing.T) {
	base.TestRequiresDCPResync(t)

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

	numOfDocs := 1000
	for i := 0; i < numOfDocs; i++ {
		rt.CreateTestDoc(fmt.Sprintf("doc%d", i))
	}

	assert.Equal(t, numOfDocs, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))
	rt.GetDatabase().DbStats.Database().SyncFunctionCount.Set(0)

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	rt.TakeDbOffline()

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// If processor is running, another start request should throw error
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusServiceUnavailable)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	assert.Equal(t, numOfDocs, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
}

// TestCorruptDbConfigHandling:
//   - Create persistent config rest tester
//   - Create a db with a non-corrupt config
//   - Grab the persisted config from the bucket for purposes of changing the bucket name on it (this simulates a
//     dbconfig becoming corrupt)
//   - Update the persisted config to change the bucket name to a non-existent bucket
//   - Assert that the db context and config are removed from server context and that operations on the database GET and
//     DELETE and operation to update the config fail with appropriate error message for user
//   - Test we are able to update the config to correct the corrupted db config using the /db1/ endpoint
//   - assert the db returns to the server context and is removed from corrupt database tracking AND that the bucket name
//     on the config now matches the rest tester bucket name
func TestCorruptDbConfigHandling(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyConfig)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure the interval time to large interval, so it doesn't run during test
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(10 * time.Minute)
		},
	})
	defer rt.Close()
	ctx := base.TestCtx(t)

	// create db with correct config
	dbConfig := rt.NewDbConfig()
	resp := rt.CreateDatabase("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// grab the persisted db config from the bucket
	databaseConfig := rest.DatabaseConfig{}
	_, err := rt.ServerContext().BootstrapContext.GetConfig(rt.Context(), rt.CustomTestBucket.GetName(), rt.ServerContext().Config.Bootstrap.ConfigGroupID, "db1", &databaseConfig)
	require.NoError(t, err)

	// update the persisted config to a fake bucket name
	newBucketName := "fakeBucket"
	_, err = rt.UpdatePersistedBucketName(&databaseConfig, &newBucketName)
	require.NoError(t, err)

	// force reload of configs from bucket
	rt.ServerContext().ForceDbConfigsReload(t, ctx)

	// assert that the in memory representation of the db config on the server context is gone now we have broken the config
	responseConfig := rt.ServerContext().GetDbConfig("db1")
	assert.Nil(t, responseConfig)

	require.Len(t, rt.ServerContext().AllInvalidDatabaseNames(t), 1)

	// assert that fetching config fails with the correct error message to the user
	resp = rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// assert trying to delete fails with the correct error message to the user
	resp = rt.SendAdminRequest(http.MethodDelete, "/db1/", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// correct the name through update to config
	resp = rt.ReplaceDbConfig("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// create db of same name with correct db config to correct the corrupt db config
	resp = rt.CreateDatabase("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// force reload of configs from bucket
	rt.ServerContext().ForceDbConfigsReload(t, ctx)

	// assert that the config is back in memory even after another interval update pass and asser the persisted config
	// bucket name matches rest tester bucket name
	var dbCtx *db.DatabaseContext
	// wait for db to be active on the server context
	err = rt.WaitForConditionWithOptions(func() bool {
		var err error
		dbCtx, err = rt.ServerContext().GetActiveDatabase("db1")
		return err == nil
	}, 200, 1000)
	require.NoError(t, err)
	assert.NotNil(t, dbCtx)
	assert.Equal(t, rt.CustomTestBucket.GetName(), dbCtx.Bucket.GetName())
	rt.ServerContext().RequireInvalidDatabaseConfigNames(t, []string{})
}

// TestBadConfigInsertionToBucket:
//   - start a rest tester
//   - insert an invalid db config to the bucket while rest tester is running
//   - assert that the db config is picked up as an invalid db config
//   - assert that a call to the db endpoint will fail with correct error message
func TestBadConfigInsertionToBucket(t *testing.T) {

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 50 milliseconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
		},
		DatabaseConfig: nil,
	})
	defer rt.Close()

	// create a new invalid db config and persist to bucket
	badName := "badBucketName"
	dbConfig := rt.NewDbConfig()
	dbConfig.Name = "db1"

	version, err := rest.GenerateDatabaseConfigVersionID(rt.Context(), "", &dbConfig)
	require.NoError(t, err)

	metadataID, metadataIDError := rt.ServerContext().BootstrapContext.ComputeMetadataIDForDbConfig(base.TestCtx(t), &dbConfig)
	require.NoError(t, metadataIDError)

	dbConfig.Bucket = &badName
	persistedConfig := rest.DatabaseConfig{
		Version:    version,
		MetadataID: metadataID,
		DbConfig:   dbConfig,
		SGVersion:  base.ProductVersion.String(),
	}
	rt.InsertDbConfigToBucket(&persistedConfig, rt.CustomTestBucket.GetName())

	// asser that the config is picked up as invalid config on server context
	err = rt.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt.ServerContext().AllInvalidDatabaseNames(t)
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that a request to the database fails with correct error message
	resp := rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")
}

// TestMismatchedBucketNameOnDbConfigUpdate:
//   - Create a db on the rest tester
//   - attempt to update the config to change the bucket name on the config to a mismatched bucket name
//   - assert the request fails
func TestMismatchedBucketNameOnDbConfigUpdate(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 50 milliseconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
		},
	})
	defer rt.Close()

	// create db with correct config
	dbConfig := rt.NewDbConfig()
	resp := rt.CreateDatabase("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// wait for db to come online
	rt.WaitForDBOnline()
	badName := tb1.GetName()
	dbConfig.Bucket = &badName

	// assert request fails
	resp = rt.ReplaceDbConfig("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusNotFound)
}

// TestMultipleBucketWithBadDbConfigScenario1:
//   - in bucketA and bucketB, write two db configs with bucket name as bucketC
//   - Start new rest tester and ensure they aren't picked up as valid configs
func TestMultipleBucketWithBadDbConfigScenario1(t *testing.T) {
	base.RequireNumTestBuckets(t, 3)
	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)
	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)
	tb3 := base.GetTestBucket(t)
	defer tb3.Close(ctx)

	const groupID = "60ce5544-c368-4b08-b0ed-4ca3b37973f9"

	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// all RestTesters all this test must use the same config ID
			config.Bootstrap.ConfigGroupID = groupID
		},
	})

	// create a db config that has bucket C in the config and persist to rt1 bucket
	dbConfig := rt1.NewDbConfig()
	dbConfig.Name = "db1"
	rt1.PersistDbConfigToBucket(dbConfig, tb3.GetName())
	defer rt1.Close()

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb2,
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure same config groupID
			config.Bootstrap.ConfigGroupID = groupID
		},
	})
	defer rt2.Close()

	// create a db config that has bucket C in the config and persist to rt2 bucket
	dbConfig = rt2.NewDbConfig()
	dbConfig.Name = "db1"
	rt2.PersistDbConfigToBucket(dbConfig, tb3.GetName())

	rt3 := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: tb3,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 50 milliseconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
			// configure same config groupID
			config.Bootstrap.ConfigGroupID = groupID
		},
	})
	defer rt3.Close()

	// assert the invalid database is picked up with new rest tester
	err := rt3.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt3.ServerContext().AllInvalidDatabaseNames(t)
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that there are no valid db configs on the server context
	err = rt3.WaitForConditionWithOptions(func() bool {
		databaseNames := rt3.ServerContext().AllDatabaseNames()
		return len(databaseNames) == 0
	}, 200, 1000)
	require.NoError(t, err)

	// assert a request to the db fails with correct error message
	resp := rt3.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")
}

// TestMultipleBucketWithBadDbConfigScenario2:
//   - create bucketA and bucketB with db configs that that both list bucket name as bucketA
//   - start a new rest tester and assert that invalid db config is picked up and the valid one is also picked up
func TestMultipleBucketWithBadDbConfigScenario2(t *testing.T) {

	base.RequireNumTestBuckets(t, 3)
	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)
	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)

	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure same config groupID
			config.Bootstrap.ConfigGroupID = "60ce5544-c368-4b08-b0ed-4ca3b37973f9"
		},
	})
	defer rt1.Close()

	// create a db config pointing to bucket C and persist to bucket A
	dbConfig := rt1.NewDbConfig()
	dbConfig.Name = "db1"
	rt1.PersistDbConfigToBucket(dbConfig, rt1.CustomTestBucket.GetName())

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb2,
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure same config groupID
			config.Bootstrap.ConfigGroupID = "60ce5544-c368-4b08-b0ed-4ca3b37973f9"
		},
	})
	defer rt2.Close()

	// create a db config pointing to bucket C and persist to bucket B
	dbConfig = rt2.NewDbConfig()
	dbConfig.Name = "db1"
	rt2.PersistDbConfigToBucket(dbConfig, "badName")

	rt3 := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 50 milliseconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
			// configure same config groupID
			config.Bootstrap.ConfigGroupID = "60ce5544-c368-4b08-b0ed-4ca3b37973f9"
		},
	})
	defer rt3.Close()

	// assert that the invalid config is picked up by the new rest tester
	err := rt3.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt3.ServerContext().AllInvalidDatabaseNames(t)
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that there is a valid database picked up as the invalid configs have this rest tester backing bucket
	err = rt3.WaitForConditionWithOptions(func() bool {
		validDatabase := rt3.ServerContext().AllDatabases()
		return len(validDatabase) == 1
	}, 200, 1000)
	require.NoError(t, err)
}

// TestMultipleBucketWithBadDbConfigScenario3:
//   - create a rest tester
//   - create a db on the rest tester
//   - persist that db config to another bucket
//   - assert that is picked up as an invalid db config
func TestMultipleBucketWithBadDbConfigScenario3(t *testing.T) {

	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)
	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 50 milliseconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
		},
	})
	defer rt.Close()

	// create a new db
	dbConfig := rt.NewDbConfig()
	dbConfig.Name = "db1"
	dbConfig.BucketConfig.Bucket = base.Ptr(rt.CustomTestBucket.GetName())
	resp := rt.CreateDatabase("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// persistence logic construction
	version, err := rest.GenerateDatabaseConfigVersionID(rt.Context(), "", &dbConfig)
	require.NoError(rt.TB(), err)

	metadataID, metadataIDError := rt.ServerContext().BootstrapContext.ComputeMetadataIDForDbConfig(base.TestCtx(rt.TB()), &dbConfig)
	require.NoError(rt.TB(), metadataIDError)

	badName := "badName"
	dbConfig.Bucket = &badName
	persistedConfig := rest.DatabaseConfig{
		Version:    version,
		MetadataID: metadataID,
		DbConfig:   dbConfig,
		SGVersion:  base.ProductVersion.String(),
	}
	// add the config to the other bucket
	rt.InsertDbConfigToBucket(&persistedConfig, tb2.GetName())

	// assert the config is picked as invalid db config
	err = rt.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt.ServerContext().AllInvalidDatabaseNames(t)
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

}

// TestConfigPollingRemoveDatabase:
//
//	Validates db is removed when polling detects that the config is not found
func TestConfigPollingRemoveDatabase(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyConfig)
	testCases := []struct {
		useXattrConfig bool
	}{
		{
			useXattrConfig: false,
		},
		{
			useXattrConfig: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("xattrConfig_%v", testCase.useXattrConfig), func(t *testing.T) {

			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				CustomTestBucket: base.GetTestBucket(t),
				PersistentConfig: true,
				MutateStartupConfig: func(config *rest.StartupConfig) {
					// configure the interval time to pick up new configs from the bucket to every 50 milliseconds
					config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
				},
				DatabaseConfig: nil,
				UseXattrConfig: testCase.useXattrConfig,
			})
			defer rt.Close()

			ctx := base.TestCtx(t)
			// create a new db
			dbName := "db1"
			dbConfig := rt.NewDbConfig()
			dbConfig.Name = dbName
			dbConfig.BucketConfig.Bucket = base.Ptr(rt.CustomTestBucket.GetName())
			resp := rt.CreateDatabase(dbName, dbConfig)
			rest.RequireStatus(t, resp, http.StatusCreated)

			// Validate that db is loaded
			_, err := rt.ServerContext().GetDatabase(ctx, dbName)
			require.NoError(t, err)

			// Force timeouts - dev-time only test enhancement to validate CBG-3947, requires manual "leaky bootstrap" handling
			// To enable:
			//  - Add "var ForceTimeouts bool" to bootstrap.go
			//  - In CouchbaseCluster.GetMetadataDocument, add the following after loadConfig:
			//    	if ForceTimeouts {
			//			return 0, gocb.ErrTimeout
			//		}
			//  - enable the code block below
			/*
				base.ForceTimeouts = true

				// Wait to ensure database doesn't disappear
				err = rt.WaitForConditionWithOptions(func() bool {
					_, err := rt.ServerContext().GetActiveDatabase(dbName)
					return errors.Is(err, base.ErrNotFound)

				}, 200, 50)
				require.Error(t, err)

				base.ForceTimeouts = false
			*/

			// Delete the config directly
			rt.RemoveDbConfigFromBucket("db1", rt.CustomTestBucket.GetName())

			// assert that the database is unloaded
			err = rt.WaitForConditionWithOptions(func() bool {
				_, err := rt.ServerContext().GetActiveDatabase(dbName)
				return errors.Is(err, base.ErrNotFound)

			}, 200, 1000)
			require.NoError(t, err)

			// assert that a request to the database fails with correct error message
			resp = rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
			rest.RequireStatus(t, resp, http.StatusNotFound)
			assert.Contains(t, resp.Body.String(), "no such database")
		})
	}
}

func TestResyncStopUsingDCPStream(t *testing.T) {
	base.TestRequiresDCPResync(t)

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

	numOfDocs := 1000
	// gsi is slower than views, so update the number of documents for views so stopping the resync will not complete
	if base.TestsDisableGSI() {
		numOfDocs = 5000
	}
	for i := range numOfDocs {
		rt.CreateTestDoc(fmt.Sprintf("doc%d", i))
	}

	rt.WaitForPendingChanges()

	require.Equal(t, int64(numOfDocs), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	rt.TakeDbOffline()

	response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)
	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rt.WaitForResyncDCPStatus(db.BackgroundProcessStateStopped)

	// make sure resync stopped before it completed
	require.Less(t, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()), numOfDocs*2)
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

	rt.WaitForDBOnline()

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

	rt.WaitForDBOnline()
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

	rt.WaitForDBOnline()
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

		rt.WaitForDBOnline()

		// Wait until after the 1 second delay, since the online request explicitly asked for a delay
		time.Sleep(1500 * time.Millisecond)

		rt.WaitForDBOnline()
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

	changes := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
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

	changes = rt.WaitForChanges(1, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
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
	assert.Equal(t, []interface{}{[]interface{}{"achannel"}}, syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test redacted
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/testdoc?redact=true&include_doc=false", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData = body[base.SyncPropertyName]
	require.NotNil(t, syncData)
	assert.NotEqual(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.NotEqual(t, []interface{}{[]interface{}{"achannel"}}, syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

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
	version := rest.DocVersionFromPutResponse(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+version.RevID+`"`)
	assert.Contains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_deleted":true`)

	// Delete the doc
	deletedVersion := rt.DeleteDoc(docID, version)

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+deletedVersion.RevID+`"`)
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
	dbConfig := &rest.DbConfig{BucketConfig: bucketConfig, SGReplicateEnabled: base.Ptr(false)}
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
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)

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
			MaxItemCount: base.Ptr(uint32(1337)), ShardCount: base.Ptr(uint16(7)),
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
	require.NotEqual(t, 0, respBody.HeapProfileCollectionThreshold)
}

func TestHandleGetRevTree(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.AllowConflicts = base.Ptr(true)

	rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

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
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{Users: map[string]*auth.PrincipalConfig{"alice": {Password: base.Ptr("password")}}}}})
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
	rt.Run("Update doc without changing channel", func(t *testing.T) {
		version := rt.PutDoc("replace", `{"chan":"`+chanName+`"}`) // init doc
		before := rt.GetDatabase().DbStats.Database().WarnChannelNameSizeCount.Value()
		_ = rt.UpdateDoc("replace", version, `{"chan":"`+chanName+`", "data":"test"}`)
		after := rt.GetDatabase().DbStats.Database().WarnChannelNameSizeCount.Value()
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
	rt.Run("Update doc with new channel", func(t *testing.T) {

		chanName := strings.Repeat("C", channelLength)
		version := rt.PutDoc("replaceNewChannel", `{"chan":"`+chanName+`"}`) // init doc
		before := rt.GetDatabase().DbStats.Database().WarnChannelNameSizeCount.Value()
		chanName = strings.Repeat("D", channelLength+5)
		_ = rt.UpdateDoc("replaceNewChannel", version, `{"chan":"`+chanName+`", "data":"test"}`)
		after := rt.GetDatabase().DbStats.Database().WarnChannelNameSizeCount.Value()
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
	rt.Run("Delete channel over max length", func(t *testing.T) {
		chanName := strings.Repeat("F", channelLength)
		version := rt.PutDoc("deleteme", `{"chan":"`+chanName+`"}`) // init channel
		before := rt.GetDatabase().DbStats.Database().WarnChannelNameSizeCount.Value()
		rt.DeleteDoc("deleteme", version)
		after := rt.GetDatabase().DbStats.Database().WarnChannelNameSizeCount.Value()
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
			base.ResetGlobalTestLogging(t)
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

			base.InitializeMemoryLoggers()
			tempDir := os.TempDir()
			test := rest.DefaultStartupConfig(tempDir)
			err := test.SetupAndValidateLogging(base.TestCtx(t))
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
	base.ResetGlobalTestLogging(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	base.InitializeMemoryLoggers()
	tempDir := os.TempDir()
	test := rest.DefaultStartupConfig(tempDir)
	err := test.SetupAndValidateLogging(base.TestCtx(t))
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
	dbConfig.RevsLimit = base.Ptr(uint32(100))

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	// Check that db revs limit is there now and error logging config
	assert.Contains(t, runtimeServerConfigResponse.Databases, "db")
	assert.Equal(t, base.Ptr(uint32(100)), runtimeServerConfigResponse.Databases["db"].RevsLimit)

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
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Get config
	resp := rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	eTag := resp.Header().Get("ETag")
	unquoteETag := strings.Trim(eTag, `"`)
	assert.NotEqual(t, "", unquoteETag)

	resp = rt.SendAdminRequestWithHeaders(http.MethodPost, "/db/_config", "{}", map[string]string{"If-Match": eTag})
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config", "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)
	putETag := resp.Header().Get("ETag")
	assert.NotEqual(t, "", putETag)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	getETag := resp.Header().Get("ETag")
	assert.Equal(t, putETag, getETag)

	quotedStr := `"x"`
	resp = rt.SendAdminRequestWithHeaders(http.MethodPost, "/db/_config", "{}", map[string]string{"If-Match": quotedStr})
	rest.RequireStatus(t, resp, http.StatusPreconditionFailed)
}

func TestDbConfigCredentials(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	var dbConfig rest.DatabaseConfig
	resp := rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &dbConfig))

	// non-runtime config, we don't expect to see any credentials present
	assert.Equal(t, "", dbConfig.Username)
	assert.Equal(t, "", dbConfig.Password)
	assert.Equal(t, "", dbConfig.CACertPath)
	assert.Equal(t, "", dbConfig.CertPath)
	assert.Equal(t, "", dbConfig.KeyPath)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &dbConfig))

	// runtime config, we expect to see the credentials used by the database (either bootstrap or per-db - but in this case, bootstrap)
	assert.Equal(t, base.TestClusterUsername(), dbConfig.Username)
	assert.Equal(t, base.RedactedStr, dbConfig.Password)
	assert.Equal(t, "", dbConfig.CACertPath)
	assert.Equal(t, "", dbConfig.CertPath)
	assert.Equal(t, "", dbConfig.KeyPath)
}

func TestInvalidDBConfig(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Put db config with invalid sync fn
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", `{"sync": "function(){"}`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body.String(), "invalid javascript syntax"))

	// Put invalid sync fn via sync specific endpoint
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/sync", `function(){`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body.String(), "invalid javascript syntax"))

	// Put invalid import fn via import specific endpoint
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/import_filter", `function(){`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body.String(), "invalid javascript syntax"))
}

func TestCreateDbOnNonExistentBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/", `{"bucket": "nonexistentbucket"}`)
	rest.RequireStatus(t, resp, http.StatusForbidden)
	assert.Contains(t, resp.Body.String(), "The specified bucket/scope/collection does not exist, or the provided credentials do not have access to it")
	resp = rt.SendAdminRequest(http.MethodPut, "/nonexistentbucket/", `{}`)
	rest.RequireStatus(t, resp, http.StatusForbidden)
	assert.Contains(t, resp.Body.String(), "The specified bucket/scope/collection does not exist, or the provided credentials do not have access to it")
}

func TestPutDbConfigChangeName(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_config", `{"name": "test"}`), http.StatusBadRequest)
}

func TestSwitchDbConfigCollectionName(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)

	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// put a doc in db
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/10001", `{"type":"test_doc"}`), http.StatusCreated)

	// update config to another collection
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.GetCollectionsConfig(t, rt.TestBucket, 2)
	rest.RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)

	// put doc in new collection
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace2}}/10001", `{"type":"test_doc1"}`), http.StatusCreated)

	// update back to original collection config
	rest.RequireStatus(t, rt.UpsertDbConfig("db", rt.NewDbConfig()), http.StatusCreated)

	// put doc in original collection name
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/100", `{"type":"test_doc1"}`), http.StatusCreated)
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
	base.ResetGlobalTestLogging(t)
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	ctx := base.TestCtx(t)
	// Get a test bucket, to use to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// Start SG with no databases
	config := rest.BootstrapStartupConfigForTest(t)
	config.Logging.Console.LogKeys = []string{base.KeyDCP.String()}
	config.Logging.Console.LogLevel.Set(base.LevelDebug)
	config.Logging.Console.Enabled = base.Ptr(true) // only necessary for tests since they avoid InitLogging, normally this is inferred by InitLogging

	sc, closeFn := rest.StartServerWithConfig(t, &config)
	defer closeFn()

	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	var dbConfig rest.DatabaseConfig
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/db/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err := base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	assert.NoError(t, err)

	// Validate a few default values to ensure they are set
	assert.Equal(t, channels.DocChannelsSyncFunction, *dbConfig.Sync)
	assert.Equal(t, db.DefaultChannelCacheMaxNumber, *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber)
	assert.Equal(t, base.DefaultOldRevExpirySeconds, *dbConfig.OldRevExpirySeconds)
	assert.Equal(t, false, *dbConfig.StartOffline)
	assert.Equal(t, db.DefaultCompactInterval, time.Duration(*dbConfig.CompactIntervalDays)*24*time.Hour)

	assert.Equal(t, dbConfig.Logging.Console.LogLevel.String(), base.LevelDebug.String())
	assert.Equal(t, dbConfig.Logging.Console.LogKeys, []string{base.KeyDCP.String()})

	var runtimeServerConfigResponse rest.RunTimeServerConfigResponse
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &runtimeServerConfigResponse)
	assert.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db")
	runtimeServerConfigDatabase := runtimeServerConfigResponse.Databases["db"]
	assert.Equal(t, channels.DocChannelsSyncFunction, *runtimeServerConfigDatabase.Sync)
	assert.Equal(t, db.DefaultChannelCacheMaxNumber, *runtimeServerConfigDatabase.CacheConfig.ChannelCacheConfig.MaxNumber)
	assert.Equal(t, base.DefaultOldRevExpirySeconds, *runtimeServerConfigDatabase.OldRevExpirySeconds)
	assert.Equal(t, false, *runtimeServerConfigDatabase.StartOffline)
	assert.Equal(t, db.DefaultCompactInterval, time.Duration(*runtimeServerConfigDatabase.CompactIntervalDays)*24*time.Hour)

	// Test unsupported options
	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db2/", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "unsupported": {"disable_clean_skipped_query": true}}`, tb2.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
	))
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &runtimeServerConfigResponse)
	assert.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db2")
	runtimeServerConfigDatabase = runtimeServerConfigResponse.Databases["db2"]
	assert.True(t, runtimeServerConfigDatabase.Unsupported.DisableCleanSkippedQuery)
}

func TestLegacyCredentialInheritance(t *testing.T) {
	rest.RequireBucketSpecificCredentials(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	// explicitly start with persistent config disabled
	sc, err := rest.SetupServerContext(ctx, &config, false)
	require.NoError(t, err)

	serverErr := make(chan error)

	closeFn := func() {
		sc.Close(ctx)
		assert.NoError(t, <-serverErr)
	}

	started := false
	defer func() {
		if !started {
			closeFn()
		}

	}()
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()

	require.NoError(t, sc.WaitForRESTAPIs(ctx))

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// No credentials should fail
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusForbidden)

	// Wrong credentials should fail
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db2/",
		`{"bucket": "`+tb.GetName()+`", "username": "test", "password": "invalid_password"}`,
	)
	resp.RequireStatus(http.StatusForbidden)

	// Proper credentials should pass
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db3/",
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
	dbConfig.StartOffline = base.Ptr(true)

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
	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn:           syncFunc,
		ImportFilter:     importFilter,
		PersistentConfig: true,
	})
	defer rt.Close()

	rest.RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	// Get config values before taking db offline, locally only
	resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	dbConfigBeforeOffline := resp.Body.String()

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/import_filter", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/sync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunc, resp.Body.String())

	// take db offline, locally online, not using rt.TakeDbOffline which will update the bucket configuration
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_offline", ""), http.StatusOK)

	require.Equal(t, "Offline", rt.GetDBState())

	// Check offline config matches online config
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, dbConfigBeforeOffline, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/import_filter", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/sync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunc, resp.Body.String())
}

// TestDbConfigPersistentSGVersions ensures that cluster-wide config updates are not applied to older nodes to avoid pushing invalid configuration.
func TestDbConfigPersistentSGVersions(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyConfig)

	// Start SG with no databases
	config := rest.BootstrapStartupConfigForTest(t)

	// enable the background update worker for this test only
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Millisecond * 250)

	sc, closeFn := rest.StartServerWithConfig(t, &config)
	ctx := base.TestCtx(t)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	dbName := "db"
	dbConfig := rest.DatabaseConfig{
		SGVersion: "", // leave empty to emulate what 3.0.0 would've written to the bucket
		DbConfig: rest.DbConfig{
			BucketConfig: rest.BucketConfig{
				Bucket: base.Ptr(tb.GetName()),
			},
			Name:         dbName,
			EnableXattrs: base.Ptr(base.TestUseXattrs()),
			UseViews:     base.Ptr(base.TestsDisableGSI()),
			AutoImport:   false, // starts faster without import feed, but will panic if turned on CBG-3455
			Index: &rest.IndexConfig{
				NumReplicas: base.Ptr(uint(0)),
			},
			RevsLimit: base.Ptr(uint32(123)), // use RevsLimit to detect config changes
		},
	}
	var err error
	dbConfig.Version, err = rest.GenerateDatabaseConfigVersionID(ctx, "", &dbConfig.DbConfig)
	require.NoError(t, err)

	groupID := sc.Config.Bootstrap.ConfigGroupID
	// initialise with db config
	_, err = sc.BootstrapContext.InsertConfig(ctx, tb.GetName(), groupID, &dbConfig)
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
		_, err = sc.BootstrapContext.UpdateConfig(base.TestCtx(t), tb.GetName(), groupID, dbName, func(db *rest.DatabaseConfig) (updatedConfig *rest.DatabaseConfig, err error) {

			db.SGVersion = version
			db.DbConfig.RevsLimit = base.Ptr(revsLimit)
			db.Version, err = rest.GenerateDatabaseConfigVersionID(ctx, db.Version, &db.DbConfig)
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
	closeFn()

	// Start a new SG node and ensure we *can* load the "newer" config version on initial startup, to support downgrade
	sc, closeFn = rest.StartServerWithConfig(t, &config)
	defer closeFn()

	assertRevsLimit(sc, 654)

	// overwrite new config back to current version post-downgrade
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "3.1.0", 321))
	assertRevsLimit(sc, 321)
}

func TestDeleteFunctionsWhileDbOffline(t *testing.T) {

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			PersistentConfig: true,
			SyncFn:           "function(doc){ throw({forbidden : \"Rejected document\"}) }",
			ImportFilter:     "function(doc) { return false }",
		},
	)
	defer rt.Close()

	rt.CreateDatabase("db", rt.NewDbConfig())

	// Make sure import and sync fail
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/TestSyncDoc", "{}"), http.StatusForbidden)

	rt.TakeDbOffline()

	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/_config/sync", ""), http.StatusOK)

	rt.TakeDbOnline()

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/sync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/TestSyncDoc", "{}"), http.StatusCreated)

	if base.TestUseXattrs() {
		// default data store - we're not using a named scope/collection in this test
		add, err := rt.GetSingleDataStore().Add("TestImportDoc", 0, db.Document{ID: "TestImportDoc", RevID: "1-abc"})
		require.NoError(t, err)
		require.Equal(t, true, add)

		// On-demand import - rejected doc
		rest.RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/TestImportDoc", ""), http.StatusNotFound)

		// Persist configs
		rest.RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/_config/import_filter", ""), http.StatusOK)

		// Check configs match
		resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/import_filter", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		require.Equal(t, "", resp.Body.String())

		// On-demand import - allowed doc after restored default import filter
		rest.RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/TestImportDoc", ""), http.StatusOK)
	}
}

func TestSetFunctionsWhileDbOffline(t *testing.T) {
	importFilter := "function(doc){ return true; }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	rt.TakeDbOffline()

	// Persist configs
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/import_filter", importFilter), http.StatusOK)

	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/sync", syncFunc), http.StatusOK)

	rt.TakeDbOffline()

	// Check configs match
	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/import_filter", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_config/sync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunc, resp.Body.String())
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
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	// db put with empty sync func and import filter
	resp := rt.SendAdminRequest(http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			rt.Bucket().GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// db config put with empty sync func and import filter
	resp = rt.SendAdminRequest(http.MethodPut, "/db/_config",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			rt.Bucket().GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// db config post, with empty sync func and import filter
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			rt.Bucket().GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	rest.RequireStatus(t, resp, http.StatusCreated)
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

	config.DisablePasswordAuth = base.Ptr(true)
	res = rt.UpsertDbConfig(dbName, config)
	rest.RequireStatus(t, res, http.StatusCreated)
	assert.True(t, rt.GetDatabase().Options.DisablePasswordAuthentication)

	config.DisablePasswordAuth = base.Ptr(false)
	res = rt.UpsertDbConfig(dbName, config)
	rest.RequireStatus(t, res, http.StatusCreated)
	assert.False(t, rt.GetDatabase().Options.DisablePasswordAuthentication)
}

// CBG-1790: Deleting a database that targets the same bucket as another causes a panic in legacy
func TestDeleteDatabasePointingAtSameBucket(t *testing.T) {
	if !base.TestUseXattrs() {
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
	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()
	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close(ctx)
	}()

	dbConfig := `{
   "bucket": "` + tb.GetName() + `",
   "name": "%s",
   "import_docs": true,
   "enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
   "use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
   "num_index_replicas": 0 }`

	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/", fmt.Sprintf(dbConfig, "db1"))
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodDelete, "/db1/", "")
	resp.RequireStatus(http.StatusOK)

	// Make another database that uses import in-order to trigger the panic instantly instead of having to time.Sleep
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db2/", fmt.Sprintf(dbConfig, "db2"))
	resp.RequireStatus(http.StatusCreated)

	scopeName := ""
	collectionNames := []string{}
	// Validate that deleted database is no longer in dest factory set
	_, fetchDb1DestErr := base.FetchDestFactory(base.ImportDestKey("db1", scopeName, collectionNames))
	assert.Equal(t, base.ErrNotFound, fetchDb1DestErr)
	_, fetchDb2DestErr := base.FetchDestFactory(base.ImportDestKey("db2", scopeName, collectionNames))
	assert.NoError(t, fetchDb2DestErr)
}

func BootstrapWaitForDatabaseState(t *testing.T, sc *rest.ServerContext, dbName string, state uint32) {
	err := base.WaitForNoError(base.TestCtx(t), func() error {
		resp := rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+dbName+"/", "")
		if resp.StatusCode() != http.StatusOK {
			return errors.New("expected 200 status")
		}
		var rootResp rest.DatabaseRoot
		require.NoError(t, json.Unmarshal([]byte(resp.Body), &rootResp))
		if rootResp.State != db.RunStateString[state] {
			return errors.New("expected db to be offline")
		}
		return nil
	})
	require.NoError(t, err)
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
			expectedErrorStatus: base.Ptr(http.StatusBadRequest),
		},
		{
			name:                "Valid _sync",
			inputBody:           map[string]interface{}{"_sync": db.SyncData{}},
			expectedErrorStatus: base.Ptr(http.StatusBadRequest),
		},
		{
			name:                        "Valid _deleted",
			inputBody:                   map[string]interface{}{"_deleted": false},
			skipDocContentsVerification: base.Ptr(true),
		},
		{
			name:                        "Valid _revisions",
			inputBody:                   map[string]interface{}{"_revisions": map[string]interface{}{"ids": "1-abc"}},
			skipDocContentsVerification: base.Ptr(true),
		},
		{
			name:                        "Valid _exp",
			inputBody:                   map[string]interface{}{"_exp": "123"},
			skipDocContentsVerification: base.Ptr(true),
		},
		{
			name:                "Invalid _exp",
			inputBody:           map[string]interface{}{"_exp": "abc"},
			expectedErrorStatus: base.Ptr(http.StatusBadRequest),
		},
		{
			name:                "_purged",
			inputBody:           map[string]interface{}{"_purged": false},
			expectedErrorStatus: base.Ptr(http.StatusBadRequest),
		},
		{
			name:                "_removed",
			inputBody:           map[string]interface{}{"_removed": false},
			expectedErrorStatus: base.Ptr(http.StatusNotFound),
		},
		{
			name:                "_sync_cookies",
			inputBody:           map[string]interface{}{"_sync_cookies": true},
			expectedErrorStatus: base.Ptr(http.StatusBadRequest),
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
			body := rt.GetDocBody(docID)
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
	version := rt.PutDoc("doc", "{}")

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := test.docID
			docRev := test.rev
			docBody := test.docBody
			if test.docID == "" {
				docID = "doc"                                                 // Used for the rev tests to branch off of
				docBody = strings.ReplaceAll(docBody, "[REV]", version.RevID) // FIX for HLV?
				docRev = strings.ReplaceAll(docRev, "[REV]", version.RevID)
			}

			resp := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, docRev), docBody)
			if test.expectError {
				rest.RequireStatus(t, resp, 400)
				return
			}
			rest.RequireStatus(t, resp, 201)
			if test.docID == "" {
				// Update rev to branch off for next test
				version = rest.DocVersionFromPutResponse(t, resp)
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
	assert.NoError(t, resp.Body.Close())

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
	serverPurgeInterval, err := cbStore.MetadataPurgeInterval(ctx)
	require.NoError(t, err)
	// Set server purge interval back to what it was for bucket reuse
	defer setServerPurgeInterval(t, rt, fmt.Sprintf("%.2f", serverPurgeInterval.Hours()/24))
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Set intervals on server and client
			setServerPurgeInterval(t, rt, test.newServerInterval)

			// Start compact to modify purge interval
			database, _ := db.GetDatabase(dbc, nil)
			_, err = database.Compact(ctx, false, nil, base.NewSafeTerminator(), false)
			require.NoError(t, err)

			assert.EqualValues(t, test.expectedPurgeIntervalAfterCompact, dbc.GetMetadataPurgeInterval(ctx))
		})
	}
}

// Make sure per DB credentials override per bucket credentials
func TestPerDBCredsOverride(t *testing.T) {
	rest.RequireBucketSpecificCredentials(t)

	ctx := base.TestCtx(t)
	// Get test bucket
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)

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

	sc, closeFn := rest.StartServerWithConfig(t, &config)
	defer closeFn()

	bootstrapConnection, err := rest.CreateBootstrapConnectionFromStartupConfig(ctx, sc.Config, base.PerUseClusterConnections)
	require.NoError(t, err)
	sc.BootstrapContext.Connection = bootstrapConnection

	dbConfig := `{
		"bucket": "` + tb1.GetName() + `",
		"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
		"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
		"num_index_replicas": 0
	}`

	res := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db/", dbConfig)
	// Make sure request failed as it could authenticate with the bucket
	assert.Equal(t, http.StatusForbidden, res.StatusCode())

	// Allow database to be created successfully
	sc.Config.DatabaseCredentials = map[string]*base.CredentialsConfig{}
	res = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db/", dbConfig)
	assert.Equal(t, http.StatusCreated, res.StatusCode())

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

func TestDatabaseCreationErrorCode(t *testing.T) {
	for _, persistentConfig := range []bool{true, false} {
		rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: persistentConfig})
		defer rt.Close()

		rt.CreateDatabase("db", rt.NewDbConfig())
		resp := rt.SendAdminRequest(http.MethodPut, "/db/", `{"bucket": "irrelevant"}`)
		rest.RequireStatus(t, resp, http.StatusPreconditionFailed)
	}
}

// TestDatabaseCreationWithEnvVariable:
//   - Create rest tester that enables admin auth and disallows db config env vars
//   - Create CBS user to authenticate with over admin port to force auth scope callback call
//   - Create db with sync function that calls env variable
//   - Assert that db is created
func TestDatabaseCreationWithEnvVariable(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer tb.Close(ctx)

	// disable AllowDbConfigEnvVars to avoid attempting to expand variables + enable admin auth
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			config.Unsupported.AllowDbConfigEnvVars = base.Ptr(false)
		},
		AdminInterfaceAuthentication: true,
		SyncFn:                       `function (doc) { console.log("${environment}"); return true }`,
		CustomTestBucket:             tb,
	})
	defer rt.Close()

	// create a role to authenticate with in admin endpoint
	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)
	rest.MakeUser(t, httpClient, eps[0], rest.MobileSyncGatewayRole.RoleName, "password", []string{fmt.Sprintf("%s[%s]", rest.MobileSyncGatewayRole.RoleName, tb.GetName())})
	defer rest.DeleteUser(t, httpClient, eps[0], rest.MobileSyncGatewayRole.RoleName)

	cfg := rt.NewDbConfig()
	input, err := base.JSONMarshal(&cfg)
	require.NoError(t, err)

	// create db with config and assert it is successful
	resp := rt.SendAdminRequestWithAuth(http.MethodPut, "/db/", string(input), rest.MobileSyncGatewayRole.RoleName, "password")
	rest.RequireStatus(t, resp, http.StatusCreated)
}

func TestDatabaseCreationWithEnvVariableWithBackticks(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer tb.Close(ctx)

	// disable AllowDbConfigEnvVars to avoid attempting to expand variables + enable admin auth
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			config.Unsupported.AllowDbConfigEnvVars = base.Ptr(false)
		},
		AdminInterfaceAuthentication: true,
		SyncFn:                       `function (doc) { console.log("${environment}"); return true }`,
		CustomTestBucket:             tb,
	})
	defer rt.Close()

	// create a role to authenticate with in admin endpoint
	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)
	rest.MakeUser(t, httpClient, eps[0], rest.MobileSyncGatewayRole.RoleName, "password", []string{fmt.Sprintf("%s[%s]", rest.MobileSyncGatewayRole.RoleName, tb.GetName())})
	defer rest.DeleteUser(t, httpClient, eps[0], rest.MobileSyncGatewayRole.RoleName)

	cfg := rt.NewDbConfig()
	input, err := base.JSONMarshal(&cfg)
	require.NoError(t, err)

	// change config to include backticks
	cfg.Bucket = base.Ptr(fmt.Sprintf("`"+"%s"+"`", tb.GetName()))

	// create db with config and assert it is successful
	resp := rt.SendAdminRequestWithAuth(http.MethodPut, "/backticks/", string(input), rest.MobileSyncGatewayRole.RoleName, "password")
	rest.RequireStatus(t, resp, http.StatusCreated)
}

func TestDatabaseConfigAuditAPI(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("Audit logging is an EE-only feature")
	}

	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// check default audit config - verbose to read event names, etc.
	resp := rt.SendAdminRequest(http.MethodGet, "/db/_config/audit?verbose=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	resp.DumpBody()
	var responseBody map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &responseBody))
	assert.Equal(t, false, responseBody["enabled"].(bool))
	// check we got the verbose output
	assert.NotEmpty(t, responseBody["events"].(map[string]interface{})[base.AuditIDPublicUserAuthenticated.String()].(map[string]interface{})["description"].(string), "expected verbose output (event description, etc.)")
	// check that global event IDs were not present
	assert.Nil(t, responseBody["events"].(map[string]interface{})[base.AuditIDSyncGatewayCollectInfoStart.String()], "expected global event ID to not be present")

	// enable auditing on the database (upsert)
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", `{"enabled":true}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	// check audit config
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config/audit", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	resp.DumpBody()
	responseBody = nil
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &responseBody))
	assert.Equal(t, true, responseBody["enabled"].(bool))
	eventsMap, ok := responseBody["events"].(map[string]interface{})
	require.True(t, ok)
	assert.False(t, eventsMap[base.AuditIDISGRStatus.String()].(bool), "audit enabled event should be disabled by default")
	assert.True(t, eventsMap[base.AuditIDPublicUserAuthenticated.String()].(bool), "public user authenticated event should be enabled by default")

	// use event IDs returned from GET response to disable all of them
	for id := range eventsMap {
		eventsMap[id] = false
	}
	eventsJSON, err := json.Marshal(eventsMap)
	require.NoError(t, err)

	// CBG-4111: Try to disable events on top of the default (nil) set... either PUT or POST where *all* of the given IDs are set to false. Bug results in a no-op.
	// CBG-4157: Ensure ALL specified events were actually disabled. Bug results in some events remaining 'true', and sometimes panicking by going out-of-bounds in a slice.
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":%s}`, eventsJSON))
	rest.RequireStatus(t, resp, http.StatusOK)
	// check all events were actually disabled
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config/audit", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	resp.DumpBody()
	responseBody = nil
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &responseBody))
	eventsMap, ok = responseBody["events"].(map[string]interface{})
	require.True(t, ok)
	for id, val := range eventsMap {
		assert.False(t, val.(bool), "event %s should be disabled", id)
	}

	// do a PUT to completely replace the full config (events not declared here will be disabled)
	// enable AuditEnabled event, but implicitly others
	resp = rt.SendAdminRequest(http.MethodPut, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":true}}`, base.AuditIDISGRStatus))
	rest.RequireStatus(t, resp, http.StatusOK)

	// check audit config
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config/audit", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	resp.DumpBody()
	responseBody = nil
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &responseBody))
	assert.Equal(t, true, responseBody["enabled"].(bool))
	assert.True(t, responseBody["events"].(map[string]interface{})[base.AuditIDISGRStatus.String()].(bool), "audit enabled event should've been enabled via PUT")
	assert.False(t, responseBody["events"].(map[string]interface{})[base.AuditIDPublicUserAuthenticated.String()].(bool), "public user authenticated event should've been disabled via PUT")

	// Verify the audit config on the database
	runtimeConfig := rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 1)
	RequireEventCount(t, runtimeConfig, base.AuditIDPublicUserAuthenticated, 0)

	// Repeat the PUT for the same event, ensure the event isn't duplicated
	resp = rt.SendAdminRequest(http.MethodPut, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":true}}`, base.AuditIDISGRStatus))
	rest.RequireStatus(t, resp, http.StatusOK)

	// Verify the audit config on the database hasn't changed
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 1)
	RequireEventCount(t, runtimeConfig, base.AuditIDPublicUserAuthenticated, 0)

	// Perform a POST for the same event, ensure it's not duplicated
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":true}}`, base.AuditIDISGRStatus))
	rest.RequireStatus(t, resp, http.StatusOK)
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 1)
	RequireEventCount(t, runtimeConfig, base.AuditIDPublicUserAuthenticated, 0)

	// Perform a POST for another event, ensure previous POST is retained
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":true}}`, base.AuditIDAttachmentCreate))
	rest.RequireStatus(t, resp, http.StatusOK)
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 1)
	RequireEventCount(t, runtimeConfig, base.AuditIDAttachmentCreate, 1)

	// Perform a POST to disable an event
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":false}}`, base.AuditIDAttachmentCreate))
	rest.RequireStatus(t, resp, http.StatusOK)
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 1)
	RequireEventCount(t, runtimeConfig, base.AuditIDAttachmentCreate, 0)

	// Duplicate the  POST to disable an event
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":false}}`, base.AuditIDAttachmentCreate))
	rest.RequireStatus(t, resp, http.StatusOK)
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 1)
	RequireEventCount(t, runtimeConfig, base.AuditIDAttachmentCreate, 0)

	// Try disabling a non-filterable event
	// We don't currently have a non-filterable database event, but this would be where we'd put one to test if we did!
	if false { // nolint
		resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"enabled": true,"events":{"%s":false}}"`, base.AuditIDAuditEnabled))
		rest.RequireStatus(t, resp, http.StatusBadRequest)
		assert.Contains(t, resp.Body.String(), "couldn't update audit configuration")
		assert.Contains(t, resp.Body.String(), fmt.Sprintf(`event \"%s\" is not filterable`, base.AuditIDAuditEnabled))
	}

	// Re-enable the event via PUT, should remove other events
	resp = rt.SendAdminRequest(http.MethodPut, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":true}}`, base.AuditIDAttachmentCreate))
	rest.RequireStatus(t, resp, http.StatusOK)
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 0)
	RequireEventCount(t, runtimeConfig, base.AuditIDAttachmentCreate, 1)

	// Remove the only event via PUT
	resp = rt.SendAdminRequest(http.MethodPut, "/db/_config/audit", fmt.Sprintf(`{"enabled":true,"events":{"%s":false}}`, base.AuditIDAttachmentCreate))
	rest.RequireStatus(t, resp, http.StatusOK)
	runtimeConfig = rt.RestTesterServerContext.GetDatabaseConfig("db")
	RequireEventCount(t, runtimeConfig, base.AuditIDISGRStatus, 0)
	RequireEventCount(t, runtimeConfig, base.AuditIDAttachmentCreate, 0)

	// Set a non-existent audit ID
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", `{"events":{"123":true}}`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), `unknown audit event ID: \"123\"`)

	// Set a global-only audit ID
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", fmt.Sprintf(`{"events":{"%s":true}}`, base.AuditIDSyncGatewayCollectInfoStart))
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), fmt.Sprintf(`event \"%s\" is not configurable at the database level`, base.AuditIDSyncGatewayCollectInfoStart))
}

func RequireEventCount(t *testing.T, runtimeConfig *rest.RuntimeDatabaseConfig, auditID base.AuditID, expectedCount int) {
	require.NotNil(t, runtimeConfig)

	loggingConfig := runtimeConfig.DbConfig.Logging
	if loggingConfig == nil || loggingConfig.Audit == nil {
		require.Zero(t, expectedCount)
	}
	actualCount := 0
	if loggingConfig.Audit.EnabledEvents != nil {
		for _, configID := range *loggingConfig.Audit.EnabledEvents {
			if configID == uint(auditID) {
				actualCount++
			}
		}
	}
	require.Equal(t, expectedCount, actualCount)
}

//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
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
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

// Reproduces #3048 Panic when attempting to make invalid update to a conflicting document
func TestNoPanicInvalidUpdate(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	docId := "conflictTest"

	// Create doc
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", docId), `{"value":"initial"}`)
	response.DumpBody()

	assertStatus(t, response, http.StatusCreated)

	// Discover revision ID
	// TODO: The schema for SG responses should be defined in our code somewhere to avoid this clunky approach
	var responseDoc map[string]interface{}
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId := responseDoc["rev"].(string)
	revGeneration, revIdHash := db.ParseRevID(revId)
	goassert.Equals(t, revGeneration, 1)

	// Update doc (normal update, no conflicting revisions added)
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", docId), fmt.Sprintf(`{"value":"secondval", db.BodyRev:"%s"}`, revId))
	response.DumpBody()

	// Create conflict
	input := fmt.Sprintf(`
                  {"value": "conflictval",
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "%s"]}}`, revIdHash)

	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(revId)
	goassert.Equals(t, revGeneration, 2)

	// Create conflict again, should be a no-op and return the same response as previous attempt
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(revId)
	goassert.Equals(t, revGeneration, 2)

}

func TestUserAPI(t *testing.T) {

	// PUT a user
	rt := NewRestTester(t, nil)
	defer rt.Close()

	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/snej", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// GET the user and make sure the result is OK
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.Equals(t, body["name"], "snej")
	goassert.Equals(t, body["email"], "jens@couchbase.com")
	goassert.DeepEquals(t, body["admin_channels"], []interface{}{"bar", "foo"})
	goassert.DeepEquals(t, body["all_channels"], []interface{}{"!", "bar", "foo"})
	goassert.Equals(t, body["password"], nil)

	// Check the list of all users:
	response = rt.SendAdminRequest("GET", "/db/_user/", "")
	assertStatus(t, response, 200)
	goassert.Equals(t, string(response.Body.Bytes()), `["snej"]`)

	// Check that the actual User object is correct:
	user, _ := rt.ServerContext().Database("db").Authenticator().GetUser("snej")
	goassert.Equals(t, user.Name(), "snej")
	goassert.Equals(t, user.Email(), "jens@couchbase.com")
	goassert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet{"bar": channels.NewVbSimpleSequence(0x1), "foo": channels.NewVbSimpleSequence(0x1)})
	goassert.True(t, user.Authenticate("letmein"))

	// Change the password and verify it:
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"123", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 200)

	user, _ = rt.ServerContext().Database("db").Authenticator().GetUser("snej")
	goassert.True(t, user.Authenticate("123"))

	// DELETE the user
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/snej", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/snej", ""), 404)

	// POST a user
	response = rt.SendAdminRequest("POST", "/db/_user", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 301)
	rt.Bucket().Dump()

	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.Equals(t, body["name"], "snej")

	// Create a role
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response = rt.SendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)

	// Give the user that role
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"admin_channels":["foo", "bar"],"admin_roles":["hipster"]}`)
	assertStatus(t, response, 200)

	// GET the user and verify that it shows the channels inherited from the role
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body["admin_roles"], []interface{}{"hipster"})
	goassert.DeepEquals(t, body["all_channels"], []interface{}{"!", "bar", "fedoras", "fixies", "foo"})

	// DELETE the user
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/snej", ""), 200)

	// POST a user with URL encoded '|' in name see #2870
	assertStatus(t, rt.SendAdminRequest("POST", "/db/_user/", `{"name":"0%7C59", "password":"letmein", "admin_channels":["foo", "bar"]}`), 201)

	// GET the user, will fail
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%7C59", ""), 404)

	// DELETE the user, will fail
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%7C59", ""), 404)

	// GET the user, double escape username, will succeed
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%257C59", ""), 200)

	// DELETE the user, double escae usename, will succeed
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%257C59", ""), 200)

	// POST a user with URL encoded '|' and unencoded @ in name see #2870
	assertStatus(t, rt.SendAdminRequest("POST", "/db/_user/", `{"name":"0%7C@59", "password":"letmein", "admin_channels":["foo", "bar"]}`), 201)

	// GET the user, will fail
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%7C@59", ""), 404)

	// DELETE the user, will fail
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%7C@59", ""), 404)

	// GET the user, double escape username, will succeed
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%257C%4059", ""), 200)

	// DELETE the user, double escae usename, will succeed
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%257C%4059", ""), 200)

}

func TestUserPasswordValidation(t *testing.T) {

	// PUT a user
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// PUT a user without a password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", `{"email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// POST a user without a password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ajresnopassword", "email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// PUT a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", `{"email":"ajres@couchbase.com", "password":"in", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// POST a user with a two character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ajresnopassword", "email":"ajres@couchbase.com", "password":"an", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// PUT a user with a zero character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", `{"email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// POST a user with a zero character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ajresnopassword", "email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// PUT update a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"an"}`)
	assertStatus(t, response, 400)

	// PUT update a user with a one character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"a"}`)
	assertStatus(t, response, 400)

	// PUT update a user with a zero character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":""}`)
	assertStatus(t, response, 400)

	// PUT update a user with a three character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"abc"}`)
	assertStatus(t, response, 200)
}

func TestUserAllowEmptyPassword(t *testing.T) {

	// PUT a user
	rt := NewRestTester(t, &RestTesterConfig{DatabaseConfig: &DbConfig{AllowEmptyPassword: true}})
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// PUT a user without a password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword1", `{"email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// POST a user without a password, should succeed
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword2", "email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// PUT a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword3", `{"email":"ajres@couchbase.com", "password":"in", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// POST a user with a two character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword4", "email":"ajres@couchbase.com", "password":"an", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 400)

	// PUT a user with a zero character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword5", `{"email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// POST a user with a zero character password, should succeed
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword6", "email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// PUT update a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"an"}`)
	assertStatus(t, response, 400)

	// PUT update a user with a one character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"a"}`)
	assertStatus(t, response, 400)

	// PUT update a user with a zero character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":""}`)
	assertStatus(t, response, 200)

	// PUT update a user with a three character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"abc"}`)
	assertStatus(t, response, 200)
}

// Test user access grant while that user has an active changes feed.  (see issue #880)
func TestUserAccessRace(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// This test only runs against Walrus due to known sporadic failures.
	// See https://github.com/couchbase/sync_gateway/issues/3006
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip this test under integration testing")
	}

	syncFunction := `
function(doc, oldDoc) {
  if (doc.type == "list") {
    channel("list-"+doc._id);
  } else if (doc.type == "profile") {
    channel("profiles");
    var user = doc._id.substring(doc._id.indexOf(":")+1);
    if (user !== doc.user_id) {
      throw({forbidden : "profile user_id must match docid"})
    }
    requireUser(user);
    access(user, "profiles");
    channel('profile-'+user);
  } else if (doc.type == "Want") {
    var parts = doc._id.split("-");
    var user = parts[1];
    var i = parts[2];
    requireUser(user);
    channel('profile-'+user);
    access(user, 'list-'+i);
  }
}

`
	rtConfig := RestTesterConfig{SyncFn: syncFunction}
	var rt = NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["profile-bernard"]}`)
	assertStatus(t, response, 201)

	//Try to force channel initialisation for user bernard
	response = rt.SendAdminRequest("GET", "/db/_user/bernard", "")
	assertStatus(t, response, 200)

	// Create list docs
	input := `{"docs": [`

	for i := 1; i <= 100; i++ {
		if i > 1 {
			input = input + `,`
		}
		docId := fmt.Sprintf("l_%d", i)
		input = input + fmt.Sprintf(`{"_id":"%s", "type":"list"}`, docId)
	}
	input = input + `]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", input)

	// Start changes feed
	var wg sync.WaitGroup

	// Init the public handler, to avoid data race initializing in the two usages below.
	_ = rt.SendRequest("GET", "/db", "")

	wg.Add(1)

	numExpectedChanges := 201

	go func() {
		defer wg.Done()

		since := ""

		maxTries := 10
		numTries := 0

		changesAccumulated := []db.ChangeEntry{}

		for {

			// Timeout allows us to read continuous changes after processing is complete.  Needs to be long enough to
			// ensure it doesn't terminate before the first change is sent.
			log.Printf("Invoking _changes?feed=continuous&since=%s&timeout=2000", since)
			changesResponse := rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?feed=continuous&since=%s&timeout=2000", since), "", "bernard"))

			changes, err := readContinuousChanges(changesResponse)
			assert.NoError(t, err)

			changesAccumulated = append(changesAccumulated, changes...)

			if len(changesAccumulated) >= numExpectedChanges {
				log.Printf("Got numExpectedChanges (%d).  Done", numExpectedChanges)
				break
			} else {
				log.Printf("Only received %d out of %d expected changes.  Attempt %d / %d.", len(changesAccumulated), numExpectedChanges, numTries, maxTries)
			}

			// Advance the since value if we got any changes
			if len(changes) > 0 {
				since = changes[len(changes)-1].Seq.String()
				log.Printf("Setting since value to: %s.", since)
			}

			numTries++
			if numTries > maxTries {
				t.Fatalf("Giving up trying to receive %d changes.  Only received %d", numExpectedChanges, len(changesAccumulated))
			}

		}

	}()

	// Make bulk docs calls, 100 docs each, all triggering access grants to the list docs.
	for j := 0; j < 1; j++ {

		input := `{"docs": [`
		for i := 1; i <= 100; i++ {
			if i > 1 {
				input = input + `,`
			}
			k := j*100 + i
			docId := fmt.Sprintf("Want-bernard-l_%d", k)
			input = input + fmt.Sprintf(`{"_id":"%s", "type":"Want", "owner":"bernard"}`, docId)
		}
		input = input + `]}`

		log.Printf("Sending 2nd round of _bulk_docs")
		response = rt.Send(requestByUser("POST", "/db/_bulk_docs", input, "bernard"))
		log.Printf("Sent 2nd round of _bulk_docs")

	}

	// wait for changes feed to complete (time out)
	wg.Wait()
}

func TestLoggingKeys(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	//Assert default log channels are enabled
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	goassert.DeepEquals(t, logKeys, map[string]interface{}{})

	//Set logKeys, Changes+ should enable Changes (PUT replaces any existing log keys)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{"Changes+":true, "Cache":true, "HTTP":true}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var updatedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &updatedLogKeys))
	goassert.DeepEquals(t, updatedLogKeys, map[string]interface{}{"Changes": true, "Cache": true, "HTTP": true})

	//Disable Changes logKey which should also disable Changes+
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var deletedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &deletedLogKeys))
	goassert.DeepEquals(t, deletedLogKeys, map[string]interface{}{"Cache": true, "HTTP": true})

	//Enable Changes++, which should enable Changes (POST append logKeys)
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var appendedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &appendedLogKeys))
	goassert.DeepEquals(t, appendedLogKeys, map[string]interface{}{"Changes": true, "Cache": true, "HTTP": true})

	//Disable Changes++ (POST modifies logKeys)
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabledLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabledLogKeys))
	goassert.DeepEquals(t, disabledLogKeys, map[string]interface{}{"Cache": true, "HTTP": true})

	//Re-Enable Changes++, which should enable Changes (POST append logKeys)
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	//Disable Changes+ which should disable Changes (POST modifies logKeys)
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes+":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabled2LogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabled2LogKeys))
	goassert.DeepEquals(t, disabled2LogKeys, map[string]interface{}{"Cache": true, "HTTP": true})

	//Re-Enable Changes++, which should enable Changes (POST append logKeys)
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	//Disable Changes (POST modifies logKeys)
	assertStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabled3LogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabled3LogKeys))
	goassert.DeepEquals(t, disabled3LogKeys, map[string]interface{}{"Cache": true, "HTTP": true})

	//Disable all logKeys by using PUT with an empty channel list
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var noLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &noLogKeys))
	goassert.DeepEquals(t, noLogKeys, map[string]interface{}{})
}

func TestLoggingLevels(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Log keys should be blank
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]bool
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	goassert.DeepEquals(t, logKeys, map[string]bool{})

	// Set log level via logLevel query parameter
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=error", ``), http.StatusOK)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=invalidLogLevel", ``), http.StatusBadRequest)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=", ``), http.StatusBadRequest)

	// Set log level via old level query parameter
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=1", ``), http.StatusOK)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=2", ``), http.StatusOK)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=3", ``), http.StatusOK)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=10", ``), http.StatusOK) // Value is clamped to acceptable range, without returning an error

	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=0", ``), http.StatusBadRequest) // Zero-value is ignored and body is to be parsed
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=0", `{}`), http.StatusOK)       // Zero-value is ignored and body is to be parsed

	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=invalidLogLevel", ``), http.StatusBadRequest)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=", ``), http.StatusBadRequest)

	// Trying to set log level via the body will not work (the endpoint expects a log key map)
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{"logLevel": "debug"}`), http.StatusBadRequest)
}

func TestLoggingCombined(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Log keys should be blank
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]bool
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	goassert.DeepEquals(t, logKeys, map[string]bool{})

	// Set log keys and log level in a single request
	assertStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=trace", `{"Changes":true, "Cache":true, "HTTP":true}`), http.StatusOK)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	goassert.DeepEquals(t, logKeys, map[string]bool{"Changes": true, "Cache": true, "HTTP": true})
}

func TestGetStatus(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendRequest("GET", "/_status", "")
	assertStatus(t, response, 404)

	response = rt.SendAdminRequest("GET", "/_status", "")
	assertStatus(t, response, 200)
	var responseBody Status
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)

	goassert.Equals(t, responseBody.Version, base.LongVersionString)

	response = rt.SendAdminRequest("OPTIONS", "/_status", "")
	assertStatus(t, response, 204)
	goassert.Equals(t, response.Header().Get("Allow"), "GET")
}

// Test user delete while that user has an active changes feed (see issue 809)
func TestUserDeleteDuringChangesWithAccess(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges, base.KeyCache, base.KeyHTTP)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); if(doc.type == "setaccess") { access(doc.owner, doc.channel);}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["foo"]}`)
	assertStatus(t, response, 201)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		changesResponse := rt.Send(requestByUser("GET", "/db/_changes?feed=continuous&since=0&timeout=3000", "", "bernard"))
		// When testing single threaded, this reproduces the issue described in #809.
		// When testing multithreaded (-cpu 4 -race), there are three (valid) possibilities"
		// 1. The DELETE gets processed before the _changes auth completes: this will return 401
		// 2. The _changes request gets processed before the DELETE: the changes response will be closed when the user is deleted
		// 3. The DELETE is processed after the _changes auth completes, but before the MultiChangesFeed is instantiated.  The
		//  changes feed doesn't have a trigger to attempt a reload of the user in this scenario, so will continue until disconnected
		//  by the client.  This should be fixed more generally (to terminate all active user sessions when the user is deleted, not just
		//  changes feeds) but that enhancement is too high risk to introduce at this time.  The timeout on changes will terminate the unit
		//  test.
		if changesResponse.Code == 401 {
			// case 1 - ok
		} else {
			// case 2 - ensure no error processing the changes response.  The number of entries may vary, depending
			// on whether the changes loop performed an additional iteration before catching the deleted user.
			_, err := readContinuousChanges(changesResponse)
			assert.NoError(t, err)
		}
	}()

	// TODO: sleep required to ensure the changes feed iteration starts before the delete gets processed.
	time.Sleep(500 * time.Millisecond)
	rt.SendAdminRequest("PUT", "/db/bernard_doc1", `{"type":"setaccess", "owner":"bernard","channel":"foo"}`)
	rt.SendAdminRequest("DELETE", "/db/_user/bernard", "")
	rt.SendAdminRequest("PUT", "/db/manny_doc1", `{"type":"setaccess", "owner":"manny","channel":"bar"}`)
	rt.SendAdminRequest("PUT", "/db/bernard_doc2", `{"type":"general", "channel":"foo"}`)

	// case 3
	for i := 0; i <= 5; i++ {
		docId := fmt.Sprintf("/db/bernard_doc%d", i+3)
		response = rt.SendAdminRequest("PUT", docId, `{"type":"setaccess", "owner":"bernard", "channel":"foo"}`)
	}

	wg.Wait()
}

// Reads continuous changes feed response into slice of ChangeEntry
func readContinuousChanges(response *TestResponse) ([]db.ChangeEntry, error) {
	var change db.ChangeEntry
	changes := make([]db.ChangeEntry, 0)
	reader := bufio.NewReader(response.Body)
	for {
		entry, readError := reader.ReadBytes('\n')
		if readError == io.EOF {
			// done
			break
		}
		if readError != nil {
			// unexpected read error
			return changes, readError
		}
		entry = bytes.TrimSpace(entry)
		if len(entry) > 0 {
			err := base.JSONUnmarshal(entry, &change)
			if err != nil {
				return changes, err
			}
			changes = append(changes, change)
			log.Printf("Got change ==> %v", change)
		}

	}
	return changes, nil
}

func TestRoleAPI(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// PUT a role
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)

	// GET the role and make sure the result is OK
	response = rt.SendAdminRequest("GET", "/db/_role/hipster", "")
	assertStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.Equals(t, body["name"], "hipster")
	goassert.DeepEquals(t, body["admin_channels"], []interface{}{"fedoras", "fixies"})
	goassert.Equals(t, body["password"], nil)

	response = rt.SendAdminRequest("GET", "/db/_role/", "")
	assertStatus(t, response, 200)
	goassert.Equals(t, string(response.Body.Bytes()), `["hipster"]`)

	// DELETE the role
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)

	// POST a role
	response = rt.SendAdminRequest("POST", "/db/_role", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 301)
	response = rt.SendAdminRequest("POST", "/db/_role/", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/hipster", "")
	assertStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.Equals(t, body["name"], "hipster")
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)
}

func TestGuestUser(t *testing.T) {

	guestUserEndpoint := fmt.Sprintf("/db/_user/%s", base.GuestUsername)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodGet, guestUserEndpoint, "")
	assertStatus(t, response, http.StatusOK)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, base.GuestUsername, body["name"])
	// This ain't no admin-party, this ain't no nightclub, this ain't no fooling around:
	assert.Nil(t, body["admin_channels"])

	// Disable the guest user:
	response = rt.SendAdminRequest(http.MethodPut, guestUserEndpoint, `{"disabled":true}`)
	assertStatus(t, response, http.StatusOK)

	// Get guest user and verify it is now disabled:
	response = rt.SendAdminRequest(http.MethodGet, guestUserEndpoint, "")
	assertStatus(t, response, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, base.GuestUsername, body["name"])
	assert.True(t, body["disabled"].(bool))

	// Check that the actual User object is correct:
	user, _ := rt.ServerContext().Database("db").Authenticator().GetUser("")
	assert.Empty(t, user.Name())
	assert.Nil(t, user.ExplicitChannels())
	assert.True(t, user.Disabled())

	// We can't delete the guest user, but we should get a reasonable error back.
	response = rt.SendAdminRequest(http.MethodDelete, guestUserEndpoint, "")
	assertStatus(t, response, http.StatusMethodNotAllowed)
}

//Test that TTL values greater than the default max offset TTL 2592000 seconds are processed correctly
// fixes #974
func TestSessionTtlGreaterThan30Days(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	a := auth.NewAuthenticator(rt.Bucket(), nil)
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	user, err = a.GetUser("")
	assert.NoError(t, err)
	goassert.True(t, user.Disabled())

	response := rt.SendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf(t, "*"))
	assert.NoError(t, a.Save(user))

	//create a session with the maximum offset ttl value (30days) 2592000 seconds
	response = rt.SendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592000}`)
	assertStatus(t, response, 200)

	layout := "2006-01-02T15:04:05"

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	log.Printf("expires %s", body["expires"].(string))
	expires, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.NoError(t, err)

	//create a session with a ttl value one second greater thatn the max offset ttl 2592001 seconds
	response = rt.SendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592001}`)
	assertStatus(t, response, 200)

	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("expires2 %s", body["expires"].(string))
	expires2, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.NoError(t, err)

	//Allow a ten second drift between the expires dates, to pass test on slow servers
	acceptableTimeDelta := time.Duration(10) * time.Second

	//The difference between the two expires dates should be less than the acceptable time delta
	goassert.True(t, expires2.Sub(expires) < acceptableTimeDelta)
}

// Check whether the session is getting extended or refreshed if 10% or more of the current
// expiration time has elapsed.
func TestSessionExtension(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	// Fake session with more than 10% of the 24 hours TTL has elapsed. It should cause a new
	// cookie to be sent by the server with the same session ID and an extended expiration date.
	fakeSession := auth.LoginSession{
		ID:         base.GenerateRandomSecret(),
		Username:   "Alice",
		Expiration: time.Now().Add(4 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	assert.NoError(t, rt.Bucket().Set(auth.DocIDForSession(fakeSession.ID), 0, fakeSession))
	reqHeaders := map[string]string{
		"Cookie": auth.DefaultCookieName + "=" + fakeSession.ID,
	}

	response := rt.SendRequestWithHeaders("PUT", "/db/doc1", `{"hi": "there"}`, reqHeaders)
	log.Printf("PUT Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	assertStatus(t, response, http.StatusCreated)
	assert.Contains(t, response.Header().Get("Set-Cookie"), auth.DefaultCookieName+"="+fakeSession.ID)

	response = rt.SendRequestWithHeaders("GET", "/db/doc1", "", reqHeaders)
	log.Printf("GET Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	assertStatus(t, response, http.StatusOK)
	assert.Equal(t, "", response.Header().Get("Set-Cookie"))

	// Explicitly delete the fake session doc from the bucket to simulate the test
	// scenario for expired session. In reality, Sync Gateway rely on Couchbase
	// Server to nuke the expired document based on TTL. Couchbase Server periodically
	// removes all items with expiration times that have passed.
	assert.NoError(t, rt.Bucket().Delete(auth.DocIDForSession(fakeSession.ID)))

	response = rt.SendRequestWithHeaders("GET", "/db/doc1", "", reqHeaders)
	log.Printf("GET Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	assertStatus(t, response, http.StatusUnauthorized)

}

func TestSessionAPI(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// create session test users
	response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"1234"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user2", "password":"1234"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user3", "password":"1234"}`)
	assertStatus(t, response, 201)

	// create multiple sessions for the users
	user1sessions := make([]string, 5)
	user2sessions := make([]string, 5)
	user3sessions := make([]string, 5)

	for i := 0; i < 5; i++ {
		user1sessions[i] = rt.createSession(t, "user1")
		user2sessions[i] = rt.createSession(t, "user2")
		user3sessions[i] = rt.createSession(t, "user3")
	}

	// GET Tests
	// 1. GET a session and make sure the result is OK
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	assertStatus(t, response, 200)

	// DELETE tests
	// 1. DELETE a session by session id
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	assertStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	assertStatus(t, response, 404)

	// 2. DELETE a session with user validation
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user1sessions[1]), "")
	assertStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[1]), "")
	assertStatus(t, response, 404)

	// 3. DELETE a session not belonging to the user (should fail)
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user2sessions[0]), "")
	assertStatus(t, response, 404)

	// GET the session and make sure it still exists
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[0]), "")
	assertStatus(t, response, 200)

	// 4. DELETE all sessions for a user
	response = rt.SendAdminRequest("DELETE", "/db/_user/user2/_session", "")
	assertStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[i]), "")
		assertStatus(t, response, 404)
	}

	// 5. DELETE sessions when password is changed
	// Change password for user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"password":"5678"}`)
	assertStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user3sessions[i]), "")
		assertStatus(t, response, 404)
	}

	// DELETE the users
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user1", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/user1", ""), 404)

	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user2", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/user2", ""), 404)

	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user3", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/user3", ""), 404)

}

func TestFlush(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("sgbucket.DeleteableBucket inteface only supported by Walrus")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.createDoc(t, "doc1")
	rt.createDoc(t, "doc2")
	assertStatus(t, rt.SendAdminRequest("GET", "/db/doc1", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/doc2", ""), 200)

	log.Printf("Flushing db...")
	assertStatus(t, rt.SendAdminRequest("POST", "/db/_flush", ""), 200)
	rt.SetAdminParty(true) // needs to be re-enabled after flush since guest user got wiped

	// After the flush, the db exists but the documents are gone:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/doc1", ""), 404)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/doc2", ""), 404)
}

//Test a single call to take DB offline
func TestDBOfflineSingle(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")
}

//Make two concurrent calls to take DB offline
// Ensure both calls succeed and that DB is offline
// when both calls return
func TestDBOfflineConcurrent(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	//Take DB offline concurrently using two goroutines
	//Both should return success and DB should be offline
	//once both goroutines return
	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *TestResponse
	go func() {
		goroutineresponse1 = rt.SendAdminRequest("POST", "/db/_offline", "")
		wg.Done()
	}()

	var goroutineresponse2 *TestResponse
	go func() {
		goroutineresponse2 = rt.SendAdminRequest("POST", "/db/_offline", "")
		wg.Done()
	}()

	err := WaitWithTimeout(&wg, time.Second*30)
	assert.NoError(t, err, "Error waiting for waitgroup")
	assertStatus(t, goroutineresponse1, http.StatusOK)
	assertStatus(t, goroutineresponse2, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

}

//Test that a DB can be created offline
func TestStartDBOffline(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")
}

//Take DB offline and ensure that normal REST calls
//fail with status 503
func TestDBOffline503Response(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	assertStatus(t, rt.SendRequest("GET", "/db/doc1", ""), 503)
}

//Take DB offline and ensure can put db config
func TestDBOfflinePutDbConfig(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	assertStatus(t, rt.SendRequest("PUT", "/db/_config", ""), 404)
}

// Tests that the users returned in the config endpoint have the correct names
// Reproduces #2223
func TestDBGetConfigNames(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	p := "password"

	rt.DatabaseConfig = &DbConfig{
		Users: map[string]*db.PrincipalConfig{
			"alice": &db.PrincipalConfig{Password: &p},
			"bob":   &db.PrincipalConfig{Password: &p},
		},
	}

	response := rt.SendAdminRequest("GET", "/db/_config", "")
	var body DbConfig
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	goassert.Equals(t, len(body.Users), len(rt.DatabaseConfig.Users))

	for k, v := range body.Users {
		goassert.Equals(t, *v.Name, k)
	}

}

//Take DB offline and ensure can post _resync
func TestDBOfflinePostResync(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	assertStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 200)
	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncStatus
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)
		return status.Status == db.ResyncStateStopped
	})
	assert.NoError(t, err)
}

//Take DB offline and ensure only one _resync can be in progress
func TestDBOfflineSingleResync(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: syncFn})
	defer rt.Close()

	//create documents in DB to cause resync to take a few seconds
	for i := 0; i < 1000; i++ {
		rt.createDoc(t, fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(1000), rt.GetDatabase().DbStats.CBLReplicationPush().SyncFunctionCount.Value())

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	assertStatus(t, response, http.StatusOK)

	// Send a second _resync request.  This must return a 400 since the first one is blocked processing
	assertStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 503)

	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncStatus
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)
		return status.Status == db.ResyncStateStopped
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(2000), rt.GetDatabase().DbStats.CBLReplicationPush().SyncFunctionCount.Value())
}

func TestResync(t *testing.T) {

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
			queryLimit:         db.DefaultResyncQueryLimit,
		},
		{
			name:               "Docs 1000, Limit Default",
			docsCreated:        1000,
			expectedSyncFnRuns: 2000,
			expectedQueryCount: 1,
			queryLimit:         db.DefaultResyncQueryLimit,
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
					DatabaseConfig: &DbConfig{
						ResyncQueryLimit: &testCase.queryLimit,
					},
					SyncFn: syncFn,
				},
			)
			defer rt.Close()

			for i := 0; i < testCase.docsCreated; i++ {
				rt.createDoc(t, fmt.Sprintf("doc%d", i))
			}

			response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			assertStatus(t, response, http.StatusServiceUnavailable)

			response = rt.SendAdminRequest("POST", "/db/_offline", "")
			assertStatus(t, response, http.StatusOK)

			waitAndAssertCondition(t, func() bool {
				state := atomic.LoadUint32(&rt.GetDatabase().State)
				return state == db.DBOffline
			})

			response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			assertStatus(t, response, http.StatusOK)

			err := rt.WaitForCondition(func() bool {
				response := rt.SendAdminRequest("GET", "/db/_resync", "")
				var status db.ResyncStatus
				err := json.Unmarshal(response.BodyBytes(), &status)
				assert.NoError(t, err)
				return status.Status == db.ResyncStateStopped
			})
			assert.NoError(t, err)

			assert.Equal(t, testCase.expectedSyncFnRuns, int(rt.GetDatabase().DbStats.CBLReplicationPush().SyncFunctionCount.Value()))

			var queryName string
			if base.TestsDisableGSI() {
				queryName = fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncGateway(), db.ViewChannels)
			} else {
				queryName = db.QueryTypeChannels
			}

			assert.Equal(t, testCase.expectedQueryCount, int(rt.GetDatabase().DbStats.Query(queryName).QueryCount.Value()))

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
			SyncFn:     syncFn,
			TestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	leakyBucket, ok := rt.Bucket().(*base.LeakyBucket)
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyBucket.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				assertStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	} else {
		leakyBucket.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				assertStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.createDoc(t, fmt.Sprintf("doc%d", i))
	}

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	assertStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	assertStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, http.StatusOK)

	waitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	assertStatus(t, response, http.StatusOK)

	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncStatus
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)
		return status.Status == db.ResyncStateStopped
	})
	assert.NoError(t, err)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	assertStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	assertStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	assertStatus(t, response, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncStatus
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)
		return status.Status == db.ResyncStateStopped
	})
	assert.NoError(t, err)

	assert.True(t, callbackFired, "expecting callback to be fired")
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
			DatabaseConfig: &DbConfig{
				ResyncQueryLimit: base.IntPtr(10),
			},
			TestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	leakyBucket, ok := rt.Bucket().(*base.LeakyBucket)
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyBucket.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
				assertStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	} else {
		leakyBucket.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
				assertStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.createDoc(t, fmt.Sprintf("doc%d", i))
	}

	err := rt.WaitForCondition(func() bool {
		return int(rt.GetDatabase().DbStats.CBLReplicationPush().SyncFunctionCount.Value()) == 1000
	})
	assert.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, http.StatusOK)

	waitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	assertStatus(t, response, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncStatus
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)
		return status.Status == db.ResyncStateStopped
	})
	assert.NoError(t, err)

	assert.True(t, callbackFired, "expecting callback to be fired")

	syncFnCount := int(rt.GetDatabase().DbStats.CBLReplicationPush().SyncFunctionCount.Value())
	assert.True(t, syncFnCount < 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

func TestResyncRegenerateSequences(t *testing.T) {
	syncFn := `
	function(doc) {
		channel("x")
		console.log("====================")
		console.log(JSON. stringify(doc))
		if (doc.userdoc){
			console.log("test")
			channel("channel_1")
		}
	}`

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	rt := NewRestTester(t,
		&RestTesterConfig{
			SyncFn: syncFn,
		},
	)
	defer rt.Close()

	var response *TestResponse
	var err error
	var docSeqArr []float64
	var body db.Body

	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("doc%d", i)
		rt.createDoc(t, docID)

		err := rt.WaitForCondition(func() bool {
			response = rt.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			return response.Code == http.StatusOK
		})
		require.NoError(t, err)

		err = json.Unmarshal(response.BodyBytes(), &body)
		assert.NoError(t, err)

		docSeqArr = append(docSeqArr, body["_sync"].(map[string]interface{})["sequence"].(float64))
	}

	role := "role1"
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_role/%s", role), fmt.Sprintf(`{"name":"%s", "admin_channels":["channel_1"]}`, role))
	assertStatus(t, response, http.StatusCreated)

	username := "user1"
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"], "admin_roles": ["%s"]}`, username, role))
	assertStatus(t, response, http.StatusCreated)

	_, err = rt.Bucket().Get(base.RolePrefix+"role1", &body)
	assert.NoError(t, err)
	role1SeqBefore := body["sequence"].(float64)

	_, err = rt.Bucket().Get(base.UserPrefix+"user1", &body)
	assert.NoError(t, err)
	user1SeqBefore := body["sequence"].(float64)

	response = rt.SendAdminRequest("PUT", "/db/userdoc", `{"userdoc": true}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/userdoc2", `{"userdoc": true}`)
	assertStatus(t, response, http.StatusCreated)

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
	assertStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &changesResp)
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))

	response = rt.SendAdminRequest("GET", "/db/_resync", "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start&regenerate_sequences=true", "")
	assertStatus(t, response, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().ResyncManager.GetStatus().Status == db.ResyncStateStopped
	})
	assert.NoError(t, err)

	_, err = rt.Bucket().Get(base.RolePrefix+"role1", &body)
	assert.NoError(t, err)
	role1SeqAfter := body["sequence"].(float64)

	_, err = rt.Bucket().Get(base.UserPrefix+"user1", &body)
	assert.NoError(t, err)
	user1SeqAfter := body["sequence"].(float64)

	assert.True(t, role1SeqAfter > role1SeqBefore)
	assert.True(t, user1SeqAfter > user1SeqBefore)

	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("doc%d", i)

		doc, err := rt.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		assert.True(t, float64(doc.Sequence) > docSeqArr[i])
	}

	response = rt.SendAdminRequest("POST", "/db/_online", "")
	assertStatus(t, response, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOnline
	})
	assert.NoError(t, err)

	// Data is wiped from walrus when brought back online
	if !base.UnitTestUrlIsWalrus() {
		request, _ = http.NewRequest("GET", "/db/_all_docs", nil)
		request.SetBasicAuth("user1", "letmein")
		response = rt.Send(request)
		assertStatus(t, response, http.StatusOK)

		fmt.Println(string(response.BodyBytes()))

		request, _ = http.NewRequest("GET", "/db/_all_docs", nil)
		request.SetBasicAuth("user1", "letmein")
		response = rt.Send(request)
		assertStatus(t, response, http.StatusOK)

		request, _ = http.NewRequest("GET", "/db/_changes?since="+changesResp.LastSeq, nil)
		request.SetBasicAuth("user1", "letmein")
		response = rt.Send(request)
		assertStatus(t, response, http.StatusOK)
		err = json.Unmarshal(response.BodyBytes(), &changesResp)
		assert.Len(t, changesResp.Results, 3)
		assert.True(t, changesRespContains(changesResp, "userdoc"))
		assert.True(t, changesRespContains(changesResp, "userdoc2"))
	}

}

// Single threaded bring DB online
func TestDBOnlineSingle(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	rt.SendAdminRequest("POST", "/db/_online", "")
	assertStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")
}

//Take DB online concurrently using two goroutines
//Both should return success and DB should be online
//once both goroutines return
func TestDBOnlineConcurrent(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *TestResponse
	go func(rt *RestTester) {
		defer wg.Done()
		goroutineresponse1 = rt.SendAdminRequest("POST", "/db/_online", "")
		assertStatus(t, goroutineresponse1, 200)
	}(rt)

	var goroutineresponse2 *TestResponse
	go func(rt *RestTester) {
		defer wg.Done()
		goroutineresponse2 = rt.SendAdminRequest("POST", "/db/_online", "")
		assertStatus(t, goroutineresponse2, 200)
	}(rt)

	//This only waits until both _online requests have been posted
	//They may not have been processed at this point
	wg.Wait()

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online with delay of 1 second
func TestSingleDBOnlineWithDelay(t *testing.T) {

	t.Skip("Use case covered by TestDBOnlineWithTwoDelays, skipping due to slow test")
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	// Wait until after the 1 second delay, since the online request explicitly asked for a delay
	time.Sleep(1500 * time.Millisecond)

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online with delay of 2 seconds
// But bring DB online immediately in separate call
// BD should should only be brought online once
// there should be no errors
func TestDBOnlineWithDelayAndImmediate(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	//Bring DB online with delay of two seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	assertStatus(t, response, 200)

	// Bring DB online immediately
	rt.SendAdminRequest("POST", "/db/_online", "")
	assertStatus(t, response, 200)

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	// Wait until after the 1 second delay, since the online request explicitly asked for a delay
	time.Sleep(1500 * time.Millisecond)

	// Wait for DB to come online (retry loop)
	errDbOnline = rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online concurrently with delay of 1 second
// and delay of 2 seconds
// BD should should only be brought online once
// there should be no errors
func TestDBOnlineWithTwoDelays(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	//Bring DB online with delay of one seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	assertStatus(t, response, 200)

	//Bring DB online with delay of two seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":2}")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Offline")

	time.Sleep(1500 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")

	time.Sleep(600 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.True(t, body["state"].(string) == "Online")
}

func (rt *RestTester) createSession(t *testing.T, username string) string {

	response := rt.SendAdminRequest("POST", "/db/_session", fmt.Sprintf(`{"name":%q}`, username))
	assertStatus(t, response, 200)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	sessionId := body["session_id"].(string)

	return sessionId
}

func TestPurgeWithBadJsonPayload(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", "foo")
	assertStatus(t, response, 400)
}

func TestPurgeWithNonArrayRevisionList(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":"list"}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithEmptyRevisionList(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":[]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithGreaterThanOneRevision(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":["rev1","rev2"]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithNonStarRevision(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":["rev1"]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithStarRevision(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"]}`)
	assertStatus(t, response, 200)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}})

	//Create new versions of the doc1 without conflicts
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
}

func TestPurgeWithMultipleValidDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"],"doc2":["*"]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}, "doc2": []interface{}{"*"}}})

	//Create new versions of the docs without conflicts
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)
}

// TestPurgeWithChannelCache will make sure thant upon calling _purge, the channel caches are also cleaned
// This was fixed in #3765, previously channel caches were not cleaned up
func TestPurgeWithChannelCache(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar", "channels": ["abc", "def"]}`), http.StatusCreated)
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car", "channels": ["abc"]}`), http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/db/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
	assert.NoError(t, err, "Error waiting for changes")
	goassert.Equals(t, changes.Results[0].ID, "doc1")
	goassert.Equals(t, changes.Results[1].ID, "doc2")

	// Purge "doc1"
	resp := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"]}`)
	assertStatus(t, resp, http.StatusOK)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}})

	changes, err = rt.WaitForChanges(1, "/db/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
	assert.NoError(t, err, "Error waiting for changes")
	goassert.Equals(t, changes.Results[0].ID, "doc2")

}

func TestPurgeWithSomeInvalidDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"],"doc2":["1-123"]}`)
	assertStatus(t, response, 200)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}})

	//Create new versions of the doc1 without conflicts
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)

	//Create new versions of the doc2 fails because it already exists
	assertStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 409)
}

func TestReplicateErrorConditions(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip replication tests during integration tests, since they might be leaving replications running in background")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	//Send empty JSON
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", ""), 400)

	//Send empty JSON Object
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{}`), 400)

	//Send JSON Object with random properties
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"foo":"bar"}`), 400)

	//Send JSON Object containing create_target property
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"create_target":true}`), 400)

	//Send JSON Object containing doc_ids property
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"doc_ids":["foo","bar","moo","car"]}`), 400)

	//Send JSON Object containing filter property other than 'sync_gateway/bychannel'
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"filter":"somefilter"}`), 400)

	//Send JSON Object containing filter 'sync_gateway/bychannel' with non array query_params property
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"filter":"`+base.ByChannelFilter+`", "query_params":{"someproperty":"somevalue"}}`), 400)

	//Send JSON Object containing filter 'sync_gateway/bychannel' with non string array query_params property
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"filter":"`+base.ByChannelFilter+`", "query_params":["someproperty",false]}`), 400)

	//Send JSON Object containing proxy property
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"proxy":"http://myproxy/"}`), 400)

	//Send JSON Object containing source as absolute URL but no target
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/mysourcedb"}`), 400)

	//Send JSON Object containing no source and target as absolute URL
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"target":"http://myhost:4985/mytargetdb"}`), 400)

	//Send JSON Object containing source as local DB but no target
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"mylocalsourcedb"}`), 400)

	//Send JSON Object containing no source and target as local DB
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"target":"mylocaltargetdb"}`), 400)

}

// These tests validate request parameters not actual replication.
// For actual replication tests, see:
// - sg-replicate mock-based unit test suite
// - Functional tests in mobile-testkit repo
func TestDocumentChangeReplicate(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate)()

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip replication tests during integration tests, since they might be leaving replications running in background")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close() // Close RestTester, which closes ServerContext, which stops all replications

	mockClient := NewMockClient()
	fakeConfigURL := "http://myhost:4985"
	mockClient.RespondToGET(fakeConfigURL+"/db", MakeResponse(200, nil, ``))
	mockClient.RespondToGET(fakeConfigURL+"/db2", MakeResponse(200, nil, ``))
	mockClient.RespondToGET(fakeConfigURL+"/db3", MakeResponse(200, nil, ``))
	mockClient.RespondToGET(fakeConfigURL+"/db4", MakeResponse(200, nil, ``))
	mockClient.RespondToGET(fakeConfigURL+"/mysourcedb", MakeResponse(200, nil, ``))
	mockClient.RespondToGET(fakeConfigURL+"/mytargetdb", MakeResponse(200, nil, ``))
	sc := rt.ServerContext()
	sc.HTTPClient = mockClient.Client

	//Initiate synchronous one shot replication
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db", "target":"http://myhost:4985/db"}`), 500)

	//Initiate asyncronous one shot replication
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db", "target":"http://myhost:4985/db", "async":true}`), 200)

	//Initiate continuous replication
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db", "target":"http://myhost:4985/db", "continuous":true}`), 200)

	//Initiate synchronous one shot replication with channel filter and JSON array of channel names
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db2", "target":"http://myhost:4985/db2", "filter":"`+base.ByChannelFilter+`", "query_params":["A"]}`), 500)

	//Initiate synchronous one shot replication with channel filter and JSON object containing a property "channels" and value of JSON Array pf channel names
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db3", "target":"http://myhost:4985/db3", "filter":"`+base.ByChannelFilter+`", "query_params":{"channels":["A"]}}`), 500)

	//Initiate synchronous one shot replication with channel filter and JSON object containing a property "channels" and value of JSON Array pf channel names and custom changes_feed_limit
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db4", "target":"http://myhost:4985/db4", "filter":"`+base.ByChannelFilter+`", "query_params":{"channels":["B"]}, "changes_feed_limit":10}`), 500)

	//Initiate continuous replication with channel filter and JSON array of channel names
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db2", "target":"http://myhost:4985/db2", "filter":"`+base.ByChannelFilter+`", "query_params":["A"], "continuous":true}`), 200)

	//Initiate continuous replication with channel filter and JSON object containing a property "channels" and value of JSON Array pf channel names
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db3", "target":"http://myhost:4985/db3", "filter":"`+base.ByChannelFilter+`", "query_params":{"channels":["A"]}, "continuous":true}`), 200)

	//Initiate continuous replication with channel filter and JSON object containing a property "channels" and value of JSON Array pf channel names and custom changes_feed_limit
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/db4", "target":"http://myhost:4985/db4", "filter":"`+base.ByChannelFilter+`", "query_params":{"channels":["B"]}, "changes_feed_limit":10, "continuous":true}`), 200)

	//Send JSON Object containing source and target as absolute URL and a replication_id
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"source":"http://myhost:4985/mysourcedb", "target":"http://myhost:4985/mytargetdb", "replication_id":"myreplicationid"}`), 500)

	//Cancel a replication
	assertStatus(t, rt.SendAdminRequest("POST", "/_replicate", `{"replication_id":"ABC", "cancel":true}`), 404)

}

func TestRawRedaction(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	res := rt.SendAdminRequest("PUT", "/db/testdoc", `{"foo":"bar", "channels": ["achannel"]}`)
	assertStatus(t, res, http.StatusCreated)

	// Test redact being disabled by default
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc", ``)
	var body map[string]interface{}
	err := base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData := body[base.SyncPropertyName]
	assert.Equal(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.Equal(t, []interface{}([]interface{}{[]interface{}{"achannel"}}), syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test redacted
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc?redact=true&include_doc=false", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData = body[base.SyncPropertyName]
	require.NotNil(t, syncData)
	assert.NotEqual(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.NotEqual(t, []interface{}([]interface{}{[]interface{}{"achannel"}}), syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test include doc false doesn't return doc
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc?include_doc=false", ``)
	assert.NotContains(t, res.Body.String(), "foo")

	// Test doc is returned by default
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.Equal(t, body["foo"], "bar")

	// Test that you can't use include_doc and redact at the same time
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc?include_doc=true&redact=true", ``)
	assertStatus(t, res, http.StatusBadRequest)
}

func TestRawTombstone(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	const docID = "testdoc"

	// Create a doc
	resp := rt.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"foo":"bar"}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+revID+`"`)
	assert.Contains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_deleted":true`)

	// Delete the doc
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/"+docID+"?rev="+revID, ``)
	assertStatus(t, resp, http.StatusOK)
	revID = respRevID(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+revID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.Contains(t, string(resp.BodyBytes()), `"_deleted":true`)
}

// respRevID returns a rev ID from the given response, or fails the given test if a rev ID was not found.
func respRevID(t *testing.T, response *TestResponse) (revID string) {
	var r struct {
		RevID *string `json:"rev"`
	}
	require.NoError(t, json.Unmarshal(response.BodyBytes(), &r), "couldn't decode JSON from response body")
	require.NotNil(t, r.RevID, "expecting non-nil rev ID from response: %s", string(response.BodyBytes()))
	require.NotEqual(t, "", *r.RevID, "expecting non-empty rev ID from response: %s", string(response.BodyBytes()))
	return *r.RevID
}

func TestHandleCreateDB(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	server := "walrus:"
	bucket := "albums"
	kvTLSPort := 11207
	resource := fmt.Sprintf("/%s/", bucket)

	bucketConfig := BucketConfig{Server: &server, Bucket: &bucket, KvTLSPort: kvTLSPort}
	dbConfig := &DbConfig{BucketConfig: bucketConfig, SGReplicateEnabled: base.BoolPtr(false)}
	var respBody db.Body

	reqBody, err := base.JSONMarshal(dbConfig)
	assert.NoError(t, err, "Error unmarshalling changes response")

	resp := rt.SendAdminRequest(http.MethodPut, resource, string(reqBody))
	assertStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, resource, string(reqBody))
	assertStatus(t, resp, http.StatusOK)
	assert.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Equal(t, bucket, respBody["db_name"].(string))
	assert.Equal(t, "Online", respBody["state"].(string))

	// Try to create database with bad JSON request body and simulate JSON
	// parsing error from the handler; handleCreateDB.
	reqBodyJson := `"server":"walrus:","pool":"default","bucket":"albums","kv_tls_port":11207`
	resp = rt.SendAdminRequest(http.MethodPut, "/photos/", reqBodyJson)
	assertStatus(t, resp, http.StatusBadRequest)
}

func TestHandlePutDbConfigWithBackticks(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Get database info before putting config.
	resp := rt.SendAdminRequest(http.MethodGet, "/backticks/", "")
	assertStatus(t, resp, http.StatusNotFound)

	// Create database with valid JSON config that contains sync function enclosed in backticks.
	syncFunc := `function(doc, oldDoc) { console.log("foo");}`
	reqBodyWithBackticks := `{
    	"server": "walrus:",
    	"bucket": "backticks",
        "sync": ` + "`" + syncFunc + "`" + `
	}`
	resp = rt.SendAdminRequest(http.MethodPut, "/backticks/", reqBodyWithBackticks)
	assertStatus(t, resp, http.StatusCreated)

	// Get database config after putting config.
	resp = rt.SendAdminRequest(http.MethodGet, "/backticks/_config", "")
	assertStatus(t, resp, http.StatusOK)
	var respBody db.Body
	require.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Equal(t, "backticks", respBody["name"].(string))
	assert.Equal(t, "walrus:", respBody["server"].(string))
	assert.Equal(t, syncFunc, respBody["sync"].(string))
}

func TestHandleDBConfig(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	server := "walrus:"
	bucket := "albums"
	kvTLSPort := 443
	certPath := "/etc/ssl/certs/client.cert"
	keyPath := "/etc/ssl/certs/client.pem"
	caCertPath := "/etc/ssl/certs/ca.cert"
	username := "Alice"
	password := "QWxpY2U="
	resource := fmt.Sprintf("/%s/", bucket)

	// Create a database with no config
	resp := rt.SendAdminRequest(http.MethodPut, resource, "{}")
	assertStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Get database config before putting any config.
	resp = rt.SendAdminRequest(http.MethodGet, resource, "")
	assertStatus(t, resp, http.StatusOK)
	var respBody db.Body
	assert.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Nil(t, respBody["bucket"])
	assert.Equal(t, bucket, respBody["db_name"].(string))
	assert.Equal(t, "Online", respBody["state"].(string))

	// Put database config
	resource = fmt.Sprintf("/%v/_config?redact=false", bucket)

	bucketConfig := BucketConfig{
		Server:     &server,
		Bucket:     &bucket,
		Username:   username,
		Password:   password,
		CertPath:   certPath,
		KeyPath:    keyPath,
		CACertPath: caCertPath,
		KvTLSPort:  kvTLSPort,
	}
	dbConfig := &DbConfig{BucketConfig: bucketConfig, SGReplicateEnabled: base.BoolPtr(false)}
	reqBody, err := base.JSONMarshal(dbConfig)
	assert.NoError(t, err, "Error unmarshalling changes response")
	resp = rt.SendAdminRequest(http.MethodPut, resource, string(reqBody))
	assertStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Get database config after putting valid database config
	resp = rt.SendAdminRequest(http.MethodGet, resource, "")
	assertStatus(t, resp, http.StatusOK)
	respBody = nil
	assert.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))

	assert.Equal(t, bucket, respBody["bucket"].(string))
	assert.Equal(t, bucket, respBody["name"].(string))
	assert.Equal(t, server, respBody["server"].(string))
	assert.Equal(t, username, respBody["username"].(string))
	assert.Equal(t, password, respBody["password"].(string))
	assert.Equal(t, certPath, respBody["certpath"].(string))
	assert.Equal(t, keyPath, respBody["keypath"].(string))
	assert.Equal(t, caCertPath, respBody["cacertpath"].(string))
	assert.Equal(t, json.Number("443"), respBody["kv_tls_port"].(json.Number))
}

func TestHandleDeleteDB(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Try to delete the database which doesn't exists
	resp := rt.SendAdminRequest(http.MethodDelete, "/albums/", "{}")
	assertStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, string(resp.BodyBytes()), "no such database")
	var v map[string]interface{}
	assert.NoError(t, json.Unmarshal(resp.BodyBytes(), &v), "couldn't unmarshal %s", string(resp.BodyBytes()))

	// Create the database
	resp = rt.SendAdminRequest(http.MethodPut, "/albums/", "{}")
	assertStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Delete the database
	resp = rt.SendAdminRequest(http.MethodDelete, "/albums/", "{}")
	assertStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "{}")
}

func TestHandleGetConfig(t *testing.T) {
	syncFunc := `function(doc) {throw({forbidden: "read only!"})}`
	conf := RestTesterConfig{SyncFn: syncFunc}
	rt := NewRestTester(t, &conf)
	defer rt.Close()

	var respBody db.Body
	resp := rt.SendAdminRequest(http.MethodGet, "/_config", "{}")
	assertStatus(t, resp, http.StatusOK)
	assert.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))

	assert.Equal(t, "127.0.0.1:4985", respBody["AdminInterface"].(string))
	databases := respBody["Databases"].(map[string]interface{})
	db := databases["db"].(map[string]interface{})
	assert.Equal(t, syncFunc, db["sync"].(string))
}

func TestHandleGetRevTree(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create three revisions of the user foo with different status and updated_at values;
	reqBodyJson := `{"new_edits": false, "docs": [
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-24T17:37:49.715Z", "status": "online", "_rev": "1-123"}, 
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-26T17:37:49.715Z", "status": "offline", "_rev": "1-456"}, 
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-25T17:37:49.715Z", "status": "offline", "_rev": "1-789"}]}`

	resp := rt.SendAdminRequest(http.MethodPost, "/db/_bulk_docs", reqBodyJson)
	assertStatus(t, resp, http.StatusCreated)
	respBodyExpected := `[{"id":"foo","rev":"1-123"},{"id":"foo","rev":"1-456"},{"id":"foo","rev":"1-789"}]`
	assert.Equal(t, respBodyExpected, resp.Body.String())

	// Get the revision tree  of the user foo
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_revtree/foo", "")
	assertStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "1-123")
	assert.Contains(t, resp.Body.String(), "1-456")
	assert.Contains(t, resp.Body.String(), "1-789")
	assert.True(t, strings.HasPrefix(resp.Body.String(), "digraph"))
}

func TestHandleActiveTasks(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	// Check the count of active tasks
	var tasks []interface{}
	resp := rt.SendAdminRequest(http.MethodGet, "/_active_tasks", "")
	assertStatus(t, resp, http.StatusOK)
	assert.NoError(t, json.Unmarshal([]byte(resp.Body.String()), &tasks))
	require.Len(t, tasks, 0)
}

func TestHandleSGCollect(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	reqBodyJson := "invalidjson"
	resource := "/_sgcollect_info"

	// Check SGCollect status before triggering it; status should be stopped if no process is running.
	resp := rt.SendAdminRequest(http.MethodGet, resource, reqBodyJson)
	assertStatus(t, resp, http.StatusOK)
	assert.Equal(t, resp.Body.String(), `{"status":"stopped"}`)

	// Try to cancel SGCollect before triggering it; Error stopping sgcollect_info: not running
	resp = rt.SendAdminRequest(http.MethodDelete, resource, reqBodyJson)
	assertStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "Error stopping sgcollect_info: not running")

	// Try to start SGCollect with invalid body; It should throw with unexpected end of JSON input error
	resp = rt.SendAdminRequest(http.MethodPost, resource, reqBodyJson)
	assertStatus(t, resp, http.StatusBadRequest)
}

func TestSessionExpirationDateTimeFormat(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	authenticator := auth.NewAuthenticator(rt.Bucket(), nil)
	user, err := authenticator.NewUser("alice", "letMe!n", channels.SetOf(t, "*"))
	assert.NoError(t, err, "Couldn't create new user")
	assert.NoError(t, authenticator.Save(user), "Couldn't save new user")

	var body db.Body
	response := rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"alice"}`)
	assertStatus(t, response, http.StatusOK)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	expires, err := time.Parse(time.RFC3339, body["expires"].(string))
	assert.NoError(t, err, "Couldn't parse session expiration datetime")
	assert.True(t, expires.Sub(time.Now()).Hours() <= 24, "Couldn't validate session expiration")

	sessionId := body["session_id"].(string)
	require.NotEmpty(t, sessionId, "Couldn't parse sessionID from response body")
	response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/db/_session/%s", sessionId), "")
	assertStatus(t, response, http.StatusOK)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	expires, err = time.Parse(time.RFC3339, body["expires"].(string))
	assert.NoError(t, err, "Couldn't parse session expiration datetime")
	assert.True(t, expires.Sub(time.Now()).Hours() <= 24, "Couldn't validate session expiration")
}

func TestUserAndRoleResponseContentType(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create a user 'christopher' through PUT request with empty request body.
	var responseBody db.Body
	body := `{"email":"christopher@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response := rt.SendAdminRequest(http.MethodPut, "/db/_user/christopher", "")
	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Create a user 'charles' through POST request with empty request body.
	body = `{"email":"charles@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response = rt.SendAdminRequest(http.MethodPost, "/db/_user/charles", "")
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Create a user 'alice' through PUT request.
	body = `{"email":"alice@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response = rt.SendAdminRequest(http.MethodPut, "/db/_user/alice", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create another user 'bob' through POST request.
	body = `{"name":"bob","email":"bob@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response = rt.SendAdminRequest(http.MethodPost, "/db/_user/", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the user details of user 'alice' through GET request.
	response = rt.SendAdminRequest(http.MethodGet, "/db/_user/alice", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Get the list of users through GET request.
	var users []string
	response = rt.SendAdminRequest(http.MethodGet, "/db/_user/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &users))
	assert.Subset(t, []string{"alice", "bob"}, users)

	// Check whether the /db/_user/bob resource exist on the server.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_user/bob", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the list of users through HEAD request.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_user/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete user 'alice'
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/alice", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete GUEST user instead of disabling.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/GUEST", "")
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))

	// Delete user 'eve' who doesn't exists at this point of time.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))

	// Create a new user and save to database to create user session.
	authenticator := auth.NewAuthenticator(rt.Bucket(), nil)
	user, err := authenticator.NewUser("eve", "cGFzc3dvcmQ=", channels.SetOf(t, "*"))
	assert.NoError(t, err, "Couldn't create new user")
	assert.NoError(t, authenticator.Save(user), "Couldn't save new user")

	// Create user session to check delete session request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"eve"}`)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))
	sessionId, _ := responseBody["session_id"].(string)
	require.NotEmpty(t, sessionId, "Couldn't parse sessionID from response body")

	// Delete user session using /db/_user/eve/_session/{sessionId}.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve/_session/"+sessionId, "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create user session to check delete session request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"eve"}`)
	assert.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Delete user session using /db/_user/eve/_session request.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve/_session", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create a role 'developer' through POST request
	body = `{"name":"developer","admin_channels":["channel1", "channel2"]}`
	response = rt.SendAdminRequest(http.MethodPost, "/db/_role/", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create another role 'coder' through PUT request.
	body = `{"admin_channels":["channel3", "channel4"]}`
	response = rt.SendAdminRequest(http.MethodPut, "/db/_role/coder", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Check whether the /db/_role/ resource exist on the server.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_role/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the created roles through GET request.
	var roles []string
	response = rt.SendAdminRequest(http.MethodGet, "/db/_role/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &roles))
	assert.Subset(t, []string{"coder", "developer"}, roles)

	// Delete role 'coder' from database.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/coder", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete role who doesn't exist.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/programmer", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))
}

func TestConfigRedaction(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{DatabaseConfig: &DbConfig{Users: map[string]*db.PrincipalConfig{"alice": {Password: base.StringPtr("password")}}}})
	defer rt.Close()

	// Test default db config redaction
	var unmarshaledConfig DbConfig
	response := rt.SendAdminRequest("GET", "/db/_config", "")
	err := json.Unmarshal(response.BodyBytes(), &unmarshaledConfig)
	require.NoError(t, err)

	assert.Equal(t, "xxxxx", unmarshaledConfig.Password)
	assert.Equal(t, "xxxxx", *unmarshaledConfig.Users["alice"].Password)

	// Test default db config redaction when redaction disabled
	response = rt.SendAdminRequest("GET", "/db/_config?redact=false", "")
	err = json.Unmarshal(response.BodyBytes(), &unmarshaledConfig)
	require.NoError(t, err)

	assert.Equal(t, base.TestClusterPassword(), unmarshaledConfig.Password)
	assert.Equal(t, "password", *unmarshaledConfig.Users["alice"].Password)

	// Test default server config redaction
	var unmarshaledServerConfig ServerConfig
	response = rt.SendAdminRequest("GET", "/_config", "")
	err = json.Unmarshal(response.BodyBytes(), &unmarshaledServerConfig)
	require.NoError(t, err)

	assert.Equal(t, "xxxxx", unmarshaledServerConfig.Databases["db"].Password)
	assert.Equal(t, "xxxxx", *unmarshaledServerConfig.Databases["db"].Users["alice"].Password)

	// Test default server config redaction when redaction disabled
	response = rt.SendAdminRequest("GET", "/_config?redact=false", "")
	err = json.Unmarshal(response.BodyBytes(), &unmarshaledServerConfig)
	require.NoError(t, err)

	assert.Equal(t, base.TestClusterPassword(), unmarshaledServerConfig.Databases["db"].Password)
	assert.Equal(t, "password", *unmarshaledServerConfig.Databases["db"].Users["alice"].Password)
}

// Reproduces panic seen in CBG-1053
func TestAdhocReplicationStatus(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll, base.KeyReplicate)()
	rt := NewRestTester(t, nil)
	defer rt.Close()

	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()

	replConf := `
	{
	  "replication_id": "pushandpull-with-target-oneshot-adhoc",
	  "remote": "` + srv.URL + `/db",
	  "direction": "pushAndPull",
	  "adhoc": true
	}`

	resp := rt.SendAdminRequest("PUT", "/db/_replication/pushandpull-with-target-oneshot-adhoc", replConf)
	assertStatus(t, resp, http.StatusCreated)

	err := rt.GetDatabase().SGReplicateMgr.StartReplications()
	require.NoError(t, err)

	// With the error hitting the replicationStatus endpoint will either return running, if not completed, and once
	// completed panics. With the fix after running it'll return a 404 as replication no longer exists.
	stateError := rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/db/_replicationStatus/pushandpull-with-target-oneshot-adhoc", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, stateError)
}

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
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
)

func TestUserAPI(t *testing.T) {
	// PUT a user
	var rt restTester
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_user/snej", ""), 404)
	response := rt.sendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// GET the user and make sure the result is OK
	response = rt.sendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "snej")
	assert.Equals(t, body["email"], "jens@couchbase.com")
	assert.DeepEquals(t, body["admin_channels"], []interface{}{"bar", "foo"})
	assert.DeepEquals(t, body["all_channels"], []interface{}{"!", "bar", "foo"})
	assert.Equals(t, body["password"], nil)

	// Check the list of all users:
	response = rt.sendAdminRequest("GET", "/db/_user/", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), `["snej"]`)

	// Check that the actual User object is correct:
	user, _ := rt.ServerContext().Database("db").Authenticator().GetUser("snej")
	assert.Equals(t, user.Name(), "snej")
	assert.Equals(t, user.Email(), "jens@couchbase.com")
	assert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet{"bar": channels.NewVbSimpleSequence(0x1), "foo": channels.NewVbSimpleSequence(0x1)})
	assert.True(t, user.Authenticate("letmein"))

	// Change the password and verify it:
	response = rt.sendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"123", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 200)

	user, _ = rt.ServerContext().Database("db").Authenticator().GetUser("snej")
	assert.True(t, user.Authenticate("123"))

	// DELETE the user
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_user/snej", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_user/snej", ""), 404)

	// POST a user
	response = rt.sendAdminRequest("POST", "/db/_user", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 301)
	response = rt.sendAdminRequest("POST", "/db/_user/", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "snej")

	// Create a role
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response = rt.sendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)

	// Give the user that role
	response = rt.sendAdminRequest("PUT", "/db/_user/snej", `{"admin_channels":["foo", "bar"],"admin_roles":["hipster"]}`)
	assertStatus(t, response, 200)

	// GET the user and verify that it shows the channels inherited from the role
	response = rt.sendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body["admin_roles"], []interface{}{"hipster"})
	assert.DeepEquals(t, body["all_channels"], []interface{}{"!", "bar", "fedoras", "fixies", "foo"})

	// DELETE the user
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_user/snej", ""), 200)
}

// Test user access grant while that user has an active changes feed.  (see issue #880)
func DisabledTestUserAccessRace(t *testing.T) {

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
	rt := restTester{syncFn: syncFunction}

	response := rt.sendAdminRequest("PUT", "/_logging", `{"HTTP":true}`)

	response = rt.sendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["profile-bernard"]}`)
	assertStatus(t, response, 201)

	//Try to force channel initialisation for user bernard
	response = rt.sendAdminRequest("GET", "/db/_user/bernard", "")
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
	response = rt.sendAdminRequest("POST", "/db/_bulk_docs", input)

	// Start changes feed
	var wg sync.WaitGroup
	go func() {
		defer wg.Done()
		wg.Add(1)
		// Timeout allows us to read continuous changes after processing is complete.  Needs to be long enough to
		// ensure it doesn't terminate before the first change is sent.
		changesResponse := rt.send(requestByUser("GET", "/db/_changes?feed=continuous&since=0&timeout=2000", "", "bernard"))

		changes, err := readContinuousChanges(changesResponse)
		assert.Equals(t, err, nil)

		log.Printf("Got %d change entries", len(changes))
		assert.Equals(t, len(changes), 201)
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
		response = rt.send(requestByUser("POST", "/db/_bulk_docs", input, "bernard"))
	}

	// wait for changes feed to complete (time out)
	wg.Wait()
}

// Test user delete while that user has an active changes feed (see issue 809)
func TestUserDeleteDuringChangesWithAccess(t *testing.T) {

	rt := restTester{syncFn: `function(doc) {channel(doc.channel); if(doc.type == "setaccess") { access(doc.owner, doc.channel);}}`}

	response := rt.sendAdminRequest("PUT", "/_logging", `{"Changes+":true, "Changes":true, "Cache":true, "HTTP":true}`)

	response = rt.sendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["foo"]}`)
	assertStatus(t, response, 201)

	changesClosed := false
	go func() {
		response = rt.send(requestByUser("GET", "/db/_changes?feed=continuous&since=0", "", "bernard"))
		// When testing single threaded, this reproduces the issue described in #809.
		// When testing multithreaded (-cpu 4 -race), there are three (valid) possibilities"
		// 1. The DELETE gets processed before the _changes auth completes: this will return 401
		// 2. The _changes request gets processed before the DELETE: the changes response will be closed when the user is deleted
		// 3. The DELETE is processed after the _changes auth completes, but before the MultiChangesFeed is instantiated.  The
		//  changes feed doesn't have a trigger to attempt a reload of the user in this scenario, so will continue until disconnected
		//  by the client.  This should be fixed more generally (to terminate all active user sessions when the user is deleted, not just
		//  changes feeds) but that enhancement is too high risk to introduce at this time.
		changesClosed = true
		if response.Code == 401 {
			// case 1 - ok
		} else {
			// case 2 - ensure no error processing the changes response.  Ensure no more than 2 entries
			changes, err := readContinuousChanges(response)
			assert.True(t, len(changes) <= 2)
			assert.Equals(t, err, nil)
		}
	}()

	// TODO: sleep required to ensure the changes feed iteration starts before the delete gets processed.
	time.Sleep(100 * time.Millisecond)
	rt.sendAdminRequest("PUT", "/db/bernard_doc1", `{"type":"setaccess", "owner":"bernard","channel":"foo"}`)
	rt.sendAdminRequest("DELETE", "/db/_user/bernard", "")
	rt.sendAdminRequest("PUT", "/db/manny_doc1", `{"type":"setaccess", "owner":"manny","channel":"bar"}`)
	rt.sendAdminRequest("PUT", "/db/bernard_doc2", `{"type":"general", "channel":"foo"}`)

	// case 3
	for i := 0; i <= 5; i++ {
		docId := fmt.Sprintf("/db/bernard_doc%d", i+3)
		response = rt.sendAdminRequest("PUT", docId, `{"type":"setaccess", "owner":"bernard", "channel":"foo"}`)
	}

}

// Reads continuous changes feed response into slice of ChangeEntry
func readContinuousChanges(response *testResponse) ([]db.ChangeEntry, error) {
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
			err := json.Unmarshal(entry, &change)
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
	var rt restTester
	// PUT a role
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response := rt.sendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)

	// GET the role and make sure the result is OK
	response = rt.sendAdminRequest("GET", "/db/_role/hipster", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "hipster")
	assert.DeepEquals(t, body["admin_channels"], []interface{}{"fedoras", "fixies"})
	assert.Equals(t, body["password"], nil)

	response = rt.sendAdminRequest("GET", "/db/_role/", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), `["hipster"]`)

	// DELETE the role
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_role/hipster", ""), 404)

	// POST a role
	response = rt.sendAdminRequest("POST", "/db/_role", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 301)
	response = rt.sendAdminRequest("POST", "/db/_role/", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/_role/hipster", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "hipster")
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)
}

func TestGuestUser(t *testing.T) {

	guestUserEndpoint := fmt.Sprintf("/db/_user/%s", base.GuestUsername)

	rt := restTester{noAdminParty: true}
	response := rt.sendAdminRequest("GET", guestUserEndpoint, "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], base.GuestUsername)
	// This ain't no admin-party, this ain't no nightclub, this ain't no fooling around:
	assert.DeepEquals(t, body["admin_channels"], nil)

	response = rt.sendAdminRequest("PUT", guestUserEndpoint, `{"disabled":true}`)
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", guestUserEndpoint, "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], base.GuestUsername)
	assert.DeepEquals(t, body["disabled"], true)

	// Check that the actual User object is correct:
	user, _ := rt.ServerContext().Database("db").Authenticator().GetUser("")
	assert.Equals(t, user.Name(), "")
	assert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet{})
	assert.Equals(t, user.Disabled(), true)
}

//Test that TTL values greater than the default max offset TTL 2592000 seconds are processed correctly
// fixes #974
func TestSessionTtlGreaterThan30Days(t *testing.T) {
	var rt restTester
	a := auth.NewAuthenticator(rt.bucket(), nil)
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	user, err = a.GetUser("")
	assert.Equals(t, err, nil)
	assert.True(t, user.Disabled())

	response := rt.sendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf("*"))
	a.Save(user)

	//create a session with the maximum offset ttl value (30days) 2592000 seconds
	response = rt.sendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592000}`)
	assertStatus(t, response, 200)

	layout := "2006-01-02T15:04:05"

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)

	log.Printf("expires %s", body["expires"].(string))
	expires, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.Equals(t, err, nil)

	//create a session with a ttl value one second greater thatn the max offset ttl 2592001 seconds
	response = rt.sendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592001}`)
	assertStatus(t, response, 200)

	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	log.Printf("expires2 %s", body["expires"].(string))
	expires2, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.Equals(t, err, nil)

	//Allow a ten second drift between the expires dates, to pass test on slow servers
	acceptableTimeDelta := time.Duration(10) * time.Second

	//The difference between the two expires dates should be less than the acceptable time delta
	assert.True(t, expires2.Sub(expires) < acceptableTimeDelta)
}

func TestSessionExtension(t *testing.T) {
	var rt restTester
	a := auth.NewAuthenticator(rt.bucket(), nil)
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	user, err = a.GetUser("")
	assert.Equals(t, err, nil)
	assert.True(t, user.Disabled())

	response := rt.sendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf("*"))
	a.Save(user)

	assertStatus(t, rt.sendAdminRequest("GET", "/db/_session", ""), 200)

	response = rt.sendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":10}`)
	assertStatus(t, response, 200)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	sessionId := body["session_id"].(string)
	sessionExpiration := body["expires"].(string)
	assert.True(t, sessionId != "")
	assert.True(t, sessionExpiration != "")
	assert.True(t, body["cookie_name"].(string) == "SyncGatewaySession")

	reqHeaders := map[string]string{
		"Cookie": "SyncGatewaySession=" + body["session_id"].(string),
	}
	response = rt.sendRequestWithHeaders("PUT", "/db/doc1", `{"hi": "there"}`, reqHeaders)
	assertStatus(t, response, 201)

	assert.True(t, response.Header().Get("Set-Cookie") == "")

	//Sleep for 2 seconds, this will ensure 10% of the 100 seconds session ttl has elapsed and
	//should cause a new Cookie to be sent by the server with the same session ID and an extended expiration date
	time.Sleep(2 * time.Second)
	response = rt.sendRequestWithHeaders("PUT", "/db/doc2", `{"hi": "there"}`, reqHeaders)
	assertStatus(t, response, 201)

	assert.True(t, response.Header().Get("Set-Cookie") != "")
}

func TestSessionAPI(t *testing.T) {

	var rt restTester

	// create session test users
	response := rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"1234"}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user2", "password":"1234"}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user3", "password":"1234"}`)
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
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	assertStatus(t, response, 200)

	// DELETE tests
	// 1. DELETE a session by session id
	response = rt.sendAdminRequest("DELETE", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	assertStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	assertStatus(t, response, 404)

	// 2. DELETE a session with user validation
	response = rt.sendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user1sessions[1]), "")
	assertStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[1]), "")
	assertStatus(t, response, 404)

	// 3. DELETE a session not belonging to the user (should fail)
	response = rt.sendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user2sessions[0]), "")
	assertStatus(t, response, 404)

	// GET the session and make sure it still exists
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[0]), "")
	assertStatus(t, response, 200)

	// 4. DELETE all sessions for a user
	response = rt.sendAdminRequest("DELETE", "/db/_user/user2/_session", "")
	assertStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[i]), "")
		assertStatus(t, response, 404)
	}

	// 5. DELETE sessions when password is changed
	// Change password for user3
	response = rt.sendAdminRequest("PUT", "/db/_user/user3", `{"password":"5678"}`)

	assertStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user3sessions[i]), "")
		assertStatus(t, response, 404)
	}

	// DELETE the users
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_user/user1", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_user/user1", ""), 404)

	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_user/user2", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_user/user2", ""), 404)

	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_user/user3", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_user/user3", ""), 404)

}

func TestFlush(t *testing.T) {
	var rt restTester
	rt.createDoc(t, "doc1")
	rt.createDoc(t, "doc2")
	assertStatus(t, rt.sendRequest("GET", "/db/doc1", ""), 200)
	assertStatus(t, rt.sendRequest("GET", "/db/doc2", ""), 200)

	log.Printf("Flushing db...")
	assertStatus(t, rt.sendAdminRequest("POST", "/db/_flush", ""), 200)
	rt.setAdminParty(true) // needs to be re-enabled after flush since guest user got wiped

	// After the flush, the db exists but the documents are gone:
	assertStatus(t, rt.sendRequest("GET", "/db/", ""), 200)
	assertStatus(t, rt.sendRequest("GET", "/db/doc1", ""), 404)
	assertStatus(t, rt.sendRequest("GET", "/db/doc2", ""), 404)
}

//Test a single call to take DB offline
func TestDBOfflineSingle(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)
	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")
}

//Make two concurrent calls to take DB offline
// Ensure both calls succeed and that DB is offline
// when both calls return
func TestDBOfflineConcurrent(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	//Take DB offline concurrently using two goroutines
	//Both should return success and DB should be offline
	//once both goroutines return
	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *testResponse
	go func() {
		goroutineresponse1 = rt.sendAdminRequest("POST", "/db/_offline", "")
		assertStatus(t, goroutineresponse1, 200)
		wg.Done()
	}()

	var goroutineresponse2 *testResponse
	go func() {
		goroutineresponse2 = rt.sendAdminRequest("POST", "/db/_offline", "")
		assertStatus(t, goroutineresponse2, 200)
		wg.Done()
	}()

	wg.Wait()

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

}

//Test that a DB can be created offline
func TestStartDBOffline(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)
	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")
}

//Take DB offline and ensure that normal REST calls
//fail with status 503
func TestDBOffline503Response(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	assertStatus(t, rt.sendRequest("GET", "/db/doc1", ""), 503)
}

//Take DB offline while there are active _changes feeds
// and other REST API calls are being made
//ensure that DB offline completes
func TestDBOfflineWithActiveClients(t *testing.T) {
	var rt restTester

	response := rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"letmein"}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user2", "password":"letmein"}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user3", "password":"letmein"}`)
	assertStatus(t, response, 201)

	go func(rt restTester) {
		log.Printf("Continuous changes 1: Starting")
		for i := 0; i <= 100000; i++ {
			rt.send(requestByUser("GET", "/db/_changes?feed=longpoll&since=1000", "", "user1"))
		}
		log.Printf("Continuous changes 1: Stopped")
	}(rt)

	time.Sleep(10 * time.Millisecond)

	go func(rt restTester) {
		log.Printf("Continuous changes 2: Starting")
		for i := 0; i <= 100000; i++ {
			rt.send(requestByUser("GET", "/db/_changes?feed=longpoll&since=1000", "", "user2"))
		}
		log.Printf("Continuous changes 2: Stopped")
	}(rt)

	time.Sleep(10 * time.Millisecond)

	go func(rt restTester) {
		log.Printf("Continuous changes 3: Starting")
		for i := 0; i <= 100000; i++ {
			rt.send(requestByUser("GET", "/db/_changes?feed=longpoll&since=1000", "", "user3"))
		}
		log.Printf("Continuous changes 2: Stopped")
	}(rt)

	time.Sleep(20 * time.Millisecond)

	go func(rt restTester) {
		for i := 0; i <= 100000; i++ {
			docId := fmt.Sprintf("/db/_local/loc%d", i)
			rt.sendRequest("PUT", docId, `{"hi": "there"}`)
			rt.sendRequest("PUT", docId, `{"hi": "again", "_rev": "0-1"}`)
			rt.sendRequest("GET", docId, "")
		}
	}(rt)

	time.Sleep(10 * time.Millisecond)

	go func(rt restTester) {
		for i := 0; i <= 100000; i++ {
			assertStatus(t, rt.sendAdminRequest("GET", "/todolite-cc/", ""), 404)
		}
	}(rt)

	time.Sleep(10 * time.Millisecond)

	go func(rt restTester) {

		//Create some docs
		rt.send(request("PUT", "/db/beta", `{"owner":"boadecia"}`)) // seq=2
		rt.send(request("PUT", "/db/delta", `{"owner":"alice"}`))   // seq=3
		rt.send(request("PUT", "/db/gamma", `{"owner":"zegpold"}`)) // seq=4
		rt.send(request("PUT", "/db/a1", `{"channel":"alpha"}`))    // seq=5
		rt.send(request("PUT", "/db/b1", `{"channel":"beta"}`))     // seq=6
		rt.send(request("PUT", "/db/d1", `{"channel":"delta"}`))    // seq=7
		rt.send(request("PUT", "/db/g1", `{"channel":"gamma"}`))    // seq=8

		// Create some bulk docs:
		input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
		rt.sendRequest("POST", "/db/_bulk_docs", input)

		// Now call _revs_diff:
		input = `{"rd1": ["13-def", "12-abc", "11-eleven"],
              "rd2": ["34-def", "31-one"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
		for i := 0; i <= 100000; i++ {
			rt.sendRequest("POST", "/db/_revs_diff", input)
		}

	}(rt)

	time.Sleep(10 * time.Millisecond)

	log.Printf("Taking DB offline")
	response = rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")
}

//Take DB offline and ensure can put db config
func TestDBOfflinePutDbConfig(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	assertStatus(t, rt.sendRequest("PUT", "/db/_config", ""), 404)
}

//Take DB offline and ensure can post _resync
func TestDBOfflinePostResync(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	assertStatus(t, rt.sendAdminRequest("POST", "/db/_resync", ""), 200)
}

//Take DB offline and ensure only one _resync can be in progress
func TestDBOfflineSingleResync(t *testing.T) {
	var rt restTester

	//create documents in DB to cause resync to take a few seconds
	for i := 0; i < 1000; i++ {
		rt.createDoc(t, fmt.Sprintf("doc%v", i))
	}

	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	response = rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	go func() {
		assertStatus(t, rt.sendAdminRequest("POST", "/db/_resync", ""), 200)
	}()

	// Allow goroutine to get scheduled
	time.Sleep(50 * time.Millisecond)

	assertStatus(t, rt.sendAdminRequest("POST", "/db/_resync", ""), 503)
}

// Single threaded bring DB online
func TestDBOnlineSingle(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	rt.sendAdminRequest("POST", "/db/_online", "")
	assertStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")
}

//Take DB online concurrently using two goroutines
//Both should return success and DB should be online
//once both goroutines return
func TestDBOnlineConcurrent(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *testResponse
	go func(rt restTester) {
		defer wg.Done()
		goroutineresponse1 = rt.sendAdminRequest("POST", "/db/_online", "")
		assertStatus(t, goroutineresponse1, 200)
	}(rt)

	var goroutineresponse2 *testResponse
	go func(rt restTester) {
		defer wg.Done()
		goroutineresponse2 = rt.sendAdminRequest("POST", "/db/_online", "")
		assertStatus(t, goroutineresponse2, 200)
	}(rt)

	wg.Wait()

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")
}

// Test bring DB online with delay of 1 second
func TestSingleDBOnlineWithDelay(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	rt.sendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	time.Sleep(1500 * time.Millisecond)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")
}

// Test bring DB online with delay of 2 seconds
// But bring DB online immediately in separate call
// BD should should only be brought online once
// there should be no errors
func TestDBOnlineWithDelayAndImmediate(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	//Bring DB online with delay of two seconds
	rt.sendAdminRequest("POST", "/db/_online", "{\"delay\":2}")
	assertStatus(t, response, 200)

	// Bring DB online immediately
	rt.sendAdminRequest("POST", "/db/_online", "")
	assertStatus(t, response, 200)

	//Allow online goroutine to get scheduled
	time.Sleep(500 * time.Millisecond)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	time.Sleep(2500 * time.Millisecond)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")
}

// Test bring DB online concurrently with delay of 1 second
// and delay of 2 seconds
// BD should should only be brought online once
// there should be no errors
func TestDBOnlineWithTwoDelays(t *testing.T) {
	var rt restTester
	log.Printf("Taking DB offline")
	response := rt.sendAdminRequest("GET", "/db/", "")
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	rt.sendAdminRequest("POST", "/db/_offline", "")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	//Bring DB online with delay of one seconds
	rt.sendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	assertStatus(t, response, 200)

	//Bring DB online with delay of two seconds
	rt.sendAdminRequest("POST", "/db/_online", "{\"delay\":2}")
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Offline")

	time.Sleep(1500 * time.Millisecond)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")

	time.Sleep(600 * time.Millisecond)

	response = rt.sendAdminRequest("GET", "/db/", "")
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.True(t, body["state"].(string) == "Online")
}

func (rt *restTester) createSession(t *testing.T, username string) string {

	response := rt.sendAdminRequest("POST", "/db/_session", fmt.Sprintf(`{"name":%q}`, username))
	assertStatus(t, response, 200)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	sessionId := body["session_id"].(string)

	return sessionId
}

func TestPurgeWithBadJsonPayload(t *testing.T) {
	var rt restTester
	response := rt.sendAdminRequest("POST", "/db/_purge", "foo")
	assertStatus(t, response, 400)
}

func TestPurgeWithNonArrayRevisionList(t *testing.T) {
	var rt restTester
	response := rt.sendAdminRequest("POST", "/db/_purge", `{"foo":"list"}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithEmptyRevisionList(t *testing.T) {
	var rt restTester
	response := rt.sendAdminRequest("POST", "/db/_purge", `{"foo":[]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithGreaterThanOneRevision(t *testing.T) {
	var rt restTester
	response := rt.sendAdminRequest("POST", "/db/_purge", `{"foo":["rev1","rev2"]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithNonStarRevision(t *testing.T) {
	var rt restTester
	response := rt.sendAdminRequest("POST", "/db/_purge", `{"foo":["rev1"]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{}})
}

func TestPurgeWithStarRevision(t *testing.T) {
	var rt restTester

	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)

	response := rt.sendAdminRequest("POST", "/db/_purge", `{"doc1":["*"]}`)
	assertStatus(t, response, 200)
	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}})

	//Create new versions of the doc1 without conflicts
	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
}

func TestPurgeWithMultipleValidDocs(t *testing.T) {
	var rt restTester
	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)

	response := rt.sendAdminRequest("POST", "/db/_purge", `{"doc1":["*"],"doc2":["*"]}`)
	assertStatus(t, response, 200)

	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}, "doc2": []interface{}{"*"}}})

	//Create new versions of the docs without conflicts
	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)
}

func TestPurgeWithSomeInvalidDocs(t *testing.T) {
	var rt restTester
	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)

	response := rt.sendAdminRequest("POST", "/db/_purge", `{"doc1":["*"],"doc2":["1-123"]}`)
	assertStatus(t, response, 200)
	var body map[string]interface{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}})

	//Create new versions of the doc1 without conflicts
	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)

	//Create new versions of the doc2 fails because it already exists
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"moo":"car"}`), 409)
}

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
	assert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet{"bar": 0x1, "foo": 0x1})
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
func TestUserAccessRace(t *testing.T) {

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

	log.Printf("hello")
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

func (rt *restTester) createSession(t *testing.T, username string) string {

	response := rt.sendAdminRequest("POST", "/db/_session", fmt.Sprintf(`{"name":%q}`, username))
	assertStatus(t, response, 200)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	sessionId := body["session_id"].(string)

	return sessionId
}

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
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
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
	assert.DeepEquals(t, body["all_channels"], []interface{}{"bar", "foo"})
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
	assert.DeepEquals(t, body["all_channels"], []interface{}{"bar", "fedoras", "fixies", "foo"})

	// DELETE the user
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/_user/snej", ""), 200)
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
	rt := restTester{noAdminParty: true}
	response := rt.sendAdminRequest("GET", "/db/_user/GUEST", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "GUEST")
	// This ain't no admin-party, this ain't no nightclub, this ain't no fooling around:
	assert.DeepEquals(t, body["admin_channels"], nil)

	response = rt.sendAdminRequest("PUT", "/db/_user/GUEST", `{"disabled":true}`)
	assertStatus(t, response, 200)

	response = rt.sendAdminRequest("GET", "/db/_user/GUEST", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "GUEST")
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
	assertStatus(t, response, 200)

	// GET the session and make sure it still exists
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[0]), "")
	assertStatus(t, response, 200)

	// 4. DELETE multiple sessions for a user
	response = rt.sendAdminRequest("DELETE", "/db/_user/user1/_session", fmt.Sprintf(`{"keys":[%q,%q]}`, user1sessions[2], user1sessions[3]))
	assertStatus(t, response, 200)

	// Validate that multiple sessions were deleted
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[2]), "")
	assertStatus(t, response, 404)
	response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[3]), "")
	assertStatus(t, response, 404)

	// 5. DELETE all sessions for a user
	response = rt.sendAdminRequest("DELETE", "/db/_user/user2/_session", `{"keys":["*"]}`)
	assertStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.sendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[i]), "")
		assertStatus(t, response, 404)
	}

	// 6. DELETE sessions when password is changed
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

func (rt *restTester) createSession(t *testing.T, username string) string {

	response := rt.sendAdminRequest("POST", "/db/_session", fmt.Sprintf(`{"name":%q}`, username))
	assertStatus(t, response, 200)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	sessionId := body["session_id"].(string)

	return sessionId
}

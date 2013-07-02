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
	"testing"

	"github.com/couchbaselabs/go.assert"

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
	assert.Equals(t, body["password"], nil)

	// Check the list of all users:
	response = rt.sendAdminRequest("GET", "/db/_user/", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), `["snej"]`)

	// Check that the actual User object is correct:
	user, _ := rt.serverContext().databases["db"].auth.GetUser("snej")
	assert.Equals(t, user.Name(), "snej")
	assert.Equals(t, user.Email(), "jens@couchbase.com")
	assert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet{"bar": 0x1, "foo": 0x1})
	assert.True(t, user.Authenticate("letmein"))

	// Change the password and verify it:
	response = rt.sendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"123", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	user, _ = rt.serverContext().databases["db"].auth.GetUser("snej")
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
	assertStatus(t, response, 201)

	response = rt.sendAdminRequest("GET", "/db/_user/GUEST", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "GUEST")
	assert.DeepEquals(t, body["disabled"], true)

	// Check that the actual User object is correct:
	user, _ := rt.serverContext().databases["db"].auth.GetUser("")
	assert.Equals(t, user.Name(), "")
	assert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet(nil))
	assert.Equals(t, user.Disabled(), true)
}

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

	"github.com/couchbaselabs/sync_gateway/db"
)

func TestUserAPI(t *testing.T) {
	// PUT a user
	var rt restTester
	assertStatus(t, rt.sendAdminRequest("GET", "/db/user/snej", ""), 404)
	response := rt.sendAdminRequest("PUT", "/db/user/snej", `{"password":"letmein", "admin_channels":{"foo": 1, "bar": 1}}`)
	assertStatus(t, response, 201)

	// GET the user and make sure the result is OK
	response = rt.sendAdminRequest("GET", "/db/user/snej", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "snej")
	assert.DeepEquals(t, body["admin_channels"], map[string]interface{}{"foo": 1.0, "bar": 1.0})
	assert.Equals(t, body["password"], nil)

	response = rt.sendAdminRequest("GET", "/db/user", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), `["snej"]`)

	// DELETE the user
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/user/snej", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/user/snej", ""), 404)

	// POST a user
	response = rt.sendAdminRequest("POST", "/db/user", `{"name":"snej", "password":"letmein", "admin_channels":{"foo": 1, "bar": 1}}`)
	assertStatus(t, response, 301)
	response = rt.sendAdminRequest("POST", "/db/user/", `{"name":"snej", "password":"letmein", "admin_channels":{"foo": 1, "bar": 1}}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "snej")
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/user/snej", ""), 200)
}

func TestRoleAPI(t *testing.T) {
	var rt restTester
	// PUT a role
	assertStatus(t, rt.sendAdminRequest("GET", "/db/role/hipster", ""), 404)
	response := rt.sendAdminRequest("PUT", "/db/role/hipster", `{"admin_channels":{"fedoras": 1, "fixies": 2}}`)
	assertStatus(t, response, 201)

	// GET the role and make sure the result is OK
	response = rt.sendAdminRequest("GET", "/db/role/hipster", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "hipster")
	assert.DeepEquals(t, body["admin_channels"], map[string]interface{}{"fedoras": 1.0, "fixies": 2.0})
	assert.Equals(t, body["password"], nil)

	response = rt.sendAdminRequest("GET", "/db/role", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), `["hipster"]`)

	// DELETE the role
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/role/hipster", ""), 200)
	assertStatus(t, rt.sendAdminRequest("GET", "/db/role/hipster", ""), 404)

	// POST a role
	response = rt.sendAdminRequest("POST", "/db/role", `{"name":"hipster", "admin_channels":{"fedoras": 1, "fixies": 2}}`)
	assertStatus(t, response, 301)
	response = rt.sendAdminRequest("POST", "/db/role/", `{"name":"hipster", "admin_channels":{"fedoras": 1, "fixies": 2}}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/role/hipster", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "hipster")
	assertStatus(t, rt.sendAdminRequest("DELETE", "/db/role/hipster", ""), 200)
}

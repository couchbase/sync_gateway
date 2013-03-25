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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchbaselabs/go.assert"

	"github.com/couchbaselabs/sync_gateway/db"
)

func callAuthREST(method, resource string, body string) *httptest.ResponseRecorder {
	sc := newServerContext(&ServerConfig{})
	if _, err := sc.addDatabase(gTestBucket, "db", "", false); err != nil {
		panic(fmt.Sprintf("Error from addDatabase: %v", err))
	}
	authHandler := createAuthHandler(sc)

	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := httptest.NewRecorder()
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	authHandler.ServeHTTP(response, request)
	return response
}

func TestUserAPI(t *testing.T) {
	// PUT a user
	assertStatus(t, callAuthREST("GET", "/db/user/snej", ""), 404)
	response := callAuthREST("PUT", "/db/user/snej", `{"password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// GET the user and make sure the result is OK
	response = callAuthREST("GET", "/db/user/snej", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "snej")
	assert.DeepEquals(t, body["admin_channels"], []interface{}{"bar", "foo"})
	assert.Equals(t, body["password"], nil)

	// DELETE the user
	assertStatus(t, callAuthREST("DELETE", "/db/user/snej", ""), 200)
	assertStatus(t, callAuthREST("GET", "/db/user/snej", ""), 404)

	// POST a user
	response = callAuthREST("POST", "/db/user", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)
	response = callAuthREST("GET", "/db/user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "snej")
	assertStatus(t, callAuthREST("DELETE", "/db/user/snej", ""), 200)
}

func TestRoleAPI(t *testing.T) {
	// PUT a role
	assertStatus(t, callAuthREST("GET", "/db/role/hipster", ""), 404)
	response := callAuthREST("PUT", "/db/role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)

	// GET the role and make sure the result is OK
	response = callAuthREST("GET", "/db/role/hipster", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "hipster")
	assert.DeepEquals(t, body["admin_channels"], []interface{}{"fedoras", "fixies"})
	assert.Equals(t, body["password"], nil)

	// DELETE the role
	assertStatus(t, callAuthREST("DELETE", "/db/role/hipster", ""), 200)
	assertStatus(t, callAuthREST("GET", "/db/role/hipster", ""), 404)

	// POST a role
	response = callAuthREST("POST", "/db/role", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)
	response = callAuthREST("GET", "/db/role/hipster", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "hipster")
	assertStatus(t, callAuthREST("DELETE", "/db/role/hipster", ""), 200)
}

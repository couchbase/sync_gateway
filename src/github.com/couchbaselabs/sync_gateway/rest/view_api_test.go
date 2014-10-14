//  Copyright (c) 2012 Couchbase, Inc.
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
	"github.com/couchbaselabs/walrus"
)

func TestDesignDocs(t *testing.T) {
	var rt restTester
	response := rt.sendRequest("GET", "/db/_design/foo", "")
	assertStatus(t, response, 403)
	response = rt.sendRequest("PUT", "/db/_design/foo", `{"prop":true}`)
	assertStatus(t, response, 403)
	response = rt.sendRequest("DELETE", "/db/_design/foo", "")
	assertStatus(t, response, 403)

	response = rt.sendAdminRequest("GET", "/db/_design/foo", "")
	assertStatus(t, response, 404)
	response = rt.sendAdminRequest("PUT", "/db/_design/foo", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/_design/foo", "")
	assertStatus(t, response, 200)
	response = rt.sendAdminRequest("GET", "/db/_design%2ffoo", "")
	assertStatus(t, response, 200)
	response = rt.sendAdminRequest("GET", "/db/_design%2Ffoo", "")
	assertStatus(t, response, 200)
	response = rt.sendAdminRequest("DELETE", "/db/_design/foo", "")
	assertStatus(t, response, 200)
	response = rt.sendAdminRequest("PUT", "/db/_design/sync_gateway", "{}")
	assertStatus(t, response, 403)
	response = rt.sendAdminRequest("GET", "/db/_design/sync_gateway", "")
	assertStatus(t, response, 200)
}

func TestViewQuery(t *testing.T) {
	var rt restTester
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc1", `{"key":10, "value":"ten"}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc2", `{"key":7, "value":"seven"}`)
	assertStatus(t, response, 201)

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar", ``)
	assertStatus(t, response, 200)
	var result walrus.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})
	assert.DeepEquals(t, result.Rows[1], &walrus.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?endkey=9", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?endkey=9&include_docs=true", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, *result.Rows[0].Doc, map[string]interface{}{"key": 7.0, "value": "seven"})
}

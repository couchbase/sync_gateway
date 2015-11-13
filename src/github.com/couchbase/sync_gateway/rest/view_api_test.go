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
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/channels"

	"github.com/couchbaselabs/go.assert"
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
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?endkey=9", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?endkey=9&include_docs=true", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, *result.Rows[0].Doc, map[string]interface{}{"key": 7.0, "value": "seven"})
}

//Tests #1109, where design doc contains multiple views
func TestViewQueryMultipleViews(t *testing.T) {
	var rt restTester
	//Define three views
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, null); }"}}}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, 201)

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/by_age", ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface {}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface {}(nil)})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/by_fname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice" , Value: interface {}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob" , Value: interface {}(nil)})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/by_lname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "Seven" , Value: interface {}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "Ten" , Value: interface {}(nil)})
}

//Waiting for a fix for couchbaselabs/Walrus #13
//Currently fails against walrus bucket as '_sync' property will exist in doc object if it is emmitted in the map function
func failingTestViewQueryMultipleViews(t *testing.T) {
	var rt restTester
	//Define three views
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, doc); }"}}}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, 201)

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/by_age", ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface {}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface {}(nil)})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/by_fname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice" , Value: interface {}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob" , Value: interface {}(nil)})

	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/by_lname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "Seven" , Value: interface {}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "Ten" , Value: interface {}(nil)})
}

func TestUserViewQuery(t *testing.T) {
	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}
	a := rt.ServerContext().Database("db").Authenticator()
	// Create a view:
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	// Create docs:
	response = rt.sendRequest("PUT", "/db/doc1", `{"key":10, "value":"ten", "channel":"W"}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("PUT", "/db/doc2", `{"key":7, "value":"seven", "channel":"Q"}`)
	assertStatus(t, response, 201)
	// Create a user:
	quinn, _ := a.NewUser("quinn", "123456", channels.SetOf("Q", "q"))
	a.Save(quinn)

	// Have the user query the view:
	request, _ := http.NewRequest("GET", "/db/_design/foo/_view/bar?include_docs=true", nil)
	request.SetBasicAuth("quinn", "123456")
	response = rt.send(request)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	row := result.Rows[0]
	assert.Equals(t, row.Key, float64(7))
	assert.Equals(t, row.Value, "seven")
	assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})

	// Admin should see both rows:
	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	row = result.Rows[0]
	assert.Equals(t, row.Key, float64(7))
	assert.Equals(t, row.Value, "seven")
	assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})
	row = result.Rows[1]
	assert.Equals(t, row.Key, float64(10))
	assert.Equals(t, row.Value, "ten")
	assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 10.0, "value": "ten", "channel": "W"})

	// Make sure users are not allowed to query internal views:
	request, _ = http.NewRequest("GET", "/db/_design/sync_gateway/_view/access", nil)
	request.SetBasicAuth("quinn", "123456")
	response = rt.send(request)
	assertStatus(t, response, 403)
}

// This includes a fix for #857
func TestAdminReduceViewQuery(t *testing.T) {

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}

	// Create a view with a reduce:
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}", "reduce": "_count"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", i), `{"key":0, "value":"0", "channel":"W"}`)
		assertStatus(t, response, 201)

	}
	response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":1, "value":"0", "channel":"W"}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true", ``)
	assertStatus(t, response, 200)

	json.Unmarshal(response.Body.Bytes(), &result)

	// we should get 1 row with the reduce result
	assert.Equals(t, len(result.Rows), 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.True(t, value == 10)

	// todo support group reduce, see #955
	// // test group=true
	// response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group=true", ``)
	// assertStatus(t, response, 200)
	// json.Unmarshal(response.Body.Bytes(), &result)
	// // we should get 2 rows with the reduce result
	// assert.Equals(t, len(result.Rows), 2)
	// row = result.Rows[0]
	// value = row.Value.(float64)
	// assert.True(t, value == 9)
	// row = result.Rows[1]
	// value = row.Value.(float64)
	// assert.True(t, value == 1)
}

func TestAdminReduceSumQuery(t *testing.T) {

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}

	// Create a view with a reduce:
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		assertStatus(t, response, 201)

	}
	response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)

	// we should get 1 row with the reduce result
	assert.Equals(t, len(result.Rows), 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.Equals(t, value, 108.0)
}

func TestAdminGroupReduceSumQuery(t *testing.T) {

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}

	// Create a view with a reduce:
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		assertStatus(t, response, 201)

	}
	response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group=true", ``)
	assertStatus(t, response, 200)
	// fmt.Println(response.Body)

	json.Unmarshal(response.Body.Bytes(), &result)

	// we should get 2 row with the reduce result
	assert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equals(t, value, 99.0)
}

func TestAdminGroupLevelReduceSumQuery(t *testing.T) {

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}

	// Create a view with a reduce:
	response := rt.sendAdminRequest("PUT", "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", i), fmt.Sprintf(`{"key":["A",{},%v], "value":1}`, i))
		assertStatus(t, response, 201)

	}
	response = rt.sendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":["B",4,1], "value":99}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group_level=2", ``)
	assertStatus(t, response, 200)
	// fmt.Println(response.Body)

	json.Unmarshal(response.Body.Bytes(), &result)

	// we should get 2 row with the reduce result
	assert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equals(t, value, 99.0)
}

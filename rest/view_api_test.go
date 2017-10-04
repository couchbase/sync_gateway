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
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/channels"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbase/sync_gateway/base"
)

func TestDesignDocs(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works under walrus -- see https://github.com/couchbase/sync_gateway/issues/2954")
	}

	var rt RestTester
	defer rt.Close()

	response := rt.SendRequest("GET", "/db/_design/foo", "")
	assertStatus(t, response, 403)
	response = rt.SendRequest("PUT", "/db/_design/foo", `{"prop":true}`)
	assertStatus(t, response, 403)
	response = rt.SendRequest("DELETE", "/db/_design/foo", "")
	assertStatus(t, response, 403)

	response = rt.SendAdminRequest("GET", "/db/_design/foo", "")
	assertStatus(t, response, 404)
	response = rt.SendAdminRequest("PUT", "/db/_design/foo", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_design/foo", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/_design%2ffoo", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/_design%2Ffoo", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("DELETE", "/db/_design/foo", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("PUT", "/db/_design/sync_gateway", "{}")
	assertStatus(t, response, 403)
	response = rt.SendAdminRequest("GET", "/db/_design/sync_gateway", "")
	assertStatus(t, response, 200)
}

func TestViewQuery(t *testing.T) {

	// TODO: this test is failing when run against actual couchbase bucket

	var rt RestTester
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"key":10, "value":"ten"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"key":7, "value":"seven"}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar", ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?limit=1", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?endkey=9", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?endkey=9&include_docs=true", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, *result.Rows[0].Doc, map[string]interface{}{"key": 7.0, "value": "seven"})
}

//Tests #1109, where design doc contains multiple views
func TestViewQueryMultipleViews(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	//Define three views
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, null); }"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_age", ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_fname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_lname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)})
}

func TestViewQueryUserAccess(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	rt.ServerContext().Database("db").SetUserViewsEnabled(true)
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map":"function (doc, meta) { if (doc.type != 'type1') { return; } if (doc.state == 'state1' || doc.state == 'state2' || doc.state == 'state3') { emit(doc.state, meta.id); }}"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"type":"type1", "state":"state1"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"type":"type1", "state":"state2"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc3", `{"type":"type2", "state":"state2"}`)
	assertStatus(t, response, 201)

	time.Sleep(5 * time.Second)

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?stale=false", ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?stale=false", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	// Create a user:
	a := rt.ServerContext().Database("db").Authenticator()
	testUser, _ := a.NewUser("testUser", "123456", channels.SetOf("*"))
	a.Save(testUser)

	var userResult sgbucket.ViewResult
	request, _ := http.NewRequest("GET", "/db/_design/foo/_view/bar?stale=false", nil)
	request.SetBasicAuth("testUser", "123456")
	userResponse := rt.Send(request)
	assertStatus(t, userResponse, 200)
	json.Unmarshal(userResponse.Body.Bytes(), &userResult)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	// Disable user view access, retry
	rt.ServerContext().Database("db").SetUserViewsEnabled(false)
	request, _ = http.NewRequest("GET", "/db/_design/foo/_view/bar?stale=false", nil)
	request.SetBasicAuth("testUser", "123456")
	userResponse = rt.Send(request)
	assertStatus(t, userResponse, 403)
}

//Waiting for a fix for couchbaselabs/Walrus #13
//Currently fails against walrus bucket as '_sync' property will exist in doc object if it is emmitted in the map function
func failingTestViewQueryMultipleViews(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	//Define three views
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, doc); }"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_age", ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_fname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_lname", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)})
}

func TestUserViewQuery(t *testing.T) {
	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	rt.ServerContext().Database("db").SetUserViewsEnabled(true)
	// Create a view:
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	// Create docs:
	response = rt.SendRequest("PUT", "/db/doc1", `{"key":10, "value":"ten", "channel":"W"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"key":7, "value":"seven", "channel":"Q"}`)
	assertStatus(t, response, 201)
	// Create a user:
	quinn, _ := a.NewUser("quinn", "123456", channels.SetOf("Q", "q"))
	a.Save(quinn)

	// Have the user query the view:
	request, _ := http.NewRequest("GET", "/db/_design/foo/_view/bar?include_docs=true", nil)
	request.SetBasicAuth("quinn", "123456")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)
	assert.Equals(t, result.TotalRows, 1)
	row := result.Rows[0]
	assert.Equals(t, row.Key, float64(7))
	assert.Equals(t, row.Value, "seven")
	assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})

	// Admin should see both rows:
	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar", ``)
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
	response = rt.Send(request)
	assertStatus(t, response, 403)
}

// This includes a fix for #857
func TestAdminReduceViewQuery(t *testing.T) {

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}", "reduce": "_count"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", i), `{"key":0, "value":"0", "channel":"W"}`)
		assertStatus(t, response, 201)

	}
	response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":1, "value":"0", "channel":"W"}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true", ``)
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

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		assertStatus(t, response, 201)

	}
	response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true", ``)
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &result)

	// we should get 1 row with the reduce result
	assert.Equals(t, len(result.Rows), 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.Equals(t, value, 108.0)
}

func TestAdminGroupReduceSumQuery(t *testing.T) {

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		assertStatus(t, response, 201)

	}
	response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group=true", ``)
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

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, 201)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", i), fmt.Sprintf(`{"key":["A",{},%v], "value":1}`, i))
		assertStatus(t, response, 201)

	}
	response = rt.SendRequest("PUT", fmt.Sprintf("/db/doc%v", 10), `{"key":["B",4,1], "value":99}`)
	assertStatus(t, response, 201)

	var result sgbucket.ViewResult

	// Admin view query:
	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group_level=2", ``)
	assertStatus(t, response, 200)
	// fmt.Println(response.Body)

	json.Unmarshal(response.Body.Bytes(), &result)

	// we should get 2 row with the reduce result
	assert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equals(t, value, 99.0)
}

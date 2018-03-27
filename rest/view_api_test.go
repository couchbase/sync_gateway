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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
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
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_design/%s", db.DesignDocSyncGateway()), "{}")
	assertStatus(t, response, 403)
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_design/%s", db.DesignDocSyncGateway()), "")
	assertStatus(t, response, 200)
}

func TestViewQuery(t *testing.T) {

	var rt RestTester
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"key":10, "value":"ten"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"key":7, "value":"seven"}`)
	assertStatus(t, response, 201)

	// The wait is needed here because the query does not have stale=false.
	// TODO: update the query to use stale=false and remove the wait
	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	assertNoError(t, err, "Got unexpected error")
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"})

	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?limit=1")
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?endkey=9")
	assert.Equals(t, len(result.Rows), 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?include_docs=true&endkey=9")
		assert.Equals(t, len(result.Rows), 1)
		assert.DeepEquals(t, *result.Rows[0].Doc, map[string]interface{}{"key": 7.0, "value": "seven"})
	}

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

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_age")
	assertNoError(t, err, "Unexpected error")
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)})

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_fname")
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)})

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_lname")
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

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")
	assertNoError(t, err, "Unexpected error")
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	// Create a user:
	a := rt.ServerContext().Database("db").Authenticator()
	password := "123456"
	testUser, _ := a.NewUser("testUser", password, channels.SetOf("*"))
	a.Save(testUser)

	result, err = rt.WaitForNUserViewResults(2, "/db/_design/foo/_view/bar?stale=false", testUser, password)
	assertNoError(t, err, "Unexpected error")
	assert.Equals(t, len(result.Rows), 2)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	// Disable user view access, retry
	rt.ServerContext().Database("db").SetUserViewsEnabled(false)
	request, _ := http.NewRequest("GET", "/db/_design/foo/_view/bar?stale=false", nil)
	request.SetBasicAuth(testUser.Name(), password)
	userResponse := rt.Send(request)
	assertStatus(t, userResponse, 403)
}

func TestViewQueryMultipleViewsInterfaceValues(t *testing.T) {
	// TODO: Waiting for a fix for couchbaselabs/Walrus #13
	// Currently fails against walrus bucket as '_sync' property will exist in doc object if it is emmitted in the map function
	t.Skip("WARNING: TEST DISABLED")

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
	password := "123456"
	quinn, _ := a.NewUser("quinn", password, channels.SetOf("Q", "q"))
	a.Save(quinn)

	// Have the user query the view:

	result, err := rt.WaitForNUserViewResults(1, "/db/_design/foo/_view/bar?include_docs=true", quinn, password)
	assertNoError(t, err, "Unexpected error")
	assert.Equals(t, len(result.Rows), 1)
	assert.Equals(t, result.TotalRows, 1)
	row := result.Rows[0]
	assert.Equals(t, row.Key, float64(7))
	assert.Equals(t, row.Value, "seven")

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})
	}

	// Admin should see both rows:

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	assertNoError(t, err, "Unexpected error")
	assert.Equals(t, len(result.Rows), 2)
	row = result.Rows[0]
	assert.Equals(t, row.Key, float64(7))
	assert.Equals(t, row.Value, "seven")
	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})
	}
	row = result.Rows[1]
	assert.Equals(t, row.Key, float64(10))
	assert.Equals(t, row.Value, "ten")

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		assert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 10.0, "value": "ten", "channel": "W"})
	}

	// Make sure users are not allowed to query internal views:
	request, _ := http.NewRequest("GET", "/db/_design/sync_gateway/_view/access", nil)
	request.SetBasicAuth(quinn.Name(), password)
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
	result, err := rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?reduce=true")
	assertNoError(t, err, "Unexpected error")

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
	result, err := rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?reduce=true")
	assertNoError(t, err, "Unexpected error")

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
	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?reduce=true&group=true")
	assertNoError(t, err, "Unexpected error")

	// we should get 2 row with the reduce result
	assert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equals(t, value, 99.0)
}

// Reproduces SG #3344.  Original issue only reproducible against Couchbase server (non-walrus)
func TestViewQueryWithKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	assertNoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/bar"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest("PUT", "/db/query_with_keys", `{"key":"channel_a", "value":99}`)
	assertStatus(t, response, 201)

	// Admin view query:
	viewUrlPath := "/db/_design/foo/_view/bar?keys=%5B%22channel_a%22%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200) // Query string was parsed properly

	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)

	// Ensure that query for non-existent keys returns no rows
	viewUrlPath = "/db/_design/foo/_view/bar?keys=%5B%22channel_b%22%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200) // Query string was parsed properly

	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 0)

}

func TestViewQueryWithCompositeKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"composite_key_test": {"map": "function(doc) {emit([doc.key, doc.seq], doc.value);}"}}}`)
	assertStatus(t, response, 201)
	assertNoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/composite_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest("PUT", "/db/doc_composite_key", `{"key":"channel_a", "seq":55, "value":99}`)
	assertStatus(t, response, 201)

	// Admin view query:
	//   keys:[["channel_a", 55]]
	viewUrlPath := "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_a%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)

	// Ensure that a query for non-existent key returns no rows
	viewUrlPath = "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_b%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)

	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 0)
}

func TestViewQueryWithIntKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"int_key_test": {"map": "function(doc) {emit(doc.seq, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	assertNoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/int_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest("PUT", "/db/doc_int_key", `{"key":"channel_a", "seq":55, "value":99}`)
	assertStatus(t, response, 201)

	// Admin view query:
	//   keys:[55,65]
	viewUrlPath := "/db/_design/foo/_view/int_key_test?keys=%5B55,65%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 1)

	// Ensure that a query for non-existent key returns no rows
	//   keys:[65,75]
	viewUrlPath = "/db/_design/foo/_view/int_key_test?keys=%5B65,75%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)

	json.Unmarshal(response.Body.Bytes(), &result)
	assert.Equals(t, len(result.Rows), 0)
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
	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?reduce=true&group_level=2")
	assertNoError(t, err, "Unexpected error")

	// we should get 2 row with the reduce result
	assert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equals(t, value, 99.0)
}

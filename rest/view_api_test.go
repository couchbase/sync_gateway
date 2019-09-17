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
	"fmt"
	"net/http"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func TestDesignDocs(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works under walrus -- see https://github.com/couchbase/sync_gateway/issues/2954")
	}

	rt := NewRestTester(t, nil)
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

	rt := NewRestTester(t, nil)
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
	assert.NoError(t, err, "Got unexpected error")
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"})

	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?limit=1")
	goassert.Equals(t, len(result.Rows), 1)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?endkey=9")
	goassert.Equals(t, len(result.Rows), 1)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"})

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?include_docs=true&endkey=9")
		goassert.Equals(t, len(result.Rows), 1)
		goassert.DeepEquals(t, *result.Rows[0].Doc, map[string]interface{}{"key": 7.0, "value": "seven"})
	}

}

//Tests #1109, wh ere design doc contains multiple views
func TestViewQueryMultipleViews(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	//Define three views
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, null); }"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, 201)

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_age")
	assert.NoError(t, err, "Unexpected error")
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)})

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_fname")
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)})

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_lname")
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)})
}

func TestViewQueryWithParams(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc1", `{"value": "foo", "key": "test1"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("PUT", "/db/doc2", `{"value": "foo", "key": "test2"}`)
	assertStatus(t, response, 201)

	result, err := rt.WaitForNAdminViewResults(2, `/db/_design/foodoc/_view/foobarview?conflicts=true&descending=false&endkey="test2"&endkey_docid=doc2&end_key_doc_id=doc2&startkey="test1"&startkey_docid=doc1`)
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, 2, len(result.Rows))
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc1", Key: "test1", Value: interface{}(nil)})
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc2", Key: "test2", Value: interface{}(nil)})

	result, err = rt.WaitForNAdminViewResults(2, `/db/_design/foodoc/_view/foobarview?conflicts=true&descending=false&conflicts=true&descending=false&keys=["test1", "test2"]`)
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, 2, len(result.Rows))
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc1", Key: "test1", Value: interface{}(nil)})
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc2", Key: "test2", Value: interface{}(nil)})
}

func TestViewQueryUserAccess(t *testing.T) {

	rt := NewRestTester(t, nil)
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
	assert.NoError(t, err, "Unexpected error in WaitForNAdminViewResults")
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")
	assert.NoError(t, err, "Unexpected error in WaitForNAdminViewResults")

	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

	// Create a user:
	a := rt.ServerContext().Database("db").Authenticator()
	password := "123456"
	testUser, _ := a.NewUser("testUser", password, channels.SetOf(t, "*"))
	a.Save(testUser)

	result, err = rt.WaitForNUserViewResults(2, "/db/_design/foo/_view/bar?stale=false", testUser, password)
	assert.NoError(t, err, "Unexpected error in WaitForNUserViewResults")

	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"})

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

	rt := NewRestTester(t, nil)
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
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_fname", ``)
	assertStatus(t, response, 200)
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)})

	response = rt.SendAdminRequest("GET", "/db/_design/foo/_view/by_lname", ``)
	assertStatus(t, response, 200)
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 2)
	goassert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)})
	goassert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)})
}

func TestUserViewQuery(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
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
	quinn, _ := a.NewUser("quinn", password, channels.SetOf(t, "Q", "q"))
	a.Save(quinn)

	// Have the user query the view:

	result, err := rt.WaitForNUserViewResults(1, "/db/_design/foo/_view/bar?include_docs=true", quinn, password)
	assert.NoError(t, err, "Unexpected error")
	goassert.Equals(t, len(result.Rows), 1)
	goassert.Equals(t, result.TotalRows, 1)
	row := result.Rows[0]
	goassert.Equals(t, row.Key, float64(7))
	goassert.Equals(t, row.Value, "seven")

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		goassert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})
	}

	// Admin should see both rows:

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	assert.NoError(t, err, "Unexpected error")
	goassert.Equals(t, len(result.Rows), 2)
	row = result.Rows[0]
	goassert.Equals(t, row.Key, float64(7))
	goassert.Equals(t, row.Value, "seven")
	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		goassert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"})
	}
	row = result.Rows[1]
	goassert.Equals(t, row.Key, float64(10))
	goassert.Equals(t, row.Value, "ten")

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		goassert.DeepEquals(t, *row.Doc, map[string]interface{}{"key": 10.0, "value": "ten", "channel": "W"})
	}

	// Make sure users are not allowed to query internal views:
	request, _ := http.NewRequest("GET", "/db/_design/sync_gateway/_view/access", nil)
	request.SetBasicAuth(quinn.Name(), password)
	response = rt.Send(request)
	assertStatus(t, response, 403)
}

// This includes a fix for #857
func TestAdminReduceViewQuery(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
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
	assert.NoError(t, err, "Unexpected error")

	// we should get 1 row with the reduce result
	goassert.Equals(t, len(result.Rows), 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	goassert.True(t, value == 10)

	// todo support group reduce, see #955
	// // test group=true
	// response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group=true", ``)
	// assertStatus(t, response, 200)
	// base.JSONUnmarshal(response.Body.Bytes(), &result)
	// // we should get 2 rows with the reduce result
	// goassert.Equals(t, len(result.Rows), 2)
	// row = result.Rows[0]
	// value = row.Value.(float64)
	// goassert.True(t, value == 9)
	// row = result.Rows[1]
	// value = row.Value.(float64)
	// goassert.True(t, value == 1)
}

func TestAdminReduceSumQuery(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
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
	assert.NoError(t, err, "Unexpected error")

	// we should get 1 row with the reduce result
	goassert.Equals(t, len(result.Rows), 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	goassert.Equals(t, value, 108.0)
}

func TestAdminGroupReduceSumQuery(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
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
	assert.NoError(t, err, "Unexpected error")

	// we should get 2 row with the reduce result
	goassert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	goassert.Equals(t, value, 99.0)
}

// Reproduces SG #3344.  Original issue only reproducible against Couchbase server (non-walrus)
func TestViewQueryWithKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/bar"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest("PUT", "/db/query_with_keys", `{"key":"channel_a", "value":99}`)
	assertStatus(t, response, 201)

	// Admin view query:
	viewUrlPath := "/db/_design/foo/_view/bar?keys=%5B%22channel_a%22%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200) // Query string was parsed properly

	var result sgbucket.ViewResult
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 1)

	// Ensure that query for non-existent keys returns no rows
	viewUrlPath = "/db/_design/foo/_view/bar?keys=%5B%22channel_b%22%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200) // Query string was parsed properly

	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 0)

}

func TestViewQueryWithCompositeKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"composite_key_test": {"map": "function(doc) {emit([doc.key, doc.seq], doc.value);}"}}}`)
	assertStatus(t, response, 201)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/composite_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest("PUT", "/db/doc_composite_key", `{"key":"channel_a", "seq":55, "value":99}`)
	assertStatus(t, response, 201)

	// Admin view query:
	//   keys:[["channel_a", 55]]
	viewUrlPath := "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_a%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 1)

	// Ensure that a query for non-existent key returns no rows
	viewUrlPath = "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_b%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)

	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 0)
}

func TestViewQueryWithIntKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest("PUT", "/db/_design/foo", `{"views":{"int_key_test": {"map": "function(doc) {emit(doc.seq, doc.value);}"}}}`)
	assertStatus(t, response, 201)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/int_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest("PUT", "/db/doc_int_key", `{"key":"channel_a", "seq":55, "value":99}`)
	assertStatus(t, response, 201)

	// Admin view query:
	//   keys:[55,65]
	viewUrlPath := "/db/_design/foo/_view/int_key_test?keys=%5B55,65%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)
	var result sgbucket.ViewResult
	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 1)

	// Ensure that a query for non-existent key returns no rows
	//   keys:[65,75]
	viewUrlPath = "/db/_design/foo/_view/int_key_test?keys=%5B65,75%5D&stale=false"
	response = rt.SendAdminRequest("GET", viewUrlPath, ``)
	assertStatus(t, response, 200)

	base.JSONUnmarshal(response.Body.Bytes(), &result)
	goassert.Equals(t, len(result.Rows), 0)
}

func TestAdminGroupLevelReduceSumQuery(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
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
	assert.NoError(t, err, "Unexpected error")

	// we should get 2 row with the reduce result
	goassert.Equals(t, len(result.Rows), 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	goassert.Equals(t, value, 99.0)
}

func TestPostInstallCleanup(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	bucket := rt.Bucket()
	mapFunction := `function (doc, meta) { emit(); }`
	// Create design docs in obsolete format
	err := bucket.PutDDoc(db.DesignDocSyncGatewayPrefix, sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

	err = bucket.PutDDoc(db.DesignDocSyncHousekeepingPrefix, sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"all_docs": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

	// Run post-upgrade in preview mode
	var postUpgradeResponse PostUpgradeResponse
	response := rt.SendAdminRequest("POST", "/_post_upgrade?preview=true", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	goassert.Equals(t, postUpgradeResponse.Preview, true)
	goassert.Equals(t, len(postUpgradeResponse.Result["db"].RemovedDDocs), 2)

	// Run post-upgrade in non-preview mode
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest("POST", "/_post_upgrade", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	goassert.Equals(t, postUpgradeResponse.Preview, false)
	goassert.Equals(t, len(postUpgradeResponse.Result["db"].RemovedDDocs), 2)

	// Run post-upgrade in preview mode again, expect no results for database
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest("POST", "/_post_upgrade?preview=true", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	goassert.Equals(t, postUpgradeResponse.Preview, true)
	goassert.Equals(t, len(postUpgradeResponse.Result["db"].RemovedDDocs), 0)

	// Run post-upgrade in non-preview mode again, expect no results for database
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest("POST", "/_post_upgrade", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	goassert.Equals(t, postUpgradeResponse.Preview, false)
	goassert.Equals(t, len(postUpgradeResponse.Result["db"].RemovedDDocs), 0)

}

func TestViewQueryWrappers(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.ServerContext().Database("db").SetUserViewsEnabled(true)

	response := rt.SendAdminRequest("PUT", "/db/admindoc", `{"value":"foo"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest("PUT", "/db/admindoc2", `{"value":"foo"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest("PUT", "/db/userdoc", `{"value":"foo", "channels": ["userchannel"]}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assert.Equal(t, 201, response.Code)

	result, err := rt.WaitForNAdminViewResults(3, "/db/_design/foodoc/_view/foobarview")
	assert.NoError(t, err)
	assert.Equal(t, 3, result.Len())

	a := rt.ServerContext().Database("db").Authenticator()
	testUser, err := a.NewUser("testUser", "password", channels.SetOf(t, "userchannel"))
	assert.NoError(t, err)
	err = a.Save(testUser)
	assert.NoError(t, err)

	result, err = rt.WaitForNUserViewResults(1, "/db/_design/foodoc/_view/foobarview", testUser, "password")
	assert.NoError(t, err)
	assert.Equal(t, 1, result.Len())
	assert.Equal(t, "userdoc", result.Rows[0].ID)
}

func TestViewQueryWithXattrAndNonXattr(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test doesn't work with Walrus")
	}

	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"value":"foo"}`)
	assertStatus(t, response, http.StatusCreated)

	//Document with sync data in body
	body := `{"_sync": { "rev": "1-fc2cf22c5e5007bd966869ebfe9e276a", "sequence": 2, "recent_sequences": [ 2 ], "history": { "revs": [ "1-fc2cf22c5e5007bd966869ebfe9e276a" ], "parents": [ -1], "channels": [ null ] }, "cas": "","value_crc32c": "", "time_saved": "2019-04-10T12:40:04.490083+01:00" }, "value": "foo"}`
	ok, err := rt.Bucket().Add("doc2", 0, []byte(body))
	assert.True(t, ok)
	assert.NoError(t, err)

	//Should handle the case where there is no sync data
	body = `{"value": "foo"}`
	ok, err = rt.Bucket().Add("doc3", 0, []byte(body))
	assert.True(t, ok)
	assert.NoError(t, err)

	//Document with sync data in xattr
	response = rt.SendAdminRequest("PUT", "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assert.Equal(t, 201, response.Code)

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foodoc/_view/foobarview")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
	assert.Contains(t, "doc1", result.Rows[0].ID)
	assert.Contains(t, "doc2", result.Rows[1].ID)
}

//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDesignDocs(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest(http.MethodGet, "/db/_design/foo", "")
	assertStatus(t, response, http.StatusForbidden)
	response = rt.SendRequest(http.MethodPut, "/db/_design/foo", `{}`)
	assertStatus(t, response, http.StatusForbidden)
	response = rt.SendRequest(http.MethodDelete, "/db/_design/foo", "")
	assertStatus(t, response, http.StatusForbidden)

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo", "")
	assertStatus(t, response, http.StatusNotFound)
	response = rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo", "")

	assertStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_design%2ffoo", "")
	assertStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_design%2Ffoo", "")
	assertStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_design/foo", "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/_design/%s", db.DesignDocSyncGateway()), "{}")
	assertStatus(t, response, http.StatusForbidden)
	response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/db/_design/%s", db.DesignDocSyncGateway()), "")
	assertStatus(t, response, http.StatusOK)
}

func TestViewQuery(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"key":10, "value":"ten"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"key":7, "value":"seven"}`)
	assertStatus(t, response, http.StatusCreated)

	// The wait is needed here because the query does not have stale=false.
	// TODO: update the query to use stale=false and remove the wait
	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	assert.NoError(t, err, "Got unexpected error")
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"}, result.Rows[1])

	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?limit=1")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"}, result.Rows[0])

	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?endkey=9")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"}, result.Rows[0])

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?include_docs=true&endkey=9")
		require.Len(t, result.Rows, 1)
		assert.Equal(t, map[string]interface{}{"key": 7.0, "value": "seven"}, *result.Rows[0].Doc)
	}

}

// Tests #1109, wh ere design doc contains multiple views
func TestViewQueryMultipleViews(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	// Define three views
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, null); }"}}}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, http.StatusCreated)

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_age")
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)}, result.Rows[1])

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_fname")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)}, result.Rows[1])

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_lname")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)}, result.Rows[1])
}

func TestViewQueryWithParams(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"value": "foo", "key": "test1"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"value": "foo", "key": "test2"}`)
	assertStatus(t, response, http.StatusCreated)

	result, err := rt.WaitForNAdminViewResults(2, `/db/_design/foodoc/_view/foobarview?conflicts=true&descending=false&endkey="test2"&endkey_docid=doc2&end_key_doc_id=doc2&startkey="test1"&startkey_docid=doc1`)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, result.Rows, 2)
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc1", Key: "test1", Value: interface{}(nil)})
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc2", Key: "test2", Value: interface{}(nil)})

	result, err = rt.WaitForNAdminViewResults(2, `/db/_design/foodoc/_view/foobarview?conflicts=true&descending=false&conflicts=true&descending=false&keys=["test1", "test2"]`)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, result.Rows, 2)
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc1", Key: "test1", Value: interface{}(nil)})
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc2", Key: "test2", Value: interface{}(nil)})
}

func TestViewQueryUserAccess(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.ServerContext().Database("db").SetUserViewsEnabled(true)
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map":"function (doc, meta) { if (doc.type != 'type1') { return; } if (doc.state == 'state1' || doc.state == 'state2' || doc.state == 'state3') { emit(doc.state, meta.id); }}"}}}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"type":"type1", "state":"state1"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"type":"type1", "state":"state2"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc3", `{"type":"type2", "state":"state2"}`)
	assertStatus(t, response, http.StatusCreated)

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")
	assert.NoError(t, err, "Unexpected error in WaitForNAdminViewResults")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"}, result.Rows[1])

	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")
	assert.NoError(t, err, "Unexpected error in WaitForNAdminViewResults")

	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"}, result.Rows[1])

	// Create a user:
	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	password := "123456"
	testUser, _ := a.NewUser("testUser", password, channels.SetOf(t, "*"))
	assert.NoError(t, a.Save(testUser))

	result, err = rt.WaitForNUserViewResults(2, "/db/_design/foo/_view/bar?stale=false", testUser, password)
	assert.NoError(t, err, "Unexpected error in WaitForNUserViewResults")

	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"}, result.Rows[1])

	// Disable user view access, retry
	rt.ServerContext().Database("db").SetUserViewsEnabled(false)
	request, _ := http.NewRequest(http.MethodGet, "/db/_design/foo/_view/bar?stale=false", nil)
	request.SetBasicAuth(testUser.Name(), password)
	userResponse := rt.Send(request)
	assertStatus(t, userResponse, http.StatusForbidden)
}

func TestViewQueryMultipleViewsInterfaceValues(t *testing.T) {
	// TODO: Waiting for a fix for couchbaselabs/Walrus #13
	// Currently fails against walrus bucket as '_sync' property will exist in doc object if it is emitted in the map function
	t.Skip("WARNING: TEST DISABLED")

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Define three views
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, doc); }"}}}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo/_view/by_age", ``)
	assertStatus(t, response, http.StatusOK)
	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)}, result.Rows[1])

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo/_view/by_fname", ``)
	assertStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)}, result.Rows[1])

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo/_view/by_lname", ``)
	assertStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)}, result.Rows[1])
}

func TestUserViewQuery(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	rt.ServerContext().Database("db").SetUserViewsEnabled(true)

	// Create a view:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, http.StatusCreated)

	// Create docs:
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"key":10, "value":"ten", "channel":"W"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"key":7, "value":"seven", "channel":"Q"}`)
	assertStatus(t, response, http.StatusCreated)

	// Create a user:
	password := "123456"
	quinn, _ := a.NewUser("quinn", password, channels.SetOf(t, "Q", "q"))
	assert.NoError(t, a.Save(quinn))

	// Have the user query the view:
	result, err := rt.WaitForNUserViewResults(1, "/db/_design/foo/_view/bar?include_docs=true", quinn, password)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, 1, result.TotalRows)
	row := result.Rows[0]
	assert.Equal(t, float64(7), row.Key)
	assert.Equal(t, "seven", row.Value)

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		assert.Equal(t, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"}, *row.Doc)
	}

	// Admin should see both rows:
	result, err = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, result.Rows, 2)
	row = result.Rows[0]
	assert.Equal(t, float64(7), row.Key)
	assert.Equal(t, "seven", row.Value)

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		assert.Equal(t, map[string]interface{}{"key": 7.0, "value": "seven", "channel": "Q"}, *row.Doc)
	}

	row = result.Rows[1]
	assert.Equal(t, float64(10), row.Key)
	assert.Equal(t, "ten", row.Value)

	if base.UnitTestUrlIsWalrus() {
		// include_docs=true only works with walrus as documented here:
		// https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		assert.Equal(t, map[string]interface{}{"key": 10.0, "value": "ten", "channel": "W"}, *row.Doc)
	}

	// Make sure users are not allowed to query internal views:
	request, _ := http.NewRequest(http.MethodGet, "/db/_design/sync_gateway/_view/access", nil)
	request.SetBasicAuth(quinn.Name(), password)
	response = rt.Send(request)
	assertStatus(t, response, http.StatusForbidden)
}

// This includes a fix for #857
func TestAdminReduceViewQuery(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}", "reduce": "_count"}}}`)
	assertStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), `{"key":0, "value":"0", "channel":"W"}`)
		assertStatus(t, response, http.StatusCreated)

	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":1, "value":"0", "channel":"W"}`)
	assertStatus(t, response, http.StatusCreated)

	// Wait for all created documents to avoid race when using "reduce"
	_, err := rt.WaitForNAdminViewResults(10, "/db/_design/foo/_view/bar?reduce=false")
	require.NoError(t, err, "Unexpected error")

	var result sgbucket.ViewResult
	// Admin view query:
	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?reduce=true")
	assert.NoError(t, err, "Unexpected error")

	// we should get 1 row with the reduce result
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.True(t, value == 10)

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
	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		assertStatus(t, response, http.StatusCreated)
	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	assertStatus(t, response, http.StatusCreated)

	// Wait for all created documents to avoid race when using "reduce"
	_, err := rt.WaitForNAdminViewResults(10, "/db/_design/foo/_view/bar?reduce=false")
	require.NoError(t, err, "Unexpected error")

	var result sgbucket.ViewResult
	// Admin view query:
	result, err = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?reduce=true")
	require.NoError(t, err, "Unexpected error")

	// we should get 1 row with the reduce result
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.Equal(t, 108.0, value)
}

func TestAdminGroupReduceSumQuery(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		assertStatus(t, response, http.StatusCreated)

	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	assertStatus(t, response, http.StatusCreated)

	var result sgbucket.ViewResult

	// Admin view query:
	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?reduce=true&group=true")
	assert.NoError(t, err, "Unexpected error")

	// we should get 2 row with the reduce result
	require.Len(t, result.Rows, 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equal(t, 99.0, value)
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
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	assertStatus(t, response, http.StatusCreated)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/bar"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest(http.MethodPut, "/db/query_with_keys", `{"key":"channel_a", "value":99}`)
	assertStatus(t, response, http.StatusCreated)

	// Admin view query:
	viewUrlPath := "/db/_design/foo/_view/bar?keys=%5B%22channel_a%22%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	assertStatus(t, response, http.StatusOK) // Query string was parsed properly

	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)

	// Ensure that query for non-existent keys returns no rows
	viewUrlPath = "/db/_design/foo/_view/bar?keys=%5B%22channel_b%22%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	assertStatus(t, response, http.StatusOK) // Query string was parsed properly

	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 0)
}

func TestViewQueryWithCompositeKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"composite_key_test": {"map": "function(doc) {emit([doc.key, doc.seq], doc.value);}"}}}`)
	assertStatus(t, response, http.StatusCreated)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/composite_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest(http.MethodPut, "/db/doc_composite_key", `{"key":"channel_a", "seq":55, "value":99}`)
	assertStatus(t, response, http.StatusCreated)

	// Admin view query:
	//   keys:[["channel_a", 55]]
	viewUrlPath := "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_a%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	assertStatus(t, response, http.StatusOK)
	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)

	// Ensure that a query for non-existent key returns no rows
	viewUrlPath = "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_b%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	assertStatus(t, response, http.StatusOK)

	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 0)
}

func TestViewQueryWithIntKeys(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support the 'keys' view parameter")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"int_key_test": {"map": "function(doc) {emit(doc.seq, doc.value);}"}}}`)
	assertStatus(t, response, http.StatusCreated)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/int_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest(http.MethodPut, "/db/doc_int_key", `{"key":"channel_a", "seq":55, "value":99}`)
	assertStatus(t, response, http.StatusCreated)

	// Admin view query:
	//   keys:[55,65]
	viewUrlPath := "/db/_design/foo/_view/int_key_test?keys=%5B55,65%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	assertStatus(t, response, http.StatusOK)
	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)

	// Ensure that a query for non-existent key returns no rows
	//   keys:[65,75]
	viewUrlPath = "/db/_design/foo/_view/int_key_test?keys=%5B65,75%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	assertStatus(t, response, http.StatusOK)

	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 0)
}

func TestAdminGroupLevelReduceSumQuery(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	assertStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), fmt.Sprintf(`{"key":["A",{},%v], "value":1}`, i))
		assertStatus(t, response, http.StatusCreated)

	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":["B",4,1], "value":99}`)
	assertStatus(t, response, http.StatusCreated)

	var result sgbucket.ViewResult

	// Admin view query:
	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?reduce=true&group_level=2")
	assert.NoError(t, err, "Unexpected error")

	// we should get 2 row with the reduce result
	require.Len(t, result.Rows, 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equal(t, 99.0, value)
}

func TestPostInstallCleanup(t *testing.T) {
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Cleanup existing design docs
	_, err := rt.GetDatabase().RemoveObsoleteDesignDocs(false)
	require.NoError(t, err)

	bucket := rt.Bucket()
	mapFunction := `function (doc, meta) { emit(); }`
	// Create design docs in obsolete format
	err = bucket.PutDDoc(db.DesignDocSyncGatewayPrefix, &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

	err = bucket.PutDDoc(db.DesignDocSyncHousekeepingPrefix, &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"all_docs": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

	// Run post-upgrade in preview mode
	var postUpgradeResponse PostUpgradeResponse
	response := rt.SendAdminRequest(http.MethodPost, "/_post_upgrade?preview=true", "")
	assertStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.True(t, postUpgradeResponse.Preview)
	require.Lenf(t, postUpgradeResponse.Result["db"].RemovedDDocs, 2, "Response: %#v", postUpgradeResponse)

	// Run post-upgrade in non-preview mode
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	assertStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.False(t, postUpgradeResponse.Preview)
	require.Len(t, postUpgradeResponse.Result["db"].RemovedDDocs, 2)

	// Run post-upgrade in preview mode again, expect no results for database
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade?preview=true", "")
	assertStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.True(t, postUpgradeResponse.Preview)
	require.Len(t, postUpgradeResponse.Result["db"].RemovedDDocs, 0)

	// Run post-upgrade in non-preview mode again, expect no results for database
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	assertStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.False(t, postUpgradeResponse.Preview)
	require.Len(t, postUpgradeResponse.Result["db"].RemovedDDocs, 0)
}

func TestViewQueryWrappers(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.ServerContext().Database("db").SetUserViewsEnabled(true)

	response := rt.SendAdminRequest(http.MethodPut, "/db/admindoc", `{"value":"foo"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest(http.MethodPut, "/db/admindoc2", `{"value":"foo"}`)
	assertStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest(http.MethodPut, "/db/userdoc", `{"value":"foo", "channels": ["userchannel"]}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodPut, "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	result, err := rt.WaitForNAdminViewResults(3, "/db/_design/foodoc/_view/foobarview")
	assert.NoError(t, err)
	assert.Equal(t, 3, result.Len())

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
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

	rtConfig := &RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: false,
		}},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"value":"foo"}`)
	assertStatus(t, response, http.StatusCreated)

	// Document with sync data in body
	body := `{"_sync": { "rev": "1-fc2cf22c5e5007bd966869ebfe9e276a", "sequence": 2, "recent_sequences": [ 2 ], "history": { "revs": [ "1-fc2cf22c5e5007bd966869ebfe9e276a" ], "parents": [ -1], "channels": [ null ] }, "cas": "","value_crc32c": "", "time_saved": "2019-04-10T12:40:04.490083+01:00" }, "value": "foo"}`
	ok, err := rt.Bucket().Add("doc2", 0, []byte(body))
	assert.True(t, ok)
	assert.NoError(t, err)

	// Should handle the case where there is no sync data
	body = `{"value": "foo"}`
	ok, err = rt.Bucket().Add("doc3", 0, []byte(body))
	assert.True(t, ok)
	assert.NoError(t, err)

	// Document with sync data in xattr
	response = rt.SendAdminRequest("PUT", "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assert.Equal(t, 201, response.Code)

	result, err := rt.WaitForNAdminViewResults(2, "/db/_design/foodoc/_view/foobarview")
	assert.NoError(t, err)
	require.Equal(t, 2, len(result.Rows))
	assert.Contains(t, "doc1", result.Rows[0].ID)
	assert.Contains(t, "doc2", result.Rows[1].ID)
}

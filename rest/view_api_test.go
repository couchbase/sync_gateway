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
	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{GuestEnabled: true}) // views only use default collection
	defer rt.Close()

	response := rt.SendRequest(http.MethodGet, "/db/_design/foo", "")
	RequireStatus(t, response, http.StatusForbidden)
	response = rt.SendRequest(http.MethodPut, "/db/_design/foo", `{}`)
	RequireStatus(t, response, http.StatusForbidden)
	response = rt.SendRequest(http.MethodDelete, "/db/_design/foo", "")
	RequireStatus(t, response, http.StatusForbidden)

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo", "")
	RequireStatus(t, response, http.StatusNotFound)
	response = rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo", "")

	RequireStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_design%2ffoo", "")
	RequireStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_design%2Ffoo", "")
	RequireStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_design/foo", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/_design/%s", db.DesignDocSyncGateway()), "{}")
	RequireStatus(t, response, http.StatusForbidden)
	response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/db/_design/%s", db.DesignDocSyncGateway()), "")
	RequireStatus(t, response, http.StatusOK)
}

func TestViewQuery(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{GuestEnabled: true}) // views only use default collection
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"key":10, "value":"ten"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"key":7, "value":"seven"}`)
	RequireStatus(t, response, http.StatusCreated)

	// The wait is needed here because the query does not have stale=false.
	// TODO: update the query to use stale=false and remove the wait
	result := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: "ten"}, result.Rows[1])

	result = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?limit=1")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"}, result.Rows[0])

	result = rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?endkey=9")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: "seven"}, result.Rows[0])

}

// Tests #1109, where design doc contains multiple views
func TestViewQueryMultipleViews(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{GuestEnabled: true}) // views only use default collection
	defer rt.Close()

	// Define three views
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, null); }"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	RequireStatus(t, response, http.StatusCreated)

	result := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_age")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)}, result.Rows[1])

	result = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_fname")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)}, result.Rows[1])

	result = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/by_lname")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)}, result.Rows[1])
}

func TestViewQueryWithParams(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{GuestEnabled: true}) // views only use default collection
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"value": "foo", "key": "test1"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"value": "foo", "key": "test2"}`)
	RequireStatus(t, response, http.StatusCreated)

	result := rt.WaitForNAdminViewResults(2, `/db/_design/foodoc/_view/foobarview?conflicts=true&descending=false&endkey="test2"&endkey_docid=doc2&end_key_doc_id=doc2&startkey="test1"&startkey_docid=doc1`)
	require.Len(t, result.Rows, 2)
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc1", Key: "test1", Value: interface{}(nil)})
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc2", Key: "test2", Value: interface{}(nil)})

	result = rt.WaitForNAdminViewResults(2, `/db/_design/foodoc/_view/foobarview?conflicts=true&descending=false&conflicts=true&descending=false&keys=["test1", "test2"]`)
	require.Len(t, result.Rows, 2)
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc1", Key: "test1", Value: interface{}(nil)})
	assert.Contains(t, result.Rows, &sgbucket.ViewRow{ID: "doc2", Key: "test2", Value: interface{}(nil)})
}

func TestViewQueryUserAccess(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{GuestEnabled: true}) // views only use default collection
	defer rt.Close()

	ctx := rt.Context()
	rt.ServerContext().Database(ctx, "db").SetUserViewsEnabled(true)
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map":"function (doc, meta) { if (doc.type != 'type1') { return; } if (doc.state == 'state1' || doc.state == 'state2' || doc.state == 'state3') { emit(doc.state, meta.id); }}"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"type":"type1", "state":"state1"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"type":"type1", "state":"state2"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc3", `{"type":"type2", "state":"state2"}`)
	RequireStatus(t, response, http.StatusCreated)

	result := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"}, result.Rows[1])

	result = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?stale=false")

	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"}, result.Rows[1])

	// Create a user:
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	password := "123456"
	testUser, _ := a.NewUser("testUser", password, channels.BaseSetOf(t, "*"))
	assert.NoError(t, a.Save(testUser))

	result = rt.WaitForNUserViewResults(2, "/db/_design/foo/_view/bar?stale=false", testUser, password)

	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "state1", Value: "doc1"}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "state2", Value: "doc2"}, result.Rows[1])

	// Disable user view access, retry
	rt.ServerContext().Database(ctx, "db").SetUserViewsEnabled(false)
	request := Request(http.MethodGet, "/db/_design/foo/_view/bar?stale=false", "")
	request.SetBasicAuth(testUser.Name(), password)
	userResponse := rt.Send(request)
	RequireStatus(t, userResponse, http.StatusForbidden)
}

func TestViewQueryMultipleViewsInterfaceValues(t *testing.T) {
	// TODO: Waiting for a fix for couchbaselabs/Walrus #13
	// Currently fails against walrus bucket as '_sync' property will exist in doc object if it is emitted in the map function
	t.Skip("WARNING: TEST DISABLED")

	rt := NewRestTesterDefaultCollection(t, nil)
	defer rt.Close()

	// Define three views
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views": {"by_fname": {"map": "function (doc, meta) { emit(doc.fname, null); }"},"by_lname": {"map": "function (doc, meta) { emit(doc.lname, null); }"},"by_age": {"map": "function (doc, meta) { emit(doc.age, doc); }"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"fname": "Alice", "lname":"Ten", "age":10}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"fname": "Bob", "lname":"Seven", "age":7}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo/_view/by_age", ``)
	RequireStatus(t, response, http.StatusOK)
	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: 7.0, Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: 10.0, Value: interface{}(nil)}, result.Rows[1])

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo/_view/by_fname", ``)
	RequireStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Alice", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Bob", Value: interface{}(nil)}, result.Rows[1])

	response = rt.SendAdminRequest(http.MethodGet, "/db/_design/foo/_view/by_lname", ``)
	RequireStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 2)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "Seven", Value: interface{}(nil)}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "Ten", Value: interface{}(nil)}, result.Rows[1])
}

func TestUserViewQuery(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		GuestEnabled: true,
	}

	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	rt.ServerContext().Database(ctx, "db").SetUserViewsEnabled(true)

	// Create a view:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	RequireStatus(t, response, http.StatusCreated)

	// Create docs:
	response = rt.SendRequest(http.MethodPut, "/db/doc1", `{"key":10, "value":"ten", "channel":"W"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendRequest(http.MethodPut, "/db/doc2", `{"key":7, "value":"seven", "channel":"Q"}`)
	RequireStatus(t, response, http.StatusCreated)

	// Create a user:
	password := "123456"
	quinn, _ := a.NewUser("quinn", password, channels.BaseSetOf(t, "Q", "q"))
	assert.NoError(t, a.Save(quinn))

	// Have the user query the view:
	result := rt.WaitForNUserViewResults(1, "/db/_design/foo/_view/bar", quinn, password)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, 1, result.TotalRows)
	row := result.Rows[0]
	assert.Equal(t, float64(7), row.Key)
	assert.Equal(t, "seven", row.Value)

	// Admin should see both rows:
	result = rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar")
	require.Len(t, result.Rows, 2)
	row = result.Rows[0]
	assert.Equal(t, float64(7), row.Key)
	assert.Equal(t, "seven", row.Value)

	row = result.Rows[1]
	assert.Equal(t, float64(10), row.Key)
	assert.Equal(t, "ten", row.Value)

	// Make sure users are not allowed to query internal views:
	request := Request(http.MethodGet, "/db/_design/sync_gateway/_view/access", "")
	request.SetBasicAuth(quinn.Name(), password)
	response = rt.Send(request)
	RequireStatus(t, response, http.StatusForbidden)
}

// This includes a fix for #857
func TestAdminReduceViewQuery(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		GuestEnabled: true,
	}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}", "reduce": "_count"}}}`)
	RequireStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), `{"key":0, "value":"0", "channel":"W"}`)
		RequireStatus(t, response, http.StatusCreated)

	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":1, "value":"0", "channel":"W"}`)
	RequireStatus(t, response, http.StatusCreated)

	// Wait for all created documents to avoid race when using "reduce"
	rt.WaitForNAdminViewResults(10, "/db/_design/foo/_view/bar?reduce=false")

	// Admin view query:
	result := rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?reduce=true")

	// we should get 1 row with the reduce result
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.True(t, value == 10)

	// todo support group reduce, see #955
	// // test group=true
	// response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true&group=true", ``)
	// RequireStatus(t, response, 200)
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
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		GuestEnabled: true,
	}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	RequireStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		RequireStatus(t, response, http.StatusCreated)
	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	RequireStatus(t, response, http.StatusCreated)

	// Wait for all created documents to avoid race when using "reduce"
	rt.WaitForNAdminViewResults(10, "/db/_design/foo/_view/bar?reduce=false")

	// Admin view query:
	result := rt.WaitForNAdminViewResults(1, "/db/_design/foo/_view/bar?reduce=true")

	// we should get 1 row with the reduce result
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	value := row.Value.(float64)
	assert.Equal(t, 108.0, value)
}

func TestAdminGroupReduceSumQuery(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		GuestEnabled: true,
	}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	RequireStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), `{"key":"A", "value":1}`)
		RequireStatus(t, response, http.StatusCreated)

	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":"B", "value":99}`)
	RequireStatus(t, response, http.StatusCreated)

	// Admin view query:
	result := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?reduce=true&group=true")

	// we should get 2 row with the reduce result
	require.Len(t, result.Rows, 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equal(t, 99.0, value)
}

// Reproduces SG #3344.
func TestViewQueryWithKeys(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channel)}`,
	}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"bar": {"map": "function(doc) {emit(doc.key, doc.value);}"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/bar"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest(http.MethodPut, "/db/query_with_keys", `{"key":"channel_a", "value":99}`)
	RequireStatus(t, response, http.StatusCreated)

	// Admin view query:
	viewUrlPath := "/db/_design/foo/_view/bar?keys=%5B%22channel_a%22%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	RequireStatus(t, response, http.StatusOK) // Query string was parsed properly

	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)

	// Ensure that query for non-existent keys returns no rows
	viewUrlPath = "/db/_design/foo/_view/bar?keys=%5B%22channel_b%22%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	RequireStatus(t, response, http.StatusOK) // Query string was parsed properly

	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 0)
}

func TestViewQueryWithCompositeKeys(t *testing.T) {

	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"composite_key_test": {"map": "function(doc) {emit([doc.key, doc.seq], doc.value);}"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/composite_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest(http.MethodPut, "/db/doc_composite_key", `{"key":"channel_a", "seq":55, "value":99}`)
	RequireStatus(t, response, http.StatusCreated)

	// Admin view query:
	//   keys:[["channel_a", 55]]
	viewUrlPath := "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_a%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	RequireStatus(t, response, http.StatusOK)
	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)

	// Ensure that a query for non-existent key returns no rows
	viewUrlPath = "/db/_design/foo/_view/composite_key_test?keys=%5B%5B%22channel_b%22%2C%2055%5D%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	RequireStatus(t, response, http.StatusOK)

	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 0)
}

func TestViewQueryWithIntKeys(t *testing.T) {

	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"views":{"int_key_test": {"map": "function(doc) {emit(doc.seq, doc.value);}"}}}`)
	RequireStatus(t, response, http.StatusCreated)
	assert.NoError(t, rt.WaitForViewAvailable("/db/_design/foo/_view/int_key_test"), "Error waiting for view availability")

	// Create a doc
	response = rt.SendAdminRequest(http.MethodPut, "/db/doc_int_key", `{"key":"channel_a", "seq":55, "value":99}`)
	RequireStatus(t, response, http.StatusCreated)

	// Admin view query:
	//   keys:[55,65]
	viewUrlPath := "/db/_design/foo/_view/int_key_test?keys=%5B55,65%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	RequireStatus(t, response, http.StatusOK)
	var result sgbucket.ViewResult
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)

	// Ensure that a query for non-existent key returns no rows
	//   keys:[65,75]
	viewUrlPath = "/db/_design/foo/_view/int_key_test?keys=%5B65,75%5D&stale=false"
	response = rt.SendAdminRequest(http.MethodGet, viewUrlPath, ``)
	RequireStatus(t, response, http.StatusOK)

	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 0)
}

func TestAdminGroupLevelReduceSumQuery(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		GuestEnabled: true,
	}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // views only use default collection
	defer rt.Close()

	// Create a view with a reduce:
	response := rt.SendAdminRequest(http.MethodPut, "/db/_design/foo", `{"options":{"raw":true},"views":{"bar": {"map": "function(doc) {if (doc.key && doc.value) emit(doc.key, doc.value);}", "reduce": "_sum"}}}`)
	RequireStatus(t, response, http.StatusCreated)

	for i := 0; i < 9; i++ {
		// Create docs:
		response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", i), fmt.Sprintf(`{"key":["A",{},%v], "value":1}`, i))
		RequireStatus(t, response, http.StatusCreated)

	}
	response = rt.SendRequest(http.MethodPut, fmt.Sprintf("/db/doc%v", 10), `{"key":["B",4,1], "value":99}`)
	RequireStatus(t, response, http.StatusCreated)

	// Admin view query:
	result := rt.WaitForNAdminViewResults(2, "/db/_design/foo/_view/bar?reduce=true&group_level=2")

	// we should get 2 row with the reduce result
	require.Len(t, result.Rows, 2)
	row := result.Rows[1]
	value := row.Value.(float64)
	assert.Equal(t, 99.0, value)
}

func TestPostInstallCleanup(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channel)}`,
	}
	rt := NewRestTesterDefaultCollection(t, &rtConfig)
	defer rt.Close()

	// Cleanup existing design docs
	_, err := rt.GetDatabase().RemoveObsoleteDesignDocs(base.TestCtx(t), false)
	require.NoError(t, err)

	bucket := rt.Bucket()
	mapFunction := `function (doc, meta) { emit(); }`

	viewStore, ok := base.AsViewStore(bucket.DefaultDataStore())
	require.True(t, ok)

	// Create design docs in obsolete format
	err = viewStore.PutDDoc(rt.Context(), db.DesignDocSyncGatewayPrefix, &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

	err = viewStore.PutDDoc(rt.Context(), db.DesignDocSyncHousekeepingPrefix, &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"all_docs": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

	// Run post-upgrade in preview mode
	var postUpgradeResponse PostUpgradeResponse
	response := rt.SendAdminRequest(http.MethodPost, "/_post_upgrade?preview=true", "")
	RequireStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.True(t, postUpgradeResponse.Preview)
	require.Lenf(t, postUpgradeResponse.Result["db"].RemovedDDocs, 2, "Response: %#v", postUpgradeResponse)

	// Run post-upgrade in non-preview mode
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	RequireStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.False(t, postUpgradeResponse.Preview)
	require.Len(t, postUpgradeResponse.Result["db"].RemovedDDocs, 2)

	// Run post-upgrade in preview mode again, expect no results for database
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade?preview=true", "")
	RequireStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.True(t, postUpgradeResponse.Preview)
	require.Len(t, postUpgradeResponse.Result["db"].RemovedDDocs, 0)

	// Run post-upgrade in non-preview mode again, expect no results for database
	postUpgradeResponse = PostUpgradeResponse{}
	response = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	RequireStatus(t, response, http.StatusOK)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &postUpgradeResponse), "Error unmarshalling post_upgrade response")
	assert.False(t, postUpgradeResponse.Preview)
	require.Len(t, postUpgradeResponse.Result["db"].RemovedDDocs, 0)
}

func TestViewQueryWrappers(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}
	rt := NewRestTesterDefaultCollection(t, nil) // views only use default collection
	defer rt.Close()

	ctx := rt.Context()
	rt.ServerContext().Database(ctx, "db").SetUserViewsEnabled(true)

	response := rt.SendAdminRequest(http.MethodPut, "/db/admindoc", `{"value":"foo"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest(http.MethodPut, "/db/admindoc2", `{"value":"foo"}`)
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest(http.MethodPut, "/db/userdoc", `{"value":"foo", "channels": ["userchannel"]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodPut, "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	result := rt.WaitForNAdminViewResults(3, "/db/_design/foodoc/_view/foobarview")
	assert.Equal(t, 3, result.Len())

	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	testUser, err := a.NewUser("testUser", "password", channels.BaseSetOf(t, "userchannel"))
	assert.NoError(t, err)
	err = a.Save(testUser)
	assert.NoError(t, err)

	result = rt.WaitForNUserViewResults(1, "/db/_design/foodoc/_view/foobarview", testUser, "password")
	assert.NoError(t, err)
	assert.Equal(t, 1, result.Len())
	assert.Equal(t, "userdoc", result.Rows[0].ID)
}

func TestViewQueryWithXattrAndNonXattr(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}
	rtConfig := &RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: false,
		}},
	}

	rt := NewRestTesterDefaultCollection(t, rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"value":"foo"}`)
	RequireStatus(t, response, http.StatusCreated)

	// Document with sync data in body
	body := `{"_sync": { "rev": "1-fc2cf22c5e5007bd966869ebfe9e276a", "sequence": 2, "recent_sequences": [ 2 ], "history": { "revs": [ "1-fc2cf22c5e5007bd966869ebfe9e276a" ], "parents": [ -1], "channels": [ null ] }, "cas": "","value_crc32c": "", "time_saved": "2019-04-10T12:40:04.490083+01:00" }, "value": "foo"}`
	ok, err := rt.Bucket().DefaultDataStore().Add("doc2", 0, []byte(body))
	assert.True(t, ok)
	assert.NoError(t, err)

	// Should handle the case where there is no sync data
	body = `{"value": "foo"}`
	ok, err = rt.Bucket().DefaultDataStore().Add("doc3", 0, []byte(body))
	assert.True(t, ok)
	assert.NoError(t, err)

	// Document with sync data in xattr
	response = rt.SendAdminRequest("PUT", "/db/_design/foodoc", `{"views": {"foobarview": {"map": "function(doc, meta) {if (doc.value == \"foo\") {emit(doc.key, null);}}"}}}`)
	assert.Equal(t, 201, response.Code)

	result := rt.WaitForNAdminViewResults(2, "/db/_design/foodoc/_view/foobarview")
	require.Len(t, result.Rows, 2)
	assert.Contains(t, "doc1", result.Rows[0].ID)
	assert.Contains(t, "doc2", result.Rows[1].ID)
}

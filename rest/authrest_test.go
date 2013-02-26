// authrest_test.go

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sdegutis/go.assert"

	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

func callAuthREST(method, resource string, body string) *httptest.ResponseRecorder {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := httptest.NewRecorder()
	mapper, _ := channels.NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	dbcontext, _ := db.NewDatabaseContext("db", gTestBucket)
	dbcontext.ChannelMapper = mapper
	context := &context{dbcontext, nil, ""}
	handler := createAuthHandler(context)
	handler.ServeHTTP(response, request)
	return response
}

func TestDesignDocs(t *testing.T) {
	response := callAuthREST("GET", "/db/_design/foo", "")
	assert.Equals(t, response.Code, 404)

	response = callAuthREST("PUT", "/db/_design/foo", `{"hi": "there"}`)
	assert.Equals(t, response.Code, 201)
	response = callAuthREST("GET", "/db/_design/foo", "")
	assert.Equals(t, response.Code, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, db.Body{
		"_id":  "_design/foo",
		"_rev": "0-1",
		"hi":   "there"})

	response = callAuthREST("DELETE", "/db/_design/foo?rev=0-1", "")
	assert.Equals(t, response.Code, 200)

	response = callAuthREST("GET", "/db/_design/foo", "")
	assert.Equals(t, response.Code, 404)
}

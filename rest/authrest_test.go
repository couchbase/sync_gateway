// authrest_test.go

package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sdegutis/go.assert"

	"github.com/couchbaselabs/sync_gateway/db"
)

func callAuthREST(method, resource string, body string) *httptest.ResponseRecorder {
	context, _, err := InitREST(gTestBucket, "db", "", false)
	if err != nil {
		panic(fmt.Sprintf("Error from InitREST: %v", err))
	}
	authHandler := createAuthHandler(context)

	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := httptest.NewRecorder()
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	authHandler.ServeHTTP(response, request)
	return response
}

func TestDesignDocs(t *testing.T) {
	response := callAuthREST("GET", "/db/_design/foo", "")
	assertStatus(t, response, 404)

	response = callAuthREST("PUT", "/db/_design/foo", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = callAuthREST("GET", "/db/_design/foo", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, db.Body{
		"_id":  "_design/foo",
		"_rev": "0-1",
		"hi":   "there"})

	response = callAuthREST("DELETE", "/db/_design/foo?rev=0-1", "")
	assertStatus(t, response, 200)

	response = callAuthREST("GET", "/db/_design/foo", "")
	assertStatus(t, response, 404)
}

func TestUserAPI(t *testing.T) {
	// PUT a user
	assertStatus(t, callAuthREST("GET", "/db/users/snej", ""), 404)
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

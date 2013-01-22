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
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/sdegutis/go.assert"

	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

var gTestBucket *couchbase.Bucket

func init() {
	var err error
	gTestBucket, err = db.ConnectToBucket("http://localhost:8091", "default", "sync_gateway_tests")
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
}

func callREST(method, resource string, body string) *httptest.ResponseRecorder {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := httptest.NewRecorder()
	mapper, _ := channels.NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	dbcontext := &db.DatabaseContext{"db", gTestBucket, mapper, nil}
	context := &context{dbcontext, nil, ""}
	handler := createHandler(context)
	handler.ServeHTTP(response, request)
	return response
}

func TestRoot(t *testing.T) {
	response := callREST("GET", "/", "")
	assert.Equals(t, response.Code, 200)
	assert.Equals(t, response.Body.String(),
		"{\"couchdb\":\"welcome\",\"version\":\""+VersionString+"\"}")
}

func createDoc(t *testing.T, docid string) string {
	response := callREST("PUT", "/db/"+docid, `{"prop":true}`)
	assert.Equals(t, response.Code, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revid := body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}
	return revid
}

func TestDocLifecycle(t *testing.T) {
	revid := createDoc(t, "doc")
	assert.Equals(t, revid, "1-45ca73d819d5b1c9b8eea95290e79004")

	response := callREST("DELETE", "/db/doc?rev="+revid, "")
	assert.Equals(t, response.Code, 200)
}

func TestBulkDocs(t *testing.T) {
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}]}`
	response := callREST("POST", "/db/_bulk_docs", input)
	assert.Equals(t, response.Code, 201)
	var docs []interface{}
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 2)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "1-50133ddd8e49efad34ad9ecae4cb9907", "id": "bulk1"})
	assert.DeepEquals(t, docs[1],
		map[string]interface{}{"rev": "1-035168c88bd4b80fb098a8da72f881ce", "id": "bulk2"})
}

func TestBulkDocsNoEdits(t *testing.T) {
	input := `{"new_edits":false, "docs": [
                    {"_id": "bdne1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "bdne2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := callREST("POST", "/db/_bulk_docs", input)
	assert.Equals(t, response.Code, 201)
	var docs []interface{}
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 2)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "12-abc", "id": "bdne1"})
	assert.DeepEquals(t, docs[1],
		map[string]interface{}{"rev": "34-def", "id": "bdne2"})

	// Now update the first doc with two new revisions:
	input = `{"new_edits":false, "docs": [
                  {"_id": "bdne1", "_rev": "14-jkl", "n": 111,
                   "_revisions": {"start": 14, "ids": ["jkl", "def", "abc", "eleven", "ten", "nine"]}}
            ]}`
	response = callREST("POST", "/db/_bulk_docs", input)
	assert.Equals(t, response.Code, 201)
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 1)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "14-jkl", "id": "bdne1"})
}

type RevDiffResponse map[string][]string
type RevsDiffResponse map[string]RevDiffResponse

func TestRevsDiff(t *testing.T) {
	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := callREST("POST", "/db/_bulk_docs", input)
	assert.Equals(t, response.Code, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-abc", "11-eleven"],
              "rd2": ["34-def", "31-one"],
              "rd9": ["1-a", "2-b", "3-c"]
             }`
	response = callREST("POST", "/db/_revs_diff", input)
	assert.Equals(t, response.Code, 200)
	var diffResponse RevsDiffResponse
	json.Unmarshal(response.Body.Bytes(), &diffResponse)
	sort.Strings(diffResponse["rd1"]["possible_ancestors"])
	assert.DeepEquals(t, diffResponse, RevsDiffResponse{
		"rd1": RevDiffResponse{"missing": []string{"13-def"},
			"possible_ancestors": []string{"10-ten", "9-nine"}},
		"rd9": RevDiffResponse{"missing": []string{"1-a", "2-b", "3-c"}}})
}

func TestLocalDocs(t *testing.T) {
	response := callREST("GET", "/db/_local/loc1", "")
	assert.Equals(t, response.Code, 404)

	response = callREST("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assert.Equals(t, response.Code, 201)
	response = callREST("GET", "/db/_local/loc1", "")
	assert.Equals(t, response.Code, 200)
	assert.Equals(t, response.Body.String(), `{"_rev":"0-1","hi":"there"}`)

	response = callREST("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assert.Equals(t, response.Code, 409)
	response = callREST("PUT", "/db/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	assert.Equals(t, response.Code, 201)
	response = callREST("GET", "/db/_local/loc1", "")
	assert.Equals(t, response.Code, 200)
	assert.Equals(t, response.Body.String(), `{"_rev":"0-2","hi":"again"}`)

	response = callREST("DELETE", "/db/_local/loc1", "")
	assert.Equals(t, response.Code, 409)
	response = callREST("DELETE", "/db/_local/loc1?rev=0-2", "")
	assert.Equals(t, response.Code, 200)
	response = callREST("GET", "/db/_local/loc1", "")
	assert.Equals(t, response.Code, 404)
	response = callREST("DELETE", "/db/_local/loc1", "")
	assert.Equals(t, response.Code, 404)
}

func TestDesignDocs(t *testing.T) {
	response := callREST("GET", "/db/_design/foo", "")
	assert.Equals(t, response.Code, 404)

	response = callREST("PUT", "/db/_design/foo", `{"hi": "there"}`)
	assert.Equals(t, response.Code, 201)
	response = callREST("GET", "/db/_design/foo", "")
	assert.Equals(t, response.Code, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, db.Body{
		"_id":  "_design/foo",
		"_rev": "1-8d99d37a3fbeed6ca6052ede5e43fb2d",
		"hi":   "there"})

	response = callREST("DELETE", "/db/_design/foo?rev=1-8d99d37a3fbeed6ca6052ede5e43fb2d", "")
	assert.Equals(t, response.Code, 200)

	response = callREST("GET", "/db/_design/foo", "")
	assert.Equals(t, response.Code, 404)
}

// rest_test.go

package basecouch

import (
	"bytes"
	"encoding/json"
	"github.com/sdegutis/go.assert"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
)

func init() {
	response := callREST("DELETE", "/resttest", "")
	response = callREST("PUT", "/resttest", "")
	if response.Code != 201 {
		log.Printf("WARNING: Couldn't create resttest database at startup")
	}
}

func callREST(method, resource string, body string) *httptest.ResponseRecorder {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := httptest.NewRecorder()
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	handler := NewRESTHandler(gTestBucket)
	handler.ServeHTTP(response, request)
	return response
}

func TestRoot(t *testing.T) {
	response := callREST("GET", "/", "")
	assert.Equals(t, response.Code, 200)
	assert.Equals(t, response.Body.String(),
		"{\"couchdb\":\"welcome\",\"version\":\"CouchGlue 0.0\"}")
}

func createDoc(t *testing.T, docid string) string {
	response := callREST("PUT", "/resttest/"+docid, `{"prop":true}`)
	assert.Equals(t, response.Code, 201)
	var body Body
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

	response := callREST("DELETE", "/resttest/doc?rev="+revid, "")
	assert.Equals(t, response.Code, 200)
}

func TestBulkDocs(t *testing.T) {
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}]}`
	response := callREST("POST", "/resttest/_bulk_docs", input)
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
	response := callREST("POST", "/resttest/_bulk_docs", input)
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
	response = callREST("POST", "/resttest/_bulk_docs", input)
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
	response := callREST("POST", "/resttest/_bulk_docs", input)
	assert.Equals(t, response.Code, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-abc", "11-eleven"],
              "rd2": ["34-def", "31-one"],
              "rd9": ["1-a", "2-b", "3-c"]
             }`
	response = callREST("POST", "/resttest/_revs_diff", input)
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
	response := callREST("GET", "/resttest/_local/loc1", "")
	assert.Equals(t, response.Code, 404)

	response = callREST("PUT", "/resttest/_local/loc1", `{"hi": "there"}`)
	assert.Equals(t, response.Code, 201)
	response = callREST("GET", "/resttest/_local/loc1", "")
	assert.Equals(t, response.Code, 200)
	assert.Equals(t, response.Body.String(), `{"hi":"there"}`)

	response = callREST("DELETE", "/resttest/_local/loc1", "")
	assert.Equals(t, response.Code, 200)
	response = callREST("GET", "/resttest/_local/loc1", "")
	assert.Equals(t, response.Code, 404)
}

func TestDesignDocs(t *testing.T) {
	response := callREST("GET", "/resttest/_design/foo", "")
	assert.Equals(t, response.Code, 404)

	response = callREST("PUT", "/resttest/_design/foo", `{"hi": "there"}`)
	assert.Equals(t, response.Code, 201)
	response = callREST("GET", "/resttest/_design/foo", "")
	assert.Equals(t, response.Code, 200)
	var body Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.DeepEquals(t, body, Body{
        "_id":"_design/foo",
        "_rev": "1-8d99d37a3fbeed6ca6052ede5e43fb2d",
        "hi": "there"})

	response = callREST("DELETE", "/resttest/_design/foo?rev=1-8d99d37a3fbeed6ca6052ede5e43fb2d", "")
	assert.Equals(t, response.Code, 200)
    
	response = callREST("GET", "/resttest/_design/foo", "")
	assert.Equals(t, response.Code, 404)
}


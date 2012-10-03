// rest_test.go

package couchglue

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "net/http/httptest"
    "sort"
    "testing"
    "github.com/sdegutis/go.assert"
)


func init() {
    response := callREST("DELETE", "/resttest", "")
    response = callREST("PUT", "/resttest", "")
    if response.Code != 201 {
        fmt.Printf("WARNING: Couldn't create resttest database at startup")
    }
}


func callREST(method, resource string, body string) *httptest.ResponseRecorder {
    input := bytes.NewBufferString(body)
    request,_ := http.NewRequest(method, "http://localhost" + resource, input)
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
    if revid == "" {t.Fatalf("No revid in response for PUT doc")}
    return revid
}


func TestDocLifecycle(t *testing.T) {
    revid := createDoc(t, "doc")
    assert.Equals(t, revid, "1-216f623488e7629b56c0fb34bf6a4e7a71bbd36a")
    
    response := callREST("DELETE", "/resttest/doc?rev="+revid, "")
    assert.Equals(t, response.Code, 200)
}


func TestBulkDocs(t *testing.T) {
    input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}]}`
    response := callREST("POST", "/resttest/_bulk_docs", input)
    assert.Equals(t, response.Code, 201)
    var body Body
    json.Unmarshal(response.Body.Bytes(), &body)
    docs := body["docs"].([]interface{})
    assert.Equals(t, len(docs), 2)
    assert.DeepEquals(t, docs[0],
         map[string]interface{}{"rev":"1-b815378c8f0d4d345199c2ee5a18f93c9366b718", "id":"bulk1"})
     assert.DeepEquals(t, docs[1],
          map[string]interface{}{"rev":"1-db4bad55d36687b6d60bead141571f811cc7aa88", "id":"bulk2"})
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
    var body Body
    json.Unmarshal(response.Body.Bytes(), &body)
    docs := body["docs"].([]interface{})
    assert.Equals(t, len(docs), 2)
    assert.DeepEquals(t, docs[0],
         map[string]interface{}{"rev":"12-abc", "id":"bdne1"})
     assert.DeepEquals(t, docs[1],
          map[string]interface{}{"rev":"34-def", "id":"bdne2"})

    // Now update the first doc with two new revisions:
    input = `{"new_edits":false, "docs": [
                  {"_id": "bdne1", "_rev": "14-jkl", "n": 111,
                   "_revisions": {"start": 14, "ids": ["jkl", "def", "abc", "eleven", "ten", "nine"]}}
            ]}`
    response = callREST("POST", "/resttest/_bulk_docs", input)
    assert.Equals(t, response.Code, 201)
    body = Body{}
    json.Unmarshal(response.Body.Bytes(), &body)
    docs = body["docs"].([]interface{})
    assert.Equals(t, len(docs), 1)
    assert.DeepEquals(t, docs[0],
         map[string]interface{}{"rev":"14-jkl", "id":"bdne1"})
}


type RevDiffResponse map[string] []string
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
    assert.DeepEquals(t, diffResponse, RevsDiffResponse {
        "rd1": RevDiffResponse{"missing":[]string{"13-def"},
                               "possible_ancestors":[]string{"10-ten", "9-nine"}},
        "rd9": RevDiffResponse{"missing":[]string{"1-a", "2-b", "3-c"}} })
}

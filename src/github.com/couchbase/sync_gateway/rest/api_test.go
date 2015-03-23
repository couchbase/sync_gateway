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
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
	"github.com/robertkrimen/otto/underscore"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

func init() {
	base.LogNoColor()
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

//////// REST TESTER HELPER CLASS:

var gBucketCounter = 0

type restTester struct {
	_bucket      base.Bucket
	_sc          *ServerContext
	noAdminParty bool   // Unless this is true, Admin Party is in full effect
	syncFn       string // put the sync() function source in here (optional)
}

func (rt *restTester) bucket() base.Bucket {
	if rt._bucket == nil {
		server := "walrus:"
		bucketName := fmt.Sprintf("sync_gateway_test_%d", gBucketCounter)
		gBucketCounter++

		var syncFnPtr *string
		if len(rt.syncFn) > 0 {
			syncFnPtr = &rt.syncFn
		}

		corsConfig := &CORSConfig{
			Origin:  []string{"http://example.com", "*", "http://staging.example.com"},
			Headers: []string{},
			MaxAge:  1728000,
		}

		rt._sc = NewServerContext(&ServerConfig{
			CORS:     corsConfig,
			Facebook: &FacebookConfig{},
			Persona:  &PersonaConfig{},
		})

		_, err := rt._sc.AddDatabaseFromConfig(&DbConfig{
			Server: &server,
			Bucket: &bucketName,
			Name:   "db",
			Sync:   syncFnPtr,
		})
		if err != nil {
			panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
		}
		rt._bucket = rt._sc.Database("db").Bucket

		if !rt.noAdminParty {
			rt.setAdminParty(true)
		}

		runtime.SetFinalizer(rt, func(rt *restTester) {
			log.Printf("Finalizing bucket %s", rt._bucket.GetName())
			rt._sc.Close()
		})
	}
	return rt._bucket
}

func (rt *restTester) ServerContext() *ServerContext {
	rt.bucket()
	return rt._sc
}

func (rt *restTester) setAdminParty(partyTime bool) {
	a := rt.ServerContext().Database("db").Authenticator()
	guest, _ := a.GetUser("")
	guest.SetDisabled(!partyTime)
	var chans channels.TimedSet
	if partyTime {
		chans = channels.AtSequence(base.SetOf("*"), 1)
	}
	guest.SetExplicitChannels(chans)
	a.Save(guest)
}

type testResponse struct {
	*httptest.ResponseRecorder
	rq *http.Request
}

func (rt *restTester) sendRequest(method, resource string, body string) *testResponse {
	return rt.send(request(method, resource, body))
}

func (rt *restTester) sendRequestWithHeaders(method, resource string, body string, headers map[string]string) *testResponse {
	req := request(method, resource, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.send(req)
}

func (rt *restTester) send(request *http.Request) *testResponse {
	response := &testResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	CreatePublicHandler(rt.ServerContext()).ServeHTTP(response, request)
	return response
}

func (rt *restTester) sendAdminRequest(method, resource string, body string) *testResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := &testResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	CreateAdminHandler(rt.ServerContext()).ServeHTTP(response, request)
	return response
}

func (rt *restTester) sendAdminRequestWithHeaders(method, resource string, body string, headers map[string]string) *testResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response := &testResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	CreateAdminHandler(rt.ServerContext()).ServeHTTP(response, request)
	return response
}

func request(method, resource, body string) *http.Request {
	request, err := http.NewRequest(method, "http://localhost"+resource, bytes.NewBufferString(body))
	request.RequestURI = resource // This doesn't get filled in by NewRequest
	fixQuotedSlashes(request)
	if err != nil {
		panic(fmt.Sprintf("http.NewRequest failed: %v", err))
	}
	return request
}

func requestByUser(method, resource, body, username string) *http.Request {
	r := request(method, resource, body)
	r.SetBasicAuth(username, "letmein")
	return r
}

func assertStatus(t *testing.T, response *testResponse, expectedStatus int) {
	if response.Code != expectedStatus {
		t.Fatalf("Response status %d (expected %d) for %s <%s> : %s",
			response.Code, expectedStatus, response.rq.Method, response.rq.URL, response.Body)
	}
}

func (sc *ServerContext) Database(name string) *db.DatabaseContext {
	db, err := sc.GetDatabase(name)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error getting db %q: %v", name, err))
	}
	return db
}

//////// AND NOW THE TESTS:

func TestRoot(t *testing.T) {
	var rt restTester
	response := rt.sendRequest("GET", "/", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["couchdb"], "Welcome")

	response = rt.sendRequest("HEAD", "/", "")
	assertStatus(t, response, 200)
	response = rt.sendRequest("OPTIONS", "/", "")
	assertStatus(t, response, 204)
	assert.Equals(t, response.Header().Get("Allow"), "GET, HEAD")
	response = rt.sendRequest("PUT", "/", "")
	assertStatus(t, response, 405)
	assert.Equals(t, response.Header().Get("Allow"), "GET, HEAD")
}

func (rt *restTester) createDoc(t *testing.T, docid string) string {
	response := rt.sendRequest("PUT", "/db/"+docid, `{"prop":true}`)
	assertStatus(t, response, 201)
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
	var rt restTester
	revid := rt.createDoc(t, "doc")
	assert.Equals(t, revid, "1-45ca73d819d5b1c9b8eea95290e79004")

	response := rt.sendRequest("DELETE", "/db/doc?rev="+revid, "")
	assertStatus(t, response, 200)
}

func TestFunkyDocIDs(t *testing.T) {
	var rt restTester
	rt.createDoc(t, "AC%2FDC")

	response := rt.sendRequest("GET", "/db/AC%2FDC", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC+DC")
	response = rt.sendRequest("GET", "/db/AC+DC", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC+DC+GC")
	response = rt.sendRequest("GET", "/db/AC+DC+GC", "")
	assertStatus(t, response, 200)

	response = rt.sendRequest("PUT", "/db/foo+bar+moo+car", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/foo+bar+moo+car", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC%2BDC2")
	response = rt.sendRequest("GET", "/db/AC%2BDC2", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC%2BDC%2BGC2")
	response = rt.sendRequest("GET", "/db/AC%2BDC%2BGC2", "")
	assertStatus(t, response, 200)

	response = rt.sendRequest("PUT", "/db/foo%2Bbar%2Bmoo%2Bcar2", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/foo%2Bbar%2Bmoo%2Bcar2", "")
	assertStatus(t, response, 200)

	response = rt.sendRequest("PUT", "/db/foo%2Bbar+moo%2Bcar3", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/foo+bar%2Bmoo+car3", "")
	assertStatus(t, response, 200)
}

func TestCORSOrigin(t *testing.T) {
	var rt restTester
	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}
	response := rt.sendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "http://example.com")

	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.sendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "*")

	// now test another origin in config
	reqHeaders = map[string]string{
		"Origin": "http://staging.example.com",
	}
	response = rt.sendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "http://staging.example.com")

	// test no header on _admin apis
	reqHeaders = map[string]string{
		"Origin": "http://example.com",
	}
	response = rt.sendAdminRequestWithHeaders("GET", "/db/_all_docs", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "")

	// test with a config without * should reject non-matches
	sc := rt.ServerContext()
	sc.config.CORS.Origin = []string{"http://example.com", "http://staging.example.com"}
	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.sendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "")

}

func TestNoCORSOriginOnSessionPost(t *testing.T) {
	var rt restTester
	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.sendRequestWithHeaders("POST", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 400)

	response = rt.sendRequestWithHeaders("POST", "/db/_persona", "", reqHeaders)
	assertStatus(t, response, 400)

	response = rt.sendRequestWithHeaders("POST", "/db/_facebook", "", reqHeaders)
	assertStatus(t, response, 400)
}

func TestManualAttachment(t *testing.T) {
	var rt restTester

	doc1revId := rt.createDoc(t, "doc1")

	// attach to existing document without rev (should fail)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response := rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)

	// attach to existing document with wrong rev (should fail)
	response = rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1?rev=1-xyz", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)

	// attach to existing document with wrong rev using If-Match header (should fail)
	reqHeaders["If-Match"] = "1-dnf"
	response = rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to existing document with correct rev (should succeed)
	response = rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != doc1revId)

	// retrieve attachment
	response = rt.sendRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// retrieve attachment as admin should have
	// Content-disposition: attachment
	response = rt.sendAdminRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == `attachment; filename="attach1"`)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// try to overwrite that attachment
	attachmentBody = "updated content"
	response = rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	body = db.Body{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterUpdateAttachment := body["rev"].(string)
	if revIdAfterUpdateAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterUpdateAttachment != revIdAfterAttachment)

	// try to overwrite that attachment again, this time using If-Match header
	attachmentBody = "updated content again"
	reqHeaders["If-Match"] = revIdAfterUpdateAttachment
	response = rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	body = db.Body{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterUpdateAttachmentAgain := body["rev"].(string)
	if revIdAfterUpdateAttachmentAgain == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterUpdateAttachmentAgain != revIdAfterUpdateAttachment)
	delete(reqHeaders, "If-Match")

	// retrieve attachment
	response = rt.sendRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add another attachment to the document
	// also no explicit Content-Type header on this one
	// should default to application/octet-stream
	attachmentBody = "separate content"
	response = rt.sendRequest("PUT", "/db/doc1/attach2?rev="+revIdAfterUpdateAttachmentAgain, attachmentBody)
	assertStatus(t, response, 201)
	body = db.Body{}
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterSecondAttachment := body["rev"].(string)
	if revIdAfterSecondAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterSecondAttachment != revIdAfterUpdateAttachment)

	// retrieve attachment
	response = rt.sendRequest("GET", "/db/doc1/attach2", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Type") == "application/octet-stream")

	// now check the attachments index on the document
	response = rt.sendRequest("GET", "/db/doc1", "")
	assertStatus(t, response, 200)
	body = db.Body{}
	json.Unmarshal(response.Body.Bytes(), &body)
	bodyAttachments, ok := body["_attachments"].(map[string]interface{})
	if !ok {
		t.Errorf("Attachments must be map")
	} else {
		assert.Equals(t, len(bodyAttachments), 2)
	}
	// make sure original document property has remained
	prop, ok := body["prop"]
	if !ok || !prop.(bool) {
		t.Errorf("property prop is now missing or modified")
	}
}

// PUT attachment on non-existant docid should create empty doc
func TestManualAttachmentNewDoc(t *testing.T) {
	var rt restTester

	// attach to new document using bogus rev (should fail)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "text/plain"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response := rt.sendRequestWithHeaders("PUT", "/db/notexistyet/attach1?rev=1-abc", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)

	// attach to new document using bogus rev using If-Match header (should fail)
	reqHeaders["If-Match"] = "1-xyz"
	response = rt.sendRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to new document without any rev (should succeed)
	response = rt.sendRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}

	// retrieve attachment
	response = rt.sendRequest("GET", "/db/notexistyet/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// now check the document
	body = db.Body{}
	response = rt.sendRequest("GET", "/db/notexistyet", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	// body should only have 3 top-level entries _id, _rev, _attachments
	assert.True(t, len(body) == 3)
}

func TestBulkDocs(t *testing.T) {
	var rt restTester
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}]}`
	response := rt.sendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	var docs []interface{}
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 2)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "1-50133ddd8e49efad34ad9ecae4cb9907", "id": "bulk1"})
	assert.DeepEquals(t, docs[1],
		map[string]interface{}{"rev": "1-035168c88bd4b80fb098a8da72f881ce", "id": "bulk2"})
}

func TestBulkDocsChangeToAccess(t *testing.T) {

	base.LogKeys["Access"] = true

	rt := restTester{syncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { requireAccess(doc.channel)}}`}
	a := rt.ServerContext().Database("db").Authenticator()
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	//Create a test user
	user, err = a.NewUser("user1", "letmein", nil)
	a.Save(user)

	input := `{"docs": [{"_id": "bulk1", "type" : "setaccess", "owner":"user1" , "channel":"chan1"}, {"_id": "bulk2" , "channel":"chan1"}]}`

	response := rt.send(requestByUser("POST", "/db/_bulk_docs", input, "user1"))
	assertStatus(t, response, 201)

	var docs []interface{}
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 2)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "1-afbcffa8a4641a0f4dd94d3fc9593e74", "id": "bulk1"})
	assert.DeepEquals(t, docs[1],
		map[string]interface{}{"rev": "1-4d79588b9fe9c38faae61f0c1b9471c0", "id": "bulk2"})
}

func TestBulkDocsNoEdits(t *testing.T) {
	var rt restTester
	input := `{"new_edits":false, "docs": [
                    {"_id": "bdne1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "bdne2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := rt.sendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
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
	response = rt.sendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 1)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "14-jkl", "id": "bdne1"})
}

type RevDiffResponse map[string][]string
type RevsDiffResponse map[string]RevDiffResponse

func TestRevsDiff(t *testing.T) {
	var rt restTester
	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := rt.sendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-abc", "11-eleven"],
              "rd2": ["34-def", "31-one"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
	response = rt.sendRequest("POST", "/db/_revs_diff", input)
	assertStatus(t, response, 200)
	var diffResponse RevsDiffResponse
	json.Unmarshal(response.Body.Bytes(), &diffResponse)
	sort.Strings(diffResponse["rd1"]["possible_ancestors"])
	assert.DeepEquals(t, diffResponse, RevsDiffResponse{
		"rd1": RevDiffResponse{"missing": []string{"13-def"},
			"possible_ancestors": []string{"10-ten", "9-nine"}},
		"rd9": RevDiffResponse{"missing": []string{"1-a", "2-b", "3-c"}}})
}

func TestOpenRevs(t *testing.T) {
	var rt restTester

	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "or1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}}
              ]}`
	response := rt.sendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	reqHeaders := map[string]string{
		"Accept": "application/json",
	}
	response = rt.sendRequestWithHeaders("GET", `/db/or1?open_revs=["12-abc","10-ten"]`, "", reqHeaders)
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `[
{"ok":{"_id":"or1","_rev":"12-abc","_revisions":{"ids":["abc","eleven","ten","nine"],"start":12},"n":1}}
,{"missing":"10-ten"}
]`)
}

func TestLocalDocs(t *testing.T) {
	var rt restTester
	response := rt.sendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)

	response = rt.sendRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-1","hi":"there"}`)

	response = rt.sendRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 409)
	response = rt.sendRequest("PUT", "/db/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-2","hi":"again"}`)

	// Check the handling of large integers, which caused trouble for us at one point:
	response = rt.sendRequest("PUT", "/db/_local/loc1", `{"big": 123456789, "_rev": "0-2"}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-3","big":123456789}`)

	response = rt.sendRequest("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 409)
	response = rt.sendRequest("DELETE", "/db/_local/loc1?rev=0-3", "")
	assertStatus(t, response, 200)
	response = rt.sendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)
	response = rt.sendRequest("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 404)

	// Check the handling of URL encoded slash at end of _local%2Fdoc
	response = rt.sendRequest("PUT", "/db/_local%2Floc12", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = rt.sendRequest("GET", "/db/_local/loc2", "")
	assertStatus(t, response, 404)
	response = rt.sendRequest("DELETE", "/db/_local%2floc2", "")
	assertStatus(t, response, 404)
}

func TestResponseEncoding(t *testing.T) {
	// Make a doc longer than 1k so the HTTP response will be compressed:
	str := "DORKY "
	for i := 0; i < 10; i++ {
		str = str + str
	}
	docJSON := fmt.Sprintf(`{"long": %q}`, str)

	var rt restTester
	response := rt.sendRequest("PUT", "/db/_local/loc1", docJSON)
	assertStatus(t, response, 201)
	response = rt.sendRequestWithHeaders("GET", "/db/_local/loc1", "",
		map[string]string{"Accept-Encoding": "foo, gzip, bar"})
	assertStatus(t, response, 200)
	assert.DeepEquals(t, response.HeaderMap["Content-Encoding"], []string{"gzip"})
	unzip, err := gzip.NewReader(response.Body)
	assert.Equals(t, err, nil)
	unjson := json.NewDecoder(unzip)
	var body db.Body
	assert.Equals(t, unjson.Decode(&body), nil)
	assert.Equals(t, body["long"], str)
}

func TestLogin(t *testing.T) {
	var rt restTester
	a := auth.NewAuthenticator(rt.bucket(), nil)
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	user, err = a.GetUser("")
	assert.Equals(t, err, nil)
	assert.True(t, user.Disabled())

	response := rt.sendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf("*"))
	a.Save(user)

	assertStatus(t, rt.sendRequest("GET", "/db/_session", ""), 200)

	response = rt.sendRequest("POST", "/db/_session", `{"name":"pupshaw", "password":"letmein"}`)
	assertStatus(t, response, 200)
	log.Printf("Set-Cookie: %s", response.Header().Get("Set-Cookie"))
	assert.True(t, response.Header().Get("Set-Cookie") != "")
}

func TestReadChangesOptionsFromJSON(t *testing.T) {
	optStr := `{"feed":"longpoll", "since": "123456:78", "limit":123, "style": "all_docs",
				"include_docs": true, "filter": "Melitta", "channels": "ABC,BBC"}`
	feed, options, filter, channelsArray, err := readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, feed, "longpoll")

	assert.Equals(t, options.Since, db.SequenceID{Seq: 78, TriggeredBy: 123456})
	assert.Equals(t, options.Limit, 123)
	assert.Equals(t, options.Conflicts, true)
	assert.Equals(t, options.IncludeDocs, true)

	assert.Equals(t, filter, "Melitta")
	assert.DeepEquals(t, channelsArray, []string{"ABC", "BBC"})
}

func TestAccessControl(t *testing.T) {
	type allDocsRow struct {
		ID    string `json:"id"`
		Key   string `json:"key"`
		Value struct {
			Rev      string              `json:"rev"`
			Channels []string            `json:"channels,omitempty"`
			Access   map[string]base.Set `json:"access,omitempty"` // for admins only
		} `json:"value"`
		Doc   db.Body `json:"doc,omitempty"`
		Error string  `json:"error"`
	}
	var allDocsResult struct {
		TotalRows int          `json:"total_rows"`
		Offset    int          `json:"offset"`
		Rows      []allDocsRow `json:"rows"`
	}

	// Create some docs:
	var rt restTester
	a := auth.NewAuthenticator(rt.bucket(), nil)
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"channels":[]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"channels":["CBS"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc3", `{"channels":["CBS", "Cinemax"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc4", `{"channels":["WB", "Cinemax"]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create a user:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf("Cinemax"))
	a.Save(alice)

	// Get a single doc the user has access to:
	request, _ := http.NewRequest("GET", "/db/doc3", nil)
	request.SetBasicAuth("alice", "letmein")
	response := rt.send(request)
	assertStatus(t, response, 200)

	// Get a single doc the user doesn't have access to:
	request, _ = http.NewRequest("GET", "/db/doc2", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 403)

	// Check that _all_docs only returns the docs the user has access to:
	request, _ = http.NewRequest("GET", "/db/_all_docs?channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 2)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[1].Value.Channels, []string{"Cinemax"})

	//Check all docs limit option
	request, _ = http.NewRequest("GET", "/db/_all_docs?limit=1&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	//Check all docs startkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?startkey=doc4&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	//Check all docs endkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?endkey=doc3&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	// Check _all_docs with include_docs option:
	request, _ = http.NewRequest("GET", "/db/_all_docs?include_docs=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 2)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc4")

	// Check POST to _all_docs:
	body := `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 4)
	assert.Equals(t, allDocsResult.Rows[0].Key, "doc4")
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[1].Key, "doc1")
	assert.Equals(t, allDocsResult.Rows[1].Error, "forbidden")
	assert.Equals(t, allDocsResult.Rows[2].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[2].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[3].Key, "b0gus")
	assert.Equals(t, allDocsResult.Rows[3].Error, "not_found")

	// Check GET to _all_docs with keys parameter:
	request, _ = http.NewRequest("GET", `/db/_all_docs?channels=true&keys=%5B%22doc4%22%2C%22doc1%22%2C%22doc3%22%2C%22b0gus%22%5D`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from GET _all_docs = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 4)
	assert.Equals(t, allDocsResult.Rows[0].Key, "doc4")
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[1].Key, "doc1")
	assert.Equals(t, allDocsResult.Rows[1].Error, "forbidden")
	assert.Equals(t, allDocsResult.Rows[2].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[2].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[3].Key, "b0gus")
	assert.Equals(t, allDocsResult.Rows[3].Error, "not_found")

	// Check POST to _all_docs with limit option:
	body = `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?limit=1&channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].Key, "doc4")
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	// Check _all_docs as admin:
	response = rt.sendAdminRequest("GET", "/db/_all_docs", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 4)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc2")

}

func TestChannelAccessChanges(t *testing.T) {
	// base.ParseLogFlags([]string{"Cache", "Changes+", "CRUD"})

	rt := restTester{syncFn: `function(doc) {access(doc.owner, doc._id);channel(doc.channel)}`}
	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create users:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf("zero"))
	a.Save(alice)
	zegpold, err := a.NewUser("zegpold", "letmein", channels.SetOf("zero"))
	a.Save(zegpold)

	// Create some docs that give users access:
	response := rt.send(request("PUT", "/db/alpha", `{"owner":"alice"}`)) // seq=1
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	alphaRevID := body["rev"].(string)

	assertStatus(t, rt.send(request("PUT", "/db/beta", `{"owner":"boadecia"}`)), 201) // seq=2
	assertStatus(t, rt.send(request("PUT", "/db/delta", `{"owner":"alice"}`)), 201)   // seq=3
	assertStatus(t, rt.send(request("PUT", "/db/gamma", `{"owner":"zegpold"}`)), 201) // seq=4

	assertStatus(t, rt.send(request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201) // seq=5
	assertStatus(t, rt.send(request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)  // seq=6
	assertStatus(t, rt.send(request("PUT", "/db/d1", `{"channel":"delta"}`)), 201) // seq=7
	assertStatus(t, rt.send(request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201) // seq=8

	// Check the _changes feed:
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "g1")
	assert.Equals(t, since, db.SequenceID{Seq: 8})

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.Channels(), channels.TimedSet{"!": 0x1, "zero": 0x1, "alpha": 0x1, "delta": 0x3})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.Channels(), channels.TimedSet{"!": 0x1, "zero": 0x1, "gamma": 0x4})

	// Update a document to revoke access to alice and grant it to zegpold:
	str := fmt.Sprintf(`{"owner":"zegpold", "_rev":%q}`, alphaRevID)
	assertStatus(t, rt.send(request("PUT", "/db/alpha", str)), 201) // seq=9

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.Channels(), channels.TimedSet{"!": 0x1, "zero": 0x1, "delta": 0x3})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.Channels(), channels.TimedSet{"!": 0x1, "zero": 0x1, "alpha": 0x9, "gamma": 0x4})

	// Look at alice's _changes feed:
	changes.Results = nil
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("//////// _changes for alice looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "d1")

	// The complete _changes feed for zegpold contains docs a1 and g1:
	changes.Results = nil
	response = rt.send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("//////// _changes for zegpold looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Results[0].ID, "g1")
	assert.Equals(t, changes.Results[0].Seq, db.SequenceID{Seq: 8})
	assert.Equals(t, changes.Results[1].ID, "a1")
	assert.Equals(t, changes.Results[1].Seq, db.SequenceID{Seq: 5, TriggeredBy: 9})

	// Changes feed with since=gamma:8 would ordinarily be empty, but zegpold got access to channel
	// alpha after sequence 8, so the pre-existing docs in that channel are included:
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", since),
		"", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "a1")

	// What happens if we call access() with a nonexistent username?
	assertStatus(t, rt.send(request("PUT", "/db/epsilon", `{"owner":"waldo"}`)), 201)

	// Finally, throw a wrench in the works by changing the sync fn. Note that normally this wouldn't
	// be changed while the database is in use (only when it's re-opened) but for testing purposes
	// we do it now because we can't close and re-open an ephemeral Walrus database.
	dbc := rt.ServerContext().Database("db")
	db, _ := db.GetDatabase(dbc, nil)
	changed, err := db.UpdateSyncFun(`function(doc) {access("alice", "beta");channel("beta");}`)
	assert.Equals(t, err, nil)
	assert.True(t, changed)
	changeCount, err := db.UpdateAllDocChannels(true, false)
	assert.Equals(t, err, nil)
	assert.Equals(t, changeCount, 9)

	changes.Results = nil
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	expectedIDs := []string{"beta", "delta", "gamma", "a1", "b1", "d1", "g1", "alpha", "epsilon"}
	assert.Equals(t, len(changes.Results), len(expectedIDs))
	for i, expectedID := range expectedIDs {
		assert.Equals(t, changes.Results[i].ID, expectedID)
	}

	// Check accumulated statistics:
	assert.Equals(t, db.ChangesClientStats.TotalCount(), uint32(5))
	assert.Equals(t, db.ChangesClientStats.MaxCount(), uint32(1))
	db.ChangesClientStats.Reset()
	assert.Equals(t, db.ChangesClientStats.TotalCount(), uint32(0))
	assert.Equals(t, db.ChangesClientStats.MaxCount(), uint32(0))
}

//Test for wrong _changes entries for user joining a populated channel
func TestUserJoiningPopulatedChannel(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Cache+", "Changes", "Changes+", "CRUD"})

	rt := restTester{syncFn: `function(doc) {channel(doc.channels)}`}
	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create user1
	response := rt.sendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	// Create 100 docs
	for i := 0; i < 100; i++ {
		docpath := fmt.Sprintf("/db/doc%d", i)
		assertStatus(t, rt.send(request("PUT", docpath, `{"foo": "bar", "channels":["alpha"]}`)), 201)
	}

	// Check the _changes feed with limit, to split feed into two
	var changes struct {
		Results []db.ChangeEntry
	}

	limit := 50
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?limit=%d", limit), "", "user1"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since := changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc48")
	assert.Equals(t, since, db.SequenceID{Seq: 50})

	//// Check the _changes feed with  since and limit, to get second half of feed
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s&limit=%d", since, limit), "", "user1"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since = changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc98")
	assert.Equals(t, since, db.SequenceID{Seq: 100})

	// Create user2
	response = rt.sendAdminRequest("PUT", "/db/_user/user2", `{"email":"user2@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	//Retrieve all changes for user2 with no limits
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes"), "", "user2"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 101)
	assert.Equals(t, changes.Results[99].ID, "doc99")

	// Create user3
	response = rt.sendAdminRequest("PUT", "/db/_user/user3", `{"email":"user3@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	//Get first 50 document changes
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?limit=%d", limit), "", "user3"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since = changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc49")
	assert.Equals(t, since, db.SequenceID{TriggeredBy: 103, Seq: 51})

	//// Get remainder of changes i.e. no limit parameter
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", since), "", "user3"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 51)
	assert.Equals(t, changes.Results[49].ID, "doc99")

	// Create user4
	response = rt.sendAdminRequest("PUT", "/db/_user/user4", `{"email":"user4@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?limit=%d", limit), "", "user4"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since = changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc49")
	assert.Equals(t, since, db.SequenceID{TriggeredBy: 104, Seq: 51})

	//// Check the _changes feed with  since and limit, to get second half of feed
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s&limit=%d", since, limit), "", "user4"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	assert.Equals(t, changes.Results[49].ID, "doc99")

}

func TestRoleAssignmentBeforeUserExists(t *testing.T) {
	base.LogKeys["Access"] = true
	base.LogKeys["CRUD"] = true
	base.LogKeys["Changes+"] = true

	rt := restTester{syncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// POST a role
	response := rt.sendAdminRequest("POST", "/db/_role/", `{"name":"role1","admin_channels":["chan1"]}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/_role/role1", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "role1")

	//Put document to trigger sync function
	response = rt.send(request("PUT", "/db/doc1", `{"user":"user1", "role":"role:role1", "channel":"chan1"}`)) // seq=1
	assertStatus(t, response, 201)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)

	// POST the new user the GET and verify that it shows the assigned role
	response = rt.sendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"letmein"}`)
	assertStatus(t, response, 201)
	response = rt.sendAdminRequest("GET", "/db/_user/user1", "")
	assertStatus(t, response, 200)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "user1")
	assert.DeepEquals(t, body["roles"], []interface{}{"role1"})
	assert.DeepEquals(t, body["all_channels"], []interface{}{"!", "chan1"})

	//assert.DeepEquals(t, body["admin_roles"], []interface{}{"hipster"})
	//assert.DeepEquals(t, body["all_channels"], []interface{}{"bar", "fedoras", "fixies", "foo"})
}

func TestRoleAccessChanges(t *testing.T) {
	base.LogKeys["Access"] = true
	base.LogKeys["CRUD"] = true
	base.LogKeys["Changes+"] = true

	rt := restTester{syncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create users:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf("alpha"))
	a.Save(alice)
	zegpold, err := a.NewUser("zegpold", "letmein", channels.SetOf("beta"))
	a.Save(zegpold)

	hipster, err := a.NewRole("hipster", channels.SetOf("gamma"))
	a.Save(hipster)

	// Create some docs in the channels:
	response := rt.send(request("PUT", "/db/fashion",
		`{"user":"alice","role":["role:hipster","role:bogus"]}`)) // seq=1
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	fashionRevID := body["rev"].(string)

	assertStatus(t, rt.send(request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201) // seq=2
	assertStatus(t, rt.send(request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201) // seq=3
	assertStatus(t, rt.send(request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)  // seq=4
	assertStatus(t, rt.send(request("PUT", "/db/d1", `{"channel":"delta"}`)), 201) // seq=5

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.InheritedChannels(), channels.TimedSet{"!": 0x1, "alpha": 0x1, "gamma": 0x1})
	assert.DeepEquals(t, alice.RoleNames(), channels.TimedSet{"bogus": 0x1, "hipster": 0x1})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.InheritedChannels(), channels.TimedSet{"!": 0x1, "beta": 0x1})
	assert.DeepEquals(t, zegpold.RoleNames(), channels.TimedSet{})

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("1st _changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	since := changes.Last_Seq
	assert.Equals(t, since, "3")

	response = rt.send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("2nd _changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since = changes.Last_Seq
	assert.Equals(t, since, "4")

	// Update "fashion" doc to grant zegpold the role "hipster" and take it away from alice:
	str := fmt.Sprintf(`{"user":"zegpold", "role":"role:hipster", "_rev":%q}`, fashionRevID)
	assertStatus(t, rt.send(request("PUT", "/db/fashion", str)), 201) // seq=6

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.InheritedChannels(), channels.TimedSet{"!": 0x1, "alpha": 0x1})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.InheritedChannels(), channels.TimedSet{"!": 0x1, "beta": 0x1, "gamma": 0x6})

	// The complete _changes feed for zegpold contains docs g1 and b1:
	changes.Results = nil
	response = rt.send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("3rd _changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Last_Seq, "6:2")
	assert.Equals(t, changes.Results[0].ID, "b1")
	assert.Equals(t, changes.Results[1].ID, "g1")

	// Changes feed with since=4 would ordinarily be empty, but zegpold got access to channel
	// gamma after sequence 4, so the pre-existing docs in that channel are included:
	base.LogKeys["Changes"] = true
	base.LogKeys["Cache"] = true
	response = rt.send(requestByUser("GET", "/db/_changes?since=4", "", "zegpold"))
	log.Printf("4th _changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "g1")
	base.LogKeys["Cache"] = false
}

func TestDocDeletionFromChannel(t *testing.T) {
	// See https://github.com/couchbase/couchbase-lite-ios/issues/59
	// base.LogKeys["Changes"] = true
	// base.LogKeys["Cache"] = true

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}
	a := rt.ServerContext().Database("db").Authenticator()

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.SetOf("zero"))
	a.Save(alice)

	// Create a doc Alice can see:
	response := rt.send(request("PUT", "/db/alpha", `{"channel":"zero"}`))

	// Check the _changes feed:
	rt.ServerContext().Database("db").WaitForPendingChanges()
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, since, db.SequenceID{Seq: 1})

	assert.Equals(t, changes.Results[0].ID, "alpha")
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Delete the document:
	assertStatus(t, rt.send(request("DELETE", "/db/alpha?rev="+rev1, "")), 200)

	// Get the updates from the _changes feed:
	time.Sleep(100 * time.Millisecond)
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", since),
		"", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)

	assert.Equals(t, changes.Results[0].ID, "alpha")
	assert.Equals(t, changes.Results[0].Deleted, true)
	assert.DeepEquals(t, changes.Results[0].Removed, base.SetOf("zero"))
	rev2 := changes.Results[0].Changes[0]["rev"]

	// Now get the deleted revision:
	response = rt.send(requestByUser("GET", "/db/alpha?rev="+rev2, "", "alice"))
	assert.Equals(t, response.Code, 200)
	log.Printf("Deletion looks like: %s", response.Body.Bytes())
	var docBody db.Body
	json.Unmarshal(response.Body.Bytes(), &docBody)
	assert.DeepEquals(t, docBody, db.Body{"_id": "alpha", "_rev": rev2, "_deleted": true})

	// Access without deletion revID shouldn't be allowed (since doc is not in Alice's channels):
	response = rt.send(requestByUser("GET", "/db/alpha", "", "alice"))
	assert.Equals(t, response.Code, 403)

	// A bogus rev ID should return a 404:
	response = rt.send(requestByUser("GET", "/db/alpha?rev=bogus", "", "alice"))
	assert.Equals(t, response.Code, 404)

	// Get the old revision, which should still be accessible:
	response = rt.send(requestByUser("GET", "/db/alpha?rev="+rev1, "", "alice"))
	assert.Equals(t, response.Code, 200)
}

func TestAllDocsChannelsAfterChannelMove(t *testing.T) {

	type allDocsRow struct {
		ID    string `json:"id"`
		Key   string `json:"key"`
		Value struct {
			Rev      string              `json:"rev"`
			Channels []string            `json:"channels,omitempty"`
			Access   map[string]base.Set `json:"access,omitempty"` // for admins only
		} `json:"value"`
		Doc   db.Body `json:"doc,omitempty"`
		Error string  `json:"error"`
	}
	var allDocsResult struct {
		TotalRows int          `json:"total_rows"`
		Offset    int          `json:"offset"`
		Rows      []allDocsRow `json:"rows"`
	}

	rt := restTester{syncFn: `function(doc) {channel(doc.channels)}`}
	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create a doc
	response := rt.send(request("PUT", "/db/doc1", `{"foo":"bar", "channels":["ch1"]}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	doc1RevID := body["rev"].(string)

	// Run GET _all_docs as admin with channels=true:
	response = rt.sendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[0].Value.Channels[0], "ch1")

	// Run POST _all_docs as admin with explicit docIDs and channels=true:
	keys := `{"keys": ["doc1"]}`
	response = rt.sendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[0].Value.Channels[0], "ch1")

	//Commit rev 2 that maps to a differenet channel
	str := fmt.Sprintf(`{"foo":"bar", "channels":["ch2"], "_rev":%q}`, doc1RevID)
	assertStatus(t, rt.send(request("PUT", "/db/doc1", str)), 201)

	// Run GET _all_docs as admin with channels=true
	// Make sure that only the new channel appears in the docs channel list
	response = rt.sendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[0].Value.Channels[0], "ch2")

	// Run POST _all_docs as admin with explicit docIDs and channels=true
	// Make sure that only the new channel appears in the docs channel list
	keys = `{"keys": ["doc1"]}`
	response = rt.sendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[0].Value.Channels[0], "ch2")
}

//Test for regression of issue #447
func TestAttachmentsNoCrossTalk(t *testing.T) {
	base.LogKeys["ANDY"] = true
	var rt restTester

	doc1revId := rt.createDoc(t, "doc1")

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response := rt.sendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != doc1revId)

	reqHeaders = map[string]string{
		"Accept": "application/json",
	}

	log.Printf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, doc1revId)
	response = rt.sendRequestWithHeaders("GET", fmt.Sprintf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, doc1revId), "", reqHeaders)
	assert.Equals(t, response.Code, 200)
	//validate attachment has data property
	json.Unmarshal(response.Body.Bytes(), &body)
	log.Printf("response body revid1 = %s", body)
	attachments := body["_attachments"].(map[string]interface{})
	attach1 := attachments["attach1"].(map[string]interface{})
	data := attach1["data"]
	assert.True(t, data != nil)

	log.Printf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, revIdAfterAttachment)
	response = rt.sendRequestWithHeaders("GET", fmt.Sprintf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, revIdAfterAttachment), "", reqHeaders)
	assert.Equals(t, response.Code, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	log.Printf("response body revid1 = %s", body)
	attachments = body["_attachments"].(map[string]interface{})
	attach1 = attachments["attach1"].(map[string]interface{})
	data = attach1["data"]
	assert.True(t, data == nil)

}

func TestOldDocHandling(t *testing.T) {

	rt := restTester{syncFn: `
		function(doc,oldDoc){
			log("doc id:"+doc._id);
			if(oldDoc){
				log("old doc id:"+oldDoc._id);
				if(!oldDoc._id){
					throw ({forbidden : "Old doc id not available"})
				}
				if(!(oldDoc._id == doc._id)) {
					throw ({forbidden : "Old doc id doesn't match doc id"})
				}
			}
		}`}
	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create user:
	frank, err := a.NewUser("charles", "1234", nil)
	a.Save(frank)

	// Create a doc:
	response := rt.send(request("PUT", "/db/testOldDocId", `{"foo":"bar"}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	alphaRevID := body["rev"].(string)

	// Update a document to validate oldDoc id handling.  Will reject if old doc id not available
	str := fmt.Sprintf(`{"foo":"ball", "_rev":%q}`, alphaRevID)
	assertStatus(t, rt.send(request("PUT", "/db/testOldDocId", str)), 201)

}

func TestStarAccess(t *testing.T) {
	type allDocsRow struct {
		ID    string `json:"id"`
		Key   string `json:"key"`
		Value struct {
			Rev      string              `json:"rev"`
			Channels []string            `json:"channels,omitempty"`
			Access   map[string]base.Set `json:"access,omitempty"` // for admins only
		} `json:"value"`
		Doc   db.Body `json:"doc,omitempty"`
		Error string  `json:"error"`
	}
	var allDocsResult struct {
		TotalRows int          `json:"total_rows"`
		Offset    int          `json:"offset"`
		Rows      []allDocsRow `json:"rows"`
	}

	// Create some docs:
	var rt restTester

	base.LogKeys["Changes+"] = true
	a := auth.NewAuthenticator(rt.bucket(), nil)
	var changes struct {
		Results []db.ChangeEntry
	}
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"channels":["books"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"channels":["gifts"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc3", `{"channels":["!"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc4", `{"channels":["gifts"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc5", `{"channels":["!"]}`), 201)
	// document added to "*" channel should only end up available to users with * access
	assertStatus(t, rt.sendRequest("PUT", "/db/doc6", `{"channels":["*"]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.Equals(t, err, nil)
	//
	// Part 1 - Tests for user with single channel access:
	//
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("books"))
	a.Save(bernard)

	// GET /db/docid - basic test for channel user has
	response := rt.send(requestByUser("GET", "/db/doc1", "", "bernard"))
	assertStatus(t, response, 200)

	// GET /db/docid - negative test for channel user doesn't have
	response = rt.send(requestByUser("GET", "/db/doc2", "", "bernard"))
	assertStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.send(requestByUser("GET", "/db/doc3", "", "bernard"))
	assertStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns the docs the user has access to:
	response = rt.send(requestByUser("GET", "/db/_all_docs?channels=true", "", "bernard"))
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 3)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"books"})
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[1].Value.Channels, []string{"!"})

	// GET /db/_changes
	response = rt.send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)
	since := changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc1")
	assert.Equals(t, since, db.SequenceID{Seq: 1})

	// GET /db/_changes for single channel
	response = rt.send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=books", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 1)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc1")
	assert.Equals(t, since, db.SequenceID{Seq: 1})

	// GET /db/_changes for ! channel
	response = rt.send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since, db.SequenceID{Seq: 3})

	// GET /db/_changes for unauthorized channel
	response = rt.send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=gifts", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 0)

	//
	// Part 2 - Tests for user with * channel access
	//

	// Create a user:
	fran, err := a.NewUser("fran", "letmein", channels.SetOf("*"))
	a.Save(fran)

	// GET /db/docid - basic test for doc that has channel
	response = rt.send(requestByUser("GET", "/db/doc1", "", "fran"))
	assertStatus(t, response, 200)

	// GET /db/docid - test for doc with ! channel
	response = rt.send(requestByUser("GET", "/db/doc3", "", "fran"))
	assertStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns all docs (based on user * channel)
	response = rt.send(requestByUser("GET", "/db/_all_docs?channels=true", "", "fran"))
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 6)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"books"})

	// GET /db/_changes
	response = rt.send(requestByUser("GET", "/db/_changes", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 6)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc1")
	assert.Equals(t, since, db.SequenceID{Seq: 1})

	// GET /db/_changes for ! channel
	response = rt.send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since, db.SequenceID{Seq: 3})

	//
	// Part 3 - Tests for user with no user channel access
	//
	// Create a user:
	manny, err := a.NewUser("manny", "letmein", nil)
	a.Save(manny)

	// GET /db/docid - basic test for doc that has channel
	response = rt.send(requestByUser("GET", "/db/doc1", "", "manny"))
	assertStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.send(requestByUser("GET", "/db/doc3", "", "manny"))
	assertStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs only returns ! docs (based on doc ! channel)
	response = rt.send(requestByUser("GET", "/db/_all_docs?channels=true", "", "manny"))
	assertStatus(t, response, 200)
	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 2)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")

	// GET /db/_changes
	response = rt.send(requestByUser("GET", "/db/_changes", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since, db.SequenceID{Seq: 3})

	// GET /db/_changes for ! channel
	response = rt.send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since, db.SequenceID{Seq: 3})
}

// Test for issue #562
func TestCreateTarget(t *testing.T) {
	var rt restTester
	//Attempt to create existing target DB on public API
	response := rt.sendRequest("PUT", "/db/", "")
	assertStatus(t, response, 412)
	//Attempt to create new target DB on public API
	response = rt.sendRequest("PUT", "/foo/", "")
	assertStatus(t, response, 403)
}

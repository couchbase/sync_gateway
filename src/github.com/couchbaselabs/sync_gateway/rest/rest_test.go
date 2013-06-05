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
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/couchbaselabs/go.assert"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

//const kTestURL = "http://localhost:8091"
const kTestURL = "walrus:"

var gTestBucket base.Bucket

func init() {
	var err error
	gTestBucket, err = db.ConnectToBucket(kTestURL, "default", "sync_gateway_tests")
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
}

func newTempContext(dbName string, syncFn string) *serverContext {
	var syncFnPtr *string
	if len(syncFn) > 0 {
		syncFnPtr = &syncFn
	}
	sc := newServerContext(&ServerConfig{})
	bucket, _ := db.ConnectToBucket("walrus:", "default", dbName)
	if _, err := sc.addDatabase(bucket, "db", syncFnPtr, false); err != nil {
		panic(fmt.Sprintf("Error from addDatabase: %v", err))
	}
	return sc
}

func callRESTOnContext(sc *serverContext, request *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	createHandler(sc).ServeHTTP(response, request)
	return response
}

func callRESTOnRequest(bucket base.Bucket, request *http.Request) *httptest.ResponseRecorder {
	sc := newServerContext(&ServerConfig{})
	if _, err := sc.addDatabase(bucket, "db", nil, false); err != nil {
		panic(fmt.Sprintf("Error from addDatabase: %v", err))
	}
	return callRESTOnContext(sc, request)
}

func request(method, resource, body string) *http.Request {
	request, _ := http.NewRequest(method, "http://localhost"+resource, bytes.NewBufferString(body))
	return request
}

func requestByUser(method, resource, body, username string) *http.Request {
	r := request(method, resource, body)
	r.SetBasicAuth(username, "letmein")
	return r
}

func callRESTOn(bucket base.Bucket, method, resource string, body string) *httptest.ResponseRecorder {
	return callRESTOnRequest(bucket, request(method, resource, body))
}

func callREST(method, resource string, body string) *httptest.ResponseRecorder {
	return callRESTOn(gTestBucket, method, resource, body)
}

func assertStatus(t *testing.T, response *httptest.ResponseRecorder, expectedStatus int) {
	if response.Code != expectedStatus {
		t.Fatalf("Response status %d (expected %d): %s",
			response.Code, expectedStatus, response.Body)
	}
}

func TestRoot(t *testing.T) {
	response := callREST("GET", "/", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(),
		"{\"couchdb\":\"welcome\",\"version\":\""+VersionString+"\"}")

	response = callREST("HEAD", "/", "")
	assertStatus(t, response, 200)
	response = callREST("OPTIONS", "/", "")
	assertStatus(t, response, 200)
	response = callREST("PUT", "/", "")
	assertStatus(t, response, 405)
}

func createDoc(t *testing.T, docid string) string {
	response := callREST("PUT", "/db/"+docid, `{"prop":true}`)
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
	revid := createDoc(t, "doc")
	assert.Equals(t, revid, "1-45ca73d819d5b1c9b8eea95290e79004")

	response := callREST("DELETE", "/db/doc?rev="+revid, "")
	assertStatus(t, response, 200)
}

func TestBulkDocs(t *testing.T) {
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}]}`
	response := callREST("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
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
	response = callREST("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
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
	assertStatus(t, response, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-abc", "11-eleven"],
              "rd2": ["34-def", "31-one"],
              "rd9": ["1-a", "2-b", "3-c"]
             }`
	response = callREST("POST", "/db/_revs_diff", input)
	assertStatus(t, response, 200)
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
	assertStatus(t, response, 404)

	response = callREST("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = callREST("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-1","hi":"there"}`)

	response = callREST("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 409)
	response = callREST("PUT", "/db/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	assertStatus(t, response, 201)
	response = callREST("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-2","hi":"again"}`)

	// Check the handling of large integers, which caused trouble for us at one point:
	response = callREST("PUT", "/db/_local/loc1", `{"big": 123456789, "_rev": "0-2"}`)
	assertStatus(t, response, 201)
	response = callREST("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-3","big":123456789}`)

	response = callREST("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 409)
	response = callREST("DELETE", "/db/_local/loc1?rev=0-3", "")
	assertStatus(t, response, 200)
	response = callREST("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)
	response = callREST("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 404)
}

func TestLogin(t *testing.T) {
	bucket, _ := db.ConnectToBucket("walrus:", "default", "test")
	a := auth.NewAuthenticator(bucket, nil)
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	user, err = a.GetUser("")
	assert.Equals(t, err, nil)
	assert.True(t, user.Disabled())

	response := callRESTOn(bucket, "PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf("*"))
	a.Save(user)

	response = callRESTOn(bucket, "POST", "/db/_session", `{"name":"pupshaw", "password":"letmein"}`)
	assertStatus(t, response, 200)
	log.Printf("Set-Cookie: %s", response.Header().Get("Set-Cookie"))
	assert.True(t, response.Header().Get("Set-Cookie") != "")
}

func TestAccessControl(t *testing.T) {
	type viewRow struct {
		ID    string            `json:"id"`
		Key   string            `json:"key"`
		Value map[string]string `json:"value"`
		Doc   db.Body           `json:"doc,omitempty"`
	}
	var viewResult struct {
		TotalRows int       `json:"total_rows"`
		Offset    int       `json:"offset"`
		Rows      []viewRow `json:"rows"`
	}

	// Create some docs:
	bucket, _ := db.ConnectToBucket("walrus:", "default", "test")
	a := auth.NewAuthenticator(bucket, nil)
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	assertStatus(t, callRESTOn(bucket, "PUT", "/db/doc1", `{"channels":[]}`), 201)
	assertStatus(t, callRESTOn(bucket, "PUT", "/db/doc2", `{"channels":["CBS"]}`), 201)
	assertStatus(t, callRESTOn(bucket, "PUT", "/db/doc3", `{"channels":["CBS", "Cinemax"]}`), 201)
	assertStatus(t, callRESTOn(bucket, "PUT", "/db/doc4", `{"channels":["WB", "Cinemax"]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create a user:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf("Cinemax"))
	a.Save(alice)

	// Get a single doc the user has access to:
	request, _ := http.NewRequest("GET", "/db/doc3", nil)
	request.SetBasicAuth("alice", "letmein")
	response := callRESTOnRequest(bucket, request)
	assertStatus(t, response, 200)

	// Get a single doc the user doesn't have access to:
	request, _ = http.NewRequest("GET", "/db/doc2", nil)
	request.SetBasicAuth("alice", "letmein")
	response = callRESTOnRequest(bucket, request)
	assertStatus(t, response, 403)

	// Check that _all_docs only returns the docs the user has access to:
	request, _ = http.NewRequest("GET", "/db/_all_docs", nil)
	request.SetBasicAuth("alice", "letmein")
	response = callRESTOnRequest(bucket, request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &viewResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(viewResult.Rows), 2)
	assert.Equals(t, viewResult.Rows[0].ID, "doc3")
	assert.Equals(t, viewResult.Rows[1].ID, "doc4")

	// Check _all_docs with include_docs option:
	request, _ = http.NewRequest("GET", "/db/_all_docs?include_docs=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = callRESTOnRequest(bucket, request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &viewResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(viewResult.Rows), 2)
	assert.Equals(t, viewResult.Rows[0].ID, "doc3")
	assert.Equals(t, viewResult.Rows[1].ID, "doc4")
}

func TestChannelAccessChanges(t *testing.T) {
	//base.LogKeys["Access"] = true
	//base.LogKeys["CRUD"] = true

	sc := newTempContext("testaccesschanges", `function(doc) {access(doc.owner, doc._id);channel(doc.channel)}`)
	a := sc.databases["db"].auth
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
	response := callRESTOnContext(sc, request("PUT", "/db/alpha", `{"owner":"alice"}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	alphaRevID := body["rev"].(string)

	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/beta", `{"owner":"boadecia"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/delta", `{"owner":"alice"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/gamma", `{"owner":"zegpold"}`)), 201)

	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)

	// Check the _changes feed:
	var changes struct {
		Results []db.ChangeEntry
	}
	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, since, "gamma:8")

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.Channels(), channels.TimedSet{"zero": 0x1, "alpha": 0x1, "delta": 0x3})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.Channels(), channels.TimedSet{"zero": 0x1, "gamma": 0x4})

	// Update a document to revoke access to alice and grant it to zegpold:
	str := fmt.Sprintf(`{"owner":"zegpold", "_rev":%q}`, alphaRevID)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/alpha", str)), 201)

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.Channels(), channels.TimedSet{"zero": 0x1, "delta": 0x3})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.Channels(), channels.TimedSet{"zero": 0x1, "alpha": 0x9, "gamma": 0x4})

	// The complete _changes feed for zegpold contains docs a1 and g1:
	changes.Results = nil
	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Results[0].ID, "a1")
	assert.Equals(t, changes.Results[1].ID, "g1")

	// Changes feed with since=gamma:8 would ordinarily be empty, but zegpold got access to channel
	// alpha after sequence 8, so the pre-existing docs in that channel are included:
	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes?since="+since, "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "a1")
}

func TestRoleAccessChanges(t *testing.T) {
	base.LogKeys["Access"] = true
	base.LogKeys["CRUD"] = true

	sc := newTempContext("testroleaccesschanges", `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`)
	a := sc.databases["db"].auth
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
	response := callRESTOnContext(sc, request("PUT", "/db/fashion",
		`{"user":"alice","role":"hipster"}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	fashionRevID := body["rev"].(string)

	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.InheritedChannels(), channels.TimedSet{"alpha": 0x1, "gamma": 0x1})
	assert.DeepEquals(t, alice.RoleNames(), []string{"hipster"})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.InheritedChannels(), channels.TimedSet{"beta": 0x1})
	assert.DeepEquals(t, zegpold.RoleNames(), []string(nil))

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	since := changes.Last_Seq
	assert.Equals(t, since, "alpha:3,gamma:2")

	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since = changes.Last_Seq
	assert.Equals(t, since, "beta:4")

	// Update "fashion" doc to grant zegpold the role "hipster" and take it away from alice:
	str := fmt.Sprintf(`{"user":"zegpold", "role":"hipster", "_rev":%q}`, fashionRevID)
	assertStatus(t, callRESTOnContext(sc, request("PUT", "/db/fashion", str)), 201)

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.InheritedChannels(), channels.TimedSet{"alpha": 0x1})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.InheritedChannels(), channels.TimedSet{"beta": 0x1, "gamma": 0x1})

	// The complete _changes feed for zegpold contains docs g1 and b1:
	changes.Results = nil
	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Last_Seq, "beta:4,gamma:2")
	assert.Equals(t, changes.Results[0].ID, "g1")
	assert.Equals(t, changes.Results[1].ID, "b1")

	// Changes feed with since=beta:4 would ordinarily be empty, but zegpold got access to channel
	// gamma after sequence 4, so the pre-existing docs in that channel are included:
	response = callRESTOnContext(sc, requestByUser("GET", "/db/_changes?since="+since, "", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "g1")
}

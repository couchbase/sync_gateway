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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
	"github.com/robertkrimen/otto/underscore"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
)

func init() {
	base.LogNoColor()
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

//////// REST TESTER HELPER CLASS:

//////// AND NOW THE TESTS:

func TestRoot(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	response := rt.SendRequest("GET", "/", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["couchdb"], "Welcome")

	response = rt.SendRequest("HEAD", "/", "")
	assertStatus(t, response, 200)
	response = rt.SendRequest("OPTIONS", "/", "")
	assertStatus(t, response, 204)
	assert.Equals(t, response.Header().Get("Allow"), "GET, HEAD")
	response = rt.SendRequest("PUT", "/", "")
	assertStatus(t, response, 405)
	assert.Equals(t, response.Header().Get("Allow"), "GET, HEAD")
}

func (rt *RestTester) createDoc(t *testing.T, docid string) string {
	response := rt.SendRequest("PUT", "/db/"+docid, `{"prop":true}`)
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
	var rt RestTester
	defer rt.Close()

	revid := rt.createDoc(t, "doc")
	assert.Equals(t, revid, "1-45ca73d819d5b1c9b8eea95290e79004")

	response := rt.SendRequest("DELETE", "/db/doc?rev="+revid, "")
	assertStatus(t, response, 200)
}

//Validate that Etag header value is surrounded with double quotes, see issue #808
func TestDocEtag(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/doc", `{"prop":true}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revid := body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}

	//Validate Etag returned on doc creation
	assert.Equals(t, response.Header().Get("Etag"), strconv.Quote(revid))

	response = rt.SendRequest("GET", "/db/doc", "")
	assertStatus(t, response, 200)

	//Validate Etag returned when retrieving doc
	assert.Equals(t, response.Header().Get("Etag"), strconv.Quote(revid))

	//Validate Etag returned when updating doc
	response = rt.SendRequest("PUT", "/db/doc?rev="+revid, `{"prop":false}`)
	revid = body["rev"].(string)
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revid = body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}

	assert.Equals(t, response.Header().Get("Etag"), strconv.Quote(revid))

	//Test Attachments
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != revid)

	//validate Etag returned from adding an attachment
	assert.Equals(t, response.Header().Get("Etag"), strconv.Quote(revIdAfterAttachment))

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.Equals(t, response.Header().Get("Content-Disposition"), "")
	assert.Equals(t, response.Header().Get("Content-Type"), attachmentContentType)

	//Validate Etag returned from retrieving an attachment
	assert.Equals(t, response.Header().Get("Etag"), "\"sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=\"")

}

// Add and retrieve an attachment, including a subrange
func TestDocAttachment(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/doc", `{"prop":true}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	revid := body["rev"].(string)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.Equals(t, response.Header().Get("Accept-Ranges"), "bytes")
	assert.Equals(t, response.Header().Get("Content-Disposition"), "")
	assert.Equals(t, response.Header().Get("Content-Length"), "30")
	assert.Equals(t, response.Header().Get("Content-Type"), attachmentContentType)

	// retrieve subrange
	response = rt.SendRequestWithHeaders("GET", "/db/doc/attach1", "", map[string]string{"Range": "bytes=5-6"})
	assertStatus(t, response, 206)
	assert.Equals(t, string(response.Body.Bytes()), "is")
	assert.Equals(t, response.Header().Get("Accept-Ranges"), "bytes")
	assert.Equals(t, response.Header().Get("Content-Length"), "2")
	assert.Equals(t, response.Header().Get("Content-Range"), "bytes 5-6/30")
	assert.Equals(t, response.Header().Get("Content-Type"), attachmentContentType)
}

// Add an attachment to a document that has been removed from the users channels
func TestDocAttachmentOnRemovedRev(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	//Create a test user
	user, err = a.NewUser("user1", "letmein", channels.SetOf("foo"))
	a.Save(user)

	response := rt.Send(requestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user1"))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	revid := body["rev"].(string)

	//Put new revision removing document from users channel set
	response = rt.Send(requestByUser("PUT", "/db/doc?rev="+revid, `{"prop":true}`, "user1"))
	assertStatus(t, response, 201)
	json.Unmarshal(response.Body.Bytes(), &body)
	revid = body["rev"].(string)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should fail)
	response = rt.SendUserRequestWithHeaders("PUT", "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders, "user1", "letmein")
	assertStatus(t, response, 404)
}

func TestDocumentUpdateWithNullBody(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	//Create a test user
	user, err = a.NewUser("user1", "letmein", channels.SetOf("foo"))
	a.Save(user)
	//Create document
	response := rt.Send(requestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user1"))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	revid := body["rev"].(string)

	//Put new revision with null body
	response = rt.Send(requestByUser("PUT", "/db/doc?rev="+revid, "", "user1"))
	assertStatus(t, response, 400)
}

func TestFunkyDocIDs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	rt.createDoc(t, "AC%2FDC")

	response := rt.SendRequest("GET", "/db/AC%2FDC", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC+DC")
	response = rt.SendRequest("GET", "/db/AC+DC", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC+DC+GC")
	response = rt.SendRequest("GET", "/db/AC+DC+GC", "")
	assertStatus(t, response, 200)

	response = rt.SendRequest("PUT", "/db/foo+bar+moo+car", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/foo+bar+moo+car", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC%2BDC2")
	response = rt.SendRequest("GET", "/db/AC%2BDC2", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC%2BDC%2BGC2")
	response = rt.SendRequest("GET", "/db/AC%2BDC%2BGC2", "")
	assertStatus(t, response, 200)

	response = rt.SendRequest("PUT", "/db/foo%2Bbar%2Bmoo%2Bcar2", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/foo%2Bbar%2Bmoo%2Bcar2", "")
	assertStatus(t, response, 200)

	response = rt.SendRequest("PUT", "/db/foo%2Bbar+moo%2Bcar3", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/foo+bar%2Bmoo+car3", "")
	assertStatus(t, response, 200)
}

func TestFunkyDocAndAttachmentIDs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	//Create document with simple name
	doc1revId := rt.createDoc(t, "doc1")

	// add attachment with single embedded '/' (%2F HEX)
	response := rt.SendRequestWithHeaders("PUT", "/db/doc1/attachpath%2Fattachment.txt?rev="+doc1revId, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment := body["rev"].(string)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc1/attachpath%2Fattachment.txt", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add attachment with two embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attachpath%2Fattachpath2%2Fattachment.txt?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc1/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	//Create Doc with embedded '/' (%2F HEX) in name
	doc1revId = rt.createDoc(t, "AC%2FDC")

	response = rt.SendRequest("GET", "/db/AC%2FDC", "")
	assertStatus(t, response, 200)

	// add attachment with single embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders("PUT", "/db/AC%2FDC/attachpath%2Fattachment.txt?rev="+doc1revId, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment = body["rev"].(string)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/AC%2FDC/attachpath%2Fattachment.txt", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add attachment with two embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders("PUT", "/db/AC%2FDC/attachpath%2Fattachpath2%2Fattachment.txt?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/AC%2FDC/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	//Create Doc with embedded '+' (%2B HEX) in name
	doc1revId = rt.createDoc(t, "AC%2BDC%2BGC2")

	response = rt.SendRequest("GET", "/db/AC%2BDC%2BGC2", "")
	assertStatus(t, response, 200)

	// add attachment with single embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders("PUT", "/db/AC%2BDC%2BGC2/attachpath%2Fattachment.txt?rev="+doc1revId, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment = body["rev"].(string)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/AC%2BDC%2BGC2/attachpath%2Fattachment.txt", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add attachment with two embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders("PUT", "/db/AC%2BDC%2BGC2/attachpath%2Fattachpath2%2Fattachment.txt?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/AC%2BDC%2BGC2/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)
}

func TestCORSOrigin(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}
	response := rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "http://example.com")

	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "*")

	// now test another origin in config
	reqHeaders = map[string]string{
		"Origin": "http://staging.example.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "http://staging.example.com")

	// test no header on _admin apis
	reqHeaders = map[string]string{
		"Origin": "http://example.com",
	}
	response = rt.SendAdminRequestWithHeaders("GET", "/db/_all_docs", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "")

	// test with a config without * should reject non-matches
	sc := rt.ServerContext()
	sc.config.CORS.Origin = []string{"http://example.com", "http://staging.example.com"}
	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equals(t, response.Header().Get("Access-Control-Allow-Origin"), "")
}

func TestCORSLoginOriginOnSessionPost(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", "{\"name\":\"jchris\",\"password\":\"secret\"}", reqHeaders)
	assertStatus(t, response, 401)

	response = rt.SendRequestWithHeaders("POST", "/db/_facebook", `{"access_token":"true"}`, reqHeaders)
	assertStatus(t, response, 401)
}

// #issue 991
func TestCORSLoginOriginOnSessionPostNoCORSConfig(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	// Set CORS to nil
	sc := rt.ServerContext()
	sc.config.CORS = nil

	response := rt.SendRequestWithHeaders("POST", "/db/_session", `{"name":"jchris","password":"secret"}`, reqHeaders)
	assertStatus(t, response, 400)
}

func TestNoCORSOriginOnSessionPost(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://staging.example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", "{\"name\":\"jchris\",\"password\":\"secret\"}", reqHeaders)
	assertStatus(t, response, 400)

	response = rt.SendRequestWithHeaders("POST", "/db/_facebook", `{"access_token":"true"}`, reqHeaders)
	assertStatus(t, response, 400)
}

func TestCORSLogoutOriginOnSessionDelete(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 404)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["reason"], "no session")
}

func TestCORSLogoutOriginOnSessionDeleteNoCORSConfig(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	// Set CORS to nil
	sc := rt.ServerContext()
	sc.config.CORS = nil

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 400)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["reason"], "No CORS")
}

func TestNoCORSOriginOnSessionDelete(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://staging.example.com",
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 400)

	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["reason"], "No CORS")
}

func TestManualAttachment(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	doc1revId := rt.createDoc(t, "doc1")

	// attach to existing document without rev (should fail)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response := rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)

	// attach to existing document with wrong rev (should fail)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev=1-xyz", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)

	// attach to existing document with wrong rev using If-Match header (should fail)
	reqHeaders["If-Match"] = "1-dnf"
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to existing document with correct rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
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
	response = rt.SendRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// retrieve attachment as admin should have
	// Content-disposition: attachment
	response = rt.SendAdminRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Disposition") == `attachment; filename="attach1"`)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// try to overwrite that attachment
	attachmentBody = "updated content"
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
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
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
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
	response = rt.SendRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add another attachment to the document
	// also no explicit Content-Type header on this one
	// should default to application/octet-stream
	attachmentBody = "separate content"
	response = rt.SendRequest("PUT", "/db/doc1/attach2?rev="+revIdAfterUpdateAttachmentAgain, attachmentBody)
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
	response = rt.SendRequest("GET", "/db/doc1/attach2", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Type") == "application/octet-stream")

	// now check the attachments index on the document
	response = rt.SendRequest("GET", "/db/doc1", "")
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
	var rt RestTester
	defer rt.Close()

	// attach to new document using bogus rev (should fail)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "text/plain"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response := rt.SendRequestWithHeaders("PUT", "/db/notexistyet/attach1?rev=1-abc", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)

	// attach to new document using bogus rev using If-Match header (should fail)
	reqHeaders["If-Match"] = "1-xyz"
	response = rt.SendRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to new document without any rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/notexistyet/attach1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, string(response.Body.Bytes()), attachmentBody)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// now check the document
	body = db.Body{}
	response = rt.SendRequest("GET", "/db/notexistyet", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	// body should only have 3 top-level entries _id, _rev, _attachments
	assert.True(t, len(body) == 3)
}

func TestBulkDocs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}, {"_id": "_local/bulk3", "n": 3}]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	var docs []interface{}
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 3)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "1-50133ddd8e49efad34ad9ecae4cb9907", "id": "bulk1"})
	response = rt.SendRequest("GET", "/db/bulk1", "")
	assert.Equals(t, response.Body.String(), `{"_id":"bulk1","_rev":"1-50133ddd8e49efad34ad9ecae4cb9907","n":1}`)
	assert.DeepEquals(t, docs[1],
		map[string]interface{}{"rev": "1-035168c88bd4b80fb098a8da72f881ce", "id": "bulk2"})
	assert.DeepEquals(t, docs[2],
		map[string]interface{}{"rev": "0-1", "id": "_local/bulk3"})
	response = rt.SendRequest("GET", "/db/_local/bulk3", "")
	assert.Equals(t, response.Body.String(), `{"_id":"_local/bulk3","_rev":"0-1","n":3}`)
	assertStatus(t, response, 200)

	// update all documents
	input = `{"docs": [{"_id": "bulk1", "_rev" : "1-50133ddd8e49efad34ad9ecae4cb9907", "n": 10}, {"_id": "bulk2", "_rev":"1-035168c88bd4b80fb098a8da72f881ce", "n": 20}, {"_id": "_local/bulk3","_rev":"0-1","n": 30}]}`
	response = rt.SendRequest("POST", "/db/_bulk_docs", input)
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 3)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "2-7e384b16e63ee3218349ee568f156d6f", "id": "bulk1"})

	response = rt.SendRequest("GET", "/db/_local/bulk3", "")
	assert.Equals(t, response.Body.String(), `{"_id":"_local/bulk3","_rev":"0-2","n":30}`)
	assertStatus(t, response, 200)
}

func TestBulkDocsIDGeneration(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	input := `{"docs": [{"n": 1}, {"_id": 123, "n": 2}]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	var docs []map[string]string
	json.Unmarshal(response.Body.Bytes(), &docs)
	log.Printf("response: %s", response.Body.Bytes())
	assertStatus(t, response, 201)
	assert.Equals(t, len(docs), 2)
	assert.Equals(t, docs[0]["rev"], "1-50133ddd8e49efad34ad9ecae4cb9907")
	assert.True(t, docs[0]["id"] != "")
	assert.Equals(t, docs[1]["rev"], "1-035168c88bd4b80fb098a8da72f881ce")
	assert.True(t, docs[1]["id"] != "")
}

func TestBulkDocsUnusedSequences(t *testing.T) {

	//We want a sync function that will reject some docs
	rt := RestTester{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, _ := rt.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev.Sequence, uint64(1))

	doc3Rev, _ := rt.GetDatabase().GetDocSyncData("bulk3")
	assert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, _ := rt.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(3))

	//send another _bulk_docs and validate the sequences used
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23}]}`
	response = rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 get used here
	doc21Rev, _ := rt.GetDatabase().GetDocSyncData("bulk21")
	assert.Equals(t, doc21Rev.Sequence, uint64(3))

	doc22Rev, _ := rt.GetDatabase().GetDocSyncData("bulk22")
	assert.Equals(t, doc22Rev.Sequence, uint64(4))

	doc23Rev, _ := rt.GetDatabase().GetDocSyncData("bulk23")
	assert.Equals(t, doc23Rev.Sequence, uint64(5))

	//Get current sequence number
	lastSequence, _ = rt.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(5))
}

func TestBulkDocsUnusedSequencesMultipleSG(t *testing.T) {

	//We want a sync function that will reject some docs, create two to simulate two SG instances
	rt1 := RestTester{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt1.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev.Sequence, uint64(1))

	doc3Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk3")
	assert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, _ := rt1.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(3))

	rt2 := RestTester{RestTesterBucket: rt1.RestTesterBucket, SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt2.Close()

	rt2.RestTesterServerContext = NewServerContext(&ServerConfig{
		Facebook:       &FacebookConfig{},
		AdminInterface: &DefaultAdminInterface,
	})

	server := base.UnitTestUrl()
	bucketName := rt1.RestTesterBucket.GetName()
	spec := base.GetTestBucketSpec(base.DataBucket)
	username, password, _ := spec.Auth.GetCredentials()

	_, err := rt2.RestTesterServerContext.AddDatabaseFromConfig(&DbConfig{
		BucketConfig: BucketConfig{
			Server: &server,
			Bucket: &bucketName,
			Username: username,
			Password: password,
		},
		Name: "db",
	})

	assertNoError(t, err, "Failed to add database to rest tester")

	//send another _bulk_docs to rt2 and validate the sequences used
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23}]}`
	response = rt2.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 does not get used here as its using a different sequence allocator
	doc21Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk21")
	assert.Equals(t, doc21Rev.Sequence, uint64(4))

	doc22Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk22")
	assert.Equals(t, doc22Rev.Sequence, uint64(5))

	doc23Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk23")
	assert.Equals(t, doc23Rev.Sequence, uint64(6))

	//Get current sequence number
	lastSequence, _ = rt2.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(6))

	//Now send a bulk_doc to rt1 and see if it uses sequence 3
	input = `{"docs": [{"_id": "bulk31", "n": 31}, {"_id": "bulk32", "n": 32}, {"_id": "bulk33", "n": 33}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 get used here as its using first sequence allocator
	doc31Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk31")
	assert.Equals(t, doc31Rev.Sequence, uint64(3))

	doc32Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk32")
	assert.Equals(t, doc32Rev.Sequence, uint64(7))

	doc33Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk33")
	assert.Equals(t, doc33Rev.Sequence, uint64(8))

}

func TestBulkDocsUnusedSequencesMultiRevDoc(t *testing.T) {

	//We want a sync function that will reject some docs, create two to simulate two SG instances
	rt1 := RestTester{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt1.Close()

	//add new docs, doc2 will be rejected by sync function
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev.Sequence, uint64(1))

	//Get the revID for doc "bulk1"
	doc1RevID := doc1Rev.CurrentRev

	doc3Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk3")
	assert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, _ := rt1.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(3))

	rt2 := RestTester{RestTesterBucket: rt1.RestTesterBucket, SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt2.Close()

	rt2.RestTesterServerContext = NewServerContext(&ServerConfig{
		Facebook:       &FacebookConfig{},
		AdminInterface: &DefaultAdminInterface,
	})

	server := base.UnitTestUrl()
	bucketName := rt1.RestTesterBucket.GetName()
	spec := base.GetTestBucketSpec(base.DataBucket)
	username, password, _ := spec.Auth.GetCredentials()

	_, err := rt2.RestTesterServerContext.AddDatabaseFromConfig(&DbConfig{
		BucketConfig: BucketConfig{
			Server: &server,
			Bucket: &bucketName,
			Username: username,
			Password: password,
			},
		Name: "db",
	})

	assertNoError(t, err, "Failed to add database to rest tester")

	//send another _bulk_docs to rt2, including an update to doc "bulk1" and validate the sequences used
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23}, {"_id": "bulk1", "_rev": "` + doc1RevID + `", "n": 2}]}`
	response = rt2.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 does not get used here as its using a different sequence allocator
	doc21Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk21")
	assert.Equals(t, doc21Rev.Sequence, uint64(4))

	doc22Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk22")
	assert.Equals(t, doc22Rev.Sequence, uint64(5))

	doc23Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk23")
	assert.Equals(t, doc23Rev.Sequence, uint64(6))

	//Validate rev2 of doc "bulk1" has a new revision
	doc1Rev2, _ := rt2.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev2.Sequence, uint64(7))

	//Get the revID for doc "bulk1"
	doc1RevID2 := doc1Rev2.CurrentRev

	//Get current sequence number
	lastSequence, _ = rt2.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(7))

	//Now send a bulk_doc to rt1 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID2 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 8 should get used here as sequence 3 should have been dropped by the first sequence allocator
	doc1Rev3, _ := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev3.Sequence, uint64(8))

	//validate the doc _sync metadata, should see last sequence lower than previous sequence
	rs := doc1Rev3.RecentSequences
	assert.Equals(t, len(rs), 3)
	assert.Equals(t, rs[0], uint64(1))
	assert.Equals(t, rs[1], uint64(7))
	assert.Equals(t, rs[2], uint64(8))

}

func TestBulkDocsUnusedSequencesMultiRevDoc2SG(t *testing.T) {

	//We want a sync function that will reject some docs, create two to simulate two SG instances
	rt1 := RestTester{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt1.Close()

	//add new docs, doc2 will be rejected by sync function
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev.Sequence, uint64(1))

	//Get the revID for doc "bulk1"
	doc1RevID := doc1Rev.CurrentRev

	doc3Rev, _ := rt1.GetDatabase().GetDocSyncData("bulk3")
	assert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, _ := rt1.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(3))

	rt2 := RestTester{RestTesterBucket: rt1.RestTesterBucket, SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	defer rt2.Close()

	rt2.RestTesterServerContext = NewServerContext(&ServerConfig{
		Facebook:       &FacebookConfig{},
		AdminInterface: &DefaultAdminInterface,
	})

	server := base.UnitTestUrl()
	bucketName := rt1.RestTesterBucket.GetName()
	spec := base.GetTestBucketSpec(base.DataBucket)
	username, password, _ := spec.Auth.GetCredentials()

	_, err := rt2.RestTesterServerContext.AddDatabaseFromConfig(&DbConfig{
		BucketConfig: BucketConfig{
			Server: &server,
			Bucket: &bucketName,
			Username: username,
			Password: password,
			},
		Name: "db",
	})

	assertNoError(t, err, "Failed to add database to rest tester")

	//send another _bulk_docs to rt2, including an update to doc "bulk1" and another invalid rev to create an unused sequence
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23, "type": "invalid"}, {"_id": "bulk1", "_rev": "` + doc1RevID + `", "n": 2}]}`
	response = rt2.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 does not get used here as its using a different sequence allocator
	doc21Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk21")
	assert.Equals(t, doc21Rev.Sequence, uint64(4))

	doc22Rev, _ := rt2.GetDatabase().GetDocSyncData("bulk22")
	assert.Equals(t, doc22Rev.Sequence, uint64(5))

	//Validate rev2 of doc "bulk1" has a new revision
	doc1Rev2, _ := rt2.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev2.Sequence, uint64(7))

	//Get the revID for doc "bulk1"
	doc1RevID2 := doc1Rev2.CurrentRev

	//Get current sequence number
	lastSequence, _ = rt2.GetDatabase().LastSequence()
	assert.Equals(t, lastSequence, uint64(7))

	//Now send a bulk_doc to rt1 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID2 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 8 should get used here as sequence 3 should have been dropped by the first sequence allocator
	doc1Rev3, _ := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev3.Sequence, uint64(8))

	//Get the revID for doc "bulk1"
	doc1RevID3 := doc1Rev3.CurrentRev

	//Now send a bulk_doc to rt2 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID3 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 9 should get used here as sequence 6 should have been dropped by the second sequence allocator
	doc1Rev4, _ := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.Equals(t, doc1Rev4.Sequence, uint64(9))

	//validate the doc _sync metadata, should see last sequence lower than previous sequence
	rs := doc1Rev4.RecentSequences
	assert.Equals(t, len(rs), 4)
	assert.Equals(t, rs[0], uint64(1))
	assert.Equals(t, rs[1], uint64(7))
	assert.Equals(t, rs[2], uint64(8))
	assert.Equals(t, rs[3], uint64(9))

}

func TestBulkDocsEmptyDocs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	input := `{}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 400)
}

func TestBulkDocsMalformedDocs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	input := `{"docs":["A","B"]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 400)

	// For non-string id, ensure it reverts to id generation and doesn't panic
	input = `{"docs": [{"_id": 3, "n": 1}]}`
	response = rt.SendRequest("POST", "/db/_bulk_docs", input)
	log.Printf("response:%s", response.Body.Bytes())
	assertStatus(t, response, 201)
}

func TestBulkGetEmptyDocs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	input := `{}`
	response := rt.SendRequest("POST", "/db/_bulk_get", input)
	assertStatus(t, response, 400)
}

func TestBulkDocsChangeToAccess(t *testing.T) {

	var logKeys = map[string]bool{
		"Access": true,
	}

	base.UpdateLogKeys(logKeys, true)

	rt := RestTester{SyncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { requireAccess(doc.channel)}}`}
	defer rt.Close()

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

	response := rt.Send(requestByUser("POST", "/db/_bulk_docs", input, "user1"))
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
	var rt RestTester
	defer rt.Close()

	input := `{"new_edits":false, "docs": [
                    {"_id": "bdne1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "bdne2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
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
	response = rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	json.Unmarshal(response.Body.Bytes(), &docs)
	assert.Equals(t, len(docs), 1)
	assert.DeepEquals(t, docs[0],
		map[string]interface{}{"rev": "14-jkl", "id": "bdne1"})
}

type RevDiffResponse map[string][]string
type RevsDiffResponse map[string]RevDiffResponse

func TestRevsDiff(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-xyz"],
              "rd2": ["34-def"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
	response = rt.SendRequest("POST", "/db/_revs_diff", input)
	assertStatus(t, response, 200)
	var diffResponse RevsDiffResponse
	json.Unmarshal(response.Body.Bytes(), &diffResponse)
	sort.Strings(diffResponse["rd1"]["possible_ancestors"])
	assert.DeepEquals(t, diffResponse, RevsDiffResponse{
		"rd1": RevDiffResponse{"missing": []string{"13-def", "12-xyz"},
			"possible_ancestors": []string{"11-eleven", "12-abc"}},
		"rd9": RevDiffResponse{"missing": []string{"1-a", "2-b", "3-c"}}})
}

func TestOpenRevs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "or1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}}
              ]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	reqHeaders := map[string]string{
		"Accept": "application/json",
	}
	response = rt.SendRequestWithHeaders("GET", `/db/or1?open_revs=["12-abc","10-ten"]`, "", reqHeaders)
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `[
{"ok":{"_id":"or1","_rev":"12-abc","_revisions":{"ids":["abc","eleven","ten","nine"],"start":12},"n":1}}
,{"missing":"10-ten"}
]`)
}

func TestLocalDocs(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	response := rt.SendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)

	response = rt.SendRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-1","hi":"there"}`)

	response = rt.SendRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 409)
	response = rt.SendRequest("PUT", "/db/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-2","hi":"again"}`)

	// Check the handling of large integers, which caused trouble for us at one point:
	response = rt.SendRequest("PUT", "/db/_local/loc1", `{"big": 123456789, "_rev": "0-2"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.Equals(t, response.Body.String(), `{"_id":"_local/loc1","_rev":"0-3","big":123456789}`)

	response = rt.SendRequest("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 409)
	response = rt.SendRequest("DELETE", "/db/_local/loc1?rev=0-3", "")
	assertStatus(t, response, 200)
	response = rt.SendRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)
	response = rt.SendRequest("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 404)

	// Check the handling of URL encoded slash at end of _local%2Fdoc
	response = rt.SendRequest("PUT", "/db/_local%2Floc12", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/_local/loc2", "")
	assertStatus(t, response, 404)
	response = rt.SendRequest("DELETE", "/db/_local%2floc2", "")
	assertStatus(t, response, 404)
}

func TestResponseEncoding(t *testing.T) {
	// Make a doc longer than 1k so the HTTP response will be compressed:
	str := "DORKY "
	for i := 0; i < 10; i++ {
		str = str + str
	}
	docJSON := fmt.Sprintf(`{"long": %q}`, str)

	var rt RestTester
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/_local/loc1", docJSON)
	assertStatus(t, response, 201)
	response = rt.SendRequestWithHeaders("GET", "/db/_local/loc1", "",
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
	var rt RestTester
	defer rt.Close()

	a := auth.NewAuthenticator(rt.Bucket(), nil)
	user, err := a.GetUser("")
	assert.Equals(t, err, nil)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.Equals(t, err, nil)

	user, err = a.GetUser("")
	assert.Equals(t, err, nil)
	assert.True(t, user.Disabled())

	response := rt.SendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf("*"))
	a.Save(user)

	assertStatus(t, rt.SendRequest("GET", "/db/_session", ""), 200)

	response = rt.SendRequest("POST", "/db/_session", `{"name":"pupshaw", "password":"letmein"}`)
	assertStatus(t, response, 200)
	log.Printf("Set-Cookie: %s", response.Header().Get("Set-Cookie"))
	assert.True(t, response.Header().Get("Set-Cookie") != "")
}

func TestReadChangesOptionsFromJSON(t *testing.T) {

	h := &handler{}
	h.server = NewServerContext(&ServerConfig{})

	// Basic case, no heartbeat, no timeout
	optStr := `{"feed":"longpoll", "since": "123456:78", "limit":123, "style": "all_docs",
				"include_docs": true, "filter": "Melitta", "channels": "ABC,BBC"}`
	feed, options, filter, channelsArray, _, _, err := h.readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, feed, "longpoll")

	assert.Equals(t, options.Since.Seq, uint64(78))
	assert.Equals(t, options.Since.TriggeredBy, uint64(123456))
	assert.Equals(t, options.Limit, 123)
	assert.Equals(t, options.Conflicts, true)
	assert.Equals(t, options.IncludeDocs, true)
	assert.Equals(t, options.HeartbeatMs, uint64(kDefaultHeartbeatMS))
	assert.Equals(t, options.TimeoutMs, uint64(kDefaultTimeoutMS))

	assert.Equals(t, filter, "Melitta")
	assert.DeepEquals(t, channelsArray, []string{"ABC", "BBC"})

	// Attempt to set heartbeat, timeout to valid values
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":30000, "timeout":60000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, options.HeartbeatMs, uint64(30000))
	assert.Equals(t, options.TimeoutMs, uint64(60000))

	// Attempt to set valid timeout, no heartbeat
	optStr = `{"feed":"longpoll", "since": "1", "timeout":2000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, options.TimeoutMs, uint64(2000))

	// Disable heartbeat, timeout by explicitly setting to zero
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":0, "timeout":0}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, options.HeartbeatMs, uint64(0))
	assert.Equals(t, options.TimeoutMs, uint64(0))

	// Attempt to set heartbeat less than minimum heartbeat, timeout greater than max timeout
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":1000, "timeout":1000000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, options.HeartbeatMs, uint64(kMinHeartbeatMS))
	assert.Equals(t, options.TimeoutMs, uint64(kMaxTimeoutMS))

	// Set max heartbeat in server context, attempt to set heartbeat greater than max
	h.server.config.MaxHeartbeat = 60
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":90000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.Equals(t, err, nil)
	assert.Equals(t, options.HeartbeatMs, uint64(60000))
}

func TestAccessControl(t *testing.T) {
	restTester := initRestTester(db.IntSequenceType, `function(doc) {channel(doc.channels);}`)
	defer restTester.Close()
	testAccessControl(t, restTester)
}

func TestVbSeqAccessControl(t *testing.T) {

	restTester := initRestTester(db.ClockSequenceType, `function(doc) {channel(doc.channels);}`)
	defer restTester.Close()
	testAccessControl(t, restTester)
}

func testAccessControl(t *testing.T, rt indexTester) {
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
	a := auth.NewAuthenticator(rt.Bucket(), nil)
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	assertStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":[]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["CBS"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["CBS", "Cinemax"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["WB", "Cinemax"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":"Cinemax"}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create a user:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf("Cinemax"))
	a.Save(alice)

	// Get a single doc the user has access to:
	request, _ := http.NewRequest("GET", "/db/doc3", nil)
	request.SetBasicAuth("alice", "letmein")
	response := rt.Send(request)
	assertStatus(t, response, 200)

	// Get a single doc the user doesn't have access to:
	request, _ = http.NewRequest("GET", "/db/doc2", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 403)

	// Check that _all_docs only returns the docs the user has access to:
	request, _ = http.NewRequest("GET", "/db/_all_docs?channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 3)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[1].Value.Channels, []string{"Cinemax"})
	assert.Equals(t, allDocsResult.Rows[2].ID, "doc5")
	assert.DeepEquals(t, allDocsResult.Rows[2].Value.Channels, []string{"Cinemax"})

	//Check all docs limit option
	request, _ = http.NewRequest("GET", "/db/_all_docs?limit=1&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	//Check all docs startkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?startkey=doc5&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc5")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	//Check all docs startkey option with double quote
	request, _ = http.NewRequest("GET", `/db/_all_docs?startkey="doc5"&channels=true`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc5")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	//Check all docs endkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?endkey=doc3&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	//Check all docs endkey option
	request, _ = http.NewRequest("GET", `/db/_all_docs?endkey="doc3"&channels=true`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
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
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 3)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc4")
	assert.Equals(t, allDocsResult.Rows[2].ID, "doc5")

	// Check POST to _all_docs:
	body := `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
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
	response = rt.Send(request)
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
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].Key, "doc4")
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc4")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"Cinemax"})

	// Check _all_docs as admin:
	response = rt.SendAdminRequest("GET", "/db/_all_docs", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 5)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[1].ID, "doc2")

}




func TestChannelAccessChanges(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Changes+", "CRUD", "DIndex+"})

	rt := RestTester{SyncFn: `function(doc) {access(doc.owner, doc._id);channel(doc.channel)}`}
	defer rt.Close()

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
	response := rt.Send(request("PUT", "/db/alpha", `{"owner":"alice"}`)) // seq=1
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	alphaRevID := body["rev"].(string)

	assertStatus(t, rt.Send(request("PUT", "/db/beta", `{"owner":"boadecia"}`)), 201) // seq=2
	assertStatus(t, rt.Send(request("PUT", "/db/delta", `{"owner":"alice"}`)), 201)   // seq=3
	assertStatus(t, rt.Send(request("PUT", "/db/gamma", `{"owner":"zegpold"}`)), 201) // seq=4

	assertStatus(t, rt.Send(request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201) // seq=5
	assertStatus(t, rt.Send(request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)  // seq=6
	assertStatus(t, rt.Send(request("PUT", "/db/d1", `{"channel":"delta"}`)), 201) // seq=7
	assertStatus(t, rt.Send(request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201) // seq=8


	numChangesExpected := 1
	changes, err := rt.WaitForChanges(numChangesExpected, "/db/_changes", "zegpold")

	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), numChangesExpected)
	since := changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "g1")
	assert.Equals(t, since.Seq, uint64(8))

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.Channels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "zero": channels.NewVbSimpleSequence(0x1), "alpha": channels.NewVbSimpleSequence(0x1), "delta": channels.NewVbSimpleSequence(0x3)})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.Channels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "zero": channels.NewVbSimpleSequence(0x1), "gamma": channels.NewVbSimpleSequence(0x4)})

	// Update a document to revoke access to alice and grant it to zegpold:
	str := fmt.Sprintf(`{"owner":"zegpold", "_rev":%q}`, alphaRevID)
	assertStatus(t, rt.Send(request("PUT", "/db/alpha", str)), 201) // seq=9

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.Channels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "zero": channels.NewVbSimpleSequence(0x1), "delta": channels.NewVbSimpleSequence(0x3)})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.Channels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "zero": channels.NewVbSimpleSequence(0x1), "alpha": channels.NewVbSimpleSequence(0x9), "gamma": channels.NewVbSimpleSequence(0x4)})

	// Artificial delay to let updates show up on changes feed
	time.Sleep(time.Second)

	// Look at alice's _changes feed:
	numChangesExpected = 1
	changes, err = rt.WaitForChanges(numChangesExpected, "/db/_changes", "alice")
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), numChangesExpected)
	assert.Equals(t, changes.Results[0].ID, "d1")

	// The complete _changes feed for zegpold contains docs a1 and g1:
	numChangesExpected = 2
	changes, err = rt.WaitForChanges(numChangesExpected, "/db/_changes", "zegpold")
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), numChangesExpected)
	assert.Equals(t, changes.Results[0].ID, "g1")
	assert.Equals(t, changes.Results[0].Seq.Seq, uint64(8))
	assert.Equals(t, changes.Results[1].ID, "a1")
	assert.Equals(t, changes.Results[1].Seq.Seq, uint64(5))
	assert.Equals(t, changes.Results[1].Seq.TriggeredBy, uint64(9))

	// Changes feed with since=gamma:8 would ordinarily be empty, but zegpold got access to channel
	// alpha after sequence 8, so the pre-existing docs in that channel are included:
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=\"%s\"", since),
		"", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "a1")

	// What happens if we call access() with a nonexistent username?
	assertStatus(t, rt.Send(request("PUT", "/db/epsilon", `{"owner":"waldo"}`)), 201)

	// Wait for change caching to complete before running resync below
	time.Sleep(500 * time.Millisecond)

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


	expectedIDs := []string{"beta", "delta", "gamma", "a1", "b1", "d1", "g1", "alpha", "epsilon"}
	numChangesExpected = len(expectedIDs)
	changes, err = rt.WaitForChanges(numChangesExpected, "/db/_changes", "alice")

	changes.Results = nil
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), numChangesExpected)
	for i, expectedID := range expectedIDs {
		assert.Equals(t, changes.Results[i].ID, expectedID)
	}

	// Check accumulated statistics:
	assert.True(t, db.ChangesClientStats.TotalCount() >= uint32(5))  // there might be retries, but there should be at least 5 changes requests
	assert.Equals(t, db.ChangesClientStats.MaxCount(), uint32(1))
	db.ChangesClientStats.Reset()
	assert.Equals(t, db.ChangesClientStats.TotalCount(), uint32(0))
	assert.Equals(t, db.ChangesClientStats.MaxCount(), uint32(0))
}

func TestAccessOnTombstone(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Changes+", "CRUD", "DIndex+"})

	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 if (doc.owner) {
			 	access(doc.owner, doc.channel);
			 }
			 if (doc._deleted && oldDoc.owner) {
			 	access(oldDoc.owner, oldDoc.channel);
			 }
			 channel(doc.channel)
		 }`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("zero"))
	a.Save(bernard)

	// Create doc that gives user access to its channel
	response := rt.Send(request("PUT", "/db/alpha", `{"owner":"bernard", "channel":"PBS"}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)

	rt.WaitForPendingChanges()

	// Validate the user gets the doc on the _changes feed
	// Check the _changes feed:
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "alpha")

	// Delete the document
	response = rt.Send(request("DELETE", fmt.Sprintf("/db/alpha?rev=%s", revId), ""))
	assertStatus(t, response, 200)

	// Wait for change caching to complete
	rt.WaitForPendingChanges()

	// Check user access again:
	changes.Results = nil
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	if len(changes.Results) > 0 {
		assert.Equals(t, changes.Results[0].ID, "alpha")
		assert.Equals(t, changes.Results[0].Deleted, true)
	}

}

//Test for wrong _changes entries for user joining a populated channel
func TestUserJoiningPopulatedChannel(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Cache+", "Changes", "Changes+", "CRUD"})

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channels)}`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	// Create 100 docs
	for i := 0; i < 100; i++ {
		docpath := fmt.Sprintf("/db/doc%d", i)
		assertStatus(t, rt.Send(request("PUT", docpath, `{"foo": "bar", "channels":["alpha"]}`)), 201)
	}

	// Check the _changes feed with limit, to split feed into two
	var changes struct {
		Results []db.ChangeEntry
	}

	limit := 50
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?limit=%d", limit), "", "user1"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since := changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc48")
	assert.Equals(t, since.Seq, uint64(50))

	//// Check the _changes feed with  since and limit, to get second half of feed
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=\"%s\"&limit=%d", since, limit), "", "user1"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since = changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc98")
	assert.Equals(t, since.Seq, uint64(100))

	// Create user2
	response = rt.SendAdminRequest("PUT", "/db/_user/user2", `{"email":"user2@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	//Retrieve all changes for user2 with no limits
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes"), "", "user2"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 101)
	assert.Equals(t, changes.Results[99].ID, "doc99")

	// Create user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"email":"user3@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	//Get first 50 document changes
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?limit=%d", limit), "", "user3"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since = changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc49")
	assert.Equals(t, since.Seq, uint64(51))
	assert.Equals(t, since.TriggeredBy, uint64(103))

	//// Get remainder of changes i.e. no limit parameter
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=\"%s\"", since), "", "user3"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 51)
	assert.Equals(t, changes.Results[49].ID, "doc99")

	// Create user4
	response = rt.SendAdminRequest("PUT", "/db/_user/user4", `{"email":"user4@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?limit=%d", limit), "", "user4"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	since = changes.Results[49].Seq
	assert.Equals(t, changes.Results[49].ID, "doc49")
	assert.Equals(t, since.Seq, uint64(51))
	assert.Equals(t, since.TriggeredBy, uint64(104))

	//// Check the _changes feed with  since and limit, to get second half of feed
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s&limit=%d", since, limit), "", "user4"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 50)
	assert.Equals(t, changes.Results[49].ID, "doc99")

}

func TestRoleAssignmentBeforeUserExists(t *testing.T) {

	var logKeys = map[string]bool{
		"Access":   true,
		"CRUD":     true,
		"Changes+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	rt := RestTester{SyncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// POST a role
	response := rt.SendAdminRequest("POST", "/db/_role/", `{"name":"role1","admin_channels":["chan1"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/role1", "")
	assertStatus(t, response, 200)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["name"], "role1")

	//Put document to trigger sync function
	response = rt.Send(request("PUT", "/db/doc1", `{"user":"user1", "role":"role:role1", "channel":"chan1"}`)) // seq=1
	assertStatus(t, response, 201)
	body = nil
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)

	// POST the new user the GET and verify that it shows the assigned role
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"letmein"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/user1", "")
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

	var logKeys = map[string]bool{
		"Access":   true,
		"CRUD":     true,
		"Changes+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	rt := RestTester{SyncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	defer rt.Close()

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
	response := rt.Send(request("PUT", "/db/fashion",
		`{"user":"alice","role":["role:hipster","role:bogus"]}`)) // seq=1
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	fashionRevID := body["rev"].(string)

	assertStatus(t, rt.Send(request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201) // seq=2
	assertStatus(t, rt.Send(request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201) // seq=3
	assertStatus(t, rt.Send(request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)  // seq=4
	assertStatus(t, rt.Send(request("PUT", "/db/d1", `{"channel":"delta"}`)), 201) // seq=5

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.InheritedChannels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "alpha": channels.NewVbSimpleSequence(0x1), "gamma": channels.NewVbSimpleSequence(0x1)})
	assert.DeepEquals(t, alice.RoleNames(), channels.TimedSet{"bogus": channels.NewVbSimpleSequence(0x1), "hipster": channels.NewVbSimpleSequence(0x1)})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.InheritedChannels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "beta": channels.NewVbSimpleSequence(0x1)})
	assert.DeepEquals(t, zegpold.RoleNames(), channels.TimedSet{})

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("1st _changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	since := changes.Last_Seq
	assert.Equals(t, since, "3")

	response = rt.Send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("2nd _changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since = changes.Last_Seq
	assert.Equals(t, since, "4")

	// Update "fashion" doc to grant zegpold the role "hipster" and take it away from alice:
	str := fmt.Sprintf(`{"user":"zegpold", "role":"role:hipster", "_rev":%q}`, fashionRevID)
	assertStatus(t, rt.Send(request("PUT", "/db/fashion", str)), 201) // seq=6

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.DeepEquals(t, alice.InheritedChannels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "alpha": channels.NewVbSimpleSequence(0x1)})
	zegpold, _ = a.GetUser("zegpold")
	assert.DeepEquals(t, zegpold.InheritedChannels(), channels.TimedSet{"!": channels.NewVbSimpleSequence(0x1), "beta": channels.NewVbSimpleSequence(0x1), "gamma": channels.NewVbSimpleSequence(0x6)})

	// The complete _changes feed for zegpold contains docs g1 and b1:
	changes.Results = nil
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("3rd _changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Last_Seq, "6:2")
	assert.Equals(t, changes.Results[0].ID, "b1")
	assert.Equals(t, changes.Results[1].ID, "g1")

	// Changes feed with since=4 would ordinarily be empty, but zegpold got access to channel
	// gamma after sequence 4, so the pre-existing docs in that channel are included:
	var additionalLogKeys = map[string]bool{
		"Changes": true,
		"Cache":   true,
	}

	base.UpdateLogKeys(additionalLogKeys, false)

	response = rt.Send(requestByUser("GET", "/db/_changes?since=4", "", "zegpold"))
	log.Printf("4th _changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "g1")
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

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channels)}`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	// Create a doc
	response := rt.Send(request("PUT", "/db/doc1", `{"foo":"bar", "channels":["ch1"]}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	doc1RevID := body["rev"].(string)

	// Run GET _all_docs as admin with channels=true:
	response = rt.SendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[0].Value.Channels[0], "ch1")

	// Run POST _all_docs as admin with explicit docIDs and channels=true:
	keys := `{"keys": ["doc1"]}`
	response = rt.SendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 1)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.Equals(t, allDocsResult.Rows[0].Value.Channels[0], "ch1")

	//Commit rev 2 that maps to a differenet channel
	str := fmt.Sprintf(`{"foo":"bar", "channels":["ch2"], "_rev":%q}`, doc1RevID)
	assertStatus(t, rt.Send(request("PUT", "/db/doc1", str)), 201)

	// Run GET _all_docs as admin with channels=true
	// Make sure that only the new channel appears in the docs channel list
	response = rt.SendAdminRequest("GET", "/db/_all_docs?channels=true", "")
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
	response = rt.SendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
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

	var rt RestTester
	defer rt.Close()

	doc1revId := rt.createDoc(t, "doc1")

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response := rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
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
	response = rt.SendRequestWithHeaders("GET", fmt.Sprintf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, doc1revId), "", reqHeaders)
	assert.Equals(t, response.Code, 200)
	//validate attachment has data property
	json.Unmarshal(response.Body.Bytes(), &body)
	log.Printf("response body revid1 = %s", body)
	attachments := body["_attachments"].(map[string]interface{})
	attach1 := attachments["attach1"].(map[string]interface{})
	data := attach1["data"]
	assert.True(t, data != nil)

	log.Printf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, revIdAfterAttachment)
	response = rt.SendRequestWithHeaders("GET", fmt.Sprintf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, revIdAfterAttachment), "", reqHeaders)
	assert.Equals(t, response.Code, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	log.Printf("response body revid1 = %s", body)
	attachments = body["_attachments"].(map[string]interface{})
	attach1 = attachments["attach1"].(map[string]interface{})
	data = attach1["data"]
	assert.True(t, data == nil)

}

func TestOldDocHandling(t *testing.T) {

	rt := RestTester{SyncFn: `
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
	defer rt.Close()

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
	response := rt.Send(request("PUT", "/db/testOldDocId", `{"foo":"bar"}`))
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	alphaRevID := body["rev"].(string)

	// Update a document to validate oldDoc id handling.  Will reject if old doc id not available
	str := fmt.Sprintf(`{"foo":"ball", "_rev":%q}`, alphaRevID)
	assertStatus(t, rt.Send(request("PUT", "/db/testOldDocId", str)), 201)

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
	var rt RestTester
	defer rt.Close()

	var logKeys = map[string]bool{
		"Changes+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	a := auth.NewAuthenticator(rt.Bucket(), nil)
	var changes struct {
		Results []db.ChangeEntry
	}
	guest, err := a.GetUser("")
	assert.Equals(t, err, nil)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.Equals(t, err, nil)

	assertStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":["books"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["gifts"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["!"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["gifts"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":["!"]}`), 201)
	// document added to "*" channel should only end up available to users with * access
	assertStatus(t, rt.SendRequest("PUT", "/db/doc6", `{"channels":["*"]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.Equals(t, err, nil)
	//
	// Part 1 - Tests for user with single channel access:
	//
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("books"))
	a.Save(bernard)

	// GET /db/docid - basic test for channel user has
	response := rt.Send(requestByUser("GET", "/db/doc1", "", "bernard"))
	assertStatus(t, response, 200)

	// GET /db/docid - negative test for channel user doesn't have
	response = rt.Send(requestByUser("GET", "/db/doc2", "", "bernard"))
	assertStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(requestByUser("GET", "/db/doc3", "", "bernard"))
	assertStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns the docs the user has access to:
	response = rt.Send(requestByUser("GET", "/db/_all_docs?channels=true", "", "bernard"))
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
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)
	since := changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc1")
	assert.Equals(t, since.Seq, uint64(1))

	// GET /db/_changes for single channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=books", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 1)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc1")
	assert.Equals(t, since.Seq, uint64(1))

	// GET /db/_changes for ! channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since.Seq, uint64(3))

	// GET /db/_changes for unauthorized channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=gifts", "", "bernard"))
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
	response = rt.Send(requestByUser("GET", "/db/doc1", "", "fran"))
	assertStatus(t, response, 200)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(requestByUser("GET", "/db/doc3", "", "fran"))
	assertStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns all docs (based on user * channel)
	response = rt.Send(requestByUser("GET", "/db/_all_docs?channels=true", "", "fran"))
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 6)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc1")
	assert.DeepEquals(t, allDocsResult.Rows[0].Value.Channels, []string{"books"})

	// GET /db/_changes
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 6)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc1")
	assert.Equals(t, since.Seq, uint64(1))

	// GET /db/_changes for ! channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since.Seq, uint64(3))

	//
	// Part 3 - Tests for user with no user channel access
	//
	// Create a user:
	manny, err := a.NewUser("manny", "letmein", nil)
	a.Save(manny)

	// GET /db/docid - basic test for doc that has channel
	response = rt.Send(requestByUser("GET", "/db/doc1", "", "manny"))
	assertStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(requestByUser("GET", "/db/doc3", "", "manny"))
	assertStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs only returns ! docs (based on doc ! channel)
	response = rt.Send(requestByUser("GET", "/db/_all_docs?channels=true", "", "manny"))
	assertStatus(t, response, 200)
	log.Printf("Response = %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &allDocsResult)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(allDocsResult.Rows), 2)
	assert.Equals(t, allDocsResult.Rows[0].ID, "doc3")

	// GET /db/_changes
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since.Seq, uint64(3))

	// GET /db/_changes for ! channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	since = changes.Results[0].Seq
	assert.Equals(t, changes.Results[0].ID, "doc3")
	assert.Equals(t, since.Seq, uint64(3))
}

// Test for issue #562
func TestCreateTarget(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	//Attempt to create existing target DB on public API
	response := rt.SendRequest("PUT", "/db/", "")
	assertStatus(t, response, 412)
	//Attempt to create new target DB on public API
	response = rt.SendRequest("PUT", "/foo/", "")
	assertStatus(t, response, 403)
}

// Test for issue 758 - basic auth with stale session cookie
func TestBasicAuthWithSessionCookie(t *testing.T) {

	var rt RestTester
	defer rt.Close()

	// Create two users
	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["bernard"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/_user/manny", `{"name":"manny", "password":"letmein","admin_channels":["manny"]}`)
	assertStatus(t, response, 201)

	// Create a session for the first user
	response = rt.Send(requestByUser("POST", "/db/_session", `{"name":"bernard", "password":"letmein"}`, "bernard"))
	log.Println("response.Header()", response.Header())
	assert.True(t, response.Header().Get("Set-Cookie") != "")

	cookie := response.Header().Get("Set-Cookie")

	// Create a doc as the first user, with session auth, channel-restricted to first user
	reqHeaders := map[string]string{
		"Cookie": cookie,
	}
	response = rt.SendRequestWithHeaders("PUT", "/db/bernardDoc", `{"hi": "there", "channels":["bernard"]}`, reqHeaders)
	assertStatus(t, response, 201)
	response = rt.SendRequestWithHeaders("GET", "/db/bernardDoc", "", reqHeaders)
	assertStatus(t, response, 200)

	// Create a doc as the second user, with basic auth, channel-restricted to the second user
	response = rt.Send(requestByUser("PUT", "/db/mannyDoc", `{"hi": "there", "channels":["manny"]}`, "manny"))
	assertStatus(t, response, 201)
	response = rt.Send(requestByUser("GET", "/db/mannyDoc", "", "manny"))
	assertStatus(t, response, 200)
	response = rt.Send(requestByUser("GET", "/db/bernardDoc", "", "manny"))
	assertStatus(t, response, 403)

	// Attempt to retrieve the docs with the first user's cookie, second user's basic auth credentials.  Basic Auth should take precedence
	response = rt.SendUserRequestWithHeaders("GET", "/db/bernardDoc", "", reqHeaders, "manny", "letmein")
	assertStatus(t, response, 403)
	response = rt.SendUserRequestWithHeaders("GET", "/db/mannyDoc", "", reqHeaders, "manny", "letmein")
	assertStatus(t, response, 200)

}

func TestEventConfigValidationSuccess(t *testing.T) {

	sc := NewServerContext(&ServerConfig{})

	// Valid config
	configJSON := `{"name": "default",
        			"server": "walrus:",
        			"bucket": "default",
			        "event_handlers": {
			          "max_processes" : 1000,
			          "wait_for_process" : "15",
			          "document_changed": [
			            {"handler": "webhook",
			             "url": "http://localhost:8081/filtered",
			             "timeout": 0,
			             "filter": "function(doc){ return true }"
			            }
			          ]
			        }
      			   }`

	var dbConfig DbConfig
	err := json.Unmarshal([]byte(configJSON), &dbConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(&dbConfig)
	assert.True(t, err == nil)

	sc.Close()

}
func TestEventConfigValidationInvalid(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works under walrus")
	}

	sc := NewServerContext(&ServerConfig{})
	defer sc.Close()

	configJSON := `{"name": "invalid",
        			"server": "walrus:",
        			"bucket": "invalid",
			        "event_handlers": {
			          "max_processes" : 1000,
			          "wait_for_process" : "15",
			          "document_scribbled_on": [
			            {"handler": "webhook",
			             "url": "http://localhost:8081/filtered",
			             "timeout": 0,
			             "filter": "function(doc){ return true }"
			            }
			          ]
			        }
      			   }`

	var dbConfig DbConfig
	err := json.Unmarshal([]byte(configJSON), &dbConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(&dbConfig)
	assert.True(t, err != nil)
	assert.True(t, fmt.Sprintf("%v", err) == "Unsupported event property 'document_scribbled_on' defined for db invalid")

}

// Reproduces https://github.com/couchbase/sync_gateway/issues/2427
// NOTE: to repro, you must run with -race flag
func TestBulkGetRevPruning(t *testing.T) {

	var rt RestTester
	defer rt.Close()

	var body db.Body

	// The number of goroutines that are reading the doc via the _bulk_get endpoint
	// which causes the pruning on the map -- all goroutines end up modifying the
	// same map reference concurrently and causing the data race
	numPruningBulkGetGoroutines := 10

	// Each _bulk_get reader goroutine should only try this many times
	maxIterationsPerBulkGetGoroutine := 200

	// Do a write
	response := rt.SendRequest("PUT", "/db/doc1", `{"channels":[]}`)
	assertStatus(t, response, 201)
	json.Unmarshal(response.Body.Bytes(), &body)
	revId := body["rev"]

	// Update 10 times
	for i := 0; i < 20; i++ {
		str := fmt.Sprintf(`{"_rev":%q}`, revId)
		response = rt.Send(request("PUT", "/db/doc1", str))
		json.Unmarshal(response.Body.Bytes(), &body)
		revId = body["rev"]
	}

	// Get latest rev id
	response = rt.SendRequest("GET", "/db/doc1", "")
	json.Unmarshal(response.Body.Bytes(), &body)
	revId = body["_rev"]

	// Spin up several goroutines to all try to do a _bulk_get on the latest revision.
	// Since they will be pruning the same shared rev history, it will cause a data race
	wg := sync.WaitGroup{}
	for i := 0; i < numPruningBulkGetGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for j := 0; j < maxIterationsPerBulkGetGoroutine; j++ {
				bulkGetDocs := fmt.Sprintf(`{"docs": [{"id": "doc1", "rev": "%v"}]}`, revId)
				bulkGetResponse := rt.SendRequest("POST", "/db/_bulk_get?revs=true&revs_limit=2", bulkGetDocs)
				if bulkGetResponse.Code != 200 {
					panic(fmt.Sprintf("Got unexpected response: %v", bulkGetResponse))
				}
			}

		}()

	}

	// Wait until all pruning bulk get goroutines finish
	wg.Wait()

}

// Reproduces panic seen in https://github.com/couchbase/sync_gateway/issues/2528
func TestBulkGetBadAttachmentReproIssue2528(t *testing.T) {

	var rt RestTester
	defer rt.Close()

	var body db.Body

	// Disable rev cache so that the _bulk_get request is forced to go back to the bucket to load the doc
	// rather than loading it from the (stale) rev cache.  The rev cache will be stale since the test
	// short-circuits Sync Gateway and directly updates the bucket.
	db.KDefaultRevisionCacheCapacity = 0

	docIdDoc1 := "doc"
	attachmentName := "attach1"

	// Add a doc
	resource := fmt.Sprintf("/db/%v", docIdDoc1)
	response := rt.SendRequest("PUT", resource, `{"prop":true}`)
	assertStatus(t, response, 201)
	json.Unmarshal(response.Body.Bytes(), &body)
	revidDoc1 := body["rev"].(string)

	// Add another doc
	docIdDoc2 := "doc2"
	responseDoc2 := rt.SendRequest("PUT", fmt.Sprintf("/db/%v", docIdDoc2), `{"prop":true}`)
	assertStatus(t, responseDoc2, 201)
	revidDoc2 := body["rev"].(string)

	// attach to existing document with correct rev (should succeed)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response = rt.SendRequestWithHeaders(
		"PUT",
		fmt.Sprintf("%v/%v?rev=%v", resource, attachmentName, revidDoc1),
		attachmentBody,
		reqHeaders,
	)
	assertStatus(t, response, 201)

	// Get the couchbase doc
	couchbaseDoc := db.Body{}
	bucket := rt.Bucket()
	_, err := bucket.Get(docIdDoc1, &couchbaseDoc)
	assertNoError(t, err, "Error getting couchbaseDoc")
	log.Printf("couchbase doc: %+v", couchbaseDoc)

	// Doc at this point
	/*
		{
		  "_attachments": {
			"attach1": {
			  "content_type": "content/type",
			  "digest": "sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=",
			  "length": 30,
			  "revpos": 2,
			  "stub": true
			}
		  },
		  "prop": true
		}
	*/

	// Modify the doc directly in the bucket to delete the digest field
	attachments := db.BodyAttachments(couchbaseDoc)
	attach1 := attachments[attachmentName].(map[string]interface{})
	delete(attach1, "digest")
	delete(attach1, "content_type")
	delete(attach1, "length")
	attachments[attachmentName] = attach1
	log.Printf("couchbase doc after: %+v", couchbaseDoc)

	// Doc at this point
	/*
		{
		  "_attachments": {
			"attach1": {
			  "revpos": 2,
			  "stub": true
			}
		  },
		  "prop": true
		}
	*/

	// Put the doc back into couchbase
	err = bucket.Set(docIdDoc1, 0, couchbaseDoc)
	assertNoError(t, err, "Error putting couchbaseDoc")

	// Get latest rev id
	response = rt.SendRequest("GET", resource, "")
	json.Unmarshal(response.Body.Bytes(), &body)
	revId := body["_rev"]

	// Do a bulk_get to get the doc -- this was causing a panic prior to the fix for #2528
	bulkGetDocs := fmt.Sprintf(`{"docs": [{"id": "%v", "rev": "%v"}, {"id": "%v", "rev": "%v"}]}`, docIdDoc1, revId, docIdDoc2, revidDoc2)
	bulkGetResponse := rt.SendRequest("POST", "/db/_bulk_get?revs=true&attachments=true&revs_limit=2", bulkGetDocs)
	if bulkGetResponse.Code != 200 {
		panic(fmt.Sprintf("Got unexpected response: %v", bulkGetResponse))
	}

	bulkGetResponse.DumpBody()

	// Parse multipart/mixed docs and create reader
	contentType, attrs, _ := mime.ParseMediaType(bulkGetResponse.Header().Get("Content-Type"))
	log.Printf("content-type: %v.  attrs: %v", contentType, attrs)
	assert.Equals(t, contentType, "multipart/mixed")
	reader := multipart.NewReader(bulkGetResponse.Body, attrs["boundary"])

	// Make sure we see both docs
	sawDoc1 := false
	sawDoc2 := false

	// Iterate over multipart parts and make assertions on each part
	// Should get the following docs in their own parts:
	/*
		{
		   "error":"500",
		   "id":"doc",
		   "reason":"Internal error: Unable to load attachment for doc: doc with name: attach1 and revpos: 2 due to missing digest field",
		   "rev":"2-d501cf345b2e906547fe27dbbedf825b",
		   "status":500
		}

			and:

		{
		   "_id":"doc2",
		   "_rev":"1-45ca73d819d5b1c9b8eea95290e79004",
		   "_revisions":{
			  "ids":[
				 "45ca73d819d5b1c9b8eea95290e79004"
			  ],
			  "start":1
		   },
		   "prop":true
		}
	*/
	for {

		// Get the next part.  Break out of the loop if we hit EOF
		part, err := reader.NextPart()
		if err != nil {
			if err == io.EOF {
				break
			}

		}

		partBytes, err := ioutil.ReadAll(part)
		assertNoError(t, err, "Unexpected error")

		log.Printf("multipart part: %+v", string(partBytes))

		partJson := map[string]interface{}{}
		err = json.Unmarshal(partBytes, &partJson)
		assertNoError(t, err, "Unexpected error")

		// Assert expectations for the doc with attachment errors
		rawId, ok := partJson["id"]
		if ok {
			// expect an error
			_, hasErr := partJson["error"]
			assertTrue(t, hasErr, "Expected error field for this doc")
			assert.Equals(t, docIdDoc1, rawId)
			sawDoc1 = true

		}

		// Assert expectations for the doc with no attachment errors
		rawId, ok = partJson["_id"]
		if ok {
			_, hasErr := partJson["error"]
			assertTrue(t, !hasErr, "Did not expect error field for this doc")
			assert.Equals(t, docIdDoc2, rawId)
			sawDoc2 = true
		}

	}

	assertTrue(t, sawDoc2, "Did not see doc 2")
	assertTrue(t, sawDoc1, "Did not see doc 1")

}

// TestDocExpiry validates the value of the expiry as set in the document.  It doesn't validate actual expiration (not supported
// in walrus).
func TestDocExpiry(t *testing.T) {
	var rt RestTester
	defer rt.Close()

	var body db.Body
	response := rt.SendRequest("PUT", "/db/expNumericTTL", `{"_exp":100}`)
	assertStatus(t, response, 201)

	// Validate that exp isn't returned on the standard GET, bulk get
	response = rt.SendRequest("GET", "/db/expNumericTTL", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	_, ok := body["_exp"]
	assert.Equals(t, ok, false)

	bulkGetDocs := `{"docs": [{"id": "expNumericTTL", "rev": "1-ca9ad22802b66f662ff171f226211d5c"}]}`
	response = rt.SendRequest("POST", "/db/_bulk_get", bulkGetDocs)
	assertStatus(t, response, 200)
	responseString := string(response.Body.Bytes())
	assertTrue(t, !strings.Contains(responseString, "_exp"), "Bulk get response contains _exp property when show_exp not set.")

	response = rt.SendRequest("POST", "/db/_bulk_get?show_exp=true", bulkGetDocs)
	assertStatus(t, response, 200)
	responseString = string(response.Body.Bytes())
	assertTrue(t, strings.Contains(responseString, "_exp"), "Bulk get response doesn't contain _exp property when show_exp was set.")

	body = nil
	response = rt.SendRequest("GET", "/db/expNumericTTL?show_exp=true", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	_, ok = body["_exp"]
	assert.Equals(t, ok, true)

	// Validate other exp formats
	body = nil
	response = rt.SendRequest("PUT", "/db/expNumericUnix", `{"val":1, "_exp":4260211200}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/expNumericUnix?show_exp=true", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	log.Printf("numeric unix response: %s", response.Body.Bytes())
	_, ok = body["_exp"]
	assert.Equals(t, ok, true)

	body = nil
	response = rt.SendRequest("PUT", "/db/expNumericString", `{"val":1, "_exp":"100"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/expNumericString?show_exp=true", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	_, ok = body["_exp"]
	assert.Equals(t, ok, true)

	body = nil
	response = rt.SendRequest("PUT", "/db/expBadString", `{"_exp":"abc"}`)
	assertStatus(t, response, 400)
	response = rt.SendRequest("GET", "/db/expBadString?show_exp=true", "")
	assertStatus(t, response, 404)
	json.Unmarshal(response.Body.Bytes(), &body)
	_, ok = body["_exp"]
	assert.Equals(t, ok, false)

	body = nil
	response = rt.SendRequest("PUT", "/db/expDateString", `{"_exp":"2105-01-01T00:00:00.000+00:00"}`)
	assertStatus(t, response, 201)
	response = rt.SendRequest("GET", "/db/expDateString?show_exp=true", "")
	assertStatus(t, response, 200)
	json.Unmarshal(response.Body.Bytes(), &body)
	_, ok = body["_exp"]
	assert.Equals(t, ok, true)

	body = nil
	response = rt.SendRequest("PUT", "/db/expBadDateString", `{"_exp":"2105-0321-01T00:00:00.000+00:00"}`)
	assertStatus(t, response, 400)
	response = rt.SendRequest("GET", "/db/expBadDateString?show_exp=true", "")
	assertStatus(t, response, 404)
	json.Unmarshal(response.Body.Bytes(), &body)
	_, ok = body["_exp"]
	assert.Equals(t, ok, false)

}

// Reproduces https://github.com/couchbase/sync_gateway/issues/916.  The test-only RestartListener operation used to simulate a
// SG restart isn't race-safe, so disabling the test for now.  Should be possible to reinstate this as a proper unit test
// once we add the ability to take a bucket offline/online.
func DisabledTestLongpollWithWildcard(t *testing.T) {

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel);}`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	response := rt.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true}`)

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Issue is only reproducible when the wait counter is zero for all requested channels (including the user channel) - the count=0
	// triggers early termination of the changes loop.  This can only be reproduced if the feed is restarted after the user is created -
	// otherwise the count for the user pseudo-channel will always be non-zero
	db, _ := rt.ServerContext().GetDatabase("db")
	err = db.RestartListener()
	assert.True(t, err == nil)
	// Put a document to increment the counter for the * channel
	response = rt.Send(request("PUT", "/db/lost", `{"channel":["ABC"]}`))
	assertStatus(t, response, 201)

	// Previous bug: changeWaiter was treating the implicit '*' wildcard in the _changes request as the '*' channel, so the wait counter
	// was being initialized to 1 (the previous PUT).  Later the wildcard was resolved to actual channels (PBS, _sync:user:bernard), which
	// has a wait counter of zero (no documents writted since the listener was restarted).
	wg := sync.WaitGroup{}
	// start changes request
	go func() {
		wg.Add(1)
		defer wg.Done()
		changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
		changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
		log.Printf("_changes looks like: %s", changesResponse.Body.Bytes())
		err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
		// Checkthat the changes loop isn't returning an empty result immediately (the previous bug) - should
		// be waiting until entry 'sherlock', created below, appears.
		assert.True(t, len(changes.Results) > 0)
	}()

	// Send a doc that will properly close the longpoll response
	time.Sleep(1 * time.Second)
	response = rt.Send(request("PUT", "/db/sherlock", `{"channel":["PBS"]}`))
	wg.Wait()
}

func TestUnsupportedConfig(t *testing.T) {

	sc := NewServerContext(&ServerConfig{})
	testProviderOnlyJSON := `{"name": "test_provider_only",
        			"server": "walrus:",
        			"bucket": "test_provider_only",
			        "unsupported": {
			          "oidc_test_provider": {
			            "enabled":true,
			            "unsigned_id_token":true
			          }
			        }
      			   }`

	var testProviderOnlyConfig DbConfig
	err := json.Unmarshal([]byte(testProviderOnlyJSON), &testProviderOnlyConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(&testProviderOnlyConfig)
	assertNoError(t, err, "Error adding testProviderOnly database.")

	viewsOnlyJSON := `{"name": "views_only",
        			"server": "walrus:",
        			"bucket": "views_only",
			        "unsupported": {
			          "user_views": {
			            "enabled":true
			          }
			        }
      			   }`

	var viewsOnlyConfig DbConfig
	err = json.Unmarshal([]byte(viewsOnlyJSON), &viewsOnlyConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(&viewsOnlyConfig)
	assertNoError(t, err, "Error adding viewsOnlyConfig database.")

	viewsAndTestJSON := `{"name": "views_and_test",
        			"server": "walrus:",
        			"bucket": "views_and_test",
			        "unsupported": {
			          "oidc_test_provider": {
			            "enabled":true,
			            "unsigned_id_token":true
			          },
			          "user_views": {
			            "enabled":true
			          }
			        }
      			   }`

	var viewsAndTestConfig DbConfig
	err = json.Unmarshal([]byte(viewsAndTestJSON), &viewsAndTestConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(&viewsAndTestConfig)
	assertNoError(t, err, "Error adding viewsAndTestConfig database.")

	sc.Close()
}

var prt RestTester

func Benchmark_RestApiGetDocPerformance(b *testing.B) {

	//Create test document
	prt.SendRequest("PUT", "/db/doc", `{"prop":true}`)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		//GET the document until test run has completed
		for pb.Next() {
			prt.SendRequest("GET", "/db/doc", "")
		}
	})
}

var threekdoc = `{"cols":["Name","Address","Location","phone"],"data":[["MelyssaF.Stokes","Ap#166-9804ProinSt.","52.01352,-9.4151","(306)773-3853"],["RuthT.Richards","Ap#180-8417TemporRoad","8.07909,-118.55952","(662)733-8043"],["CedricN.Witt","Ap#575-4625NuncSt.","74.419,153.71285","(850)995-0417"],["ElianaF.Ashley","Ap#169-2030Nibh.St.","87.98632,97.47442","(903)272-5949"],["ChesterJ.Holland","2905ProinSt.","-43.14706,-64.25893","(911)435-9200"],["AleaT.Bishop","Ap#493-4894ConvallisRd.","42.54157,64.98534","(479)848-2988"],["HerrodT.Barron","Ap#822-1444EtAvenue","9.50706,-111.54064","(390)300-8534"],["YoshiP.Sanchez","Ap#796-4679Arcu.Avenue","-16.49557,-137.69","(913)606-8930"],["GrahamO.Velazquez","415EratRd.","-5.30634,171.81751","(691)700-3072"],["BryarF.Sargent","Ap#180-6507Lacus.St.","17.64959,-19.93008","(516)890-6469"],["XerxesM.Gaines","370-1967NislStreet","-39.28978,-23.74924","(461)907-9563"],["KayI.Jones","565-351ElitAve","25.58317,17.43545","(145)441-5007"],["ImaZ.Curry","Ap#143-8377MagnaAve","-86.72025,-161.94081","(484)924-8145"],["GiselleW.Macdonald","962AdipiscingRoad","-21.55826,-121.06657","(137)255-2241"],["TarikJ.Kane","P.O.Box447,5949PhasellusSt.","57.28914,-125.89595","(356)758-8271"],["ChristopherJ.Travis","5246InRd.","-69.12682,31.20181","(298)963-1855"],["QuinnT.Pace","P.O.Box935,212Laoreet,St.","-62.00241,1.31111","(157)419-0182"],["BrentK.Guy","156-417LoremSt.","26.67571,-29.35786","(125)687-6610"],["JocelynN.Cash","Ap#502-9209VehiculaSt.","-26.05925,160.61357","(782)351-4211"],["DaphneS.King","571-1485FringillaRoad","-76.33262,-142.5655","(356)476-4508"],["MicahJ.Eaton","3468ProinRd.","61.30187,-128.8584","(942)467-7558"],["ChaneyM.Gay","444-1647Pede.Rd.","84.32739,-43.59781","(386)231-0872"],["LacotaM.Guerra","9863NuncRoad","21.81253,-54.90423","(694)443-8520"],["KimberleyY.Jensen","6403PurusSt.","67.5704,65.90554","(181)309-7780"],["JenaY.Brennan","Ap#533-7088MalesuadaStreet","78.58624,-172.89351","(886)688-0617"],["CarterK.Dotson","Ap#828-1931IpsumAve","59.54845,53.30366","(203)546-8704"],["EllaU.Buckner","Ap#141-1401CrasSt.","78.34425,-172.24474","(214)243-6054"],["HamiltonE.Estrada","8676Iaculis,St.","11.67468,-130.12233","(913)624-2612"],["IanT.Saunders","P.O.Box519,3762DictumRd.","-10.97019,73.47059","(536)391-7018"],["CairoK.Craft","6619Sem.St.","9.28931,-5.69682","(476)804-7898"],["JohnB.Norman","Ap#865-7121CubiliaAve","50.96552,-126.5271","(309)323-6975"],["SawyerD.Hale","Ap#512-820EratRd.","-65.1931,166.14822","(180)527-8987"],["CiaranQ.Cole","P.O.Box262,9220SedAvenue","69.753,121.39921","(272)654-8755"],["BrandenJ.Thompson","Ap#846-5470MetusAv.","44.61386,-44.18375","(388)776-0689"]]}`

func Benchmark_RestApiPutDocPerformanceDefaultSyncFunc(b *testing.B) {

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		//PUT a new document until test run has completed
		for pb.Next() {
			prt.SendRequest("PUT", fmt.Sprintf("/db/doc-%v", base.CreateUUID()), threekdoc)
		}
	})
}


func Benchmark_RestApiPutDocPerformanceExplicitSyncFunc(b *testing.B) {

	qrt := RestTester{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	defer qrt.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		//PUT a new document until test run has completed
		for pb.Next() {
			qrt.SendRequest("PUT", fmt.Sprintf("/db/doc-%v", base.CreateUUID()), threekdoc)
		}
	})
}

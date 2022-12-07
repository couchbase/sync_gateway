// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package rest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/walrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//from api_test.go

// Validate that Etag header value is surrounded with double quotes, see issue #808
func TestDocEtag(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/doc", `{"prop":true}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revid := body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}

	// Validate Etag returned on doc creation
	assert.Equal(t, strconv.Quote(revid), response.Header().Get("Etag"))

	response = rt.SendRequest("GET", "/db/doc", "")
	RequireStatus(t, response, 200)

	// Validate Etag returned when retrieving doc
	assert.Equal(t, strconv.Quote(revid), response.Header().Get("Etag"))

	// Validate Etag returned when updating doc
	response = rt.SendRequest("PUT", "/db/doc?rev="+revid, `{"prop":false}`)
	revid = body["rev"].(string)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revid = body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}

	assert.Equal(t, strconv.Quote(revid), response.Header().Get("Etag"))

	// Test Attachments
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != revid)

	// validate Etag returned from adding an attachment
	assert.Equal(t, strconv.Quote(revIdAfterAttachment), response.Header().Get("Etag"))

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc/attach1", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.Equal(t, "", response.Header().Get("Content-Disposition"))
	assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))

	// Validate Etag returned from retrieving an attachment
	assert.Equal(t, "\"sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=\"", response.Header().Get("Etag"))

}

// Add and retrieve an attachment, including a subrange
func TestDocAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/doc", `{"prop":true}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid = body["rev"].(string)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc/attach1", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.Equal(t, "bytes", response.Header().Get("Accept-Ranges"))
	assert.Equal(t, "", response.Header().Get("Content-Disposition"))
	assert.Equal(t, "30", response.Header().Get("Content-Length"))
	assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))

	// retrieve subrange
	response = rt.SendRequestWithHeaders("GET", "/db/doc/attach1", "", map[string]string{"Range": "bytes=5-6"})
	RequireStatus(t, response, 206)
	assert.Equal(t, "is", string(response.Body.Bytes()))
	assert.Equal(t, "bytes", response.Header().Get("Accept-Ranges"))
	assert.Equal(t, "2", response.Header().Get("Content-Length"))
	assert.Equal(t, "bytes 5-6/30", response.Header().Get("Content-Range"))
	assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))

	// attempt to delete an attachment that is not on the document
	response = rt.SendRequest("DELETE", "/db/doc/attach2?rev="+revid, "")
	RequireStatus(t, response, 404)

	// attempt to delete attachment from non existing doc
	response = rt.SendRequest("DELETE", "/db/doc1/attach1?rev=1-xzy", "")
	RequireStatus(t, response, 404)

	// attempt to delete attachment using incorrect revid
	response = rt.SendRequest("DELETE", "/db/doc/attach1?rev=1-xzy", "")
	RequireStatus(t, response, 409)

	// delete the attachment calling the delete attachment endpoint
	response = rt.SendRequest("DELETE", "/db/doc/attach1?rev="+revid, "")
	RequireStatus(t, response, 200)

	// attempt to access deleted attachment (should return error)
	response = rt.SendRequest("GET", "/db/doc/attach1", "")
	RequireStatus(t, response, 404)
}

func TestDocAttachmentMetaOption(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest(http.MethodPut, "/db/doc", `{"prop":true}`)
	RequireStatus(t, response, http.StatusCreated)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// Validate attachment response.
	assertAttachmentResponse := func(response *TestResponse) {
		RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
		assert.Equal(t, "bytes", response.Header().Get("Accept-Ranges"))
		assert.Empty(t, response.Header().Get("Content-Disposition"))
		assert.Equal(t, "30", response.Header().Get("Content-Length"))
		assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))
	}

	// Attach to existing document.
	response = rt.SendRequestWithHeaders(http.MethodPut, "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/doc/attach1", "")
	assertAttachmentResponse(response)

	// Retrieve attachment meta only by explicitly enabling meta option.
	response = rt.SendRequest(http.MethodGet, "/db/doc/attach1?meta=true", "")
	RequireStatus(t, response, http.StatusOK)

	responseBody := make(map[string]interface{})
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	require.NoError(t, err)

	contentType, contentTypeOK := responseBody["content_type"].(string)
	require.True(t, contentTypeOK)
	assert.Equal(t, attachmentContentType, contentType)

	digest, digestOK := responseBody["digest"].(string)
	require.True(t, digestOK)
	assert.Equal(t, "sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=", digest)

	key, keyOK := responseBody["key"].(string)
	require.True(t, keyOK)
	assert.Equal(t, "_sync:att2:E51US4IbE+vqFPGw/hhXciLkFcKWbjo1EcQZYFUjIgI=:sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=", key)

	length, lengthOK := responseBody["length"].(float64)
	require.True(t, lengthOK)
	assert.Equal(t, float64(30), length)

	revpos, revposOK := responseBody["revpos"].(float64)
	require.True(t, revposOK)
	assert.Equal(t, float64(2), revpos)

	version, versionOK := responseBody["ver"].(float64)
	require.True(t, versionOK)
	assert.Equal(t, float64(2), version)

	stub, stubOK := responseBody["stub"].(bool)
	require.True(t, stubOK)
	require.True(t, stub)

	// Retrieve attachment by explicitly disabling meta option.
	response = rt.SendRequest(http.MethodGet, "/db/doc/attach1?meta=false", "")
	assertAttachmentResponse(response)
}

// Add an attachment to a document that has been removed from the users channels
func TestDocAttachmentOnRemovedRev(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", channels.BaseSetOf(t, "foo"))
	assert.NoError(t, a.Save(user))

	response := rt.Send(RequestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user1"))
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	// Put new revision removing document from users channel set
	response = rt.Send(RequestByUser("PUT", "/db/doc?rev="+revid, `{"prop":true}`, "user1"))
	RequireStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid = body["rev"].(string)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should fail)
	response = rt.SendUserRequestWithHeaders("PUT", "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders, "user1", "letmein")
	RequireStatus(t, response, 404)
}

func TestFunkyDocAndAttachmentIDs(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// requireRevID asserts that the response body contains the revision ID.
	requireRevID := func(response *TestResponse) (revID string) {
		var body db.Body
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		require.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		require.NotEmpty(t, revID)
		return revID
	}

	// createDoc creates a document revision and returns the revision ID.
	createDoc := func(docID string) (revID string) {
		response := rt.SendRequest(http.MethodPut, "/db/"+docID, `{"prop":true}`)
		RequireStatus(t, response, http.StatusCreated)
		return requireRevID(response)
	}

	// assertResponse asserts that the specified attachment exists in the response body.
	assertResponse := func(response *TestResponse, attachmentBody string) {
		RequireStatus(t, response, http.StatusOK)
		require.Equal(t, attachmentBody, string(response.Body.Bytes()))
		require.Empty(t, response.Header().Get("Content-Disposition"))
		require.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))
	}

	// Create document with simple name
	doc1revId := rt.CreateDoc(t, "doc1")

	// Add attachment with single embedded '/' (%2F HEX)
	resource := "/db/doc1/attachpath%2Fattachment.txt?rev=" + doc1revId
	response := rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	revIdAfterAttachment := requireRevID(response)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/doc1/attachpath%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Add attachment with two embedded '/' (%2F HEX)
	resource = "/db/doc1/attachpath%2Fattachpath2%2Fattachment.txt?rev=" + revIdAfterAttachment
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/doc1/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Create Doc with embedded '/' (%2F HEX) in name
	doc1revId = createDoc("AC%2FDC")

	response = rt.SendRequest(http.MethodGet, "/db/AC%2FDC", "")
	RequireStatus(t, response, http.StatusOK)

	// Add attachment with single embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders(http.MethodPut, "/db/AC%2FDC/attachpath%2Fattachment.txt?rev="+doc1revId,
		attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	revIdAfterAttachment = requireRevID(response)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2FDC/attachpath%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Add attachment with two embedded '/' (%2F HEX)
	resource = "/db/AC%2FDC/attachpath%2Fattachpath2%2Fattachment.txt?rev=" + revIdAfterAttachment
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2FDC/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Create Doc with embedded '+' (%2B HEX) in name
	doc1revId = createDoc("AC%2BDC%2BGC2")
	response = rt.SendRequest(http.MethodGet, "/db/AC%2BDC%2BGC2", "")
	RequireStatus(t, response, http.StatusOK)

	// Add attachment with single embedded '/' (%2F HEX)
	resource = "/db/AC%2BDC%2BGC2/attachpath%2Fattachment.txt?rev=" + doc1revId
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	revIdAfterAttachment = requireRevID(response)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2BDC%2BGC2/attachpath%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Add attachment with two embedded '/' (%2F HEX)
	resource = "/db/AC%2BDC%2BGC2/attachpath%2Fattachpath2%2Fattachment.txt?rev=" + revIdAfterAttachment
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2BDC%2BGC2/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)
}

func TestManualAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	doc1revId := rt.CreateDoc(t, "doc1")

	// attach to existing document without rev (should fail)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response := rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	RequireStatus(t, response, 409)

	// attach to existing document with wrong rev (should fail)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev=1-xyz", attachmentBody, reqHeaders)
	RequireStatus(t, response, 409)

	// attach to existing document with wrong rev using If-Match header (should fail)
	reqHeaders["If-Match"] = `"` + "1-dnf" + `"`
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	RequireStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to existing document with correct rev (should succeed)
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != doc1revId)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc1/attach1", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// retrieve attachment as admin should have
	// Content-disposition: attachment
	response = rt.SendAdminRequest("GET", "/db/doc1/attach1", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Disposition") == `attachment`)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// try to overwrite that attachment
	attachmentBody = "updated content"
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)
	body = db.Body{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterUpdateAttachment := body["rev"].(string)
	if revIdAfterUpdateAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterUpdateAttachment != revIdAfterAttachment)

	// try to overwrite that attachment again, this time using If-Match header
	attachmentBody = "updated content again"
	reqHeaders["If-Match"] = `"` + revIdAfterUpdateAttachment + `"`
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)
	body = db.Body{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterUpdateAttachmentAgain := body["rev"].(string)
	if revIdAfterUpdateAttachmentAgain == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterUpdateAttachmentAgain != revIdAfterUpdateAttachment)
	delete(reqHeaders, "If-Match")

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc1/attach1", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add another attachment to the document
	// also no explicit Content-Type header on this one
	// should default to application/octet-stream
	attachmentBody = "separate content"
	response = rt.SendRequest("PUT", "/db/doc1/attach2?rev="+revIdAfterUpdateAttachmentAgain, attachmentBody)
	RequireStatus(t, response, 201)
	body = db.Body{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterSecondAttachment := body["rev"].(string)
	if revIdAfterSecondAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterSecondAttachment != revIdAfterUpdateAttachment)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc1/attach2", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Type") == "application/octet-stream")

	// now check the attachments index on the document
	response = rt.SendRequest("GET", "/db/doc1", "")
	RequireStatus(t, response, 200)
	body = db.Body{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	bodyAttachments, ok := body["_attachments"].(map[string]interface{})
	if !ok {
		t.Errorf("Attachments must be map")
	} else {
		assert.Equal(t, 2, len(bodyAttachments))
	}
	// make sure original document property has remained
	prop, ok := body["prop"]
	if !ok || !prop.(bool) {
		t.Errorf("property prop is now missing or modified")
	}
}

// PUT attachment on non-existant docid should create empty doc
func TestManualAttachmentNewDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// attach to new document using bogus rev (should fail)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "text/plain"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response := rt.SendAdminRequestWithHeaders("PUT", "/db/notexistyet/attach1?rev=1-abc", attachmentBody, reqHeaders)
	RequireStatus(t, response, 409)

	// attach to new document using bogus rev using If-Match header (should fail)
	reqHeaders["If-Match"] = `"1-xyz"`
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	RequireStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to new document without any rev (should succeed)
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}

	// retrieve attachment
	response = rt.SendAdminRequest("GET", "/db/notexistyet/attach1", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// now check the document
	body = db.Body{}
	response = rt.SendAdminRequest("GET", "/db/notexistyet", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	// body should only have 3 top-level entries _id, _rev, _attachments
	assert.True(t, len(body) == 3)
}

// Test for regression of issue #447
func TestAttachmentsNoCrossTalk(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	doc1revId := rt.CreateDoc(t, "doc1")

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response := rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != doc1revId)

	reqHeaders = map[string]string{
		"Accept": "application/json",
	}

	log.Printf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, doc1revId)
	response = rt.SendAdminRequestWithHeaders("GET", fmt.Sprintf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, doc1revId), "", reqHeaders)
	assert.Equal(t, 200, response.Code)
	// validate attachment has data property
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("response body revid1 = %s", body)
	attachments := body["_attachments"].(map[string]interface{})
	attach1 := attachments["attach1"].(map[string]interface{})
	data := attach1["data"]
	assert.True(t, data != nil)

	log.Printf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, revIdAfterAttachment)
	response = rt.SendAdminRequestWithHeaders("GET", fmt.Sprintf("/db/doc1?rev=%s&revs=true&attachments=true&atts_since=[\"%s\"]", revIdAfterAttachment, revIdAfterAttachment), "", reqHeaders)
	assert.Equal(t, 200, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("response body revid1 = %s", body)
	attachments = body["_attachments"].(map[string]interface{})
	attach1 = attachments["attach1"].(map[string]interface{})
	data = attach1["data"]
	assert.True(t, data == nil)

}

func TestAddingAttachment(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	defer func() { walrus.MaxDocSize = 0 }()

	walrus.MaxDocSize = 20 * 1024 * 1024

	testCases := []struct {
		name        string
		docName     string
		byteSize    int
		expectedPut int
		expectedGet int
	}{
		{
			name:        "Regular attachment",
			docName:     "doc1",
			byteSize:    20,
			expectedPut: http.StatusCreated,
			expectedGet: http.StatusOK,
		},
		{
			name:        "Too large attachment",
			docName:     "doc2",
			byteSize:    22000000,
			expectedPut: http.StatusRequestEntityTooLarge,
			expectedGet: http.StatusNotFound,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(tt *testing.T) {
			docrevId := rt.CreateDoc(tt, testCase.docName)

			attachmentBody := base64.StdEncoding.EncodeToString(make([]byte, testCase.byteSize))
			attachmentContentType := "content/type"
			reqHeaders := map[string]string{
				"Content-Type": attachmentContentType,
			}

			// Set attachment
			response := rt.SendAdminRequestWithHeaders("PUT", "/db/"+testCase.docName+"/attach1?rev="+docrevId,
				attachmentBody, reqHeaders)
			RequireStatus(tt, response, testCase.expectedPut)

			// Get attachment back
			response = rt.SendAdminRequestWithHeaders("GET", "/db/"+testCase.docName+"/attach1", "", reqHeaders)
			RequireStatus(tt, response, testCase.expectedGet)

			// If able to retrieve document check it is same as original
			if response.Code == 200 {
				assert.Equal(tt, response.Body.String(), attachmentBody)
			}
		})
	}

}

// Reproduces panic seen in https://github.com/couchbase/sync_gateway/issues/2528
func TestBulkGetBadAttachmentReproIssue2528(t *testing.T) {

	if base.TestUseXattrs() {
		// Since we now store attachment metadata in sync data,
		// this test cannot modify the xattrs to reproduce the panic
		t.Skip("This test only works with XATTRS disabled")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	docIdDoc1 := "doc"
	attachmentName := "attach1"

	// Add a doc
	resource := fmt.Sprintf("/db/%v", docIdDoc1)
	response := rt.SendAdminRequest("PUT", resource, `{"prop":true}`)
	RequireStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revidDoc1 := body["rev"].(string)

	// Add another doc
	docIdDoc2 := "doc2"
	responseDoc2 := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v", docIdDoc2), `{"prop":true}`)
	RequireStatus(t, responseDoc2, 201)
	revidDoc2 := body["rev"].(string)

	// attach to existing document with correct rev (should succeed)
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response = rt.SendAdminRequestWithHeaders(
		"PUT",
		fmt.Sprintf("%v/%v?rev=%v", resource, attachmentName, revidDoc1),
		attachmentBody,
		reqHeaders,
	)
	RequireStatus(t, response, 201)

	// Get the couchbase doc
	couchbaseDoc := db.Body{}
	_, err := rt.GetSingleTestDataStore().Get(docIdDoc1, &couchbaseDoc)
	assert.NoError(t, err, "Error getting couchbaseDoc")
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
	s, ok := couchbaseDoc[base.SyncPropertyName].(map[string]interface{})
	require.True(t, ok)
	couchbaseDoc["_attachments"], ok = s["attachments"].(map[string]interface{})
	require.True(t, ok)
	attachments, ok := couchbaseDoc["_attachments"].(map[string]interface{})
	require.True(t, ok)

	attach1, ok := attachments[attachmentName].(map[string]interface{})
	require.True(t, ok)

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
	err = rt.GetSingleTestDataStore().Set(docIdDoc1, 0, nil, couchbaseDoc)
	assert.NoError(t, err, "Error putting couchbaseDoc")

	// Flush rev cache so that the _bulk_get request is forced to go back to the bucket to load the doc
	// rather than loading it from the (stale) rev cache.  The rev cache will be stale since the test
	// short-circuits Sync Gateway and directly updates the bucket.
	// Reset at the end of the test, to avoid bleed into other tests
	rt.GetDatabase().GetSingleDatabaseCollection().FlushRevisionCacheForTest()

	// Get latest rev id
	response = rt.SendAdminRequest("GET", resource, "")
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId := body[db.BodyRev]

	// Do a bulk_get to get the doc -- this was causing a panic prior to the fix for #2528
	bulkGetDocs := fmt.Sprintf(`{"docs": [{"id": "%v", "rev": "%v"}, {"id": "%v", "rev": "%v"}]}`, docIdDoc1, revId, docIdDoc2, revidDoc2)
	bulkGetResponse := rt.SendAdminRequest("POST", "/db/_bulk_get?revs=true&attachments=true&revs_limit=2", bulkGetDocs)
	if bulkGetResponse.Code != 200 {
		panic(fmt.Sprintf("Got unexpected response: %v", bulkGetResponse))
	}

	bulkGetResponse.DumpBody()

	// Parse multipart/mixed docs and create reader
	contentType, attrs, _ := mime.ParseMediaType(bulkGetResponse.Header().Get("Content-Type"))
	log.Printf("content-type: %v.  attrs: %v", contentType, attrs)
	assert.Equal(t, "multipart/mixed", contentType)
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

		partBytes, err := io.ReadAll(part)
		assert.NoError(t, err, "Unexpected error")

		log.Printf("multipart part: %+v", string(partBytes))

		partJson := map[string]interface{}{}
		err = base.JSONUnmarshal(partBytes, &partJson)
		assert.NoError(t, err, "Unexpected error")

		// Assert expectations for the doc with attachment errors
		rawId, ok := partJson["id"]
		if ok {
			// expect an error
			_, hasErr := partJson["error"]
			assert.True(t, hasErr, "Expected error field for this doc")
			assert.Equal(t, rawId, docIdDoc1)
			sawDoc1 = true

		}

		// Assert expectations for the doc with no attachment errors
		rawId, ok = partJson[db.BodyId]
		if ok {
			_, hasErr := partJson["error"]
			assert.True(t, !hasErr, "Did not expect error field for this doc")
			assert.Equal(t, rawId, docIdDoc2)
			sawDoc2 = true
		}

	}

	assert.True(t, sawDoc2, "Did not see doc 2")
	assert.True(t, sawDoc1, "Did not see doc 1")

}

func TestConflictWithInvalidAttachment(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create Doc
	docrevId := rt.CreateDoc(t, "doc1")

	docRevDigest := strings.Split(docrevId, "-")[1]

	// Setup Attachment
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// Set attachment
	attachmentBody := "aGVsbG8gd29ybGQ=" // hello.txt
	response := rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+docrevId, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docrevId2 := body["rev"].(string)

	// Update Doc
	rev3Input := `{"_attachments":{"attach1":{"content-type": "content/type", "digest":"sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub": true}}, "_id": "doc1", "_rev": "` + docrevId2 + `", "prop":true}`
	response = rt.SendAdminRequest("PUT", "/db/doc1", rev3Input)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docrevId3 := body["rev"].(string)

	// Get Existing Doc & Update rev
	rev4Input := `{"_attachments":{"attach1":{"content-type": "content/type", "digest":"sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub": true}}, "_id": "doc1", "_rev": "` + docrevId3 + `", "prop":true}`
	response = rt.SendAdminRequest("PUT", "/db/doc1", rev4Input)
	RequireStatus(t, response, http.StatusCreated)

	// Get Existing Doc to Modify
	response = rt.SendAdminRequest("GET", "/db/doc1?revs=true", "")
	RequireStatus(t, response, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	// Modify Doc
	parentRevList := [3]string{"foo3", "foo2", docRevDigest}
	body["_rev"] = "3-foo3"
	body["rev"] = "3-foo3"
	body["_revisions"].(map[string]interface{})["ids"] = parentRevList
	body["_revisions"].(map[string]interface{})["start"] = 3
	delete(body["_attachments"].(map[string]interface{})["attach1"].(map[string]interface{}), "digest")

	// Prepare changed doc
	temp, err := base.JSONMarshal(body)
	assert.NoError(t, err)
	newBody := string(temp)

	// Send changed / conflict doc
	response = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", newBody)
	RequireStatus(t, response, http.StatusBadRequest)
}

// Create doc with attachment at rev 1 using pre-2.5 metadata (outside of _sync)
// Create rev 2 with stub using att revpos 1 and make sure we fetch the attachment correctly
// Reproduces CBG-616
func TestAttachmentRevposPre25Metadata(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("Skipping with xattrs due to use of AddRaw _sync data")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	ok, err := rt.GetSingleTestDataStore().Add("doc1", 0, []byte(`{"_attachments":{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1,"stub":true}},"_sync":{"rev":"1-6e5a9ed9e2e8637d495ac5dd2fa90479","sequence":2,"recent_sequences":[2],"history":{"revs":["1-6e5a9ed9e2e8637d495ac5dd2fa90479"],"parents":[-1],"channels":[null]},"cas":"","time_saved":"2019-12-06T20:02:25.523013Z"},"test":true}`))
	require.NoError(t, err)
	require.True(t, ok)

	response := rt.SendAdminRequest("PUT", "/db/doc1?rev=1-6e5a9ed9e2e8637d495ac5dd2fa90479", `{"test":false,"_attachments":{"hello.txt":{"stub":true,"revpos":1}}}`)
	RequireStatus(t, response, 201)
	var putResp struct {
		OK  bool   `json:"ok"`
		Rev string `json:"rev"`
	}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &putResp))
	require.True(t, putResp.OK)

	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	RequireStatus(t, response, 200)
	var body struct {
		Test        bool             `json:"test"`
		Attachments db.AttachmentMap `json:"_attachments"`
	}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.False(t, body.Test)
	att, ok := body.Attachments["hello.txt"]
	require.True(t, ok)
	assert.Equal(t, 1, att.Revpos)
	assert.True(t, att.Stub)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", att.Digest)
}

func TestConflictingBranchAttachments(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create a document
	docRevId := rt.CreateDoc(t, "doc1")
	docRevDigest := strings.Split(docRevId, "-")[1]

	// //Create diverging tree
	var body db.Body

	reqBodyRev2 := `{"_rev": "2-two", "_revisions": {"ids": ["two", "` + docRevDigest + `"], "start": 2}}`
	response := rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", reqBodyRev2)
	RequireStatus(t, response, http.StatusCreated)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId2 := body["rev"].(string)
	assert.Equal(t, "2-two", docRevId2)

	reqBodyRev2a := `{"_rev": "2-two", "_revisions": {"ids": ["twoa", "` + docRevDigest + `"], "start": 2}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", reqBodyRev2a)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId2a := body["rev"].(string)
	assert.Equal(t, "2-twoa", docRevId2a)

	// Create an attachment
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// Put attachment on doc1 rev 2
	rev3Attachment := `aGVsbG8gd29ybGQ=` // hello.txt
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1?rev=2-two", rev3Attachment, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId3 := body["rev"].(string)

	// Put attachment on doc1 conflicting rev 2a
	rev3aAttachment := `Z29vZGJ5ZSBjcnVlbCB3b3JsZA==` // bye.txt
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1a?rev=2-twoa", rev3aAttachment, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId3a := body["rev"].(string)

	// Perform small update on doc3
	rev4Body := `{"_id": "doc1", "_attachments": {"attach1": {"content_type": "content/type", "digest": "sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 3, "stub":true}}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId3, rev4Body)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId4 := body["rev"].(string)

	// Perform small update on doc3a
	rev4aBody := `{"_id": "doc1", "_attachments": {"attach1a": {"content_type": "content/type", "digest": "sha1-rdfKyt3ssqPHnWBUxl/xauXXcUs=", "length": 28, "revpos": 3, "stub": true}}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId3a, rev4aBody)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId4a := body["rev"].(string)

	// Ensure the two attachments are different
	response1 := rt.SendAdminRequest("GET", "/db/doc1?atts_since=[\""+docRevId+"\"]&rev="+docRevId4, "")
	response2 := rt.SendAdminRequest("GET", "/db/doc1?rev="+docRevId4a, "")

	var body1 db.Body
	var body2 db.Body

	require.NoError(t, base.JSONUnmarshal(response1.Body.Bytes(), &body1))
	require.NoError(t, base.JSONUnmarshal(response2.Body.Bytes(), &body2))

	assert.NotEqual(t, body1["_attachments"], body2["_attachments"])

}

func TestAttachmentsWithTombstonedConflict(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create a document
	docRevId := rt.CreateDoc(t, "doc1")
	// docRevDigest := strings.Split(docRevId, "-")[1]

	// Create an attachment
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// Add an attachment at rev 2
	var body db.Body
	rev2Attachment := `aGVsbG8gd29ybGQ=` // hello.txt
	response := rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+docRevId, rev2Attachment, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId2 := body["rev"].(string)

	// Create rev 3, preserve the attachment
	rev3Body := `{"_id": "doc1", "mod":"mod_3", "_attachments": {"attach1": {"content_type": "content/type", "digest": "sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub":true}}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId2, rev3Body)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId3 := body["rev"].(string)

	// Add another attachment at rev 4
	rev4Attachment := `Z29vZGJ5ZSBjcnVlbCB3b3JsZA==` // bye.txt
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach2?rev="+docRevId3, rev4Attachment, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId4 := body["rev"].(string)

	// Create rev 5, preserve the attachments
	rev5Body := `{"_id": "doc1",` +
		`"mod":"mod_5",` +
		`"_attachments": ` +
		`{"attach1": {"content_type": "content/type", "digest": "sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub":true},` +
		` "attach2": {"content_type": "content/type", "digest": "sha1-rdfKyt3ssqPHnWBUxl/xauXXcUs=", "length": 28, "revpos": 4, "stub":true}}` +
		`}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId4, rev5Body)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId5 := body["rev"].(string)

	// Create rev 6, preserve the attachments
	rev6Body := `{"_id": "doc1",` +
		`"mod":"mod_5",` +
		`"_attachments": ` +
		`{"attach1": {"content_type": "content/type", "digest": "sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub":true},` +
		` "attach2": {"content_type": "content/type", "digest": "sha1-rdfKyt3ssqPHnWBUxl/xauXXcUs=", "length": 28, "revpos": 4, "stub":true}}` +
		`}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId5, rev6Body)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	// docRevId6 := body["rev"].(string)

	response = rt.SendAdminRequest("GET", "/db/doc1?atts_since=[\""+docRevId+"\"]", "")
	log.Printf("Rev6 GET: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, attachmentsPresent := body["_attachments"]
	assert.True(t, attachmentsPresent)

	// Create conflicting rev 6 that doesn't have attachments
	reqBodyRev6a := `{"_rev": "6-a", "_revisions": {"ids": ["a", "` + docRevId5 + `"], "start": 6}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", reqBodyRev6a)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId2a := body["rev"].(string)
	assert.Equal(t, "6-a", docRevId2a)

	var rev6Response db.Body
	response = rt.SendAdminRequest("GET", "/db/doc1?atts_since=[\""+docRevId+"\"]", "")
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &rev6Response))
	_, attachmentsPresent = rev6Response["_attachments"]
	assert.False(t, attachmentsPresent)

	// Tombstone revision 6-a, leaves 6-7368e68932e8261dba7ad831e3cd5a5e as winner
	response = rt.SendAdminRequest("DELETE", "/db/doc1?rev=6-a", "")
	RequireStatus(t, response, http.StatusOK)

	// Retrieve current winning rev with attachments
	var rev7Response db.Body
	response = rt.SendAdminRequest("GET", "/db/doc1?atts_since=[\""+docRevId+"\"]", "")
	log.Printf("Rev6 GET: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &rev7Response))
	_, attachmentsPresent = rev7Response["_attachments"]
	assert.True(t, attachmentsPresent)
}

func TestAttachmentGetReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// Put document as usual with attachment
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar", "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))

	// Get a document with rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true", ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
		assert.Contains(t, body[db.BodyAttachments], "hello.txt")
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}
}

func TestWebhookPropsWithAttachments(t *testing.T) {
	wg := sync.WaitGroup{}
	handler := func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		bodyBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err, "Error reading request body")
		require.NoError(t, r.Body.Close(), "Error closing request body")

		var body db.Body
		require.NoError(t, base.JSONUnmarshal(bodyBytes, &body), "Error parsing document body")
		assert.Equal(t, "doc1", body[db.BodyId])
		assert.Equal(t, "bar", body["foo"])

		if strings.HasPrefix(body[db.BodyRev].(string), "1-") {
			assert.Equal(t, "1-cd809becc169215072fd567eebd8b8de", body[db.BodyRev])
		}

		if strings.HasPrefix(body[db.BodyRev].(string), "2-") {
			assert.Equal(t, "2-6433ff70e11791fcb7fdf16746f4b9e7", body[db.BodyRev])
			attachments := body[db.BodyAttachments].(map[string]interface{})
			attachment1 := attachments["attach1"].(map[string]interface{})
			assert.Equal(t, "sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=", attachment1["digest"])
			assert.Equal(t, float64(30), attachment1["length"])
			assert.Equal(t, float64(2), attachment1["revpos"])
			assert.True(t, attachment1["stub"].(bool))
			assert.Equal(t, "content/type", attachment1["content_type"])
		}
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: true,
			EventHandlers: &EventHandlerConfig{
				DocumentChanged: []*EventConfig{
					{Url: s.URL, Filter: "function(doc){return true;}", HandlerType: "webhook"},
				},
			},
		},
		}}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	// Create first revision of the document with no attachment.
	wg.Add(1)
	response := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"foo": "bar"}`)
	RequireStatus(t, response, http.StatusCreated)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.True(t, body["ok"].(bool))
	doc1revId := body["rev"].(string)

	// Add attachment to the doc.
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{"Content-Type": attachmentContentType}
	resource := "/db/doc1/attach1?rev=" + doc1revId
	wg.Add(1)
	response = rt.SendAdminRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	RequireStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.True(t, body["ok"].(bool))
	revIdAfterAttachment := body["rev"].(string)
	assert.NotEmpty(t, revIdAfterAttachment, "No revid in response for PUT attachment")
	assert.NotEqual(t, revIdAfterAttachment, doc1revId)
	wg.Wait()
}

func TestAttachmentContentType(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	type attTest struct {
		setContentType                bool
		putContentType                string
		expectedContentDispositionSet bool
	}

	tests := []attTest{
		{
			setContentType:                true,
			putContentType:                "text/html",
			expectedContentDispositionSet: true,
		},
		{
			setContentType:                true,
			putContentType:                "text/html; charset=utf-8",
			expectedContentDispositionSet: true,
		},
		{
			setContentType:                true,
			putContentType:                "application/xhtml+xml",
			expectedContentDispositionSet: true,
		},
		{
			setContentType:                true,
			putContentType:                "image/svg+xml",
			expectedContentDispositionSet: true,
		},
		{
			setContentType:                true,
			putContentType:                "",
			expectedContentDispositionSet: true,
		},
		{
			setContentType:                true,
			putContentType:                "application/json",
			expectedContentDispositionSet: false,
		},
		{
			setContentType:                false,
			putContentType:                "",
			expectedContentDispositionSet: true,
		},
	}

	// Tests will be ran against default config
	for index, test := range tests {
		contentType := ""
		if test.setContentType {
			contentType = fmt.Sprintf(`, "content_type":"%s"`, test.putContentType)
		}
		attachmentBody := fmt.Sprintf(`{"key":"val", "_attachments": {"login.aspx": {"data": "PGgxPllvdXJCYW5rIExvZ2luPC9oMT4KPGlucHV0Lz4KPGlucHV0Lz4KPGlucHV0IHR5cGU9InN1Ym1pdCIvPg=="%s}}}`, contentType)
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc_%d", index), attachmentBody)
		RequireStatus(t, response, http.StatusCreated)

		response = rt.SendRequest("GET", fmt.Sprintf("/db/doc_%d/login.aspx", index), "")
		contentDisposition := response.Header().Get("Content-Disposition")

		if test.expectedContentDispositionSet {
			assert.Equal(t, `attachment`, contentDisposition, fmt.Sprintf("Failed with doc_%d", index))
		} else {
			assert.Equal(t, "", contentDisposition)
		}
	}

	// Ran against allow insecure
	rt.DatabaseConfig.ServeInsecureAttachmentTypes = base.BoolPtr(true)
	for index, _ := range tests {
		response := rt.SendRequest("GET", fmt.Sprintf("/db/doc_allow_insecure_%d/login.aspx", index), "")
		contentDisposition := response.Header().Get("Content-Disposition")

		assert.Equal(t, "", contentDisposition)
	}
}

func TestBasicAttachmentRemoval(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	attContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attContentType,
	}

	storeAttachment := func(doc, rev, attName, attBody string) string {
		resource := fmt.Sprintf("/db/%s/%s?rev=%s", doc, attName, rev)
		response := rt.SendRequestWithHeaders(http.MethodPut, resource, attBody, reqHeaders)
		RequireStatus(t, response, http.StatusCreated)
		var body db.Body
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		require.True(t, body["ok"].(bool))
		return body["rev"].(string)
	}

	retrieveAttachment := func(docID, attName string) (attBody string) {
		resource := fmt.Sprintf("/db/%s/%s", docID, attName)
		response := rt.SendRequest(http.MethodGet, resource, "")
		RequireStatus(t, response, http.StatusOK)
		return string(response.Body.Bytes())
	}

	retrieveAttachmentKey := func(docID, attName string) (key string) {
		resource := fmt.Sprintf("/db/%s/%s?meta=true", docID, attName)
		response := rt.SendRequest(http.MethodGet, resource, "")
		var meta map[string]interface{}
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &meta))
		RequireStatus(t, response, http.StatusOK)
		key, found := meta["key"].(string)
		require.True(t, found)
		return key
	}

	requireAttachmentNotFound := func(docID, attName string) {
		resource := fmt.Sprintf("/db/%s/%s", docID, attName)
		response := rt.SendRequest(http.MethodGet, resource, "")
		RequireStatus(t, response, http.StatusNotFound)
	}

	retrieveAttachmentMeta := func(docID string) (attMeta map[string]interface{}) {
		body := rt.GetDoc(docID)
		attachments, ok := body["_attachments"].(map[string]interface{})
		require.True(t, ok)
		return attachments
	}

	dataStore := rt.GetSingleTestDataStore()
	requireAttachmentFound := func(attKey string, attBodyExpected []byte) {
		var attBodyActual []byte
		_, err := dataStore.Get(attKey, &attBodyActual)
		require.NoError(t, err)
		assert.Equal(t, attBodyExpected, attBodyActual)
	}

	CreateDocWithLegacyAttachment := func(docID string, rawDoc []byte, attKey string, attBody []byte) {
		CreateDocWithLegacyAttachment(t, rt, docID, rawDoc, attKey, attBody)
	}

	t.Run("single attachment removal upon document update", func(t *testing.T) {
		// Create a document.
		docID := "foo"
		revID := rt.CreateDoc(t, docID)
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Add an attachment to the document.
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		revID = storeAttachment(docID, revID, attName, attBody)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Remove attachment from the bucket via document update.
		response := rt.UpdateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("single attachment removal upon document delete", func(t *testing.T) {
		// Create a document.
		docID := "bar"
		revID := rt.CreateDoc(t, docID)
		require.NotEmpty(t, revID)

		// Add an attachment to the document.
		attName := "bar.txt"
		attBody := "this is the body of attachment bar.txt"
		revID = storeAttachment(docID, revID, attName, attBody)
		require.NotEmpty(t, revID)

		// Retrieve the attachment added from the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-Av0dem1kCRIddzAlnK4A2Mgn6Uo=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve attachment key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Delete/tombstone the document.
		rt.DeleteDoc(docID, revID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("single attachment removal upon document purge", func(t *testing.T) {
		// Create a document.
		docID := "baz"
		revID := rt.CreateDoc(t, docID)
		require.NotEmpty(t, revID)

		// Add an attachment to the document.
		attName := "baz.txt"
		attBody := "this is the body of attachment baz.txt"
		revID = storeAttachment(docID, revID, attName, attBody)
		require.NotEmpty(t, revID)

		// Retrieve attachment associated with the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-8i/O8CzFsxHmwT4SLoVI6PIKRDo=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve attachment key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Purge the entire document.
		rt.PurgeDoc(docID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)
	})

	t.Run("single attachment removal upon attachment update", func(t *testing.T) {
		// Create a document.
		docID := "qux"
		revID := rt.CreateDoc(t, docID)
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Add an attachment to the document.
		attName := "qux.txt"
		attBody := "this is the body of attachment qux.txt"
		revID = storeAttachment(docID, revID, attName, attBody)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the attachment added from the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the doc and check the attachment meta.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-0naD6SgfLVDr+zakP8RkNlBYORw=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve attachment key used for internal attachment storage and retrieval.
		attKeyOld := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:IfWNJ/gn0pX/zYYMZQRWheO68a1FBsqgFAETsxZkdTQ=:sha1-0naD6SgfLVDr+zakP8RkNlBYORw=", attKeyOld)

		// Update the attachment body bytes.
		attBodyUpdated := "this is the updated body of attachment qux.txt"
		revID = storeAttachment(docID, revID, attName, attBodyUpdated)
		require.Equal(t, "3-9d8d0bf2e87982d97356a181a693291b", revID)

		// Retrieve the updated attachment added from the document.
		actualAttBody = retrieveAttachment(docID, attName)
		require.Equal(t, attBodyUpdated, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok = attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-dDdppdY7RC4gq550G7eGJgQmk6g=", meta["digest"].(string))
		assert.Equal(t, float64(46), meta["length"].(float64))
		assert.Equal(t, float64(3), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Check whether the old attachment blob is removed from the underlying storage.
		rt.RequireDocNotFound(attKeyOld)

		// Retrieve new attachment key used for internal attachment storage and retrieval.
		attKeyNew := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:IfWNJ/gn0pX/zYYMZQRWheO68a1FBsqgFAETsxZkdTQ=:sha1-0naD6SgfLVDr+zakP8RkNlBYORw=", attKeyOld)
		require.NotEqual(t, attKeyOld, attKeyNew)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("multiple attachments removal upon document update", func(t *testing.T) {
		// Create a document.
		docID := "foo1"
		revID := rt.CreateDoc(t, docID)
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Add an attachment to the document.
		att1Name := "alice.txt"
		att1Body := "this is the body of attachment alice.txt"
		revID = storeAttachment(docID, revID, att1Name, att1Body)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, att1Name)
		require.Equal(t, att1Body, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att1Key := retrieveAttachmentKey(docID, att1Name)
		require.NotEmpty(t, att1Key)

		// Add another attachment to the same document.
		att2Name := "bob.txt"
		att2Body := "this is the body of attachment bob.txt"
		revID = storeAttachment(docID, revID, att2Name, att2Body)
		require.Equal(t, "3-9d8d0bf2e87982d97356a181a693291b", revID)

		// Retrieve the second attachment added to the document.
		actualAtt2Body := retrieveAttachment(docID, att2Name)
		require.Equal(t, att2Body, actualAtt2Body)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 2)

		meta, ok = attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		meta, ok = attachments[att2Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-3oMVZvHjOQkkEK7K/xp0tqkuj1Q=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(3), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att2Key := retrieveAttachmentKey(docID, att2Name)
		require.NotEmpty(t, att2Key)
		require.NotEqual(t, att1Key, att2Key)

		// Remove both attachments from the bucket via document update.
		response := rt.UpdateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether both attachments are removed from the underlying storage.
		requireAttachmentNotFound(docID, att1Name)
		rt.RequireDocNotFound(att1Key)
		requireAttachmentNotFound(docID, att2Name)
		rt.RequireDocNotFound(att2Key)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("multiple attachments removal upon document delete", func(t *testing.T) {
		// Create a document.
		docID := "foo2"
		revID := rt.CreateDoc(t, docID)
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Add an attachment to the document.
		att1Name := "alice.txt"
		att1Body := "this is the body of attachment alice.txt"
		revID = storeAttachment(docID, revID, att1Name, att1Body)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, att1Name)
		require.Equal(t, att1Body, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att1Key := retrieveAttachmentKey(docID, att1Name)
		require.NotEmpty(t, att1Key)

		// Add another attachment to the same document.
		att2Name := "bob.txt"
		att2Body := "this is the body of attachment bob.txt"
		revID = storeAttachment(docID, revID, att2Name, att2Body)
		require.Equal(t, "3-9d8d0bf2e87982d97356a181a693291b", revID)

		// Retrieve the second attachment added to the document.
		actualAtt2Body := retrieveAttachment(docID, att2Name)
		require.Equal(t, att2Body, actualAtt2Body)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 2)

		meta, ok = attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		meta, ok = attachments[att2Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-3oMVZvHjOQkkEK7K/xp0tqkuj1Q=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(3), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att2Key := retrieveAttachmentKey(docID, att2Name)
		require.NotEmpty(t, att2Key)
		require.NotEqual(t, att1Key, att2Key)

		// Delete/tombstone the document.
		rt.DeleteDoc(docID, revID)

		// Check whether both attachments are removed from the underlying storage.
		requireAttachmentNotFound(docID, att1Name)
		rt.RequireDocNotFound(att1Key)
		requireAttachmentNotFound(docID, att2Name)
		rt.RequireDocNotFound(att2Key)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("multiple attachments removal upon document purge", func(t *testing.T) {
		// Create a document.
		docID := "foo3"
		revID := rt.CreateDoc(t, docID)
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Add an attachment to the document.
		att1Name := "alice.txt"
		att1Body := "this is the body of attachment alice.txt"
		revID = storeAttachment(docID, revID, att1Name, att1Body)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, att1Name)
		require.Equal(t, att1Body, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att1Key := retrieveAttachmentKey(docID, att1Name)
		require.NotEmpty(t, att1Key)

		// Add another attachment to the same document.
		att2Name := "bob.txt"
		att2Body := "this is the body of attachment bob.txt"
		revID = storeAttachment(docID, revID, att2Name, att2Body)
		require.Equal(t, "3-9d8d0bf2e87982d97356a181a693291b", revID)

		// Retrieve the second attachment added to the document.
		actualAtt2Body := retrieveAttachment(docID, att2Name)
		require.Equal(t, att2Body, actualAtt2Body)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 2)

		meta, ok = attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		meta, ok = attachments[att2Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-3oMVZvHjOQkkEK7K/xp0tqkuj1Q=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(3), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att2Key := retrieveAttachmentKey(docID, att2Name)
		require.NotEmpty(t, att2Key)
		require.NotEqual(t, att1Key, att2Key)

		// Purge the entire document.
		rt.PurgeDoc(docID)

		// Check whether both attachments are removed from the underlying storage.
		requireAttachmentNotFound(docID, att1Name)
		rt.RequireDocNotFound(att1Key)
		requireAttachmentNotFound(docID, att2Name)
		rt.RequireDocNotFound(att2Key)
	})

	t.Run("single inline attachment removal upon document update", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo8"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.PutDoc(docID, body)
		revID := putResponse.Rev
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Remove attachment from the bucket via document update.
		response := rt.UpdateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("single inline attachment removal upon document delete", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo9"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.PutDoc(docID, body)
		revID := putResponse.Rev
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Delete/tombstone the document.
		rt.DeleteDoc(docID, revID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("single inline attachment removal upon document purge", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo10"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.PutDoc(docID, body)
		revID := putResponse.Rev
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:ccNG7Q7yTHRLEo8vQ3aDuYDnBmZfFu3E2YtCqbg8/dk=:sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", attKey)

		// Purge the entire document.
		rt.PurgeDoc(docID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)
	})

	t.Run("single inline attachment removal upon attachment update", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo11"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.PutDoc(docID, body)
		revID := putResponse.Rev
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Retrieve the attachment added from the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the doc and check the attachment meta.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve attachment key used for internal attachment storage and retrieval.
		attKeyOld := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:8moUa62DqG+wrhztGWL8Sj9qpCQz7tat6Z5LRt6/DWE=:sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", attKeyOld)

		// Update the attachment body bytes.
		attBodyUpdated := "this is the updated body of attachment qux.txt"
		revID = storeAttachment(docID, revID, attName, attBodyUpdated)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the updated attachment added from the document.
		actualAttBody = retrieveAttachment(docID, attName)
		require.Equal(t, attBodyUpdated, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok = attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-dDdppdY7RC4gq550G7eGJgQmk6g=", meta["digest"].(string))
		assert.Equal(t, float64(46), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Check whether the old attachment blob is removed from the underlying storage.
		rt.RequireDocNotFound(attKeyOld)

		// Retrieve new attachment key used for internal attachment storage and retrieval.
		attKeyNew := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:8moUa62DqG+wrhztGWL8Sj9qpCQz7tat6Z5LRt6/DWE=:sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", attKeyOld)
		require.NotEqual(t, attKeyOld, attKeyNew)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("attachment removal upon document delete via SDK", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() {
			t.Skip("This import test won't work under walrus")
		}

		if !base.TestUseXattrs() {
			t.Skip("Test requires xattrs")
		}

		// Create a document with inline attachment.
		docID := "foo10"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.PutDoc(docID, body)
		revID := putResponse.Rev
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Delete/tombstone the document via SDK.
		err := dataStore.Delete(docID)
		require.NoError(t, err, "Unable to delete doc %q", docID)

		// Wait until the "delete" mutation appears on the changes feed.
		changes, err := rt.WaitForChanges(1, "/db/_changes", "", true)
		assert.NoError(t, err, "Error waiting for changes")
		log.Printf("changes: %+v", changes)
		rt.RequireDocNotFound(docID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.RequireDocNotFound(attKey)
	})

	t.Run("skip attachment removal upon document update via SDK", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() {
			t.Skip("This import test won't work under walrus")
		}

		if !base.TestUseXattrs() {
			t.Skip("Test requires xattrs")
		}

		// Create a document with inline attachment.
		docID := "foo11"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.PutDoc(docID, body)
		revID := putResponse.Rev
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		attKey := retrieveAttachmentKey(docID, attName)
		require.NotEmpty(t, attKey)

		// Update the document via SDK.
		err := dataStore.Set(docID, 0, nil, []byte(`{"prop": false}`))
		require.NoError(t, err, "Error updating the document")

		// Wait until the "update" mutation appears on the changes feed.
		changes, err := rt.WaitForChanges(1, "/db/_changes", "", true)
		assert.NoError(t, err, "Error waiting for changes")
		log.Printf("changes: %+v", changes)

		// Verify that the attachment is not removed.
		actualAttBody = retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check doc body and attachment metadata.
		updatedBody := rt.GetDoc(docID)
		require.False(t, updatedBody["prop"].(bool))
		revID, ok = updatedBody["_rev"].(string)
		require.True(t, ok)
		require.Equal(t, "2-7ae7c29fb73baf11cd0459ca3c5a6ac4", revID)
		attachments, ok = updatedBody["_attachments"].(map[string]interface{})
		require.True(t, ok)
		require.Len(t, attachments, 1)
		meta, ok = attachments[attName].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(1), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")
	})

	t.Run("doc with multiple attachments and removal of a single one upon document update", func(t *testing.T) {
		// Create a document.
		docID := "foo12"
		revID := rt.CreateDoc(t, docID)
		require.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revID)

		// Add an attachment to the document.
		att1Name := "alice.txt"
		att1Body := "this is the body of attachment alice.txt"
		revID = storeAttachment(docID, revID, att1Name, att1Body)
		require.Equal(t, "2-abe7339f42c9218acb7b906f5977adcf", revID)

		// Retrieve the attachment added to the document.
		actualAttBody := retrieveAttachment(docID, att1Name)
		require.Equal(t, att1Body, actualAttBody)

		// Get the document and check the attachment metadata.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok := attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att1Key := retrieveAttachmentKey(docID, att1Name)
		require.NotEmpty(t, att1Key)

		// Add another attachment to the same document.
		att2Name := "bob.txt"
		att2Body := "this is the body of attachment bob.txt"
		revID = storeAttachment(docID, revID, att2Name, att2Body)
		require.Equal(t, "3-9d8d0bf2e87982d97356a181a693291b", revID)

		// Retrieve the second attachment added to the document.
		actualAtt2Body := retrieveAttachment(docID, att2Name)
		require.Equal(t, att2Body, actualAtt2Body)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 2)

		meta, ok = attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		meta, ok = attachments[att2Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-3oMVZvHjOQkkEK7K/xp0tqkuj1Q=", meta["digest"].(string))
		assert.Equal(t, float64(38), meta["length"].(float64))
		assert.Equal(t, float64(3), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Retrieve the key used for internal attachment storage and retrieval.
		att2Key := retrieveAttachmentKey(docID, att2Name)
		require.NotEmpty(t, att2Key)
		require.NotEqual(t, att1Key, att2Key)

		// Remove one of the attachment from the bucket via document update.
		response := rt.UpdateDoc(docID, revID, `{"prop":true, "_attachments": {"alice.txt": {"stub": true, "revpos": 2}}}`)
		require.NotEmpty(t, response.Rev)

		// Get the document and check the attachment metadata.
		attachments = retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
		meta, ok = attachments[att2Name].(map[string]interface{})
		require.False(t, ok)
		meta, ok = attachments[att1Name].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, meta["stub"].(bool))
		assert.Equal(t, "content/type", meta["content_type"].(string))
		assert.Equal(t, "sha1-5vJRip1gGo8YsI9yEJmmv6DabXk=", meta["digest"].(string))
		assert.Equal(t, float64(40), meta["length"].(float64))
		assert.Equal(t, float64(2), meta["revpos"].(float64))
		assert.Nil(t, meta["ver"], "Attachment version shouldn't be exposed")

		// Check whether removed attachment is also removed from the underlying storage.
		requireAttachmentNotFound(docID, att2Name)
		rt.RequireDocNotFound(att2Key)

		// Verify that att1Name is still found in the bucket.
		actualAttBody = retrieveAttachment(docID, att1Name)
		require.Equal(t, att1Body, actualAttBody)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("legacy attachment persistence upon doc delete (single doc referencing an attachment)", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
			t.Skip("Test only works with a Couchbase server and Xattrs")
		}
		docID := "foo15"
		attBody := []byte(`hi`)
		digest := db.Sha1DigestKey(attBody)
		attKey := db.MakeAttachmentKey(db.AttVersion1, docID, digest)
		rawDoc := rawDocWithAttachmentAndSyncMeta()

		// Create a document with legacy attachment.
		CreateDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

		// Get the document and grab the revID.
		responseBody := rt.GetDoc(docID)
		revID := responseBody["_rev"].(string)
		require.NotEmpty(t, revID)

		// Delete/tombstone the document.
		rt.DeleteDoc(docID, revID)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("legacy attachment persistence upon doc delete (multiple docs referencing same attachment)", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
			t.Skip("Test only works with a Couchbase server and Xattrs")
		}
		docID1 := "foo16"
		docID2 := "bar16"
		attBody := []byte(`hi`)
		digest := db.Sha1DigestKey(attBody)
		attKey := db.MakeAttachmentKey(db.AttVersion1, docID1, digest)
		rawDoc := rawDocWithAttachmentAndSyncMeta()

		// Create a document with legacy attachment.
		CreateDocWithLegacyAttachment(docID1, rawDoc, attKey, attBody)

		// Create another document referencing the same legacy attachment.
		CreateDocWithLegacyAttachment(docID2, rawDoc, attKey, attBody)

		// Get revID of the first document.
		responseBody := rt.GetDoc(docID1)
		revID1 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID1)

		// Delete/tombstone the first document.
		rt.DeleteDoc(docID1, revID1)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Get revID of the second document.
		responseBody = rt.GetDoc(docID2)
		revID2 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID2)

		// Delete/tombstone the second document.
		rt.DeleteDoc(docID2, revID2)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID1)
		rt.PurgeDoc(docID2)
	})

	t.Run("legacy attachment persistence upon doc update (single doc referencing an attachment)", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
			t.Skip("Test only works with a Couchbase server and Xattrs")
		}
		docID := "foo17"
		attBody := []byte(`hi`)
		digest := db.Sha1DigestKey(attBody)
		attKey := db.MakeAttachmentKey(db.AttVersion1, docID, digest)
		rawDoc := rawDocWithAttachmentAndSyncMeta()

		// Create a document with legacy attachment.
		CreateDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

		// Get the document and grab the revID.
		responseBody := rt.GetDoc(docID)
		revID := responseBody["_rev"].(string)
		require.NotEmpty(t, revID)

		// Remove attachment from the document via document update.
		response := rt.UpdateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID)
	})

	t.Run("legacy attachment persistence upon doc update (multiple docs referencing same attachment)", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
			t.Skip("Test only works with a Couchbase server and Xattrs")
		}
		docID1 := "foo18"
		docID2 := "bar18"
		attBody := []byte(`hi`)
		digest := db.Sha1DigestKey(attBody)
		attKey := db.MakeAttachmentKey(db.AttVersion1, docID1, digest)
		rawDoc := rawDocWithAttachmentAndSyncMeta()

		// Create a document with legacy attachment.
		CreateDocWithLegacyAttachment(docID1, rawDoc, attKey, attBody)

		// Create another document referencing the same legacy attachment.
		CreateDocWithLegacyAttachment(docID2, rawDoc, attKey, attBody)

		// Get revID of the first document.
		responseBody := rt.GetDoc(docID1)
		revID1 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID1)

		// Remove attachment from the first document via document update.
		response := rt.UpdateDoc(docID1, revID1, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Get revID of the second document.
		responseBody = rt.GetDoc(docID2)
		revID2 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID2)

		// Remove attachment from the second document via document update.
		response = rt.UpdateDoc(docID2, revID2, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.PurgeDoc(docID1)
		rt.PurgeDoc(docID2)
	})

	t.Run("legacy attachment persistence upon doc purge (single doc referencing an attachment)", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
			t.Skip("Test only works with a Couchbase server and Xattrs")
		}
		docID := "foo19"
		attBody := []byte(`hi`)
		digest := db.Sha1DigestKey(attBody)
		attKey := db.MakeAttachmentKey(db.AttVersion1, docID, digest)
		rawDoc := rawDocWithAttachmentAndSyncMeta()

		// Create a document with legacy attachment.
		CreateDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

		// Get the document and grab the revID.
		responseBody := rt.GetDoc(docID)
		revID := responseBody["_rev"].(string)
		require.NotEmpty(t, revID)

		// Purge the entire document.
		rt.PurgeDoc(docID)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)
	})

	t.Run("legacy attachment persistence upon doc purge (multiple docs referencing same attachment)", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
			t.Skip("Test only works with a Couchbase server and Xattrs")
		}
		docID1 := "foo20"
		docID2 := "bar20"
		attBody := []byte(`hi`)
		digest := db.Sha1DigestKey(attBody)
		attKey := db.MakeAttachmentKey(db.AttVersion1, docID1, digest)
		rawDoc := rawDocWithAttachmentAndSyncMeta()

		// Create a document with legacy attachment.
		CreateDocWithLegacyAttachment(docID1, rawDoc, attKey, attBody)

		// Create another document referencing the same legacy attachment.
		CreateDocWithLegacyAttachment(docID2, rawDoc, attKey, attBody)

		// Get revID of the first document.
		responseBody := rt.GetDoc(docID1)
		revID1 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID1)

		// Purge the first document.
		rt.PurgeDoc(docID1)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Get revID of the second document.
		responseBody = rt.GetDoc(docID2)
		revID2 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID2)

		// Purge the second document.
		rt.PurgeDoc(docID2)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)
	})
}

func TestAttachmentRemovalWithConflicts(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AllowConflicts: base.BoolPtr(true),
			},
		},
	})

	defer rt.Close()

	// Create doc rev 1
	revid := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"test": "x"})

	// Create doc rev 2 with attachment
	revid = rt.CreateDocReturnRev(t, "doc", revid, map[string]interface{}{"_attachments": map[string]interface{}{"hello.txt": map[string]interface{}{"data": "aGVsbG8gd29ybGQ="}}})
	err := rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Create doc rev 3 referencing previous attachment
	resp := rt.SendAdminRequest("PUT", "/db/doc?rev="+revid, `{"_attachments": {"hello.txt": {"revpos":2,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`)
	RequireStatus(t, resp, http.StatusCreated)
	losingRev3 := RespRevID(t, resp)

	// Create doc conflicting with previous revid referencing previous attachment too
	_, revIDHash := db.ParseRevID(revid)
	resp = rt.SendAdminRequest("PUT", "/db/doc?new_edits=false", `{"_rev": "3-b", "_revisions": {"ids": ["b", "`+revIDHash+`"], "start": 3}, "_attachments": {"hello.txt": {"revpos":2,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}, "Winning Rev": true}`)
	RequireStatus(t, resp, http.StatusCreated)
	winningRev3 := RespRevID(t, resp)

	// Update the winning rev 3 and ensure attachment remains around as the other leaf still references this attachment
	resp = rt.SendAdminRequest("PUT", "/db/doc?rev="+winningRev3, `{"update": 2}`)
	RequireStatus(t, resp, http.StatusCreated)
	finalRev4 := RespRevID(t, resp)

	type docResp struct {
		Attachments db.AttachmentsMeta `json:"_attachments"`
	}

	var doc1 docResp
	// Get losing rev and ensure attachment is still there and has not been deleted
	resp = rt.SendAdminRequestWithHeaders("GET", "/db/doc?attachments=true&rev="+losingRev3, "", map[string]string{"Accept": "application/json"})
	RequireStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &doc1)
	assert.NoError(t, err)
	require.Contains(t, doc1.Attachments, "hello.txt")
	attachmentData, ok := doc1.Attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", attachmentData["digest"])
	assert.Equal(t, float64(11), attachmentData["length"])
	assert.Equal(t, float64(2), attachmentData["revpos"])
	assert.Equal(t, "aGVsbG8gd29ybGQ=", attachmentData["data"])

	attachmentKey := db.MakeAttachmentKey(2, "doc", attachmentData["digest"].(string))

	var doc2 docResp
	// Get winning rev and ensure attachment is indeed removed from this rev
	resp = rt.SendAdminRequestWithHeaders("GET", "/db/doc?attachments=true&rev="+finalRev4, "", map[string]string{"Accept": "application/json"})
	RequireStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &doc2)
	assert.NoError(t, err)
	require.NotContains(t, doc2.Attachments, "hello.txt")

	// Now remove the attachment in the losing rev by deleting the revision and ensure the attachment gets deleted
	resp = rt.SendAdminRequest("DELETE", "/db/doc?rev="+losingRev3, "")
	RequireStatus(t, resp, http.StatusOK)

	_, _, err = rt.GetSingleTestDataStore().GetRaw(attachmentKey)
	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
}

func TestAttachmentsMissing(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	_ = rt.Bucket()

	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello.txt": {"data": "aGVsbG8gd29ybGQ="}}}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev1ID := RespRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?rev="+rev1ID, `{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}, "testval": ["xxx","xxx"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev2ID := RespRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?new_edits=false", `{"_rev": "2-b", "_revisions": {"ids": ["b", "ca9ad22802b66f662ff171f226211d5c"], "start": 2}, "Winning Rev": true}`)
	RequireStatus(t, resp, http.StatusCreated)

	rt.GetDatabase().GetSingleDatabaseCollection().FlushRevisionCacheForTest()

	resp = rt.SendAdminRequest("GET", "/db/"+t.Name()+"?rev="+rev2ID, ``)
	RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")
}

func TestAttachmentsMissingNoBody(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	_ = rt.Bucket()

	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello.txt": {"data": "aGVsbG8gd29ybGQ="}}}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev1ID := RespRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?rev="+rev1ID, `{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev2ID := RespRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?new_edits=false", `{"_rev": "2-b", "_revisions": {"ids": ["b", "ca9ad22802b66f662ff171f226211d5c"], "start": 2}}`)
	RequireStatus(t, resp, http.StatusCreated)

	rt.GetDatabase().GetSingleDatabaseCollection().FlushRevisionCacheForTest()

	resp = rt.SendAdminRequest("GET", "/db/"+t.Name()+"?rev="+rev2ID, ``)
	RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")
}

func TestAttachmentDeleteOnPurge(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create doc with attachment
	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello": {"data": "aGVsbG8gd29ybGQ="}}}`)
	RequireStatus(t, resp, http.StatusCreated)
	err := rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Ensure attachment is uploaded and key the attachment doc key
	resp = rt.SendAdminRequest("GET", "/db/"+t.Name()+"/hello?meta=true", "")
	RequireStatus(t, resp, http.StatusOK)

	var body db.Body
	err = base.JSONUnmarshal(resp.BodyBytes(), &body)
	require.NoError(t, err)

	key, ok := body["key"].(string)
	assert.True(t, ok)

	// Ensure we can get the attachment doc
	_, _, err = rt.GetSingleTestDataStore().GetRaw(key)
	assert.NoError(t, err)

	// Purge the document
	resp = rt.SendAdminRequest("POST", "/db/_purge", `{"`+t.Name()+`": ["*"]}`)
	RequireStatus(t, resp, http.StatusOK)

	// Ensure that the attachment has now been deleted
	_, _, err = rt.GetDatabase().Bucket.DefaultDataStore().GetRaw(key)
	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
}

func TestAttachmentDeleteOnExpiry(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Expiry only supported by Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	dataStore := rt.GetSingleTestDataStore()

	// Create doc with attachment and expiry
	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello.txt": {"data": "aGVsbG8gd29ybGQ="}}, "_exp": 2}`)
	RequireStatus(t, resp, http.StatusCreated)
	err := rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Wait for document to be expired - this bucket get should also trigger the expiry purge interval
	err = rt.WaitForCondition(func() bool {
		_, _, err = dataStore.GetRaw(t.Name())
		return base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)

	// Trigger OnDemand Import for that doc to trigger tombstone
	resp = rt.SendAdminRequest("GET", "/db/"+t.Name(), "")
	RequireStatus(t, resp, http.StatusNotFound)

	att2Key := db.MakeAttachmentKey(db.AttVersion2, t.Name(), "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")

	// With xattrs doc will be imported and will be captured as tombstone and therefore purge attachments
	// Otherwise attachment will not be purged
	_, _, err = dataStore.GetRaw(att2Key)
	if base.TestUseXattrs() {
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
	} else {
		assert.NoError(t, err)
	}

}
func TestUpdateExistingAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClient(t, rt)
	assert.NoError(t, err)
	defer btc.Close()

	var doc1Body db.Body
	var doc2Body db.Body

	// Add doc1 and doc2
	req := rt.SendAdminRequest("PUT", "/db/doc1", `{}`)
	RequireStatus(t, req, http.StatusCreated)
	doc1Bytes := req.BodyBytes()
	req = rt.SendAdminRequest("PUT", "/db/doc2", `{}`)
	RequireStatus(t, req, http.StatusCreated)
	doc2Bytes := req.BodyBytes()

	require.NoError(t, rt.WaitForPendingChanges())

	err = json.Unmarshal(doc1Bytes, &doc1Body)
	assert.NoError(t, err)
	err = json.Unmarshal(doc2Bytes, &doc2Body)
	assert.NoError(t, err)

	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok := btc.WaitForRev("doc1", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)
	_, ok = btc.WaitForRev("doc2", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)

	attachmentAData := base64.StdEncoding.EncodeToString([]byte("attachmentA"))
	attachmentBData := base64.StdEncoding.EncodeToString([]byte("attachmentB"))

	revIDDoc1, err := btc.PushRev("doc1", doc1Body["rev"].(string), []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentAData+`"}}}`))
	require.NoError(t, err)
	revIDDoc2, err := btc.PushRev("doc2", doc2Body["rev"].(string), []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentBData+`"}}}`))
	require.NoError(t, err)

	err = rt.WaitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)
	err = rt.WaitForRev("doc2", revIDDoc2)
	assert.NoError(t, err)

	_, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "doc1", db.DocUnmarshalAll)
	_, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "doc2", db.DocUnmarshalAll)

	revIDDoc1, err = btc.PushRev("doc1", revIDDoc1, []byte(`{"key": "val", "_attachments":{"attachment":{"digest":"sha1-SKk0IV40XSHW37d3H0xpv2+z9Ck=","length":11,"content_type":"","stub":true,"revpos":3}}}`))
	require.NoError(t, err)

	err = rt.WaitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)

	doc1, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "doc1", db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, "sha1-SKk0IV40XSHW37d3H0xpv2+z9Ck=", doc1.Attachments["attachment"].(map[string]interface{})["digest"])

	req = rt.SendAdminRequest("GET", "/db/doc1/attachment", "")
	assert.Equal(t, "attachmentB", string(req.BodyBytes()))
}

// TestPushUnknownAttachmentAsStub sets revpos to an older generation, for an attachment that doesn't exist on the server.
// Verifies that getAttachment is triggered, and attachment is properly persisted.
func TestPushUnknownAttachmentAsStub(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClient(t, rt)
	assert.NoError(t, err)
	defer btc.Close()

	var doc1Body db.Body

	// Add doc1 and doc2
	req := rt.SendAdminRequest("PUT", "/db/doc1", `{}`)
	RequireStatus(t, req, http.StatusCreated)
	doc1Bytes := req.BodyBytes()

	require.NoError(t, rt.WaitForPendingChanges())

	err = json.Unmarshal(doc1Bytes, &doc1Body)
	assert.NoError(t, err)

	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	rev1ID := "1-ca9ad22802b66f662ff171f226211d5c"
	_, ok := btc.WaitForRev("doc1", rev1ID)
	require.True(t, ok)

	// force attachment into test client's store to validate it's fetched
	attachmentAData := base64.StdEncoding.EncodeToString([]byte("attachmentA"))
	contentType := "text/plain"
	length, digest, err := btc.saveAttachment(contentType, attachmentAData)
	require.NoError(t, err)

	// Update doc1, include reference to non-existing attachment with recent revpos
	revIDDoc1, err := btc.PushRev("doc1", rev1ID, []byte(fmt.Sprintf(`{"key": "val", "_attachments":{"attachment":{"digest":"%s","length":%d,"content_type":"%s","stub":true,"revpos":1}}}`, digest, length, contentType)))
	require.NoError(t, err)

	err = rt.WaitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)

	// verify that attachment exists on document and was persisted
	attResponse := rt.SendAdminRequest("GET", "/db/doc1/attachment", "")
	assert.Equal(t, 200, attResponse.Code)
	assert.Equal(t, "attachmentA", string(attResponse.BodyBytes()))

}

func TestMinRevPosWorkToAvoidUnnecessaryProveAttachment(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AllowConflicts: base.BoolPtr(true),
			},
		},
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	// Push an initial rev with attachment data
	initialRevID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"_attachments": map[string]interface{}{"hello.txt": map[string]interface{}{"data": "aGVsbG8gd29ybGQ="}}})
	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Replicate data to client and ensure doc arrives
	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	_, found := btc.WaitForRev("doc", initialRevID)
	assert.True(t, found)

	// Push a revision with a bunch of history simulating doc updated on mobile device
	// Note this references revpos 1 and therefore SGW has it - Shouldn't need proveAttachment
	proveAttachmentBefore := btc.pushReplication.replicationStats.ProveAttachment.Value()
	revid, err := btc.PushRevWithHistory("doc", initialRevID, []byte(`{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`), 25, 5)
	assert.NoError(t, err)
	proveAttachmentAfter := btc.pushReplication.replicationStats.ProveAttachment.Value()
	assert.Equal(t, proveAttachmentBefore, proveAttachmentAfter)

	// Push another bunch of history
	_, err = btc.PushRevWithHistory("doc", revid, []byte(`{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`), 25, 5)
	assert.NoError(t, err)
	proveAttachmentAfter = btc.pushReplication.replicationStats.ProveAttachment.Value()
	assert.Equal(t, proveAttachmentBefore, proveAttachmentAfter)
}
func TestAttachmentWithErroneousRevPos(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	// Create rev 1 with the hello.txt attachment
	revid := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"val": "val", "_attachments": map[string]interface{}{"hello.txt": map[string]interface{}{"data": "aGVsbG8gd29ybGQ="}}})
	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Pull rev and attachment down to client
	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	_, found := btc.WaitForRev("doc", revid)
	assert.True(t, found)

	// Add an attachment to client
	btc.AttachmentsLock().Lock()
	btc.Attachments()["sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc="] = []byte("goodbye cruel world")
	btc.AttachmentsLock().Unlock()

	// Put doc with an erroneous revpos 1 but with a different digest, referring to the above attachment
	_, err = btc.PushRevWithHistory("doc", revid, []byte(`{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"length": 19,"digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc="}}}`), 1, 0)
	require.NoError(t, err)

	// Ensure message and attachment is pushed up
	_, ok := btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	// Get the attachment and ensure the data is updated
	resp := rt.SendAdminRequest(http.MethodGet, "/db/doc/hello.txt", "")
	RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, "goodbye cruel world", string(resp.BodyBytes()))
}

// CBG-2004: Test that prove attachment over Blip works correctly when receiving a ErrAttachmentNotFound
func TestProveAttachmentNotFound(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	bt, err := NewBlipTesterFromSpecWithRT(t, nil, rt)
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	attachmentData := []byte("attachmentA")
	attachmentDataEncoded := base64.StdEncoding.EncodeToString(attachmentData)

	bt.blipContext.HandlerForProfile[db.MessageProveAttachment] = func(msg *blip.Message) {
		status, errMsg := base.ErrorAsHTTPStatus(db.ErrAttachmentNotFound)
		msg.Response().SetError("HTTP", status, errMsg)
	}

	// Handler for when full attachment is requested
	bt.blipContext.HandlerForProfile[db.MessageGetAttachment] = func(msg *blip.Message) {
		resp := msg.Response()
		resp.SetBody(attachmentData)
		resp.SetCompressed(msg.Properties[db.BlipCompress] == "true")
	}

	// Initial set up
	sent, _, _, err := bt.SendRev("doc1", "1-abc", []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentDataEncoded+`"}}}`), blip.Properties{})
	require.True(t, sent)
	require.NoError(t, err)

	err = rt.WaitForPendingChanges()
	require.NoError(t, err)

	// Should log:
	// "Peer sent prove attachment error 404 attachment not found, falling back to getAttachment for proof in doc <ud>doc1</ud> (digest sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=)"
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Use different attachment name to bypass digest check in ForEachStubAttachment() which skips prove attachment code
	// Set attachment to V2 so it can be retrieved by RT successfully
	sent, _, _, err = bt.SendRev("doc1", "2-abc", []byte(`{"key": "val", "_attachments":{"attach":{"digest":"sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=","length":11,"stub":true,"revpos":1,"ver":2}}}`), blip.Properties{})
	require.True(t, sent)
	require.NoError(t, err)

	err = rt.WaitForPendingChanges()
	require.NoError(t, err)
	// Check attachment is on the document
	body := rt.GetDoc("doc1")
	assert.Equal(t, "2-abc", body.ExtractRev())
	resp := rt.SendAdminRequest("GET", "/db/doc1/attach", "")
	RequireStatus(t, resp, 200)
	assert.EqualValues(t, attachmentData, resp.BodyBytes())
}

// Test adding / retrieving attachments
func TestAttachments(t *testing.T) {
	// TODO: Write tests to cover scenario
	t.Skip("not tested")
}

// Reproduces the issue seen in https://github.com/couchbase/couchbase-lite-core/issues/790
// Makes sure that Sync Gateway rejects attachments sent to it that does not match the given digest and/or length
func TestPutInvalidAttachment(t *testing.T) {

	tests := []struct {
		name                  string
		correctAttachmentBody string
		invalidAttachmentBody string
		expectedType          blip.MessageType
		expectedErrorCode     string
	}{
		{
			name:                  "truncated",
			correctAttachmentBody: "attach",
			invalidAttachmentBody: "att", // truncate so length and digest are incorrect
			expectedType:          blip.ErrorType,
			expectedErrorCode:     strconv.Itoa(http.StatusBadRequest),
		},
		{
			name:                  "malformed",
			correctAttachmentBody: "attach",
			invalidAttachmentBody: "attahc", // swap two chars so only digest doesn't match
			expectedType:          blip.ErrorType,
			expectedErrorCode:     strconv.Itoa(http.StatusBadRequest),
		},
		{
			name:                  "correct",
			correctAttachmentBody: "attach",
			invalidAttachmentBody: "attach",
			expectedType:          blip.ResponseType,
			expectedErrorCode:     "",
		},
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			digest := db.Sha1DigestKey([]byte(test.correctAttachmentBody))

			// Send revision with attachment
			input := SendRevWithAttachmentInput{
				docId:            test.name,
				revId:            "1-rev1",
				attachmentName:   "myAttachment",
				attachmentLength: len(test.correctAttachmentBody),
				attachmentBody:   test.invalidAttachmentBody,
				attachmentDigest: digest,
			}
			sent, _, resp := bt.SendRevWithAttachment(input)
			assert.True(t, sent)

			// Make sure we get the expected response back
			assert.Equal(t, test.expectedType, resp.Type())
			if test.expectedErrorCode != "" {
				assert.Equal(t, test.expectedErrorCode, resp.Properties["Error-Code"])
			}

			respBody, err := resp.Body()
			assert.NoError(t, err)
			t.Logf("resp.Body: %v", string(respBody))
		})
	}
}

// TestCBLRevposHandling mimics CBL 2.x's revpos handling (setting incoming revpos to the incoming generation).  Test
// validates that proveAttachment isn't being invoked when the attachment is already present and the
// digest doesn't change, regardless of revpos.
func TestCBLRevposHandling(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClient(t, rt)
	assert.NoError(t, err)
	defer btc.Close()

	var doc1Body db.Body
	var doc2Body db.Body

	// Add doc1 and doc2
	req := rt.SendAdminRequest("PUT", "/db/doc1", `{}`)
	RequireStatus(t, req, http.StatusCreated)
	doc1Bytes := req.BodyBytes()
	req = rt.SendAdminRequest("PUT", "/db/doc2", `{}`)
	RequireStatus(t, req, http.StatusCreated)
	doc2Bytes := req.BodyBytes()

	require.NoError(t, rt.WaitForPendingChanges())

	err = json.Unmarshal(doc1Bytes, &doc1Body)
	assert.NoError(t, err)
	err = json.Unmarshal(doc2Bytes, &doc2Body)
	assert.NoError(t, err)

	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok := btc.WaitForRev("doc1", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)
	_, ok = btc.WaitForRev("doc2", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)

	attachmentAData := base64.StdEncoding.EncodeToString([]byte("attachmentA"))
	attachmentBData := base64.StdEncoding.EncodeToString([]byte("attachmentB"))

	revIDDoc1, err := btc.PushRev("doc1", doc1Body["rev"].(string), []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentAData+`"}}}`))
	require.NoError(t, err)
	revIDDoc2, err := btc.PushRev("doc2", doc2Body["rev"].(string), []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentBData+`"}}}`))
	require.NoError(t, err)

	err = rt.WaitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)
	err = rt.WaitForRev("doc2", revIDDoc2)
	assert.NoError(t, err)

	_, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "doc1", db.DocUnmarshalAll)
	_, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "doc2", db.DocUnmarshalAll)

	// Update doc1, don't change attachment, use correct revpos
	revIDDoc1, err = btc.PushRev("doc1", revIDDoc1, []byte(`{"key": "val", "_attachments":{"attachment":{"digest":"sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=","length":11,"content_type":"","stub":true,"revpos":2}}}`))
	require.NoError(t, err)

	err = rt.WaitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)

	// Update doc1, don't change attachment, use revpos=generation of revid, as CBL 2.x does.  Should not proveAttachment on digest match.
	revIDDoc1, err = btc.PushRev("doc1", revIDDoc1, []byte(`{"key": "val", "_attachments":{"attachment":{"digest":"sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=","length":11,"content_type":"","stub":true,"revpos":4}}}`))
	require.NoError(t, err)

	// Validate attachment exists
	attResponse := rt.SendAdminRequest("GET", "/db/doc1/attachment", "")
	assert.Equal(t, 200, attResponse.Code)
	assert.Equal(t, "attachmentA", string(attResponse.BodyBytes()))

	attachmentPushCount := rt.GetDatabase().DbStats.CBLReplicationPushStats.AttachmentPushCount.Value()
	// Update doc1, change attachment digest with CBL revpos=generation.  Should getAttachment
	_, err = btc.PushRev("doc1", revIDDoc1, []byte(`{"key": "val", "_attachments":{"attachment":{"digest":"sha1-SKk0IV40XSHW37d3H0xpv2+z9Ck=","length":11,"content_type":"","stub":true,"revpos":5}}}`))
	require.NoError(t, err)

	// Validate attachment exists and is updated
	attResponse = rt.SendAdminRequest("GET", "/db/doc1/attachment", "")
	assert.Equal(t, 200, attResponse.Code)
	assert.Equal(t, "attachmentB", string(attResponse.BodyBytes()))

	attachmentPushCountAfter := rt.GetDatabase().DbStats.CBLReplicationPushStats.AttachmentPushCount.Value()
	assert.Equal(t, attachmentPushCount+1, attachmentPushCountAfter)

}

// Helper_Functions
func CreateDocWithLegacyAttachment(t *testing.T, rt *RestTester, docID string, rawDoc []byte, attKey string, attBody []byte) {
	// Write attachment directly to the datastore.
	dataStore := rt.GetSingleTestDataStore()
	_, err := dataStore.Add(attKey, 0, attBody)
	require.NoError(t, err)

	body := db.Body{}
	err = body.Unmarshal(rawDoc)
	require.NoError(t, err, "Error unmarshalling body")

	// Write raw document to the datastore.
	_, err = dataStore.Add(docID, 0, rawDoc)
	require.NoError(t, err)

	// Migrate document metadata from document body to system xattr.
	attachments := retrieveAttachmentMeta(t, rt, docID)
	require.Len(t, attachments, 1)
}

func retrieveAttachmentMeta(t *testing.T, rt *RestTester, docID string) (attMeta map[string]interface{}) {
	body := rt.GetDoc(docID)
	attachments, ok := body["_attachments"].(map[string]interface{})
	require.True(t, ok)
	return attachments
}

func rawDocWithAttachmentAndSyncMeta() []byte {
	return []byte(`{
   "_sync": {
      "rev": "1-5fc93bd36377008f96fdae2719c174ed",
      "sequence": 2,
      "recent_sequences": [
         2
      ],
      "history": {
         "revs": [
            "1-5fc93bd36377008f96fdae2719c174ed"
         ],
         "parents": [
            -1
         ],
         "channels": [
            null
         ]
      },
      "cas": "",
      "attachments": {
         "hi.txt": {
            "revpos": 1,
            "content_type": "text/plain",
            "length": 2,
            "stub": true,
            "digest": "sha1-witfkXg0JglCjW9RssWvTAveakI="
         }
      },
      "time_saved": "2021-09-01T17:33:03.054227821Z"
   },
  "key": "value"
}`)
}

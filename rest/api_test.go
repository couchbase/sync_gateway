//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/walrus"
	"github.com/robertkrimen/otto/underscore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

// ////// REST TESTER HELPER CLASS:

// ////// AND NOW THE TESTS:

func TestRoot(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendRequest("GET", "/", "")
	assertStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "Welcome", body["couchdb"])

	response = rt.SendRequest("HEAD", "/", "")
	assertStatus(t, response, 200)
	response = rt.SendRequest("OPTIONS", "/", "")
	assertStatus(t, response, 204)
	assert.Equal(t, "GET, HEAD", response.Header().Get("Allow"))
	response = rt.SendRequest("PUT", "/", "")
	assertStatus(t, response, 405)
	assert.Equal(t, "GET, HEAD", response.Header().Get("Allow"))
}

func TestDBRoot(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest("GET", "/db/", "")
	assertStatus(t, response, 200)
	var body db.Body
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)

	assert.Equal(t, "db", body["db_name"])
	assert.Equal(t, "Online", body["state"])

	response = rt.SendRequest("HEAD", "/db/", "")
	assertStatus(t, response, 200)
	response = rt.SendRequest("OPTIONS", "/db/", "")
	assertStatus(t, response, 204)
	assert.Equal(t, "GET, HEAD, POST, PUT", response.Header().Get("Allow"))
}

func TestDisablePublicBasicAuth(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DisablePasswordAuth: true,
				Guest: &db.PrincipalConfig{
					Disabled: base.BoolPtr(true),
				},
			},
		},
	})
	defer rt.Close()

	response := rt.SendRequest(http.MethodGet, "/db/", "")
	assertStatus(t, response, http.StatusUnauthorized)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	// Double-check that even if we provide valid credentials we still won't be let in
	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	user, err := a.NewUser("user1", "letmein", channels.SetOf(t, "foo"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	response = rt.Send(requestByUser(http.MethodGet, "/db/", "", "user1"))
	assertStatus(t, response, http.StatusUnauthorized)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	// Also check that we can't create a session through POST /db/_session
	response = rt.SendRequest(http.MethodPost, "/db/_session", `{"name":"user1","password":"letmein"}`)
	assertStatus(t, response, http.StatusUnauthorized)

	// As a sanity check, ensure it does work when the setting is disabled
	rt.ServerContext().Database("db").Options.DisablePasswordAuthentication = false
	response = rt.Send(requestByUser(http.MethodGet, "/db/", "", "user1"))
	assertStatus(t, response, http.StatusOK)

	response = rt.SendRequest(http.MethodPost, "/db/_session", `{"name":"user1","password":"letmein"}`)
	assertStatus(t, response, http.StatusOK)
}

func (rt *RestTester) createDoc(t *testing.T, docid string) string {
	response := rt.SendAdminRequest("PUT", "/db/"+docid, `{"prop":true}`)
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revid := body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}
	return revid
}

func TestDocLifecycle(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	revid := rt.createDoc(t, "doc")
	assert.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revid)

	response := rt.SendAdminRequest("DELETE", "/db/doc?rev="+revid, "")
	assertStatus(t, response, 200)
}

// Validate that Etag header value is surrounded with double quotes, see issue #808
func TestDocEtag(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/doc", `{"prop":true}`)
	assertStatus(t, response, 201)
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
	assertStatus(t, response, 200)

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
	assertStatus(t, response, 201)

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
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.Equal(t, "", response.Header().Get("Content-Disposition"))
	assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))

	// Validate Etag returned from retrieving an attachment
	assert.Equal(t, "\"sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=\"", response.Header().Get("Etag"))

}

// Add and retrieve an attachment, including a subrange
func TestDocAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest("PUT", "/db/doc", `{"prop":true}`)
	assertStatus(t, response, 201)
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
	assertStatus(t, response, 201)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc/attach1", "")
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.Equal(t, "bytes", response.Header().Get("Accept-Ranges"))
	assert.Equal(t, "", response.Header().Get("Content-Disposition"))
	assert.Equal(t, "30", response.Header().Get("Content-Length"))
	assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))

	// retrieve subrange
	response = rt.SendRequestWithHeaders("GET", "/db/doc/attach1", "", map[string]string{"Range": "bytes=5-6"})
	assertStatus(t, response, 206)
	assert.Equal(t, "is", string(response.Body.Bytes()))
	assert.Equal(t, "bytes", response.Header().Get("Accept-Ranges"))
	assert.Equal(t, "2", response.Header().Get("Content-Length"))
	assert.Equal(t, "bytes 5-6/30", response.Header().Get("Content-Range"))
	assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))
}

func TestDocAttachmentMetaOption(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest(http.MethodPut, "/db/doc", `{"prop":true}`)
	assertStatus(t, response, http.StatusCreated)
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
		assertStatus(t, response, http.StatusOK)
		assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
		assert.Equal(t, "bytes", response.Header().Get("Accept-Ranges"))
		assert.Empty(t, response.Header().Get("Content-Disposition"))
		assert.Equal(t, "30", response.Header().Get("Content-Length"))
		assert.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))
	}

	// Attach to existing document.
	response = rt.SendRequestWithHeaders(http.MethodPut, "/db/doc/attach1?rev="+revid, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/doc/attach1", "")
	assertAttachmentResponse(response)

	// Retrieve attachment meta only by explicitly enabling meta option.
	response = rt.SendRequest(http.MethodGet, "/db/doc/attach1?meta=true", "")
	assertStatus(t, response, http.StatusOK)

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

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", channels.SetOf(t, "foo"))
	assert.NoError(t, a.Save(user))

	response := rt.Send(requestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user1"))
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	// Put new revision removing document from users channel set
	response = rt.Send(requestByUser("PUT", "/db/doc?rev="+revid, `{"prop":true}`, "user1"))
	assertStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
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
	rt := NewRestTester(t, nil)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", channels.SetOf(t, "foo"))
	assert.NoError(t, a.Save(user))
	// Create document
	response := rt.Send(requestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user1"))
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	// Put new revision with null body
	response = rt.Send(requestByUser("PUT", "/db/doc?rev="+revid, "", "user1"))
	assertStatus(t, response, 400)
}

func TestFunkyDocIDs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.createDoc(t, "AC%2FDC")

	response := rt.SendAdminRequest("GET", "/db/AC%2FDC", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC+DC")
	response = rt.SendAdminRequest("GET", "/db/AC+DC", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC+DC+GC")
	response = rt.SendAdminRequest("GET", "/db/AC+DC+GC", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/db/foo+bar+moo+car", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/foo+bar+moo+car", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC%2BDC2")
	response = rt.SendAdminRequest("GET", "/db/AC%2BDC2", "")
	assertStatus(t, response, 200)

	rt.createDoc(t, "AC%2BDC%2BGC2")
	response = rt.SendAdminRequest("GET", "/db/AC%2BDC%2BGC2", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/db/foo%2Bbar%2Bmoo%2Bcar2", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/foo%2Bbar%2Bmoo%2Bcar2", "")
	assertStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/db/foo%2Bbar+moo%2Bcar3", `{"prop":true}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/foo+bar%2Bmoo+car3", "")
	assertStatus(t, response, 200)
}

func TestFunkyUsernames(t *testing.T) {
	cases := []struct {
		Name     string
		UserName string
	}{
		{
			Name:     "hashes",
			UserName: "foo#bar",
		},
		{
			Name:     "question mark",
			UserName: "foo?bar",
		},
		{
			Name:     "dollars",
			UserName: "$foo$bar",
		},
		{
			Name:     "underscore prefix",
			UserName: "_sync-foobar",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			require.Truef(t, auth.IsValidPrincipalName(tc.UserName), "expected '%s' to be accepted", tc.UserName)
			rt := NewRestTester(t, nil)
			defer rt.Close()

			a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))

			// Create a test user
			user, err := a.NewUser(tc.UserName, "letmein", channels.SetOf(t, "foo"))
			require.NoError(t, err)
			require.NoError(t, a.Save(user))

			response := rt.Send(requestByUser("PUT", "/db/AC+DC", `{"foo":"bar", "channels": ["foo"]}`, tc.UserName))
			assertStatus(t, response, 201)

			response = rt.Send(requestByUser("GET", "/db/_all_docs", "", tc.UserName))
			assertStatus(t, response, 200)

			response = rt.Send(requestByUser("GET", "/db/AC+DC", "", tc.UserName))
			assertStatus(t, response, 200)
			var overlay struct {
				Rev string `json:"_rev"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &overlay))

			response = rt.Send(requestByUser("DELETE", "/db/AC+DC?rev="+overlay.Rev, "", tc.UserName))
			assertStatus(t, response, 200)
		})
	}
}

func TestFunkyRoleNames(t *testing.T) {
	cases := []struct {
		Name     string
		RoleName string
	}{
		{
			Name:     "hashes",
			RoleName: "foo#bar",
		},
		{
			Name:     "question mark",
			RoleName: "foo?bar",
		},
		{
			Name:     "dollars",
			RoleName: "$foo$bar",
		},
		{
			Name:     "underscore prefix",
			RoleName: "_sync-foobar",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			require.Truef(t, auth.IsValidPrincipalName(tc.RoleName), "expected '%s' to be accepted", tc.RoleName)
			roleNameJSON, err := json.Marshal("role:" + tc.RoleName)
			require.NoError(t, err)
			const username = "user1"
			syncFn := fmt.Sprintf(`function(doc) {channel(doc.channels); role("%s", %s);}`, username, string(roleNameJSON))
			rt := NewRestTester(t, &RestTesterConfig{
				SyncFn: syncFn,
			})
			defer rt.Close()

			a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))

			// Create a test user
			user, err := a.NewUser(username, "letmein", channels.SetOf(t))
			require.NoError(t, err)
			require.NoError(t, a.Save(user))

			// Create role
			response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_role/%s", url.PathEscape(tc.RoleName)), `{"admin_channels": ["testchannel"]}`)
			assertStatus(t, response, 201)

			// Create test document
			response = rt.SendAdminRequest("PUT", "/db/testdoc", `{"channels":["testchannel"]}`)
			assertStatus(t, response, 201)

			// Assert user can access it
			response = rt.Send(requestByUser("GET", "/db/testdoc", "", username))
			assertStatus(t, response, 200)
		})
	}
}

func TestFunkyDocAndAttachmentIDs(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
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
		assertStatus(t, response, http.StatusCreated)
		return requireRevID(response)
	}

	// assertResponse asserts that the specified attachment exists in the response body.
	assertResponse := func(response *TestResponse, attachmentBody string) {
		assertStatus(t, response, http.StatusOK)
		require.Equal(t, attachmentBody, string(response.Body.Bytes()))
		require.Empty(t, response.Header().Get("Content-Disposition"))
		require.Equal(t, attachmentContentType, response.Header().Get("Content-Type"))
	}

	// Create document with simple name
	doc1revId := rt.createDoc(t, "doc1")

	// Add attachment with single embedded '/' (%2F HEX)
	resource := "/db/doc1/attachpath%2Fattachment.txt?rev=" + doc1revId
	response := rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)
	revIdAfterAttachment := requireRevID(response)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/doc1/attachpath%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Add attachment with two embedded '/' (%2F HEX)
	resource = "/db/doc1/attachpath%2Fattachpath2%2Fattachment.txt?rev=" + revIdAfterAttachment
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/doc1/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Create Doc with embedded '/' (%2F HEX) in name
	doc1revId = createDoc("AC%2FDC")

	response = rt.SendRequest(http.MethodGet, "/db/AC%2FDC", "")
	assertStatus(t, response, http.StatusOK)

	// Add attachment with single embedded '/' (%2F HEX)
	response = rt.SendRequestWithHeaders(http.MethodPut, "/db/AC%2FDC/attachpath%2Fattachment.txt?rev="+doc1revId,
		attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)
	revIdAfterAttachment = requireRevID(response)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2FDC/attachpath%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Add attachment with two embedded '/' (%2F HEX)
	resource = "/db/AC%2FDC/attachpath%2Fattachpath2%2Fattachment.txt?rev=" + revIdAfterAttachment
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2FDC/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Create Doc with embedded '+' (%2B HEX) in name
	doc1revId = createDoc("AC%2BDC%2BGC2")
	response = rt.SendRequest(http.MethodGet, "/db/AC%2BDC%2BGC2", "")
	assertStatus(t, response, http.StatusOK)

	// Add attachment with single embedded '/' (%2F HEX)
	resource = "/db/AC%2BDC%2BGC2/attachpath%2Fattachment.txt?rev=" + doc1revId
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)
	revIdAfterAttachment = requireRevID(response)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2BDC%2BGC2/attachpath%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)

	// Add attachment with two embedded '/' (%2F HEX)
	resource = "/db/AC%2BDC%2BGC2/attachpath%2Fattachpath2%2Fattachment.txt?rev=" + revIdAfterAttachment
	response = rt.SendRequestWithHeaders(http.MethodPut, resource, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	// Retrieve attachment
	response = rt.SendRequest(http.MethodGet, "/db/AC%2BDC%2BGC2/attachpath%2Fattachpath2%2Fattachment.txt", "")
	assertResponse(response, attachmentBody)
}

func TestCORSOrigin(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}
	response := rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))

	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))

	// now test another origin in config
	reqHeaders = map[string]string{
		"Origin": "http://staging.example.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equal(t, "http://staging.example.com", response.Header().Get("Access-Control-Allow-Origin"))

	// test no header on _admin apis
	reqHeaders = map[string]string{
		"Origin": "http://example.com",
	}
	response = rt.SendAdminRequestWithHeaders("GET", "/db/_all_docs", "", reqHeaders)
	assert.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))

	// test with a config without * should reject non-matches
	sc := rt.ServerContext()
	sc.config.API.CORS.Origin = []string{"http://example.com", "http://staging.example.com"}
	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
}

// assertGatewayStatus is like assertStatus but with StatusGatewayTimeout error checking for temporary network failures.
func assertGatewayStatus(t *testing.T, response *TestResponse, expected int) {
	if response.Code == http.StatusGatewayTimeout {
		respBody := response.Body.String()
		t.Skipf("WARNING: Host could not be reached: %s", respBody)
	}
	assertStatus(t, response, expected)
}

func TestCORSLoginOriginOnSessionPost(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", "{\"name\":\"jchris\",\"password\":\"secret\"}", reqHeaders)
	assertStatus(t, response, 401)

	response = rt.SendRequestWithHeaders("POST", "/db/_facebook", `{"access_token":"true"}`, reqHeaders)
	assertGatewayStatus(t, response, 401)
}

// #issue 991
func TestCORSLoginOriginOnSessionPostNoCORSConfig(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	// Set CORS to nil
	sc := rt.ServerContext()
	sc.config.API.CORS = nil

	response := rt.SendRequestWithHeaders("POST", "/db/_session", `{"name":"jchris","password":"secret"}`, reqHeaders)
	assertStatus(t, response, 400)
}

func TestNoCORSOriginOnSessionPost(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://staging.example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", "{\"name\":\"jchris\",\"password\":\"secret\"}", reqHeaders)
	assertStatus(t, response, 400)

	response = rt.SendRequestWithHeaders("POST", "/db/_facebook", `{"access_token":"true"}`, reqHeaders)
	assertGatewayStatus(t, response, 400)
}

func TestCORSLogoutOriginOnSessionDelete(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 404)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "no session", body["reason"])
}

func TestCORSLogoutOriginOnSessionDeleteNoCORSConfig(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	// Set CORS to nil
	sc := rt.ServerContext()
	sc.config.API.CORS = nil

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 400)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "No CORS", body["reason"])
}

func TestNoCORSOriginOnSessionDelete(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://staging.example.com",
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	assertStatus(t, response, 400)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "No CORS", body["reason"])
}

func TestManualAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
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
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}
	assert.True(t, revIdAfterAttachment != doc1revId)

	// retrieve attachment
	response = rt.SendRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Disposition") == "")
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// retrieve attachment as admin should have
	// Content-disposition: attachment
	response = rt.SendAdminRequest("GET", "/db/doc1/attach1", "")
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Disposition") == `attachment`)
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// try to overwrite that attachment
	attachmentBody = "updated content"
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+revIdAfterAttachment, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
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
	reqHeaders["If-Match"] = revIdAfterUpdateAttachment
	response = rt.SendRequestWithHeaders("PUT", "/db/doc1/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
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
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// add another attachment to the document
	// also no explicit Content-Type header on this one
	// should default to application/octet-stream
	attachmentBody = "separate content"
	response = rt.SendRequest("PUT", "/db/doc1/attach2?rev="+revIdAfterUpdateAttachmentAgain, attachmentBody)
	assertStatus(t, response, 201)
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
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Type") == "application/octet-stream")

	// now check the attachments index on the document
	response = rt.SendRequest("GET", "/db/doc1", "")
	assertStatus(t, response, 200)
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
	assertStatus(t, response, 409)

	// attach to new document using bogus rev using If-Match header (should fail)
	reqHeaders["If-Match"] = "1-xyz"
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 409)
	delete(reqHeaders, "If-Match")

	// attach to new document without any rev (should succeed)
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/notexistyet/attach1", attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revIdAfterAttachment := body["rev"].(string)
	if revIdAfterAttachment == "" {
		t.Fatalf("No revid in response for PUT attachment")
	}

	// retrieve attachment
	response = rt.SendAdminRequest("GET", "/db/notexistyet/attach1", "")
	assertStatus(t, response, 200)
	assert.Equal(t, attachmentBody, string(response.Body.Bytes()))
	assert.True(t, response.Header().Get("Content-Type") == attachmentContentType)

	// now check the document
	body = db.Body{}
	response = rt.SendAdminRequest("GET", "/db/notexistyet", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	// body should only have 3 top-level entries _id, _rev, _attachments
	assert.True(t, len(body) == 3)
}

func TestBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}, {"_id": "_local/bulk3", "n": 3}]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	var docs []interface{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Len(t, docs, 3)
	assert.Equal(t, map[string]interface{}{"rev": "1-50133ddd8e49efad34ad9ecae4cb9907", "id": "bulk1"}, docs[0])

	response = rt.SendAdminRequest("GET", "/db/bulk1", "")
	assertStatus(t, response, 200)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "bulk1", respBody[db.BodyId])
	assert.Equal(t, "1-50133ddd8e49efad34ad9ecae4cb9907", respBody[db.BodyRev])
	assert.Equal(t, float64(1), respBody["n"])
	assert.Equal(t, map[string]interface{}{"rev": "1-035168c88bd4b80fb098a8da72f881ce", "id": "bulk2"}, docs[1])
	assert.Equal(t, map[string]interface{}{"rev": "0-1", "id": "_local/bulk3"}, docs[2])

	response = rt.SendAdminRequest("GET", "/db/_local/bulk3", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/bulk3", respBody[db.BodyId])
	assert.Equal(t, "0-1", respBody[db.BodyRev])
	assert.Equal(t, float64(3), respBody["n"])

	// update all documents
	input = `{"docs": [{"_id": "bulk1", "_rev" : "1-50133ddd8e49efad34ad9ecae4cb9907", "n": 10}, {"_id": "bulk2", "_rev":"1-035168c88bd4b80fb098a8da72f881ce", "n": 20}, {"_id": "_local/bulk3","_rev":"0-1","n": 30}]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Len(t, docs, 3)
	assert.Equal(t, map[string]interface{}{"rev": "2-7e384b16e63ee3218349ee568f156d6f", "id": "bulk1"}, docs[0])

	response = rt.SendAdminRequest("GET", "/db/_local/bulk3", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/bulk3", respBody[db.BodyId])
	assert.Equal(t, "0-2", respBody[db.BodyRev])
	assert.Equal(t, float64(30), respBody["n"])
}

func TestBulkDocsIDGeneration(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs": [{"n": 1}, {"_id": 123, "n": 2}]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	var docs []map[string]string
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	log.Printf("response: %s", response.Body.Bytes())
	assertStatus(t, response, 201)
	assert.Equal(t, 2, len(docs))
	assert.Equal(t, "1-50133ddd8e49efad34ad9ecae4cb9907", docs[0]["rev"])
	assert.True(t, docs[0]["id"] != "")
	assert.Equal(t, "1-035168c88bd4b80fb098a8da72f881ce", docs[1]["rev"])
	assert.True(t, docs[1]["id"] != "")
}

/*
func TestBulkDocsUnusedSequences(t *testing.T) {

	//We want a sync function that will reject some docs
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, err := rt.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev.Sequence, uint64(1))

	doc3Rev, err := rt.GetDatabase().GetDocSyncData("bulk3")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, err := rt.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(3))

	//send another _bulk_docs and validate the sequences used
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23}]}`
	response = rt.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 get used here
	doc21Rev, err := rt.GetDatabase().GetDocSyncData("bulk21")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc21Rev.Sequence, uint64(3))

	doc22Rev, err := rt.GetDatabase().GetDocSyncData("bulk22")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc22Rev.Sequence, uint64(4))

	doc23Rev, err := rt.GetDatabase().GetDocSyncData("bulk23")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc23Rev.Sequence, uint64(5))

	//Get current sequence number
	lastSequence, err = rt.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(5))
}

func TestBulkDocsUnusedSequencesMultipleSG(t *testing.T) {

	//We want a sync function that will reject some docs, create two to simulate two SG instances
	rtConfig1 := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt1 := NewRestTester(t, &rtConfig1)
	defer rt1.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev.Sequence, uint64(1))

	doc3Rev, err := rt1.GetDatabase().GetDocSyncData("bulk3")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, err := rt1.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(3))

	rtConfig2 := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt2 := NewRestTesterWithBucket(t, &rtConfig2, rt1.RestTesterBucket)
	defer rt2.Close()

	rt2.RestTesterServerContext = NewServerContext(&LegacyServerConfig{
		Facebook:       &FacebookConfigLegacy{},
		AdminInterface: &DefaultAdminInterface,
	})

	// For the second rest tester, create a copy of the original database config and
	// clear out the sync function.
	dbConfigCopy, err := rt1.DatabaseConfig.DeepCopy()
	assert.NoError(t, err, "Unexpected error")
	dbConfigCopy.Sync = base.StringPtr("")

	// Add a second database that uses the same underlying bucket.
	_, err = rt2.RestTesterServerContext.AddDatabaseFromConfig(dbConfigCopy)

	assert.NoError(t, err, "Failed to add database to rest tester")

	//send another _bulk_docs to rt2 and validate the sequences used
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23}]}`
	response = rt2.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 does not get used here as its using a different sequence allocator
	doc21Rev, err := rt2.GetDatabase().GetDocSyncData("bulk21")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc21Rev.Sequence, uint64(4))

	doc22Rev, err := rt2.GetDatabase().GetDocSyncData("bulk22")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc22Rev.Sequence, uint64(5))

	doc23Rev, err := rt2.GetDatabase().GetDocSyncData("bulk23")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc23Rev.Sequence, uint64(6))

	//Get current sequence number
	lastSequence, err = rt2.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(6))

	//Now send a bulk_doc to rt1 and see if it uses sequence 3
	input = `{"docs": [{"_id": "bulk31", "n": 31}, {"_id": "bulk32", "n": 32}, {"_id": "bulk33", "n": 33}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 get used here as its using first sequence allocator
	doc31Rev, err := rt1.GetDatabase().GetDocSyncData("bulk31")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc31Rev.Sequence, uint64(3))

	doc32Rev, err := rt1.GetDatabase().GetDocSyncData("bulk32")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc32Rev.Sequence, uint64(7))

	doc33Rev, err := rt1.GetDatabase().GetDocSyncData("bulk33")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc33Rev.Sequence, uint64(8))

}

func TestBulkDocsUnusedSequencesMultiRevDoc(t *testing.T) {

	//We want a sync function that will reject some docs, create two to simulate two SG instances
	rtConfig1 := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt1 := NewRestTester(t, &rtConfig1)
	defer rt1.Close()

	//add new docs, doc2 will be rejected by sync function
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev.Sequence, uint64(1))

	//Get the revID for doc "bulk1"
	doc1RevID := doc1Rev.CurrentRev

	doc3Rev, err := rt1.GetDatabase().GetDocSyncData("bulk3")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, err := rt1.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(3))

	rtConfig2 := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt2 := NewRestTesterWithBucket(t, &rtConfig2, rt1.RestTesterBucket)
	defer rt2.Close()

	rt2.RestTesterServerContext = NewServerContext(&LegacyServerConfig{
		Facebook:       &FacebookConfigLegacy{},
		AdminInterface: &DefaultAdminInterface,
	})

	// For the second rest tester, create a copy of the original database config and
	// clear out the sync function.
	dbConfigCopy, err := rt1.DatabaseConfig.DeepCopy()
	assert.NoError(t, err, "Unexpected error calling DeepCopy()")
	dbConfigCopy.Sync = base.StringPtr("")

	// Add a second database that uses the same underlying bucket.
	_, err = rt2.RestTesterServerContext.AddDatabaseFromConfig(dbConfigCopy)

	assert.NoError(t, err, "Failed to add database to rest tester")

	//send another _bulk_docs to rt2, including an update to doc "bulk1" and validate the sequences used
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23}, {"_id": "bulk1", "_rev": "` + doc1RevID + `", "n": 2}]}`
	response = rt2.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 does not get used here as its using a different sequence allocator
	doc21Rev, err := rt2.GetDatabase().GetDocSyncData("bulk21")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc21Rev.Sequence, uint64(4))

	doc22Rev, err := rt2.GetDatabase().GetDocSyncData("bulk22")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc22Rev.Sequence, uint64(5))

	doc23Rev, err := rt2.GetDatabase().GetDocSyncData("bulk23")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc23Rev.Sequence, uint64(6))

	//Validate rev2 of doc "bulk1" has a new revision
	doc1Rev2, err := rt2.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev2.Sequence, uint64(7))

	//Get the revID for doc "bulk1"
	doc1RevID2 := doc1Rev2.CurrentRev

	//Get current sequence number
	lastSequence, err = rt2.GetDatabase().LastSequence()
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, lastSequence, uint64(7))

	//Now send a bulk_doc to rt1 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID2 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 8 should get used here as sequence 3 should have been dropped by the first sequence allocator
	doc1Rev3, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev3.Sequence, uint64(8))

	//validate the doc _sync metadata, should see last sequence lower than previous sequence
	rs := doc1Rev3.RecentSequences
	goassert.Equals(t, len(rs), 3)
	goassert.Equals(t, rs[0], uint64(1))
	goassert.Equals(t, rs[1], uint64(7))
	goassert.Equals(t, rs[2], uint64(8))

}

func TestBulkDocsUnusedSequencesMultiRevDoc2SG(t *testing.T) {

	//We want a sync function that will reject some docs, create two to simulate two SG instances
	rtConfig1 := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt1 := NewRestTester(t, &rtConfig1)
	defer rt1.Close()

	//add new docs, doc2 will be rejected by sync function
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2, "type": "invalid"}, {"_id": "bulk3", "n": 3}]}`
	response := rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	doc1Rev, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev.Sequence, uint64(1))

	//Get the revID for doc "bulk1"
	doc1RevID := doc1Rev.CurrentRev

	doc3Rev, err := rt1.GetDatabase().GetDocSyncData("bulk3")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc3Rev.Sequence, uint64(2))

	//Get current sequence number, this will be 3, as SG allocates enough sequences to process all bulk docs
	lastSequence, err := rt1.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(3))

	rtConfig2 := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "invalid") {throw("Rejecting invalid doc")}}`}
	rt2 := NewRestTesterWithBucket(t, &rtConfig2, rt1.RestTesterBucket)
	defer rt2.Close()

	rt2.RestTesterServerContext = NewServerContext(&LegacyServerConfig{
		Facebook:       &FacebookConfigLegacy{},
		AdminInterface: &DefaultAdminInterface,
	})

	// For the second rest tester, create a copy of the original database config and
	// clear out the sync function.
	dbConfigCopy, err := rt1.DatabaseConfig.DeepCopy()
	assert.NoError(t, err, "Unexpected error calling DeepCopy()")
	dbConfigCopy.Sync = base.StringPtr("")

	// Add a second database that uses the same underlying bucket.
	_, err = rt2.RestTesterServerContext.AddDatabaseFromConfig(dbConfigCopy)

	assert.NoError(t, err, "Failed to add database to rest tester")

	//send another _bulk_docs to rt2, including an update to doc "bulk1" and another invalid rev to create an unused sequence
	input = `{"docs": [{"_id": "bulk21", "n": 21}, {"_id": "bulk22", "n": 22}, {"_id": "bulk23", "n": 23, "type": "invalid"}, {"_id": "bulk1", "_rev": "` + doc1RevID + `", "n": 2}]}`
	response = rt2.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 3 does not get used here as its using a different sequence allocator
	doc21Rev, err := rt2.GetDatabase().GetDocSyncData("bulk21")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc21Rev.Sequence, uint64(4))

	doc22Rev, err := rt2.GetDatabase().GetDocSyncData("bulk22")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc22Rev.Sequence, uint64(5))

	//Validate rev2 of doc "bulk1" has a new revision
	doc1Rev2, err := rt2.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev2.Sequence, uint64(7))

	//Get the revID for doc "bulk1"
	doc1RevID2 := doc1Rev2.CurrentRev

	//Get current sequence number
	lastSequence, err = rt2.GetDatabase().LastSequence()
	assert.NoError(t, err, "LastSequence error")
	goassert.Equals(t, lastSequence, uint64(7))

	//Now send a bulk_doc to rt1 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID2 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 8 should get used here as sequence 3 should have been dropped by the first sequence allocator
	doc1Rev3, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev3.Sequence, uint64(8))

	//Get the revID for doc "bulk1"
	doc1RevID3 := doc1Rev3.CurrentRev

	//Now send a bulk_doc to rt2 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID3 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	//Sequence 9 should get used here as sequence 6 should have been dropped by the second sequence allocator
	doc1Rev4, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev4.Sequence, uint64(9))

	//validate the doc _sync metadata, should see last sequence lower than previous sequence
	rs := doc1Rev4.RecentSequences
	goassert.Equals(t, len(rs), 4)
	goassert.Equals(t, rs[0], uint64(1))
	goassert.Equals(t, rs[1], uint64(7))
	goassert.Equals(t, rs[2], uint64(8))
	goassert.Equals(t, rs[3], uint64(9))

}
*/

func TestBulkDocsEmptyDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 400)
}

func TestBulkDocsMalformedDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs":["A","B"]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 400)

	// For non-string id, ensure it reverts to id generation and doesn't panic
	input = `{"docs": [{"_id": 3, "n": 1}]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	log.Printf("response:%s", response.Body.Bytes())
	assertStatus(t, response, 201)
}

// TestBulkGetEfficientBodyCompression makes sure that the multipart writer of the bulk get response is efficiently compressing the document bodies.
// This is to catch a case where document bodies are marshalled with random property ordering, and reducing compression ratio between multiple doc body instances.
func TestBulkGetEfficientBodyCompression(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	const (
		numDocs = 300
		doc     = docSample20k

		// Since all docs are identical, a very high rate of compression is expected
		minCompressionRatio = 85
		maxCompressionRatio = 100
		minUncompressedSize = len(doc) * numDocs // ~6000 KB - actually larger due to _bulk_get overhead

		docKeyPrefix = "doc-"
	)

	// create a bunch of identical large-ish docs
	for i := 0; i < numDocs; i++ {
		resp := rt.SendAdminRequest(http.MethodPut,
			"/db/"+docKeyPrefix+strconv.Itoa(i), doc)
		assertStatus(t, resp, http.StatusCreated)
	}

	// craft a _bulk_get body to get all of them back out
	bulkGetBody := `{"docs":[{"id":"doc-0"}`
	for i := 1; i < numDocs; i++ {
		bulkGetBody += `,{"id":"doc-` + strconv.Itoa(i) + `"}`
	}
	bulkGetBody += `]}`

	bulkGetHeaders := map[string]string{
		"User-Agent":   "CouchbaseLite/1.2",
		"Content-Type": "application/json",
	}

	// request an uncompressed _bulk_get
	resp := rt.SendAdminRequestWithHeaders(http.MethodPost, "/db/_bulk_get", bulkGetBody, bulkGetHeaders)
	assertStatus(t, resp, http.StatusOK)

	uncompressedBodyLen := resp.Body.Len()
	assert.Truef(t, uncompressedBodyLen >= minUncompressedSize, "Expected uncompressed response to be larger than minUncompressedSize (%d bytes) - got %d bytes", minUncompressedSize, uncompressedBodyLen)

	// try the request again, but accept gzip encoding
	bulkGetHeaders["Accept-Encoding"] = "gzip"
	resp = rt.SendAdminRequestWithHeaders(http.MethodPost, "/db/_bulk_get", bulkGetBody, bulkGetHeaders)
	assertStatus(t, resp, http.StatusOK)

	compressedBodyLen := resp.Body.Len()
	compressionRatio := float64(uncompressedBodyLen) / float64(compressedBodyLen)
	assert.Truef(t, compressedBodyLen <= minUncompressedSize, "Expected compressed responsebody to be smaller than minUncompressedSize (%d bytes) - got %d bytes", minUncompressedSize, compressedBodyLen)
	assert.Truef(t, compressionRatio >= minCompressionRatio, "Expected compression ratio to be greater than minCompressionRatio (%d) - got %.2f", minCompressionRatio, compressionRatio)
	assert.Truef(t, compressionRatio <= maxCompressionRatio, "Expected compression ratio to be less than maxCompressionRatio (%d) - got %.2f", maxCompressionRatio, compressionRatio)
}

func TestBulkGetEmptyDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_get", input)
	assertStatus(t, response, 400)
}

func TestBulkDocsChangeToAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { requireAccess(doc.channel)}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", nil)
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	input := `{"docs": [{"_id": "bulk1", "type" : "setaccess", "owner":"user1" , "channel":"chan1"}, {"_id": "bulk2" , "channel":"chan1"}]}`

	response := rt.Send(requestByUser("POST", "/db/_bulk_docs", input, "user1"))
	assertStatus(t, response, 201)

	var docs []interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 2, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "1-afbcffa8a4641a0f4dd94d3fc9593e74", "id": "bulk1"}, docs[0])

	assert.Equal(t, map[string]interface{}{"rev": "1-4d79588b9fe9c38faae61f0c1b9471c0", "id": "bulk2"}, docs[1])

}

func TestBulkDocsChangeToRoleAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `
		function(doc) {
			if(doc.type == "roleaccess") {
				channel(doc.channel);
				access(doc.grantTo, doc.grantChannel);
			} else {
				requireAccess(doc.mustHaveAccess)
			}
		}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create a role with no channels assigned to it
	authenticator := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	role, err := authenticator.NewRole("role1", nil)
	assert.NoError(t, authenticator.Save(role))

	// Create a user with an explicit role grant for role1
	user, err := authenticator.NewUser("user1", "letmein", nil)
	user.SetExplicitRoles(channels.TimedSet{"role1": channels.NewVbSimpleSequence(1)}, 1)
	err = authenticator.Save(user)
	assert.NoError(t, err)

	// Bulk docs with 2 docs.  First doc grants role1 access to chan1.  Second requires chan1 for write.
	input := `{"docs": [
				{
					"_id": "bulk1",
				 	"type" : "roleaccess",
				 	"grantTo":"role:role1",
				 	"grantChannel":"chan1"
				 },
				{
					"_id": "bulk2",
					"mustHaveAccess":"chan1"
				}
				]}`

	response := rt.Send(requestByUser("POST", "/db/_bulk_docs", input, "user1"))
	assertStatus(t, response, 201)

	var docs []interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 2, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "1-17424d2a21bf113768dfdbcd344741ac", "id": "bulk1"}, docs[0])

	assert.Equal(t, map[string]interface{}{"rev": "1-f120ccb33c0a6ef43ef202ade28f98ef", "id": "bulk2"}, docs[1])

}

func TestBulkDocsNoEdits(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"new_edits":false, "docs": [
                    {"_id": "bdne1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "bdne2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	var docs []interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 2, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "12-abc", "id": "bdne1"}, docs[0])

	assert.Equal(t, map[string]interface{}{"rev": "34-def", "id": "bdne2"}, docs[1])

	// Now update the first doc with two new revisions:
	input = `{"new_edits":false, "docs": [
                  {"_id": "bdne1", "_rev": "14-jkl", "n": 111,
                   "_revisions": {"start": 14, "ids": ["jkl", "def", "abc", "eleven", "ten", "nine"]}}
            ]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 1, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "14-jkl", "id": "bdne1"}, docs[0])

}

type RevDiffResponse map[string][]string
type RevsDiffResponse map[string]RevDiffResponse

func TestRevsDiff(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
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
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &diffResponse))
	sort.Strings(diffResponse["rd1"]["possible_ancestors"])
	assert.Equal(t, RevsDiffResponse{
		"rd1": RevDiffResponse{"missing": []string{"13-def", "12-xyz"},
			"possible_ancestors": []string{"11-eleven", "12-abc"}},
		"rd9": RevDiffResponse{"missing": []string{"1-a", "2-b", "3-c"}}}, diffResponse)

}

func TestOpenRevs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "or1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}}
              ]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	assertStatus(t, response, 201)

	reqHeaders := map[string]string{
		"Accept": "application/json",
	}
	response = rt.SendAdminRequestWithHeaders("GET", `/db/or1?open_revs=["12-abc","10-ten"]`, "", reqHeaders)
	assertStatus(t, response, 200)

	var respBody []map[string]interface{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))

	assert.Len(t, respBody, 2)

	ok := respBody[0]["ok"].(map[string]interface{})
	assert.NotNil(t, ok)
	assert.Equal(t, "or1", ok[db.BodyId])
	assert.Equal(t, "12-abc", ok[db.BodyRev])
	assert.Equal(t, float64(1), ok["n"])

	assert.Equal(t, "10-ten", respBody[1]["missing"])
}

// Attempts to get a varying number of revisions on a per-doc basis.
// Covers feature implemented in issue #2992
func TestBulkGetPerDocRevsLimit(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Map of doc IDs to latest rev IDs
	docs := map[string]string{
		"doc1": "",
		"doc2": "",
		"doc3": "",
		"doc4": "",
	}

	// Add each doc with a few revisions
	for k := range docs {
		var body db.Body

		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v", k), fmt.Sprintf(`{"val":"1-%s"}`, k))
		assertStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev := body["rev"].(string)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v?rev=%s", k, rev), fmt.Sprintf(`{"val":"2-%s"}`, k))
		assertStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev = body["rev"].(string)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v?rev=%s", k, rev), fmt.Sprintf(`{"val":"3-%s"}`, k))
		assertStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev = body["rev"].(string)

		docs[k] = rev
	}

	reqRevsLimit := 2

	// Fetch docs by bulk with varying revs_limits
	bulkGetResponse := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_bulk_get?revs=true&revs_limit=%v", reqRevsLimit), fmt.Sprintf(`
		{
			"docs": [
				{
					"rev": "%s",
					"id": "doc1"
				},
				{
					"revs_limit": 1,
					"rev": "%s",
					"id": "doc2"
				},
				{
					"revs_limit": 0,
					"rev": "%s",
					"id": "doc3"
				},
				{
					"revs_limit": -1,
					"rev": "%s",
					"id": "doc4"
				}
			]
		}`, docs["doc1"], docs["doc2"], docs["doc3"], docs["doc4"]))
	assert.Equal(t, http.StatusOK, bulkGetResponse.Code)

	bulkGetResponse.DumpBody()

	// Parse multipart/mixed docs and create reader
	contentType, attrs, _ := mime.ParseMediaType(bulkGetResponse.Header().Get("Content-Type"))
	log.Printf("content-type: %v.  attrs: %v", contentType, attrs)
	assert.Equal(t, "multipart/mixed", contentType)
	reader := multipart.NewReader(bulkGetResponse.Body, attrs["boundary"])

readerLoop:
	for {

		// Get the next part.  Break out of the loop if we hit EOF
		part, err := reader.NextPart()
		if err != nil {
			if err == io.EOF {
				break readerLoop
			}
			t.Fatal(err)
		}

		partBytes, err := ioutil.ReadAll(part)
		assert.NoError(t, err, "Unexpected error")

		log.Printf("multipart part: %+v", string(partBytes))

		partJSON := map[string]interface{}{}
		err = base.JSONUnmarshal(partBytes, &partJSON)
		assert.NoError(t, err, "Unexpected error")

		var exp int

		switch partJSON[db.BodyId] {
		case "doc1":
			exp = reqRevsLimit
		case "doc2":
			exp = 1
		case "doc3":
			// revs_limit of zero should display no revision object at all
			assert.Equal(t, nil, partJSON[db.BodyRevisions])
			break readerLoop
		case "doc4":
			// revs_limit must be >= 0
			assert.Equal(t, "bad_request", partJSON["error"])
			break readerLoop
		default:
			t.Error("unrecognised part in response")
		}

		revisions := partJSON[db.BodyRevisions].(map[string]interface{})
		assert.Equal(t, exp, len(revisions[db.RevisionsIds].([]interface{})))
	}

}

func TestLocalDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)

	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-1", respBody[db.BodyRev])
	assert.Equal(t, "there", respBody["hi"])

	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 409)
	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-2", respBody[db.BodyRev])
	assert.Equal(t, "again", respBody["hi"])

	// Check the handling of large integers, which caused trouble for us at one point:
	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"big": 123456789, "_rev": "0-2"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-3", respBody[db.BodyRev])
	assert.Equal(t, float64(123456789), respBody["big"])

	response = rt.SendAdminRequest("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 409)
	response = rt.SendAdminRequest("DELETE", "/db/_local/loc1?rev=0-3", "")
	assertStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 404)
	response = rt.SendAdminRequest("DELETE", "/db/_local/loc1", "")
	assertStatus(t, response, 404)

	// Check the handling of URL encoded slash at end of _local%2Fdoc
	response = rt.SendAdminRequest("PUT", "/db/_local%2Floc12", `{"hi": "there"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc2", "")
	assertStatus(t, response, 404)
	response = rt.SendAdminRequest("DELETE", "/db/_local%2floc2", "")
	assertStatus(t, response, 404)
}

func TestLocalDocExpiry(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket for expiry")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	timeNow := uint32(time.Now().Unix())
	oneMoreHour := uint32(time.Now().Add(time.Hour).Unix())

	// set localDocExpiry to 30m
	rt.GetDatabase().Options.LocalDocExpirySecs = uint32(60 * 30)
	log.Printf("Expiry expected between %d and %d", timeNow, oneMoreHour)

	response := rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	assertStatus(t, response, 201)

	cbStore, ok := base.AsCouchbaseStore(rt.Bucket())
	require.True(t, ok)

	localDocKey := db.RealSpecialDocID(db.DocTypeLocal, "loc1")
	expiry, getMetaError := cbStore.GetExpiry(localDocKey)
	log.Printf("Expiry after PUT is %v", expiry)
	assert.True(t, expiry > timeNow, "expiry is not greater than current time")
	assert.True(t, expiry < oneMoreHour, "expiry is not greater than current time")
	assert.NoError(t, getMetaError)

	// Retrieve local doc, ensure non-zero expiry is preserved
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	assertStatus(t, response, 200)
	expiry, getMetaError = cbStore.GetExpiry(localDocKey)
	log.Printf("Expiry after GET is %v", expiry)
	assert.True(t, expiry > timeNow, "expiry is not greater than current time")
	assert.True(t, expiry < oneMoreHour, "expiry is not greater than current time")
	assert.NoError(t, getMetaError)
}

func TestResponseEncoding(t *testing.T) {
	// Make a doc longer than 1k so the HTTP response will be compressed:
	str := "DORKY "
	for i := 0; i < 10; i++ {
		str = str + str
	}
	docJSON := fmt.Sprintf(`{"long": %q}`, str)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_local/loc1", docJSON)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequestWithHeaders("GET", "/db/_local/loc1", "",
		map[string]string{"Accept-Encoding": "foo, gzip, bar"})
	assertStatus(t, response, 200)
	assert.Equal(t, "gzip", response.Header().Get("Content-Encoding"))
	unzip, err := gzip.NewReader(response.Body)
	assert.NoError(t, err)
	unjson := base.JSONDecoder(unzip)
	var body db.Body
	assert.Equal(t, nil, unjson.Decode(&body))
	assert.Equal(t, str, body["long"])
}

func TestLogin(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	a := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	user, err = a.GetUser("")
	assert.NoError(t, err)
	assert.True(t, user.Disabled())

	response := rt.SendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	assertStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.SetOf(t, "*"))
	assert.NoError(t, a.Save(user))

	assertStatus(t, rt.SendRequest("GET", "/db/_session", ""), 200)

	response = rt.SendRequest("POST", "/db/_session", `{"name":"pupshaw", "password":"letmein"}`)
	assertStatus(t, response, 200)
	log.Printf("Set-Cookie: %s", response.Header().Get("Set-Cookie"))
	assert.True(t, response.Header().Get("Set-Cookie") != "")
}

func TestInvalidSession(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/testdoc", `{"hi": "there"}`)
	assertStatus(t, response, 201)

	headers := map[string]string{}
	headers["Cookie"] = fmt.Sprintf("%s=%s", auth.DefaultCookieName, "FakeSession")
	response = rt.SendRequestWithHeaders("GET", "/db/testdoc", "", headers)
	assertStatus(t, response, 401)

	var body db.Body
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)

	assert.NoError(t, err)
	assert.Equal(t, "Session Invalid", body["reason"])
}

func TestCustomCookieName(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	customCookieName := "TestCustomCookieName"
	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		Name:              "db",
		SessionCookieName: customCookieName,
	}}

	// Disable guest user
	a := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a user
	response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"1234"}`)
	assertStatus(t, response, 201)

	// Create a session
	resp := rt.SendRequest("POST", "/db/_session", `{"name":"user1", "password":"1234"}`)
	assert.Equal(t, 200, resp.Code)

	// Extract the cookie from the create session response to verify the "Set-Cookie" value returned by Sync Gateway
	cookies := resp.Result().Cookies()
	assert.True(t, len(cookies) == 1)
	cookie := cookies[0]
	assert.Equal(t, customCookieName, cookie.Name)
	assert.Equal(t, "/db", cookie.Path)

	// Attempt to use default cookie name to authenticate -- expect a 401 error
	headers := map[string]string{}
	headers["Cookie"] = fmt.Sprintf("%s=%s", auth.DefaultCookieName, cookie.Value)
	resp = rt.SendRequestWithHeaders("GET", "/db/foo", `{}`, headers)
	assert.Equal(t, 401, resp.Result().StatusCode)

	// Attempt to use custom cookie name to authenticate
	headers["Cookie"] = fmt.Sprintf("%s=%s", customCookieName, cookie.Value)
	resp = rt.SendRequestWithHeaders("POST", "/db/", `{"_id": "foo", "key": "val"}`, headers)
	assert.Equal(t, 200, resp.Result().StatusCode)

}

func TestReadChangesOptionsFromJSON(t *testing.T) {

	h := &handler{}
	h.server = NewServerContext(&StartupConfig{}, false)
	defer h.server.Close()

	// Basic case, no heartbeat, no timeout
	optStr := `{"feed":"longpoll", "since": "123456:78", "limit":123, "style": "all_docs",
				"include_docs": true, "filter": "Melitta", "channels": "ABC,BBC"}`
	feed, options, filter, channelsArray, _, _, err := h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, "longpoll", feed)

	assert.Equal(t, uint64(78), options.Since.Seq)
	assert.Equal(t, uint64(123456), options.Since.TriggeredBy)
	assert.Equal(t, 123, options.Limit)
	assert.Equal(t, true, options.Conflicts)
	assert.Equal(t, true, options.IncludeDocs)
	assert.Equal(t, uint64(kDefaultHeartbeatMS), options.HeartbeatMs)
	assert.Equal(t, uint64(kDefaultTimeoutMS), options.TimeoutMs)

	assert.Equal(t, "Melitta", filter)
	assert.Equal(t, []string{"ABC", "BBC"}, channelsArray)

	// Attempt to set heartbeat, timeout to valid values
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":30000, "timeout":60000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(30000), options.HeartbeatMs)
	assert.Equal(t, uint64(60000), options.TimeoutMs)

	// Attempt to set valid timeout, no heartbeat
	optStr = `{"feed":"longpoll", "since": "1", "timeout":2000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2000), options.TimeoutMs)

	// Disable heartbeat, timeout by explicitly setting to zero
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":0, "timeout":0}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), options.HeartbeatMs)
	assert.Equal(t, uint64(0), options.TimeoutMs)

	// Attempt to set heartbeat less than minimum heartbeat, timeout greater than max timeout
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":1000, "timeout":1000000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(kMinHeartbeatMS), options.HeartbeatMs)
	assert.Equal(t, uint64(kMaxTimeoutMS), options.TimeoutMs)

	// Set max heartbeat in server context, attempt to set heartbeat greater than max
	h.server.config.Replicator.MaxHeartbeat = base.NewConfigDuration(time.Minute)
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":90000}`
	feed, options, filter, channelsArray, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(60000), options.HeartbeatMs)
}

// Test _all_docs API call under different security scenarios
func TestAllDocsAccessControl(t *testing.T) {
	// restTester := initRestTester(db.IntSequenceType, `function(doc) {channel(doc.channels);}`)
	rt := NewRestTester(t, nil)
	defer rt.Close()
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
	type allDocsResponse struct {
		TotalRows int          `json:"total_rows"`
		Offset    int          `json:"offset"`
		Rows      []allDocsRow `json:"rows"`
	}

	// Create some docs:
	a := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	assertStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":"Cinemax"}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["WB", "Cinemax"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["CBS", "Cinemax"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["CBS"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":[]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create a user:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf(t, "Cinemax"))
	assert.NoError(t, a.Save(alice))

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

	allDocsResult := allDocsResponse{}
	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 3, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc4", allDocsResult.Rows[1].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[1].Value.Channels)
	assert.Equal(t, "doc5", allDocsResult.Rows[2].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[2].Value.Channels)

	// Check all docs limit option
	request, _ = http.NewRequest("GET", "/db/_all_docs?limit=1&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs startkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?startkey=doc5&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc5", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs startkey option with double quote
	request, _ = http.NewRequest("GET", `/db/_all_docs?startkey="doc5"&channels=true`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc5", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs endkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?endkey=doc3&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs endkey option
	request, _ = http.NewRequest("GET", `/db/_all_docs?endkey="doc3"&channels=true`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check _all_docs with include_docs option:
	request, _ = http.NewRequest("GET", "/db/_all_docs?include_docs=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 3, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, "doc4", allDocsResult.Rows[1].ID)
	assert.Equal(t, "doc5", allDocsResult.Rows[2].ID)

	// Check POST to _all_docs:
	body := `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 4, len(allDocsResult.Rows))
	assert.Equal(t, "doc4", allDocsResult.Rows[0].Key)
	assert.Equal(t, "doc4", allDocsResult.Rows[0].ID)
	assert.Equal(t, "1-e0351a57554e023a77544d33dd21e56c", allDocsResult.Rows[0].Value.Rev)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc1", allDocsResult.Rows[1].Key)
	assert.Equal(t, "forbidden", allDocsResult.Rows[1].Error)
	assert.Equal(t, "", allDocsResult.Rows[1].Value.Rev)
	assert.Equal(t, "doc3", allDocsResult.Rows[2].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[2].Value.Channels)
	assert.Equal(t, "1-20912648f85f2bbabefb0993ddd37b41", allDocsResult.Rows[2].Value.Rev)
	assert.Equal(t, "b0gus", allDocsResult.Rows[3].Key)
	assert.Equal(t, "not_found", allDocsResult.Rows[3].Error)
	assert.Equal(t, "", allDocsResult.Rows[3].Value.Rev)

	// Check GET to _all_docs with keys parameter:
	request, _ = http.NewRequest("GET", `/db/_all_docs?channels=true&keys=%5B%22doc4%22%2C%22doc1%22%2C%22doc3%22%2C%22b0gus%22%5D`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from GET _all_docs = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 4, len(allDocsResult.Rows))
	assert.Equal(t, "doc4", allDocsResult.Rows[0].Key)
	assert.Equal(t, "doc4", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc1", allDocsResult.Rows[1].Key)
	assert.Equal(t, "forbidden", allDocsResult.Rows[1].Error)
	assert.Equal(t, "doc3", allDocsResult.Rows[2].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[2].Value.Channels)
	assert.Equal(t, "b0gus", allDocsResult.Rows[3].Key)
	assert.Equal(t, "not_found", allDocsResult.Rows[3].Error)

	// Check POST to _all_docs with limit option:
	body = `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?limit=1&channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc4", allDocsResult.Rows[0].Key)
	assert.Equal(t, "doc4", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check _all_docs as admin:
	response = rt.SendAdminRequest("GET", "/db/_all_docs", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 5, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "doc2", allDocsResult.Rows[1].ID)
}

func TestChannelAccessChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {access(doc.owner, doc._id);channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create users:
	alice, err := a.NewUser("alice", "letmein", channels.SetOf(t, "zero"))
	assert.NoError(t, a.Save(alice))
	zegpold, err := a.NewUser("zegpold", "letmein", channels.SetOf(t, "zero"))
	assert.NoError(t, a.Save(zegpold))

	// Create some docs that give users access:
	response := rt.Send(request("PUT", "/db/alpha", `{"owner":"alice"}`))
	assertStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	alphaRevID := body["rev"].(string)

	assertStatus(t, rt.Send(request("PUT", "/db/beta", `{"owner":"boadecia"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/delta", `{"owner":"alice"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/gamma", `{"owner":"zegpold"}`)), 201)

	assertStatus(t, rt.Send(request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)

	rt.MustWaitForDoc("g1", t)

	changes := changesResults{}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)

	assert.NoError(t, err)
	require.Len(t, changes.Results, 1)
	since := changes.Results[0].Seq
	assert.Equal(t, "g1", changes.Results[0].ID)

	// Look up sequences for created docs
	deltaGrantDocSeq, err := rt.SequenceForDoc("delta")
	assert.NoError(t, err, "Error retrieving document sequence")
	gammaGrantDocSeq, err := rt.SequenceForDoc("gamma")
	assert.NoError(t, err, "Error retrieving document sequence")

	alphaDocSeq, err := rt.SequenceForDoc("a1")
	assert.NoError(t, err, "Error retrieving document sequence")
	gammaDocSeq, err := rt.SequenceForDoc("g1")
	assert.NoError(t, err, "Error retrieving document sequence")

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"alpha": channels.NewVbSimpleSequence(uint64(1)),
			"delta": channels.NewVbSimpleSequence(deltaGrantDocSeq),
		}, alice.Channels())

	zegpold, _ = a.GetUser("zegpold")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"gamma": channels.NewVbSimpleSequence(gammaGrantDocSeq),
		}, zegpold.Channels())

	// Update a document to revoke access to alice and grant it to zegpold:
	str := fmt.Sprintf(`{"owner":"zegpold", "_rev":%q}`, alphaRevID)
	assertStatus(t, rt.Send(request("PUT", "/db/alpha", str)), 201)

	alphaGrantDocSeq, err := rt.SequenceForDoc("alpha")
	assert.NoError(t, err, "Error retrieving document sequence")

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"delta": channels.NewVbSimpleSequence(deltaGrantDocSeq),
		}, alice.Channels())

	zegpold, _ = a.GetUser("zegpold")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"alpha": channels.NewVbSimpleSequence(alphaGrantDocSeq),
			"gamma": channels.NewVbSimpleSequence(gammaGrantDocSeq),
		}, zegpold.Channels())

	rt.MustWaitForDoc("alpha", t)

	// Look at alice's _changes feed:
	changes = changesResults{}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "alice"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	assert.NoError(t, err)
	assert.Equal(t, "d1", changes.Results[0].ID)

	// The complete _changes feed for zegpold contains docs a1 and g1:
	changes = changesResults{}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	assert.NoError(t, err)
	require.Len(t, changes.Results, 2)
	assert.Equal(t, "g1", changes.Results[0].ID)
	assert.Equal(t, gammaDocSeq, changes.Results[0].Seq.Seq)
	assert.Equal(t, "a1", changes.Results[1].ID)
	assert.Equal(t, alphaDocSeq, changes.Results[1].Seq.Seq)
	assert.Equal(t, alphaGrantDocSeq, changes.Results[1].Seq.TriggeredBy)

	// Changes feed with since=gamma:8 would ordinarily be empty, but zegpold got access to channel
	// alpha after sequence 8, so the pre-existing docs in that channel are included:
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=\"%s\"", since),
		"", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "a1", changes.Results[0].ID)

	// What happens if we call access() with a nonexistent username?
	assertStatus(t, rt.Send(request("PUT", "/db/epsilon", `{"owner":"waldo"}`)), 201) // seq 10

	// Must wait for sequence to arrive in cache, since the cache processor will be paused when UpdateSyncFun() is called
	// below, which could lead to a data race if the cache processor is paused while it's processing a change
	rt.MustWaitForDoc("epsilon", t)

	// Finally, throw a wrench in the works by changing the sync fn. Note that normally this wouldn't
	// be changed while the database is in use (only when it's re-opened) but for testing purposes
	// we do it now because we can't close and re-open an ephemeral Walrus database.
	dbc := rt.ServerContext().Database("db")
	database, _ := db.GetDatabase(dbc, nil)

	changed, err := database.UpdateSyncFun(`function(doc) {access("alice", "beta");channel("beta");}`)
	assert.NoError(t, err)
	assert.True(t, changed)
	changeCount, err := database.UpdateAllDocChannels(false, func(docsProcessed, docsChanged *int) {}, base.NewSafeTerminator())
	assert.NoError(t, err)
	assert.Equal(t, 9, changeCount)

	expectedIDs := []string{"beta", "delta", "gamma", "a1", "b1", "d1", "g1", "alpha", "epsilon"}
	changes, err = rt.WaitForChanges(len(expectedIDs), "/db/_changes", "alice", false)
	assert.NoError(t, err, "Unexpected error")
	log.Printf("_changes looks like: %+v", changes)
	assert.Equal(t, len(expectedIDs), len(changes.Results))

	require.Len(t, changes.Results, len(expectedIDs))
	for i, expectedID := range expectedIDs {
		if changes.Results[i].ID != expectedID {
			log.Printf("changes.Results[i].ID != expectedID.  changes.Results: %+v, expectedIDs: %v", changes.Results, expectedIDs)
		}
		assert.Equal(t, expectedID, changes.Results[i].ID)
	}

}

func TestAccessOnTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD)

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 if (doc.owner) {
			 	access(doc.owner, doc.channel);
			 }
			 if (doc._deleted && oldDoc.owner) {
			 	access(oldDoc.owner, oldDoc.channel);
			 }
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "zero"))
	assert.NoError(t, a.Save(bernard))

	// Create doc that gives user access to its channel
	response := rt.SendAdminRequest("PUT", "/db/alpha", `{"owner":"bernard", "channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	assert.NoError(t, rt.WaitForPendingChanges())

	// Validate the user gets the doc on the _changes feed
	// Check the _changes feed:
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	require.Len(t, changes.Results, 1)
	if len(changes.Results) > 0 {
		assert.Equal(t, "alpha", changes.Results[0].ID)
	}

	// Delete the document
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/alpha?rev=%s", revId), "")
	assertStatus(t, response, 200)

	// Make sure it actually was deleted
	response = rt.SendAdminRequest("GET", "/db/alpha", "")
	assertStatus(t, response, 404)

	// Wait for change caching to complete
	assert.NoError(t, rt.WaitForPendingChanges())

	// Check user access again:
	changes.Results = nil
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	if len(changes.Results) > 0 {
		assert.Equal(t, "alpha", changes.Results[0].ID)
		assert.Equal(t, true, changes.Results[0].Deleted)
	}

}

// TestSyncFnBodyProperties puts a document into channels based on which properties are present on the document.
// This can be used to introspect what properties ended up in the body passed to the sync function.
func TestSyncFnBodyProperties(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must EXACTLY match the top-level properties seen in the sync function body.
	// Properties not present in this list, but present in the sync function body will be caught.
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
		db.BodyRev,
	}

	// This sync function routes into channels based on top-level properties contained in doc
	syncFn := `function(doc) {
		console.log("full doc: "+JSON.stringify(doc));
		for (var p in doc) {
			console.log("doc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/"+testDocID, `{"`+testdataKey+`":true}`)
	assertStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnBodyPropertiesTombstone puts a document into channels based on which properties are present on the document.
// It creates a doc, and then tombstones it to see what properties are present in the body of the tombstone.
func TestSyncFnBodyPropertiesTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must be present in the sync function body for a tombstone
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
		db.BodyRev,
		db.BodyDeleted,
	}

	// This sync function routes into channels based on top-level properties contained in doc
	syncFn := `function(doc) {
		console.log("full doc: "+JSON.stringify(doc));
		for (var p in doc) {
			console.log("doc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/"+testDocID, `{"`+testdataKey+`":true}`)
	assertStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+revID, `{}`)
	assertStatus(t, response, 200)

	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnOldDocBodyProperties puts a document into channels based on which properties are present in the 'oldDoc' body.
// It creates a doc, and updates it to inspect what properties are present on the oldDoc body.
func TestSyncFnOldDocBodyProperties(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must be present in the sync function oldDoc body for a regular PUT containing testdataKey
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
	}

	// This sync function routes into channels based on top-level properties contained in oldDoc
	syncFn := `function(doc, oldDoc) {
		console.log("full doc: "+JSON.stringify(doc));
		console.log("full oldDoc: "+JSON.stringify(oldDoc));
		for (var p in oldDoc) {
			console.log("oldDoc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/"+testDocID, `{"`+testdataKey+`":true}`)
	assertStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+revID, `{"`+testdataKey+`":true,"update":2}`)
	assertStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn oldDoc body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnOldDocBodyPropertiesTombstoneResurrect puts a document into channels based on which properties are present in the 'oldDoc' body.
// It creates a doc, tombstones it, and then resurrects it to inspect oldDoc properties on the tombstone.
func TestSyncFnOldDocBodyPropertiesTombstoneResurrect(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// All of these properties must be present in the sync function body for a regular PUT containing testdataKey
	expectedProperties := []string{
		testdataKey,
		db.BodyId,
		db.BodyDeleted,
	}

	// This sync function routes into channels based on top-level properties contained in oldDoc
	syncFn := `function(doc, oldDoc) {
		console.log("full doc: "+JSON.stringify(doc));
		console.log("full oldDoc: "+JSON.stringify(oldDoc));
		for (var p in oldDoc) {
			console.log("oldDoc property: "+p);
			channel(p);
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/"+testDocID, `{"`+testdataKey+`":true}`)
	assertStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+revID, `{}`)
	assertStatus(t, response, 200)
	body = nil
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID = body["rev"].(string)

	response = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+revID, `{"`+testdataKey+`":true}`)
	assertStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn oldDoc body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnDocBodyPropertiesSwitchActiveTombstone creates a branched revtree, where the first tombstone created becomes active again after the shorter b branch is tombstoned.
// The test makes sure that in this scenario, the "doc" body of the sync function when switching from (T) 3-b to (T) 4-a contains a _deleted property (stamped by getAvailable1xRev)
//     1-a
//     ├── 2-a
//     │   └── 3-a
//     │       └──────── (T) 4-a
//     └──────────── 2-b
//                   └────────────── (T) 3-b
func TestSyncFnDocBodyPropertiesSwitchActiveTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	const (
		testDocID   = "testdoc"
		testdataKey = "testdata"
	)

	// This sync function logs a warning for each revision pushed through the sync function, and an error when it sees _deleted inside doc, when oldDoc contains syncOldDocBodyCheck=true
	//
	// These are then asserted by looking at the expvar stats for warn and error counts.
	// We can't rely on channels to get information out of the sync function environment, because we'd need an active doc, which this test does not allow for.
	syncFn := `function(doc, oldDoc) {
		console.log("full doc: "+JSON.stringify(doc));
		console.log("full oldDoc: "+JSON.stringify(oldDoc));

		if (doc.testdata == 1 || (oldDoc != null && !oldDoc.syncOldDocBodyCheck)) {
			console.log("skipping oldDoc property checks for this rev")
			return
		}

		if (doc != null && doc._deleted) {
			console.error("doc contained _deleted")
		}
	}`

	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// rev 1-a
	resp := rt.SendAdminRequest("PUT", "/db/"+testDocID, `{"`+testdataKey+`":1}`)
	assertStatus(t, resp, 201)
	rev1ID := respRevID(t, resp)

	// rev 2-a
	resp = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+rev1ID, `{"`+testdataKey+`":2}`)
	assertStatus(t, resp, 201)
	rev2aID := respRevID(t, resp)

	// rev 3-a
	resp = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+rev2aID, `{"`+testdataKey+`":3,"syncOldDocBodyCheck":true}`)
	assertStatus(t, resp, 201)
	rev3aID := respRevID(t, resp)

	// rev 2-b
	_, rev1Hash := db.ParseRevID(rev1ID)
	resp = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?new_edits=false", `{"`+db.BodyRevisions+`":{"start":2,"ids":["b", "`+rev1Hash+`"]}}`)
	assertStatus(t, resp, 201)
	rev2bID := respRevID(t, resp)

	// tombstone at 4-a
	resp = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+rev3aID, `{}`)
	assertStatus(t, resp, 200)

	numErrorsBefore, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)
	// tombstone at 3-b
	resp = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+rev2bID, `{}`)
	assertStatus(t, resp, 200)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, 1, numErrorsAfter-numErrorsBefore, "expecting to see only only 1 error logged")
}

// Test for wrong _changes entries for user joining a populated channel
func TestUserJoiningPopulatedChannel(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	assert.NoError(t, a.Save(guest))

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	// Create 100 docs
	for i := 0; i < 100; i++ {
		docpath := fmt.Sprintf("/db/doc%d", i)
		assertStatus(t, rt.Send(request("PUT", docpath, `{"foo": "bar", "channels":["alpha"]}`)), 201)
	}

	limit := 50
	changesResults, err := rt.WaitForChanges(50, fmt.Sprintf("/db/_changes?limit=%d", limit), "user1", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since := changesResults.Results[49].Seq
	assert.Equal(t, "doc48", changesResults.Results[49].ID)

	// // Check the _changes feed with  since and limit, to get second half of feed
	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/db/_changes?since=\"%s\"&limit=%d", since, limit), "user1", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc98", changesResults.Results[49].ID)

	// Create user2
	response = rt.SendAdminRequest("PUT", "/db/_user/user2", `{"email":"user2@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	// Retrieve all changes for user2 with no limits
	changesResults, err = rt.WaitForChanges(101, fmt.Sprintf("/db/_changes"), "user2", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 101)
	assert.Equal(t, "doc99", changesResults.Results[99].ID)

	// Create user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"email":"user3@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	getUserResponse := rt.SendAdminRequest("GET", "/db/_user/user3", "")
	assertStatus(t, getUserResponse, 200)
	log.Printf("create user response: %s", getUserResponse.Body.Bytes())

	// Get the sequence from the user doc to validate against the triggered by value in the changes results
	user3, _ := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetUser("user3")
	userSequence := user3.Sequence()

	// Get first 50 document changes.
	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/db/_changes?limit=%d", limit), "user3", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc49", changesResults.Results[49].ID)
	assert.Equal(t, userSequence, since.TriggeredBy)

	// // Get remainder of changes i.e. no limit parameter
	changesResults, err = rt.WaitForChanges(51, fmt.Sprintf("/db/_changes?since=\"%s\"", since), "user3", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 51)
	assert.Equal(t, "doc99", changesResults.Results[49].ID)

	// Create user4
	response = rt.SendAdminRequest("PUT", "/db/_user/user4", `{"email":"user4@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)
	// Get the sequence from the user doc to validate against the triggered by value in the changes results
	user4, _ := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetUser("user4")
	user4Sequence := user4.Sequence()

	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/db/_changes?limit=%d", limit), "user4", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc49", changesResults.Results[49].ID)
	assert.Equal(t, user4Sequence, since.TriggeredBy)

	// // Check the _changes feed with  since and limit, to get second half of feed
	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/db/_changes?since=%s&limit=%d", since, limit), "user4", false)
	assert.Equal(t, nil, err)
	require.Len(t, changesResults.Results, 50)
	assert.Equal(t, "doc99", changesResults.Results[49].ID)

}

func TestRoleAssignmentBeforeUserExists(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// POST a role
	response := rt.SendAdminRequest("POST", "/db/_role/", `{"name":"role1","admin_channels":["chan1"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/role1", "")
	assertStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "role1", body["name"])

	// Put document to trigger sync function
	response = rt.Send(request("PUT", "/db/doc1", `{"user":"user1", "role":"role:role1", "channel":"chan1"}`)) // seq=1
	assertStatus(t, response, 201)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])

	// POST the new user the GET and verify that it shows the assigned role
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"letmein"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/user1", "")
	assertStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "user1", body["name"])
	assert.Equal(t, []interface{}{"role1"}, body["roles"])
	assert.Equal(t, []interface{}{"!", "chan1"}, body["all_channels"])

	// goassert.DeepEquals(t, body["admin_roles"], []interface{}{"hipster"})
	// goassert.DeepEquals(t, body["all_channels"], []interface{}{"bar", "fedoras", "fixies", "foo"})
}

func TestRoleAccessChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	require.NoError(t, err)

	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create users:
	response := rt.SendAdminRequest("PUT", "/db/_user/alice", `{"password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_user/zegpold", `{"password":"letmein", "admin_channels":["beta"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["gamma"]}`)
	assertStatus(t, response, 201)
	/*
		alice, err := a.NewUser("alice", "letmein", channels.SetOf(t, "alpha"))
		assert.NoError(t, a.Save(alice))
		zegpold, err := a.NewUser("zegpold", "letmein", channels.SetOf(t, "beta"))
		assert.NoError(t, a.Save(zegpold))

		hipster, err := a.NewRole("hipster", channels.SetOf(t, "gamma"))
		assert.NoError(t, a.Save(hipster))
	*/

	// Create some docs in the channels:
	cacheWaiter := rt.ServerContext().Database("db").NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(1)
	response = rt.Send(request("PUT", "/db/fashion",
		`{"user":"alice","role":["role:hipster","role:bogus"]}`))
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	fashionRevID := body["rev"].(string)

	roleGrantSequence := rt.GetDocumentSequence("fashion")

	cacheWaiter.Add(4)
	assertStatus(t, rt.Send(request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	assertStatus(t, rt.Send(request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)

	// Check user access:
	alice, _ := a.GetUser("alice")
	assert.Equal(t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(1),
			"alpha": channels.NewVbSimpleSequence(alice.Sequence()),
			"gamma": channels.NewVbSimpleSequence(roleGrantSequence),
		}, alice.InheritedChannels())

	assert.Equal(t,

		channels.TimedSet{
			"bogus":   channels.NewVbSimpleSequence(roleGrantSequence),
			"hipster": channels.NewVbSimpleSequence(roleGrantSequence),
		}, alice.RoleNames())

	zegpold, _ := a.GetUser("zegpold")
	assert.Equal(t,

		channels.TimedSet{
			"!":    channels.NewVbSimpleSequence(1),
			"beta": channels.NewVbSimpleSequence(zegpold.Sequence()),
		}, zegpold.InheritedChannels())

	assert.Equal(t, channels.TimedSet{}, zegpold.RoleNames())

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	cacheWaiter.Wait()
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("1st _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 3, len(changes.Results))
	assert.Equal(t, "_user/alice", changes.Results[0].ID)
	assert.Equal(t, "g1", changes.Results[1].ID)
	assert.Equal(t, "a1", changes.Results[2].ID)

	response = rt.Send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("2nd _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "_user/zegpold", changes.Results[0].ID)
	assert.Equal(t, "b1", changes.Results[1].ID)
	lastSeqPreGrant := changes.Last_Seq

	// Update "fashion" doc to grant zegpold the role "hipster" and take it away from alice:
	cacheWaiter.Add(1)
	str := fmt.Sprintf(`{"user":"zegpold", "role":"role:hipster", "_rev":%q}`, fashionRevID)
	assertStatus(t, rt.Send(request("PUT", "/db/fashion", str)), 201)

	updatedRoleGrantSequence := rt.GetDocumentSequence("fashion")

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.Equal(t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(0x1),
			"alpha": channels.NewVbSimpleSequence(alice.Sequence()),
		}, alice.InheritedChannels())

	zegpold, _ = a.GetUser("zegpold")
	assert.Equal(t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(0x1),
			"beta":  channels.NewVbSimpleSequence(zegpold.Sequence()),
			"gamma": channels.NewVbSimpleSequence(updatedRoleGrantSequence),
		}, zegpold.InheritedChannels())

	// The complete _changes feed for zegpold contains docs g1 and b1:
	cacheWaiter.Wait()
	changes.Results = nil
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("3rd _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	log.Printf("changes: %+v", changes.Results)
	require.Equal(t, len(changes.Results), 3)
	assert.Equal(t, "_user/zegpold", changes.Results[0].ID)
	assert.Equal(t, "b1", changes.Results[1].ID)
	assert.Equal(t, "g1", changes.Results[2].ID)

	// Changes feed with since=lastSeqPreGrant would ordinarily be empty, but zegpold got access to channel
	// gamma after lastSeqPreGrant, so the pre-existing docs in that channel are included:
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%v", lastSeqPreGrant), "", "zegpold"))
	log.Printf("4th _changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "g1", changes.Results[0].ID)
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

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create a doc
	response := rt.Send(request("PUT", "/db/doc1", `{"foo":"bar", "channels":["ch1"]}`))
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	doc1RevID := body["rev"].(string)

	// Run GET _all_docs as admin with channels=true:
	response = rt.SendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Run POST _all_docs as admin with explicit docIDs and channels=true:
	keys := `{"keys": ["doc1"]}`
	response = rt.SendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Commit rev 2 that maps to a differenet channel
	str := fmt.Sprintf(`{"foo":"bar", "channels":["ch2"], "_rev":%q}`, doc1RevID)
	assertStatus(t, rt.Send(request("PUT", "/db/doc1", str)), 201)

	// Run GET _all_docs as admin with channels=true
	// Make sure that only the new channel appears in the docs channel list
	response = rt.SendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch2", allDocsResult.Rows[0].Value.Channels[0])

	// Run POST _all_docs as admin with explicit docIDs and channels=true
	// Make sure that only the new channel appears in the docs channel list
	keys = `{"keys": ["doc1"]}`
	response = rt.SendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
	assertStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch2", allDocsResult.Rows[0].Value.Channels[0])
}

// Test for regression of issue #447
func TestAttachmentsNoCrossTalk(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	doc1revId := rt.createDoc(t, "doc1")

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// attach to existing document with correct rev (should succeed)
	response := rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+doc1revId, attachmentBody, reqHeaders)
	assertStatus(t, response, 201)
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
			docrevId := rt.createDoc(tt, testCase.docName)

			attachmentBody := base64.StdEncoding.EncodeToString(make([]byte, testCase.byteSize))
			attachmentContentType := "content/type"
			reqHeaders := map[string]string{
				"Content-Type": attachmentContentType,
			}

			// Set attachment
			response := rt.SendAdminRequestWithHeaders("PUT", "/db/"+testCase.docName+"/attach1?rev="+docrevId,
				attachmentBody, reqHeaders)
			assertStatus(tt, response, testCase.expectedPut)

			// Get attachment back
			response = rt.SendAdminRequestWithHeaders("GET", "/db/"+testCase.docName+"/attach1", "", reqHeaders)
			assertStatus(tt, response, testCase.expectedGet)

			// If able to retrieve document check it is same as original
			if response.Code == 200 {
				assert.Equal(tt, response.Body.String(), attachmentBody)
			}
		})
	}

}

func TestAddingLargeDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	defer func() { walrus.MaxDocSize = 0 }()

	walrus.MaxDocSize = 20 * 1024 * 1024

	docBody := `{"value":"` + base64.StdEncoding.EncodeToString(make([]byte, 22000000)) + `"}`

	response := rt.SendAdminRequest("PUT", "/db/doc1", docBody)
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)

	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
}

func TestOldDocHandling(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `
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
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create user:
	frank, err := a.NewUser("charles", "1234", nil)
	assert.NoError(t, a.Save(frank))

	// Create a doc:
	response := rt.Send(request("PUT", "/db/testOldDocId", `{"foo":"bar"}`))
	assertStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	alphaRevID := body["rev"].(string)

	// Update a document to validate oldDoc id handling.  Will reject if old doc id not available
	str := fmt.Sprintf(`{"foo":"ball", "_rev":%q}`, alphaRevID)
	assertStatus(t, rt.Send(request("PUT", "/db/testOldDocId", str)), 201)

}

func TestStarAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges)

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
	rt := NewRestTester(t, nil)
	defer rt.Close()

	a := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	var changes struct {
		Results []db.ChangeEntry
	}
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	assertStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":["books"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["gifts"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["!"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["gifts"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":["!"]}`), 201)
	// document added to no channel should only end up available to users with * access
	assertStatus(t, rt.SendRequest("PUT", "/db/doc6", `{"channels":[]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.NoError(t, err)
	//
	// Part 1 - Tests for user with single channel access:
	//
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "books"))
	assert.NoError(t, a.Save(bernard))

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
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"books"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc3", allDocsResult.Rows[1].ID)
	assert.Equal(t, []string{"!"}, allDocsResult.Rows[1].Value.Channels)

	// Ensure docs have been processed before issuing changes requests
	expectedSeq := uint64(6)
	_ = rt.WaitForSequence(expectedSeq)

	// GET /db/_changes
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(changes.Results))
	since := changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for single channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=books", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	// GET /db/_changes for unauthorized channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=gifts", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(changes.Results))

	//
	// Part 2 - Tests for user with * channel access
	//

	// Create a user:
	fran, err := a.NewUser("fran", "letmein", channels.SetOf(t, "*"))
	assert.NoError(t, a.Save(fran))

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
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"books"}, allDocsResult.Rows[0].Value.Channels)

	// GET /db/_changes
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	//
	// Part 3 - Tests for user with no user channel access
	//
	// Create a user:
	manny, err := a.NewUser("manny", "letmein", nil)
	assert.NoError(t, a.Save(manny))

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
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)

	// GET /db/_changes
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(requestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)
}

// Test for issue #562
func TestCreateTarget(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Attempt to create existing target DB on public API
	response := rt.SendRequest("PUT", "/db/", "")
	assertStatus(t, response, 412)
	// Attempt to create new target DB on public API
	response = rt.SendRequest("PUT", "/foo/", "")
	assertStatus(t, response, 403)
}

// Test for issue 758 - basic auth with stale session cookie
func TestBasicAuthWithSessionCookie(t *testing.T) {

	rt := NewRestTester(t, nil)
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

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip this test under integration testing")
	}

	sc := NewServerContext(&StartupConfig{}, false)
	defer sc.Close()

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
	err := base.JSONUnmarshal([]byte(configJSON), &dbConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig})
	assert.True(t, err == nil)
}

func TestEventConfigValidationInvalid(t *testing.T) {
	dbConfigJSON := `{
  "name": "invalid",
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

	buf := bytes.NewBufferString(dbConfigJSON)
	var dbConfig DbConfig
	err := decodeAndSanitiseConfig(buf, &dbConfig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "document_scribbled_on")
}

// Reproduces https://github.com/couchbase/sync_gateway/issues/2427
// NOTE: to repro, you must run with -race flag
func TestBulkGetRevPruning(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// The number of goroutines that are reading the doc via the _bulk_get endpoint
	// which causes the pruning on the map -- all goroutines end up modifying the
	// same map reference concurrently and causing the data race
	numPruningBulkGetGoroutines := 10

	// Each _bulk_get reader goroutine should only try this many times
	maxIterationsPerBulkGetGoroutine := 200

	// Do a write
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":[]}`)
	assertStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId := body["rev"]

	// Update 10 times
	for i := 0; i < 20; i++ {
		str := fmt.Sprintf(`{"_rev":%q}`, revId)
		response = rt.SendAdminRequest("PUT", "/db/doc1", str)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		revId = body["rev"]
	}

	// Get latest rev id
	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId = body[db.BodyRev]

	// Spin up several goroutines to all try to do a _bulk_get on the latest revision.
	// Since they will be pruning the same shared rev history, it will cause a data race
	wg := sync.WaitGroup{}
	for i := 0; i < numPruningBulkGetGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for j := 0; j < maxIterationsPerBulkGetGoroutine; j++ {
				bulkGetDocs := fmt.Sprintf(`{"docs": [{"id": "doc1", "rev": "%v"}]}`, revId)
				bulkGetResponse := rt.SendAdminRequest("POST", "/db/_bulk_get?revs=true&revs_limit=2", bulkGetDocs)
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
	assertStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revidDoc1 := body["rev"].(string)

	// Add another doc
	docIdDoc2 := "doc2"
	responseDoc2 := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v", docIdDoc2), `{"prop":true}`)
	assertStatus(t, responseDoc2, 201)
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
	assertStatus(t, response, 201)

	// Get the couchbase doc
	couchbaseDoc := db.Body{}
	bucket := rt.Bucket()
	_, err := bucket.Get(docIdDoc1, &couchbaseDoc)
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
	err = bucket.Set(docIdDoc1, 0, nil, couchbaseDoc)
	assert.NoError(t, err, "Error putting couchbaseDoc")

	// Flush rev cache so that the _bulk_get request is forced to go back to the bucket to load the doc
	// rather than loading it from the (stale) rev cache.  The rev cache will be stale since the test
	// short-circuits Sync Gateway and directly updates the bucket.
	// Reset at the end of the test, to avoid bleed into other tests
	rt.GetDatabase().FlushRevisionCacheForTest()

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

		partBytes, err := ioutil.ReadAll(part)
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

// TestDocExpiry validates the value of the expiry as set in the document.  It doesn't validate actual expiration (not supported
// in walrus).
func TestDocExpiry(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body
	response := rt.SendAdminRequest("PUT", "/db/expNumericTTL", `{"_exp":100}`)
	assertStatus(t, response, 201)

	// Validate that exp isn't returned on the standard GET, bulk get
	response = rt.SendAdminRequest("GET", "/db/expNumericTTL", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok := body["_exp"]
	assert.Equal(t, false, ok)

	bulkGetDocs := `{"docs": [{"id": "expNumericTTL", "rev": "1-ca9ad22802b66f662ff171f226211d5c"}]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_get", bulkGetDocs)
	assertStatus(t, response, 200)
	responseString := string(response.Body.Bytes())
	assert.True(t, !strings.Contains(responseString, "_exp"), "Bulk get response contains _exp property when show_exp not set.")

	response = rt.SendAdminRequest("POST", "/db/_bulk_get?show_exp=true", bulkGetDocs)
	assertStatus(t, response, 200)
	responseString = string(response.Body.Bytes())
	assert.True(t, strings.Contains(responseString, "_exp"), "Bulk get response doesn't contain _exp property when show_exp was set.")

	body = nil
	response = rt.SendAdminRequest("GET", "/db/expNumericTTL?show_exp=true", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	// Validate other exp formats
	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expNumericUnix", `{"val":1, "_exp":4260211200}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/expNumericUnix?show_exp=true", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("numeric unix response: %s", response.Body.Bytes())
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expNumericString", `{"val":1, "_exp":"100"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/expNumericString?show_exp=true", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expBadString", `{"_exp":"abc"}`)
	assertStatus(t, response, 400)
	response = rt.SendAdminRequest("GET", "/db/expBadString?show_exp=true", "")
	assertStatus(t, response, 404)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, false, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expDateString", `{"_exp":"2105-01-01T00:00:00.000+00:00"}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/expDateString?show_exp=true", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expBadDateString", `{"_exp":"2105-0321-01T00:00:00.000+00:00"}`)
	assertStatus(t, response, 400)
	response = rt.SendAdminRequest("GET", "/db/expBadDateString?show_exp=true", "")
	assertStatus(t, response, 404)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, false, ok)

}

// Validate that sync function based expiry writes the _exp property to SG metadata in addition to setting CBS expiry
func TestDocSyncFunctionExpiry(t *testing.T) {
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {expiry(doc.expiry)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var body db.Body
	response := rt.SendAdminRequest("PUT", "/db/expNumericTTL", `{"expiry":100}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("GET", "/db/expNumericTTL?show_exp=true", "")
	assertStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	value, ok := body["_exp"]
	assert.Equal(t, true, ok)
	log.Printf("value: %v", value)
}

// Repro attempt for SG #3307.  Before fix for #3307, fails when SG_TEST_USE_XATTRS=true and run against an actual couchbase server
func TestWriteTombstonedDocUsingXattrs(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus until https://github.com/couchbase/sync_gateway/issues/2390")
	}

	if !base.TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}

	// This doesn't need to specify XATTR's because that is controlled by the test
	// env variable: SG_TEST_USE_XATTRS
	rt := NewRestTester(t, nil)
	defer rt.Close()

	bulkDocsBody := `
{
  "docs": [
  	{
         "_id":"-21SK00U-ujxUO9fU2HezxL",
         "_rev":"2-466a1fab90a810dc0a63565b70680e4e",
         "_deleted":true,
         "_revisions":{
            "ids":[
               "466a1fab90a810dc0a63565b70680e4e",
               "9e1084304cd2e60c5c106b308a82f40e"
            ],
            "start":2
         }
  	}
  ],
  "new_edits": false
}
`

	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", bulkDocsBody)
	log.Printf("Response: %s", response.Body)

	bulkDocsResponse := []map[string]interface{}{}
	err := base.JSONUnmarshal(response.Body.Bytes(), &bulkDocsResponse)
	assert.NoError(t, err, "Unexpected error")
	log.Printf("bulkDocsResponse: %+v", bulkDocsResponse)
	for _, bulkDocResponse := range bulkDocsResponse {
		bulkDocErr, gotErr := bulkDocResponse["error"]
		if gotErr && bulkDocErr.(string) == "not_found" {
			t.Errorf("The bulk docs response had an embedded error: %v.  Response for this doc: %+v", bulkDocErr, bulkDocsResponse)
		}
	}

	// Fetch the xattr and make sure it contains the above value
	baseBucket := rt.GetDatabase().Bucket
	subdocXattrStore, _ := base.AsSubdocXattrStore(baseBucket)
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err = subdocXattrStore.SubdocGetBodyAndXattr("-21SK00U-ujxUO9fU2HezxL", base.SyncXattrName, "", &retrievedVal, &retrievedXattr, nil)
	assert.NoError(t, err, "Unexpected Error")
	assert.Equal(t, "2-466a1fab90a810dc0a63565b70680e4e", retrievedXattr["rev"])

}

// Reproduces https://github.com/couchbase/sync_gateway/issues/916.  The test-only RestartListener operation used to simulate a
// SG restart isn't race-safe, so disabling the test for now.  Should be possible to reinstate this as a proper unit test
// once we add the ability to take a bucket offline/online.
func TestLongpollWithWildcard(t *testing.T) {
	// TODO: Test disabled because it fails with -race
	t.Skip("WARNING: TEST DISABLED")

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.True(t, err == nil)
	assert.NoError(t, a.Save(bernard))

	// Issue is only reproducible when the wait counter is zero for all requested channels (including the user channel) - the count=0
	// triggers early termination of the changes loop.  This can only be reproduced if the feed is restarted after the user is created -
	// otherwise the count for the user pseudo-channel will always be non-zero
	db, _ := rt.ServerContext().GetDatabase("db")
	err = db.RestartListener()
	assert.True(t, err == nil)
	// Put a document to increment the counter for the * channel
	response := rt.Send(request("PUT", "/db/lost", `{"channel":["ABC"]}`))
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
		err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
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

	sc := NewServerContext(&StartupConfig{}, false)
	defer sc.Close()
	testProviderOnlyJSON := `{"name": "test_provider_only",
        			"server": "walrus:",
        			"bucket": "test_provider_only",
					"sgreplicate": {
						"enabled": false
					},
			        "unsupported": {
			          "oidc_test_provider": {
			            "enabled":true,
			            "unsigned_id_token":true
			          }
			        }
      			   }`

	var testProviderOnlyConfig DbConfig
	err := base.JSONUnmarshal([]byte(testProviderOnlyJSON), &testProviderOnlyConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(DatabaseConfig{DbConfig: testProviderOnlyConfig})
	assert.NoError(t, err, "Error adding testProviderOnly database.")

	viewsOnlyJSON := `{"name": "views_only",
        			"server": "walrus:",
        			"bucket": "views_only",
					"sgreplicate": {
						"enabled": false
					},
			        "unsupported": {
			          "user_views": {
			            "enabled":true
			          }
			        }
      			   }`

	var viewsOnlyConfig DbConfig
	err = base.JSONUnmarshal([]byte(viewsOnlyJSON), &viewsOnlyConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(DatabaseConfig{DbConfig: viewsOnlyConfig})
	assert.NoError(t, err, "Error adding viewsOnlyConfig database.")

	viewsAndTestJSON := `{"name": "views_and_test",
        			"server": "walrus:",
        			"bucket": "views_and_test",
					"sgreplicate": {
						"enabled": false
					},
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
	err = base.JSONUnmarshal([]byte(viewsAndTestJSON), &viewsAndTestConfig)
	assert.True(t, err == nil)

	_, err = sc.AddDatabaseFromConfig(DatabaseConfig{DbConfig: viewsAndTestConfig})
	assert.NoError(t, err, "Error adding viewsAndTestConfig database.")
}

func TestImportingPurgedDocument(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus until https://github.com/couchbase/sync_gateway/issues/2390")
	}

	if !base.TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	body := `{"_purged": true, "foo": "bar"}`
	ok, err := rt.Bucket().Add("key", 0, []byte(body))
	assert.NoError(t, err)
	assert.True(t, ok)

	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	response := rt.SendRequest("GET", "/db/key", "")
	fmt.Println(response.Body)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, numErrors, numErrorsAfter)
}

func TestDocIDFilterResurrection(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create User
	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))
	jacques, err := a.NewUser("jacques", "letmein", channels.SetOf(t, "A", "B"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(jacques))

	// Create Doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": ["A"]}`)
	assert.Equal(t, http.StatusCreated, response.Code)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevID := body["rev"].(string)

	// Delete Doc
	response = rt.SendAdminRequest("DELETE", "/db/doc1?rev="+docRevID, "")
	assert.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevID2 := body["rev"].(string)

	// Update / Revive Doc
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevID2, `{"channels": ["B"]}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	require.NoError(t, rt.WaitForPendingChanges())

	// Changes call
	request, _ := http.NewRequest("GET", "/db/_changes", nil)
	request.SetBasicAuth("jacques", "letmein")
	response = rt.Send(request)
	assert.Equal(t, http.StatusOK, response.Code)

	var changesResponse = make(map[string]interface{})
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changesResponse))
	assert.NotContains(t, changesResponse["results"].([]interface{})[1], "deleted")
}

func TestSyncFunctionErrorLogging(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyJavascript)

	rtConfig := RestTesterConfig{SyncFn: `
		function(doc) {
			console.error("Error");
			console.log("Log");
			channel(doc.channel);
		}`}

	rt := NewRestTester(t, &rtConfig)

	defer rt.Close()

	// Wait for the DB to be ready before attempting to get initial error count
	assert.NoError(t, rt.WaitForDBOnline())

	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar"}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, numErrors+1, numErrorsAfter)
}

func TestConflictWithInvalidAttachment(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create Doc
	docrevId := rt.createDoc(t, "doc1")

	docRevDigest := strings.Split(docrevId, "-")[1]

	// Setup Attachment
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}

	// Set attachment
	attachmentBody := "aGVsbG8gd29ybGQ=" // hello.txt
	response := rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1?rev="+docrevId, attachmentBody, reqHeaders)
	assertStatus(t, response, http.StatusCreated)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docrevId2 := body["rev"].(string)

	// Update Doc
	rev3Input := `{"_attachments":{"attach1":{"content-type": "content/type", "digest":"sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub": true}}, "_id": "doc1", "_rev": "` + docrevId2 + `", "prop":true}`
	response = rt.SendAdminRequest("PUT", "/db/doc1", rev3Input)
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docrevId3 := body["rev"].(string)

	// Get Existing Doc & Update rev
	rev4Input := `{"_attachments":{"attach1":{"content-type": "content/type", "digest":"sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub": true}}, "_id": "doc1", "_rev": "` + docrevId3 + `", "prop":true}`
	response = rt.SendAdminRequest("PUT", "/db/doc1", rev4Input)
	assertStatus(t, response, http.StatusCreated)

	// Get Existing Doc to Modify
	response = rt.SendAdminRequest("GET", "/db/doc1?revs=true", "")
	assertStatus(t, response, http.StatusOK)
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
	assertStatus(t, response, http.StatusBadRequest)
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

	ok, err := rt.GetDatabase().Bucket.Add("doc1", 0, []byte(`{"_attachments":{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1,"stub":true}},"_sync":{"rev":"1-6e5a9ed9e2e8637d495ac5dd2fa90479","sequence":2,"recent_sequences":[2],"history":{"revs":["1-6e5a9ed9e2e8637d495ac5dd2fa90479"],"parents":[-1],"channels":[null]},"cas":"","time_saved":"2019-12-06T20:02:25.523013Z"},"test":true}`))
	require.NoError(t, err)
	require.True(t, ok)

	response := rt.SendAdminRequest("PUT", "/db/doc1?rev=1-6e5a9ed9e2e8637d495ac5dd2fa90479", `{"test":false,"_attachments":{"hello.txt":{"stub":true,"revpos":1}}}`)
	assertStatus(t, response, 201)
	var putResp struct {
		OK  bool   `json:"ok"`
		Rev string `json:"rev"`
	}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &putResp))
	require.True(t, putResp.OK)

	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	assertStatus(t, response, 200)
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
	docRevId := rt.createDoc(t, "doc1")
	docRevDigest := strings.Split(docRevId, "-")[1]

	// //Create diverging tree
	var body db.Body

	reqBodyRev2 := `{"_rev": "2-two", "_revisions": {"ids": ["two", "` + docRevDigest + `"], "start": 2}}`
	response := rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", reqBodyRev2)
	assertStatus(t, response, http.StatusCreated)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId2 := body["rev"].(string)
	assert.Equal(t, "2-two", docRevId2)

	reqBodyRev2a := `{"_rev": "2-two", "_revisions": {"ids": ["twoa", "` + docRevDigest + `"], "start": 2}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", reqBodyRev2a)
	assertStatus(t, response, http.StatusCreated)
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
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId3 := body["rev"].(string)

	// Put attachment on doc1 conflicting rev 2a
	rev3aAttachment := `Z29vZGJ5ZSBjcnVlbCB3b3JsZA==` // bye.txt
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach1a?rev=2-twoa", rev3aAttachment, reqHeaders)
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId3a := body["rev"].(string)

	// Perform small update on doc3
	rev4Body := `{"_id": "doc1", "_attachments": {"attach1": {"content_type": "content/type", "digest": "sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 3, "stub":true}}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId3, rev4Body)
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId4 := body["rev"].(string)

	// Perform small update on doc3a
	rev4aBody := `{"_id": "doc1", "_attachments": {"attach1a": {"content_type": "content/type", "digest": "sha1-rdfKyt3ssqPHnWBUxl/xauXXcUs=", "length": 28, "revpos": 3, "stub": true}}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId3a, rev4aBody)
	assertStatus(t, response, http.StatusCreated)
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
	docRevId := rt.createDoc(t, "doc1")
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
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId2 := body["rev"].(string)

	// Create rev 3, preserve the attachment
	rev3Body := `{"_id": "doc1", "mod":"mod_3", "_attachments": {"attach1": {"content_type": "content/type", "digest": "sha1-b7fDq/pHG8Nf5F3fe0K2nu0xcw0=", "length": 16, "revpos": 2, "stub":true}}}`
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev="+docRevId2, rev3Body)
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevId3 := body["rev"].(string)

	// Add another attachment at rev 4
	rev4Attachment := `Z29vZGJ5ZSBjcnVlbCB3b3JsZA==` // bye.txt
	response = rt.SendAdminRequestWithHeaders("PUT", "/db/doc1/attach2?rev="+docRevId3, rev4Attachment, reqHeaders)
	assertStatus(t, response, http.StatusCreated)
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
	assertStatus(t, response, http.StatusCreated)
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
	assertStatus(t, response, http.StatusCreated)
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
	assertStatus(t, response, http.StatusCreated)
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
	assertStatus(t, response, http.StatusOK)

	// Retrieve current winning rev with attachments
	var rev7Response db.Body
	response = rt.SendAdminRequest("GET", "/db/doc1?atts_since=[\""+docRevId+"\"]", "")
	log.Printf("Rev6 GET: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &rev7Response))
	_, attachmentsPresent = rev7Response["_attachments"]
	assert.True(t, attachmentsPresent)
}

func TestNumAccessErrors(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc, oldDoc){if (doc.channels.indexOf("foo") > -1){requireRole("foobar")}}`,
	}

	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t))

	// Create a test user
	user, err := a.NewUser("user", "letmein", channels.SetOf(t, "A"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	response := rt.Send(requestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user"))
	assertStatus(t, response, 403)

	base.WaitForStat(func() int64 { return rt.GetDatabase().DbStats.SecurityStats.NumAccessErrors.Value() }, 1)
}

func TestNonImportedDuplicateID(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Skip this test under walrus testing")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	bucket := rt.Bucket()
	body := `{"foo":"bar"}`
	ok, err := bucket.Add("key", 0, []byte(body))

	assert.True(t, ok)
	assert.Nil(t, err)
	res := rt.SendAdminRequest("PUT", "/db/key", `{"prop":true}`)
	assertStatus(t, res, http.StatusConflict)
}

func TestChanCacheActiveRevsStat(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	responseBody := make(map[string]interface{})
	response := rt.SendAdminRequest("PUT", "/db/testdoc", `{"value":"a value", "channels":["a"]}`)
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)
	rev1 := fmt.Sprint(responseBody["rev"])
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/testdoc2", `{"value":"a value", "channels":["a"]}`)
	err = base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)
	rev2 := fmt.Sprint(responseBody["rev"])
	assertStatus(t, response, http.StatusCreated)

	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	response = rt.SendAdminRequest("GET", "/db/_changes?active_only=true&include_docs=true&filter=sync_gateway/bychannel&channels=a&feed=normal&since=0&heartbeat=0&timeout=300000", "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("PUT", "/db/testdoc?new_edits=true&rev="+rev1, `{"value":"a value", "channels":[]}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/testdoc2?new_edits=true&rev="+rev2, `{"value":"a value", "channels":[]}`)
	assertStatus(t, response, http.StatusCreated)

	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Cache().ChannelCacheRevsActive.Value()))

}

func TestGetRawDocumentError(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/_raw/doc", ``)
	assert.Equal(t, http.StatusNotFound, response.Code)
}

func TestWebhookProperties(t *testing.T) {

	wg := sync.WaitGroup{}

	handler := func(w http.ResponseWriter, r *http.Request) {
		out, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		err = r.Body.Close()
		assert.NoError(t, err)

		var body db.Body
		err = base.JSONUnmarshal(out, &body)
		assert.NoError(t, err)
		assert.Contains(t, string(out), db.BodyId)
		assert.Contains(t, string(out), db.BodyRev)
		assert.Equal(t, "1-cd809becc169215072fd567eebd8b8de", body[db.BodyRev])
		wg.Done()
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
				EventHandlers: &EventHandlerConfig{
					DocumentChanged: []*EventConfig{
						{Url: s.URL, Filter: "function(doc){return true;}", HandlerType: "webhook"},
					},
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar"}`)
	wg.Add(1)

	if base.TestUseXattrs() {
		body := make(map[string]interface{})
		body["foo"] = "bar"
		added, err := rt.Bucket().Add("doc2", 0, body)
		assert.True(t, added)
		assert.NoError(t, err)
		wg.Add(1)
	}

	wg.Wait()

}

func TestBasicGetReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// Put document as usual
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar"}`)
	assertStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)

	// Get a document with rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true&rev="+revID, ``)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}

	// Get a document without specifying rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true", ``)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}
}

func TestAttachmentGetReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// Put document as usual with attachment
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar", "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	assertStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))

	// Get a document with rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true", ``)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
		assert.Contains(t, body[db.BodyAttachments], "hello.txt")
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}
}

func TestBasicPutReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var (
		body  db.Body
		revID string
		err   error
	)

	response := rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true", `{}`)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}

	// Put basic doc with replicator2 flag and ensure it saves correctly
	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID, `{"foo": "bar"}`)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("GET", "/db/doc1", ``)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		assertStatus(t, response, http.StatusNotFound)
	}
}

func TestDeletedPutReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	response := rt.SendAdminRequest("PUT", "/db/doc1", "{}")
	assertStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().NumDocWrites.Value())

	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID+"&deleted=true", "{}")
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/db/doc1", ``)
		assertStatus(t, response, http.StatusNotFound)
		assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID+"&deleted=false", `{}`)
	if base.IsEnterpriseEdition() {
		assertStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 3, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/db/doc1", ``)
		assertStatus(t, response, http.StatusOK)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		assertStatus(t, response, http.StatusNotImplemented)
	}
}

func TestWebhookSpecialProperties(t *testing.T) {

	wg := sync.WaitGroup{}

	handler := func(w http.ResponseWriter, r *http.Request) {
		wg.Done()

		var body db.Body
		d := base.JSONDecoder(r.Body)
		require.NoError(t, d.Decode(&body))
		require.Contains(t, body, db.BodyId)
		require.Contains(t, body, db.BodyRev)
		require.Contains(t, body, db.BodyDeleted)
		assert.True(t, body[db.BodyDeleted].(bool))
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
				EventHandlers: &EventHandlerConfig{
					DocumentChanged: []*EventConfig{
						{Url: s.URL, Filter: "function(doc){return true;}", HandlerType: "webhook"},
					},
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	wg.Add(1)
	res := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"foo": "bar", "_deleted": true}`)
	assertStatus(t, res, http.StatusCreated)
	wg.Wait()
}

func TestWebhookPropsWithAttachments(t *testing.T) {
	wg := sync.WaitGroup{}
	handler := func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		bodyBytes, err := ioutil.ReadAll(r.Body)
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
	assertStatus(t, response, http.StatusCreated)
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
	assertStatus(t, response, http.StatusCreated)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.True(t, body["ok"].(bool))
	revIdAfterAttachment := body["rev"].(string)
	assert.NotEmpty(t, revIdAfterAttachment, "No revid in response for PUT attachment")
	assert.NotEqual(t, revIdAfterAttachment, doc1revId)
	wg.Wait()
}

// TestWebhookWinningRevChangedEvent ensures the winning_rev_changed event is only fired for a winning revision change, and checks that document_changed is always fired.
func TestWebhookWinningRevChangedEvent(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyEvents)

	wg := sync.WaitGroup{}

	var WinningRevChangedCount uint32
	var DocumentChangedCount uint32

	handler := func(w http.ResponseWriter, r *http.Request) {
		var body db.Body
		d := base.JSONDecoder(r.Body)
		require.NoError(t, d.Decode(&body))
		require.Contains(t, body, db.BodyId)
		require.Contains(t, body, db.BodyRev)

		event := r.URL.Query().Get("event")
		switch event {
		case "WinningRevChanged":
			atomic.AddUint32(&WinningRevChangedCount, 1)
		case "DocumentChanged":
			atomic.AddUint32(&DocumentChangedCount, 1)
		default:
			t.Fatalf("unknown event type: %s", event)
		}

		wg.Done()
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			EventHandlers: &EventHandlerConfig{
				DocumentChanged: []*EventConfig{
					{Url: s.URL + "?event=DocumentChanged", Filter: "function(doc){return true;}", HandlerType: "webhook"},
					{Url: s.URL + "?event=WinningRevChanged", Filter: "function(doc){return true;}", HandlerType: "webhook",
						Options: map[string]interface{}{db.EventOptionDocumentChangedWinningRevOnly: true},
					},
				},
			},
		},
		}}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	wg.Add(2)
	res := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`)
	assertStatus(t, res, http.StatusCreated)
	rev1 := respRevID(t, res)
	_, rev1Hash := db.ParseRevID(rev1)

	// push winning branch
	wg.Add(2)
	res = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", `{"foo":"buzz","_revisions":{"start":3,"ids":["buzz","bar","`+rev1Hash+`"]}}`)
	assertStatus(t, res, http.StatusCreated)

	// push non-winning branch
	wg.Add(1)
	res = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", `{"foo":"buzzzzz","_revisions":{"start":2,"ids":["buzzzzz","`+rev1Hash+`"]}}`)
	assertStatus(t, res, http.StatusCreated)

	wg.Wait()
	assert.Equal(t, 2, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 3, int(atomic.LoadUint32(&DocumentChangedCount)))

	// tombstone the winning branch and ensure we get a rev changed message for the promoted branch
	wg.Add(2)
	res = rt.SendAdminRequest("DELETE", "/db/doc1?rev=3-buzz", ``)
	assertStatus(t, res, http.StatusOK)

	wg.Wait()
	assert.Equal(t, 3, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 4, int(atomic.LoadUint32(&DocumentChangedCount)))

	// push a separate winning branch
	wg.Add(2)
	res = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", `{"foo":"quux","_revisions":{"start":4,"ids":["quux", "buzz","bar","`+rev1Hash+`"]}}`)
	assertStatus(t, res, http.StatusCreated)

	// tombstone the winning branch, we should get a second webhook fired for rev 2-buzzzzz now it's been resurrected
	wg.Add(2)
	res = rt.SendAdminRequest("DELETE", "/db/doc1?rev=4-quux", ``)
	assertStatus(t, res, http.StatusOK)

	wg.Wait()
	assert.Equal(t, 5, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 6, int(atomic.LoadUint32(&DocumentChangedCount)))
}

func Benchmark_RestApiGetDocPerformance(b *testing.B) {

	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	prt := NewRestTester(b, nil)
	defer prt.Close()

	// Create test document
	prt.SendRequest("PUT", "/db/doc", `{"prop":true}`)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			prt.SendRequest("GET", "/db/doc", "")
		}
	})
}

var threekdoc = `{"cols":["Name","Address","Location","phone"],"data":[["MelyssaF.Stokes","Ap#166-9804ProinSt.","52.01352,-9.4151","(306)773-3853"],["RuthT.Richards","Ap#180-8417TemporRoad","8.07909,-118.55952","(662)733-8043"],["CedricN.Witt","Ap#575-4625NuncSt.","74.419,153.71285","(850)995-0417"],["ElianaF.Ashley","Ap#169-2030Nibh.St.","87.98632,97.47442","(903)272-5949"],["ChesterJ.Holland","2905ProinSt.","-43.14706,-64.25893","(911)435-9200"],["AleaT.Bishop","Ap#493-4894ConvallisRd.","42.54157,64.98534","(479)848-2988"],["HerrodT.Barron","Ap#822-1444EtAvenue","9.50706,-111.54064","(390)300-8534"],["YoshiP.Sanchez","Ap#796-4679Arcu.Avenue","-16.49557,-137.69","(913)606-8930"],["GrahamO.Velazquez","415EratRd.","-5.30634,171.81751","(691)700-3072"],["BryarF.Sargent","Ap#180-6507Lacus.St.","17.64959,-19.93008","(516)890-6469"],["XerxesM.Gaines","370-1967NislStreet","-39.28978,-23.74924","(461)907-9563"],["KayI.Jones","565-351ElitAve","25.58317,17.43545","(145)441-5007"],["ImaZ.Curry","Ap#143-8377MagnaAve","-86.72025,-161.94081","(484)924-8145"],["GiselleW.Macdonald","962AdipiscingRoad","-21.55826,-121.06657","(137)255-2241"],["TarikJ.Kane","P.O.Box447,5949PhasellusSt.","57.28914,-125.89595","(356)758-8271"],["ChristopherJ.Travis","5246InRd.","-69.12682,31.20181","(298)963-1855"],["QuinnT.Pace","P.O.Box935,212Laoreet,St.","-62.00241,1.31111","(157)419-0182"],["BrentK.Guy","156-417LoremSt.","26.67571,-29.35786","(125)687-6610"],["JocelynN.Cash","Ap#502-9209VehiculaSt.","-26.05925,160.61357","(782)351-4211"],["DaphneS.King","571-1485FringillaRoad","-76.33262,-142.5655","(356)476-4508"],["MicahJ.Eaton","3468ProinRd.","61.30187,-128.8584","(942)467-7558"],["ChaneyM.Gay","444-1647Pede.Rd.","84.32739,-43.59781","(386)231-0872"],["LacotaM.Guerra","9863NuncRoad","21.81253,-54.90423","(694)443-8520"],["KimberleyY.Jensen","6403PurusSt.","67.5704,65.90554","(181)309-7780"],["JenaY.Brennan","Ap#533-7088MalesuadaStreet","78.58624,-172.89351","(886)688-0617"],["CarterK.Dotson","Ap#828-1931IpsumAve","59.54845,53.30366","(203)546-8704"],["EllaU.Buckner","Ap#141-1401CrasSt.","78.34425,-172.24474","(214)243-6054"],["HamiltonE.Estrada","8676Iaculis,St.","11.67468,-130.12233","(913)624-2612"],["IanT.Saunders","P.O.Box519,3762DictumRd.","-10.97019,73.47059","(536)391-7018"],["CairoK.Craft","6619Sem.St.","9.28931,-5.69682","(476)804-7898"],["JohnB.Norman","Ap#865-7121CubiliaAve","50.96552,-126.5271","(309)323-6975"],["SawyerD.Hale","Ap#512-820EratRd.","-65.1931,166.14822","(180)527-8987"],["CiaranQ.Cole","P.O.Box262,9220SedAvenue","69.753,121.39921","(272)654-8755"],["BrandenJ.Thompson","Ap#846-5470MetusAv.","44.61386,-44.18375","(388)776-0689"]]}`

func Benchmark_RestApiPutDocPerformanceDefaultSyncFunc(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	prt := NewRestTester(b, nil)
	defer prt.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// PUT a new document until test run has completed
		for pb.Next() {
			docid, err := base.GenerateRandomID()
			require.NoError(b, err)
			prt.SendRequest("PUT", fmt.Sprintf("/db/doc-%v", docid), threekdoc)
		}
	})
}

func Benchmark_RestApiPutDocPerformanceExplicitSyncFunc(b *testing.B) {

	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	qrtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	qrt := NewRestTester(b, &qrtConfig)
	defer qrt.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// PUT a new document until test run has completed
		for pb.Next() {
			docid, err := base.GenerateRandomID()
			require.NoError(b, err)
			qrt.SendRequest("PUT", fmt.Sprintf("/db/doc-%v", docid), threekdoc)
		}
	})
}

func Benchmark_RestApiGetDocPerformanceFullRevCache(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelWarn, base.KeyHTTP)
	// Create test documents
	rt := NewRestTester(b, nil)
	defer rt.Close()
	keys := make([]string, 5000)
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("doc%d", i)
		keys[i] = key
		rt.SendRequest("PUT", "/db/"+key, `{"prop":true}`)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			key := keys[rand.Intn(5000)]
			rt.SendRequest("GET", "/db/"+key+"?rev=1-45ca73d819d5b1c9b8eea95290e79004", "")
		}
	})
}

func TestHandleProfiling(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	dirPath, err := ioutil.TempDir("", "pprof")
	require.NoError(t, err, "Temp directory should be created")
	defer func() { assert.NoError(t, os.RemoveAll(dirPath)) }()

	tests := []struct {
		inputProfile string
	}{
		{inputProfile: "goroutine"},
		{inputProfile: "threadcreate"},
		{inputProfile: "heap"},
		{inputProfile: "allocs"},
		{inputProfile: "block"},
		{inputProfile: "mutex"},
	}

	for _, tc := range tests {
		// Send a valid profile request.
		resource := fmt.Sprintf("/_profile/%v", tc.inputProfile)
		filePath := filepath.Join(dirPath, fmt.Sprintf("%s.pprof", tc.inputProfile))
		reqBodyText := fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
		response := rt.SendAdminRequest(http.MethodPost, resource, reqBodyText)
		assertStatus(t, response, http.StatusOK)
		fi, err := os.Stat(filePath)
		assert.NoError(t, err, "fetching the file information")
		assert.True(t, fi.Size() > 0)

		// Send profile request with missing JSON 'file' parameter.
		response = rt.SendAdminRequest(http.MethodPost, resource, "{}")
		assertStatus(t, response, http.StatusBadRequest)
		assert.Contains(t, string(response.BodyBytes()), "Missing JSON 'file' parameter")

		// Send a profile request with invalid json body
		response = rt.SendAdminRequest(http.MethodPost, resource, "invalid json body")
		assertStatus(t, response, http.StatusBadRequest)
		assert.Contains(t, string(response.BodyBytes()), "Invalid JSON")

		// Send a profile request with unknown file path; Internal Server Error
		reqBodyText = `{"file":"sftp://unknown/path"}`
		response = rt.SendAdminRequest(http.MethodPost, resource, reqBodyText)
		assertStatus(t, response, http.StatusInternalServerError)
		assert.Contains(t, string(response.BodyBytes()), "Internal Server Error")
	}

	// Send profile request for a profile which doesn't exists; unknown
	filePath := filepath.Join(dirPath, "unknown.pprof")
	reqBodyText := fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
	response := rt.SendAdminRequest(http.MethodPost, "/_profile/unknown", reqBodyText)
	log.Printf("string(response.BodyBytes()): %v", string(response.BodyBytes()))
	assertStatus(t, response, http.StatusNotFound)
	assert.Contains(t, string(response.BodyBytes()), `No such profile \"unknown\"`)

	// Send profile request with filename and empty profile name; it should end up creating cpu profile
	filePath = filepath.Join(dirPath, "cpu.pprof")
	reqBodyText = fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
	response = rt.SendAdminRequest(http.MethodPost, "/_profile", reqBodyText)
	log.Printf("string(response.BodyBytes()): %v", string(response.BodyBytes()))
	assertStatus(t, response, http.StatusOK)
	fi, err := os.Stat(filePath)
	assert.NoError(t, err, "fetching the file information")
	assert.False(t, fi.Size() > 0)

	// Send profile request with no filename and profile name; it should stop cpu profile
	response = rt.SendAdminRequest(http.MethodPost, "/_profile", "")
	log.Printf("string(response.BodyBytes()): %v", string(response.BodyBytes()))
	assertStatus(t, response, http.StatusOK)
	fi, err = os.Stat(filePath)
	assert.NoError(t, err, "fetching the file information")
	assert.True(t, fi.Size() > 0)
}

func TestHandleHeapProfiling(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	dirPath, err := ioutil.TempDir("", "heap-pprof")
	require.NoError(t, err, "Temp directory should be created")
	defer func() { assert.NoError(t, os.RemoveAll(dirPath)) }()

	// Send a valid request for heap profiling
	filePath := filepath.Join(dirPath, "heap.pprof")
	reqBodyText := fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
	response := rt.SendAdminRequest(http.MethodPost, "/_heap", reqBodyText)
	assertStatus(t, response, http.StatusOK)
	fi, err := os.Stat(filePath)
	assert.NoError(t, err, "fetching heap profile file information")
	assert.True(t, fi.Size() > 0)

	// Send a profile request with invalid json body
	response = rt.SendAdminRequest(http.MethodPost, "/_heap", "invalid json body")
	assertStatus(t, response, http.StatusBadRequest)
	assert.Contains(t, string(response.BodyBytes()), "Invalid JSON")

	// Send profile request with missing JSON 'file' parameter.
	response = rt.SendAdminRequest(http.MethodPost, "/_heap", "{}")
	assertStatus(t, response, http.StatusInternalServerError)
	assert.Contains(t, string(response.BodyBytes()), "Internal error: open")
}

func TestHandlePprofsCmdlineAndSymbol(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	tests := []struct {
		inputProfile string
	}{
		{inputProfile: "cmdline"},
		{inputProfile: "symbol"},
	}

	for _, tc := range tests {
		t.Run(tc.inputProfile, func(t *testing.T) {
			inputResource := fmt.Sprintf("/_debug/pprof/%v", tc.inputProfile)
			response := rt.SendAdminRequest(http.MethodGet, inputResource, "")
			assert.Equal(t, "", response.Header().Get("Content-Disposition"))
			assert.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			assertStatus(t, response, http.StatusOK)

			response = rt.SendAdminRequest(http.MethodPost, inputResource, "")
			assert.Equal(t, "", response.Header().Get("Content-Disposition"))
			assert.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			assertStatus(t, response, http.StatusOK)
		})
	}
}

func TestHandlePprofs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	rt := NewRestTester(t, nil)
	defer rt.Close()

	tests := []struct {
		inputProfile  string
		inputResource string
	}{
		{
			inputProfile:  "heap",
			inputResource: "/_debug/pprof/heap?seconds=1",
		},
		{
			inputProfile:  "profile",
			inputResource: "/_debug/pprof/profile?seconds=1",
		},
		{
			inputProfile:  "block",
			inputResource: "/_debug/pprof/block?seconds=1",
		},
		{
			inputProfile:  "threadcreate",
			inputResource: "/_debug/pprof/threadcreate",
		},
		{
			inputProfile:  "mutex",
			inputResource: "/_debug/pprof/mutex?seconds=1",
		},
		{
			inputProfile:  "goroutine",
			inputResource: "/_debug/pprof/goroutine?debug=0&gc=1",
		},
		{
			inputProfile:  "trace",
			inputResource: "/_debug/pprof/trace?seconds=1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.inputProfile, func(t *testing.T) {
			expectedContentDispositionPrefix := fmt.Sprintf(`attachment; filename="%v`, tc.inputProfile)
			response := rt.SendAdminRequest(http.MethodGet, tc.inputResource, "")
			cdHeader := response.Header().Get("Content-Disposition")
			assert.Truef(t, strings.HasPrefix(cdHeader, expectedContentDispositionPrefix), "Expected header prefix: %q but got %q", expectedContentDispositionPrefix, cdHeader)
			assert.Equal(t, "application/octet-stream", response.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			assertStatus(t, response, http.StatusOK)

			response = rt.SendAdminRequest(http.MethodPost, tc.inputResource, "")
			cdHeader = response.Header().Get("Content-Disposition")
			assert.Truef(t, strings.HasPrefix(cdHeader, expectedContentDispositionPrefix), "Expected header prefix: %q but got %q", expectedContentDispositionPrefix, cdHeader)
			assert.Equal(t, "application/octet-stream", response.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			assertStatus(t, response, http.StatusOK)
		})
	}
}

func TestHandleStats(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Get request for fetching runtime and other stats
	response := rt.SendAdminRequest(http.MethodGet, "/_stats", "")
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	assert.Contains(t, string(response.BodyBytes()), "MemStats")
	assertStatus(t, response, http.StatusOK)
}

// Try to create session with invalid cert but valid credentials
func TestSessionFail(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create user
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"name":"user1", "password":"letmein", "admin_channels":["user1"]}`)
	assertStatus(t, response, http.StatusCreated)

	id, err := base.GenerateRandomSecret()
	require.NoError(t, err)

	// Create fake, invalid session
	fakeSession := auth.LoginSession{
		ID:         id,
		Username:   "user1",
		Expiration: time.Now().Add(4 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	reqHeaders := map[string]string{
		"Cookie": auth.DefaultCookieName + "=" + fakeSession.ID,
	}

	// Attempt to create session with invalid cert but valid login
	response = rt.SendRequestWithHeaders("POST", "/db/_session", `{"name":"user1", "password":"letmein"}`, reqHeaders)
	assertStatus(t, response, http.StatusOK)
}

func TestImportOnWriteMigration(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test doesn't work with Walrus")
	}

	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Put doc with sync data / non-xattr
	key := "doc1"
	body := []byte(`{"_sync": { "rev": "1-fc2cf22c5e5007bd966869ebfe9e276a", "sequence": 1, "recent_sequences": [ 1 ], "history": { "revs": [ "1-fc2cf22c5e5007bd966869ebfe9e276a" ], "parents": [ -1], "channels": [ null ] }, "cas": "","value_crc32c": "", "time_saved": "2019-04-10T12:40:04.490083+01:00" }, "value": "foo"}`)
	ok, err := rt.Bucket().Add(key, 0, body)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Update doc with xattr - get 409, creates new rev, has old body
	response := rt.SendAdminRequest("PUT", "/db/doc1?rev=1-fc2cf22c5e5007bd966869ebfe9e276a", `{"value":"new"}`)
	assertStatus(t, response, http.StatusCreated)

	// Update doc with xattr - successful update
	response = rt.SendAdminRequest("PUT", "/db/doc1?rev=2-44ad6f128a2b1f75d0d0bb49b1fc0019", `{"value":"newer"}`)
	assertStatus(t, response, http.StatusCreated)
}

func TestAttachmentContentType(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
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
		assertStatus(t, response, http.StatusCreated)

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

// TestHideProductInfo ensures that detailed product info is not shown on non-admin REST API responses if set.
func TestHideProductInfo(t *testing.T) {
	tests := []struct {
		hideProductInfo, admin, expectedProductInfo bool
	}{
		{
			hideProductInfo:     false,
			admin:               false,
			expectedProductInfo: true,
		},
		{
			hideProductInfo:     false,
			admin:               true,
			expectedProductInfo: true,
		},
		{
			hideProductInfo:     true,
			admin:               true,
			expectedProductInfo: true,
		},
		{
			hideProductInfo:     true,
			admin:               false,
			expectedProductInfo: false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("hide:%v admin:%v", test.hideProductInfo, test.admin), func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{hideProductInfo: test.hideProductInfo})
			defer rt.Close()

			// admins can always see product info, even if setting is enabled
			if test.admin {
				resp := rt.SendAdminRequest(http.MethodGet, "/", "")
				assertStatus(t, resp, http.StatusOK)

				assert.Equal(t, base.VersionString, resp.Header().Get("Server"))

				body := string(resp.BodyBytes())
				assert.Contains(t, body, base.ProductVersionNumber)
				return
			}

			// non-admins can only see product info when the hideProductInfo option is disabled
			resp := rt.SendRequest(http.MethodGet, "/", "")
			assertStatus(t, resp, http.StatusOK)

			serverHeader := resp.Header().Get("Server")
			body := string(resp.BodyBytes())

			if test.hideProductInfo {
				assert.Equal(t, base.ProductNameString, serverHeader)
				assert.Contains(t, body, base.ProductNameString)
				// no versions
				assert.NotEqual(t, base.VersionString, serverHeader)
				assert.NotContains(t, body, base.ProductVersionNumber)
			} else {
				assert.Equal(t, base.VersionString, serverHeader)
				assert.Contains(t, body, base.ProductNameString)
				assert.Contains(t, body, base.ProductVersionNumber)
			}
		})
	}
}

func TestDeleteNonExistentDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("DELETE", "/db/fake", "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/fake", "")
	assertStatus(t, response, http.StatusNotFound)

	var body map[string]interface{}
	_, err := rt.GetDatabase().Bucket.Get("fake", &body)

	if base.TestUseXattrs() {
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
		assert.Nil(t, body)
	} else {
		assert.NoError(t, err)
	}
}

// CBG-1153
func TestDeleteEmptyBodyDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body
	response := rt.SendAdminRequest("PUT", "/db/doc1", "{}")
	assertStatus(t, response, http.StatusCreated)
	assert.NoError(t, json.Unmarshal(response.BodyBytes(), &body))
	rev := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/db/doc1?rev="+rev, "")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	assertStatus(t, response, http.StatusNotFound)

	var doc map[string]interface{}
	_, err := rt.GetDatabase().Bucket.Get("doc1", &doc)

	if base.TestUseXattrs() {
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
	} else {
		assert.NoError(t, err)
	}
}

func TestPutEmptyDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/doc", "{}")
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/db/doc", "")
	assertStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`, string(response.BodyBytes()))

	response = rt.SendAdminRequest("PUT", "/db/doc?rev=1-ca9ad22802b66f662ff171f226211d5c", `{"val": "newval"}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/db/doc", "")
	assertStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"2-2f981cadffde70e8a1d9dc386a410e0d","val":"newval"}`, string(response.BodyBytes()))
}

func TestTombstonedBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	assertStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err := rt.GetDatabase().Bucket.Get(t.Name(), &body)

	if base.TestUseXattrs() {
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
		assert.Nil(t, body)
	} else {
		assert.NoError(t, err)
	}
}

func TestTombstonedBulkDocsWithPriorPurge(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: `function(doc,oldDoc){
			console.log("doc:"+JSON.stringify(doc))
			console.log("oldDoc:"+JSON.stringify(oldDoc))
		}`,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{AutoImport: false}}, // prevent importing the doc before the purge
	})
	defer rt.Close()

	bucket := rt.Bucket()
	_, ok := base.AsCouchbaseStore(bucket)
	if !ok {
		t.Skip("Requires Couchbase bucket")
	}

	_, err := bucket.Add(t.Name(), 0, map[string]interface{}{"val": "val"})
	require.NoError(t, err)

	resp := rt.SendAdminRequest("POST", "/db/_purge", `{"`+t.Name()+`": ["*"]}`)
	assertStatus(t, resp, http.StatusOK)

	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	assertStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err = rt.GetDatabase().Bucket.Get(t.Name(), &body)

	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
	assert.Nil(t, body)

}

func TestTombstonedBulkDocsWithExistingTombstone(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: `function(doc,oldDoc){
			console.log("doc:"+JSON.stringify(doc))
			console.log("oldDoc:"+JSON.stringify(oldDoc))
		}`,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{AutoImport: false}}, // prevent importing the doc before the delete
	})
	defer rt.Close()

	bucket := rt.Bucket()
	_, ok := base.AsCouchbaseStore(bucket)
	if !ok {
		t.Skip("Requires Couchbase bucket")
	}

	// Create the document to trigger cas failure
	value := make(map[string]interface{})
	value["foo"] = "bar"
	insCas, err := bucket.WriteCas(t.Name(), 0, 0, 0, value, 0)
	require.NoError(t, err)

	// Delete document
	_, err = bucket.Remove(t.Name(), insCas)
	require.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	assertStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err = rt.GetDatabase().Bucket.Get(t.Name(), &body)

	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
	assert.Nil(t, body)
}

// This test is skipped usually as it requires code to be manually injected into bucket_gocb.go
func TestPutTombstoneWithoutCreateAsDeletedFlagCasFailure(t *testing.T) {
	t.Skip("Requires manual intervention to run")

	// In order to run this test obviously remove the above skip and then:
	// Insert the below as global vars in base:
	// ==========================
	// var RunXattrCallback bool
	// var UpdateXattrCallback func()
	// ==========================

	// Insert the below into UpdateTombstoneXattr in bucket_gocb.go before the 'if requiresBodyRemoval' check
	// and after the first 'Kick off retry loop' block
	// ==========================
	// if RunXattrCallback {
	//		RunXattrCallback = false
	//		UpdateXattrCallback()
	//	}
	// ==========================

	// Finally uncomment the lines below setting the callback to true and the callback

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Couchbase buckets only")
	}

	rt := NewRestTester(t, &RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{AutoImport: true}}})
	defer rt.Close()

	gocbBucket, ok := base.AsGoCBBucket(rt.Bucket())
	assert.True(t, ok)

	// Force it to fall into the non CreateAsDeleted flag handling
	gocbBucket.OverrideClusterCompatVersion(5, 5)

	// base.RunXattrCallback = true
	// base.UpdateXattrCallback = func() {
	// 	response := rt.SendAdminRequest("PUT", "/db/doc", `{"val": "update"}`)
	// 	assertStatus(t, response, http.StatusCreated)
	// }

	response := rt.SendAdminRequest("PUT", "/db/doc", `{"_deleted": true, "val": "original"}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/db/doc", "")
	assertStatus(t, response, http.StatusOK)

	var body db.Body
	err := json.Unmarshal(response.BodyBytes(), &body)
	assert.NoError(t, err)

	assert.Equal(t, "update", body["val"])

	// If xattrs being used ensure that this doc operation wasn't treated as an import (cas set correctly)
	if base.TestUseXattrs() {
		assert.Equal(t, 0, int(rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value()))
	}
}

func TestUptimeStat(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	uptime1, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().Uptime.String())
	require.NoError(t, err)
	time.Sleep(10 * time.Nanosecond)

	uptime2, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().Uptime.String())
	require.NoError(t, err)
	log.Printf("uptime1: %d, uptime2: %d", uptime1, uptime2)
	assert.True(t, uptime1 < uptime2)
}

func TestUserXattrAutoImport(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   true,
			UserXattrKey: xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
	})

	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.Bucket())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	// Add doc
	resp := rt.SendAdminRequest("PUT", "/db/"+docKey, "{}")
	assertStatus(t, resp, http.StatusCreated)

	// Add xattr to doc
	_, err := userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Wait for doc to be imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has ran twice (once for PUT and once for xattr addition)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Get Xattr and ensure channel value set correctly
	var syncData db.SyncData
	subdocXattrStore, ok := base.AsSubdocXattrStore(rt.Bucket())
	require.True(t, ok)
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

	// Update xattr again but same value and ensure it isn't imported again (crc32 hash should match)
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value() == 1
	})
	assert.NoError(t, err)

	var syncData2 db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	assert.Equal(t, syncData.Crc32c, syncData2.Crc32c)
	assert.Equal(t, syncData.Crc32cUserXattr, syncData2.Crc32cUserXattr)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	// Update body but same value and ensure it isn't imported again (crc32 hash should match)
	err = rt.Bucket().Set(docKey, 0, nil, map[string]interface{}{})
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value() == 2
	})
	assert.NoError(t, err)

	var syncData3 db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData3)
	assert.NoError(t, err)

	assert.Equal(t, syncData2.Crc32c, syncData3.Crc32c)
	assert.Equal(t, syncData2.Crc32cUserXattr, syncData3.Crc32cUserXattr)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	// Update body and ensure import occurs
	updateVal := []byte(`{"prop":"val"}`)
	err = rt.Bucket().Set(docKey, 0, nil, updateVal)
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(3), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	var syncData4 db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData4)
	assert.NoError(t, err)

	assert.Equal(t, base.Crc32cHashString(updateVal), syncData4.Crc32c)
	assert.Equal(t, syncData3.Crc32cUserXattr, syncData4.Crc32cUserXattr)
}

func TestUserXattrOnDemandImportGET(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   false,
			UserXattrKey: xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
	})

	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.Bucket())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}
	subdocXattrStore, ok := base.AsSubdocXattrStore(rt.Bucket())
	require.True(t, ok)

	// Add doc with SDK
	err := rt.Bucket().Set(docKey, 0, nil, []byte(`{}`))
	assert.NoError(t, err)

	// GET to trigger import
	resp := rt.SendAdminRequest("GET", "/db/"+docKey, "")
	assertStatus(t, resp, http.StatusOK)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has been ran on import
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Write user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// GET to trigger import
	resp = rt.SendAdminRequest("GET", "/db/"+docKey, "")
	assertStatus(t, resp, http.StatusOK)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	// Ensure sync function has ran on import
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Get sync data for doc and ensure user xattr has been used correctly to set channel
	var syncData db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

	// Write same xattr value
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Perform GET and ensure import isn't triggered as crc32 hash is the same
	resp = rt.SendAdminRequest("GET", "/db/"+docKey, "")
	assertStatus(t, resp, http.StatusOK)

	var syncData2 db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	assert.Equal(t, syncData.Crc32c, syncData2.Crc32c)
	assert.Equal(t, syncData.Crc32cUserXattr, syncData2.Crc32cUserXattr)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
}

func TestUserXattrOnDemandImportWrite(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   false,
			UserXattrKey: xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
	})

	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.Bucket())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	// Initial PUT
	resp := rt.SendAdminRequest("PUT", "/db/"+docKey, `{}`)
	assertStatus(t, resp, http.StatusCreated)

	// SDK PUT
	err := rt.Bucket().Set(docKey, 0, nil, []byte(`{"update": "update"}`))
	assert.NoError(t, err)

	// Trigger Import
	resp = rt.SendAdminRequest("PUT", "/db/"+docKey, `{}`)
	assertStatus(t, resp, http.StatusConflict)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has ran on import
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Write user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Trigger import
	resp = rt.SendAdminRequest("PUT", "/db/"+docKey, `{"update": "update"}`)
	assertStatus(t, resp, http.StatusConflict)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	// Ensure sync function has ran on import
	assert.Equal(t, int64(3), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	subdocXattrStore, ok := base.AsSubdocXattrStore(rt.Bucket())
	require.True(t, ok)
	var syncData db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())
}

func TestRemovingUserXattr(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	testCases := []struct {
		name          string
		autoImport    bool
		importTrigger func(t *testing.T, rt *RestTester, docKey string)
	}{
		{
			name:       "GET",
			autoImport: false,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				resp := rt.SendAdminRequest("GET", "/db/"+docKey, "")
				assertStatus(t, resp, http.StatusOK)
			},
		},
		{
			name:       "PUT",
			autoImport: false,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				resp := rt.SendAdminRequest("PUT", "/db/"+docKey, "{}")
				assertStatus(t, resp, http.StatusConflict)
			},
		},
		{
			name:       "Auto",
			autoImport: true,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				// No op
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			docKey := testCase.name
			xattrKey := "myXattr"
			channelName := "testChan"

			// Sync function to set channel access to whatever xattr is
			rt := NewRestTester(t, &RestTesterConfig{
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					AutoImport:   testCase.autoImport,
					UserXattrKey: xattrKey,
				}},
				SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
			})

			defer rt.Close()

			gocbBucket, ok := base.AsUserXattrStore(rt.Bucket())
			if !ok {
				t.Skip("Test requires Couchbase Bucket")
			}

			// Initial PUT
			resp := rt.SendAdminRequest("PUT", "/db/"+docKey, `{}`)
			assertStatus(t, resp, http.StatusCreated)

			// Add xattr
			_, err := gocbBucket.WriteUserXattr(docKey, xattrKey, channelName)
			assert.NoError(t, err)

			// Trigger import
			testCase.importTrigger(t, rt, docKey)
			err = rt.WaitForCondition(func() bool {
				return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
			})
			assert.NoError(t, err)

			// Get sync data for doc and ensure user xattr has been used correctly to set channel
			var syncData db.SyncData
			subdocStore, ok := base.AsSubdocXattrStore(rt.Bucket())
			require.True(t, ok)
			_, err = subdocStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData)
			assert.NoError(t, err)

			assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

			// Delete user xattr
			_, err = gocbBucket.DeleteUserXattr(docKey, xattrKey)
			assert.NoError(t, err)

			// Trigger import
			testCase.importTrigger(t, rt, docKey)
			err = rt.WaitForCondition(func() bool {
				return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
			})
			assert.NoError(t, err)

			// Ensure old channel set with user xattr has been removed
			var syncData2 db.SyncData
			_, err = subdocStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData2)
			assert.NoError(t, err)

			assert.Equal(t, uint64(3), syncData2.Channels[channelName].Seq)
		})
	}
}

func TestUserXattrAvoidRevisionIDGeneration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   true,
			UserXattrKey: xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
					console.log(JSON.stringify(meta));
				}
			}`,
	})

	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.Bucket())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	subdocXattrStore, ok := base.AsSubdocXattrStore(rt.Bucket())
	require.True(t, ok)

	// Initial PUT
	resp := rt.SendAdminRequest("PUT", "/db/"+docKey, `{}`)
	assertStatus(t, resp, http.StatusCreated)

	require.NoError(t, rt.WaitForPendingChanges())

	// Get current sync data
	var syncData db.SyncData
	_, err := subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	docRev, err := rt.GetDatabase().GetRevisionCacheForTest().Get(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(docRev.Channels.ToArray()))
	assert.Equal(t, syncData.CurrentRev, docRev.RevID)

	// Write xattr to trigger import of user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure import worked and sequence incremented but that sequence did not
	var syncData2 db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	docRev2, err := rt.GetDatabase().GetRevisionCacheForTest().Get(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
	assert.NoError(t, err)
	assert.Equal(t, syncData2.CurrentRev, docRev2.RevID)

	assert.Equal(t, syncData.CurrentRev, syncData2.CurrentRev)
	assert.True(t, syncData2.Sequence > syncData.Sequence)
	assert.Equal(t, []string{channelName}, syncData2.Channels.KeySet())
	assert.Equal(t, syncData2.Channels.KeySet(), docRev2.Channels.ToArray())

	err = rt.Bucket().Set(docKey, 0, nil, []byte(`{"update": "update"}`))
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		log.Printf("Import count is: %v", rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	var syncData3 db.SyncData
	_, err = subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	assert.NotEqual(t, syncData2.CurrentRev, syncData3.CurrentRev)
}

func TestDocumentChannelHistory(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// Create doc in channel test and ensure a single channel history entry with only a start sequence
	// and no old channel history entries
	resp := rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["test"]}`)
	assertStatus(t, resp, http.StatusCreated)
	err := json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 1)
	assert.Equal(t, syncData.ChannelSet[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 0})
	assert.Len(t, syncData.ChannelSetHistory, 0)

	// Update doc to remove from channel and ensure a single channel history entry with both start and end sequences
	// and no old channel history entries
	resp = rt.SendAdminRequest("PUT", "/db/doc?rev="+body["rev"].(string), `{"channels": []}`)
	assertStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err = rt.GetDatabase().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 1)
	assert.Equal(t, syncData.ChannelSet[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 2})
	assert.Len(t, syncData.ChannelSetHistory, 0)

	// Update doc to add to channels test and test2 and ensure a single channel history entry for both test and test2
	// both with start sequences only and ensure old test entry was moved to old
	resp = rt.SendAdminRequest("PUT", "/db/doc?rev="+body["rev"].(string), `{"channels": ["test", "test2"]}`)
	assertStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err = rt.GetDatabase().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 2)
	assert.Contains(t, syncData.ChannelSet, db.ChannelSetEntry{Name: "test", Start: 3, End: 0})
	assert.Contains(t, syncData.ChannelSet, db.ChannelSetEntry{Name: "test2", Start: 3, End: 0})
	require.Len(t, syncData.ChannelSetHistory, 1)
	assert.Equal(t, syncData.ChannelSetHistory[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 2})
}

func TestChannelHistoryLegacyDoc(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	docData := `
	{
	  "channels": [
		"test"
	  ],
	  "_sync": {
		"rev": "1-08267a64bf0e3963bab7dece1ea0887a",
		"sequence": 1,
		"recent_sequences": [
		  1
		],
		"history": {
		  "revs": [
			"1-08267a64bf0e3963bab7dece1ea0887a"
		  ],
		  "parents": [
			-1
		  ],
		  "channels": [
			[
			  "test"
			]
		  ]
		},
		"channels": {
		  "test": null
		},
		"cas": "",
		"value_crc32c": "",
		"time_saved": "2021-05-04T18:37:07.559778+01:00"
	  }
	}`

	// Insert raw 'legacy' doc with no channel history info
	err := rt.GetDatabase().Bucket.Set("doc1", 0, nil, []byte(docData))
	assert.NoError(t, err)

	var body db.Body

	// Get doc and ensure its available
	resp := rt.SendAdminRequest("GET", "/db/doc1", "")
	assertStatus(t, resp, http.StatusOK)

	// Remove doc from channel and ensure that channel history is built correctly with current end sequence and
	// setting start sequence
	resp = rt.SendAdminRequest("PUT", "/db/doc1?rev=1-08267a64bf0e3963bab7dece1ea0887a", `{"channels": []}`)
	assertStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), "doc1")
	assert.NoError(t, err)
	require.Len(t, syncData.ChannelSet, 1)
	assert.Contains(t, syncData.ChannelSet, db.ChannelSetEntry{
		Name:  "test",
		Start: 1,
		End:   2,
	})
	assert.Len(t, syncData.ChannelSetHistory, 0)
}

type UserRolesTemp struct {
	Roles map[string][]string `json:"roles"`
}

type ChannelsTemp struct {
	Channels map[string][]string `json:"channels"`
}

type ChannelRevocationTester struct {
	restTester *RestTester
	test       *testing.T

	fillerDocRev   string
	roleRev        string
	roleChannelRev string
	userChannelRev string

	roles        UserRolesTemp
	roleChannels ChannelsTemp
	userChannels ChannelsTemp
}

func (tester *ChannelRevocationTester) addRole(user, role string) {
	if tester.roles.Roles == nil {
		tester.roles.Roles = map[string][]string{}
	}

	tester.roles.Roles[user] = append(tester.roles.Roles[user], fmt.Sprintf("role:%s", role))
	revID := tester.restTester.createDocReturnRev(tester.test, "userRoles", tester.roleRev, tester.roles)
	tester.roleRev = revID
}

func (tester *ChannelRevocationTester) removeRole(user, role string) {
	delIdx := -1
	roles := tester.roles.Roles[user]
	for idx, val := range roles {
		if val == fmt.Sprintf("role:%s", role) {
			delIdx = idx
			break
		}
	}
	tester.roles.Roles[user] = append(roles[:delIdx], roles[delIdx+1:]...)
	tester.roleRev = tester.restTester.createDocReturnRev(tester.test, "userRoles", tester.roleRev, tester.roles)
}

func (tester *ChannelRevocationTester) addRoleChannel(role, channel string) {
	if tester.roleChannels.Channels == nil {
		tester.roleChannels.Channels = map[string][]string{}
	}

	role = fmt.Sprintf("role:%s", role)

	tester.roleChannels.Channels[role] = append(tester.roleChannels.Channels[role], channel)
	tester.roleChannelRev = tester.restTester.createDocReturnRev(tester.test, "roleChannels", tester.roleChannelRev, tester.roleChannels)
}

func (tester *ChannelRevocationTester) removeRoleChannel(role, channel string) {
	delIdx := -1
	role = fmt.Sprintf("role:%s", role)
	channelsSlice := tester.roleChannels.Channels[role]
	for idx, val := range channelsSlice {
		if val == channel {
			delIdx = idx
			break
		}
	}
	tester.roleChannels.Channels[role] = append(channelsSlice[:delIdx], channelsSlice[delIdx+1:]...)
	tester.roleChannelRev = tester.restTester.createDocReturnRev(tester.test, "roleChannels", tester.roleChannelRev, tester.roleChannels)
}

func (tester *ChannelRevocationTester) addUserChannel(user, channel string) {
	if tester.userChannels.Channels == nil {
		tester.userChannels.Channels = map[string][]string{}
	}

	tester.userChannels.Channels[user] = append(tester.userChannels.Channels[user], channel)
	tester.userChannelRev = tester.restTester.createDocReturnRev(tester.test, "userChannels", tester.userChannelRev, tester.userChannels)
}

func (tester *ChannelRevocationTester) removeUserChannel(user, channel string) {
	delIdx := -1
	channelsSlice := tester.userChannels.Channels[user]
	for idx, val := range channelsSlice {
		if val == channel {
			delIdx = idx
			break
		}
	}
	tester.userChannels.Channels[user] = append(channelsSlice[:delIdx], channelsSlice[delIdx+1:]...)
	tester.userChannelRev = tester.restTester.createDocReturnRev(tester.test, "userChannels", tester.userChannelRev, tester.userChannels)
}

func (tester *ChannelRevocationTester) fillToSeq(seq uint64) {
	currentSeq, err := tester.restTester.GetDatabase().LastSequence()
	require.NoError(tester.test, err)

	loopCount := seq - currentSeq
	for i := 0; i < int(loopCount); i++ {
		requestURL := "/db/fillerDoc"
		if tester.fillerDocRev != "" {
			requestURL += "?rev=" + tester.fillerDocRev
		}
		resp := tester.restTester.SendAdminRequest("PUT", requestURL, "{}")
		require.Equal(tester.test, http.StatusCreated, resp.Code)

		var body db.Body
		err = json.Unmarshal(resp.BodyBytes(), &body)
		require.NoError(tester.test, err)

		tester.fillerDocRev = body["rev"].(string)
	}
}

func (tester *ChannelRevocationTester) getChanges(sinceSeq interface{}, expectedLength int) changesResults {
	var changes changesResults

	// Ensure any previous mutations have caught up before issuing changes request
	err := tester.restTester.WaitForPendingChanges()
	assert.NoError(tester.test, err)

	err = tester.restTester.WaitForCondition(func() bool {
		resp := tester.restTester.SendUserRequestWithHeaders("GET", fmt.Sprintf("/db/_changes?since=%v&revocations=true", sinceSeq), "", nil, "user", "test")
		require.Equal(tester.test, http.StatusOK, resp.Code)
		err := json.Unmarshal(resp.BodyBytes(), &changes)
		require.NoError(tester.test, err)

		return len(changes.Results) == expectedLength
	})
	assert.NoError(tester.test, err, fmt.Sprintf("Unexpected: %d. Expected %d", len(changes.Results), expectedLength))

	err = tester.restTester.WaitForPendingChanges()
	assert.NoError(tester.test, err)

	return changes
}

func initScenario(t *testing.T, rtConfig *RestTesterConfig) (ChannelRevocationTester, *RestTester) {
	defaultSyncFn := `
			function (doc, oldDoc){
				if (doc._id === 'userRoles'){				
					for (var key in doc.roles){
						role(key, doc.roles[key]);
					}
				}
				if (doc._id === 'roleChannels'){				
					for (var key in doc.channels){
						access(key, doc.channels[key]);
					}
				}
				if (doc._id === 'userChannels'){				
					for (var key in doc.channels){
						access(key, doc.channels[key]);
					}
				}
				if (doc._id.indexOf("doc") >= 0){				
					channel(doc.channels);
				}
			}`

	if rtConfig == nil {
		rtConfig = &RestTesterConfig{
			SyncFn: defaultSyncFn,
		}
	} else {
		if rtConfig.SyncFn == "" {
			rtConfig.SyncFn = defaultSyncFn
		}
	}

	rt := NewRestTester(t, rtConfig)

	revocationTester := ChannelRevocationTester{
		test:       t,
		restTester: rt,
	}

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", `{"name": "user", "password": "test"}`)
	assertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/_role/foo", `{}`)
	assertStatus(t, resp, http.StatusCreated)

	return revocationTester, rt
}

func (rt *RestTester) createDocReturnRev(t *testing.T, docID string, revID string, bodyIn interface{}) string {
	bodyJSON, err := base.JSONMarshal(bodyIn)
	assert.NoError(t, err)

	url := "/db/" + docID
	if revID != "" {
		url += "?rev=" + revID
	}

	resp := rt.SendAdminRequest("PUT", url, string(bodyJSON))
	assertStatus(t, resp, http.StatusCreated)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID = body["rev"].(string)
	if revID == "" {
		t.Fatalf("No revID in response for PUT doc")
	}

	require.NoError(t, rt.WaitForPendingChanges())
	return revID
}

func TestRevocationScenario1(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(40)

	changes = revocationTester.getChanges(25, 0)
	assert.Equal(t, "25", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(40, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario2(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(50)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario3(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(60)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario4(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(70)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "55:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario5(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario6(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(90)

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "55:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(90, 0)
	assert.Equal(t, "90", changes.Last_Seq)
}

func TestRevocationScenario7(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(100)

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(100, 0)
	assert.Equal(t, "100", changes.Last_Seq)
}

func TestRevocationScenario8(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(50)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(50, 0)
	assert.Equal(t, "50", changes.Last_Seq)
}

func TestRevocationScenario9(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(60)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(60, 0)
	assert.Equal(t, "60", changes.Last_Seq)
}

func TestRevocationScenario10(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(70)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(70, 0)
	assert.Equal(t, "70", changes.Last_Seq)
}

func TestRevocationScenario11(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(80)
	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario12(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(90)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(90, 0)
	assert.Equal(t, "90", changes.Last_Seq)
}

func TestRevocationScenario13(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(100)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(100, 0)
	assert.Equal(t, "100", changes.Last_Seq)
}

func TestRevocationScenario14(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": "ch1"}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(25)
	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestEnsureRevocationAfterDocMutation(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	// Give role access to channel A and give user access to role
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 Create doc channel A
	revocationTester.fillToSeq(4)
	revID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	// Skip to seq 10 then do pull since 4 to get doc
	revocationTester.fillToSeq(10)
	changes := revocationTester.getChanges(4, 1)
	assert.Equal(t, "5", changes.Last_Seq)

	// Skip to seq 14 then revoke role from user
	revocationTester.fillToSeq(14)
	revocationTester.removeRole("user", "foo")

	// Skip to seq 19 and then update doc foo
	revocationTester.fillToSeq(19)
	revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A"})
	err := rt.WaitForPendingChanges()
	require.NoError(t, err)

	// Get changes and ensure doc is revoked through ID-only revocation
	changes = revocationTester.getChanges(10, 1)
	assert.Equal(t, "15:20", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestEnsureRevocationUsingDocHistory(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	// Give role access to channel A and give user access to role
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 Create doc channel A
	revocationTester.fillToSeq(4)
	revID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	// Do pull to get doc
	changes := revocationTester.getChanges(4, 1)
	assert.Equal(t, "5", changes.Last_Seq)

	// Revoke channel from role at seq 8
	revocationTester.fillToSeq(7)
	revocationTester.removeRoleChannel("foo", "A")

	// Remove doc from A and re-add
	revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{})
	revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A"})

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "8:10", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithAdminChannels(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_channels": ["A"], "password": "letmein"}`)
	assertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["A"]}`)
	assertStatus(t, resp, http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/db/_changes?since=0&revocations=true", "user", false)
	require.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[1].ID)
	assert.False(t, changes.Results[0].Revoked)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_channels": [], "password": "letmein"}`)
	assertStatus(t, resp, http.StatusOK)

	changes, err = rt.WaitForChanges(2, fmt.Sprintf("/db/_changes?since=%d&revocations=true", 2), "user", false)
	require.NoError(t, err)
	require.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithAdminRoles(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels": ["A"]}`)
	assertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_roles": ["role"], "password": "letmein"}`)
	assertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["A"]}`)
	assertStatus(t, resp, http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/db/_changes?since=0&revocations=true", "user", false)
	require.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[1].ID)
	assert.False(t, changes.Results[1].Revoked)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_roles": []}`)
	assertStatus(t, resp, http.StatusOK)

	changes, err = rt.WaitForChanges(2, fmt.Sprintf("/db/_changes?since=%d&revocations=true", 3), "user", false)
	require.NoError(t, err)
	require.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationMutationMovesIntoRevokedChannel(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "A")

	revocationTester.fillToSeq(4)
	docRevID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{}})
	doc2RevID := rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"A"}})

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)
	assert.Equal(t, "doc2", changes.Results[1].ID)

	revocationTester.removeRole("user", "foo")
	docRevID = rt.createDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{"A"}})
	doc2RevID = rt.createDocReturnRev(t, "doc2", doc2RevID, map[string]interface{}{"channels": []string{"A"}, "val": "mutate"})

	changes = revocationTester.getChanges(6, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationResumeAndLowSeqCheck(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_role/foo2", `{}`)
	assertStatus(t, resp, http.StatusCreated)

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.addRole("user", "foo2")
	revocationTester.addRoleChannel("foo2", "ch2")

	revocationTester.fillToSeq(9)
	revIDDoc := rt.createDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	revIDDoc2 := rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch2"}})

	changes := revocationTester.getChanges(0, 3)

	revocationTester.fillToSeq(19)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(29)
	revocationTester.removeRoleChannel("foo2", "ch2")

	revocationTester.fillToSeq(39)
	revIDDoc = rt.createDocReturnRev(t, "doc1", revIDDoc, map[string]interface{}{"channels": []string{"ch1"}, "val": "mutate"})

	revocationTester.fillToSeq(49)
	revIDDoc2 = rt.createDocReturnRev(t, "doc2", revIDDoc2, map[string]interface{}{"channels": []string{"ch2"}, "val": "mutate"})

	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, revIDDoc, changes.Results[0].Changes[0]["rev"])
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, "doc2", changes.Results[1].ID)
	assert.Equal(t, revIDDoc2, changes.Results[1].Changes[0]["rev"])
	assert.True(t, changes.Results[1].Revoked)

	changes = revocationTester.getChanges("20:40", 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	// Check no results with 60
	changes = revocationTester.getChanges("60", 0)

	// Ensure 11 low sequence means we get revocations from that far back
	changes = revocationTester.getChanges("20:0:60", 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

}

func TestRevocationResumeSameRoleAndLowSeqCheck(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.addRoleChannel("foo", "ch2")

	revocationTester.fillToSeq(9)
	revIDDoc := rt.createDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	revIDDoc2 := rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch2"}})

	changes := revocationTester.getChanges(0, 3)

	revocationTester.fillToSeq(19)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(29)
	revocationTester.removeRoleChannel("foo", "ch2")

	revocationTester.fillToSeq(39)
	revIDDoc = rt.createDocReturnRev(t, "doc1", revIDDoc, map[string]interface{}{"channels": []string{"ch1"}, "val": "mutate"})

	revocationTester.fillToSeq(49)
	revIDDoc2 = rt.createDocReturnRev(t, "doc2", revIDDoc2, map[string]interface{}{"channels": []string{"ch2"}, "val": "mutate"})

	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, "doc2", changes.Results[1].ID)
	assert.True(t, changes.Results[1].Revoked)

	changes = revocationTester.getChanges("20:40", 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	// Check no results with 60
	changes = revocationTester.getChanges("60", 0)

	// Ensure 11 low sequence means we get revocations from that far back
	changes = revocationTester.getChanges("11:0:60", 2)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, "doc2", changes.Results[1].ID)
	assert.True(t, changes.Results[1].Revoked)
}

func TestMetricsHandler(t *testing.T) {
	base.SkipPrometheusStatsRegistration = false
	defer func() {
		base.SkipPrometheusStatsRegistration = true
	}()

	// Create and remove a database
	// This ensures that creation and removal of a DB is possible without a re-registration issue ( the below rest tester will re-register "db")
	context, err := db.NewDatabaseContext("db", base.GetTestBucket(t), false, db.DatabaseContextOptions{})
	require.NoError(t, err)
	context.Close()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.SendAdminRequest("PUT", "/db/doc", "{}")

	srv := httptest.NewServer(rt.TestMetricsHandler())
	defer srv.Close()

	httpClient := http.DefaultClient

	// Ensure metrics endpoint is accessible and that db database has entries
	resp, err := httpClient.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyString, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(bodyString), `database="db"`)
	err = resp.Body.Close()
	assert.NoError(t, err)

	// Initialize another database to ensure both are registered successfully
	context, err = db.NewDatabaseContext("db2", base.GetTestBucket(t), false, db.DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()

	// Validate that metrics still works with both db and db2 databases and that they have entries
	resp, err = httpClient.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyString, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(bodyString), `database="db"`)
	assert.Contains(t, string(bodyString), `database="db2"`)
	err = resp.Body.Close()
	assert.NoError(t, err)

	// Ensure metrics endpoint is not serving any other routes
	resp, err = httpClient.Get(srv.URL + "/db/")
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	err = resp.Body.Close()
	assert.NoError(t, err)
}

func TestRevocationsWithQueryLimit(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.IntPtr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					Size: base.Uint32Ptr(0),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.IntPtr(0),
				},
			},
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(9)
	_ = rt.createDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.createDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"ch1"}})

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	// Run changes once (which has its own wait)
	sinceVal := changes.Last_Seq
	changes = revocationTester.getChanges(sinceVal, 3)

	var queryKey string
	if base.TestsDisableGSI() {
		queryKey = fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncGateway(), db.ViewChannels)
	} else {
		queryKey = db.QueryTypeChannels
	}

	// Once we know the changes will return to right count run again to validate queries ran
	channelQueryCountBefore := rt.GetDatabase().DbStats.Query(queryKey).QueryCount.Value()
	changes = revocationTester.getChanges(sinceVal, 3)
	channelQueryCountAfter := rt.GetDatabase().DbStats.Query(queryKey).QueryCount.Value()

	assert.Equal(t, int64(3), channelQueryCountAfter-channelQueryCountBefore)
}

func TestRevocationsWithQueryLimitChangesLimit(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.IntPtr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					Size: base.Uint32Ptr(0),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.IntPtr(0),
				},
			},
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(9)
	_ = rt.createDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.createDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"ch1"}})

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	waitForUserChangesWithLimit := func(sinceVal interface{}, limit int) changesResults {
		var changesRes changesResults
		err := rt.WaitForCondition(func() bool {
			resp := rt.SendUserRequestWithHeaders("GET", fmt.Sprintf("/db/_changes?since=%v&revocations=true&limit=%d", sinceVal, limit), "", nil, "user", "test")
			require.Equal(t, http.StatusOK, resp.Code)
			err := json.Unmarshal(resp.BodyBytes(), &changesRes)
			require.NoError(t, err)

			return len(changesRes.Results) == limit
		})
		assert.NoError(t, err)
		return changesRes
	}

	sinceVal := changes.Last_Seq
	changes = waitForUserChangesWithLimit(sinceVal, 2)
	assert.Len(t, changes.Results, 2)

	changes = waitForUserChangesWithLimit(sinceVal, 3)
	assert.Len(t, changes.Results, 3)
}

func TestUserHasDocAccessDocNotFound(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.IntPtr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					Size: base.Uint32Ptr(0),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.IntPtr(0),
				},
			},
		}},
	})
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["A"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	database, err := db.CreateDatabase(rt.GetDatabase())
	assert.NoError(t, err)

	userHasDocAccess, err := db.UserHasDocAccess(database, "doc", revID)
	assert.NoError(t, err)
	assert.True(t, userHasDocAccess)

	err = rt.GetDatabase().Bucket.Delete("doc")
	assert.NoError(t, err)

	userHasDocAccess, err = db.UserHasDocAccess(database, "doc", revID)
	assert.NoError(t, err)
	assert.False(t, userHasDocAccess)
}

func TestRevocationUserHasDocAccessDocNotFound(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency when running in integration")
	}

	revocationTester, rt := initScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.IntPtr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					Size: base.Uint32Ptr(0),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.IntPtr(0),
				},
			},
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "A")

	resp := rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["A"]}`)
	assertStatus(t, resp, http.StatusCreated)

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeRoleChannel("foo", "A")
	require.NoError(t, rt.WaitForPendingChanges())

	leakyBucket, ok := base.AsLeakyBucket(rt.Bucket())
	require.True(t, ok)

	leakyBucket.SetGetRawCallback(func(s string) {
		assert.NoError(t, leakyBucket.Delete("doc"))
	})

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	require.Len(t, changes.Results, 1)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, changes.Results[0].ID, "doc")
}

// Test does not directly run wasDocInChannelAtSeq but aims to test this through performing revocation operations
// that will hit the various cases that wasDocInChannelAtSeq will handle
func TestWasDocInChannelAtSeq(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "a")
	revocationTester.addRoleChannel("foo", "c")

	revID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeRoleChannel("foo", "a")
	revocationTester.removeRoleChannel("foo", "c")

	_ = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": []string{}})
	_ = rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"b", "a"}})

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addRoleChannel("foo", "c")
	doc3Rev := rt.createDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"c"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.removeRoleChannel("foo", "c")
	_ = rt.createDocReturnRev(t, "doc3", doc3Rev, map[string]interface{}{"channels": []string{"c"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
}

// Test does not directly run ChannelGrantedPeriods but aims to test this through performing revocation operations
// that will hit the various cases that ChannelGrantedPeriods will handle
func TestChannelGrantedPeriods(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	revocationTester.addUserChannel("user", "a")
	revId := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})
	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeUserChannel("user", "a")
	revId = rt.createDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate", "channels": []string{"a"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addUserChannel("user", "a")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.removeUserChannel("user", "a")
	revId = rt.createDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate2", "channels": []string{"a"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addUserChannel("user", "a")
	revId = rt.createDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate3", "channels": []string{"a"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "b")
	revId = rt.createDocReturnRev(t, "doc", revId, map[string]interface{}{"channels": []string{"b"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)

	revocationTester.removeRoleChannel("foo", "b")
	revocationTester.removeRole("user", "foo")
	revId = rt.createDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate", "channels": []string{"b"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
}

func TestChannelHistoryPruning(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "a")

	resp := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels": ["a"]}`)
	assertStatus(t, resp, http.StatusCreated)

	// Enter a load of history by looping over adding and removing a channel. Needs a get changes in there to trigger
	// the actual rebuild
	var changes changesResults
	for i := 0; i < 20; i++ {
		changes = revocationTester.getChanges(0, 2)
		assert.Len(t, changes.Results, 2)
		revocationTester.removeRoleChannel("foo", "a")
		changes = revocationTester.getChanges(0, 2)
		assert.Len(t, changes.Results, 2)
		revocationTester.addRoleChannel("foo", "a")
	}

	// Validate history is pruned properly with first entry merged to span a wider period
	authenticator := rt.GetDatabase().Authenticator(base.TestCtx(t))
	role, err := authenticator.GetRole("foo")
	assert.NoError(t, err)
	require.Contains(t, role.ChannelHistory(), "a")
	require.Len(t, role.ChannelHistory()["a"].Entries, 10)
	assert.Equal(t, role.ChannelHistory()["a"].Entries[0], auth.GrantHistorySequencePair{StartSeq: 4, EndSeq: 26})

	// Add an additional channel to ensure only the latter one is pruned
	revocationTester.addRoleChannel("foo", "b")
	resp = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels": ["b"]}`)
	assertStatus(t, resp, http.StatusCreated)
	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Len(t, changes.Results, 2)
	revocationTester.removeRoleChannel("foo", "b")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	// Override role history with a unix time older than 30 days. Means next time rebuildRoles is ran we will end up
	// pruning those entries.
	role, err = authenticator.GetRole("foo")
	assert.NoError(t, err)
	channelHistory := role.ChannelHistory()
	aHistory := channelHistory["a"]
	aHistory.UpdatedAt = time.Now().Add(-31 * time.Hour * 24).Unix()
	channelHistory["a"] = aHistory

	role.SetChannelHistory(channelHistory)
	err = authenticator.Save(role)
	assert.NoError(t, err)

	// Add another so we have something to wait on
	revocationTester.addRoleChannel("foo", "random")
	resp = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels": ["random"]}`)

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	role, err = authenticator.GetRole("foo")
	assert.NoError(t, err)

	assert.NotContains(t, role.ChannelHistory(), "a")
	assert.Contains(t, role.ChannelHistory(), "b")
}

func TestChannelRevocationWithContiguousSequences(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	revocationTester.addUserChannel("user", "a")
	revID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "a"})
	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeUserChannel("user", "a")
	revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"mutate": "mutate", "channels": "a"})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.addUserChannel("user", "a")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.False(t, changes.Results[0].Revoked)

	revocationTester.removeUserChannel("user", "a")
	revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"mutate": "mutate2", "channels": "a"})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithUserXattrs(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	xattrKey := "channelInfo"

	revocationTester, rt := initScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   true,
			UserXattrKey: xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (doc._id === 'accessDoc' && meta.xattrs.channelInfo !== undefined){
					for (var key in meta.xattrs.channelInfo.userChannels){
						access(key, meta.xattrs.channelInfo.userChannels[key]);
					}
				}
				if (doc._id.indexOf("doc") >= 0){				
					channel(doc.channels);
				}
			}`,
	})

	defer rt.Close()

	gocbBucket, ok := base.AsGoCBBucket(rt.Bucket())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	resp := rt.SendAdminRequest("PUT", "/db/accessDoc", `{}`)
	assertStatus(t, resp, http.StatusCreated)

	_, err := gocbBucket.WriteUserXattr("accessDoc", xattrKey, map[string]interface{}{"userChannels": map[string]interface{}{"user": "a"}})
	assert.NoError(t, err)

	_ = rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	_, err = gocbBucket.WriteUserXattr("accessDoc", xattrKey, map[string]interface{}{})
	assert.NoError(t, err)

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
}

func TestDocChannelSetPruning(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	rt := NewRestTester(t, nil)
	defer rt.Close()

	revID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})

	for i := 0; i < 10; i++ {
		revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": []string{}})
		revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": []string{"a"}})
	}

	syncData, err := rt.GetDatabase().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSetHistory, db.DocumentHistoryMaxEntriesPerChannel)
	assert.Equal(t, "a", syncData.ChannelSetHistory[0].Name)
	assert.Equal(t, uint64(1), syncData.ChannelSetHistory[0].Start)
	assert.Equal(t, uint64(12), syncData.ChannelSetHistory[0].End)
}

func TestBasicAttachmentRemoval(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	attContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attContentType,
	}

	storeAttachment := func(doc, rev, attName, attBody string) string {
		resource := fmt.Sprintf("/db/%s/%s?rev=%s", doc, attName, rev)
		response := rt.SendRequestWithHeaders(http.MethodPut, resource, attBody, reqHeaders)
		assertStatus(t, response, http.StatusCreated)
		var body db.Body
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		require.True(t, body["ok"].(bool))
		return body["rev"].(string)
	}

	retrieveAttachment := func(docID, attName string) (attBody string) {
		resource := fmt.Sprintf("/db/%s/%s", docID, attName)
		response := rt.SendRequest(http.MethodGet, resource, "")
		assertStatus(t, response, http.StatusOK)
		return string(response.Body.Bytes())
	}

	retrieveAttachmentKey := func(docID, attName string) (key string) {
		resource := fmt.Sprintf("/db/%s/%s?meta=true", docID, attName)
		response := rt.SendRequest(http.MethodGet, resource, "")
		var meta map[string]interface{}
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &meta))
		assertStatus(t, response, http.StatusOK)
		key, found := meta["key"].(string)
		require.True(t, found)
		return key
	}

	requireAttachmentNotFound := func(docID, attName string) {
		resource := fmt.Sprintf("/db/%s/%s", docID, attName)
		response := rt.SendRequest(http.MethodGet, resource, "")
		assertStatus(t, response, http.StatusNotFound)
	}

	retrieveAttachmentMeta := func(docID string) (attMeta map[string]interface{}) {
		body := rt.getDoc(docID)
		attachments, ok := body["_attachments"].(map[string]interface{})
		require.True(t, ok)
		return attachments
	}

	requireAttachmentFound := func(attKey string, attBodyExpected []byte) {
		var attBodyActual []byte
		_, err := rt.Bucket().Get(attKey, &attBodyActual)
		require.NoError(t, err)
		assert.Equal(t, attBodyExpected, attBodyActual)
	}

	rawDocWithAttachmentAndSyncMeta := func() []byte {
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

	createDocWithLegacyAttachment := func(docID string, rawDoc []byte, attKey string, attBody []byte) {
		// Write attachment directly to the bucket.
		_, err := rt.Bucket().Add(attKey, 0, attBody)
		require.NoError(t, err)

		body := db.Body{}
		err = body.Unmarshal(rawDoc)
		require.NoError(t, err, "Error unmarshalling body")

		// Write raw document to the bucket.
		_, err = rt.Bucket().Add(docID, 0, rawDoc)
		require.NoError(t, err)

		// Migrate document metadata from document body to system xattr.
		attachments := retrieveAttachmentMeta(docID)
		require.Len(t, attachments, 1)
	}

	t.Run("single attachment removal upon document update", func(t *testing.T) {
		// Create a document.
		docID := "foo"
		revID := rt.createDoc(t, docID)
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
		response := rt.updateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("single attachment removal upon document delete", func(t *testing.T) {
		// Create a document.
		docID := "bar"
		revID := rt.createDoc(t, docID)
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
		rt.deleteDoc(docID, revID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("single attachment removal upon document purge", func(t *testing.T) {
		// Create a document.
		docID := "baz"
		revID := rt.createDoc(t, docID)
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
		rt.purgeDoc(docID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)
	})

	t.Run("single attachment removal upon attachment update", func(t *testing.T) {
		// Create a document.
		docID := "qux"
		revID := rt.createDoc(t, docID)
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
		rt.requireDocNotFound(attKeyOld)

		// Retrieve new attachment key used for internal attachment storage and retrieval.
		attKeyNew := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:IfWNJ/gn0pX/zYYMZQRWheO68a1FBsqgFAETsxZkdTQ=:sha1-0naD6SgfLVDr+zakP8RkNlBYORw=", attKeyOld)
		require.NotEqual(t, attKeyOld, attKeyNew)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("multiple attachments removal upon document update", func(t *testing.T) {
		// Create a document.
		docID := "foo1"
		revID := rt.createDoc(t, docID)
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
		response := rt.updateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether both attachments are removed from the underlying storage.
		requireAttachmentNotFound(docID, att1Name)
		rt.requireDocNotFound(att1Key)
		requireAttachmentNotFound(docID, att2Name)
		rt.requireDocNotFound(att2Key)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("multiple attachments removal upon document delete", func(t *testing.T) {
		// Create a document.
		docID := "foo2"
		revID := rt.createDoc(t, docID)
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
		rt.deleteDoc(docID, revID)

		// Check whether both attachments are removed from the underlying storage.
		requireAttachmentNotFound(docID, att1Name)
		rt.requireDocNotFound(att1Key)
		requireAttachmentNotFound(docID, att2Name)
		rt.requireDocNotFound(att2Key)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("multiple attachments removal upon document purge", func(t *testing.T) {
		// Create a document.
		docID := "foo3"
		revID := rt.createDoc(t, docID)
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
		rt.purgeDoc(docID)

		// Check whether both attachments are removed from the underlying storage.
		requireAttachmentNotFound(docID, att1Name)
		rt.requireDocNotFound(att1Key)
		requireAttachmentNotFound(docID, att2Name)
		rt.requireDocNotFound(att2Key)
	})

	t.Run("single inline attachment removal upon document update", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo8"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.putDoc(docID, body)
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
		response := rt.updateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("single inline attachment removal upon document delete", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo9"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.putDoc(docID, body)
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
		rt.deleteDoc(docID, revID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
	})

	t.Run("single inline attachment removal upon document purge", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo10"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.putDoc(docID, body)
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
		rt.purgeDoc(docID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)
	})

	t.Run("single inline attachment removal upon attachment update", func(t *testing.T) {
		// Create a document with inline attachment.
		docID := "foo11"
		attName := "foo.txt"
		attBody := "this is the body of attachment foo.txt"
		attBodyEncoded := base64.StdEncoding.EncodeToString([]byte(attBody))
		body := fmt.Sprintf(`{"prop": true, "_attachments": {"%s": {"data":"%s"}}}`, attName, attBodyEncoded)
		putResponse := rt.putDoc(docID, body)
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
		rt.requireDocNotFound(attKeyOld)

		// Retrieve new attachment key used for internal attachment storage and retrieval.
		attKeyNew := retrieveAttachmentKey(docID, attName)
		require.Equal(t, "_sync:att2:8moUa62DqG+wrhztGWL8Sj9qpCQz7tat6Z5LRt6/DWE=:sha1-CTJaowVFZ4ozgmvBageTH9w+OKU=", attKeyOld)
		require.NotEqual(t, attKeyOld, attKeyNew)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
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
		putResponse := rt.putDoc(docID, body)
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
		err := rt.Bucket().Delete(docID)
		require.NoError(t, err, "Unable to delete doc %q", docID)

		// Wait until the "delete" mutation appears on the changes feed.
		changes, err := rt.WaitForChanges(1, "/db/_changes", "", true)
		assert.NoError(t, err, "Error waiting for changes")
		log.Printf("changes: %+v", changes)
		rt.requireDocNotFound(docID)

		// Check whether the attachment is removed from the underlying storage.
		requireAttachmentNotFound(docID, attName)
		rt.requireDocNotFound(attKey)
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
		putResponse := rt.putDoc(docID, body)
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
		err := rt.Bucket().Set(docID, 0, nil, []byte(`{"prop": false}`))
		require.NoError(t, err, "Error updating the document")

		// Wait until the "update" mutation appears on the changes feed.
		changes, err := rt.WaitForChanges(1, "/db/_changes", "", true)
		assert.NoError(t, err, "Error waiting for changes")
		log.Printf("changes: %+v", changes)

		// Verify that the attachment is not removed.
		actualAttBody = retrieveAttachment(docID, attName)
		require.Equal(t, attBody, actualAttBody)

		// Get the document and check doc body and attachment metadata.
		updatedBody := rt.getDoc(docID)
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
		revID := rt.createDoc(t, docID)
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
		response := rt.updateDoc(docID, revID, `{"prop":true, "_attachments": {"alice.txt": {"stub": true, "revpos": 2}}}`)
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
		rt.requireDocNotFound(att2Key)

		// Verify that att1Name is still found in the bucket.
		actualAttBody = retrieveAttachment(docID, att1Name)
		require.Equal(t, att1Body, actualAttBody)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
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
		createDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

		// Get the document and grab the revID.
		responseBody := rt.getDoc(docID)
		revID := responseBody["_rev"].(string)
		require.NotEmpty(t, revID)

		// Delete/tombstone the document.
		rt.deleteDoc(docID, revID)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
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
		createDocWithLegacyAttachment(docID1, rawDoc, attKey, attBody)

		// Create another document referencing the same legacy attachment.
		createDocWithLegacyAttachment(docID2, rawDoc, attKey, attBody)

		// Get revID of the first document.
		responseBody := rt.getDoc(docID1)
		revID1 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID1)

		// Delete/tombstone the first document.
		rt.deleteDoc(docID1, revID1)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Get revID of the second document.
		responseBody = rt.getDoc(docID2)
		revID2 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID2)

		// Delete/tombstone the second document.
		rt.deleteDoc(docID2, revID2)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID1)
		rt.purgeDoc(docID2)
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
		createDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

		// Get the document and grab the revID.
		responseBody := rt.getDoc(docID)
		revID := responseBody["_rev"].(string)
		require.NotEmpty(t, revID)

		// Remove attachment from the document via document update.
		response := rt.updateDoc(docID, revID, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID)
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
		createDocWithLegacyAttachment(docID1, rawDoc, attKey, attBody)

		// Create another document referencing the same legacy attachment.
		createDocWithLegacyAttachment(docID2, rawDoc, attKey, attBody)

		// Get revID of the first document.
		responseBody := rt.getDoc(docID1)
		revID1 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID1)

		// Remove attachment from the first document via document update.
		response := rt.updateDoc(docID1, revID1, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Get revID of the second document.
		responseBody = rt.getDoc(docID2)
		revID2 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID2)

		// Remove attachment from the second document via document update.
		response = rt.updateDoc(docID2, revID2, `{"prop":true}`)
		require.NotEmpty(t, response.Rev)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Perform cleanup after the test ends.
		rt.purgeDoc(docID1)
		rt.purgeDoc(docID2)
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
		createDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

		// Get the document and grab the revID.
		responseBody := rt.getDoc(docID)
		revID := responseBody["_rev"].(string)
		require.NotEmpty(t, revID)

		// Purge the entire document.
		rt.purgeDoc(docID)

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
		createDocWithLegacyAttachment(docID1, rawDoc, attKey, attBody)

		// Create another document referencing the same legacy attachment.
		createDocWithLegacyAttachment(docID2, rawDoc, attKey, attBody)

		// Get revID of the first document.
		responseBody := rt.getDoc(docID1)
		revID1 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID1)

		// Purge the first document.
		rt.purgeDoc(docID1)

		// Check whether legacy attachment is still persisted in the bucket.
		requireAttachmentFound(attKey, attBody)

		// Get revID of the second document.
		responseBody = rt.getDoc(docID2)
		revID2 := responseBody["_rev"].(string)
		require.NotEmpty(t, revID2)

		// Purge the second document.
		rt.purgeDoc(docID2)

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
	revid := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"test": "x"})

	// Create doc rev 2 with attachment
	revid = rt.createDocReturnRev(t, "doc", revid, map[string]interface{}{"_attachments": map[string]interface{}{"hello.txt": map[string]interface{}{"data": "aGVsbG8gd29ybGQ="}}})
	err := rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Create doc rev 3 referencing previous attachment
	resp := rt.SendAdminRequest("PUT", "/db/doc?rev="+revid, `{"_attachments": {"hello.txt": {"revpos":2,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`)
	assertStatus(t, resp, http.StatusCreated)
	losingRev3 := respRevID(t, resp)

	// Create doc conflicting with previous revid referencing previous attachment too
	_, revIDHash := db.ParseRevID(revid)
	resp = rt.SendAdminRequest("PUT", "/db/doc?new_edits=false", `{"_rev": "3-b", "_revisions": {"ids": ["b", "`+revIDHash+`"], "start": 3}, "_attachments": {"hello.txt": {"revpos":2,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}, "Winning Rev": true}`)
	assertStatus(t, resp, http.StatusCreated)
	winningRev3 := respRevID(t, resp)

	// Update the winning rev 3 and ensure attachment remains around as the other leaf still references this attachment
	resp = rt.SendAdminRequest("PUT", "/db/doc?rev="+winningRev3, `{"update": 2}`)
	assertStatus(t, resp, http.StatusCreated)
	finalRev4 := respRevID(t, resp)

	type docResp struct {
		Attachments db.AttachmentsMeta `json:"_attachments"`
	}

	var doc1 docResp
	// Get losing rev and ensure attachment is still there and has not been deleted
	resp = rt.SendAdminRequestWithHeaders("GET", "/db/doc?attachments=true&rev="+losingRev3, "", map[string]string{"Accept": "application/json"})
	assertStatus(t, resp, http.StatusOK)

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
	assertStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &doc2)
	assert.NoError(t, err)
	require.NotContains(t, doc2.Attachments, "hello.txt")

	// Now remove the attachment in the losing rev by deleting the revision and ensure the attachment gets deleted
	resp = rt.SendAdminRequest("DELETE", "/db/doc?rev="+losingRev3, "")
	assertStatus(t, resp, http.StatusOK)

	_, _, err = rt.GetDatabase().Bucket.GetRaw(attachmentKey)
	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
}

func TestTombstoneCompactionAPI(t *testing.T) {
	rt := NewRestTester(t, nil)
	rt.GetDatabase().PurgeInterval = 0
	defer rt.Close()

	for i := 0; i < 100; i++ {
		resp := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc%d", i), "{}")
		assertStatus(t, resp, http.StatusCreated)
		rev := respRevID(t, resp)
		resp = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/doc%d?rev=%s", i, rev), "{}")
		assertStatus(t, resp, http.StatusOK)
	}

	resp := rt.SendAdminRequest("GET", "/db/_compact", "")
	assertStatus(t, resp, http.StatusOK)

	var tombstoneCompactionStatus db.TombstoneManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
	assert.NoError(t, err)

	assert.Equal(t, db.BackgroundProcessStateCompleted, tombstoneCompactionStatus.State)
	assert.Empty(t, tombstoneCompactionStatus.LastErrorMessage)
	assert.Equal(t, 0, int(tombstoneCompactionStatus.DocsPurged))
	firstStartTimeStat := rt.GetDatabase().DbStats.Database().CompactionAttachmentStartTime.Value()
	assert.NotEqual(t, 0, firstStartTimeStat)

	resp = rt.SendAdminRequest("POST", "/db/_compact", "")
	assertStatus(t, resp, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/db/_compact", "")
		assertStatus(t, resp, http.StatusOK)

		err = base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
		assert.NoError(t, err)

		return tombstoneCompactionStatus.State == db.BackgroundProcessStateCompleted
	})
	assert.NoError(t, err)
	assert.True(t, rt.GetDatabase().DbStats.Database().CompactionAttachmentStartTime.Value() > firstStartTimeStat)

	resp = rt.SendAdminRequest("GET", "/db/_compact", "")
	assertStatus(t, resp, http.StatusOK)
	err = base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
	assert.NoError(t, err)

	assert.Equal(t, db.BackgroundProcessStateCompleted, tombstoneCompactionStatus.State)
	assert.Empty(t, tombstoneCompactionStatus.LastErrorMessage)

	if base.TestUseXattrs() {
		assert.Equal(t, 100, int(tombstoneCompactionStatus.DocsPurged))
	} else {
		assert.Equal(t, 0, int(tombstoneCompactionStatus.DocsPurged))
	}
}

func TestAttachmentsMissing(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	_ = rt.Bucket()

	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello.txt": {"data": "aGVsbG8gd29ybGQ="}}}`)
	assertStatus(t, resp, http.StatusCreated)
	rev1ID := respRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?rev="+rev1ID, `{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}, "testval": ["xxx","xxx"]}`)
	assertStatus(t, resp, http.StatusCreated)
	rev2ID := respRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?new_edits=false", `{"_rev": "2-b", "_revisions": {"ids": ["b", "ca9ad22802b66f662ff171f226211d5c"], "start": 2}, "Winning Rev": true}`)
	assertStatus(t, resp, http.StatusCreated)

	rt.GetDatabase().FlushRevisionCacheForTest()

	resp = rt.SendAdminRequest("GET", "/db/"+t.Name()+"?rev="+rev2ID, ``)
	assertStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")
}

func TestAttachmentsMissingNoBody(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	_ = rt.Bucket()

	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello.txt": {"data": "aGVsbG8gd29ybGQ="}}}`)
	assertStatus(t, resp, http.StatusCreated)
	rev1ID := respRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?rev="+rev1ID, `{"_attachments": {"hello.txt": {"revpos":1,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`)
	assertStatus(t, resp, http.StatusCreated)
	rev2ID := respRevID(t, resp)

	resp = rt.SendAdminRequest("PUT", "/db/"+t.Name()+"?new_edits=false", `{"_rev": "2-b", "_revisions": {"ids": ["b", "ca9ad22802b66f662ff171f226211d5c"], "start": 2}}`)
	assertStatus(t, resp, http.StatusCreated)

	rt.GetDatabase().FlushRevisionCacheForTest()

	resp = rt.SendAdminRequest("GET", "/db/"+t.Name()+"?rev="+rev2ID, ``)
	assertStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")
}

func TestAttachmentDeleteOnPurge(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create doc with attachment
	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello": {"data": "aGVsbG8gd29ybGQ="}}}`)
	assertStatus(t, resp, http.StatusCreated)
	err := rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Ensure attachment is uploaded and key the attachment doc key
	resp = rt.SendAdminRequest("GET", "/db/"+t.Name()+"/hello?meta=true", "")
	assertStatus(t, resp, http.StatusOK)

	var body db.Body
	err = base.JSONUnmarshal(resp.BodyBytes(), &body)
	require.NoError(t, err)

	key, ok := body["key"].(string)
	assert.True(t, ok)

	// Ensure we can get the attachment doc
	_, _, err = rt.GetDatabase().Bucket.GetRaw(key)
	assert.NoError(t, err)

	// Purge the document
	resp = rt.SendAdminRequest("POST", "/db/_purge", `{"`+t.Name()+`": ["*"]}`)
	assertStatus(t, resp, http.StatusOK)

	// Ensure that the attachment has now been deleted
	_, _, err = rt.GetDatabase().Bucket.GetRaw(key)
	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
}

func TestAttachmentDeleteOnExpiry(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Expiry only supported by Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create doc with attachment and expiry
	resp := rt.SendAdminRequest("PUT", "/db/"+t.Name(), `{"_attachments": {"hello.txt": {"data": "aGVsbG8gd29ybGQ="}}, "_exp": 2}`)
	assertStatus(t, resp, http.StatusCreated)
	err := rt.WaitForPendingChanges()
	assert.NoError(t, err)

	// Wait for document to be expired - this bucket get should also trigger the expiry purge interval
	err = rt.WaitForCondition(func() bool {
		_, _, err = rt.GetDatabase().Bucket.GetRaw(t.Name())
		return base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)

	// Trigger OnDemand Import for that doc to trigger tombstone
	resp = rt.SendAdminRequest("GET", "/db/"+t.Name(), "")
	assertStatus(t, resp, http.StatusNotFound)

	att2Key := db.MakeAttachmentKey(db.AttVersion2, t.Name(), "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")

	// With xattrs doc will be imported and will be captured as tombstone and therefore purge attachments
	// Otherwise attachment will not be purged
	_, _, err = rt.GetDatabase().Bucket.GetRaw(att2Key)
	if base.TestUseXattrs() {
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
	} else {
		assert.NoError(t, err)
	}

}

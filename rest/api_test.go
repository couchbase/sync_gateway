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
	RequireStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "Welcome", body["couchdb"])

	response = rt.SendRequest("HEAD", "/", "")
	RequireStatus(t, response, 200)
	response = rt.SendRequest("OPTIONS", "/", "")
	RequireStatus(t, response, 204)
	assert.Equal(t, "GET, HEAD", response.Header().Get("Allow"))
	response = rt.SendRequest("PUT", "/", "")
	RequireStatus(t, response, 405)
	assert.Equal(t, "GET, HEAD", response.Header().Get("Allow"))
}

func TestDBRoot(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	response := rt.SendRequest("GET", "/db/", "")
	RequireStatus(t, response, 200)
	var body db.Body
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)

	assert.Equal(t, "db", body["db_name"])
	assert.Equal(t, "Online", body["state"])

	response = rt.SendRequest("HEAD", "/db/", "")
	RequireStatus(t, response, 200)
	response = rt.SendRequest("OPTIONS", "/db/", "")
	RequireStatus(t, response, 204)
	assert.Equal(t, "GET, HEAD, POST, PUT", response.Header().Get("Allow"))
}

func TestDisablePublicBasicAuth(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DisablePasswordAuth: base.BoolPtr(true),
				Guest: &auth.PrincipalConfig{
					Disabled: base.BoolPtr(true),
				},
			},
		},
	})
	defer rt.Close()
	ctx := rt.Context()

	response := rt.SendRequest(http.MethodGet, "/db/", "")
	RequireStatus(t, response, http.StatusUnauthorized)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	// Double-check that even if we provide valid credentials we still won't be let in
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.NewUser("user1", "letmein", channels.BaseSetOf(t, "foo"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	response = rt.Send(RequestByUser(http.MethodGet, "/db/", "", "user1"))
	RequireStatus(t, response, http.StatusUnauthorized)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	// Also check that we can't create a session through POST /db/_session
	response = rt.SendRequest(http.MethodPost, "/db/_session", `{"name":"user1","password":"letmein"}`)
	RequireStatus(t, response, http.StatusUnauthorized)

	// As a sanity check, ensure it does work when the setting is disabled
	rt.ServerContext().Database(ctx, "db").Options.DisablePasswordAuthentication = false
	response = rt.Send(RequestByUser(http.MethodGet, "/db/", "", "user1"))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequest(http.MethodPost, "/db/_session", `{"name":"user1","password":"letmein"}`)
	RequireStatus(t, response, http.StatusOK)
}

func TestDocLifecycle(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	revid := rt.CreateDoc(t, "doc")
	assert.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", revid)

	response := rt.SendAdminRequest("DELETE", "/db/doc?rev="+revid, "")
	RequireStatus(t, response, 200)
}

func TestDocumentUpdateWithNullBody(t *testing.T) {
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
	// Create document
	response := rt.Send(RequestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user1"))
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	// Put new revision with null body
	response = rt.Send(RequestByUser("PUT", "/db/doc?rev="+revid, "", "user1"))
	RequireStatus(t, response, 400)
}

func TestFunkyDocIDs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateDoc(t, "AC%2FDC")

	response := rt.SendAdminRequest("GET", "/db/AC%2FDC", "")
	RequireStatus(t, response, 200)

	rt.CreateDoc(t, "AC+DC")
	response = rt.SendAdminRequest("GET", "/db/AC+DC", "")
	RequireStatus(t, response, 200)

	rt.CreateDoc(t, "AC+DC+GC")
	response = rt.SendAdminRequest("GET", "/db/AC+DC+GC", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/db/foo+bar+moo+car", `{"prop":true}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/foo+bar+moo+car", "")
	RequireStatus(t, response, 200)

	rt.CreateDoc(t, "AC%2BDC2")
	response = rt.SendAdminRequest("GET", "/db/AC%2BDC2", "")
	RequireStatus(t, response, 200)

	rt.CreateDoc(t, "AC%2BDC%2BGC2")
	response = rt.SendAdminRequest("GET", "/db/AC%2BDC%2BGC2", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/db/foo%2Bbar%2Bmoo%2Bcar2", `{"prop":true}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/foo%2Bbar%2Bmoo%2Bcar2", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/db/foo%2Bbar+moo%2Bcar3", `{"prop":true}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/foo+bar%2Bmoo+car3", "")
	RequireStatus(t, response, 200)
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

			ctx := rt.Context()
			a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

			// Create a test user
			user, err := a.NewUser(tc.UserName, "letmein", channels.BaseSetOf(t, "foo"))
			require.NoError(t, err)
			require.NoError(t, a.Save(user))

			response := rt.Send(RequestByUser("PUT", "/db/AC+DC", `{"foo":"bar", "channels": ["foo"]}`, tc.UserName))
			RequireStatus(t, response, 201)

			response = rt.Send(RequestByUser("GET", "/db/_all_docs", "", tc.UserName))
			RequireStatus(t, response, 200)

			response = rt.Send(RequestByUser("GET", "/db/AC+DC", "", tc.UserName))
			RequireStatus(t, response, 200)
			var overlay struct {
				Rev string `json:"_rev"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &overlay))

			response = rt.Send(RequestByUser("DELETE", "/db/AC+DC?rev="+overlay.Rev, "", tc.UserName))
			RequireStatus(t, response, 200)
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

			ctx := rt.Context()
			a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

			// Create a test user
			user, err := a.NewUser(username, "letmein", channels.BaseSetOf(t))
			require.NoError(t, err)
			require.NoError(t, a.Save(user))

			// Create role
			response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_role/%s", url.PathEscape(tc.RoleName)), `{"admin_channels": ["testchannel"]}`)
			RequireStatus(t, response, 201)

			// Create test document
			response = rt.SendAdminRequest("PUT", "/db/testdoc", `{"channels":["testchannel"]}`)
			RequireStatus(t, response, 201)

			// Assert user can access it
			response = rt.Send(RequestByUser("GET", "/db/testdoc", "", username))
			RequireStatus(t, response, 200)
		})
	}
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
	sc.Config.API.CORS.Origin = []string{"http://example.com", "http://staging.example.com"}
	// now test a non-listed origin
	// b/c * is in config we get *
	reqHeaders = map[string]string{
		"Origin": "http://hack0r.com",
	}
	response = rt.SendRequestWithHeaders("GET", "/db/", "", reqHeaders)
	assert.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
}

// assertGatewayStatus is like requireStatus but with StatusGatewayTimeout error checking for temporary network failures.
func assertGatewayStatus(t *testing.T, response *TestResponse, expected int) {
	if response.Code == http.StatusGatewayTimeout {
		respBody := response.Body.String()
		t.Skipf("WARNING: Host could not be reached: %s", respBody)
	}
	RequireStatus(t, response, expected)
}

func TestBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}, {"_id": "_local/bulk3", "n": 3}]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	RequireStatus(t, response, 201)

	var docs []interface{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Len(t, docs, 3)
	assert.Equal(t, map[string]interface{}{"rev": "1-50133ddd8e49efad34ad9ecae4cb9907", "id": "bulk1"}, docs[0])

	response = rt.SendAdminRequest("GET", "/db/bulk1", "")
	RequireStatus(t, response, 200)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "bulk1", respBody[db.BodyId])
	assert.Equal(t, "1-50133ddd8e49efad34ad9ecae4cb9907", respBody[db.BodyRev])
	assert.Equal(t, float64(1), respBody["n"])
	assert.Equal(t, map[string]interface{}{"rev": "1-035168c88bd4b80fb098a8da72f881ce", "id": "bulk2"}, docs[1])
	assert.Equal(t, map[string]interface{}{"rev": "0-1", "id": "_local/bulk3"}, docs[2])

	response = rt.SendAdminRequest("GET", "/db/_local/bulk3", "")
	RequireStatus(t, response, 200)
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
	RequireStatus(t, response, 200)
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
	RequireStatus(t, response, 201)
	var docs []map[string]string
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	log.Printf("response: %s", response.Body.Bytes())
	RequireStatus(t, response, 201)
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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)

	//Sequence 8 should get used here as sequence 3 should have been dropped by the first sequence allocator
	doc1Rev3, err := rt1.GetDatabase().GetDocSyncData("bulk1")
	assert.NoError(t, err, "GetDocSyncData error")
	goassert.Equals(t, doc1Rev3.Sequence, uint64(8))

	//Get the revID for doc "bulk1"
	doc1RevID3 := doc1Rev3.CurrentRev

	//Now send a bulk_doc to rt2 to update doc bulk1 again
	input = `{"docs": [{"_id": "bulk1", "_rev": "` + doc1RevID3 + `", "n": 2}]}`
	response = rt1.SendRequest("POST", "/db/_bulk_docs", input)
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 400)
}

func TestBulkDocsMalformedDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs":["A","B"]}`
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	RequireStatus(t, response, 400)

	// For non-string id, ensure it reverts to id generation and doesn't panic
	input = `{"docs": [{"_id": 3, "n": 1}]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", input)
	log.Printf("response:%s", response.Body.Bytes())
	RequireStatus(t, response, 201)
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
		RequireStatus(t, resp, http.StatusCreated)
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
	RequireStatus(t, resp, http.StatusOK)

	uncompressedBodyLen := resp.Body.Len()
	assert.Truef(t, uncompressedBodyLen >= minUncompressedSize, "Expected uncompressed response to be larger than minUncompressedSize (%d bytes) - got %d bytes", minUncompressedSize, uncompressedBodyLen)

	// try the request again, but accept gzip encoding
	bulkGetHeaders["Accept-Encoding"] = "gzip"
	resp = rt.SendAdminRequestWithHeaders(http.MethodPost, "/db/_bulk_get", bulkGetBody, bulkGetHeaders)
	RequireStatus(t, resp, http.StatusOK)

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
	RequireStatus(t, response, 400)
}

func TestBulkDocsChangeToAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { requireAccess(doc.channel)}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
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

	response := rt.Send(RequestByUser("POST", "/db/_bulk_docs", input, "user1"))
	RequireStatus(t, response, 201)

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
	ctx := rt.Context()

	// Create a role with no channels assigned to it
	authenticator := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
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

	response := rt.Send(RequestByUser("POST", "/db/_bulk_docs", input, "user1"))
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 201)
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
	RequireStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 1, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "14-jkl", "id": "bdne1"}, docs[0])

}

type RevDiffResponse map[string][]string
type RevsDiffResponse map[string]RevDiffResponse

func TestRevsDiff(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	response := rt.SendRequest("POST", "/db/_bulk_docs", input)
	RequireStatus(t, response, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-xyz"],
              "rd2": ["34-def"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
	response = rt.SendRequest("POST", "/db/_revs_diff", input)
	RequireStatus(t, response, 200)
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
	RequireStatus(t, response, 201)

	reqHeaders := map[string]string{
		"Accept": "application/json",
	}
	response = rt.SendAdminRequestWithHeaders("GET", `/db/or1?open_revs=["12-abc","10-ten"]`, "", reqHeaders)
	RequireStatus(t, response, 200)

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
		RequireStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev := body["rev"].(string)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v?rev=%s", k, rev), fmt.Sprintf(`{"val":"2-%s"}`, k))
		RequireStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev = body["rev"].(string)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%v?rev=%s", k, rev), fmt.Sprintf(`{"val":"3-%s"}`, k))
		RequireStatus(t, response, 201)
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

		partBytes, err := io.ReadAll(part)
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
	RequireStatus(t, response, 404)

	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	RequireStatus(t, response, 200)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-1", respBody[db.BodyRev])
	assert.Equal(t, "there", respBody["hi"])

	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "there"}`)
	RequireStatus(t, response, 409)
	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	RequireStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-2", respBody[db.BodyRev])
	assert.Equal(t, "again", respBody["hi"])

	// Check the handling of large integers, which caused trouble for us at one point:
	response = rt.SendAdminRequest("PUT", "/db/_local/loc1", `{"big": 123456789, "_rev": "0-2"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	RequireStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-3", respBody[db.BodyRev])
	assert.Equal(t, float64(123456789), respBody["big"])

	response = rt.SendAdminRequest("DELETE", "/db/_local/loc1", "")
	RequireStatus(t, response, 409)
	response = rt.SendAdminRequest("DELETE", "/db/_local/loc1?rev=0-3", "")
	RequireStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/_local/loc1", "")
	RequireStatus(t, response, 404)
	response = rt.SendAdminRequest("DELETE", "/db/_local/loc1", "")
	RequireStatus(t, response, 404)

	// Check the handling of URL encoded slash at end of _local%2Fdoc
	response = rt.SendAdminRequest("PUT", "/db/_local%2Floc12", `{"hi": "there"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_local/loc2", "")
	RequireStatus(t, response, 404)
	response = rt.SendAdminRequest("DELETE", "/db/_local%2floc2", "")
	RequireStatus(t, response, 404)
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
	RequireStatus(t, response, 201)

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
	RequireStatus(t, response, 200)
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
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequestWithHeaders("GET", "/db/_local/loc1", "",
		map[string]string{"Accept-Encoding": "foo, gzip, bar"})
	RequireStatus(t, response, 200)
	assert.Equal(t, "gzip", response.Header().Get("Content-Encoding"))
	unzip, err := gzip.NewReader(response.Body)
	assert.NoError(t, err)
	unjson := base.JSONDecoder(unzip)
	var body db.Body
	assert.Equal(t, nil, unjson.Decode(&body))
	assert.Equal(t, str, body["long"])
}

func TestReadChangesOptionsFromJSON(t *testing.T) {

	ctx := base.TestCtx(t)
	h := &handler{}
	h.server = NewServerContext(ctx, &StartupConfig{}, false)
	defer h.server.Close(ctx)

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
	h.server.Config.Replicator.MaxHeartbeat = base.NewConfigDuration(time.Minute)
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

	RequireStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":"Cinemax"}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["WB", "Cinemax"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["CBS", "Cinemax"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["CBS"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":[]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create a user:
	alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "Cinemax"))
	assert.NoError(t, a.Save(alice))

	// Get a single doc the user has access to:
	request, _ := http.NewRequest("GET", "/db/doc3", nil)
	request.SetBasicAuth("alice", "letmein")
	response := rt.Send(request)
	RequireStatus(t, response, 200)

	// Get a single doc the user doesn't have access to:
	request, _ = http.NewRequest("GET", "/db/doc2", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 403)

	// Check that _all_docs only returns the docs the user has access to:
	request, _ = http.NewRequest("GET", "/db/_all_docs?channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create users:
	alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(alice))
	zegpold, err := a.NewUser("zegpold", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(zegpold))

	// Create some docs that give users access:
	response := rt.Send(Request("PUT", "/db/alpha", `{"owner":"alice"}`))
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	alphaRevID := body["rev"].(string)

	RequireStatus(t, rt.Send(Request("PUT", "/db/beta", `{"owner":"boadecia"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/delta", `{"owner":"alice"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/gamma", `{"owner":"zegpold"}`)), 201)

	RequireStatus(t, rt.Send(Request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)

	rt.MustWaitForDoc("g1", t)

	changes := ChangesResults{}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "zegpold"))
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
	RequireStatus(t, rt.Send(Request("PUT", "/db/alpha", str)), 201)

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
	changes = ChangesResults{}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "alice"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	assert.NoError(t, err)
	assert.Equal(t, "d1", changes.Results[0].ID)

	// The complete _changes feed for zegpold contains docs a1 and g1:
	changes = ChangesResults{}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "zegpold"))
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
	response = rt.Send(RequestByUser("GET", fmt.Sprintf("/db/_changes?since=\"%s\"", since),
		"", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "a1", changes.Results[0].ID)

	// What happens if we call access() with a nonexistent username?
	RequireStatus(t, rt.Send(Request("PUT", "/db/epsilon", `{"owner":"waldo"}`)), 201) // seq 10

	// Must wait for sequence to arrive in cache, since the cache processor will be paused when UpdateSyncFun() is called
	// below, which could lead to a data race if the cache processor is paused while it's processing a change
	rt.MustWaitForDoc("epsilon", t)

	// Finally, throw a wrench in the works by changing the sync fn. Note that normally this wouldn't
	// be changed while the database is in use (only when it's re-opened) but for testing purposes
	// we do it now because we can't close and re-open an ephemeral Walrus database.
	dbc := rt.ServerContext().Database(ctx, "db")
	database, _ := db.GetDatabase(dbc, nil)

	collection := database.GetSingleDatabaseCollectionWithUser()

	changed, err := database.UpdateSyncFun(ctx, `function(doc) {access("alice", "beta");channel("beta");}`)
	assert.NoError(t, err)
	assert.True(t, changed)
	changeCount, err := collection.UpdateAllDocChannels(ctx, false, func(docsProcessed, docsChanged *int) {}, base.NewSafeTerminator())
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(bernard))

	// Create doc that gives user access to its channel
	response := rt.SendAdminRequest("PUT", "/db/alpha", `{"owner":"bernard", "channel":"PBS"}`)
	RequireStatus(t, response, 201)
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
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	require.Len(t, changes.Results, 1)
	if len(changes.Results) > 0 {
		assert.Equal(t, "alpha", changes.Results[0].ID)
	}

	// Delete the document
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/alpha?rev=%s", revId), "")
	RequireStatus(t, response, 200)

	// Make sure it actually was deleted
	response = rt.SendAdminRequest("GET", "/db/alpha", "")
	RequireStatus(t, response, 404)

	// Wait for change caching to complete
	assert.NoError(t, rt.WaitForPendingChanges())

	// Check user access again:
	changes.Results = nil
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "bernard"))
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

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Uint32Ptr(0)}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/"+testDocID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+revID, `{}`)
	RequireStatus(t, response, 200)

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+revID, `{"`+testdataKey+`":true,"update":2}`)
	RequireStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
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
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+revID, `{}`)
	RequireStatus(t, response, 200)
	body = nil
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revID = body["rev"].(string)

	response = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+revID, `{"`+testdataKey+`":true}`)
	RequireStatus(t, response, 201)

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), testDocID)
	assert.NoError(t, err)

	actualProperties := syncData.Channels.KeySet()
	assert.ElementsMatchf(t, expectedProperties, actualProperties, "Expected sync fn oldDoc body %q to match expectedProperties: %q", actualProperties, expectedProperties)
}

// TestSyncFnDocBodyPropertiesSwitchActiveTombstone creates a branched revtree, where the first tombstone created becomes active again after the shorter b branch is tombstoned.
// The test makes sure that in this scenario, the "doc" body of the sync function when switching from (T) 3-b to (T) 4-a contains a _deleted property (stamped by getAvailable1xRev)
//
//	1-a
//	├── 2-a
//	│   └── 3-a
//	│       └──────── (T) 4-a
//	└──────────── 2-b
//	              └────────────── (T) 3-b
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
	RequireStatus(t, resp, 201)
	rev1ID := RespRevID(t, resp)

	// rev 2-a
	resp = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+rev1ID, `{"`+testdataKey+`":2}`)
	RequireStatus(t, resp, 201)
	rev2aID := RespRevID(t, resp)

	// rev 3-a
	resp = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?rev="+rev2aID, `{"`+testdataKey+`":3,"syncOldDocBodyCheck":true}`)
	RequireStatus(t, resp, 201)
	rev3aID := RespRevID(t, resp)

	// rev 2-b
	_, rev1Hash := db.ParseRevID(rev1ID)
	resp = rt.SendAdminRequest("PUT", "/db/"+testDocID+"?new_edits=false", `{"`+db.BodyRevisions+`":{"start":2,"ids":["b", "`+rev1Hash+`"]}}`)
	RequireStatus(t, resp, 201)
	rev2bID := RespRevID(t, resp)

	// tombstone at 4-a
	resp = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+rev3aID, `{}`)
	RequireStatus(t, resp, 200)

	numErrorsBefore, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)
	// tombstone at 3-b
	resp = rt.SendAdminRequest("DELETE", "/db/"+testDocID+"?rev="+rev2bID, `{}`)
	RequireStatus(t, resp, 200)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, 1, numErrorsAfter-numErrorsBefore, "expecting to see only only 1 error logged")
}

// Test for wrong _changes entries for user joining a populated channel
func TestUserJoiningPopulatedChannel(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	assert.NoError(t, a.Save(guest))

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	RequireStatus(t, response, 201)

	// Create 100 docs
	for i := 0; i < 100; i++ {
		docpath := fmt.Sprintf("/db/doc%d", i)
		RequireStatus(t, rt.Send(Request("PUT", docpath, `{"foo": "bar", "channels":["alpha"]}`)), 201)
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
	RequireStatus(t, response, 201)

	// Retrieve all changes for user2 with no limits
	changesResults, err = rt.WaitForChanges(101, fmt.Sprintf("/db/_changes"), "user2", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 101)
	assert.Equal(t, "doc99", changesResults.Results[99].ID)

	// Create user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"email":"user3@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	RequireStatus(t, response, 201)

	getUserResponse := rt.SendAdminRequest("GET", "/db/_user/user3", "")
	RequireStatus(t, getUserResponse, 200)
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
	RequireStatus(t, response, 201)
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// POST a role
	response := rt.SendAdminRequest("POST", "/db/_role/", `{"name":"role1","admin_channels":["chan1"]}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/role1", "")
	RequireStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "role1", body["name"])

	// Put document to trigger sync function
	response = rt.Send(Request("PUT", "/db/doc1", `{"user":"user1", "role":"role:role1", "channel":"chan1"}`)) // seq=1
	RequireStatus(t, response, 201)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])

	// POST the new user the GET and verify that it shows the assigned role
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"letmein"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/user1", "")
	RequireStatus(t, response, 200)
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	require.NoError(t, err)

	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create users:
	response := rt.SendAdminRequest("PUT", "/db/_user/alice", `{"password":"letmein", "admin_channels":["alpha"]}`)
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_user/zegpold", `{"password":"letmein", "admin_channels":["beta"]}`)
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["gamma"]}`)
	RequireStatus(t, response, 201)
	/*
		alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "alpha"))
		assert.NoError(t, a.Save(alice))
		zegpold, err := a.NewUser("zegpold", "letmein", channels.BaseSetOf(t, "beta"))
		assert.NoError(t, a.Save(zegpold))

		hipster, err := a.NewRole("hipster", channels.BaseSetOf(t, "gamma"))
		assert.NoError(t, a.Save(hipster))
	*/

	// Create some docs in the channels:
	cacheWaiter := rt.ServerContext().Database(ctx, "db").NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(1)
	response = rt.Send(Request("PUT", "/db/fashion",
		`{"user":"alice","role":["role:hipster","role:bogus"]}`))
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	fashionRevID := body["rev"].(string)

	roleGrantSequence := rt.GetDocumentSequence("fashion")

	cacheWaiter.Add(4)
	RequireStatus(t, rt.Send(Request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)

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
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("1st _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 3, len(changes.Results))
	assert.Equal(t, "_user/alice", changes.Results[0].ID)
	assert.Equal(t, "g1", changes.Results[1].ID)
	assert.Equal(t, "a1", changes.Results[2].ID)

	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("2nd _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "_user/zegpold", changes.Results[0].ID)
	assert.Equal(t, "b1", changes.Results[1].ID)
	lastSeqPreGrant := changes.Last_Seq

	// Update "fashion" doc to grant zegpold the role "hipster" and take it away from alice:
	cacheWaiter.Add(1)
	str := fmt.Sprintf(`{"user":"zegpold", "role":"role:hipster", "_rev":%q}`, fashionRevID)
	RequireStatus(t, rt.Send(Request("PUT", "/db/fashion", str)), 201)

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
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "zegpold"))
	log.Printf("3rd _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	log.Printf("changes: %+v", changes.Results)
	require.Equal(t, len(changes.Results), 3)
	assert.Equal(t, "_user/zegpold", changes.Results[0].ID)
	assert.Equal(t, "b1", changes.Results[1].ID)
	assert.Equal(t, "g1", changes.Results[2].ID)

	// Changes feed with since=lastSeqPreGrant would ordinarily be empty, but zegpold got access to channel
	// gamma after lastSeqPreGrant, so the pre-existing docs in that channel are included:
	response = rt.Send(RequestByUser("GET", fmt.Sprintf("/db/_changes?since=%v", lastSeqPreGrant), "", "zegpold"))
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create a doc
	response := rt.Send(Request("PUT", "/db/doc1", `{"foo":"bar", "channels":["ch1"]}`))
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	doc1RevID := body["rev"].(string)

	// Run GET _all_docs as admin with channels=true:
	response = rt.SendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Run POST _all_docs as admin with explicit docIDs and channels=true:
	keys := `{"keys": ["doc1"]}`
	response = rt.SendAdminRequest("POST", "/db/_all_docs?channels=true", keys)
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Commit rev 2 that maps to a differenet channel
	str := fmt.Sprintf(`{"foo":"bar", "channels":["ch2"], "_rev":%q}`, doc1RevID)
	RequireStatus(t, rt.Send(Request("PUT", "/db/doc1", str)), 201)

	// Run GET _all_docs as admin with channels=true
	// Make sure that only the new channel appears in the docs channel list
	response = rt.SendAdminRequest("GET", "/db/_all_docs?channels=true", "")
	RequireStatus(t, response, 200)

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
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch2", allDocsResult.Rows[0].Value.Channels[0])
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create user:
	frank, err := a.NewUser("charles", "1234", nil)
	assert.NoError(t, a.Save(frank))

	// Create a doc:
	response := rt.Send(Request("PUT", "/db/testOldDocId", `{"foo":"bar"}`))
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	alphaRevID := body["rev"].(string)

	// Update a document to validate oldDoc id handling.  Will reject if old doc id not available
	str := fmt.Sprintf(`{"foo":"ball", "_rev":%q}`, alphaRevID)
	RequireStatus(t, rt.Send(Request("PUT", "/db/testOldDocId", str)), 201)

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

	RequireStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":["books"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["gifts"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["!"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["gifts"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":["!"]}`), 201)
	// document added to no channel should only end up available to users with * access
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc6", `{"channels":[]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.NoError(t, err)
	//
	// Part 1 - Tests for user with single channel access:
	//
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "books"))
	assert.NoError(t, a.Save(bernard))

	// GET /db/docid - basic test for channel user has
	response := rt.Send(RequestByUser("GET", "/db/doc1", "", "bernard"))
	RequireStatus(t, response, 200)

	// GET /db/docid - negative test for channel user doesn't have
	response = rt.Send(RequestByUser("GET", "/db/doc2", "", "bernard"))
	RequireStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(RequestByUser("GET", "/db/doc3", "", "bernard"))
	RequireStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns the docs the user has access to:
	response = rt.Send(RequestByUser("GET", "/db/_all_docs?channels=true", "", "bernard"))
	RequireStatus(t, response, 200)

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
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(changes.Results))
	since := changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for single channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=books", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	// GET /db/_changes for unauthorized channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=gifts", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(changes.Results))

	//
	// Part 2 - Tests for user with * channel access
	//

	// Create a user:
	fran, err := a.NewUser("fran", "letmein", channels.BaseSetOf(t, "*"))
	assert.NoError(t, a.Save(fran))

	// GET /db/docid - basic test for doc that has channel
	response = rt.Send(RequestByUser("GET", "/db/doc1", "", "fran"))
	RequireStatus(t, response, 200)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(RequestByUser("GET", "/db/doc3", "", "fran"))
	RequireStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns all docs (based on user * channel)
	response = rt.Send(RequestByUser("GET", "/db/_all_docs?channels=true", "", "fran"))
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"books"}, allDocsResult.Rows[0].Value.Channels)

	// GET /db/_changes
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "fran"))
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
	response = rt.Send(RequestByUser("GET", "/db/doc1", "", "manny"))
	RequireStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(RequestByUser("GET", "/db/doc3", "", "manny"))
	RequireStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs only returns ! docs (based on doc ! channel)
	response = rt.Send(RequestByUser("GET", "/db/_all_docs?channels=true", "", "manny"))
	RequireStatus(t, response, 200)
	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)

	// GET /db/_changes
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "manny"))
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
	RequireStatus(t, response, 412)
	// Attempt to create new target DB on public API
	response = rt.SendRequest("PUT", "/foo/", "")
	RequireStatus(t, response, 403)
}

func TestEventConfigValidationSuccess(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip this test under integration testing")
	}

	ctx := base.TestCtx(t)
	sc := NewServerContext(ctx, &StartupConfig{}, false)
	defer sc.Close(ctx)

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

	_, err = sc.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig})
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
	err := DecodeAndSanitiseConfig(buf, &dbConfig)
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
	RequireStatus(t, response, 201)
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

// TestDocExpiry validates the value of the expiry as set in the document.  It doesn't validate actual expiration (not supported
// in walrus).
func TestDocExpiry(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body
	response := rt.SendAdminRequest("PUT", "/db/expNumericTTL", `{"_exp":100}`)
	RequireStatus(t, response, 201)

	// Validate that exp isn't returned on the standard GET, bulk get
	response = rt.SendAdminRequest("GET", "/db/expNumericTTL", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok := body["_exp"]
	assert.Equal(t, false, ok)

	bulkGetDocs := `{"docs": [{"id": "expNumericTTL", "rev": "1-ca9ad22802b66f662ff171f226211d5c"}]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_get", bulkGetDocs)
	RequireStatus(t, response, 200)
	responseString := string(response.Body.Bytes())
	assert.True(t, !strings.Contains(responseString, "_exp"), "Bulk get response contains _exp property when show_exp not set.")

	response = rt.SendAdminRequest("POST", "/db/_bulk_get?show_exp=true", bulkGetDocs)
	RequireStatus(t, response, 200)
	responseString = string(response.Body.Bytes())
	assert.True(t, strings.Contains(responseString, "_exp"), "Bulk get response doesn't contain _exp property when show_exp was set.")

	body = nil
	response = rt.SendAdminRequest("GET", "/db/expNumericTTL?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	// Validate other exp formats
	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expNumericUnix", `{"val":1, "_exp":4260211200}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/expNumericUnix?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("numeric unix response: %s", response.Body.Bytes())
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expNumericString", `{"val":1, "_exp":"100"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/expNumericString?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expBadString", `{"_exp":"abc"}`)
	RequireStatus(t, response, 400)
	response = rt.SendAdminRequest("GET", "/db/expBadString?show_exp=true", "")
	RequireStatus(t, response, 404)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, false, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expDateString", `{"_exp":"2105-01-01T00:00:00.000+00:00"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/expDateString?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/db/expBadDateString", `{"_exp":"2105-0321-01T00:00:00.000+00:00"}`)
	RequireStatus(t, response, 400)
	response = rt.SendAdminRequest("GET", "/db/expBadDateString?show_exp=true", "")
	RequireStatus(t, response, 404)
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
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("GET", "/db/expNumericTTL?show_exp=true", "")
	RequireStatus(t, response, 200)
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.True(t, err == nil)
	assert.NoError(t, a.Save(bernard))

	// Issue is only reproducible when the wait counter is zero for all requested channels (including the user channel) - the count=0
	// triggers early termination of the changes loop.  This can only be reproduced if the feed is restarted after the user is created -
	// otherwise the count for the user pseudo-channel will always be non-zero
	db, _ := rt.ServerContext().GetDatabase(ctx, "db")
	err = db.RestartListener()
	assert.True(t, err == nil)
	// Put a document to increment the counter for the * channel
	response := rt.Send(Request("PUT", "/db/lost", `{"channel":["ABC"]}`))
	RequireStatus(t, response, 201)

	// Previous bug: changeWaiter was treating the implicit '*' wildcard in the _changes request as the '*' channel, so the wait counter
	// was being initialized to 1 (the previous PUT).  Later the wildcard was resolved to actual channels (PBS, _sync:user:bernard), which
	// has a wait counter of zero (no documents writted since the listener was restarted).
	wg := sync.WaitGroup{}
	// start changes request
	go func() {
		wg.Add(1)
		defer wg.Done()
		changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
		changesResponse := rt.Send(RequestByUser("POST", "/db/_changes", changesJSON, "bernard"))
		log.Printf("_changes looks like: %s", changesResponse.Body.Bytes())
		err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
		// Checkthat the changes loop isn't returning an empty result immediately (the previous bug) - should
		// be waiting until entry 'sherlock', created below, appears.
		assert.True(t, len(changes.Results) > 0)
	}()

	// Send a doc that will properly close the longpoll response
	time.Sleep(1 * time.Second)
	response = rt.Send(Request("PUT", "/db/sherlock", `{"channel":["PBS"]}`))
	wg.Wait()
}

func TestUnsupportedConfig(t *testing.T) {

	ctx := base.TestCtx(t)
	sc := NewServerContext(ctx, &StartupConfig{}, false)
	defer sc.Close(ctx)

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

	_, err = sc.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: testProviderOnlyConfig})
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

	_, err = sc.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: viewsOnlyConfig})
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

	_, err = sc.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: viewsAndTestConfig})
	assert.NoError(t, err, "Error adding viewsAndTestConfig database.")
}

func TestDocIDFilterResurrection(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create User
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	jacques, err := a.NewUser("jacques", "letmein", channels.BaseSetOf(t, "A", "B"))
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

func TestNumAccessErrors(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc, oldDoc){if (doc.channels.indexOf("foo") > -1){requireRole("foobar")}}`,
	}

	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

	// Create a test user
	user, err := a.NewUser("user", "letmein", channels.BaseSetOf(t, "A"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	response := rt.Send(RequestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user"))
	RequireStatus(t, response, 403)

	base.WaitForStat(func() int64 { return rt.GetDatabase().DbStats.SecurityStats.NumAccessErrors.Value() }, 1)
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
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/testdoc2", `{"value":"a value", "channels":["a"]}`)
	err = base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)
	rev2 := fmt.Sprint(responseBody["rev"])
	RequireStatus(t, response, http.StatusCreated)

	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	response = rt.SendAdminRequest("GET", "/db/_changes?active_only=true&include_docs=true&filter=sync_gateway/bychannel&channels=a&feed=normal&since=0&heartbeat=0&timeout=300000", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("PUT", "/db/testdoc?new_edits=true&rev="+rev1, `{"value":"a value", "channels":[]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/testdoc2?new_edits=true&rev="+rev2, `{"value":"a value", "channels":[]}`)
	RequireStatus(t, response, http.StatusCreated)

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
		out, err := io.ReadAll(r.Body)
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

	wg.Add(1)
	rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar"}`)

	if base.TestUseXattrs() {
		wg.Add(1)
		body := make(map[string]interface{})
		body["foo"] = "bar"
		added, err := rt.Bucket().Add("doc2", 0, body)
		assert.True(t, added)
		assert.NoError(t, err)
	}

	require.NoError(t, WaitWithTimeout(&wg, 30*time.Second))
}

func TestBasicGetReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// Put document as usual
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar"}`)
	RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)

	// Get a document with rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true&rev="+revID, ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	// Get a document without specifying rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true", ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
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
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	// Put basic doc with replicator2 flag and ensure it saves correctly
	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID, `{"foo": "bar"}`)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("GET", "/db/doc1", ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotFound)
	}
}

func TestDeletedPutReplicator2(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	response := rt.SendAdminRequest("PUT", "/db/doc1", "{}")
	RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().NumDocWrites.Value())

	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID+"&deleted=true", "{}")
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/db/doc1", ``)
		RequireStatus(t, response, http.StatusNotFound)
		assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID+"&deleted=false", `{}`)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 3, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/db/doc1", ``)
		RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
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
	RequireStatus(t, res, http.StatusCreated)
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
	RequireStatus(t, res, http.StatusCreated)
	rev1 := RespRevID(t, res)
	_, rev1Hash := db.ParseRevID(rev1)

	// push winning branch
	wg.Add(2)
	res = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", `{"foo":"buzz","_revisions":{"start":3,"ids":["buzz","bar","`+rev1Hash+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)

	// push non-winning branch
	wg.Add(1)
	res = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", `{"foo":"buzzzzz","_revisions":{"start":2,"ids":["buzzzzz","`+rev1Hash+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)

	wg.Wait()
	assert.Equal(t, 2, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 3, int(atomic.LoadUint32(&DocumentChangedCount)))

	// tombstone the winning branch and ensure we get a rev changed message for the promoted branch
	wg.Add(2)
	res = rt.SendAdminRequest("DELETE", "/db/doc1?rev=3-buzz", ``)
	RequireStatus(t, res, http.StatusOK)

	wg.Wait()
	assert.Equal(t, 3, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 4, int(atomic.LoadUint32(&DocumentChangedCount)))

	// push a separate winning branch
	wg.Add(2)
	res = rt.SendAdminRequest("PUT", "/db/doc1?new_edits=false", `{"foo":"quux","_revisions":{"start":4,"ids":["quux", "buzz","bar","`+rev1Hash+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)

	// tombstone the winning branch, we should get a second webhook fired for rev 2-buzzzzz now it's been resurrected
	wg.Add(2)
	res = rt.SendAdminRequest("DELETE", "/db/doc1?rev=4-quux", ``)
	RequireStatus(t, res, http.StatusOK)

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

	dirPath := t.TempDir()

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
		RequireStatus(t, response, http.StatusOK)
		fi, err := os.Stat(filePath)
		assert.NoError(t, err, "fetching the file information")
		assert.True(t, fi.Size() > 0)

		// Send profile request with missing JSON 'file' parameter.
		response = rt.SendAdminRequest(http.MethodPost, resource, "{}")
		RequireStatus(t, response, http.StatusBadRequest)
		assert.Contains(t, string(response.BodyBytes()), "Missing JSON 'file' parameter")

		// Send a profile request with invalid json body
		response = rt.SendAdminRequest(http.MethodPost, resource, "invalid json body")
		RequireStatus(t, response, http.StatusBadRequest)
		assert.Contains(t, string(response.BodyBytes()), "Invalid JSON")

		// Send a profile request with unknown file path; Internal Server Error
		reqBodyText = `{"file":"sftp://unknown/path"}`
		response = rt.SendAdminRequest(http.MethodPost, resource, reqBodyText)
		RequireStatus(t, response, http.StatusInternalServerError)
		assert.Contains(t, string(response.BodyBytes()), "Internal Server Error")
	}

	// Send profile request for a profile which doesn't exists; unknown
	filePath := filepath.Join(dirPath, "unknown.pprof")
	reqBodyText := fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
	response := rt.SendAdminRequest(http.MethodPost, "/_profile/unknown", reqBodyText)
	log.Printf("string(response.BodyBytes()): %v", string(response.BodyBytes()))
	RequireStatus(t, response, http.StatusNotFound)
	assert.Contains(t, string(response.BodyBytes()), `No such profile \"unknown\"`)

	// Send profile request with filename and empty profile name; it should end up creating cpu profile
	filePath = filepath.Join(dirPath, "cpu.pprof")
	reqBodyText = fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
	response = rt.SendAdminRequest(http.MethodPost, "/_profile", reqBodyText)
	log.Printf("string(response.BodyBytes()): %v", string(response.BodyBytes()))
	RequireStatus(t, response, http.StatusOK)
	fi, err := os.Stat(filePath)
	assert.NoError(t, err, "fetching the file information")
	assert.False(t, fi.Size() > 0)

	// Send profile request with no filename and profile name; it should stop cpu profile
	response = rt.SendAdminRequest(http.MethodPost, "/_profile", "")
	log.Printf("string(response.BodyBytes()): %v", string(response.BodyBytes()))
	RequireStatus(t, response, http.StatusOK)
	fi, err = os.Stat(filePath)
	assert.NoError(t, err, "fetching the file information")
	assert.True(t, fi.Size() > 0)
}

func TestHandleHeapProfiling(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	dirPath := t.TempDir()

	// Send a valid request for heap profiling
	filePath := filepath.Join(dirPath, "heap.pprof")
	reqBodyText := fmt.Sprintf(`{"file":"%v"}`, filepath.ToSlash(filePath))
	response := rt.SendAdminRequest(http.MethodPost, "/_heap", reqBodyText)
	RequireStatus(t, response, http.StatusOK)
	fi, err := os.Stat(filePath)
	assert.NoError(t, err, "fetching heap profile file information")
	assert.True(t, fi.Size() > 0)

	// Send a profile request with invalid json body
	response = rt.SendAdminRequest(http.MethodPost, "/_heap", "invalid json body")
	RequireStatus(t, response, http.StatusBadRequest)
	assert.Contains(t, string(response.BodyBytes()), "Invalid JSON")

	// Send profile request with missing JSON 'file' parameter.
	response = rt.SendAdminRequest(http.MethodPost, "/_heap", "{}")
	RequireStatus(t, response, http.StatusInternalServerError)
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
			RequireStatus(t, response, http.StatusOK)

			response = rt.SendAdminRequest(http.MethodPost, inputResource, "")
			assert.Equal(t, "", response.Header().Get("Content-Disposition"))
			assert.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			RequireStatus(t, response, http.StatusOK)
		})
	}
}

func TestHandlePprofs(t *testing.T) {
	base.LongRunningTest(t)

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
			RequireStatus(t, response, http.StatusOK)

			response = rt.SendAdminRequest(http.MethodPost, tc.inputResource, "")
			cdHeader = response.Header().Get("Content-Disposition")
			assert.Truef(t, strings.HasPrefix(cdHeader, expectedContentDispositionPrefix), "Expected header prefix: %q but got %q", expectedContentDispositionPrefix, cdHeader)
			assert.Equal(t, "application/octet-stream", response.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			RequireStatus(t, response, http.StatusOK)
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
	RequireStatus(t, response, http.StatusOK)
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
			rt := NewRestTester(t, &RestTesterConfig{HideProductInfo: test.hideProductInfo})
			defer rt.Close()

			// admins can always see product info, even if setting is enabled
			if test.admin {
				resp := rt.SendAdminRequest(http.MethodGet, "/", "")
				RequireStatus(t, resp, http.StatusOK)

				assert.Equal(t, base.VersionString, resp.Header().Get("Server"))

				body := string(resp.BodyBytes())
				assert.Contains(t, body, base.ProductAPIVersion)
				return
			}

			// non-admins can only see product info when the hideProductInfo option is disabled
			resp := rt.SendRequest(http.MethodGet, "/", "")
			RequireStatus(t, resp, http.StatusOK)

			serverHeader := resp.Header().Get("Server")
			body := string(resp.BodyBytes())

			if test.hideProductInfo {
				assert.Equal(t, base.ProductNameString, serverHeader)
				assert.Contains(t, body, base.ProductNameString)
				// no versions
				assert.NotEqual(t, base.VersionString, serverHeader)
				assert.NotContains(t, body, base.ProductAPIVersion)
			} else {
				assert.Equal(t, base.VersionString, serverHeader)
				assert.Contains(t, body, base.ProductNameString)
				assert.Contains(t, body, base.ProductAPIVersion)
			}
		})
	}
}

func TestDeleteNonExistentDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("DELETE", "/db/fake", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/fake", "")
	RequireStatus(t, response, http.StatusNotFound)

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
	RequireStatus(t, response, http.StatusCreated)
	assert.NoError(t, json.Unmarshal(response.BodyBytes(), &body))
	rev := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/db/doc1?rev="+rev, "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	RequireStatus(t, response, http.StatusNotFound)

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
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/db/doc", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`, string(response.BodyBytes()))

	response = rt.SendAdminRequest("PUT", "/db/doc?rev=1-ca9ad22802b66f662ff171f226211d5c", `{"val": "newval"}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/db/doc", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"2-2f981cadffde70e8a1d9dc386a410e0d","val":"newval"}`, string(response.BodyBytes()))
}

func TestTombstonedBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	RequireStatus(t, response, http.StatusCreated)

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
	RequireStatus(t, resp, http.StatusOK)

	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	RequireStatus(t, response, http.StatusCreated)

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
	RequireStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err = rt.GetDatabase().Bucket.Get(t.Name(), &body)

	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err))
	assert.Nil(t, body)
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

	defer db.SuspendSequenceBatching()()

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
				RequireStatus(t, resp, http.StatusOK)
			},
		},
		{
			name:       "PUT",
			autoImport: false,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				resp := rt.SendAdminRequest("PUT", "/db/"+docKey, "{}")
				RequireStatus(t, resp, http.StatusConflict)
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
			RequireStatus(t, resp, http.StatusCreated)

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
	RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, rt.WaitForPendingChanges())

	// Get current sync data
	var syncData db.SyncData
	_, err := subdocXattrStore.SubdocGetXattr(docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	docRev, err := rt.GetDatabase().GetSingleDatabaseCollection().GetRevisionCacheForTest().Get(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
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

	docRev2, err := rt.GetDatabase().GetSingleDatabaseCollection().GetRevisionCacheForTest().Get(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
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
	RequireStatus(t, resp, http.StatusCreated)
	err := json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 1)
	assert.Equal(t, syncData.ChannelSet[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 0})
	assert.Len(t, syncData.ChannelSetHistory, 0)

	// Update doc to remove from channel and ensure a single channel history entry with both start and end sequences
	// and no old channel history entries
	resp = rt.SendAdminRequest("PUT", "/db/doc?rev="+body["rev"].(string), `{"channels": []}`)
	RequireStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 1)
	assert.Equal(t, syncData.ChannelSet[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 2})
	assert.Len(t, syncData.ChannelSetHistory, 0)

	// Update doc to add to channels test and test2 and ensure a single channel history entry for both test and test2
	// both with start sequences only and ensure old test entry was moved to old
	resp = rt.SendAdminRequest("PUT", "/db/doc?rev="+body["rev"].(string), `{"channels": ["test", "test2"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
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
	RequireStatus(t, resp, http.StatusOK)

	// Remove doc from channel and ensure that channel history is built correctly with current end sequence and
	// setting start sequence
	resp = rt.SendAdminRequest("PUT", "/db/doc1?rev=1-08267a64bf0e3963bab7dece1ea0887a", `{"channels": []}`)
	RequireStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc1")
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

func (rt *RestTester) CreateDocReturnRev(t *testing.T, docID string, revID string, bodyIn interface{}) string {
	bodyJSON, err := base.JSONMarshal(bodyIn)
	assert.NoError(t, err)

	url := "/db/" + docID
	if revID != "" {
		url += "?rev=" + revID
	}

	resp := rt.SendAdminRequest("PUT", url, string(bodyJSON))
	RequireStatus(t, resp, http.StatusCreated)

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

func TestMetricsHandler(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SkipPrometheusStatsRegistration = false
	defer func() {
		base.SkipPrometheusStatsRegistration = true
	}()

	// Create and remove a database
	// This ensures that creation and removal of a DB is possible without a re-registration issue ( the below rest tester will re-register "db")
	ctx := base.TestCtx(t)
	context, err := db.NewDatabaseContext(ctx, "db", base.GetTestBucket(t), false, db.DatabaseContextOptions{})
	require.NoError(t, err)
	context.Close(context.AddDatabaseLogContext(ctx))

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
	bodyString, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(bodyString), `database="db"`)
	err = resp.Body.Close()
	assert.NoError(t, err)

	// Initialize another database to ensure both are registered successfully
	context, err = db.NewDatabaseContext(ctx, "db2", base.GetTestBucket(t), false, db.DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close(context.AddDatabaseLogContext(ctx))

	// Validate that metrics still works with both db and db2 databases and that they have entries
	resp, err = httpClient.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyString, err = io.ReadAll(resp.Body)
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
	ctx := rt.Context()

	resp := rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	database, err := db.CreateDatabase(rt.GetDatabase())
	assert.NoError(t, err)

	collection := database.GetSingleDatabaseCollectionWithUser()
	userHasDocAccess, err := db.UserHasDocAccess(ctx, collection, "doc")
	assert.NoError(t, err)
	assert.True(t, userHasDocAccess)

	// Purge the document from the bucket to force 'not found'
	err = collection.Purge(ctx, "doc")

	userHasDocAccess, err = db.UserHasDocAccess(ctx, collection, "doc")
	assert.NoError(t, err)
	assert.False(t, userHasDocAccess)
}

func TestDocChannelSetPruning(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	rt := NewRestTester(t, nil)
	defer rt.Close()

	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})

	for i := 0; i < 10; i++ {
		revID = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": []string{}})
		revID = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": []string{"a"}})
	}

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSetHistory, db.DocumentHistoryMaxEntriesPerChannel)
	assert.Equal(t, "a", syncData.ChannelSetHistory[0].Name)
	assert.Equal(t, uint64(1), syncData.ChannelSetHistory[0].Start)
	assert.Equal(t, uint64(12), syncData.ChannelSetHistory[0].End)
}

func TestTombstoneCompactionAPI(t *testing.T) {
	rt := NewRestTester(t, nil)
	rt.GetDatabase().PurgeInterval = 0
	defer rt.Close()

	for i := 0; i < 100; i++ {
		resp := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc%d", i), "{}")
		RequireStatus(t, resp, http.StatusCreated)
		rev := RespRevID(t, resp)
		resp = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/doc%d?rev=%s", i, rev), "{}")
		RequireStatus(t, resp, http.StatusOK)
	}

	resp := rt.SendAdminRequest("GET", "/db/_compact", "")
	RequireStatus(t, resp, http.StatusOK)

	var tombstoneCompactionStatus db.TombstoneManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
	assert.NoError(t, err)

	assert.Equal(t, db.BackgroundProcessStateCompleted, tombstoneCompactionStatus.State)
	assert.Empty(t, tombstoneCompactionStatus.LastErrorMessage)
	assert.Equal(t, 0, int(tombstoneCompactionStatus.DocsPurged))
	firstStartTimeStat := rt.GetDatabase().DbStats.Database().CompactionAttachmentStartTime.Value()
	assert.NotEqual(t, 0, firstStartTimeStat)

	resp = rt.SendAdminRequest("POST", "/db/_compact", "")
	RequireStatus(t, resp, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/db/_compact", "")
		RequireStatus(t, resp, http.StatusOK)

		err = base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
		assert.NoError(t, err)

		return tombstoneCompactionStatus.State == db.BackgroundProcessStateCompleted
	})
	assert.NoError(t, err)
	assert.True(t, rt.GetDatabase().DbStats.Database().CompactionAttachmentStartTime.Value() > firstStartTimeStat)

	resp = rt.SendAdminRequest("GET", "/db/_compact", "")
	RequireStatus(t, resp, http.StatusOK)
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

// CBG-2143: Make sure the REST API is returning forbidden errors if when unsupported config option is set
func TestForceAPIForbiddenErrors(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyHTTP)
	testCases := []struct {
		forceForbiddenErrors bool
	}{
		{
			forceForbiddenErrors: true,
		},
		{
			forceForbiddenErrors: false,
		},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("Forbidden errors %v", test.forceForbiddenErrors), func(t *testing.T) {
			// assertRespStatus changes behaviour depending on if forcing forbidden errors
			assertRespStatus := func(resp *TestResponse, statusIfForbiddenErrorsFalse int) {
				if test.forceForbiddenErrors {
					assertHTTPErrorReason(t, resp, http.StatusForbidden, "forbidden")
					return
				}
				AssertStatus(t, resp, statusIfForbiddenErrorsFalse)
			}

			rt := NewRestTester(t, &RestTesterConfig{
				SyncFn: `
				function(doc, oldDoc) {
					if (!doc.doNotSync) {
						access("NoPerms", "chan2");
						access("Perms", "chan2");
						requireAccess("chan");
						channel(doc.channels);
					}
				}`,
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					Unsupported: &db.UnsupportedOptions{
						ForceAPIForbiddenErrors: test.forceForbiddenErrors,
					},
					Guest: &auth.PrincipalConfig{
						Disabled: base.BoolPtr(false),
					},
					Users: map[string]*auth.PrincipalConfig{
						"NoPerms": {
							Password: base.StringPtr("password"),
						},
						"Perms": {
							ExplicitChannels: base.SetOf("chan"),
							Password:         base.StringPtr("password"),
						},
					},
				}},
			})
			defer rt.Close()

			// Create the initial document
			resp := rt.SendAdminRequest(http.MethodPut, "/db/doc", `{"doNotSync": true, "foo": "bar", "channels": "chan", "_attachment":{"attach": {"data": "`+base64.StdEncoding.EncodeToString([]byte("attachmentA"))+`"}}}`)
			RequireStatus(t, resp, http.StatusCreated)
			rev := RespRevID(t, resp)

			// GET requests
			// User has no permissions to access document
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// Guest has no permissions to access document
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc", "", nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)

			// User has no permissions to access rev
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc?rev="+rev, "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusOK)

			// Guest has no permissions to access rev
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc?rev="+rev, "", nil, "", "")
			assertRespStatus(resp, http.StatusOK)

			// Attachments should be forbidden as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// Attachment revs should be forbidden as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach?rev="+rev, "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusNotFound)

			// Attachments should be forbidden for guests as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach", "", nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)

			// Attachment revs should be forbidden for guests as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach?rev="+rev, "", nil, "", "")
			assertRespStatus(resp, http.StatusNotFound)

			// Document does not exist should cause 403
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/notfound", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusNotFound)

			// Document does not exist for guest should cause 403
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/notfound", "", nil, "", "")
			assertRespStatus(resp, http.StatusNotFound)

			// PUT requests
			// PUT doc with user with no write perms
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc", `{}`, nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// PUT with rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev="+rev, `{}`, nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// PUT with incorrect rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev=1-abc", `{}`, nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// PUT request as Guest
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc", `{}`, nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// PUT with rev as Guest
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev="+rev, `{}`, nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)

			// PUT with incorrect rev as Guest
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev=1-abc", `{}`, nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// PUT with access but no rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc", `{}`, nil, "Perms", "password")
			assertHTTPErrorReason(t, resp, http.StatusConflict, "Document exists")

			// PUT with access but wrong rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev=1-abc", `{}`, nil, "Perms", "password")
			assertHTTPErrorReason(t, resp, http.StatusConflict, "Document revision conflict")

			// Confirm no access grants where granted
			resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/NoPerms", ``)
			RequireStatus(t, resp, http.StatusOK)
			var allChannels struct {
				Channels []string `json:"all_channels"`
			}
			err := json.Unmarshal(resp.BodyBytes(), &allChannels)
			require.NoError(t, err)
			assert.NotContains(t, allChannels.Channels, "chan2")

			resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/Perms", ``)
			RequireStatus(t, resp, http.StatusOK)
			err = json.Unmarshal(resp.BodyBytes(), &allChannels)
			require.NoError(t, err)
			assert.NotContains(t, allChannels.Channels, "chan2")

			// Successful PUT which will grant access grants
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev="+rev, `{"channels": "chan"}`, nil, "Perms", "password")
			AssertStatus(t, resp, http.StatusCreated)

			// Make sure channel access grant was successful
			resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/Perms", ``)
			RequireStatus(t, resp, http.StatusOK)
			err = json.Unmarshal(resp.BodyBytes(), &allChannels)
			require.NoError(t, err)
			assert.Contains(t, allChannels.Channels, "chan2")

			// DELETE requests
			// Attempt to delete document with no permissions
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document rev with no permissions
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev="+rev, "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document with wrong rev
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev=1-abc", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document document that does not exist
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/notfound", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// Attempt to delete document with no permissions as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc", "", nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document rev with no write perms as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev="+rev, "", nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document with wrong rev as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev=1-abc", "", nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document that does not exist as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/notfound", "", nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)
		})
	}
}

func TestSyncFnTimeout(t *testing.T) {
	syncFn := `function(doc) { while(true) {} }`

	rtConfig := RestTesterConfig{SyncFn: syncFn, DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{JavascriptTimeoutSecs: base.Uint32Ptr(1)}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	syncFnFinishedWG := sync.WaitGroup{}
	syncFnFinishedWG.Add(1)
	go func() {
		response := rt.SendAdminRequest("PUT", "/db/doc", `{"foo": "bar"}`)
		assertHTTPErrorReason(t, response, 500, "JS sync function timed out")
		syncFnFinishedWG.Done()
	}()
	timeoutErr := WaitWithTimeout(&syncFnFinishedWG, time.Second*15)
	assert.NoError(t, timeoutErr)
}

func assertHTTPErrorReason(t testing.TB, response *TestResponse, expectedStatus int, expectedReason string) {
	var httpError struct {
		Reason string `json:"reason"`
	}
	err := base.JSONUnmarshal(response.BodyBytes(), &httpError)
	require.NoError(t, err, "Failed to unmarshal HTTP error: %v", response.BodyBytes())

	AssertStatus(t, response, expectedStatus)

	assert.Equal(t, expectedReason, httpError.Reason)
}

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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
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

func TestPublicRESTStatCount(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// create a user to authenticate as for public api calls and assert the stat hasn't incremented as a result
	rt.CreateUser("greg", []string{"ABC"})
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 0)

	// use public api to put a doc through SGW then assert the stat has increased by 1
	resp := rt.SendUserRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"foo":"bar", "channels":["ABC"]}`, "greg")
	RequireStatus(t, resp, http.StatusCreated)
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 1)

	// send admin request assert that the public rest count doesn't increase
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 1)

	// send another public request to assert the stat increases by 1
	resp = rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/doc1", "", "greg")
	RequireStatus(t, resp, http.StatusOK)
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 2)

	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/_blipsync", "", "greg")
	RequireStatus(t, resp, http.StatusUpgradeRequired)

	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 2)

	srv := httptest.NewServer(rt.TestMetricsHandler())
	defer srv.Close()

	// test metrics endpoint
	response, err := http.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	// assert the stat doesn't increment
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 2)

	// test public endpoint but one that doesn't access a db
	resp = rt.SendUserRequest(http.MethodGet, "/", "", "greg")
	RequireStatus(t, resp, http.StatusOK)
	// assert the stat doesn't increment
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.NumPublicRestRequests.Value()
	}, 2)
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

	response := rt.SendRequest(http.MethodGet, "/{{.db}}/", "")
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	response = rt.SendRequest(http.MethodGet, "/notadb/", "")
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	// Double-check that even if we provide valid credentials we still won't be let in
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.NewUser("user1", "letmein", channels.BaseSetOf(t, "foo"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	response = rt.SendUserRequest(http.MethodGet, "/{{.db}}/", "", "user1")
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	response = rt.SendUserRequest(http.MethodGet, "/notadb/", "", "user1")
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)
	assert.NotContains(t, response.Header(), "WWW-Authenticate", "expected to not receive a WWW-Auth header when password auth is disabled")

	// Also check that we can't create a session through POST /db/_session
	response = rt.SendRequest(http.MethodPost, "/{{.db}}/_session", `{"name":"user1","password":"letmein"}`)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendRequest(http.MethodPost, "/notadb/_session", `{"name":"user1","password":"letmein"}`)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	// As a sanity check, ensure it does work when the setting is disabled
	rt.ServerContext().Database(ctx, "db").Options.DisablePasswordAuthentication = false
	response = rt.SendUserRequest(http.MethodGet, "/{{.db}}/", "", "user1")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequest(http.MethodPost, "/{{.db}}/_session", `{"name":"user1","password":"letmein"}`)
	RequireStatus(t, response, http.StatusOK)
}

func TestDocLifecycle(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	version := rt.CreateTestDoc("doc")
	assert.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", version.RevID)

	response := rt.SendAdminRequest("DELETE", "/{{.keyspace}}/doc?rev="+version.RevID, "")
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
	require.NoError(t, err)
	assert.NoError(t, a.Save(user))

	// Create document
	response := rt.SendUserRequest("PUT", "/{{.keyspace}}/doc", `{"prop":true, "channels":["foo"]}`, "user1")
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	// Put new revision with null body
	response = rt.SendUserRequest("PUT", "/{{.keyspace}}/doc?rev="+revid, "", "user1")
	RequireStatus(t, response, 400)
}

func TestFunkyDocIDs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateTestDoc("AC%2FDC")

	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/AC%2FDC", "")
	RequireStatus(t, response, 200)

	rt.CreateTestDoc("AC+DC")
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/AC+DC", "")
	RequireStatus(t, response, 200)

	rt.CreateTestDoc("AC+DC+GC")
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/AC+DC+GC", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/foo+bar+moo+car", `{"prop":true}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/foo+bar+moo+car", "")
	RequireStatus(t, response, 200)

	rt.CreateTestDoc("AC%2BDC2")
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/AC%2BDC2", "")
	RequireStatus(t, response, 200)

	rt.CreateTestDoc("AC%2BDC%2BGC2")
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/AC%2BDC%2BGC2", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/foo%2Bbar%2Bmoo%2Bcar2", `{"prop":true}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/foo%2Bbar%2Bmoo%2Bcar2", "")
	RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/foo%2Bbar+moo%2Bcar3", `{"prop":true}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/foo+bar%2Bmoo+car3", "")
	RequireStatus(t, response, 200)
}

func TestCORSOrigin(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	tests := []struct {
		origin       string
		headerOutput string
	}{
		{
			origin:       "http://example.com",
			headerOutput: "http://example.com",
		},
		{
			origin:       "http://staging.example.com",
			headerOutput: "http://staging.example.com",
		},

		{
			origin:       "http://hack0r.com",
			headerOutput: "*",
		},
	}

	for _, tc := range tests {
		t.Run(tc.origin, func(t *testing.T) {

			invalidDatabaseName := "invalid database name"
			reqHeaders := map[string]string{
				"Origin": tc.origin,
			}
			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				response := rt.SendRequestWithHeaders(method, "/{{.keyspace}}/", "", reqHeaders)
				assert.Equal(t, tc.headerOutput, response.Header().Get("Access-Control-Allow-Origin"))
				if method == http.MethodGet {
					if base.TestsUseNamedCollections() {
						RequireStatus(t, response, http.StatusBadRequest)
						require.Contains(t, response.Body.String(), invalidDatabaseName)
					} else { // CBG-2978, should not be different from GSI/collections
						RequireStatus(t, response, http.StatusUnauthorized)
					}
				} else {
					RequireStatus(t, response, http.StatusNoContent)

				}
			}
			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
				assert.Equal(t, tc.headerOutput, response.Header().Get("Access-Control-Allow-Origin"))
				if method == http.MethodGet {
					RequireStatus(t, response, http.StatusUnauthorized)
					require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
				} else {
					RequireStatus(t, response, http.StatusNoContent)

				}
			}
			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
				assert.Equal(t, tc.headerOutput, response.Header().Get("Access-Control-Allow-Origin"))
				if method == http.MethodGet {
					RequireStatus(t, response, http.StatusUnauthorized)
					require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
				} else {
					RequireStatus(t, response, http.StatusNoContent)

				}
			}

			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				// admin port doesn't have CORS
				response := rt.SendAdminRequestWithHeaders(method, "/{{.keyspace}}/_all_docs", "", reqHeaders)
				assert.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
				if method == http.MethodGet {
					RequireStatus(t, response, http.StatusOK)
				} else {
					RequireStatus(t, response, http.StatusNoContent)
				}
			}
			// test with a config without * should reject non-matches
			sc := rt.ServerContext()
			defer func() {
				sc.Config.API.CORS.Origin = defaultTestingCORSOrigin
			}()

			sc.Config.API.CORS.Origin = []string{"http://example.com", "http://staging.example.com"}
			if !base.StringSliceContains(sc.Config.API.CORS.Origin, tc.origin) {
				for _, method := range []string{http.MethodGet, http.MethodOptions} {
					response := rt.SendRequestWithHeaders(method, "/{{.keyspace}}/", "", reqHeaders)
					assert.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
				}
			}
		})
	}
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
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 201)

	var docs []interface{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Len(t, docs, 3)
	assert.Equal(t, map[string]interface{}{"rev": "1-50133ddd8e49efad34ad9ecae4cb9907", "id": "bulk1"}, docs[0])

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/bulk1", "")
	RequireStatus(t, response, 200)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "bulk1", respBody[db.BodyId])
	assert.Equal(t, "1-50133ddd8e49efad34ad9ecae4cb9907", respBody[db.BodyRev])
	assert.Equal(t, float64(1), respBody["n"])
	assert.Equal(t, map[string]interface{}{"rev": "1-035168c88bd4b80fb098a8da72f881ce", "id": "bulk2"}, docs[1])
	assert.Equal(t, map[string]interface{}{"rev": "0-1", "id": "_local/bulk3"}, docs[2])

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/bulk3", "")
	RequireStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/bulk3", respBody[db.BodyId])
	assert.Equal(t, "0-1", respBody[db.BodyRev])
	assert.Equal(t, float64(3), respBody["n"])

	// update all documents
	input = `{"docs": [{"_id": "bulk1", "_rev" : "1-50133ddd8e49efad34ad9ecae4cb9907", "n": 10}, {"_id": "bulk2", "_rev":"1-035168c88bd4b80fb098a8da72f881ce", "n": 20}, {"_id": "_local/bulk3","_rev":"0-1","n": 30}]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Len(t, docs, 3)
	assert.Equal(t, map[string]interface{}{"rev": "2-7e384b16e63ee3218349ee568f156d6f", "id": "bulk1"}, docs[0])

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/bulk3", "")
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
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
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
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 400)
}

func TestBulkDocsMalformedDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs":["A","B"]}`
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 400)

	// For non-string id, ensure it reverts to id generation and doesn't panic
	input = `{"docs": [{"_id": 3, "n": 1}]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
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
			"/{{.keyspace}}/"+docKeyPrefix+strconv.Itoa(i), doc)
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
	resp := rt.SendAdminRequestWithHeaders(http.MethodPost, "/{{.keyspace}}/_bulk_get", bulkGetBody, bulkGetHeaders)
	RequireStatus(t, resp, http.StatusOK)

	uncompressedBodyLen := resp.Body.Len()
	assert.Truef(t, uncompressedBodyLen >= minUncompressedSize, "Expected uncompressed response to be larger than minUncompressedSize (%d bytes) - got %d bytes", minUncompressedSize, uncompressedBodyLen)

	// try the request again, but accept gzip encoding
	bulkGetHeaders["Accept-Encoding"] = "gzip"
	resp = rt.SendAdminRequestWithHeaders(http.MethodPost, "/{{.keyspace}}/_bulk_get", bulkGetBody, bulkGetHeaders)
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
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_get", input)
	RequireStatus(t, response, 400)
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
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
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
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
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
	response := rt.SendRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-xyz"],
              "rd2": ["34-def"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
	response = rt.SendRequest("POST", "/{{.keyspace}}/_revs_diff", input)
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
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 201)

	reqHeaders := map[string]string{
		"Accept": "application/json",
	}
	response = rt.SendAdminRequestWithHeaders("GET", `/{{.keyspace}}/or1?open_revs=["12-abc","10-ten"]`, "", reqHeaders)
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

		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%v", k), fmt.Sprintf(`{"val":"1-%s"}`, k))
		RequireStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev := body["rev"].(string)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%v?rev=%s", k, rev), fmt.Sprintf(`{"val":"2-%s"}`, k))
		RequireStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev = body["rev"].(string)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%v?rev=%s", k, rev), fmt.Sprintf(`{"val":"3-%s"}`, k))
		RequireStatus(t, response, 201)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		rev = body["rev"].(string)

		docs[k] = rev
	}

	reqRevsLimit := 2

	// Fetch docs by bulk with varying revs_limits
	bulkGetResponse := rt.SendAdminRequest("POST", fmt.Sprintf("/{{.keyspace}}/_bulk_get?revs=true&revs_limit=%v", reqRevsLimit), fmt.Sprintf(`
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

	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 404)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local/loc1", `{"hi": "there"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 200)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-1", respBody[db.BodyRev])
	assert.Equal(t, "there", respBody["hi"])

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local/loc1", `{"hi": "there"}`)
	RequireStatus(t, response, 409)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local/loc1", `{"hi": "again", "_rev": "0-1"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-2", respBody[db.BodyRev])
	assert.Equal(t, "again", respBody["hi"])

	// Check the handling of large integers, which caused trouble for us at one point:
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local/loc1", `{"big": 123456789, "_rev": "0-2"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 200)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.Equal(t, "_local/loc1", respBody[db.BodyId])
	assert.Equal(t, "0-3", respBody[db.BodyRev])
	assert.Equal(t, float64(123456789), respBody["big"])

	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 409)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/_local/loc1?rev=0-3", "")
	RequireStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 404)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 404)

	// Check the handling of URL encoded slash at end of _local%2Fdoc
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local%2Floc12", `{"hi": "there"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc2", "")
	RequireStatus(t, response, 404)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/_local%2floc2", "")
	RequireStatus(t, response, 404)
}

func TestLocalDocExpiry(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	timeNow := uint32(time.Now().Unix())
	oneMoreHour := uint32(time.Now().Add(time.Hour).Unix())

	// set localDocExpiry to 30m
	rt.GetDatabase().Options.LocalDocExpirySecs = uint32(60 * 30)
	log.Printf("Expiry expected between %d and %d", timeNow, oneMoreHour)

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local/loc1", `{"hi": "there"}`)
	RequireStatus(t, response, 201)

	localDocKey := db.RealSpecialDocID(db.DocTypeLocal, "loc1")
	dataStore := rt.GetSingleDataStore()
	expiry, getMetaError := dataStore.GetExpiry(rt.Context(), localDocKey)
	log.Printf("Expiry after PUT is %v", expiry)
	assert.True(t, expiry > timeNow, "expiry is not greater than current time")
	assert.True(t, expiry < oneMoreHour, "expiry is not greater than current time")
	assert.NoError(t, getMetaError)

	// Retrieve local doc, ensure non-zero expiry is preserved
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/loc1", "")
	RequireStatus(t, response, 200)
	expiry, getMetaError = dataStore.GetExpiry(rt.Context(), localDocKey)
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

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/_local/loc1", docJSON)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequestWithHeaders("GET", "/{{.keyspace}}/_local/loc1", "",
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
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo":"bar", "channels":["ch1"]}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	doc1RevID := body["rev"].(string)

	// Run GET _all_docs as admin with channels=true:
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_all_docs?channels=true", "")
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Run POST _all_docs as admin with explicit docIDs and channels=true:
	keys := `{"keys": ["doc1"]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_all_docs?channels=true", keys)
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Commit rev 2 that maps to a differenet channel
	str := fmt.Sprintf(`{"foo":"bar", "channels":["ch2"], "_rev":%q}`, doc1RevID)
	RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", str), 201)

	// Run GET _all_docs as admin with channels=true
	// Make sure that only the new channel appears in the docs channel list
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_all_docs?channels=true", "")
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
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_all_docs?channels=true", keys)
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
	defer func() { rosmar.MaxDocSize = 0 }()

	rosmar.MaxDocSize = 20 * 1024 * 1024

	docBody := `{"value":"` + base64.StdEncoding.EncodeToString(make([]byte, 22000000)) + `"}`

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", docBody)
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
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
	require.NoError(t, err)
	assert.NoError(t, a.Save(frank))

	// Create a doc:
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/testOldDocId", `{"foo":"bar"}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	alphaRevID := body["rev"].(string)

	// Update a document to validate oldDoc id handling.  Will reject if old doc id not available
	str := fmt.Sprintf(`{"foo":"ball", "_rev":%q}`, alphaRevID)
	RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/testOldDocId", str), 201)

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
	err := DecodeAndSanitiseConfig(base.TestCtx(t), buf, &dbConfig, true)
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
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":[]}`)
	RequireStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId := body["rev"]

	// Update 10 times
	for i := 0; i < 20; i++ {
		str := fmt.Sprintf(`{"_rev":%q}`, revId)
		response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", str)
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		revId = body["rev"]
	}

	// Get latest rev id
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
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
				bulkGetResponse := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_get?revs=true&revs_limit=2", bulkGetDocs)
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
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/expNumericTTL", `{"_exp":100}`)
	RequireStatus(t, response, 201)

	// Validate that exp isn't returned on the standard GET, bulk get
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expNumericTTL", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok := body["_exp"]
	assert.Equal(t, false, ok)

	bulkGetDocs := `{"docs": [{"id": "expNumericTTL", "rev": "1-ca9ad22802b66f662ff171f226211d5c"}]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_get", bulkGetDocs)
	RequireStatus(t, response, 200)
	responseString := string(response.Body.Bytes())
	assert.True(t, !strings.Contains(responseString, "_exp"), "Bulk get response contains _exp property when show_exp not set.")

	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_get?show_exp=true", bulkGetDocs)
	RequireStatus(t, response, 200)
	responseString = string(response.Body.Bytes())
	assert.True(t, strings.Contains(responseString, "_exp"), "Bulk get response doesn't contain _exp property when show_exp was set.")

	body = nil
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expNumericTTL?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	// Validate other exp formats
	body = nil
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/expNumericUnix", `{"val":1, "_exp":4260211200}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expNumericUnix?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("numeric unix response: %s", response.Body.Bytes())
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/expNumericString", `{"val":1, "_exp":"100"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expNumericString?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/expBadString", `{"_exp":"abc"}`)
	RequireStatus(t, response, 400)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expBadString?show_exp=true", "")
	RequireStatus(t, response, 404)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, false, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/expDateString", `{"_exp":"2105-01-01T00:00:00.000+00:00"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expDateString?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	_, ok = body["_exp"]
	assert.Equal(t, true, ok)

	body = nil
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/expBadDateString", `{"_exp":"2105-0321-01T00:00:00.000+00:00"}`)
	RequireStatus(t, response, 400)
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expBadDateString?show_exp=true", "")
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
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/expNumericTTL", `{"expiry":100}`)
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/expNumericTTL?show_exp=true", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	value, ok := body["_exp"]
	assert.Equal(t, true, ok)
	log.Printf("value: %v", value)
}

// Repro attempt for SG #3307.  Before fix for #3307, fails when SG_TEST_USE_XATTRS=true and run against an actual couchbase server
func TestWriteTombstonedDocUsingXattrs(t *testing.T) {

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

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", bulkDocsBody)
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
	var retrievedVal map[string]interface{}
	var retrievedSyncData db.SyncData
	_, err = rt.GetSingleDataStore().GetWithXattr(rt.Context(), "-21SK00U-ujxUO9fU2HezxL", base.SyncXattrName, "", &retrievedVal, &retrievedSyncData, nil)
	assert.NoError(t, err, "Unexpected Error")
	assert.Equal(t, "2-466a1fab90a810dc0a63565b70680e4e", retrievedSyncData.CurrentRev)

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
	err = db.RestartListener(base.TestCtx(t))
	assert.True(t, err == nil)
	// Put a document to increment the counter for the * channel
	response := rt.Send(Request("PUT", "/{{.keyspace}}/lost", `{"channel":["ABC"]}`))
	RequireStatus(t, response, 201)

	// Previous bug: changeWaiter was treating the implicit '*' wildcard in the _changes request as the '*' channel, so the wait counter
	// was being initialized to 1 (the previous PUT).  Later the wildcard was resolved to actual channels (PBS, _sync:user:bernard), which
	// has a wait counter of zero (no documents writted since the listener was restarted).
	wg := sync.WaitGroup{}
	// start changes request
	wg.Add(1)
	go func() {
		defer wg.Done()
		changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
		changesResponse := rt.SendUserRequest("POST", "/{{.keyspace}}/_changes", changesJSON, "bernard")
		log.Printf("_changes looks like: %s", changesResponse.Body.Bytes())
		err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
		// Checkthat the changes loop isn't returning an empty result immediately (the previous bug) - should
		// be waiting until entry 'sherlock', created below, appears.
		assert.True(t, len(changes.Results) > 0)
	}()

	// Send a doc that will properly close the longpoll response
	time.Sleep(1 * time.Second)
	response = rt.Send(Request("PUT", "/{{.keyspace}}/sherlock", `{"channel":["PBS"]}`))
	RequireStatus(t, response, http.StatusOK)
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
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// Create User
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	jacques, err := a.NewUser("jacques", "letmein", channels.BaseSetOf(t, "A", "B"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(jacques))

	// Create Doc
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": ["A"]}`)
	assert.Equal(t, http.StatusCreated, response.Code)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevID := body["rev"].(string)

	// Delete Doc
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/doc1?rev="+docRevID, "")
	assert.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	docRevID2 := body["rev"].(string)

	// Update / Revive Doc
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev="+docRevID2, `{"channels": ["B"]}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	require.NoError(t, rt.WaitForPendingChanges())

	// Changes call
	response = rt.SendUserRequest(
		"GET", "/{{.keyspace}}/_changes", "", "jacques")
	assert.Equal(t, http.StatusOK, response.Code)

	var changesResponse = make(map[string]interface{})
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changesResponse))
	assert.NotContains(t, changesResponse["results"].([]interface{})[1], "deleted")
}

func TestChanCacheActiveRevsStat(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	responseBody := make(map[string]interface{})
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc", `{"value":"a value", "channels":["a"]}`)
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)
	rev1 := fmt.Sprint(responseBody["rev"])
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc2", `{"value":"a value", "channels":["a"]}`)
	err = base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)
	rev2 := fmt.Sprint(responseBody["rev"])
	RequireStatus(t, response, http.StatusCreated)

	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_changes?active_only=true&include_docs=true&filter=sync_gateway/bychannel&channels=a&feed=normal&since=0&heartbeat=0&timeout=300000", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc?new_edits=true&rev="+rev1, `{"value":"a value", "channels":[]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc2?new_edits=true&rev="+rev2, `{"value":"a value", "channels":[]}`)
	RequireStatus(t, response, http.StatusCreated)

	err = rt.WaitForPendingChanges()
	assert.NoError(t, err)

	assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Cache().ChannelCacheRevsActive.Value()))

}

func TestGetRawDocumentError(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/doc", ``)
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
	rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo": "bar"}`)

	if base.TestUseXattrs() {
		wg.Add(1)
		body := make(map[string]interface{})
		body["foo"] = "bar"
		added, err := rt.GetSingleDataStore().Add("doc2", 0, body)
		assert.True(t, added)
		assert.NoError(t, err)
	}

	require.NoError(t, WaitWithTimeout(&wg, 30*time.Second))
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
	res := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+t.Name(), `{"foo": "bar", "_deleted": true}`)
	RequireStatus(t, res, http.StatusCreated)
	wg.Wait()
}

func Benchmark_RestApiGetDocPerformance(b *testing.B) {

	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	prt := NewRestTester(b, nil)
	defer prt.Close()

	// Create test document
	prt.SendRequest("PUT", "/{{.keyspace}}/doc", `{"prop":true}`)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			prt.SendRequest("GET", "/{{.keyspace}}/doc", "")
		}
	})
}

var threekdoc = `{"cols":["Name","Address","Location","phone"],"data":[["MelyssaF.Stokes","Ap#166-9804ProinSt.","52.01352,-9.4151","(306)773-3853"],["RuthT.Richards","Ap#180-8417TemporRoad","8.07909,-118.55952","(662)733-8043"],["CedricN.Witt","Ap#575-4625NuncSt.","74.419,153.71285","(850)995-0417"],["ElianaF.Ashley","Ap#169-2030Nibh.St.","87.98632,97.47442","(903)272-5949"],["ChesterJ.Holland","2905ProinSt.","-43.14706,-64.25893","(911)435-9200"],["AleaT.Bishop","Ap#493-4894ConvallisRd.","42.54157,64.98534","(479)848-2988"],["HerrodT.Barron","Ap#822-1444EtAvenue","9.50706,-111.54064","(390)300-8534"],["YoshiP.Sanchez","Ap#796-4679Arcu.Avenue","-16.49557,-137.69","(913)606-8930"],["GrahamO.Velazquez","415EratRd.","-5.30634,171.81751","(691)700-3072"],["BryarF.Sargent","Ap#180-6507Lacus.St.","17.64959,-19.93008","(516)890-6469"],["XerxesM.Gaines","370-1967NislStreet","-39.28978,-23.74924","(461)907-9563"],["KayI.Jones","565-351ElitAve","25.58317,17.43545","(145)441-5007"],["ImaZ.Curry","Ap#143-8377MagnaAve","-86.72025,-161.94081","(484)924-8145"],["GiselleW.Macdonald","962AdipiscingRoad","-21.55826,-121.06657","(137)255-2241"],["TarikJ.Kane","P.O.Box447,5949PhasellusSt.","57.28914,-125.89595","(356)758-8271"],["ChristopherJ.Travis","5246InRd.","-69.12682,31.20181","(298)963-1855"],["QuinnT.Pace","P.O.Box935,212Laoreet,St.","-62.00241,1.31111","(157)419-0182"],["BrentK.Guy","156-417LoremSt.","26.67571,-29.35786","(125)687-6610"],["JocelynN.Cash","Ap#502-9209VehiculaSt.","-26.05925,160.61357","(782)351-4211"],["DaphneS.King","571-1485FringillaRoad","-76.33262,-142.5655","(356)476-4508"],["MicahJ.Eaton","3468ProinRd.","61.30187,-128.8584","(942)467-7558"],["ChaneyM.Gay","444-1647Pede.Rd.","84.32739,-43.59781","(386)231-0872"],["LacotaM.Guerra","9863NuncRoad","21.81253,-54.90423","(694)443-8520"],["KimberleyY.Jensen","6403PurusSt.","67.5704,65.90554","(181)309-7780"],["JenaY.Brennan","Ap#533-7088MalesuadaStreet","78.58624,-172.89351","(886)688-0617"],["CarterK.Dotson","Ap#828-1931IpsumAve","59.54845,53.30366","(203)546-8704"],["EllaU.Buckner","Ap#141-1401CrasSt.","78.34425,-172.24474","(214)243-6054"],["HamiltonE.Estrada","8676Iaculis,St.","11.67468,-130.12233","(913)624-2612"],["IanT.Saunders","P.O.Box519,3762DictumRd.","-10.97019,73.47059","(536)391-7018"],["CairoK.Craft","6619Sem.St.","9.28931,-5.69682","(476)804-7898"],["JohnB.Norman","Ap#865-7121CubiliaAve","50.96552,-126.5271","(309)323-6975"],["SawyerD.Hale","Ap#512-820EratRd.","-65.1931,166.14822","(180)527-8987"],["CiaranQ.Cole","P.O.Box262,9220SedAvenue","69.753,121.39921","(272)654-8755"],["BrandenJ.Thompson","Ap#846-5470MetusAv.","44.61386,-44.18375","(388)776-0689"]]}`

func Benchmark_RestApiPutDocPerformanceDefaultSyncFunc(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	prt := NewRestTester(b, nil)
	defer prt.Close()

	_ = prt.Bucket() // prep bucket for parallel access
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// PUT a new document until test run has completed
		for pb.Next() {
			docid, err := base.GenerateRandomID()
			require.NoError(b, err)
			prt.SendRequest("PUT", fmt.Sprintf("/{{.keyspace}}/doc-%v", docid), threekdoc)
		}
	})
}

func Benchmark_RestApiPutDocPerformanceExplicitSyncFunc(b *testing.B) {

	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	qrtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	qrt := NewRestTester(b, &qrtConfig)
	defer qrt.Close()

	_ = qrt.Bucket() // prep bucket for parallel access
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// PUT a new document until test run has completed
		for pb.Next() {
			docid, err := base.GenerateRandomID()
			require.NoError(b, err)
			qrt.SendRequest("PUT", fmt.Sprintf("/{{.keyspace}}/doc-%v", docid), threekdoc)
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
		rt.SendRequest("PUT", "/{{.keyspace}}/"+key, `{"prop":true}`)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			key := keys[rand.Intn(5000)]
			rt.SendRequest("GET", fmt.Sprintf("/{{.keyspace}}/%s?rev=1-45ca73d819d5b1c9b8eea95290e79004", key), "")
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

	response := rt.SendAdminRequest("DELETE", "/{{.keyspace}}/fake", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/fake", "")
	RequireStatus(t, response, http.StatusNotFound)

	var body map[string]interface{}
	_, err := rt.GetSingleDataStore().Get("fake", &body)

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
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", "{}")
	RequireStatus(t, response, http.StatusCreated)
	assert.NoError(t, json.Unmarshal(response.BodyBytes(), &body))
	rev := body["rev"].(string)

	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/doc1?rev="+rev, "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, response, http.StatusNotFound)

	var doc map[string]interface{}
	_, err := rt.GetSingleDataStore().Get("doc1", &doc)

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

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", "{}")
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`, string(response.BodyBytes()))

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc?rev=1-ca9ad22802b66f662ff171f226211d5c", `{"val": "newval"}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"2-2f981cadffde70e8a1d9dc386a410e0d","val":"newval"}`, string(response.BodyBytes()))
}

func TestTombstonedBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	RequireStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err := rt.GetSingleDataStore().Get(t.Name(), &body)

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
	_, ok := base.AsCouchbaseBucketStore(bucket)
	if !ok {
		t.Skip("Requires Couchbase bucket")
	}

	dataStore := rt.GetSingleDataStore()
	_, err := dataStore.Add(t.Name(), 0, map[string]interface{}{"val": "val"})
	require.NoError(t, err)

	resp := rt.SendAdminRequest("POST", "/{{.keyspace}}/_purge", `{"`+t.Name()+`": ["*"]}`)
	RequireStatus(t, resp, http.StatusOK)

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	RequireStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err = dataStore.Get(t.Name(), &body)

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
	_, ok := base.AsCouchbaseBucketStore(bucket)
	if !ok {
		t.Skip("Requires Couchbase bucket")
	}

	// Create the document to trigger cas failure
	value := make(map[string]interface{})
	value["foo"] = "bar"
	insCas, err := bucket.DefaultDataStore().WriteCas(t.Name(), 0, 0, 0, value, 0)
	require.NoError(t, err)

	// Delete document
	_, err = bucket.DefaultDataStore().Remove(t.Name(), insCas)
	require.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"new_edits": false, "docs": [{"_id":"`+t.Name()+`", "_deleted": true, "_revisions":{"start":9, "ids":["c45c049b7fe6cf64cd8595c1990f6504", "6e01ac52ffd5ce6a4f7f4024c08d296f"]}}]}`)
	RequireStatus(t, response, http.StatusCreated)

	var body map[string]interface{}
	_, err = rt.GetDatabase().Bucket.DefaultDataStore().Get(t.Name(), &body)

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

func TestDocumentChannelHistory(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	var body db.Body

	// Create doc in channel test and ensure a single channel history entry with only a start sequence
	// and no old channel history entries
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["test"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	err := json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 1)
	assert.Equal(t, syncData.ChannelSet[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 0})
	assert.Len(t, syncData.ChannelSetHistory, 0)

	// Update doc to remove from channel and ensure a single channel history entry with both start and end sequences
	// and no old channel history entries
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc?rev="+body["rev"].(string), `{"channels": []}`)
	RequireStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 1)
	assert.Equal(t, syncData.ChannelSet[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 2})
	assert.Len(t, syncData.ChannelSetHistory, 0)

	// Update doc to add to channels test and test2 and ensure a single channel history entry for both test and test2
	// both with start sequences only and ensure old test entry was moved to old
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc?rev="+body["rev"].(string), `{"channels": ["test", "test2"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSet, 2)
	assert.Contains(t, syncData.ChannelSet, db.ChannelSetEntry{Name: "test", Start: 3, End: 0})
	assert.Contains(t, syncData.ChannelSet, db.ChannelSetEntry{Name: "test2", Start: 3, End: 0})
	require.Len(t, syncData.ChannelSetHistory, 1)
	assert.Equal(t, syncData.ChannelSetHistory[0], db.ChannelSetEntry{Name: "test", Start: 1, End: 2})
}

func TestChannelHistoryLegacyDoc(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
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
	err := rt.GetSingleDataStore().Set("doc1", 0, nil, []byte(docData))
	assert.NoError(t, err)

	var body db.Body

	// Get doc and ensure its available
	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)

	// Remove doc from channel and ensure that channel history is built correctly with current end sequence and
	// setting start sequence
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev=1-08267a64bf0e3963bab7dece1ea0887a", `{"channels": []}`)
	RequireStatus(t, resp, http.StatusCreated)
	err = json.Unmarshal(resp.BodyBytes(), &body)
	assert.NoError(t, err)
	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc1")
	assert.NoError(t, err)
	require.Len(t, syncData.ChannelSet, 1)
	assert.Contains(t, syncData.ChannelSet, db.ChannelSetEntry{
		Name:  "test",
		Start: 1,
		End:   2,
	})
	assert.Len(t, syncData.ChannelSetHistory, 0)
}

type ChannelsTemp struct {
	Channels map[string][]string `json:"channels"`
}

func (rt *RestTester) CreateDocReturnRev(t *testing.T, docID string, revID string, bodyIn interface{}) string {
	bodyJSON, err := base.JSONMarshal(bodyIn)
	assert.NoError(t, err)

	url := "/{{.keyspace}}/" + docID
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
	tBucket := base.GetTestBucket(t)
	context, err := db.NewDatabaseContext(ctx, "db", tBucket, false, db.DatabaseContextOptions{Scopes: db.GetScopesOptions(t, tBucket, 1)})
	require.NoError(t, err)
	context.Close(context.AddDatabaseLogContext(ctx))

	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", "{}")

	srv := httptest.NewServer(rt.TestMetricsHandler())
	defer srv.Close()

	// Ensure metrics endpoint is accessible and that db database has entries
	resp, err := http.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyString, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(bodyString), `database="db"`)
	err = resp.Body.Close()
	assert.NoError(t, err)

	// Initialize another database to ensure both are registered successfully
	tBucket2 := base.GetTestBucket(t)
	context, err = db.NewDatabaseContext(ctx, "db2", tBucket2, false, db.DatabaseContextOptions{Scopes: db.GetScopesOptions(t, tBucket2, 1)})
	require.NoError(t, err)
	defer context.Close(context.AddDatabaseLogContext(ctx))

	// Validate that metrics still works with both db and db2 databases and that they have entries
	resp, err = http.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyString, err = io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(bodyString), `database="db"`)
	assert.Contains(t, string(bodyString), `database="db2"`)
	err = resp.Body.Close()
	assert.NoError(t, err)

	// Ensure metrics endpoint is not serving any other routes
	resp, err = http.Get(srv.URL + "/" + rt.GetSingleKeyspace() + "/")
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	err = resp.Body.Close()
	assert.NoError(t, err)
}

func TestDocChannelSetPruning(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	const docID = "doc"
	version := rt.PutDoc(docID, `{"channels": ["a"]}`)

	for i := 0; i < 10; i++ {
		version = rt.UpdateDoc(docID, version, `{"channels": []}`)
		version = rt.UpdateDoc(docID, version, `{"channels": ["a"]}`)
	}

	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSetHistory, db.DocumentHistoryMaxEntriesPerChannel)
	assert.Equal(t, "a", syncData.ChannelSetHistory[0].Name)
	assert.Equal(t, uint64(1), syncData.ChannelSetHistory[0].Start)
	assert.Equal(t, uint64(12), syncData.ChannelSetHistory[0].End)
}

func TestNullDocHandlingForMutable1xBody(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollectionWithUser()

	documentRev := db.DocumentRevision{DocID: "doc1", BodyBytes: []byte("null")}

	body, err := documentRev.Mutable1xBody(rt.Context(), collection, nil, nil, false)
	require.Error(t, err)
	require.Nil(t, body)
	assert.Contains(t, err.Error(), "null doc body for doc")
}

// TestDatabaseXattrConfigHandlingForDBConfigUpdate:
//   - Create database with xattrs enabled
//   - Test updating the config to disable the use of xattrs in this database through replacing + upserting the config
//   - Assert error code is returned and response contains error string
func TestDatabaseXattrConfigHandlingForDBConfigUpdate(t *testing.T) {
	base.LongRunningTest(t)
	const (
		dbName  = "db1"
		errResp = "sync gateway requires enable_shared_bucket_access=true"
	)

	testCases := []struct {
		name         string
		upsertConfig bool
	}{
		{
			name:         "POST update",
			upsertConfig: true,
		},
		{
			name:         "PUT update",
			upsertConfig: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				PersistentConfig: true,
			})
			defer rt.Close()

			dbConfig := rt.NewDbConfig()

			resp := rt.CreateDatabase(dbName, dbConfig)
			RequireStatus(t, resp, http.StatusCreated)
			assert.NoError(t, rt.WaitForDBOnline())

			dbConfig.EnableXattrs = base.BoolPtr(false)

			if testCase.upsertConfig {
				resp = rt.UpsertDbConfig(dbName, dbConfig)
				RequireStatus(t, resp, http.StatusInternalServerError)
				assert.Contains(t, resp.Body.String(), errResp)
			} else {
				resp = rt.ReplaceDbConfig(dbName, dbConfig)
				RequireStatus(t, resp, http.StatusInternalServerError)
				assert.Contains(t, resp.Body.String(), errResp)
			}
		})
	}
}

// TestCreateDBWithXattrsDisbaled:
//   - Test that you cannot create a database with xattrs disabled
//   - Assert error code is returned and response contains error string
func TestCreateDBWithXattrsDisbaled(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()
	const (
		dbName  = "db1"
		errResp = "sync gateway requires enable_shared_bucket_access=true"
	)

	dbConfig := rt.NewDbConfig()
	dbConfig.EnableXattrs = base.BoolPtr(false)

	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusInternalServerError)
	assert.Contains(t, resp.Body.String(), errResp)
}

// TestPutDocUpdateVersionVector:
//   - Put a doc and assert that the versions and the source for the hlv is correctly updated
//   - Update that doc and assert HLV has also been updated
//   - Delete the doc and assert that the HLV has been updated in deletion event
func TestPutDocUpdateVersionVector(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	bucketUUID, err := rt.GetDatabase().Bucket.UUID()
	require.NoError(t, err)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"key": "value"}`)
	RequireStatus(t, resp, http.StatusCreated)

	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc1")
	assert.NoError(t, err)
	uintCAS := base.HexCasToUint64(syncData.Cas)

	assert.Equal(t, bucketUUID, syncData.HLV.SourceID)
	assert.Equal(t, uintCAS, syncData.HLV.Version)
	assert.Equal(t, uintCAS, syncData.HLV.CurrentVersionCAS)

	// Put a new revision of this doc and assert that the version vector SourceID and Version is updated
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1?rev="+syncData.CurrentRev, `{"key1": "value1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc1")
	assert.NoError(t, err)
	uintCAS = base.HexCasToUint64(syncData.Cas)

	assert.Equal(t, bucketUUID, syncData.HLV.SourceID)
	assert.Equal(t, uintCAS, syncData.HLV.Version)
	assert.Equal(t, uintCAS, syncData.HLV.CurrentVersionCAS)

	// Delete doc and assert that the version vector SourceID and Version is updated
	resp = rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/doc1?rev="+syncData.CurrentRev, "")
	RequireStatus(t, resp, http.StatusOK)

	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc1")
	assert.NoError(t, err)
	uintCAS = base.HexCasToUint64(syncData.Cas)

	assert.Equal(t, bucketUUID, syncData.HLV.SourceID)
	assert.Equal(t, uintCAS, syncData.HLV.Version)
	assert.Equal(t, uintCAS, syncData.HLV.CurrentVersionCAS)
}

// TestHLVOnPutWithImportRejection:
//   - Put a doc successfully and assert the HLV is updated correctly
//   - Put a doc that will be rejected by the custom import filter
//   - Assert that the HLV values on the sync data are still correctly updated/preserved
func TestHLVOnPutWithImportRejection(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)
	importFilter := `function (doc) { return doc.type == "mobile"}`
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   false,
			ImportFilter: &importFilter,
		}},
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	bucketUUID, err := rt.GetDatabase().Bucket.UUID()
	require.NoError(t, err)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"type": "mobile"}`)
	RequireStatus(t, resp, http.StatusCreated)

	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc1")
	assert.NoError(t, err)
	uintCAS := base.HexCasToUint64(syncData.Cas)

	assert.Equal(t, bucketUUID, syncData.HLV.SourceID)
	assert.Equal(t, uintCAS, syncData.HLV.Version)
	assert.Equal(t, uintCAS, syncData.HLV.CurrentVersionCAS)

	// Put a doc that will be rejected by the import filter on the attempt to perform on demand import for write
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc2", `{"type": "not-mobile"}`)
	RequireStatus(t, resp, http.StatusCreated)

	// assert that the hlv is correctly updated and in tact after the import was cancelled on the doc
	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(base.TestCtx(t), "doc2")
	assert.NoError(t, err)
	uintCAS = base.HexCasToUint64(syncData.Cas)

	assert.Equal(t, bucketUUID, syncData.HLV.SourceID)
	assert.Equal(t, uintCAS, syncData.HLV.Version)
	assert.Equal(t, uintCAS, syncData.HLV.CurrentVersionCAS)
}

func TestTombstoneCompactionAPI(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	zero := time.Duration(0)
	rt.GetDatabase().Options.PurgeInterval = &zero

	for i := 0; i < 100; i++ {
		docID := fmt.Sprintf("doc%d", i)
		version := rt.PutDoc(docID, "{}")
		rt.DeleteDoc(docID, version)
	}

	resp := rt.SendAdminRequest("GET", "/{{.db}}/_compact", "")
	RequireStatus(t, resp, http.StatusOK)

	var tombstoneCompactionStatus db.TombstoneManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
	assert.NoError(t, err)

	assert.Equal(t, db.BackgroundProcessStateCompleted, tombstoneCompactionStatus.State)
	assert.Empty(t, tombstoneCompactionStatus.LastErrorMessage)
	assert.Equal(t, 0, int(tombstoneCompactionStatus.DocsPurged))
	firstStartTimeStat := rt.GetDatabase().DbStats.Database().CompactionAttachmentStartTime.Value()
	assert.NotEqual(t, 0, firstStartTimeStat)

	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact", "")
	RequireStatus(t, resp, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/{{.db}}/_compact", "")
		RequireStatus(t, resp, http.StatusOK)

		err = base.JSONUnmarshal(resp.BodyBytes(), &tombstoneCompactionStatus)
		assert.NoError(t, err)

		return tombstoneCompactionStatus.State == db.BackgroundProcessStateCompleted
	})
	assert.NoError(t, err)
	assert.True(t, rt.GetDatabase().DbStats.Database().CompactionAttachmentStartTime.Value() > firstStartTimeStat)

	resp = rt.SendAdminRequest("GET", "/{{.db}}/_compact", "")
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

func assertHTTPErrorReason(t testing.TB, response *TestResponse, expectedStatus int, expectedReason string) {
	var httpError struct {
		Reason string `json:"reason"`
	}
	err := base.JSONUnmarshal(response.BodyBytes(), &httpError)
	require.NoError(t, err, "Failed to unmarshal HTTP error: %v", response.BodyBytes())

	AssertStatus(t, response, expectedStatus)

	assert.Equal(t, expectedReason, httpError.Reason)
}

// TestPing ensures that /_ping is accessible on all APIs for GET and HEAD without authentication.
func TestPing(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyHTTPResp)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	tests := []struct {
		method         string
		expectedStatus int
		expectedBody   string
	}{
		{method: http.MethodGet, expectedStatus: http.StatusOK, expectedBody: "OK"},
		{method: http.MethodHead, expectedStatus: http.StatusOK, expectedBody: ""},
		{method: http.MethodPost, expectedStatus: http.StatusMethodNotAllowed},
		{method: http.MethodPut, expectedStatus: http.StatusMethodNotAllowed},
		{method: http.MethodPatch, expectedStatus: http.StatusMethodNotAllowed},
		{method: http.MethodDelete, expectedStatus: http.StatusMethodNotAllowed},
		{method: http.MethodConnect, expectedStatus: http.StatusMethodNotAllowed},
		{method: http.MethodOptions, expectedStatus: http.StatusNoContent},
		{method: http.MethodTrace, expectedStatus: http.StatusMethodNotAllowed},
	}

	testAPIs := []struct {
		apiName   string
		requestFn func(string, string, string) *TestResponse
	}{
		{"Public", rt.SendRequest},
		{"Admin", rt.SendAdminRequest},
		{"Metrics", rt.SendMetricsRequest},
	}

	for _, test := range tests {
		for _, api := range testAPIs {
			t.Run(fmt.Sprintf("%s %s", test.method, api.apiName), func(t *testing.T) {
				resp := api.requestFn(test.method, "/_ping", "")
				AssertStatus(t, resp, test.expectedStatus)
				if test.expectedStatus == http.StatusOK {
					assert.Containsf(t, resp.Header().Get("Content-Type"), "text/plain", "Should have text/plain Content-Type")
					assert.Equalf(t, "2", resp.Header().Get("Content-Length"), "Should have a Content-Length")
					assert.Equalf(t, test.expectedBody, string(resp.BodyBytes()), "Unexpected response body")
				}
			})
		}
	}
}

func TestAllDbs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodGet, "/_all_dbs", "")
	RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, fmt.Sprintf(`["%s"]`, rt.GetDatabase().Name), resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, "/_all_dbs?verbose=true", "")
	RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, fmt.Sprintf(`[{"db_name":"%s","bucket":"%s","state":"Online"}]`, rt.GetDatabase().Name, rt.GetDatabase().Bucket.GetName()), resp.Body.String())
}

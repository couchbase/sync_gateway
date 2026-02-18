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
	"runtime/pprof"
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
	"github.com/google/uuid"
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
	isAdmin, ok := body["ADMIN"].(bool)
	assert.False(t, ok)
	assert.False(t, isAdmin)

	response = rt.SendAdminRequest("GET", "/", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	isAdmin, ok = body["ADMIN"].(bool)
	assert.True(t, ok)
	assert.True(t, isAdmin)

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
	require.NoError(t, response.Body.Close())
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
				DisablePasswordAuth: base.Ptr(true),
				Guest: &auth.PrincipalConfig{
					Disabled: base.Ptr(true),
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
	assert.Equal(t, "1-45ca73d819d5b1c9b8eea95290e79004", version.RevTreeID)

	response := rt.SendAdminRequest("DELETE", "/{{.keyspace}}/doc?rev="+version.RevTreeID, "")
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
	tests := []struct {
		occKey string
	}{
		{occKey: db.BodyRev},
		{occKey: db.BodyCV},
	}

	for _, test := range tests {
		t.Run(test.occKey, func(t *testing.T) {
			rt := NewRestTester(t, nil)
			defer rt.Close()

			const bulk1DocID = "bulk1"
			const bulk2DocID = "bulk2"
			const bulk3LocalDocID = "_local/bulk3"

			// insert all

			input := fmt.Sprintf(
				`{"docs": [{%q:%q, %q:%d}, {%q:%q, %q:%d}, {%q:%q, %q:%d}]}`,
				db.BodyId, bulk1DocID, "n", 1,
				db.BodyId, bulk2DocID, "n", 2,
				db.BodyId, bulk3LocalDocID, "n", 3,
			)
			response := rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/_bulk_docs", input)
			RequireStatus(t, response, http.StatusCreated)

			// get persisted versions
			bulk1Version, _ := rt.GetDoc(bulk1DocID)
			bulk2Version, _ := rt.GetDoc(bulk2DocID)
			bulk3Version, _ := rt.GetDoc(bulk3LocalDocID)

			var docs []any
			require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
			assert.Equal(t, []any{
				map[string]any{"rev": bulk1Version.RevTreeID, "cv": bulk1Version.CV.String(), "id": bulk1DocID},
				map[string]any{"rev": bulk2Version.RevTreeID, "cv": bulk2Version.CV.String(), "id": bulk2DocID},
				map[string]any{"rev": bulk3Version.RevTreeID, "id": bulk3LocalDocID},
			}, docs)

			// check stored docs
			response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", bulk1DocID), "")
			RequireStatus(t, response, http.StatusOK)
			assert.JSONEq(t, fmt.Sprintf(`{%q:%q, %q:%q, %q:%q, %q:%d}`,
				db.BodyId, bulk1DocID,
				db.BodyRev, bulk1Version.RevTreeID,
				db.BodyCV, bulk1Version.CV,
				"n", 1,
			), response.BodyString())
			response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", bulk2DocID), "")
			RequireStatus(t, response, http.StatusOK)
			assert.JSONEq(t, fmt.Sprintf(`{%q:%q, %q:%q, %q:%q, %q:%d}`,
				db.BodyId, bulk2DocID,
				db.BodyRev, bulk2Version.RevTreeID,
				db.BodyCV, bulk2Version.CV,
				"n", 2,
			), response.BodyString())
			response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", bulk3LocalDocID), "")
			RequireStatus(t, response, http.StatusOK)
			assert.JSONEq(t, fmt.Sprintf(`{%q:%q, %q:%q, %q:%d}`,
				db.BodyId, bulk3LocalDocID,
				db.BodyRev, bulk3Version.RevTreeID,
				"n", 3,
			), response.BodyString())

			// Update all documents
			switch test.occKey {
			case db.BodyCV:
				input = fmt.Sprintf(
					`{"docs": [{%q:%q, %q:%q, %q:%d}, {%q:%q, %q:%q, %q:%d}, {%q:%q, %q:%q, %q:%d}]}`,
					db.BodyId, bulk1DocID, db.BodyCV, bulk1Version.CV, "n", 10,
					db.BodyId, bulk2DocID, db.BodyCV, bulk2Version.CV, "n", 20,
					db.BodyId, bulk3LocalDocID, db.BodyRev, bulk3Version.RevTreeID, "n", 30, //  local docs don't have a CV so we can only use the returned rev as the OCC value for update
				)
			case db.BodyRev:
				input = fmt.Sprintf(
					`{"docs": [{%q:%q, %q:%q, %q:%d}, {%q:%q, %q:%q, %q:%d}, {%q:%q, %q:%q, %q:%d}]}`,
					db.BodyId, bulk1DocID, db.BodyRev, bulk1Version.RevTreeID, "n", 10,
					db.BodyId, bulk2DocID, db.BodyRev, bulk2Version.RevTreeID, "n", 20,
					db.BodyId, bulk3LocalDocID, db.BodyRev, bulk3Version.RevTreeID, "n", 30,
				)
			default:
				t.Fatalf("Unexpected occKey: %q", test.occKey)
			}
			response = rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/_bulk_docs", input)
			RequireStatus(t, response, http.StatusCreated)

			// refresh versions
			bulk1Version, _ = rt.GetDoc(bulk1DocID)
			bulk2Version, _ = rt.GetDoc(bulk2DocID)
			bulk3Version, _ = rt.GetDoc(bulk3LocalDocID)

			docs = nil
			require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
			assert.Equal(t, []any{
				map[string]any{"rev": bulk1Version.RevTreeID, "cv": bulk1Version.CV.String(), "id": bulk1DocID},
				map[string]any{"rev": bulk2Version.RevTreeID, "cv": bulk2Version.CV.String(), "id": bulk2DocID},
				map[string]any{"rev": bulk3Version.RevTreeID, "id": bulk3LocalDocID},
			}, docs)

			// check for stored updates
			response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", bulk1DocID), "")
			RequireStatus(t, response, http.StatusOK)
			assert.JSONEq(t, fmt.Sprintf(`{%q:%q, %q:%q, %q:%q, %q:%d}`,
				db.BodyId, bulk1DocID,
				db.BodyRev, bulk1Version.RevTreeID,
				db.BodyCV, bulk1Version.CV,
				"n", 10,
			), response.BodyString())
			response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", bulk2DocID), "")
			RequireStatus(t, response, http.StatusOK)
			assert.JSONEq(t, fmt.Sprintf(`{%q:%q, %q:%q, %q:%q, %q:%d}`,
				db.BodyId, bulk2DocID,
				db.BodyRev, bulk2Version.RevTreeID,
				db.BodyCV, bulk2Version.CV,
				"n", 20,
			), response.BodyString())
			response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", bulk3LocalDocID), "")
			RequireStatus(t, response, http.StatusOK)
			assert.JSONEq(t, fmt.Sprintf(`{%q:%q, %q:%q, %q:%d}`,
				db.BodyId, bulk3LocalDocID,
				db.BodyRev, bulk3Version.RevTreeID,
				"n", 30,
			), response.BodyString())
		})
	}
}

func TestBulkDocsIDGeneration(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	input := `{"docs": [{"n": 1}, {"_id": 123, "n": 2}]}`
	response := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 201)
	var docs []map[string]string
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	RequireStatus(t, response, 201)
	assert.Len(t, docs, 2)
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
	dbConfigCopy.Sync = base.Ptr("")

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
	dbConfigCopy.Sync = base.Ptr("")

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
	dbConfigCopy.Sync = base.Ptr("")

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
		base.HTTPHeaderUserAgent: "CouchbaseLite/1.2",
		"Content-Type":           "application/json",
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

// TestBulkDocsLegacyRevNoop verifies that POSTing /_bulk_docs with an existing legacy RevTree ID is a no-op and that CV is not set on the response.
func TestBulkDocsLegacyRevNoop(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// create legacy rev (without CV)
	dbc, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	revId, _ := dbc.CreateDocNoHLV(t, ctx, "legacydoc", db.Body{"foo": "bar"})
	revIDGen, revIDDigest := db.ParseRevID(ctx, revId)

	// now POST it back to _bulk_docs - should be a no-op
	input := fmt.Sprintf(`{"new_edits":false,"docs": [{"_id": "legacydoc", "_rev": "%s", "_revisions":{"start": %d, "ids":["%s"]}, "foo": "bar"}]}`, revId, revIDGen, revIDDigest)
	response := rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, http.StatusCreated)
	var docs []any
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, []any{
		// no CV present since we didn't actually update the doc
		map[string]any{"rev": revId, "id": "legacydoc"},
	}, docs)
}

func TestCreateLegacyRevDoc(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// create legacy rev (without CV)
	dbc, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	_, _ = dbc.CreateDocNoHLV(t, ctx, "legacydoc", db.Body{"foo": "bar"})

	// fetch doc and assert that no _vv is present
	ds := dbc.GetCollectionDatastore()
	_, _, err := ds.GetXattrs(ctx, "legacydoc", []string{base.VvXattrName})
	require.Error(t, err) // should error as xattr not found
	base.RequireXattrNotFoundError(t, err)
}

// TestBulkDocsNoEdits verifies that POSTing /_bulk_docs with new_edits=false stores
// the provided revisions verbatim, and subsequent posts append the given history without generating new rev IDs.
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
	var docs []any
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	require.Len(t, docs, 2)
	first, ok := docs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "bdne1", first["id"])
	assert.Equal(t, "12-abc", first["rev"])

	second, ok := docs[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "bdne2", second["id"])
	assert.Equal(t, "34-def", second["rev"])

	// Now update the first doc with two new revisions:
	input = `{"new_edits":false, "docs": [
                  {"_id": "bdne1", "_rev": "14-jkl", "n": 111,
                   "_revisions": {"start": 14, "ids": ["jkl", "def", "abc", "eleven", "ten", "nine"]}}
            ]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, 201)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	require.Len(t, docs, 1)
	updated, ok := docs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "bdne1", updated["id"])
	assert.Equal(t, "14-jkl", updated["rev"])

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

	var allDocsResult allDocsResponse

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
	assert.Len(t, allDocsResult.Rows, 1)
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "ch1", allDocsResult.Rows[0].Value.Channels[0])

	// Run POST _all_docs as admin with explicit docIDs and channels=true:
	keys := `{"keys": ["doc1"]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_all_docs?channels=true", keys)
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Len(t, allDocsResult.Rows, 1)
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
	assert.Len(t, allDocsResult.Rows, 1)
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
	assert.Len(t, allDocsResult.Rows, 1)
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
	err := DecodeAndSanitiseStartupConfig(base.TestCtx(t), buf, &dbConfig, true)
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
	xattrs, _, err := rt.GetSingleDataStore().GetXattrs(rt.Context(), "-21SK00U-ujxUO9fU2HezxL", []string{base.SyncXattrName})
	require.NoError(t, err)
	require.Contains(t, xattrs, base.SyncXattrName)
	var retrievedSyncData db.SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedSyncData))
	assert.Equal(t, "2-466a1fab90a810dc0a63565b70680e4e", retrievedSyncData.GetRevTreeID())

}

// Reproduces https://github.com/couchbase/sync_gateway/issues/916.  The test-only RestartListener operation used to simulate a
// SG restart isn't race-safe, so disabling the test for now.  Should be possible to reinstate this as a proper unit test
// once we add the ability to take a bucket offline/online.
func TestLongpollWithWildcard(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

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
	db, err := rt.ServerContext().GetDatabase(ctx, "db")
	require.NoError(t, err)
	err = db.RestartListener(base.TestCtx(t))
	require.NoError(t, err)
	// Put a document to increment the counter for the * channel
	rt.PutDoc("lost", `{"channel":["ABC"]}`)

	// make sure docs are written to change cache
	rt.WaitForPendingChanges()

	// Previous bug: changeWaiter was treating the implicit '*' wildcard in the _changes request as the '*' channel, so the wait counter
	// was being initialized to 1 (the previous PUT).  Later the wildcard was resolved to actual channels (PBS, _sync:user:bernard), which
	// has a wait counter of zero (no documents writted since the listener was restarted).
	wg := sync.WaitGroup{}
	// start changes request
	wg.Add(1)
	go func() {
		defer wg.Done()
		changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
		changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
		// Checkthat the changes loop isn't returning an empty result immediately (the previous bug) - should
		// be waiting until entry 'sherlock', created below, appears.
		assert.Greater(t, len(changes.Results), 0)
	}()

	// Send a doc that will properly close the longpoll response
	time.Sleep(1 * time.Second)
	rt.PutDoc("sherlock", `{"channel":["PBS"]}`)
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

	rt.WaitForPendingChanges()

	// Changes call, one user, one doc
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "jacques")
	require.Len(t, changes.Results, 2)
	assert.Equal(t, changes.Results[1].Deleted, false)
}

var exampleDoc string = `{
  "_id": "jdoe.activity.%s",
  "type": "activity",
  "createdAt": "2025-06-15T08:00:00.000Z",
  "updatedAt": "2025-06-15T12:30:00.000Z",
  "owner": "jdoe",
  "name": "Morning Century Ride",
  "bikeId": "jdoe.bike_profile.bike001",
  "client": "karoo3",
  "activityType": "GRAVEL",
  "activeTime": 7200000,
  "duration": {
    "elapsedTime": 10800000,
    "startTime": "2025-06-15T08:00:00.000Z",
    "endTime": "2025-06-15T11:00:00.000Z"
  },
  "activityInfo": [
    {
      "key": "avgSpeed",
      "value": {
        "format": "double",
        "value": 28.5
      }
    },
    {
      "key": "maxSpeed",
      "value": {
        "format": "double",
        "value": 55.2
      }
    },
    {
      "key": "avgPower",
      "value": {
        "format": "int",
        "value": 210
      }
    },
    {
      "key": "avgCadence",
      "value": {
        "format": "float",
        "value": 85.3
      }
    },
    {
      "key": "totalDistance",
      "value": {
        "format": "long",
        "value": 100500
      }
    },
    {
      "key": "avgHr",
      "value": {
        "format": "int",
        "value": 145
      }
    },
    {
      "key": "maxHr",
      "value": {
        "format": "int",
        "value": 178
      }
    },
    {
      "key": "totalElevGain",
      "value": {
        "format": "float",
        "value": 1250.5
      }
    }
  ],
  "polyline": "m~beFnzvuOlAkB~@{AdAcBnBeDhCqE",
  "laps": [
    {
      "lapNumber": 1,
      "activeTime": 3600000,
      "duration": {
        "elapsedTime": 5400000,
        "startTime": "2025-06-15T08:00:00.000Z",
        "endTime": "2025-06-15T09:30:00.000Z"
      },
      "pauses": [
        {
          "elapsedTime": 1800000,
          "startTime": "2025-06-15T08:45:00.000Z",
          "endTime": "2025-06-15T09:15:00.000Z"
        }
      ],
      "lapInfo": [
        {
          "key": "avgSpeed",
          "value": {
            "format": "double",
            "value": 30.1
          }
        },
        {
          "key": "avgPower",
          "value": {
            "format": "int",
            "value": 220
          }
        }
      ],
      "trigger": "DISTANCE",
      "workout": {
        "primaryTarget": {
          "type": "power",
          "value": 200,
          "output": 215,
          "minValue": 180,
          "maxValue": 220,
          "rampType": "linear"
        },
        "secondaryTarget": {
          "type": "cadence",
          "value": 90,
          "output": 87,
          "minValue": 80,
          "maxValue": 100
        }
      }
    },
    {
      "lapNumber": 2,
      "activeTime": 3600000,
      "duration": {
        "elapsedTime": 5400000,
        "startTime": "2025-06-15T09:30:00.000Z",
        "endTime": "2025-06-15T11:00:00.000Z"
      },
      "pauses": [
        {
          "elapsedTime": 900000,
          "startTime": "2025-06-15T10:00:00.000Z",
          "endTime": "2025-06-15T10:15:00.000Z"
        },
        {
          "elapsedTime": 900000,
          "startTime": "2025-06-15T10:30:00.000Z",
          "endTime": "2025-06-15T10:45:00.000Z"
        }
      ],
      "lapInfo": [
        {
          "key": "avgSpeed",
          "value": {
            "format": "double",
            "value": 26.8
          }
        }
      ],
      "trigger": "MANUAL"
    }
  ],
  "sync": {
    "description": "Morning gravel ride through the hills",
    "type": "ride",
    "tags": [
      "gravel",
      "century",
      "training"
    ],
    "synced": true,
    "partners": [
      {
        "partner": "strava",
        "needsUpload": false,
        "attempts": 1,
        "uploadedAt": "2025-06-15T12:05:00.000Z",
        "externalId": "strava_activity_12345",
        "externalUrl": "https://www.strava.com/activities/12345"
      },
      {
        "partner": "trainingpeaks",
        "needsUpload": true,
        "attempts": 0,
        "error": "authentication_expired"
      }
    ]
  },
  "pointsOfInterest": [
    {
      "type": "summit",
      "location": {
        "lat": 40.7128,
        "lng": -74.006
      },
      "name": "Hilltop View",
      "description": "Great panoramic view from the top",
      "bearing": 180.5,
      "updatedAt": "2025-06-15T09:45:00.000Z",
      "sourceId": "poi-summit-001"
    },
    {
      "type": "water",
      "location": {
        "lat": 40.75,
        "lng": -73.98
      },
      "name": "Refill Station"
    }
  ],
  "climbs": [
    {
      "startDistance": 15200,
      "endDistance": 18500,
      "distance": 3300
    },
    {
      "startDistance": 45000,
      "endDistance": 48200,
      "distance": 3200
    }
  ]
}`

func TestWrite(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn:     syncFunc,
		AutoImport: base.Ptr(true),
	})
	defer rt.Close()

	var hugeAttachment = base.FastRandBytes(t, 500000)

	version := rt.PutDoc("doc", fmt.Sprintf(tesDoc, 1))

	rt.storeAttachment("doc", version, "myAtt", string(hugeAttachment))
	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/doc/myAtt", "")
	RequireStatus(rt.TB(), resp, http.StatusOK)

}

func TestWriteExampleDoc(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn:     syncFunc,
		AutoImport: base.Ptr(true),
	})
	defer rt.Close()

	postFix := uuid.NewString()

	rt.PutDoc("jdoe.activity."+postFix, fmt.Sprintf(exampleDoc, postFix))

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/jdoe.activity."+postFix, "")
	RequireStatus(rt.TB(), resp, http.StatusOK)

	fmt.Println(resp.Body.String())
}

var syncFunc string = `function synctos(doc, oldDoc) {
  // Whether the given value is either null or undefined
  function isValueNullOrUndefined(value) {
    return typeof(value) === 'undefined' || value === null;
  }

  // Whether the given document is missing/nonexistant (i.e. null or undefined) or deleted (its "_deleted" property is true)
  function isDocumentMissingOrDeleted(candidate) {
    return isValueNullOrUndefined(candidate) || candidate._deleted;
  }

  // A property validator that is suitable for use on type identifier properties. Ensures the value is a string, is neither null nor
  // undefined, is not an empty string and cannot be modified.
  var typeIdValidator = {
    type: 'string',
    required: true,
    mustNotBeEmpty: true,
    immutable: true
  };

  // A type filter that matches on the document's type property
  function simpleTypeFilter(doc, oldDoc, candidateDocType) {
    if (oldDoc) {
      if (doc._deleted) {
        return oldDoc.type === candidateDocType;
      } else {
        return doc.type === oldDoc.type && oldDoc.type === candidateDocType;
      }
    } else {
      return doc.type === candidateDocType;
    }
  }

  // Retrieves the old doc's effective value. If it is null, undefined or its "_deleted" property is true, returns null. Otherwise, returns
  // the value of the "oldDoc" parameter.
  function getEffectiveOldDoc(oldDoc) {
    return !isDocumentMissingOrDeleted(oldDoc) ? oldDoc : null;
  }

  // Load the document authorization module
  var authorizationModule = function() {
    // A document definition may define its authorizations (channels, roles or users) for each operation type (view, add, replace, delete or
    // write) as either a string or an array of strings. In either case, add them to the list if they are not already present.
    function appendToAuthorizationList(allAuthorizations, authorizationsToAdd) {
      if (!isValueNullOrUndefined(authorizationsToAdd)) {
        if (authorizationsToAdd instanceof Array) {
          for (var i = 0; i < authorizationsToAdd.length; i++) {
            var authorization = authorizationsToAdd[i];
            if (allAuthorizations.indexOf(authorization) < 0) {
              allAuthorizations.push(authorization);
            }
          }
        } else if (allAuthorizations.indexOf(authorizationsToAdd) < 0) {
          allAuthorizations.push(authorizationsToAdd);
        }
      }
    }

    // A document definition may define its authorized channels, roles or users as either a function or an object/hashtable
    function getAuthorizationMap(doc, oldDoc, authorizationDefinition) {
      if (typeof(authorizationDefinition) === 'function') {
        return authorizationDefinition(doc, getEffectiveOldDoc(oldDoc));
      } else {
        return authorizationDefinition;
      }
    }

    // Retrieves a list of channels the document belongs to based on its specified type
    function getAllDocChannels(doc, oldDoc, docDefinition) {
      var docChannelMap = getAuthorizationMap(doc, oldDoc, docDefinition.channels);

      var allChannels = [ ];
      if (docChannelMap) {
        appendToAuthorizationList(allChannels, docChannelMap.view);
        appendToAuthorizationList(allChannels, docChannelMap.write);
        appendToAuthorizationList(allChannels, docChannelMap.add);
        appendToAuthorizationList(allChannels, docChannelMap.replace);
        appendToAuthorizationList(allChannels, docChannelMap.remove);
      }

      return allChannels;
    }

    // Retrieves a list of authorizations (e.g. channels, roles, users) for the current document write operation type (add, replace or remove)
    function getRequiredAuthorizations(doc, oldDoc, authorizationDefinition) {
      var authorizationMap = getAuthorizationMap(doc, oldDoc, authorizationDefinition);

      if (isValueNullOrUndefined(authorizationMap)) {
        // This document type does not define any authorizations (channels, roles, users) at all
        return null;
      }

      var requiredAuthorizations = [ ];
      var writeAuthorizationFound = false;
      if (authorizationMap.write) {
        writeAuthorizationFound = true;
        appendToAuthorizationList(requiredAuthorizations, authorizationMap.write);
      }

      if (doc._deleted) {
        if (authorizationMap.remove) {
          writeAuthorizationFound = true;
          appendToAuthorizationList(requiredAuthorizations, authorizationMap.remove);
        }
      } else if (!isDocumentMissingOrDeleted(oldDoc) && authorizationMap.replace) {
        writeAuthorizationFound = true;
        appendToAuthorizationList(requiredAuthorizations, authorizationMap.replace);
      } else if (isDocumentMissingOrDeleted(oldDoc) && authorizationMap.add) {
        writeAuthorizationFound = true;
        appendToAuthorizationList(requiredAuthorizations, authorizationMap.add);
      }

      if (writeAuthorizationFound) {
        return requiredAuthorizations;
      } else {
        // This document type does not define any authorizations (channels, roles, users) that apply to this particular write operation type
        return null;
      }
    }

    // Ensures the user is authorized to create/replace/delete this document
    function authorize(doc, oldDoc, docDefinition) {
      var authorizedChannels = getRequiredAuthorizations(doc, oldDoc, docDefinition.channels);
      var authorizedRoles = getRequiredAuthorizations(doc, oldDoc, docDefinition.authorizedRoles);
      var authorizedUsers = getRequiredAuthorizations(doc, oldDoc, docDefinition.authorizedUsers);

      var channelMatch = false;
      if (authorizedChannels) {
        try {
          requireAccess(authorizedChannels);
          channelMatch = true;
        } catch (ex) {
          // The user has none of the authorized channels
          if (!authorizedRoles && !authorizedUsers) {
            // ... and the document definition does not specify any authorized roles or users
            throw ex;
          }
        }
      }

      var roleMatch = false;
      if (authorizedRoles) {
        try {
          requireRole(authorizedRoles);
          roleMatch = true;
        } catch (ex) {
          // The user belongs to none of the authorized roles
          if (!authorizedChannels && !authorizedUsers) {
            // ... and the document definition does not specify any authorized channels or users
            throw ex;
          }
        }
      }

      var userMatch = false;
      if (authorizedUsers) {
        try {
          requireUser(authorizedUsers);
          userMatch = true;
        } catch (ex) {
          // The user does not match any of the authorized usernames
          if (!authorizedChannels && !authorizedRoles) {
            // ... and the document definition does not specify any authorized channels or roles
            throw ex;
          }
        }
      }

      if (!authorizedChannels && !authorizedRoles && !authorizedUsers) {
        // The document type does not define any channels, roles or users that apply to this particular write operation type, so fall back to
        // Sync Gateway's default behaviour for an empty channel list: 403 Forbidden for requests via the public API and either 200 OK or 201
        // Created for requests via the admin API. That way, the admin API will always be able to create, replace or remove documents,
        // regardless of their authorized channels, roles or users, as intended.
        requireAccess([ ]);
      } else if (!channelMatch && !roleMatch && !userMatch) {
        // None of the authorization methods (e.g. channels, roles, users) succeeded
        throw({ forbidden: 'missing channel access' });
      }

      return {
        channels: authorizedChannels,
        roles: authorizedRoles,
        users: authorizedUsers
      };
    }

    return {
      authorize: authorize,
      getAllDocChannels: getAllDocChannels
    };
  }();

  // Load the document validation module
  var validationModule = function() {
    // Determine if a given value is an integer. Exists as a failsafe because Number.isInteger is not guaranteed to exist in all environments.
    var isInteger = Number.isInteger || function(value) {
      return typeof value === 'number' && isFinite(value) && Math.floor(value) === value;
    };

    // Check that a given value is a valid ISO 8601 format date string with optional time and time zone components
    function isIso8601DateTimeString(value) {
      var regex = /^(([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]))([T ]([01][0-9]|2[0-4])(:[0-5][0-9])?(:[0-5][0-9])?([\\.,][0-9]{1,3})?)?([zZ]|([\\+-])([01][0-9]|2[0-3]):?([0-5][0-9])?)?$/;

      return regex.test(value);
    }

    // Check that a given value is a valid ISO 8601 date string without time and time zone components
    function isIso8601DateString(value) {
      var regex = /^(([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]))$/;

      return regex.test(value);
    }

    // A regular expression that matches one of the given file extensions
    function buildSupportedExtensionsRegex(extensions) {
      // Note that this regex uses double quotes rather than single quotes as a workaround to https://github.com/Kashoo/synctos/issues/116
      return new RegExp("\\\\.(" + extensions.join("|") + ")$", "i");
    }

    // Constructs the fully qualified path of the item at the top of the given stack
    function buildItemPath(itemStack) {
      var nameComponents = [ ];
      for (var i = 0; i < itemStack.length; i++) {
        var itemName = itemStack[i].itemName;

        if (!itemName) {
          // Skip null or empty names (e.g. the first element is typically the root of the document, which has no name)
          continue;
        } else if (nameComponents.length < 1 || itemName.indexOf('[') === 0) {
          nameComponents.push(itemName);
        } else {
          nameComponents.push('.' + itemName);
        }
      }

      return nameComponents.join('');
    }

    function resolveDocConstraint(doc, oldDoc, constraintDefinition) {
      return (typeof(constraintDefinition) === 'function') ? constraintDefinition(doc, getEffectiveOldDoc(oldDoc)) : constraintDefinition;
    }

    // Ensures the document structure and content are valid
    function validateDoc(doc, oldDoc, docDefinition, docType) {
      var validationErrors = [ ];

      validateDocImmutability(doc, oldDoc, docDefinition, validationErrors);

      // Only validate the document's contents if it's being created or replaced. There's no need if it's being deleted.
      if (!doc._deleted) {
        validateDocContents(
          doc,
          oldDoc,
          docDefinition,
          validationErrors);
      }

      if (validationErrors.length > 0) {
        throw { forbidden: 'Invalid ' + docType + ' document: ' + validationErrors.join('; ') };
      }
    }

    function validateDocImmutability(doc, oldDoc, docDefinition, validationErrors) {
      if (!isDocumentMissingOrDeleted(oldDoc)) {
        if (resolveDocConstraint(doc, oldDoc, docDefinition.immutable)) {
          validationErrors.push('documents of this type cannot be replaced or deleted');
        } else if (doc._deleted) {
          if (resolveDocConstraint(doc, oldDoc, docDefinition.cannotDelete)) {
            validationErrors.push('documents of this type cannot be deleted');
          }
        } else {
          if (resolveDocConstraint(doc, oldDoc, docDefinition.cannotReplace)) {
            validationErrors.push('documents of this type cannot be replaced');
          }
        }
      }
    }

    function validateDocContents(doc, oldDoc, docDefinition, validationErrors) {
      var attachmentReferenceValidators = { };
      var itemStack = [
        {
          itemValue: doc,
          oldItemValue: oldDoc,
          itemName: null
        }
      ];

      var resolvedPropertyValidators = resolveDocConstraint(doc, oldDoc, docDefinition.propertyValidators);

      // Ensure that, if the document type uses the simple type filter, it supports the "type" property
      if (docDefinition.typeFilter === simpleTypeFilter && isValueNullOrUndefined(resolvedPropertyValidators.type)) {
        resolvedPropertyValidators.type = typeIdValidator;
      }

      // Execute each of the document's property validators while ignoring the specified whitelisted properties at the root level
      validateProperties(
        resolvedPropertyValidators,
        resolveDocConstraint(doc, oldDoc, docDefinition.allowUnknownProperties),
        [ '_id', '_rev', '_deleted', '_revisions', '_attachments' ]);

      if (doc._attachments) {
        validateAttachments();
      }

      // The following functions are nested within this function so they can share access to the doc, oldDoc and validationErrors params and
      // the attachmentReferenceValidators and itemStack variables
      function resolveValidationConstraint(constraintDefinition) {
        if (typeof(constraintDefinition) === 'function') {
          var currentItemEntry = itemStack[itemStack.length - 1];

          return constraintDefinition(doc, getEffectiveOldDoc(oldDoc), currentItemEntry.itemValue, currentItemEntry.oldItemValue);
        } else {
          return constraintDefinition;
        }
      }

      function validateProperties(propertyValidators, allowUnknownProperties, whitelistedProperties) {
        var currentItemEntry = itemStack[itemStack.length - 1];
        var objectValue = currentItemEntry.itemValue;
        var oldObjectValue = currentItemEntry.oldItemValue;

        var supportedProperties = [ ];
        for (var propertyValidatorName in propertyValidators) {
          var validator = propertyValidators[propertyValidatorName];
          if (isValueNullOrUndefined(validator) || isValueNullOrUndefined(resolveValidationConstraint(validator.type))) {
            // Skip over non-validator fields/properties
            continue;
          }

          var propertyValue = objectValue[propertyValidatorName];

          var oldPropertyValue;
          if (!isValueNullOrUndefined(oldObjectValue)) {
            oldPropertyValue = oldObjectValue[propertyValidatorName];
          }

          supportedProperties.push(propertyValidatorName);

          itemStack.push({
            itemValue: propertyValue,
            oldItemValue: oldPropertyValue,
            itemName: propertyValidatorName
          });

          validateItemValue(validator);

          itemStack.pop();
        }

        // Verify there are no unsupported properties in the object
        if (!allowUnknownProperties) {
          for (var propertyName in objectValue) {
            if (whitelistedProperties && whitelistedProperties.indexOf(propertyName) >= 0) {
              // These properties are special cases that should always be allowed - generally only applied at the root level of the document
              continue;
            }

            if (supportedProperties.indexOf(propertyName) < 0) {
              var objectPath = buildItemPath(itemStack);
              var fullPropertyPath = objectPath ? objectPath + '.' + propertyName : propertyName;
              validationErrors.push('property "' + fullPropertyPath + '" is not supported');
            }
          }
        }
      }

      function validateItemValue(validator) {
        var currentItemEntry = itemStack[itemStack.length - 1];
        var itemValue = currentItemEntry.itemValue;
        var validatorType = resolveValidationConstraint(validator.type);

        if (validator.customValidation) {
          performCustomValidation(validator);
        }

        if (resolveValidationConstraint(validator.immutable)) {
          validateImmutable(false);
        }

        if (resolveValidationConstraint(validator.immutableWhenSet)) {
          validateImmutable(true);
        }

        if (!isValueNullOrUndefined(itemValue)) {
          if (resolveValidationConstraint(validator.mustNotBeEmpty) && itemValue.length < 1) {
            validationErrors.push('item "' + buildItemPath(itemStack) + '" must not be empty');
          }

          var minimumValue = resolveValidationConstraint(validator.minimumValue);
          if (!isValueNullOrUndefined(minimumValue)) {
            var minComparator = function(left, right) {
              return left < right;
            };
            validateRangeConstraint(minimumValue, validatorType, minComparator, 'less than');
          }

          var minimumValueExclusive = resolveValidationConstraint(validator.minimumValueExclusive);
          if (!isValueNullOrUndefined(minimumValueExclusive)) {
            var minExclusiveComparator = function(left, right) {
              return left <= right;
            };
            validateRangeConstraint(minimumValueExclusive, validatorType, minExclusiveComparator, 'less than or equal to');
          }

          var maximumValue = resolveValidationConstraint(validator.maximumValue);
          if (!isValueNullOrUndefined(maximumValue)) {
            var maxComparator = function(left, right) {
              return left > right;
            };
            validateRangeConstraint(maximumValue, validatorType, maxComparator, 'greater than');
          }

          var maximumValueExclusive = resolveValidationConstraint(validator.maximumValueExclusive);
          if (!isValueNullOrUndefined(maximumValueExclusive)) {
            var maxExclusiveComparator = function(left, right) {
              return left >= right;
            };
            validateRangeConstraint(maximumValueExclusive, validatorType, maxExclusiveComparator, 'greater than or equal to');
          }

          var minimumLength = resolveValidationConstraint(validator.minimumLength);
          if (!isValueNullOrUndefined(minimumLength) && itemValue.length < minimumLength) {
            validationErrors.push('length of item "' + buildItemPath(itemStack) + '" must not be less than ' + minimumLength);
          }

          var maximumLength = resolveValidationConstraint(validator.maximumLength);
          if (!isValueNullOrUndefined(maximumLength) && itemValue.length > maximumLength) {
            validationErrors.push('length of item "' + buildItemPath(itemStack) + '" must not be greater than ' + maximumLength);
          }

          switch (validatorType) {
            case 'string':
              var regexPattern = resolveValidationConstraint(validator.regexPattern);
              if (typeof itemValue !== 'string') {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be a string');
              } else if (regexPattern && !(regexPattern.test(itemValue))) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must conform to expected format ' + regexPattern);
              }
              break;
            case 'integer':
              if (!isInteger(itemValue)) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be an integer');
              }
              break;
            case 'float':
              if (typeof itemValue !== 'number') {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be a floating point or integer number');
              }
              break;
            case 'boolean':
              if (typeof itemValue !== 'boolean') {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be a boolean');
              }
              break;
            case 'datetime':
              if (typeof itemValue !== 'string' || !isIso8601DateTimeString(itemValue)) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be an ISO 8601 date string with optional time and time zone components');
              }
              break;
            case 'date':
              if (typeof itemValue !== 'string' || !isIso8601DateString(itemValue)) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be an ISO 8601 date string with no time or time zone components');
              }
              break;
            case 'enum':
              var enumPredefinedValues = resolveValidationConstraint(validator.predefinedValues);
              if (!(enumPredefinedValues instanceof Array)) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" belongs to an enum that has no predefined values');
              } else if (enumPredefinedValues.indexOf(itemValue) < 0) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be one of the predefined values: ' + enumPredefinedValues.toString());
              }
              break;
            case 'object':
              var childPropertyValidators = resolveValidationConstraint(validator.propertyValidators);
              if (typeof itemValue !== 'object' || itemValue instanceof Array) {
                validationErrors.push('item "' + buildItemPath(itemStack) + '" must be an object');
              } else if (childPropertyValidators) {
                validateProperties(childPropertyValidators, resolveValidationConstraint(validator.allowUnknownProperties));
              }
              break;
            case 'array':
              validateArray(resolveValidationConstraint(validator.arrayElementsValidator));
              break;
            case 'hashtable':
              validateHashtable(validator);
              break;
            case 'attachmentReference':
              validateAttachmentRef(validator);
              break;
            default:
              // This is not a document validation error; the item validator is configured incorrectly and must be fixed
              throw({ forbidden: 'No data type defined for validator of item "' + buildItemPath(itemStack) + '"' });
          }
        } else if (resolveValidationConstraint(validator.required)) {
          // The item has no value (either it's null or undefined), but the validator indicates it is required
          validationErrors.push('required item "' + buildItemPath(itemStack) + '" is missing');
        }
      }

      function validateImmutable(onlyEnforceIfHasValue) {
        if (!isDocumentMissingOrDeleted(oldDoc)) {
          var currentItemEntry = itemStack[itemStack.length - 1];
          var itemValue = currentItemEntry.itemValue;
          var oldItemValue = currentItemEntry.oldItemValue;

          if (onlyEnforceIfHasValue && isValueNullOrUndefined(oldItemValue)) {
            // No need to continue; the constraint only applies if the old value is neither null nor undefined
            return;
          }

          // Only compare the item's value to the old item if the item's parent existed in the old document. For example, if the item in
          // question is the value of a property in an object that is itself in an array, but the object did not exist in the array in the old
          // document, then there is nothing to validate.
          var oldParentItemValue = (itemStack.length >= 2) ? itemStack[itemStack.length - 2].oldItemValue : null;
          var constraintSatisfied;
          if (isValueNullOrUndefined(oldParentItemValue)) {
            constraintSatisfied = true;
          } else {
            constraintSatisfied = validateImmutableItem(itemValue, oldItemValue);
          }

          if (!constraintSatisfied) {
            validationErrors.push('value of item "' + buildItemPath(itemStack) + '" may not be modified');
          }
        }
      }

      function validateImmutableItem(itemValue, oldItemValue) {
        var itemMissing = isValueNullOrUndefined(itemValue);
        var oldItemMissing = isValueNullOrUndefined(oldItemValue);
        if (oldItemValue === itemValue || (itemMissing && oldItemMissing)) {
          return true;
        } else if (itemMissing !== oldItemMissing) {
          // One value is null or undefined but the other is not, so they cannot be equal
          return false;
        } else {
          if (itemValue instanceof Array || oldItemValue instanceof Array) {
            return validateImmutableArray(itemValue, oldItemValue);
          } else if (typeof(itemValue) === 'object' || typeof(oldItemValue) === 'object') {
            return validateImmutableObject(itemValue, oldItemValue);
          } else {
            return false;
          }
        }
      }

      function validateImmutableArray(itemValue, oldItemValue) {
        if (!(itemValue instanceof Array && oldItemValue instanceof Array)) {
          return false;
        } else if (itemValue.length !== oldItemValue.length) {
          return false;
        }

        for (var elementIndex = 0; elementIndex < itemValue.length; elementIndex++) {
          var elementValue = itemValue[elementIndex];
          var oldElementValue = oldItemValue[elementIndex];

          if (!validateImmutableItem(elementValue, oldElementValue)) {
            return false;
          }
        }

        // If we got here, all elements match
        return true;
      }

      function validateImmutableObject(itemValue, oldItemValue) {
        if (typeof(itemValue) !== 'object' || typeof(oldItemValue) !== 'object') {
          return false;
        }

        var itemProperties = [ ];
        for (var itemProp in itemValue) {
          itemProperties.push(itemProp);
        }

        for (var oldItemProp in oldItemValue) {
          if (itemProperties.indexOf(oldItemProp) < 0) {
            itemProperties.push(oldItemProp);
          }
        }

        for (var propIndex = 0; propIndex < itemProperties.length; propIndex++) {
          var propertyName = itemProperties[propIndex];
          var propertyValue = itemValue[propertyName];
          var oldPropertyValue = oldItemValue[propertyName];

          if (!validateImmutableItem(propertyValue, oldPropertyValue)) {
            return false;
          }
        }

        // If we got here, all properties match
        return true;
      }

      function validateRangeConstraint(rangeLimit, validationType, comparator, violationDescription) {
        var itemValue = itemStack[itemStack.length - 1].itemValue;
        if (validationType === 'datetime' || rangeLimit instanceof Date) {
          // Date/times require special handling because their time and time zone components are optional and time zones may differ
          try {
            var itemDate = new Date(itemValue);
            var constraintDate = (rangeLimit instanceof Date) ? rangeLimit : new Date(rangeLimit);
            if (comparator(itemDate.getTime(), constraintDate.getTime())) {
              var formattedRangeLimit = (rangeLimit instanceof Date) ? rangeLimit.toISOString() : rangeLimit;
              addOutOfRangeValidationError(formattedRangeLimit, violationDescription);
            }
          } catch (ex) {
            // The date/time's format may be invalid but it isn't technically in violation of the range constraint
          }
        } else if (comparator(itemValue, rangeLimit)) {
          addOutOfRangeValidationError(rangeLimit, violationDescription);
        }
      }

      function addOutOfRangeValidationError(rangeLimit, violationDescription) {
        validationErrors.push('item "' + buildItemPath(itemStack) + '" must not be ' + violationDescription + ' ' + rangeLimit);
      }

      function validateArray(elementValidator) {
        var currentItemEntry = itemStack[itemStack.length - 1];
        var itemValue = currentItemEntry.itemValue;
        var oldItemValue = currentItemEntry.oldItemValue;

        if (!(itemValue instanceof Array)) {
          validationErrors.push('item "' + buildItemPath(itemStack) + '" must be an array');
        } else if (elementValidator) {
          // Validate each element in the array
          for (var elementIndex = 0; elementIndex < itemValue.length; elementIndex++) {
            var elementName = '[' + elementIndex + ']';
            var elementValue = itemValue[elementIndex];
            var oldElementValue =
              (!isValueNullOrUndefined(oldItemValue) && elementIndex < oldItemValue.length) ? oldItemValue[elementIndex] : null;

            itemStack.push({
              itemName: elementName,
              itemValue: elementValue,
              oldItemValue: oldElementValue
            });

            validateItemValue(elementValidator);

            itemStack.pop();
          }
        }
      }

      function validateHashtable(validator) {
        var keyValidator = resolveValidationConstraint(validator.hashtableKeysValidator);
        var valueValidator = resolveValidationConstraint(validator.hashtableValuesValidator);
        var currentItemEntry = itemStack[itemStack.length - 1];
        var itemValue = currentItemEntry.itemValue;
        var oldItemValue = currentItemEntry.oldItemValue;
        var hashtablePath = buildItemPath(itemStack);

        if (typeof itemValue !== 'object' || itemValue instanceof Array) {
          validationErrors.push('item "' + buildItemPath(itemStack) + '" must be an object/hashtable');
        } else {
          var size = 0;
          for (var elementKey in itemValue) {
            size++;
            var elementValue = itemValue[elementKey];

            var elementName = '[' + elementKey + ']';
            if (keyValidator) {
              var fullKeyPath = hashtablePath ? hashtablePath + elementName : elementName;
              if (typeof elementKey !== 'string') {
                validationErrors.push('hashtable key "' + fullKeyPath + '" is not a string');
              } else {
                if (resolveValidationConstraint(keyValidator.mustNotBeEmpty) && elementKey.length < 1) {
                  validationErrors.push('empty hashtable key in item "' + buildItemPath(itemStack) + '" is not allowed');
                }
                var regexPattern = resolveValidationConstraint(keyValidator.regexPattern);
                if (regexPattern && !(regexPattern.test(elementKey))) {
                  validationErrors.push('hashtable key "' + fullKeyPath + '" does not conform to expected format ' + regexPattern);
                }
              }
            }

            if (valueValidator) {
              var oldElementValue;
              if (!isValueNullOrUndefined(oldItemValue)) {
                oldElementValue = oldItemValue[elementKey];
              }

              itemStack.push({
                itemName: elementName,
                itemValue: elementValue,
                oldItemValue: oldElementValue
              });

              validateItemValue(valueValidator);

              itemStack.pop();
            }
          }

          var maximumSize = resolveValidationConstraint(validator.maximumSize);
          if (!isValueNullOrUndefined(maximumSize) && size > maximumSize) {
            validationErrors.push('hashtable "' + hashtablePath + '" must not be larger than ' + maximumSize + ' elements');
          }

          var minimumSize = resolveValidationConstraint(validator.minimumSize);
          if (!isValueNullOrUndefined(minimumSize) && size < minimumSize) {
            validationErrors.push('hashtable "' + hashtablePath + '" must not be smaller than ' + minimumSize + ' elements');
          }
        }
      }

      function validateAttachmentRef(validator) {
        var currentItemEntry = itemStack[itemStack.length - 1];
        var itemValue = currentItemEntry.itemValue;

        if (typeof itemValue !== 'string') {
          validationErrors.push('attachment reference "' + buildItemPath(itemStack) + '" must be a string');
        } else {
          attachmentReferenceValidators[itemValue] = validator;

          var supportedExtensions = resolveValidationConstraint(validator.supportedExtensions);
          if (supportedExtensions) {
            var extRegex = buildSupportedExtensionsRegex(supportedExtensions);
            if (!extRegex.test(itemValue)) {
              validationErrors.push('attachment reference "' + buildItemPath(itemStack) + '" must have a supported file extension (' + supportedExtensions.join(',') + ')');
            }
          }

          // Because the addition of an attachment is typically a separate operation from the creation/update of the associated document, we
          // can't guarantee that the attachment is present when the attachment reference property is created/updated for it, so only
          // validate it if it's present. The good news is that, because adding an attachment is a two part operation (create/update the
          // document and add the attachment), the sync function will be run once for each part, thus ensuring the content is verified once
          // both parts have been synced.
          if (doc._attachments && doc._attachments[itemValue]) {
            var attachment = doc._attachments[itemValue];

            var supportedContentTypes = resolveValidationConstraint(validator.supportedContentTypes);
            if (supportedContentTypes && supportedContentTypes.indexOf(attachment.content_type) < 0) {
              validationErrors.push('attachment reference "' + buildItemPath(itemStack) + '" must have a supported content type (' + supportedContentTypes.join(',') + ')');
            }

            var maximumSize = resolveValidationConstraint(validator.maximumSize);
            if (!isValueNullOrUndefined(maximumSize) && attachment.length > maximumSize) {
              validationErrors.push('attachment reference "' + buildItemPath(itemStack) + '" must not be larger than ' + maximumSize + ' bytes');
            }
          }
        }
      }

      function validateAttachments() {
        var attachmentConstraints = resolveDocConstraint(doc, oldDoc, docDefinition.attachmentConstraints);

        var maximumAttachmentCount =
          attachmentConstraints ? resolveDocConstraint(doc, oldDoc, attachmentConstraints.maximumAttachmentCount) : null;
        var maximumIndividualAttachmentSize =
          attachmentConstraints ? resolveDocConstraint(doc, oldDoc, attachmentConstraints.maximumIndividualSize) : null;
        var maximumTotalAttachmentSize =
          attachmentConstraints ? resolveDocConstraint(doc, oldDoc, attachmentConstraints.maximumTotalSize) : null;

        var supportedExtensions = attachmentConstraints ? resolveDocConstraint(doc, oldDoc, attachmentConstraints.supportedExtensions) : null;
        var supportedExtensionsRegex = supportedExtensions ? buildSupportedExtensionsRegex(supportedExtensions) : null;

        var supportedContentTypes =
          attachmentConstraints ? resolveDocConstraint(doc, oldDoc, attachmentConstraints.supportedContentTypes) : null;

        var requireAttachmentReferences =
          attachmentConstraints ? resolveDocConstraint(doc, oldDoc, attachmentConstraints.requireAttachmentReferences) : false;

        var totalSize = 0;
        var attachmentCount = 0;
        for (var attachmentName in doc._attachments) {
          attachmentCount++;

          var attachment = doc._attachments[attachmentName];

          var attachmentSize = attachment.length;
          totalSize += attachmentSize;

          var attachmentRefValidator = attachmentReferenceValidators[attachmentName];

          if (requireAttachmentReferences && isValueNullOrUndefined(attachmentRefValidator)) {
            validationErrors.push('attachment ' + attachmentName + ' must have a corresponding attachment reference property');
          }

          if (isInteger(maximumIndividualAttachmentSize) && attachmentSize > maximumIndividualAttachmentSize) {
            // If this attachment is owned by an attachment reference property, that property's size constraint (if any) takes precedence
            if (isValueNullOrUndefined(attachmentRefValidator) || !isInteger(attachmentRefValidator.maximumSize)) {
              validationErrors.push('attachment ' + attachmentName + ' must not exceed ' + maximumIndividualAttachmentSize + ' bytes');
            }
          }

          if (supportedExtensionsRegex && !supportedExtensionsRegex.test(attachmentName)) {
            // If this attachment is owned by an attachment reference property, that property's extensions constraint (if any) takes
            // precedence
            if (isValueNullOrUndefined(attachmentRefValidator) || isValueNullOrUndefined(attachmentRefValidator.supportedExtensions)) {
              validationErrors.push('attachment "' + attachmentName + '" must have a supported file extension (' + supportedExtensions.join(',') + ')');
            }
          }

          if (supportedContentTypes && supportedContentTypes.indexOf(attachment.content_type) < 0) {
            // If this attachment is owned by an attachment reference property, that property's content types constraint (if any) takes
            // precedence
            if (isValueNullOrUndefined(attachmentRefValidator) || isValueNullOrUndefined(attachmentRefValidator.supportedContentTypes)) {
              validationErrors.push('attachment "' + attachmentName + '" must have a supported content type (' + supportedContentTypes.join(',') + ')');
            }
          }
        }

        if (isInteger(maximumTotalAttachmentSize) && totalSize > maximumTotalAttachmentSize) {
          validationErrors.push('the total size of all attachments must not exceed ' + maximumTotalAttachmentSize + ' bytes');
        }

        if (isInteger(maximumAttachmentCount) && attachmentCount > maximumAttachmentCount) {
          validationErrors.push('the total number of attachments must not exceed ' + maximumAttachmentCount);
        }

        if (!resolveDocConstraint(doc, oldDoc, docDefinition.allowAttachments) && attachmentCount > 0) {
          validationErrors.push('document type does not support attachments');
        }
      }

      function performCustomValidation(validator) {
        var currentItemEntry = itemStack[itemStack.length - 1];

        // Copy all but the last/top element so that the item's parent is at the top of the stack for the custom validation function
        var customValidationItemStack = itemStack.slice(-1);

        var customValidationErrors = validator.customValidation(doc, oldDoc, currentItemEntry, customValidationItemStack);

        if (customValidationErrors instanceof Array) {
          for (var errorIndex = 0; errorIndex < customValidationErrors.length; errorIndex++) {
            validationErrors.push(customValidationErrors[errorIndex]);
          }
        }
      }
    }

    return {
      validateDoc: validateDoc
    };
  }();

  // Load the access assignment module
  var accessAssignmentModule = function() {
    // Adds a prefix to the specified item if the prefix is defined
    function prefixItem(item, prefix) {
      return (prefix ? prefix + item : item.toString());
    }

    // Transforms the given item or items into a new list of items with the specified prefix (if any) appended to each element
    function resolveCollectionItems(originalItems, itemPrefix) {
      if (isValueNullOrUndefined(originalItems)) {
        return [ ];
      } else if (originalItems instanceof Array) {
        var resultItems = [ ];
        for (var i = 0; i < originalItems.length; i++) {
          var item = originalItems[i];

          if (isValueNullOrUndefined(item)) {
            continue;
          }

          resultItems.push(prefixItem(item, itemPrefix));
        }

        return resultItems;
      } else {
        // Represents a single item
        return [ prefixItem(originalItems, itemPrefix) ];
      }
    }

    // Transforms the given collection definition, which may have been defined as a single item, a list of items or a function that returns a
    // list of items into a simple list, where each item has the specified prefix, if any
    function resolveCollectionDefinition(doc, oldDoc, collectionDefinition, itemPrefix) {
      if (isValueNullOrUndefined(collectionDefinition)) {
        return [ ];
      } else {
        if (typeof(collectionDefinition) === 'function') {
          var fnResults = collectionDefinition(doc, oldDoc);

          return resolveCollectionItems(fnResults, itemPrefix);
        } else {
          return resolveCollectionItems(collectionDefinition, itemPrefix);
        }
      }
    }

    // Transforms a role collection definition into a simple list and prefixes each element with "role:"
    function resolveRoleCollectionDefinition(doc, oldDoc, rolesDefinition) {
      return resolveCollectionDefinition(doc, oldDoc, rolesDefinition, 'role:');
    }

    // Assigns channel access to users/roles
    function assignChannelsToUsersAndRoles(doc, oldDoc, accessAssignmentDefinition) {
      var usersAndRoles = [ ];

      var users = resolveCollectionDefinition(doc, oldDoc, accessAssignmentDefinition.users);
      for (var userIndex = 0; userIndex < users.length; userIndex++) {
        usersAndRoles.push(users[userIndex]);
      }

      var roles = resolveRoleCollectionDefinition(doc, oldDoc, accessAssignmentDefinition.roles);
      for (var roleIndex = 0; roleIndex < roles.length; roleIndex++) {
        usersAndRoles.push(roles[roleIndex]);
      }

      var channels = resolveCollectionDefinition(doc, oldDoc, accessAssignmentDefinition.channels);

      access(usersAndRoles, channels);

      return {
        type: 'channel',
        usersAndRoles: usersAndRoles,
        channels: channels
      };
    }

    // Assigns role access to users
    function assignRolesToUsers(doc, oldDoc, accessAssignmentDefinition) {
      var users = resolveCollectionDefinition(doc, oldDoc, accessAssignmentDefinition.users);
      var roles = resolveRoleCollectionDefinition(doc, oldDoc, accessAssignmentDefinition.roles);

      role(users, roles);

      return {
        type: 'role',
        users: users,
        roles: roles
      };
    }

    // Assigns role access to users and/or channel access to users/roles according to the given access assignment definitions
    function assignUserAccess(doc, oldDoc, accessAssignmentDefinitions) {
      var effectiveOldDoc = getEffectiveOldDoc(oldDoc);

      var effectiveAssignments = [ ];
      for (var assignmentIndex = 0; assignmentIndex < accessAssignmentDefinitions.length; assignmentIndex++) {
        var definition = accessAssignmentDefinitions[assignmentIndex];

        if (definition.type === 'role') {
          effectiveAssignments.push(assignRolesToUsers(doc, effectiveOldDoc, definition));
        } else if (definition.type === 'channel' || isValueNullOrUndefined(definition.type)) {
          effectiveAssignments.push(assignChannelsToUsersAndRoles(doc, effectiveOldDoc, definition));
        }
      }

      return effectiveAssignments;
    }

    return {
      assignUserAccess: assignUserAccess
    };
  }();

  var rawDocDefinitions = // Copyright (c) 2025 Hammerhead Navigation Inc.

    function() {
      var adminRole = 'role:admin';
      var moderatorRole = 'role:moderator';
      var analyserRole = 'role:analyser';
      var userRole = 'role:user';

      function hasPrefix(value, prefix){
        var pattern = new RegExp('^' + prefix);
        return pattern.test(value);
      }

      function isNullOrUndefined(value){
        return value === null || value === undefined;
      }

      function isNotANumber(value){
        return isNaN(value);
      }

      function isNotPositiveOrZero(value){
        return isNaN(value) || value < 0;
      }

      function isGreaterThanOrEqualToZero(value){
        return value >= 0;
      }

      function isGreaterThanZero(value){
        return value > 0;
      }

      function idShouldPrefixWithOwner(doc, oldDoc, currentItem, validationItemStack) {
        var owner = doc.owner;
        var prefix = owner + '.';
        var id = currentItem.itemValue;
        var validationErrors = [];
        if(!hasPrefix(id, prefix)){
          validationErrors.push('_id must start with owner name');
        }
        return validationErrors;
      }

      function idPrefixValidation(docType) {
        return function(doc, oldDoc, currentItem, validationItemStack) {
          var id = currentItem.itemValue;
          var validationErrors = [];
          if(!doc.owner || !doc.owner.length){
            validationErrors.push('_id must be {{owner}}.' + docType + '.uuid');
          }else {
            var prefixLength = doc.owner.length + (docType.length + 2);
            var prefix = id.substring(0, prefixLength);
            var suffix = id.substring(prefixLength, id.length);
            if(prefix !== doc.owner + '.' + docType + '.' || suffix.length === 0){
              validationErrors.push('_id must be {{owner}}.' + docType + '.uuid');
            }
          }
          return validationErrors;
        };
      }

      function idPrefixValidationWithUniqueDeviceId(docType) {
        return function(doc, oldDoc, currentItem, validationItemStack) {
          var id = currentItem.itemValue;
          var validationErrors = [];
          if (!doc.owner || !doc.owner.length) {
            validationErrors.push('_id must be {{owner}}.' + docType +  '.deviceId' + '.uuid');
          } else {
            var prefixLength = doc.owner.length + (docType.length + 2);
            var splitFields = id.split(".");
            var owner = splitFields[0];
            var type = splitFields[1];
            var deviceId = splitFields[2];
            var uuid = splitFields[3];
            if (owner !== doc.owner || type !== docType || deviceId.length === 0 || uuid.length === 0 || splitFields.length !== 4) {
              validationErrors.push('_id must be {{owner}}.' + docType +  '.deviceId' + '.uuid');
            }
          }
          return validationErrors;
        };
      }

      function collectionIdValidation() {
        return function (doc, oldDoc, currentItem, validationItemStack) {
          var docType = 'collection'
            var validationErrors = [];
          var id = currentItem.itemValue;
          if (id !== 'favorite' && id !== 'archive') {
            if (!doc.owner || !doc.owner.length) {
              validationErrors.push('_id must be {{owner}}.' + docType +  '.collectionId');
            } else {
              var splitFields = id.split(".");
              var owner = splitFields[0];
              var type = splitFields[1];
              var collectionId = splitFields[2];
              if (owner !== doc.owner || type !== docType || collectionId.length === 0 || splitFields.length !== 3) {
                validationErrors.push('_id must be {{owner}}.' + docType +  '.collectionId');
              }
            }
          }
          return validationErrors;
        }
      }

      function durationValidation(doc, oldDoc, currentItemElement, validationItemStack) {
        if(doc._deleted){
          return;
        }

        var validationErrors = [];
        var duration = currentItemElement.itemValue;
        // This is captured by required field
        if(!duration || !duration.startTime ||
          !duration.endTime || isNullOrUndefined(duration.elapsedTime)) {
          return;
        }

        var startTime = new Date(duration.startTime).getTime();
        var endTime = new Date(duration.endTime).getTime();
        if(endTime < startTime){
          validationErrors.push('endTime should be greater than startTime');
        }
        var elapsedTime = endTime - startTime;

        // We are not equating time because of this issue https://github.com/robertkrimen/otto/issues/264
        if(Math.abs(duration.elapsedTime - elapsedTime) >= 5){
          validationErrors.push('elapsedTime should be a diff of startTime and endTime');
        }
        return validationErrors;
      }

      function docOwner(doc, oldDoc) {
        return doc._deleted ? oldDoc.owner : doc.owner;
      }

      function docType(doc, oldDoc) {
        return doc._deleted ? oldDoc.type : doc.type;
      }

      function docChannel(doc, oldDoc) {
        return doc._id + '-READ'
      }

      function ownerChannel(doc, oldDoc) {
        return docOwner(doc, oldDoc) + '_data';
      }

      function defaultAuthorizedUsers(doc, oldDoc) {
        var owner = doc._deleted ? oldDoc.owner : doc.owner;
        return {
          write: owner
        };
      }

      function defaultAuthorizedRoles(doc, oldDoc) {
        return {
          write: adminRole
        };
      }

      return {
        user_profile: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              // the id should start with the owner name
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function userProfileId(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if (!doc || id !== doc.owner + '.' + 'user_profile') {
                  validationErrors.push('_id must be {{owner}}.user_profile');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            firstName: {
              type: 'string',
              mustNotBeEmpty: true
            },
            lastName: {
              type: 'string',
              mustNotBeEmpty: true
            },
            email: {
              type: 'string',
              required: false,
              mustNotBeEmpty: true
            },
            dob: {
              type: 'date'
            },
            weightInKilograms: {
              type: 'float',
              minimumValue: 0
            },
            hasConfiguredWeight: {
              type: 'boolean',
              required: false
            },
            heightInCentimetres: {
              type: 'float',
              minimumValue: 0
            },
            sex: {
              type: 'enum',
              predefinedValues: ['MALE', 'FEMALE', 'NOT_DEFINED']
            },
            lifetimeAthlete: {
              type: 'boolean'
            },
            lastOnboardingStep: {
              type: 'string'
            },
            preferredUnits: {
              type: 'object',
              required: false,
              propertyValidators: {
                distance: {
                  type: 'enum',
                  predefinedValues: ['METRIC', 'IMPERIAL'],
                  required: true
                },
                elevation: {
                  type: 'enum',
                  predefinedValues: ['METRIC', 'IMPERIAL'],
                  required: true
                },
                temperature: {
                  type: 'enum',
                  predefinedValues: ['METRIC', 'IMPERIAL'],
                  required: true
                },
                weight: {
                  type: 'enum',
                  predefinedValues: ['METRIC', 'IMPERIAL'],
                  required: true
                },
                pressure: {
                  type: 'enum',
                  predefinedValues: ['KPA', 'PSI', 'BAR'],
                  required: false
                }
              }
            },
            preferredUnit: {
              type: 'enum',
              predefinedValues: ['AUTOMATIC', 'METRIC', 'IMPERIAL'],
              required: false
            },
            locale: {
              type: 'string',
              required: false
            },
            uploadMode: {
              type: 'enum',
              predefinedValues: ['AUTOMATIC', 'MANUAL'],
              required: false
            },
            turnByTurnMode: {
              type: 'enum',
              predefinedValues: ['ALWAYS_ON', 'ON_CUE', 'OFF'],
              required: false
            },
            hideKeyButtons: {
              type: 'boolean',
              required: false
            },
            showKeyButtons: {
              type: 'boolean',
              required: false
            },
            climbBroMode: {
              type: 'enum',
              predefinedValues: ['SMALL', 'MEDIUM', 'LARGE', 'NONE'],
              required: false
            },
            climberSettings: {
              type: 'object',
              required: false,
              propertyValidators: {
                mode: {
                  type: 'enum',
                  required: true,
                  predefinedValues: ['OFF', 'ROUTES', 'ALWAYS']
                },
                minLevel: {
                  type: 'enum',
                  required: true,
                  predefinedValues: ['SMALL', 'MEDIUM', 'LARGE']
                }
              }
            },
            maxHr: {
              type: 'integer',
              minimumValue: 0,
              customValidation: function(doc, oldDoc, currentItemElement, validationItemStack) {
                if (doc._deleted) {
                  return;
                }
                if(isNullOrUndefined(doc.maxHr)) {
                  return;
                }
                var MAX_HR = 255;
                var validationErrors = [];
                if (doc.maxHr > MAX_HR) {
                  validationErrors.push("maxHr should be less than or equal to MAX_HR");
                }
                if (doc.maxHr < doc.restingHr) {
                  validationErrors.push("maxHr should be greater than restingHr");
                }
                return validationErrors;
              }
            },
            restingHr: {
              type: 'integer',
              minimumValue: 0
            },
            powerFtp: {
              type: 'integer',
              minimumValue: 0
            },
            isHrZoneAutomatic: {
              type: 'boolean'
            },
            isPowerZoneAutomatic: {
              type: 'boolean'
            },
            heartRateZones: {
              type: 'array',
              minimumLength: 0,
              customValidation: function(doc, oldDoc, currentItemElement, validationItemStack) {
                if (doc._deleted) {
                  return;
                }
                if(isNullOrUndefined(doc.heartRateZones)) {
                  return;
                }
                var MAX_HR_ZONES_COUNT = 10;
                var MIN_HR = 0;
                var MAX_HR = 255;
                var validationErrors = [];
                if (doc.heartRateZones.length > MAX_HR_ZONES_COUNT) {
                  validationErrors.push("HeartRateZones size should be less than or equal to MAX_HR_ZONES_COUNT");
                }
                if (doc.heartRateZones.length >= 1) {
                  for (var i = 0; i < doc.heartRateZones.length; i++) {
                    if (doc.heartRateZones[i].min > doc.heartRateZones[i].max) {
                      validationErrors.push("HeartRateZones min cannot be greater than max");
                    }
                  }
                }

                if (doc.heartRateZones.length > 2) {
                  if (doc.heartRateZones[0].min !== MIN_HR) {
                    validationErrors.push("First heartRateZone min should start with MIN_HR");
                  }
                  if (doc.heartRateZones[doc.heartRateZones.length - 1].max > MAX_HR) {
                    validationErrors.push("Last heartRateZone max should be less than or equal to MAX_HR");
                  }
                  for (var j = 0; j <= doc.heartRateZones.length - 2; j++) {
                    if (doc.heartRateZones[j].max !== (doc.heartRateZones[j + 1].min - 1)) {
                      validationErrors.push("Heart rate zones ranges should be sequential");
                    }
                  }
                }
                return validationErrors;
              },
              arrayElementsValidator: {
                type: 'object',
                propertyValidators: {
                  min: {
                    type: 'integer'
                  },
                  max: {
                    type: 'integer'
                  },
                  name: {
                    type: 'string',
                    required: false
                  }
                }
              }
            },
            powerZones: {
              type: 'array',
              minimumLength: 0,
              customValidation: function(doc, oldDoc, currentItemElement, validationItemStack) {
                if (doc._deleted) {
                  return;
                }
                if(isNullOrUndefined(doc.powerZones)) {
                  return;
                }
                var MAX_POWER_ZONES_COUNT = 10;
                var MIN_POWER = 0;
                var MAX_POWER_THRESHOLD = 9999;
                var validationErrors = [];
                if (doc.powerZones.length > MAX_POWER_ZONES_COUNT) {
                  validationErrors.push("PowerZones size should be less than or equal to MAX_POWER_ZONES_COUNT");
                }

                if (doc.powerZones.length >= 1) {
                  for (var i = 0; i < doc.powerZones.length; i++) {
                    if (doc.powerZones[i].min > doc.powerZones[i].max) {
                      validationErrors.push("PowerZones min cannot be greater than max");
                    }
                  }
                }

                if (doc.powerZones.length > 2) {
                  if (doc.powerZones[0].min !== MIN_POWER) {
                    validationErrors.push("First powerZones min should start with MIN_POWER");
                  }
                  if (doc.powerZones[doc.powerZones.length - 1].max > MAX_POWER_THRESHOLD) {
                    validationErrors.push("Last powerZones max should be less than or equal to MAX_POWER_THRESHOLD");
                  }
                  for (var j = 0; j <= doc.powerZones.length - 2; j++) {
                    if (doc.powerZones[j].max !== (doc.powerZones[j + 1].min - 1)) {
                      validationErrors.push("PowerZone ranges should be sequential");
                    }
                  }
                }
                return validationErrors;
              },
              arrayElementsValidator: {
                type: 'object',
                propertyValidators: {
                  min: {
                    type: 'integer'
                  },
                  max: {
                    type: 'integer'
                  },
                  name: {
                    type: 'string',
                    required: false
                  }
                }
              }
            },
            autoPause: {
              type: 'object',
              required: false,
              propertyValidators: {
                enabled: {
                  type: 'boolean',
                  required: true
                },
                threshold: {
                  type: 'float',
                  required: true
                },
                state: {
                  type: 'string',
                  required: true
                }
              }
            },
            autoLap: {
              type: 'object',
              required: false,
              propertyValidators: {
                mode: {
                  type: 'enum',
                  predefinedValues: ['OFF', 'DISTANCE', 'TIME', 'LOCATION'],
                  required: true
                },
                distance: {
                  type: 'float',
                  required: true
                },
                time: {
                  type: 'integer',
                  required: true
                },
                dropPinOnRideStart: {
                  type: 'boolean',
                  required: false
                },
              }
            },
            powerZeroPreference: {
              type: 'enum',
              predefinedValues: ['INCLUDE_ZEROS', 'EXCLUDE_ZEROS'],
              required: false
            },
            cadenceZeroPreference: {
              type: 'enum',
              predefinedValues: ['INCLUDE_ZEROS', 'EXCLUDE_ZEROS'],
              required: false
            },
            tbtCuesEnabled: {
              type: 'boolean',
              required: false
            },
            persistentTbtEnabled: {
              type: 'boolean',
              required: false
            },
            heatmapsEnabled: {
              type: 'boolean',
              required: false
            },
            pillsEnabled: {
              type: 'boolean',
              required: false
            },
            dataFieldIconsEnabled: {
              type: 'boolean',
              required: false
            },
            dataFieldLabelSize: {
              type: 'enum',
              predefinedValues: [ 'SMALL', 'LARGE' ],
              required: false
            },
            dataFieldAlignment: {
              type: 'enum',
              predefinedValues: [ 'LEFT', 'CENTER', 'RIGHT' ],
              required: false
            },
            radarAlignment: {
              type: 'enum',
              predefinedValues: [ 'LEFT', 'RIGHT' ],
              required: false
            },
            autoBrightnessEnabled: {
              type: 'boolean',
              required: false
            },
            deviceTheme: {
              type: 'enum',
              predefinedValues: [ 'DARK', 'LIGHT', 'AUTO' ],
              required: false
            },
            axsBikeSettings: {
              type: 'object',
              required: false,
              propertyValidators: {
                enabled: {
                  type: 'boolean',
                  required: true
                },
                ignoredBikes: {
                  type: 'array',
                  required: true,
                  arrayElementsValidator: {
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  }
                },
              }
            },
            backgroundUrl: {
              type: 'string',
              required: false
            },
            enduranceModeSettings: {
              type: 'object',
              required: false,
              propertyValidators: {
                screenOffTimeS: {
                  type: 'integer',
                  required: true
                },
                triggers: {
                  type: 'object',
                  required: true
                },
                maxBrightness: {
                  type: 'string',
                  required: false
                },
                slowerProcessing: {
                  type: 'boolean',
                  required: false
                },
                screenOffEnabled: {
                  type: 'boolean',
                  required: false
                }
              }
            },
            newAudioAlerts: {
              type: 'boolean',
              required: false
            },
            timeFormat: {
              type: 'enum',
              predefinedValues: [ 'HOURS_12', 'HOURS_24' ],
              required: false
            },
            dateFormat: {
              type: 'enum',
              predefinedValues: [ 'MONTH_FIRST', 'DAY_FIRST' ],
              required: false
            }
          },
          channels: function(doc, oldDoc) {
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        email_lookup: // Copyright (c) 2022 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function userProfileId(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if(id.substring(0, 7) !=='email::'){
                  validationErrors.push('_id must start with "email::"');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            ref: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            userId: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            isVerified: {
              type: 'boolean',
              required: true,
            },
            verificationToken: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            resetPasswordToken: {
              type : 'string',
              required: false,
              mustNotBeEmpty: true,
            },
            resetPasswordExpiry: {
              type : 'datetime',
              required: false,
            }
          },
          authorizedRoles: defaultAuthorizedRoles
        },
        sram_lookup: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function userProfileId(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if(id.substring(0, 6) !=='sram::'){
                  validationErrors.push('_id must start with "sram::"');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            userId: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            imported: {
              type: 'boolean',
              required: false
            },
          },
          authorizedRoles: defaultAuthorizedRoles
        },
        strava_settings: // Copyright (c) 2022 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              // the id should start with the owner name
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function userProfileId(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if(id !== doc.owner + '.' + 'strava_settings'){
                  validationErrors.push('_id must be {{owner}}.strava_settings');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            accessToken: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            refreshToken: {
              type: 'string',
              required: false,
              mustNotBeEmpty: false
            },
            expiresAt: {
              type: 'integer',
              required: false,
              mustNotBeEmpty: false
            },
            activityUploadMode: {
              type: 'enum',
              predefinedValues: [ 'AUTOMATIC_WITH_REVIEW', 'MANUAL', 'AUTOMATIC' ],
              required: false
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            preferredPrivateUpload: {
              type: 'boolean',
              required: true
            },
            premium: {
              type: 'boolean',
              required: true
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        bike_profile: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('bike_profile')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            index: {
              type: 'integer',
              required: true
            },
            wheelCircumference: {
              type: 'float',
              required: false
            },
            odometer: {
              type: 'float',
              required: false
            },
            sensors: {
              type: 'array',
              minimumLength: 0,
              required: false,
              arrayElementsValidator: {
                type: 'string',
                mustNotBeEmpty: true,
                required: true
              },
            },
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        route: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('route')
            },
            createdAt: {
              type: 'datetime',
              required: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            distance:{
              type:'float',
              required: true,
              minimumValue: 0
            },
            elevation:{
              type: 'object',
              propertyValidators: {
                gain: {
                  type: 'float',
                  required: true
                },
                loss: {
                  type: 'float',
                  required: true
                },
                min: {
                  type: 'float',
                  required: true
                },
                max: {
                  type: 'float',
                  required: true
                },
                polyline: {
                  type: 'string',
                  required: true,
                  mustNotBeEmpty: true
                },
                source: {
                  type: 'string',
                  required: false
                }
              },
              required: true
            },
            oldElevation:{
              type: 'object',
              propertyValidators: {
                gain: {
                  type: 'float',
                  required: true
                },
                loss: {
                  type: 'float',
                  required: true
                },
                min: {
                  type: 'float',
                  required: true
                },
                max: {
                  type: 'float',
                  required: true
                },
                polyline: {
                  type: 'string',
                  required: true,
                  mustNotBeEmpty: true
                },
              },
              required: false
            },
            startLocationName:{
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            endLocationName:{
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            startLocation:{
              type: 'object',
              required: true,
              propertyValidators:{
                lat:{
                  type: 'float',
                  required: true,
                  minimumValue: -90,
                  maximumValue: 90
                },
                lng:{
                  type: 'float',
                  required: true,
                  minimumValue: -180,
                  maximumValue: 180
                }
              }
            },
            endLocation:{
              type: 'object',
              required: true,
              propertyValidators:{
                lat:{
                  type: 'float',
                  required: true,
                  minimumValue: -90,
                  maximumValue: 90
                },
                lng:{
                  type: 'float',
                  required: true,
                  minimumValue: -180,
                  maximumValue: 180
                }
              }
            },
            waypoints:{
              type: 'array',
              minimumLength: 0,
              required: true,
              arrayElementsValidator:{
                type: 'object',
                propertyValidators:{
                  lat:{
                    type: 'float',
                    required: true,
                    minimumValue: -90,
                    maximumValue: 90
                  },
                  lng:{
                    type: 'float',
                    required: true,
                    minimumValue: -180,
                    maximumValue: 180
                  },
                  waypointType:{
                    type: 'enum',
                    predefinedValues: [ 'BREAK', 'VIA', 'THROUGH', 'BREAK_THROUGH', 'FREE_HAND' ],
                    required: false
                  },
                  polylineIndex:{
                    type: 'integer',
                    minimumValue: 0,
                    required: false
                  },
                  oneway:{
                    type: 'boolean',
                    required: false
                  }
                }
              }
            },
            pointsOfInterest:{
              type: 'array',
              minimumLength: 0,
              required: false,
              arrayElementsValidator:{
                type: 'object',
                propertyValidators:{
                  type:{
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  },
                  location:{
                    type: 'object',
                    required: true,
                    propertyValidators:{
                      lat:{
                        type: 'float',
                        required: true,
                        minimumValue: -90,
                        maximumValue: 90
                      },
                      lng:{
                        type: 'float',
                        required: true,
                        minimumValue: -180,
                        maximumValue: 180
                      }
                    }
                  },
                  name:{
                    type: 'string',
                    required: false
                  },
                  description:{
                    type: 'string',
                    required: false
                  },
                  bearing:{
                    type: 'float',
                    required: false,
                  },
                  updatedAt: {
                    type: 'datetime',
                    required: false
                  },
                  sourceId: {
                    type: 'string',
                    required: false
                  }
                }
              }
            },
            surfaceSummary:{
              type: 'object',
              required: false,
              propertyValidators:{
                pavedLength:{
                  type: 'float',
                  required: false,
                  minimumValue: 0,
                },
                gravelLength:{
                  type: 'float',
                  required: false,
                  minimumValue: 0,
                },
                trailLength:{
                  type: 'float',
                  required: false,
                  minimumValue: 0,
                },
                unknownLength:{
                  type: 'float',
                  required: false,
                  minimumValue: 0,
                },
              }
            },
            summaryPolyline:{
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            routePolyline:{
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            isStarred:{
              type: 'boolean',
              required: true
            },
            source:{
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            sourceId:{
              type: 'string',
              mustNotBeEmpty: true,
              immutableWhenSet: true
            },
            routingType:{
              type: 'enum',
              predefinedValues: [ 'road', 'gravel', 'mtb' ],
              required: false
            },
            isPublic:{
              type: 'boolean'
            },
            imageVersion:{
              type: 'string',
              required: false
            },
            isAutoImported:{
              type: 'boolean',
              required: false
            },
            updatedAt: {
              type: 'datetime',
              required: false
            },
            collections: {
              type: 'array',
              required: false,
              minimumLength: 0,
              arrayElementsValidator:{
                type: 'string',
                required: true,
                customValidation: collectionIdValidation,
              }
            },
            bounds:{
              type: 'array',
              minimumLength: 2,
              required: false,
              arrayElementsValidator:{
                type: 'object',
                propertyValidators:{
                  lat:{
                    type: 'float',
                    required: true,
                    minimumValue: -90,
                    maximumValue: 90
                  },
                  lng:{
                    type: 'float',
                    required: true,
                    minimumValue: -180,
                    maximumValue: 180
                  }
                }
              }
            },
            'image.jpg':{
              type: 'object',
              required: false
            },
          },
          allowAttachments: true,
          attachmentConstraints: {
            maximumAttachmentCount: 1,
            supportedExtensions: [ 'jpg' ],
            supportedContentTypes: [ 'image/jpeg' ]
          },
          channels: function(doc, oldDoc){
            var channels = [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)];
            if(!doc._deleted && doc.isPublic){
              channels.push("public_routes-READ");
            }
            return {
              view: channels
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        point_of_interest: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('point_of_interest')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: false
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            poiType: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            location: {
              type: 'object',
              required: true,
              propertyValidators:{
                lat:{
                  type: 'float',
                  required: true,
                  minimumValue: -90,
                  maximumValue: 90
                },
                lng:{
                  type: 'float',
                  required: true,
                  minimumValue: -180,
                  maximumValue: 180
                }
              }
            },
            name: {
              type: 'string',
              required: false
            },
            description: {
              type: 'string',
              required: false
            },
            bearing: {
              type: 'float',
              required: false,
            },
            sourceId: {
              type: 'string',
              required: false
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        tracking: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('tracking')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            isPublic: {
              type: 'boolean'
            },
            state: {
              type: 'string',
              required: true
            },
            location: {
              type: 'object',
              required: false,
              propertyValidators: {
                lat: {
                  type: 'float',
                  required: true,
                  minimumValue: -90,
                  maximumValue: 90
                },
                lng: {
                  type: 'float',
                  required: true,
                  minimumValue: -180,
                  maximumValue: 180
                }
              }
            },
            bearing: {
              type: 'float',
              required: false
            },
            routeId: {
              type: 'string',
              required: false
            },
            activityInfo: {
              type: 'array',
              required: true,
              arrayElementsValidator: {
                type: 'object',
                required: true,
                propertyValidators: {
                  key:{
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  },
                  value: {
                    type: 'object',
                    required: true,
                    propertyValidators: {
                      format: {
                        type: 'enum',
                        required: true,
                        predefinedValues: ['int', 'float', 'double', 'long']
                      },
                      value: {
                        type: 'float',
                        required: true
                      }
                    }
                  }
                }
              }
            },
            shareCode: {
              type: 'string',
              required: false
            },
            units: {
              type: 'enum',
              predefinedValues: ['METRIC', 'IMPERIAL'],
              required: false
            },
            enabled: {
              type: 'boolean',
              required: false
            },
            autoShare: {
              type: 'boolean',
              required: false
            },
            contacts: {
              type: 'array',
              required: false,
              arrayElementsValidator: {
                type: 'object',
                required: true,
                propertyValidators: {
                  name: {
                    type: 'string',
                  },
                  type: {
                    type: 'enum',
                    required: true,
                    predefinedValues: ['email', 'phone']
                  },
                  email: {
                    type: 'string'
                  }
                }
              }
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        tracking_data: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('tracking_data')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            state: {
              type: 'string',
              required: true
            },
            location: {
              type: 'object',
              required: false,
              propertyValidators: {
                lat: {
                  type: 'float',
                  required: true,
                  minimumValue: -90,
                  maximumValue: 90
                },
                lng: {
                  type: 'float',
                  required: true,
                  minimumValue: -180,
                  maximumValue: 180
                }
              }
            },
            bearing: {
              type: 'float',
              required: false
            },
            routeId: {
              type: 'string',
              required: false
            },
            activityId: {
              type: 'string',
              required: false
            },
            activityInfo: {
              type: 'array',
              required: true,
              arrayElementsValidator: {
                type: 'object',
                required: true,
                propertyValidators: {
                  key:{
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  },
                  value: {
                    type: 'object',
                    required: true,
                    propertyValidators: {
                      format: {
                        type: 'enum',
                        required: true,
                        predefinedValues: ['int', 'float', 'double', 'long']
                      },
                      value: {
                        type: 'float',
                        required: true
                      }
                    }
                  }
                }
              }
            },
            units: {
              type: 'enum',
              predefinedValues: ['METRIC', 'IMPERIAL'],
              required: false
            },
            userTrace: {
              type: 'string',
              required: false
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        activity: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('activity')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: false
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            bikeId: {
              type: 'string',
              required: false
            },
            client:{
              type: 'string',
              required: true,
              immutable: true
            },
            activityType:{
              type: 'enum',
              predefinedValues: [ 'RIDE', 'EBIKE', 'MOUNTAIN_BIKE', 'GRAVEL', 'EMOUNTAIN_BIKE', 'VELOMOBILE' ],
              required: false,
              immutable: false
            },
            activeTime:{
              type : 'integer',
              required: true,
              minimumValue: 1,
              immutable: true,
              customValidation: function(doc, oldDoc, currentItemElement, validationItemStack) {
                if(doc._deleted){
                  return;
                }

                // This is captured by required field
                if(isNullOrUndefined(doc.activeTime) || !doc.laps) {
                  return;
                }

                var totalLapsActiveTime = 0;
                for(var i = 0; i < doc.laps.length; i++){
                  // This is captured by required field
                  if(isNullOrUndefined(doc.laps[i].activeTime)){
                    return;
                  }
                  totalLapsActiveTime += doc.laps[i].activeTime;
                }

                if(currentItemElement.itemValue && currentItemElement.itemValue !== totalLapsActiveTime){
                  return ['activeTime should be a sum of all lap activeTimes'];
                }
                return;
              }
            },
            duration:{
              type: 'object',
              required: true,
              customValidation: durationValidation,
              propertyValidators:{
                elapsedTime:{
                  type: 'integer',
                  required: true,
                  immutable: true
                },
                startTime:{
                  type: 'datetime',
                  required: true,
                  immutable: true
                },
                endTime:{
                  type: 'datetime',
                  required: true,
                  immutable: true,
                  customValidation: function(doc, oldDoc, currentItemElement, validationItemStack) {
                    if(doc._deleted){
                      return;
                    }

                    // This is captured by required field
                    if(!doc.duration || !doc.duration.endTime || !doc.laps) {
                      return;
                    }

                    var endTime = new Date(doc.duration.endTime).getTime();
                    var lastLap = doc.laps[doc.laps.length - 1];
                    if(!lastLap.duration || !lastLap.duration.endTime){
                      return;
                    }
                    var lastLapEndTime = new Date(lastLap.duration.endTime).getTime();
                    if(endTime !== lastLapEndTime){
                      return ['activity endTime should be same as last lap endTime'];
                    }
                    return;
                  }
                }
              }
            },
            activityInfo:{
              type: 'array',
              required: true,
              arrayElementsValidator:{
                type: 'object',
                required: true,
                propertyValidators:{
                  key:{
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  },
                  value:{
                    type: 'object',
                    required: true,
                    propertyValidators:{
                      format: {
                        type: 'enum',
                        required: true,
                        predefinedValues: ['int', 'float', 'double', 'long']
                      },
                      value: {
                        type: 'float',
                        required: true
                      }
                    }
                  }
                }
              }
            },
            polyline: {
              type: 'string',
              required: false
            },
            laps:{
              type: 'array',
              minimumLength: 1,
              required: true,
              customValidation: function(doc, oldDoc, currentItemElement, validationItemStack) {
                if(doc._deleted){
                  return;
                }

                // This is captured by required field
                if(!doc.duration || !doc.duration.startTime || !doc.duration.endTime || !doc.laps) {
                  return;
                }

                var validationErrors = [];
                for(var i = 0; i < doc.laps.length; i++){
                  var lap = doc.laps[i];
                  // lapNumber validation

                  if(lap.lapNumber && lap.lapNumber !== i + 1){
                    validationErrors.push('lapNumber should be plus one than lap index');
                  }

                  if(!lap.pauses || !lap.duration || !lap.duration.startTime ||
                    !lap.duration.endTime || isNullOrUndefined(lap.duration.elapsedTime)){
                    return;
                  }

                  // activeTime validation && sequential pause validation
                  var totalPausedTime = 0;
                  for(var j = 0; j < lap.pauses.length; j++){
                    var pause = lap.pauses[j];

                    if(!pause.startTime || !pause.endTime || isNullOrUndefined(pause.elapsedTime)){
                      return;
                    }

                    totalPausedTime += pause.elapsedTime;

                    // pause startTime validation
                    if(j === 0){
                      if(pause.startTime < lap.duration.startTime){
                        validationErrors.push('pause startTime should be greater than or equal to lap startTime');
                      }
                    }else{
                      var previousPause = lap.pauses[j - 1];
                      if(pause.startTime < previousPause.endTime){
                        validationErrors.push('pause startTime should be greater than or equal to last pause endTime');
                      }
                    }

                    // last pause endTime validation
                    if(j === lap.pauses.length - 1){
                      if(pause.endTime > lap.duration.endTime){
                        validationErrors.push('pause endTime should be less than or equal to lap endTime');
                      }
                    }
                  }

                  if(lap.activeTime && lap.activeTime !== lap.duration.elapsedTime - totalPausedTime){
                    validationErrors.push('lap activeTime should be elapsedTime minus totalPauseTime');
                  }

                  // sequential duration validation
                  if(i === 0){
                    if(lap.duration.startTime !== doc.duration.startTime){
                      validationErrors.push('first lap startTime should be same as activity startTime');
                    }
                  }else{
                    var lastLap = doc.laps[i - 1];
                    if(lap.duration.startTime !== lastLap.duration.endTime){
                      validationErrors.push('lap startTime should be same as last lap endTime');
                    }
                  }

                }

                return validationErrors;
              },

              arrayElementsValidator:{
                type: 'object',
                required: true,
                propertyValidators:{
                  lapNumber:{
                    type: 'integer',
                    required: true,
                    minimumValue: 1,
                    immutable: true
                  },
                  activeTime:{
                    type: 'integer',
                    required: true,
                    immutable: true,
                  },
                  duration:{
                    type: 'object',
                    required: true,
                    immutable: true,
                    customValidation: durationValidation,
                    propertyValidators:{
                      elapsedTime:{
                        type: 'integer',
                        required: true,
                        minimumValue: 0,
                        immutable: true
                      },
                      startTime:{
                        type: 'datetime',
                        required: true,
                        minimumValue: 1,
                        immutable: true
                      },
                      endTime:{
                        type: 'datetime',
                        required: true,
                        minimumValue: 1,
                        immutable: true
                      }
                    }
                  },
                  pauses:{
                    type: 'array',
                    required: true,
                    immutable: true,
                    arrayElementsValidator:{
                      type: 'object',
                      required: true,
                      immutable: true,
                      customValidation: durationValidation,
                      propertyValidators:{
                        elapsedTime:{
                          type: 'integer',
                          required: true,
                          immutable: true
                        },
                        startTime:{
                          type: 'datetime',
                          required: true,
                          immutable: true
                        },
                        endTime:{
                          type: 'datetime',
                          required: true,
                          immutable: true,
                        }
                      }
                    }
                  },
                  lapInfo:{
                    type: 'array',
                    required: true,
                    arrayElementsValidator:{
                      type: 'object',
                      required: true,
                      propertyValidators:{
                        key:{
                          type: 'string',
                          mustNotBeEmpty: true,
                          required: true
                        },
                        value:{
                          type: 'object',
                          required: true,
                          propertyValidators:{
                            format: {
                              type: 'enum',
                              required: true,
                              predefinedValues: ['int', 'float', 'double', 'long']
                            },
                            value: {
                              type: 'float',
                              required: true
                            }
                          }
                        }
                      }
                    }
                  },
                  trigger:{
                    type: 'enum',
                    predefinedValues: ['MANUAL', 'DISTANCE', 'TIME', 'WORKOUT', 'LOCATION'],
                    required: false
                  },
                  workout:{
                    type: 'object',
                    required: false,
                    propertyValidators:{
                      primaryTarget:{
                        type: 'object',
                        required: true,
                        propertyValidators: {
                          type: {
                            type: 'string',
                            required: true,
                            mustNotBeEmpty: true
                          },
                          value: {
                            type: 'float',
                            required: true
                          },
                          output: {
                            type: 'float',
                            required: false
                          },
                          minValue: {
                            type: 'float',
                            required: false
                          },
                          maxValue: {
                            type: 'float',
                            required: false
                          },
                          rampType: {
                            type: 'string',
                            required: false
                          },
                        }
                      },
                      secondaryTarget:{
                        type: 'object',
                        required: false,
                        propertyValidators: {
                          type: {
                            type: 'string',
                            required: true,
                            mustNotBeEmpty: true
                          },
                          value: {
                            type: 'float',
                            required: true
                          },
                          output: {
                            type: 'float',
                            required: false
                          },
                          minValue: {
                            type: 'float',
                            required: false
                          },
                          maxValue: {
                            type: 'float',
                            required: false
                          },
                          rampType: {
                            type: 'string',
                            required: false
                          },
                        }
                      }
                    }
                  }
                }
              }
            },
            'fitFile.fit':{
              type: 'object',
              required: false
            },
            sync:{
              type: 'object',
              required: false,
              propertyValidators: {
                description:{
                  type: 'string',
                  required: true
                },
                type:{
                  type: 'string',
                  required: false
                },
                tags:{
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'string',
                    required: true,
                    mustNotBeEmpty: true
                  }
                },
                synced:{
                  type: 'boolean',
                  required: false
                },
                partners:{
                  type: 'array',
                  required: true,
                  arrayElementsValidator:{
                    type: 'object',
                    propertyValidators:{
                      partner:{
                        type: 'string',
                        required: true,
                        mustNotBeEmpty: true
                      },
                      needsUpload:{
                        type: 'boolean',
                        required: true
                      },
                      attempts:{
                        type: 'integer',
                        required: false
                      },
                      uploadedAt:{
                        type: 'datetime',
                        required: false
                      },
                      externalId:{
                        type: 'string',
                        required: false,
                        mustNotBeEmpty: true
                      },
                      externalUrl:{
                        type: 'string',
                        required: false,
                        mustNotBeEmpty: true
                      },
                      error:{
                        type: 'string',
                        required: false,
                        mustNotBeEmpty: true
                      }
                    }
                  }
                }
              }
            },
            pointsOfInterest:{
              type: 'array',
              minimumLength: 0,
              required: false,
              arrayElementsValidator:{
                type: 'object',
                propertyValidators:{
                  type:{
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  },
                  location:{
                    type: 'object',
                    required: true,
                    propertyValidators:{
                      lat:{
                        type: 'float',
                        required: true,
                        minimumValue: -90,
                        maximumValue: 90
                      },
                      lng:{
                        type: 'float',
                        required: true,
                        minimumValue: -180,
                        maximumValue: 180
                      }
                    }
                  },
                  name:{
                    type: 'string',
                    required: false
                  },
                  description:{
                    type: 'string',
                    required: false
                  },
                  bearing:{
                    type: 'float',
                    required: false,
                  },
                  updatedAt: {
                    type: 'datetime',
                    required: false
                  },
                  sourceId: {
                    type: 'string',
                    required: false
                  }
                }
              }
            },
            climbs:{
              type: 'array',
              minimumLength: 0,
              required: false,
              arrayElementsValidator:{
                type: 'object',
                propertyValidators:{
                  startDistance:{
                    type: 'float',
                    required: true
                  },
                  endDistance:{
                    type: 'float',
                    required: true
                  },
                  distance:{
                    type: 'float',
                    required: true
                  }
                }
              }
            },
          },
          allowAttachments: true,
          attachmentConstraints: {
            maximumAttachmentCount: 1,
            supportedExtensions: [ 'fit' ],
            supportedContentTypes: [ 'binary' ]
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        fuze_activity: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('fuze_activity')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            serial: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            'fuzeFile.fuze':{
              type: 'object',
              required: false
            },
          },
          allowAttachments: true,
          attachmentConstraints: {
            maximumAttachmentCount: 1,
            supportedExtensions: [ 'fuze' ],
            supportedContentTypes: [ 'binary' ]
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        share: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('share')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            objectType: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            objectId: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            objectContext: {
              type: 'string',
              required: false
            },
            shareCode: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            shareScope: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        ride_profile: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('ride_profile')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            index: {
              type: 'integer',
              minimumValue: 0,
              required: true,
            },
            alerts: {
              type: 'object',
              required: false
            },
            pages: {
              type: 'array',
              minimumLength: 1,
              required: true,
              arrayElementsValidator:{
                type: 'object',
                required: true,
                propertyValidators: {
                  elements: {
                    type: 'array',
                    required: true,
                    arrayElementsValidator:{
                      type: 'object',
                      required: true,
                      propertyValidators: {
                        elementViewId: {
                          type: 'integer',
                          required: false
                        },
                        id: {
                          type: 'string',
                          mustNotBeEmpty: true,
                          required: true
                        },
                        position: {
                          type: 'object',
                          required: false
                        },
                        size: {
                          type: 'object',
                          required: true
                        }
                      }
                    }
                  },
                  enabled: {
                    type: 'boolean',
                    required: false
                  },
                  grid: {
                    type: 'object',
                    required: false
                  },
                  name: {
                    type: 'string',
                    mustNotBeEmpty: true,
                    required: true
                  },
                  position: {
                    type: 'integer',
                    minimumValue: 0,
                    required: true
                  },
                  pageType: {
                    type: 'integer',
                    required: false
                  },
                  navigationMode: {
                    type: 'integer',
                    required: false
                  },
                  layoutType: {
                    type: 'integer',
                    required: false
                  },
                  id: {
                    type: 'string',
                    required: false,
                  }
                }
              }
            },
            autoPause:{
              type: 'object',
              required: false,
              propertyValidators:{
                enabled:{
                  type: 'boolean',
                  required: true
                },
                threshold:{
                  type: 'float',
                  required: true,
                  minimumValue: 0
                },
                state:{
                  type: 'enum',
                  predefinedValues: [ 'WHEN_STOPPED', 'CUSTOM_SPEED' ],
                  required: true
                }
              }
            },
            navigationSettings:{
              type: 'object',
              required: false
            },
            settings:{
              type: 'object',
              required: false,
              propertyValidators: {
                profileType: {
                  type: 'string',
                  required: true
                },
                defaultBikeId: {
                  type: 'string',
                  required: false
                },
                defaultActivityType: {
                  type: 'string',
                  required: false
                },
                disabledDevices: {
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'string',
                    required: true,
                    mustNotBeEmpty: true
                  }
                },
                tbtCuesEnabled: {
                  type: 'boolean',
                  required: false
                },
                persistentTbtEnabled: {
                  type: 'boolean',
                  required: false
                },
                switchToMapEnabled: {
                  type: 'boolean',
                  required: false
                },
                poiCuesEnabled: {
                  type: 'boolean',
                  required: false
                },
                surfaceCuesEnabled: {
                  type: 'boolean',
                  required: false
                },
                reroutingEnabled: {
                  type: 'boolean',
                  required: false
                },
                phoneNotificationsEnabled: {
                  type: 'boolean',
                  required: false
                },
                phoneNotificationTypes: {
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'string',
                    required: true,
                    mustNotBeEmpty: true
                  }
                },
                workoutTextCuesEnabled: {
                  type: 'boolean',
                  required: false
                },
                liveSegmentsEnabled: {
                  type: 'boolean',
                  required: false
                },
                climberSettingsMode: {
                  type: 'string',
                  required: false
                },
                climberSettingsMinLevel: {
                  type: 'string',
                  required: false
                },
                climberSettingsDrawerElements: {
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'object',
                    required: true,
                    propertyValidators: {
                      elementViewId: {
                        type: 'integer',
                        required: false
                      },
                      id: {
                        type: 'string',
                        mustNotBeEmpty: true,
                        required: true
                      },
                      position: {
                        type: 'object',
                        required: false
                      },
                      size: {
                        type: 'object',
                        required: true
                      }
                    }
                  }
                },
                autoLapMode: {
                  type: 'string',
                  required: false
                },
                autoLapDistance: {
                  type: 'float',
                  required: false
                },
                autoLapTime: {
                  type: 'integer',
                  required: false
                },
                autoLapDropPinOnStart: {
                  type: 'boolean',
                  required: false
                },
                lapTabMetric: {
                  type: 'string',
                  required: false
                },
                autoPause:{
                  type: 'object',
                  required: false
                },
                audioAlertsEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsTbtEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsWorkoutsEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsRadarEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsSLSEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsPhoneEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsLevEnabled: {
                  type: 'boolean',
                  required: false
                },
                audioAlertsRadarSound: {
                  type: 'string',
                  required: false
                },
                enduranceModeEnabled: {
                  type: 'boolean',
                  required: false
                },
                touchLockEnabled: {
                  type: 'boolean',
                  required: false
                },
                bikeBellEnabled: {
                  type: 'boolean',
                  required: false
                },
                inRideDataFieldEditingEnabled: {
                  type: 'boolean',
                  required: false
                },
                routingType:{
                  type: 'enum',
                  predefinedValues: [ 'road', 'gravel', 'mtb' ],
                  required: false
                },
                lapDrawerElements: {
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'object',
                    required: true,
                    propertyValidators: {
                      id: {
                        type: 'string',
                        mustNotBeEmpty: true,
                        required: true
                      },
                      size: {
                        type: 'object',
                        required: true
                      }
                    }
                  }
                },
                workoutDrawerElements: {
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'object',
                    required: true,
                    propertyValidators: {
                      id: {
                        type: 'string',
                        mustNotBeEmpty: true,
                        required: true
                      },
                      size: {
                        type: 'object',
                        required: true
                      }
                    }
                  }
                },
                workoutTargetFieldType: {
                  type: 'string',
                  required: false,
                },
                workoutPowerSmoothingDataTypeId: {
                  type: 'string',
                  required: false,
                },
                customAlertsEnabled: {
                  type: 'boolean',
                  required: false
                },
                customAlerts: {
                  type: 'array',
                  required: false,
                  arrayElementsValidator: {
                    type: 'object',
                    required: true,
                    propertyValidators: {
                      id: {
                        type: 'string',
                        mustNotBeEmpty: true,
                        required: true
                      },
                      message: {
                        type: 'string',
                        required: false
                      },
                      frequency: {
                        type: 'string',
                        mustNotBeEmpty: true,
                        required: true
                      },
                      type: {
                        type: 'string',
                        mustNotBeEmpty: true,
                        required: true
                      },
                      interval: {
                        type: 'float',
                        required: true
                      },
                      eventWhen: {
                        type: 'object',
                        required: false
                      }
                    }
                  }
                },
                customAlertSnooze: {
                  type: 'string',
                  required: false,
                },
                version: {
                  type: 'integer',
                  required: false
                }
              }
            },
            version:{
              type: 'string',
              required: false
            }
          },
          channels: function(doc, oldDoc) {
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        account_info: // Copyright (c) 2022 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              // the id should start with the owner name
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function userProfileId(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if(id !== doc.owner + '.' + 'account_info'){
                  validationErrors.push('_id must be {{owner}}.account_info');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            isEmailVerified: {
              type: 'boolean',
              required: true
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedRoles: defaultAuthorizedRoles
        },
        route_offline_pref: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('route_offline_pref')
            },
            createdAt: {
              type: 'datetime',
              required: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            routeId: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            isOfflineRequired: {
              type: 'boolean',
              required: true,
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles,
        },
        oauth_credentials: // Copyright (c) 2022 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('oauth_credentials')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            partner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            remoteUid: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            accessToken: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            refreshToken: {
              type: 'string',
              required: false
            },
            tokenExpiresAt: {
              type: 'datetime',
              required: false
            },
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        third_party_connection: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('third_party_connection')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            partner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            grantedScopes: {
              type: 'array',
              required: true,
              arrayElementsValidator:{
                type: 'string',
                required: true,
              }
            },
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        link_credentials: // Copyright (c) 2022 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('link_credentials')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            partner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            username: {
              type: 'string',
              required: false,
              mustNotBeEmpty: false
            },
            password: {
              type: 'string',
              required: false,
              mustNotBeEmpty: false
            },
            authToken: {
              type: 'string',
              required: false,
              mustNotBeEmpty: false
            },
            userId: {
              type: 'string',
              required: false,
              mustNotBeEmpty: false
            },
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        facade_credentials: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('facade_credentials'),
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true,
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true,
            },
            partner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true,
            },
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles,
        },
        hx_device: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              // the id should start with the owner name
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('hx_device')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            deviceId: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            loginTimeStamp: {
              type: 'datetime',
              required: false,
              immutable: false
            },
            logoutTimeStamp: {
              type: 'datetime',
              required: false,
              immutable: false
            }
          },
          channels : function(doc,oldDoc) {
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        workout: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('workout')
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: true
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            description: {
              type: 'string',
              required: false
            },
            preRideNotes: {
              type: 'string',
              required: false
            },
            plannedDate: {
              type: 'date',
              required: false
            },
            plannedTss: {
              type: 'float',
              required: false
            },
            completedAt: {
              type: 'datetime',
              required: false
            },
            structure: {
              type: 'array',
              minimumLength: 0,
              required: true,
              arrayElementsValidator: {
                type: 'object',
                propertyValidators: {
                  type: {
                    type: 'string',
                    required: true,
                    mustNotBeEmpty: true
                  },
                  class: {
                    type: 'string',
                    required: false,
                    mustNotBeEmpty: true
                  },
                  name: {
                    type: 'string',
                    required: false
                  },
                  notes: {
                    type: 'string',
                    required: false
                  },
                  length: {
                    type: 'float',
                    required: false
                  },
                  lengthType: {
                    type: 'string',
                    required: false,
                    mustNotBeEmpty: true
                  },
                  lengthOpen: {
                    type: 'boolean',
                    required: false
                  },
                  excludeFromSummary: {
                    type: 'boolean',
                    required: false
                  },
                  primaryTarget: {
                    type: 'object',
                    required: false,
                    propertyValidators: {
                      type: {
                        type: 'string',
                        required: true,
                        mustNotBeEmpty: true
                      },
                      value: {
                        type: 'float',
                        required: true
                      },
                      minValue: {
                        type: 'float',
                        required: false
                      },
                      maxValue: {
                        type: 'float',
                        required: false
                      },
                      rampType: {
                        type: 'string',
                        required: false
                      },
                    }
                  },
                  secondaryTarget: {
                    type: 'object',
                    required: false,
                    propertyValidators: {
                      type: {
                        type: 'string',
                        required: true,
                        mustNotBeEmpty: true
                      },
                      value: {
                        type: 'float',
                        required: true
                      },
                      minValue: {
                        type: 'float',
                        required: false
                      },
                      maxValue: {
                        type: 'float',
                        required: false
                      },
                      rampType: {
                        type: 'string',
                        required: false
                      },
                    }
                  },
                  repeat: {
                    type: 'integer',
                    minimumValue: 0,
                    required: false
                  },
                  steps: {
                    type: 'array',
                    minimumLength: 1,
                    required: false,
                    arrayElementsValidator: {
                      type: 'object',
                      propertyValidators: {
                        type: {
                          type: 'string',
                          required: true,
                          mustNotBeEmpty: true
                        },
                        class: {
                          type: 'string',
                          required: false,
                          mustNotBeEmpty: true
                        },
                        name: {
                          type: 'string',
                          required: false
                        },
                        notes: {
                          type: 'string',
                          required: false
                        },
                        length: {
                          type: 'float',
                          required: true
                        },
                        lengthType: {
                          type: 'string',
                          required: true,
                          mustNotBeEmpty: true
                        },
                        lengthOpen: {
                          type: 'boolean',
                          required: false
                        },
                        excludeFromSummary: {
                          type: 'boolean',
                          required: false
                        },
                        primaryTarget: {
                          type: 'object',
                          required: false,
                          propertyValidators: {
                            type: {
                              type: 'string',
                              required: true,
                              mustNotBeEmpty: true
                            },
                            value: {
                              type: 'float',
                              required: true
                            },
                            minValue: {
                              type: 'float',
                              required: false
                            },
                            maxValue: {
                              type: 'float',
                              required: false
                            },
                            rampType: {
                              type: 'string',
                              required: false
                            },
                          }
                        },
                        secondaryTarget: {
                          type: 'object',
                          required: false,
                          propertyValidators: {
                            type: {
                              type: 'string',
                              required: true,
                              mustNotBeEmpty: true
                            },
                            value: {
                              type: 'float',
                              required: true
                            },
                            minValue: {
                              type: 'float',
                              required: false
                            },
                            maxValue: {
                              type: 'float',
                              required: false
                            },
                            rampType: {
                              type: 'string',
                              required: false
                            },
                          }
                        },
                      }
                    }
                  }
                }
              }
            },
            source: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            sourceId: {
              type: 'string',
              mustNotBeEmpty: true,
              immutableWhenSet: true
            },
            thresholdHr: {
              type: 'float',
              required: false
            },
            thresholdSpeed: {
              type: 'float',
              required: false
            },
          },
          allowAttachments: true,
          attachmentConstraints: {
            maximumAttachmentCount: 1,
            supportedContentTypes: [ 'application/zwo+xml', 'application/vnd.ant.fit' ]
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        offline_regions: // Copyright (c) 2025 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              // the id should start with the owner name
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function userProfileId(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if(id !== doc.owner + '.' + 'offline_regions'){
                  validationErrors.push('_id must be {{owner}}.offline_regions');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: false
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            regionIds: {
              type: 'array',
              required: true,
              arrayElementsValidator:{
                type: 'string',
                required: true,
              }
            },
            heatmapIds: {
              type: 'array',
              required: false,
              arrayElementsValidator:{
                type: 'string',
                required: true,
              }
            },
            contourIds: {
              type: 'array',
              required: false,
              arrayElementsValidator:{
                type: 'string',
                required: true,
              }
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
        device_auth: // Copyright (c) 2022 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: function deviceAuthid(doc, oldDoc, currentItem, validationItemStack) {
                var id = currentItem.itemValue;
                var validationErrors = [];
                if(id.substring(0, 12) !=='device_auth.') {
                  validationErrors.push('_id must start with "device_auth."');
                }
                return validationErrors;
              }
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            code: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            secret: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            token: {
              type: 'object',
              required: false
            },
            visited: {
              type: 'boolean',
              required: true,
            },
            signedIn: {
              type: 'boolean',
              required: true,
            },
            seedData: {
              type: 'object',
              required: true
            }
          }
        },
        collection: // Copyright (c) 2024 Hammerhead Navigation Inc.

        {
          typeFilter: simpleTypeFilter,
          propertyValidators: {
            _id: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              customValidation: idPrefixValidation('collection')
            },
            owner: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true,
              immutable: true
            },
            createdAt: {
              type: 'datetime',
              required: true,
              immutable: true
            },
            updatedAt: {
              type: 'datetime',
              required: false
            },
            name: {
              type: 'string',
              required: true,
              mustNotBeEmpty: true
            },
            description: {
              type: 'string',
              required: false,
              mustNotBeEmpty: false
            }
          },
          channels: function(doc, oldDoc){
            return {
              view: [docChannel(doc, oldDoc), ownerChannel(doc, oldDoc)]
            };
          },
          authorizedUsers: defaultAuthorizedUsers,
          authorizedRoles: defaultAuthorizedRoles
        },
      };
    };

  var docDefinitions;
  if (typeof rawDocDefinitions === 'function') {
    docDefinitions = rawDocDefinitions();
  } else {
    docDefinitions = rawDocDefinitions;
  }

  function getDocumentType(doc, oldDoc) {
    var effectiveOldDoc = getEffectiveOldDoc(oldDoc);

    for (var docType in docDefinitions) {
      var docDefn = docDefinitions[docType];
      if (docDefn.typeFilter(doc, effectiveOldDoc, docType)) {
        return docType;
      }
    }

    // The document type does not exist
    return null;
  }

  // Now put the pieces together
  var theDocType = getDocumentType(doc, oldDoc);

  if (isValueNullOrUndefined(theDocType)) {
    if (isDocumentMissingOrDeleted(oldDoc) && isDocumentMissingOrDeleted(doc)) {
      // Attempting to delete a document that does not exist. This may occur when bucket access/sharing
      // (https://developer.couchbase.com/documentation/mobile/current/guides/sync-gateway/shared-bucket-access.html)
      // is enabled and the document was deleted via the Couchbase SDK. Skip everything else and simply assign the
      // public channel
      // (https://developer.couchbase.com/documentation/mobile/current/guides/sync-gateway/channels/index.html#special-channels)
      // to the document so that users will get a 404 Not Found if they attempt to fetch (i.e. "view") the deleted
      // document rather than a 403 Forbidden.
      requireAccess('!');
      channel('!');

      return;
    } else {
      throw({ forbidden: 'Unknown document type' });
    }
  }

  var theDocDefinition = docDefinitions[theDocType];

  var customActionMetadata = {
    documentTypeId: theDocType,
    documentDefinition: theDocDefinition
  };

  if (theDocDefinition.customActions && typeof(theDocDefinition.customActions.onTypeIdentificationSucceeded) === 'function') {
    theDocDefinition.customActions.onTypeIdentificationSucceeded(doc, oldDoc, customActionMetadata);
  }

  customActionMetadata.authorization = authorizationModule.authorize(doc, oldDoc, theDocDefinition);

  if (theDocDefinition.customActions && typeof(theDocDefinition.customActions.onAuthorizationSucceeded) === 'function') {
    theDocDefinition.customActions.onAuthorizationSucceeded(doc, oldDoc, customActionMetadata);
  }

  validationModule.validateDoc(doc, oldDoc, theDocDefinition, theDocType);

  if (theDocDefinition.customActions && typeof(theDocDefinition.customActions.onValidationSucceeded) === 'function') {
    theDocDefinition.customActions.onValidationSucceeded(doc, oldDoc, customActionMetadata);
  }

  if (theDocDefinition.accessAssignments && theDocDefinition.accessAssignments.length > 0) {
    customActionMetadata.accessAssignments = accessAssignmentModule.assignUserAccess(doc, oldDoc, theDocDefinition.accessAssignments);

    if (theDocDefinition.customActions && typeof(theDocDefinition.customActions.onAccessAssignmentsSucceeded) === 'function') {
      theDocDefinition.customActions.onAccessAssignmentsSucceeded(doc, oldDoc, customActionMetadata);
    }
  }

  // Getting here means the document revision is authorized and valid, and the appropriate channel(s) should now be assigned
  var allDocChannels = authorizationModule.getAllDocChannels(doc, oldDoc, theDocDefinition);
  channel(allDocChannels);
  customActionMetadata.documentChannels = allDocChannels;

  if (theDocDefinition.customActions && typeof(theDocDefinition.customActions.onDocumentChannelAssignmentSucceeded) === 'function') {
    theDocDefinition.customActions.onDocumentChannelAssignmentSucceeded(doc, oldDoc, customActionMetadata);
  }
}`

var tesDoc = `{"channels": ["%d"],
"testDoc": [
  {
    "_id": "666acdcd7bc4dbb3289a2b0a",
    "index": 0,
    "guid": "d8aca2f5-daac-47aa-8d8d-7f6692d5097e",
    "isActive": false,
    "balance": "$1,367.98",
    "picture": "http://placehold.it/32x32",
    "age": 26,
    "eyeColor": "blue",
    "name": "Aurora Wheeler",
    "gender": "female",
    "company": "BEZAL",
    "email": "aurorawheeler@bezal.com",
    "phone": "+1 (972) 570-2140",
    "address": "162 Newel Street, Kula, Georgia, 8484",
    "about": "Dolore nisi esse sit ullamco tempor do exercitation nisi mollit. Cupidatat incididunt consequat nostrud Lorem Lorem dolore irure. Veniam labore laborum et fugiat officia ad nostrud. Commodo quis qui cillum elit pariatur laborum duis veniam minim aliquip esse do et quis. Aliqua proident velit adipisicing laboris mollit qui enim Lorem ad commodo nostrud irure pariatur. Fugiat incididunt tempor id quis consequat tempor exercitation est eu officia cupidatat consectetur cupidatat cillum. Consectetur duis consequat cupidatat eu ex commodo consectetur duis reprehenderit sunt deserunt sint dolore qui."
  }
]}`

func (rt *RestTester) createUsers(num int, wg *sync.WaitGroup) {
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("user%d", num)
		rt.CreateUser(name, []string{fmt.Sprint(num)})
		resp := rt.SendUserRequest("GET", "/{{.keyspace}}/_changes", "", name)
		RequireStatus(rt.TB(), resp, http.StatusOK)
		num++
	}
	wg.Done()
}

func (rt *RestTester) doEverything(num int, wg *sync.WaitGroup, b *testing.B) {

	for i := 0; i < 1000; i++ {
		//id := RandomString(40)
		id := uuid.NewString()
		name := fmt.Sprintf("user%d", num)
		rt.CreateUser(name, []string{"jdoe.activity." + id + "-READ"})
		response := rt.SendUserRequest("GET", "/{{.keyspace}}/_changes", "", name)
		RequireStatus(rt.TB(), response, http.StatusOK)
		//resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+id, fmt.Sprintf(tesDoc, num))
		resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/jdoe.activity."+id, fmt.Sprintf(exampleDoc, id))
		if resp.Code == http.StatusConflict {
			// try again
			//id = RandomString(60)
			id = uuid.NewString()
			//resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+id, fmt.Sprintf(tesDoc, num))
			resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/jdoe.activity."+id, fmt.Sprintf(exampleDoc, id))
			if resp.Code == http.StatusConflict {
				continue
			}
			continue
		}
		rt.WaitForPendingChanges()
		version := DocVersionFromPutResponse(rt.TB(), resp)
		rt.storeAttachment("jdoe.activity."+id, version, "myAttachment", string(base.FastRandBytes(rt.TB(), 500000)))
		resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/jdoe.activity."+id+"/myAttachment", "")
		RequireStatus(rt.TB(), resp, http.StatusOK)
		rt.WaitForPendingChanges()
		//if i == 500 {
		//	WriteHeapProfile(b, "./heapMidLoop"+fmt.Sprint(num)+".pprof")
		//}
		num++
	}
	wg.Done()

}

func (rt *RestTester) createDocs(num int, wg *sync.WaitGroup) {
	for i := 0; i < 1000; i++ {
		id := RandomString(40)
		//id := uuid.NewString()
		resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+id, fmt.Sprintf(tesDoc, num))
		//resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+id, fmt.Sprintf(exampleDoc, id))
		if resp.Code == http.StatusConflict {
			// try again
			id = RandomString(60)
			//id = uuid.NewString()
			resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+id, fmt.Sprintf(tesDoc, num))
			//resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+id, fmt.Sprintf(exampleDoc, id))
			if resp.Code == http.StatusConflict {
				continue
			}
			continue
		}
		rt.WaitForPendingChanges()
		//version := DocVersionFromPutResponse(rt.TB(), resp)
		//rt.storeAttachment(id, version, "myAttachment", string(base.FastRandBytes(rt.TB(), 500000)))
		//resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+id+"/myAttachment", "")
		//RequireStatus(rt.TB(), resp, http.StatusOK)
		num++
	}
	wg.Done()
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func BenchmarkCaching(b *testing.B) {

	handler := func(w http.ResponseWriter, r *http.Request) {
		out, err := io.ReadAll(r.Body)
		assert.NoError(b, err)
		err = r.Body.Close()
		assert.NoError(b, err)

		fmt.Println(string(out))
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	rt := NewRestTester(b, &RestTesterConfig{
		//SyncFn: channels.DocChannelsSyncFunction,
		SyncFn:     syncFunc,
		AutoImport: base.Ptr(true),
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
				CacheConfig: &CacheConfig{
					ChannelCacheConfig: &ChannelCacheConfig{
						MaxNumber: base.Ptr(300000),
					},
				},
				EventHandlers: &EventHandlerConfig{
					DocumentChanged: []*EventConfig{
						{Url: s.URL, Filter: "function(doc){return true;}", HandlerType: "webhook"},
					},
				},
			},
		},
	})
	defer rt.Close()

	count := 0

	_, _ = rt.GetSingleTestDatabaseCollectionWithUser()

	//var startWg sync.WaitGroup
	var writeWg sync.WaitGroup

	//for i := 0; i < 50; i++ {
	//	startWg.Add(1)
	//	go rt.createUsers(count, &startWg)
	//	count = count + 1000
	//}
	//
	//startWg.Wait()
	//count = 0
	//for b.Loop() {
	//	count = 0
	//	for i := 0; i < 50; i++ {
	//		writeWg.Add(1)
	//		go rt.createDocs(count, &writeWg)
	//		count = count + 1000
	//	}
	//	writeWg.Wait()
	//}

	for b.Loop() {
		for i := 0; i < 50; i++ {
			writeWg.Add(1)
			go rt.doEverything(count, &writeWg, b)
			count = count + 1000
		}
		writeWg.Wait()
		fmt.Println("chans", rt.GetDatabase().DbStats.Cache().ChannelCacheNumChannels.Value())
		WriteHeapProfile(b, "./heapLoopKVPool1.pprof")
	}
	WriteHeapProfile(b, "./heapAfterLoopKVPool1.pprof")
}

func WriteHeapProfile(t *testing.B, name string) {
	f, err := os.Create(name)
	if err != nil {
		t.Fatalf("could not create heap profile: %v", err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatalf("could not write heap profile: %v", err)
	}
	t.Logf("heap profile written to %s", name)
}

func TestChanCacheActiveRevsStat(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})
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

	rt.WaitForPendingChanges()

	changes := rt.PostChangesAdmin("/{{.keyspace}}/_changes?active_only=true&include_docs=true&filter=sync_gateway/bychannel&channels=a&feed=normal&since=0&heartbeat=0&timeout=300000", "{}")
	assert.Equal(t, 2, len(changes.Results))

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc?new_edits=true&rev="+rev1, `{"value":"a value", "channels":[]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc2?new_edits=true&rev="+rev2, `{"value":"a value", "channels":[]}`)
	RequireStatus(t, response, http.StatusCreated)

	rt.WaitForPendingChanges()

	assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Cache().ChannelCacheRevsActive.Value()))

}

func TestGetRawDocumentErrors(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// not found
	response := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+t.Name(), ``)
	assert.Equal(t, http.StatusNotFound, response.Code)

	// invalid sync data
	_, err := rt.GetSingleDataStore().WriteWithXattrs(t.Context(), t.Name()+"invalsyncdata", 0, 0, []byte(`{"doc":"body"}`),
		map[string][]byte{base.SyncXattrName: []byte(`"invalid sync data"`)}, nil, nil)
	require.NoError(t, err)
	response = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+t.Name()+"invalsyncdata", ``)
	assert.Equal(t, http.StatusOK, response.Code)
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
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// only test endpoints that do not require time based sampling to make this test fast, net/http/pprof package is itself tested
	tests := []struct {
		inputProfile  string
		inputResource string
	}{
		{
			inputProfile:  "heap",
			inputResource: "/_debug/pprof/heap",
		},
		{
			inputProfile:  "threadcreate",
			inputResource: "/_debug/pprof/threadcreate",
		},
		{
			inputProfile:  "goroutine",
			inputResource: "/_debug/pprof/goroutine?debug=0&gc=1",
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

	const docID = "doc"

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docID, "{}")
	RequireStatus(t, response, http.StatusCreated)

	body := rt.GetDocBody(docID)
	assert.NotEmpty(t, body[db.BodyId])
	assert.NotEmpty(t, body[db.BodyRev])

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docID+"?rev="+body[db.BodyRev].(string), `{"val": "newval"}`)
	RequireStatus(t, response, http.StatusCreated)

	body = rt.GetDocBody(docID)
	assert.NotEmpty(t, body[db.BodyId])
	assert.NotEmpty(t, body[db.BodyRev])
	assert.Equal(t, "newval", body["val"].(string))
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
	insCas, err := bucket.DefaultDataStore().WriteCas(t.Name(), 0, 0, value, 0)
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
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, "doc")
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
	syncData, err = collection.GetDocSyncData(ctx, "doc")
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
	syncData, err = collection.GetDocSyncData(ctx, "doc")
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
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, "doc1")
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

func TestMetricsHandler(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SkipPrometheusStatsRegistration = false
	defer func() {
		base.SkipPrometheusStatsRegistration = true
	}()

	// Create and remove a databaseion
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

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	syncData, err := collection.GetDocSyncData(ctx, "doc")
	assert.NoError(t, err)

	require.Len(t, syncData.ChannelSetHistory, db.DocumentHistoryMaxEntriesPerChannel)
	assert.Equal(t, "a", syncData.ChannelSetHistory[0].Name)
	assert.Equal(t, uint64(1), syncData.ChannelSetHistory[0].Start)
	assert.Equal(t, uint64(12), syncData.ChannelSetHistory[0].End)
}

func TestRejectWritesWhenInBroadcastSlowMode(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			CacheConfig: &CacheConfig{
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxWaitPending: base.Ptr(uint32(100)),
				},
			},
			Unsupported: &db.UnsupportedOptions{
				RejectWritesWithSkippedSequences: true,
			},
		}},
	})
	defer rt.Close()

	docID := t.Name() + "_doc1"
	ctx := base.TestCtx(t)

	docVrs := rt.PutDoc(docID, `{"test": "value"}`)

	// alter sync data of this doc to artificially create skipped sequences
	ds := rt.GetSingleDataStore()
	xattrs, cas, err := ds.GetXattrs(ctx, docID, []string{base.SyncXattrName})
	require.NoError(t, err)

	var retrievedXattr map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
	retrievedXattr["sequence"] = uint64(20)
	newXattrVal := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
	}

	_, err = ds.UpdateXattrs(ctx, docID, 0, cas, newXattrVal, nil)
	require.NoError(t, err)

	// wait for value to move from pending to cache and skipped list to fill
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rt.GetDatabase().UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(1), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
		assert.True(c, rt.GetDatabase().BroadcastSlowMode.Load())
	}, time.Second*10, time.Millisecond*100)

	// try to update the doc and expect a 503 Service Unavailable
	resp := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, docVrs), `{"test": "new value"}`)
	RequireStatus(t, resp, http.StatusServiceUnavailable)
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.DatabaseStats.NumDocWritesRejected.Value())

	// post doc endpoint should also reject writes
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/", `{"_id": "foo", "key": "val"}`)
	RequireStatus(t, resp, http.StatusServiceUnavailable)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.DatabaseStats.NumDocWritesRejected.Value())

	// try to delete the doc and expect a 503 Service Unavailable
	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, docVrs), "")
	RequireStatus(t, resp, http.StatusServiceUnavailable)
	assert.Equal(t, int64(3), rt.GetDatabase().DbStats.DatabaseStats.NumDocWritesRejected.Value())

	// try bulk docs endpoint
	input := `{"docs": [{"_id": "bulk1", "new": "doc"}, {"_id": "bulk2","new": "doc"}]}`
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, resp, http.StatusServiceUnavailable)
	assert.Equal(t, int64(4), rt.GetDatabase().DbStats.DatabaseStats.NumDocWritesRejected.Value())
}

func TestNullDocHandlingForMutable1xBody(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	documentRev := db.DocumentRevision{DocID: "doc1", BodyBytes: []byte("null")}

	body, err := documentRev.Mutable1xBody(ctx, collection, nil, nil, false, false)
	require.Error(t, err)
	require.Nil(t, body)
	assert.Contains(t, err.Error(), "null doc body for doc")

	bodyBytes, err := documentRev.Inject1xBodyProperties(rt.Context(), collection, nil, nil, false)
	require.Error(t, err)
	require.Nil(t, bodyBytes)
	assert.Contains(t, err.Error(), "b is not a JSON object")
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
			rt.WaitForDBOnline()

			dbConfig.EnableXattrs = base.Ptr(false)

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

// TestCreateDBWithXattrsDisabled:
//   - Test that you cannot create a database with xattrs disabled
//   - Test that an existing database cannot be loaded with xattrs disabled after upgrade
//   - Assert error code is returned and response contains error string
func TestCreateDBWithXattrsDisabled(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()
	const (
		dbName  = "db1"
		errResp = "sync gateway requires enable_shared_bucket_access=true"
	)

	dbConfig := rt.NewDbConfig()
	dbConfig.EnableXattrs = base.Ptr(false)

	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusInternalServerError)
	assert.Contains(t, resp.Body.String(), errResp)

	dbConfig = rt.NewDbConfig()
	resp = rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	rt.RestTesterServerContext.dbConfigs[dbName].DatabaseConfig.EnableXattrs = base.Ptr(false)

	_, err := rt.RestTesterServerContext.ReloadDatabase(t.Context(), dbName, false)
	assert.Error(t, err, errResp)
}

// TestPvDeltaReadAndWrite:
//   - Write a doc from another hlv aware peer to the bucket
//   - Force import of this doc, then update this doc via rest tester source
//   - Assert that the document hlv is as expected
//   - Update the doc from a new hlv aware peer and force the import of this new write
//   - Assert that the new hlv is as expected, testing that the hlv went through transformation to the persisted delta
//     version and back to the in memory version as expected
func TestPvDeltaReadAndWrite(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	testSource := rt.GetDatabase().EncodedSourceID

	const docID = "doc1"
	otherSource := "otherSource"
	hlvHelper := db.NewHLVAgent(t, rt.GetSingleDataStore(), otherSource, "_vv")
	existingHLVKey := docID
	cas := hlvHelper.InsertWithHLV(ctx, existingHLVKey)
	casV1 := cas
	encodedSourceV1 := db.EncodeSource(otherSource)

	// force import of this write
	version1, _ := rt.GetDoc(docID)

	// update the above doc, this should push CV to PV and adds a new CV
	version2 := rt.UpdateDoc(docID, version1, `{"new": "update!"}`)
	newDoc, _, err := collection.GetDocWithXattrs(ctx, existingHLVKey, db.DocUnmarshalAll)
	require.NoError(t, err)
	casV2 := newDoc.Cas
	encodedSourceV2 := testSource

	// assert that we have a prev CV drop to pv and a new CV pair, assert pv values are as expected after delta conversions
	assert.Equal(t, testSource, newDoc.HLV.SourceID)
	assert.Equal(t, version2.CV.Value, newDoc.HLV.Version)
	assert.Len(t, newDoc.HLV.PreviousVersions, 1)
	assert.Equal(t, casV1, newDoc.HLV.PreviousVersions[encodedSourceV1])

	otherSource = "diffSource"
	hlvHelper = db.NewHLVAgent(t, rt.GetSingleDataStore(), otherSource, "_vv")
	cas = hlvHelper.UpdateWithHLV(ctx, existingHLVKey, newDoc.Cas, newDoc.HLV)
	encodedSourceV3 := db.EncodeSource(otherSource)
	casV3 := cas

	// import and get raw doc
	_, _ = rt.GetDoc(docID)
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	// assert that we have two entries in previous versions, and they are correctly converted from deltas back to full value
	assert.Equal(t, encodedSourceV3, bucketDoc.HLV.SourceID)
	assert.Equal(t, casV3, bucketDoc.HLV.Version)
	assert.Len(t, bucketDoc.HLV.PreviousVersions, 2)
	assert.Equal(t, casV1, bucketDoc.HLV.PreviousVersions[encodedSourceV1])
	assert.Equal(t, casV2, bucketDoc.HLV.PreviousVersions[encodedSourceV2])
}

// TestPutDocUpdateVersionVector:
//   - Put a doc and assert that the versions and the source for the hlv is correctly updated
//   - Update that doc and assert HLV has also been updated
//   - Delete the doc and assert that the HLV has been updated in deletion event
func TestPutDocUpdateVersionVector(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	bucketUUID := rt.GetDatabase().EncodedSourceID

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"key": "value"}`)
	RequireStatus(t, resp, http.StatusCreated)

	collection, _ := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(base.TestCtx(t), "doc1", db.DocUnmarshalSync)
	assert.NoError(t, err)

	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)

	// Put a new revision of this doc and assert that the version vector SourceID and Version is updated
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1?rev="+doc.SyncData.GetRevTreeID(), `{"key1": "value1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	doc, err = collection.GetDocument(base.TestCtx(t), "doc1", db.DocUnmarshalSync)
	assert.NoError(t, err)

	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)

	// Delete doc and assert that the version vector SourceID and Version is updated
	resp = rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/doc1?rev="+doc.SyncData.GetRevTreeID(), "")
	RequireStatus(t, resp, http.StatusOK)

	doc, err = collection.GetDocument(base.TestCtx(t), "doc1", db.DocUnmarshalSync)
	assert.NoError(t, err)

	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)
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

	bucketUUID := rt.GetDatabase().EncodedSourceID

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"type": "mobile"}`)
	RequireStatus(t, resp, http.StatusCreated)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, "doc1", db.DocUnmarshalSync)
	assert.NoError(t, err)

	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)

	// Put a doc that will be rejected by the import filter on the attempt to perform on demand import for write
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc2", `{"type": "not-mobile"}`)
	RequireStatus(t, resp, http.StatusCreated)

	// assert that the hlv is correctly updated and in tact after the import was cancelled on the doc
	doc, err = collection.GetDocument(ctx, "doc2", db.DocUnmarshalSync)
	require.NoError(t, err)

	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)
}

func TestTombstoneCompactionAPI(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// force compaction
	rt.GetDatabase().Options.TestPurgeIntervalOverride = base.Ptr(time.Duration(0))

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
	firstStartTimeStat := rt.GetDatabase().DbStats.Database().CompactionTombstoneStartTime.Value()
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
	assert.True(t, rt.GetDatabase().DbStats.Database().CompactionTombstoneStartTime.Value() > firstStartTimeStat)

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
		{"Diagnostic", rt.SendDiagnosticRequest},
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

// TestBufferFlush will test for http.ResponseWriter implements Flusher interface
func TestBufferFlush(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})
	defer rt.Close()
	ctx := base.TestCtx(t)

	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

	// Create a test user
	user, err := a.NewUser("foo", "letmein", channels.BaseSetOf(t, "foo"))
	require.NoError(t, err)
	require.NoError(t, a.Save(user))

	var wg sync.WaitGroup
	var resp *TestResponse
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp = rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes?feed=continuous&since=0&timeout=500&include_docs=true", "", "foo")
		RequireStatus(t, resp, http.StatusOK)
	}()
	wg.Wait()

	// assert that the response is a flushed response
	assert.True(t, resp.Flushed)
}

func TestPublicAllDocsApiStats(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})
	defer rt.Close()

	rt.CreateUser("user1", []string{"ABC"})

	rt.PutDoc("doc1", `{"channels": ["ABC"]}`)
	rt.PutDoc("doc2", `{"channels": ["DEF"]}`)

	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.DatabaseStats.NumPublicAllDocsRequests.Value())
	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPreFilterPublicAllDocs.Value())
	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPostFilterPublicAllDocs.Value())

	resp := rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_all_docs", "", "user1")
	RequireStatus(t, resp, http.StatusOK)
	// two docs in the db but user only has access to one of them, ensure stats are correct in that assessment
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.DatabaseStats.NumPublicAllDocsRequests.Value())
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPreFilterPublicAllDocs.Value())
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPostFilterPublicAllDocs.Value())

	// add a doc ID that doesn't exist into the mix here
	body := `{"keys": ["doc2", "doc1", "fakeDoc"]}`
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_all_docs", body, "user1")
	RequireStatus(t, resp, http.StatusOK)
	// docs that don't exist or the user doesn't have access to are returned in result but with errors when a key filter applies, so these
	// should still count in post filter count as they're still returned to the user in some capacity
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.DatabaseStats.NumPublicAllDocsRequests.Value())
	assert.Equal(t, int64(5), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPreFilterPublicAllDocs.Value())
	assert.Equal(t, int64(4), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPostFilterPublicAllDocs.Value())

	// try admin port _all_docs call and assert that stats are unchanged
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_all_docs", "")
	RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.DatabaseStats.NumPublicAllDocsRequests.Value())
	assert.Equal(t, int64(5), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPreFilterPublicAllDocs.Value())
	assert.Equal(t, int64(4), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPostFilterPublicAllDocs.Value())

	// try make error case request and assert that the number of request count increments but other stats remain unchanged
	body = `{"keys": [1]}`
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_all_docs", body, "user1")
	RequireStatus(t, resp, http.StatusBadRequest)
	assert.Equal(t, int64(3), rt.GetDatabase().DbStats.DatabaseStats.NumPublicAllDocsRequests.Value())
	assert.Equal(t, int64(5), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPreFilterPublicAllDocs.Value())
	assert.Equal(t, int64(4), rt.GetDatabase().DbStats.DatabaseStats.NumDocsPostFilterPublicAllDocs.Value())
}

func TestSilentHandlerLoggingInTrace(t *testing.T) {
	// must have debug to assert that the endpoints below don't show in debug logs
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyHTTPResp)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	base.AssertLogNotContains(t, "_ping", func() {
		resp := rt.SendRequest(http.MethodGet, "/_ping", "")
		RequireStatus(t, resp, http.StatusOK)
	})

	base.AssertLogNotContains(t, "/metrics", func() {
		headers := map[string]string{
			"Authorization": GetBasicAuthHeader(t, base.TestClusterUsername(), base.TestClusterPassword()),
		}
		resp := rt.SendMetricsRequestWithHeaders(http.MethodGet, "/metrics", "", headers)
		RequireStatus(t, resp, http.StatusOK)
	})

	base.AssertLogNotContains(t, "/_metrics", func() {
		headers := map[string]string{
			"Authorization": GetBasicAuthHeader(t, base.TestClusterUsername(), base.TestClusterPassword()),
		}
		resp := rt.SendMetricsRequestWithHeaders(http.MethodGet, "/_metrics", "", headers)
		RequireStatus(t, resp, http.StatusOK)
	})

	base.AssertLogNotContains(t, "/_expvar", func() {
		headers := map[string]string{
			"Authorization": GetBasicAuthHeader(t, base.TestClusterUsername(), base.TestClusterPassword()),
		}
		resp := rt.SendMetricsRequestWithHeaders(http.MethodGet, "/_expvar", "", headers)
		RequireStatus(t, resp, http.StatusOK)
	})

	base.AssertLogNotContains(t, "/_expvar", func() {
		resp := rt.SendAdminRequest(http.MethodGet, "/_expvar", "")
		RequireStatus(t, resp, http.StatusOK)
	})
}

// TestHLVUpdateOnRevReplicatorPut:
//   - Test that the HLV is updated correctly when a document is put via the rev replicator running in rev mode
func TestHLVUpdateOnRevReplicatorPut(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	docID := t.Name()
	newVersion := DocVersion{
		RevTreeID: "1-abc",
	}
	_ = rt.PutNewEditsFalse(docID, newVersion, EmptyDocVersion(), `{"some":"body"}`)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalSync)
	require.NoError(t, err)

	assert.Equal(t, rt.GetDatabase().EncodedSourceID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)
}

func TestDocCRUDWithCV(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	const docID = "doc1"
	createVersion := rt.PutDoc(docID, `{"create":true}`)

	getDocVersion, _ := rt.GetDoc(docID)
	require.Equal(t, createVersion, getDocVersion)

	revIDGen := func(v DocVersion) int {
		gen, _ := db.ParseRevID(base.TestCtx(t), v.RevTreeID)
		return gen
	}

	updateVersion := rt.UpdateDoc(docID, createVersion, `{"update":true}`)
	require.NotEqual(t, createVersion, updateVersion)
	assert.Greaterf(t, updateVersion.CV.Value, createVersion.CV.Value, "Expected CV Value to be bumped on update")
	assert.Greaterf(t, revIDGen(updateVersion), revIDGen(createVersion), "Expected revision generation to be bumped on update")

	getDocVersion, _ = rt.GetDoc(docID)
	require.Equal(t, updateVersion, getDocVersion)

	// fetch by CV (using the first create version to test cache retrieval)
	resp := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, url.QueryEscape(createVersion.CV.String())), "")
	RequireStatus(t, resp, http.StatusOK)
	assert.NotContains(t, resp.BodyString(), `"update":true`)
	assert.Contains(t, resp.BodyString(), `"create":true`)
	assert.Contains(t, resp.BodyString(), `"_cv":"`+createVersion.CV.String()+`"`)
	assert.Contains(t, resp.BodyString(), `"_rev":"`+createVersion.RevTreeID+`"`)

	// fetch by CV - updated version
	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, url.QueryEscape(updateVersion.CV.String())), "")
	RequireStatus(t, resp, http.StatusOK)
	assert.NotContains(t, resp.BodyString(), `"create":true`)
	assert.Contains(t, resp.BodyString(), `"update":true`)
	assert.Contains(t, resp.BodyString(), `"_cv":"`+updateVersion.CV.String()+`"`)
	assert.Contains(t, resp.BodyString(), `"_rev":"`+updateVersion.RevTreeID+`"`)

	deleteVersion := rt.DeleteDoc(docID, updateVersion)
	require.NotEqual(t, updateVersion, deleteVersion)
	assert.Greaterf(t, deleteVersion.CV.Value, updateVersion.CV.Value, "Expected CV Value to be bumped on delete")
	assert.Greaterf(t, revIDGen(deleteVersion), revIDGen(updateVersion), "Expected revision generation to be bumped on delete")
}

// TestAllowConflictsConfig verifies that the database configuration does not allow
// the `allow_conflicts` property to be set to true. `allow_conflicts` is no longer
// supported in SGW 4.0. This test ensures that a config loaded will add the
// database to invalid configs. The config will be fixed when a database is created
// without `allow_conflicts`
func TestAllowConflictsConfig(t *testing.T) {

	ctx := t.Context()

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	const (
		dbName    = "db1"
		errResp   = "allow_conflicts cannot be set to true"
		allDBsURL = "/_all_dbs?verbose=true"
	)

	dbConfig := rt.NewDbConfig()
	dbConfig.AllowConflicts = base.Ptr(true)

	resp := rt.CreateDatabase(dbName, dbConfig)

	RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), errResp)

	dbConfig = rt.NewDbConfig()
	dbConfig.Name = dbName

	rt.CreateDatabase(dbName, dbConfig)

	// Send an admin request to retrieve all databases and verify the response.
	resp = rt.SendAdminRequest(http.MethodGet, allDBsURL, "")
	RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, fmt.Sprintf(`[{"db_name":"%s","bucket":"%s","state":"Online"}]`, rt.GetDatabase().Name, rt.GetDatabase().Bucket.GetName()), resp.Body.String())
	bucketName := rt.GetDatabase().Bucket.GetName()

	// Attempt to set the `AllowConflicts` property to true in the database configuration.
	rt.RestTesterServerContext.dbConfigs[dbName].DatabaseConfig.DbConfig.AllowConflicts = base.Ptr(true)

	// Reload the database configuration and verify that an error is returned.
	_, err := rt.RestTesterServerContext.ReloadDatabase(ctx, dbName, false)
	require.Error(t, err)

	// Verify that the database is now in an offline state with the appropriate error message.
	resp = rt.SendAdminRequest(http.MethodGet, allDBsURL, "")
	RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, fmt.Sprintf(`[{"db_name":"%s","bucket":"%s","state":"Offline","database_error":{"error_message":"Allow conflicts is set to true","error_code":9}}]`, dbName, bucketName), resp.Body.String())

	// Recreate the database with the original configuration and verify it is online.
	rt.CreateDatabase(dbName, dbConfig)
	rt.WaitForDBOnline()
	resp = rt.SendAdminRequest(http.MethodGet, allDBsURL, "")
	RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, fmt.Sprintf(`[{"db_name":"%s","bucket":"%s","state":"Online"}]`, rt.GetDatabase().Name, rt.GetDatabase().Bucket.GetName()), resp.Body.String())
}

// TestDisableAllowStarChannel verifies that the database configuration does not allow
// a database to be loaded with the `enable_star_channel` property to be set to false.
// The star channel is a special channel in Sync Gateway that provides access to all
// documents, and disabling it is not supported.
func TestDisableAllowStarChannel(t *testing.T) {
	ctx := t.Context()
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		SyncFn:           channels.DocChannelsSyncFunction,
	})
	defer rt.Close()

	const (
		dbName  = "db"
		errResp = "enable_star_channel cannot be set to false"
	)

	dbConfig := rt.NewDbConfig()
	dbConfig.Name = dbName

	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// Attempting to disable `enable_star_channel`
	rt.ServerContext().dbConfigs[dbName].DatabaseConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel = base.Ptr(false)

	// Reloading the database after updating the config
	_, err := rt.ServerContext().ReloadDatabase(ctx, dbName, false)
	assert.Error(t, err, errResp)
	base.DebugfCtx(t.Context(), base.KeySGTest, "additional logs")
}

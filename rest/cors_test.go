// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const accessControlAllowOrigin = "Access-Control-Allow-Origin"

func TestCORSDynamicSet(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	// CORS is set to http://example.com by RestTester ServerContext
	dbName := "corsdb"
	dbConfig := rt.NewDbConfig()

	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	const username = "alice"
	rt.CreateUser(username, nil)

	invalidDatabaseName := "invalid database name"
	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.keyspace}}/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusBadRequest)
			require.Contains(t, response.Body.String(), invalidDatabaseName)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	// successful request
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	dbConfig = rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.org"},
	}

	resp = rt.ReplaceDbConfig(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// this falls back to the server config CORS without the user being authenticated
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.keyspace}}/", "", reqHeaders)
		if method == http.MethodGet {
			if base.TestsUseNamedCollections() {
				RequireStatus(t, response, http.StatusBadRequest)
				require.Contains(t, response.Body.String(), invalidDatabaseName)
			} else { // CBG-2978, should not be different from GSI/collections
				RequireStatus(t, response, http.StatusUnauthorized)
			}
		} else {
			// information leak: the options request knows about the database and knows it doesn't match
			require.Equal(t, "", response.Header().Get(accessControlAllowOrigin))
			RequireStatus(t, response, http.StatusNoContent)
		}
	}

	// successful request - mismatched headers
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}

	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
			require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		} else {
			RequireStatus(t, response, http.StatusNoContent)
			// information leak: the options request knows about the database and knows it doesn't match
			require.Equal(t, "", response.Header().Get(accessControlAllowOrigin))
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}

	// successful request - matched headers
	reqHeaders = map[string]string{
		"Origin": "http://example.org",
	}

	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "http://example.org", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}

	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		if method == http.MethodGet {
			require.Equal(t, "*", response.Header().Get(accessControlAllowOrigin))
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			// information leak: the options request knows about the database and knows it doesn't match
			require.Equal(t, "http://example.org", response.Header().Get(accessControlAllowOrigin))
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
		require.Equal(t, "*", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			RequireStatus(t, response, http.StatusNoContent)

		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "http://example.org", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
}

func TestCORSNoMux(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	// this method doesn't exist
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/_notanendpoint/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		RequireStatus(t, response, http.StatusNotFound)
		require.Contains(t, response.Body.String(), "unknown URL")
	}

	// admin port shouldn't populate CORS
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendAdminRequestWithHeaders(method, "/_notanendpoint/", "", reqHeaders)
		require.Equal(t, "", response.Header().Get(accessControlAllowOrigin))
		RequireStatus(t, response, http.StatusNotFound)
		require.Contains(t, response.Body.String(), "unknown URL")
	}
	// this method doesn't exist
	for _, method := range []string{http.MethodDelete, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodDelete {
			RequireStatus(t, response, http.StatusMethodNotAllowed)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
		require.Equal(t, strconv.Itoa(rt.ServerContext().Config.API.CORS.MaxAge), response.Header().Get("Access-Control-Max-Age"))
		require.Equal(t, "GET, HEAD, POST, PUT", response.Header().Get("Access-Control-Allow-Methods"))
	}

	for _, method := range []string{http.MethodDelete, http.MethodOptions} {
		response := rt.SendAdminRequestWithHeaders(method, "/_stats/", "", reqHeaders)
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusMethodNotAllowed)
		}

		require.Equal(t, "", response.Header().Get(accessControlAllowOrigin))
		require.Equal(t, "", response.Header().Get("Access-Control-Max-Age"))
		require.Equal(t, "", response.Header().Get("Access-Control-Allow-Methods"))
	}
}

func TestCORSUserNoAccess(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				CORS: &auth.CORSConfig{
					Origin: []string{"http://couchbase.com"},
				},
			},
		},
	})
	defer rt.Close()

	const alice = "alice"
	response := rt.SendAdminRequest(http.MethodPut,
		"/"+rt.GetDatabase().Name+"/_user/"+alice,
		`{"name": "`+alice+`", "password": "`+RestTesterDefaultUserPassword+`"}`)
	RequireStatus(t, response, http.StatusCreated)

	for _, endpoint := range []string{"/{{.db}}/", "/notadb/"} {
		t.Run(endpoint, func(t *testing.T) {
			reqHeaders := map[string]string{
				"Origin": "http://couchbase.com",
			}
			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				response := rt.SendRequestWithHeaders(method, endpoint, "", reqHeaders)
				if method == http.MethodOptions && endpoint == "/{{.db}}/" {
					// information leak: the options request knows about the database and knows it doesn't match
					assert.Equal(t, "http://couchbase.com", response.Header().Get(accessControlAllowOrigin))
				} else {
					assert.Equal(t, "*", response.Header().Get(accessControlAllowOrigin))
				}

				if method == http.MethodGet {
					RequireStatus(t, response, http.StatusUnauthorized)
					require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

				} else {
					RequireStatus(t, response, http.StatusNoContent)
				}
			}
		})
	}
}

func TestCORSOriginPerDatabase(t *testing.T) {
	// Override the default (example.com) CORS configuration in the DbConfig for /db:
	const perDBMaxAge = 1234
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				CORS: &auth.CORSConfig{
					Origin:      []string{"http://couchbase.com", "http://staging.couchbase.com"},
					LoginOrigin: []string{"http://couchbase.com"},
					Headers:     []string{},
					MaxAge:      perDBMaxAge,
				},
			},
		},
		GuestEnabled: true,
	})
	defer rt.Close()

	testCases := []struct {
		name                  string
		endpoint              string
		origin                string
		headerResponse        string
		headerResponseOptions string
		responseCode          int
	}{
		{
			name:                  "CORS origin allowed couchbase",
			endpoint:              "/{{.db}}/",
			origin:                "http://couchbase.com",
			headerResponse:        "http://couchbase.com",
			headerResponseOptions: "http://couchbase.com",
			responseCode:          http.StatusOK,
		},
		{
			name:           "CORS origin allowed example.com",
			endpoint:       "/{{.db}}/",
			origin:         "http://example.com",
			headerResponse: "",
			responseCode:   http.StatusOK,
		},
		{
			name:           "not allowed domain",
			endpoint:       "/{{.db}}/",
			origin:         "http://hack0r.com",
			headerResponse: "",
			responseCode:   http.StatusOK,
		},
		{
			name:           "root url allow hack0r",
			endpoint:       "/",
			origin:         "http://hack0r.com",
			headerResponse: "*",
			responseCode:   http.StatusOK,
		},
		{
			name:                  "root url allow couchbase",
			endpoint:              "/",
			origin:                "http://couchbase.com",
			headerResponse:        "*",
			headerResponseOptions: "*",
			responseCode:          http.StatusOK,
		},
		{
			name:                  "root url allow example.com",
			endpoint:              "/",
			origin:                "http://example.com",
			headerResponse:        "http://example.com",
			headerResponseOptions: "http://example.com",
			responseCode:          http.StatusOK,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			reqHeaders := map[string]string{
				"Origin": test.origin,
			}
			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				response := rt.SendRequestWithHeaders(method, test.endpoint, "", reqHeaders)
				if method == http.MethodGet {
					require.Equal(t, test.responseCode, response.Code)
				} else {
					require.Equal(t, http.StatusNoContent, response.Code)
				}
				require.Equal(t, test.headerResponse, response.Header().Get(accessControlAllowOrigin))
				if method == http.MethodOptions {
					if strings.Contains(test.endpoint, "{{.db}}") {
						require.Equal(t, strconv.Itoa(perDBMaxAge), response.Header().Get("Access-Control-Max-Age"))
					} else {
						require.Equal(t, strconv.Itoa(rt.ServerContext().Config.API.CORS.MaxAge), response.Header().Get("Access-Control-Max-Age"))
					}
				}
			}

		})
	}
}

func TestCORSValidation(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	const dbName = "corsdb"

	// make sure you are allowed to set CORS values that aren't max_age
	CORSDbConfig := rt.NewDbConfig()
	CORSDbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.com"},
		MaxAge: 1000,
	}

	resp := rt.CreateDatabase(dbName, CORSDbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.ReplaceDbConfig(dbName, CORSDbConfig)
	RequireStatus(t, resp, http.StatusCreated)

}

func TestCORSBlipSync(t *testing.T) {
	rtConfig := &RestTesterConfig{
		PersistentConfig: true,
	}

	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.com"},
	}

	rt.CreateDatabase("corsdb", dbConfig)
	require.NoError(t, rt.SetAdminParty(true))
	testCases := []struct {
		name         string
		origin       *string
		errorMessage string
	}{
		{
			name:   "CORS matching origin",
			origin: base.StringPtr("http://example.com"),
		},
		{
			name:         "CORS non-matching origin",
			origin:       base.StringPtr("http://example2.com"),
			errorMessage: "expected handshake response",
		},
		{
			name:   "CORS empty",
			origin: base.StringPtr(""),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			spec := getDefaultBlipTesterSpec()
			spec.origin = test.origin
			_, err := createBlipTesterWithSpec(t, spec, rt)
			if test.errorMessage == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "expected handshake response")
			}
		})
	}
	requireBlipHandshakeEmptyCORS(rt)
	requireBlipHandshakeMatchingHost(rt)
}

func TestCORSBlipSyncStar(t *testing.T) {
	rtConfig := &RestTesterConfig{
		PersistentConfig: true,
	}

	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"*"},
	}
	rt.CreateDatabase("corsdb", dbConfig)
	require.NoError(t, rt.SetAdminParty(true))
	urls := []string{"http://example.com", "http://example2.com", "https://example.com"}
	for _, url := range urls {
		t.Run(url, func(t *testing.T) {
			spec := getDefaultBlipTesterSpec()
			spec.origin = &url
			_, err := createBlipTesterWithSpec(t, spec, rt)
			require.NoError(t, err)
		})
	}
	requireBlipHandshakeEmptyCORS(rt)
	requireBlipHandshakeMatchingHost(rt)
}

// TestCORSBlipNoConfig has no CORS config set on the database, and should fail any CORS checks.
func TestCORSBlipNoConfig(t *testing.T) {
	rtConfig := &RestTesterConfig{
		PersistentConfig: true,
	}

	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{""},
	}

	rt.CreateDatabase("corsdb", dbConfig)
	require.NoError(t, rt.SetAdminParty(true))

	urls := []string{"http://example.com", "http://example2.com", "https://example.com"}
	for _, url := range urls {
		t.Run(url, func(t *testing.T) {
			spec := getDefaultBlipTesterSpec()
			spec.origin = &url
			_, err := createBlipTesterWithSpec(t, spec, rt)
			require.Error(t, err)
		})
	}
	requireBlipHandshakeEmptyCORS(rt)
	requireBlipHandshakeMatchingHost(rt)
}

// requireBlipHandshakeEmptyCORS creates a new blip tester with no Origin header
func requireBlipHandshakeEmptyCORS(rt *RestTester) {
	spec := getDefaultBlipTesterSpec()
	_, err := createBlipTesterWithSpec(rt.TB, spec, rt)
	require.NoError(rt.TB, err)
}

// requireBlipHandshakeMatchingHost creates a new blip tester with an Origin header that matches the host name of the test
func requireBlipHandshakeMatchingHost(rt *RestTester) {
	spec := getDefaultBlipTesterSpec()
	spec.useHostOrigin = true
	_, err := createBlipTesterWithSpec(rt.TB, spec, rt)
	require.NoError(rt.TB, err)
}

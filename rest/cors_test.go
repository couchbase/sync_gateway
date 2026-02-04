// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const accessControlAllowOrigin = "Access-Control-Allow-Origin"

func TestCORSDynamicSet(t *testing.T) {
	base.LongRunningTest(t)

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

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
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
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		if method == http.MethodGet {
			require.Equal(t, "http://example.com", response.Header().Get(accessControlAllowOrigin))
			RequireStatus(t, response, http.StatusUnauthorized)
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
		rt.Run(endpoint, func(t *testing.T) {
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

// TestCORSResponseHeadersEmptyConfig ensures that an empty CORS config results in no CORS headers being set on the response.
func TestCORSResponseHeadersEmptyConfig(t *testing.T) {
	rt := NewRestTester(t, nil)
	// RestTester initializes using defaultTestingCORSOrigin - override to empty for this test
	rt.ServerContext().Config.API.CORS = &auth.CORSConfig{}
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}
	response := rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/", "", reqHeaders)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
	assert.NotContains(t, response.Header(), "Access-Control-Allow-Origin")
	assert.NotContains(t, response.Header(), "Access-Control-Allow-Credentials")
	assert.NotContains(t, response.Header(), "Access-Control-Allow-Headers")
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
		rt.Run(test.name, func(t *testing.T) {
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

func TestCORSLoginOriginPerDatabase(t *testing.T) {
	// Override the default (example.com) CORS configuration in the DbConfig for /db:
	rt := NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()
	testCases := []struct {
		name               string
		unsupportedOptions *db.UnsupportedOptions
		sameSite           http.SameSite
		useTLS             bool
	}{
		{
			name:               "No unsupported options with TLS",
			unsupportedOptions: nil,
			sameSite:           http.SameSiteNoneMode,
			useTLS:             true,
		},
		{
			name:               "No unsupported options without TLS",
			unsupportedOptions: nil,
			sameSite:           0, // go 1.25 doesn't have a constant for not present when reading from Set-Cookie, this could turn into SameSiteDefaultMode (1) in future
			useTLS:             false,
		},
		{
			name: "With unsupported options and TLS",
			unsupportedOptions: &db.UnsupportedOptions{
				SameSiteCookie: base.Ptr("Strict"),
			},
			sameSite: http.SameSiteStrictMode,
			useTLS:   true,
		},
		{
			name: "With unsupported options and no TLS",
			unsupportedOptions: &db.UnsupportedOptions{
				SameSiteCookie: base.Ptr("Strict"),
			},
			sameSite: http.SameSiteStrictMode, // forces strict mode even though this would result in an unusable cookie
			useTLS:   false,
		},
	}
	for _, dbTestCases := range testCases {
		rt.Run(dbTestCases.name, func(t *testing.T) {
			// Override the default (example.com) CORS configuration in the DbConfig for /db:
			rt := NewRestTesterPersistentConfigNoDB(t)
			defer rt.Close()

			// fake TLS on public port
			if dbTestCases.useTLS {
				rt.ServerContext().Config.API.HTTPS.TLSCertPath = "/pretend/valid/cert"
			} else {
				require.Empty(t, rt.ServerContext().Config.API.HTTPS.TLSCertPath)
			}

			dbConfig := rt.NewDbConfig()
			dbConfig.Unsupported = dbTestCases.unsupportedOptions
			dbConfig.CORS = &auth.CORSConfig{
				Origin:      []string{"http://couchbase.com", "http://staging.couchbase.com"},
				LoginOrigin: []string{"http://couchbase.com"},
				Headers:     []string{},
			}
			RequireStatus(t, rt.CreateDatabase(SafeDatabaseName(t, dbTestCases.name), dbConfig), http.StatusCreated)
			const username = "alice"
			rt.CreateUser(username, nil)

			testCases := []struct {
				name              string
				origin            string
				responseCode      int
				responseErrorBody string
			}{
				{
					name:         "CORS login origin allowed couchbase",
					origin:       "http://couchbase.com",
					responseCode: http.StatusOK,
				},
				{
					name:              "CORS login origin not allowed staging",
					origin:            "http://staging.couchbase.com",
					responseCode:      http.StatusBadRequest,
					responseErrorBody: "No CORS",
				},
			}
			for _, test := range testCases {
				rt.Run(test.name, func(t *testing.T) {
					reqHeaders := map[string]string{
						"Origin":        test.origin,
						"Authorization": GetBasicAuthHeader(t, username, RestTesterDefaultUserPassword),
					}
					resp := rt.SendRequestWithHeaders(http.MethodPost, "/{{.db}}/_session", "", reqHeaders)
					RequireStatus(t, resp, test.responseCode)
					if test.responseErrorBody != "" {
						require.Contains(t, resp.Body.String(), test.responseErrorBody)
						// the access control headers are returned based on Origin and not LoginOrigin which could be considered a bug
						require.Equal(t, test.origin, resp.Header().Get(accessControlAllowOrigin))
					} else {
						require.Equal(t, test.origin, resp.Header().Get(accessControlAllowOrigin))
					}
					if test.responseCode == http.StatusOK {
						cookie, err := http.ParseSetCookie(resp.Header().Get("Set-Cookie"))
						require.NoError(t, err)
						require.NotEmpty(t, cookie.Path)
						require.Equal(t, dbTestCases.sameSite, cookie.SameSite, "Cookie=%#+v", cookie)
						reqHeaders["Cookie"] = fmt.Sprintf("%s=%s", cookie.Name, cookie.Value)
					}
					resp = rt.SendRequestWithHeaders(http.MethodDelete, "/{{.db}}/_session", "", reqHeaders)
					RequireStatus(t, resp, test.responseCode)
					if test.responseErrorBody != "" {
						require.Contains(t, resp.Body.String(), test.responseErrorBody)
						// the access control headers are returned based on Origin and not LoginOrigin which could be considered a bug
						require.Equal(t, test.origin, resp.Header().Get(accessControlAllowOrigin))
					} else {
						require.Equal(t, test.origin, resp.Header().Get(accessControlAllowOrigin))
					}

				})
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
	rt.SetAdminParty(true)
	testCases := []struct {
		name         string
		origin       *string
		errorMessage string
	}{
		{
			name:   "CORS matching origin",
			origin: base.Ptr("http://example.com"),
		},
		{
			name:         "CORS non-matching origin",
			origin:       base.Ptr("http://example2.com"),
			errorMessage: "expected handshake response",
		},
		{
			name:   "CORS empty",
			origin: base.Ptr(""),
		},
	}
	for _, test := range testCases {
		rt.Run(test.name, func(t *testing.T) {

			spec := getDefaultBlipTesterSpec()
			spec.origin = test.origin
			_, err := createBlipTesterWithSpec(rt, spec)
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
	rt.SetAdminParty(true)
	urls := []string{"http://example.com", "http://example2.com", "https://example.com"}
	for _, url := range urls {
		rt.Run(url, func(t *testing.T) {
			spec := getDefaultBlipTesterSpec()
			spec.origin = &url
			_, err := createBlipTesterWithSpec(rt, spec)
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
	rt.SetAdminParty(true)

	urls := []string{"http://example.com", "http://example2.com", "https://example.com"}
	for _, url := range urls {
		rt.Run(url, func(t *testing.T) {
			spec := getDefaultBlipTesterSpec()
			spec.origin = &url
			_, err := createBlipTesterWithSpec(rt, spec)
			require.Error(t, err)
		})
	}
	requireBlipHandshakeEmptyCORS(rt)
	requireBlipHandshakeMatchingHost(rt)
}

// requireBlipHandshakeEmptyCORS creates a new blip tester with no Origin header
func requireBlipHandshakeEmptyCORS(rt *RestTester) {
	spec := getDefaultBlipTesterSpec()
	_, err := createBlipTesterWithSpec(rt, spec)
	require.NoError(rt.TB(), err)
}

// requireBlipHandshakeMatchingHost creates a new blip tester with an Origin header that matches the host name of the test
func requireBlipHandshakeMatchingHost(rt *RestTester) {
	spec := getDefaultBlipTesterSpec()
	spec.useHostOrigin = true
	_, err := createBlipTesterWithSpec(rt, spec)
	require.NoError(rt.TB(), err)
}

func TestBadCORSValuesConfig(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	// expect database to be created with bad CORS values, but do log a warning
	dbConfig := rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.com", "1http://example.com"},
	}
	base.AssertLogContains(t, "cors.origin contains values", func() {
		rt.CreateDatabase("db", dbConfig)
	})
}

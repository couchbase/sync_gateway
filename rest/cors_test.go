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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
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
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
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
			require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
			RequireStatus(t, response, http.StatusBadRequest)
			require.Contains(t, response.Body.String(), invalidDatabaseName)
		} else {
			// information leak: the options request knows about the database and knows it doesn't match
			require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
			RequireStatus(t, response, http.StatusNoContent)
		}
	}

	// successful request - mismatched headers
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
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
			require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
		} else {
			RequireStatus(t, response, http.StatusNoContent)
			// information leak: the options request knows about the database and knows it doesn't match
			require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
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
		require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusOK)
		} else {
			RequireStatus(t, response, http.StatusNoContent)
		}
	}

	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders)
		if method == http.MethodGet {
			require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			// information leak: the options request knows about the database and knows it doesn't match
			require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
			RequireStatus(t, response, http.StatusNoContent)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
		require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
		if method == http.MethodGet {
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
		} else {
			RequireStatus(t, response, http.StatusNoContent)

		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendUserRequestWithHeaders(method, "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
		require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
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
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
		RequireStatus(t, response, http.StatusNotFound)
		require.Contains(t, response.Body.String(), "unknown URL")
	}

	// admin port shouldn't populate CORS
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		response := rt.SendAdminRequestWithHeaders(method, "/_notanendpoint/", "", reqHeaders)
		require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
		RequireStatus(t, response, http.StatusNotFound)
		require.Contains(t, response.Body.String(), "unknown URL")
	}
	// this method doesn't exist
	for _, method := range []string{http.MethodDelete, http.MethodOptions} {
		response := rt.SendRequestWithHeaders(method, "/notadb/", "", reqHeaders)
		require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
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

		require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
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
					assert.Equal(t, "http://couchbase.com", response.Header().Get("Access-Control-Allow-Origin"))
				} else {
					assert.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
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
				require.Equal(t, test.headerResponse, response.Header().Get("Access-Control-Allow-Origin"))
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

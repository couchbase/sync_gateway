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
	response := rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/", "", reqHeaders)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusBadRequest)
	require.Contains(t, response.Body.String(), invalidDatabaseName)

	// successful request
	response = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendUserRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

	dbConfig = rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.org"},
	}

	resp = rt.ReplaceDbConfig(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// this falls back to the server config CORS without the user being authenticated
	response = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/", "", reqHeaders)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusBadRequest)
	require.Contains(t, response.Body.String(), invalidDatabaseName)

	// successful request - mismatched headers
	response = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendRequestWithHeaders("GET", "/notadb/", "", reqHeaders)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendUserRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

	// successful request - matched headers
	reqHeaders = map[string]string{
		"Origin": "http://example.org",
	}
	response = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/_all_docs", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders)
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendRequestWithHeaders("GET", "/notadb/", "", reqHeaders)
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendUserRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

}

func TestCORSNoMux(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}
	// this method doesn't exist
	response := rt.SendRequestWithHeaders("GET", "/_notanendpoint/", "", reqHeaders)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusNotFound)
	require.Contains(t, response.Body.String(), "unknown URL")

	// admin port shouldn't populate CORS
	response = rt.SendAdminRequestWithHeaders("GET", "/_notanendpoint/", "", reqHeaders)
	require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusNotFound)
	require.Contains(t, response.Body.String(), "unknown URL")

	// this method doesn't exist
	response = rt.SendRequestWithHeaders(http.MethodDelete, "/notadb/", "", reqHeaders)
	require.Equal(t, "http://example.com", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusMethodNotAllowed)
	require.Equal(t, strconv.Itoa(rt.ServerContext().Config.API.CORS.MaxAge), response.Header().Get("Access-Control-Max-Age"))
	require.Equal(t, "GET, HEAD, POST, PUT", response.Header().Get("Access-Control-Allow-Methods"))

	response = rt.SendAdminRequestWithHeaders(http.MethodDelete, "/_stats/", "", reqHeaders)
	RequireStatus(t, response, http.StatusMethodNotAllowed)
	require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "", response.Header().Get("Access-Control-Max-Age"))
	require.Equal(t, "", response.Header().Get("Access-Control-Allow-Methods"))

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

	for _, endpoint := range []string{"/{{.db}}/", "/notadb/"} {
		t.Run(endpoint, func(t *testing.T) {
			reqHeaders := map[string]string{
				"Origin": "http://couchbase.com",
			}
			response = rt.SendRequestWithHeaders(http.MethodGet, endpoint, "", reqHeaders)
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), ErrLoginRequired.Message)
			assert.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
		})
	}
}

func TestCORSOriginPerDatabase(t *testing.T) {
	// Override the default (example.com) CORS configuration in the DbConfig for /db:
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				CORS: &auth.CORSConfig{
					Origin:      []string{"http://couchbase.com", "http://staging.couchbase.com"},
					LoginOrigin: []string{"http://couchbase.com"},
					Headers:     []string{},
					MaxAge:      1728000,
				},
			},
		},
		GuestEnabled: true,
	})
	defer rt.Close()

	testCases := []struct {
		name           string
		endpoint       string
		origin         string
		headerResponse string
		responseCode   int
	}{
		{
			name:           "CORS origin allowed couchbase",
			endpoint:       "/{{.db}}/",
			origin:         "http://couchbase.com",
			headerResponse: "http://couchbase.com",
			responseCode:   http.StatusOK,
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
			name:           "root url allow couchbase",
			endpoint:       "/",
			origin:         "http://couchbase.com",
			headerResponse: "*",
			responseCode:   http.StatusOK,
		},
		{
			name:           "root url allow example.com",
			endpoint:       "/",
			origin:         "http://example.com",
			headerResponse: "http://example.com",
			responseCode:   http.StatusOK,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			reqHeaders := map[string]string{
				"Origin": test.origin,
			}
			response := rt.SendRequestWithHeaders(http.MethodGet, test.endpoint, "", reqHeaders)
			require.Equal(t, test.responseCode, response.Code)
			require.Equal(t, test.headerResponse, response.Header().Get("Access-Control-Allow-Origin"))

		})
	}
}

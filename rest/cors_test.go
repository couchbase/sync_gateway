package rest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
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
	require.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusUnauthorized)
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
	require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendUserRequestWithHeaders("GET", "/{{.db}}/", "", reqHeaders, username, RestTesterDefaultUserPassword)
	require.Equal(t, "http://example.org", response.Header().Get("Access-Control-Allow-Origin"))
	RequireStatus(t, response, http.StatusOK)

}

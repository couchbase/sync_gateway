package rest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/stretchr/testify/require"
)

func TestHandleOIDCCommon(t *testing.T) {
	var (
		wellKnownPath       = "/.well-known/openid-configuration"
		headerLocation      = "Location"
		providerNameGoogle  = "google"
		clientIDGoogle      = "client.id.google"
		validationKeyGoogle = "validation.key.google"
	)

	// Mock OpenID Connect Provider Discovery Endpoints
	var issuerGoogle string
	mux := http.NewServeMux()
	mux.HandleFunc("/google"+wellKnownPath, func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		body := `{
			"issuer": "${issuer}", 
			"authorization_endpoint": "${issuer}/auth", 
        	"token_endpoint": "https://oauth2.googleapis.com/token",
			"jwks_uri": "https://www.googleapis.com/oauth2/v3/certs", 
			"id_token_signing_alg_values_supported": ["RS256"]
		}`
		_, err := io.WriteString(res, strings.ReplaceAll(body, "${issuer}", issuerGoogle))
		require.NoError(t, err, "Failed to write response body for google provider")
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	issuerGoogle = ts.URL + "/google"
	providerGoogle := auth.OIDCProvider{
		Name:          providerNameGoogle,
		Issuer:        issuerGoogle,
		ClientID:      &clientIDGoogle,
		ValidationKey: &validationKeyGoogle,
		DiscoveryURI:  issuerGoogle + wellKnownPath,
	}

	providerMap := auth.OIDCProviderMap{providerNameGoogle: &providerGoogle}
	openIDConnectOptions := auth.OIDCOptions{Providers: providerMap, DefaultProvider: &providerGoogle.Name}
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &openIDConnectOptions}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	t.Run("check state parameter in handle oidc", func(t *testing.T) {
		resp := rt.SendRequest(http.MethodGet, "/db/_oidc?provider=google&offline=true", "")
		require.Equal(t, http.StatusFound, resp.Code)
		location := resp.Header().Get(headerLocation)
		require.NotEmpty(t, location, "Location should be available in response header")
		locationURL, err := url.Parse(location)
		require.NoError(t, err, "Location header doesn't look like a valid URL")
		stateParam := locationURL.Query().Get(requestParamState)
		require.NotEmpty(t, stateParam, "State parameter is empty or not found")
	})

	t.Run("check state parameter in handle oidc challenge", func(t *testing.T) {
		resp := rt.SendRequest(http.MethodGet, "/db/_oidc_challenge?provider=google&offline=true", "")
		require.Equal(t, http.StatusUnauthorized, resp.Code)
		authHeader := resp.Header().Get("WWW-Authenticate")
		require.NotEmpty(t, authHeader, "WWW-Authenticate should be available in response header")
		authHeader = strings.ReplaceAll(authHeader, "OIDC login=", "")
		authURL, err := url.Parse(strings.ReplaceAll(authHeader, "\"", ""))
		require.NoError(t, err, "Error parsing auth URL")
		stateParam := authURL.Query().Get(requestParamState)
		require.NotEmpty(t, stateParam, "State parameter is empty or not found")
	})
}

package rest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOIDCCallbackURL(t *testing.T) {
	var (
		wellKnownPath           = "/.well-known/openid-configuration"
		headerLocation          = "Location"
		authProvider            = "provider"
		authRedirectURI         = "redirect_uri"
		providerNameGoogle      = "google"
		providerNameSalesforce  = "salesforce"
		clientIDGoogle          = "client.id.google"
		clientIDSalesforce      = "client.id.salesforce"
		validationKeySalesforce = "validation.key.salesforce"
		validationKeyGoogle     = "validation.key.google"
	)

	// Mock OpenID Connect Provider Discovery Endpoints
	var issuerGoogle, issuerSalesforce string
	mux := http.NewServeMux()
	mux.HandleFunc("/google/", func(res http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/google/.well-known/openid-configuration" {
			http.NotFound(res, req)
			return
		}
		res.Header().Set("Content-Type", "application/json")
		body := `{
			"issuer": "${issuer}", 
			"authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth", 
        	"token_endpoint": "https://oauth2.googleapis.com/token",
			"jwks_uri": "https://www.googleapis.com/oauth2/v3/certs", 
			"id_token_signing_alg_values_supported": ["RS256"]
		}`
		_, err := io.WriteString(res, strings.ReplaceAll(body, "${issuer}", issuerGoogle))
		require.NoError(t, err, "Failed to write response body for google provider")
	})
	mux.HandleFunc("/salesforce/", func(res http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/salesforce/.well-known/openid-configuration" {
			http.NotFound(res, req)
			return
		}
		res.Header().Set("Content-Type", "application/json")
		body := `{
			"issuer": "${issuer}", 
			"authorization_endpoint": "https://login.salesforce.com/services/oauth2/authorize",
        	"token_endpoint": "https://login.salesforce.com/services/oauth2/token",
			"jwks_uri": "https://login.salesforce.com/id/keys", 
			"id_token_signing_alg_values_supported": ["RS256"]
		}`
		_, err := io.WriteString(res, strings.ReplaceAll(body, "${issuer}", issuerSalesforce))
		require.NoError(t, err, "Failed to write response body for salesforce provider")
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	issuerGoogle = ts.URL + "/google"
	issuerSalesforce = ts.URL + "/salesforce"

	// Default OpenID Connect Provider
	providerGoogle := auth.OIDCProvider{
		Name:          providerNameGoogle,
		Issuer:        issuerGoogle,
		ClientID:      &clientIDGoogle,
		ValidationKey: &validationKeyGoogle,
		DiscoveryURI:  issuerGoogle + wellKnownPath,
	}

	// Non-default OpenID Connect Provider
	providerSalesforce := auth.OIDCProvider{
		Name:          providerNameSalesforce,
		Issuer:        issuerSalesforce,
		ClientID:      &clientIDSalesforce,
		ValidationKey: &validationKeySalesforce,
		DiscoveryURI:  issuerSalesforce + wellKnownPath,
	}

	providerMap := auth.OIDCProviderMap{
		providerNameGoogle:     &providerGoogle,
		providerNameSalesforce: &providerSalesforce,
	}

	openIDConnectOptions := auth.OIDCOptions{Providers: providerMap, DefaultProvider: &providerGoogle.Name}
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &openIDConnectOptions}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	t.Run("default provider configured but current provider is not default", func(t *testing.T) {
		// When multiple providers are defined, default provider is specified and the current provider is
		// not default, then current provider should be added to the generated OpenID Connect callback URL.
		resp := rt.SendAdminRequest(http.MethodGet, "/db/_oidc?provider=salesforce&offline=true", "")
		require.Equal(t, http.StatusFound, resp.Code)
		location := resp.Header().Get(headerLocation)
		require.NotEmpty(t, location, "Location should be available in response header")
		locationURL, err := url.Parse(location)
		require.NoError(t, err, "Location header should be a valid URL")
		redirectURI := locationURL.Query().Get(authRedirectURI)
		require.NotEmpty(t, location, "redirect_uri should be available in auth URL")
		redirectURL, err := url.Parse(redirectURI)
		require.NoError(t, err, "redirect_uri should be a valid URL")
		assert.Equal(t, providerSalesforce.Name, redirectURL.Query().Get(authProvider))
	})

	t.Run("default provider configured and current provider is default", func(t *testing.T) {
		// When multiple providers are defined, default provider is specified and the current provider is
		// default, then current provider should NOT be added to the generated OpenID Connect callback URL.
		resp := rt.SendAdminRequest(http.MethodGet, "/db/_oidc?provider=google&offline=true", "")
		require.Equal(t, http.StatusFound, resp.Code)
		location := resp.Header().Get(headerLocation)
		require.NotEmpty(t, location, "Location should be available in response header")
		locationURL, err := url.Parse(location)
		require.NoError(t, err, "Location header should be a valid URL")
		redirectURI := locationURL.Query().Get(authRedirectURI)
		require.NotEmpty(t, location, "redirect_uri should be available in auth URL")
		redirectURL, err := url.Parse(redirectURI)
		require.NoError(t, err, "redirect_uri should be a valid URL")
		assert.Equal(t, "", redirectURL.Query().Get(authProvider))
	})

	t.Run("default provider configured but no current provider", func(t *testing.T) {
		// When multiple providers are defined, default provider is specified and no current provider is
		// provided, then provider name should NOT be added to the generated OpenID Connect callback URL.
		resp := rt.SendAdminRequest(http.MethodGet, "/db/_oidc?offline=true", "")
		require.Equal(t, http.StatusFound, resp.Code)
		location := resp.Header().Get(headerLocation)
		require.NotEmpty(t, location, "Location should be available in response header")
		locationURL, err := url.Parse(location)
		require.NoError(t, err, "Location header should be a valid URL")
		redirectURI := locationURL.Query().Get(authRedirectURI)
		require.NotEmpty(t, location, "redirect_uri should be available in auth URL")
		redirectURL, err := url.Parse(redirectURI)
		require.NoError(t, err, "redirect_uri should be a valid URL")
		assert.Equal(t, "", redirectURL.Query().Get(authProvider))
	})
}

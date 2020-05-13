package rest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2/jwt"
)

// wantErrType represents a specific error type
// needs to be mocked during test execution.
type wantErrType int

const (

	// noCodeErr option forces the mock auth server to return the callback URL
	// with no authorization code.
	noCodeErr wantErrType = iota + 1

	// callbackErr option forces the mock auth server to return an auth error
	// in the callback URL with a short error description.
	callbackErr

	// untoldProviderErr option forces the mock auth server to return an unknown
	// provider (provider which is not configured) the callback URL.
	untoldProviderErr

	// discoveryErr forces the mock auth server to return an error response
	// during custom provider discovery.
	discoveryErr

	// callbackTokenExchangeErr forces the mock auth server to return an error
	// response while exchanging authorization code for an access token.
	callbackTokenExchangeErr

	// callbackNoIDTokenErr forces the mock auth server to return no ID token in
	// the response while exchanging authorization code for an access token.
	callbackNoIDTokenErr

	// refreshTokenExchangeErr forces the mock auth server to return an error
	// response during token refresh.
	refreshTokenExchangeErr

	// refreshNoIDTokenErr forces the mock auth server to return no ID token in
	// the response during token refresh.
	refreshNoIDTokenErr

	// noAutoRegistrationErr represents an error type returned from SG when a new
	// user authenticates through OpenID connect with auto registration is disabled.
	noAutoRegistrationErr

	// notConfiguredProviderErr represents the error type returned from SG when
	// initiate the auth request with a provider which is not already configured.
	notConfiguredProviderErr
)

// grantType refers to the way a relying party gets an access token.
type grantType int

const (
	// grantTypeAuthCode tells the token endpoint that the
	// relying party is using the authorization code grant type.
	grantTypeAuthCode grantType = iota + 1

	// grantTypeRefreshToken tells the token endpoint that the
	// relying party is using the refresh token grant type.
	grantTypeRefreshToken
)

// The mockAuthServer represents a mock OAuth2 server for verifying OpenID Connect client code.
// It is not intended to be used as an actual OAuth 2 server. It lacks many features that would
// be required in a classic implementation. See https://tools.ietf.org/html/rfc6749 to know more
// about the OAuth 2.0 Authorization Framework specification.
type mockAuthServer struct {

	// URL represents base URL of the OAuth2 server of the
	// form http://ipaddr:port with no trailing slash.
	URL string

	// server is an underlying HTTP server listening on a system-chosen
	// port on the local loopback interface, for use in end-to-end HTTP tests.
	server *httptest.Server

	// The options represents a collection of custom options you want to store
	// on the server in memory and change it's behavior against specific tests.
	// An empty options struct is allocated with enough space to hold the specified
	// number of elements when mock auth server is created.
	options *options
}

// options represents a set of settings to be configured on mock auth server
// to simulate the specific authentication behavior during test execution.
type options struct {

	// OpenID Connect  Provider Issuer URL
	issuer string

	// wantErrType represents a specific error type needs to be mocked during test execution.
	wantErrType wantErrType

	// grantType refers to the way a relying party gets an access token.
	grantType grantType

	// tokenResponse represents the token response from SG on successful authentication.
	tokenResponse OIDCTokenResponse
}

// The newMockAuthServer returns a new mock OAuth Server but doesn't start it.
// The caller should call Start when needed, to start it up.
func newMockAuthServer() *mockAuthServer {
	server := new(mockAuthServer)
	server.options = new(options)
	return server
}

// Start registers mock handlers and starts the mock OAuth server.
// The caller should call Shutdown when finished, to shut it down.
func (s *mockAuthServer) Start() {
	router := mux.NewRouter()
	router.HandleFunc("/{provider}"+auth.OIDCDiscoveryConfigPath, s.mockDiscoveryHandler).Methods(http.MethodGet)
	router.HandleFunc("/{provider}/auth", s.mockAuthHandler).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc("/{provider}/token", s.mockTokenHandler).Methods(http.MethodPost)
	s.server = httptest.NewServer(router)
	s.URL = s.server.URL
}

// Shutdown shuts down the underlying httptest server and blocks
// until all outstanding requests on that server have completed.
func (s *mockAuthServer) Shutdown() {
	s.server.Close()
}

// mockDiscoveryHandler mocks the provider discovery endpoint with a mock response.
// Makes a JSON document available at the path formed by concatenating the string
// /.well-known/openid-configuration to the Issuer.
func (s *mockAuthServer) mockDiscoveryHandler(w http.ResponseWriter, r *http.Request) {
	if s.options.wantErrType == discoveryErr {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	issuer := s.options.issuer
	metadata := auth.OidcProviderConfiguration{
		Issuer:                  issuer,
		TokenEndpoint:           issuer + "/token",
		JwksUri:                 issuer + "/oauth2/v3/certs",
		AuthEndpoint:            issuer + "/auth",
		IDTokenSigningAlgValues: []string{"RS256"},
	}
	renderJSON(w, r, http.StatusOK, metadata)
}

// renderJSON renders the response data as "application/json" with the status code.
// It may report status 500 Internal Server Error if there is any failure in converting the
// response data to JSON document.
func renderJSON(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		base.Errorf("Error rendering JSON response: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// mockAuthHandler mocks the authentication process performed by the OAuth Authorization Server.
// The behavior of mockAuthHandler can be modified through the options map when needed.
// Clients are redirected to the specified callback URL after successful authentication.
func (s *mockAuthServer) mockAuthHandler(w http.ResponseWriter, r *http.Request) {
	var redirectionURL string
	state := r.URL.Query().Get(requestParamState)
	redirect := r.URL.Query().Get(requestParamRedirectURI)
	if redirect == "" {
		base.Errorf("No redirect URL found in auth request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if s.options.wantErrType == callbackErr {
		err := "?error=unsupported_response_type&error_description=response_type%20not%20supported"
		redirectionURL = fmt.Sprintf("%s?error=%s", redirect, err)
		http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
	}
	code := base.GenerateRandomSecret()
	if s.options.wantErrType == noCodeErr {
		code = ""
	}
	redirectionURL = fmt.Sprintf("%s?code=%s", redirect, code)
	if state != "" {
		redirectionURL = fmt.Sprintf("%s&state=%s", redirectionURL, state)
	}
	if s.options.wantErrType == untoldProviderErr {
		uri, err := auth.SetURLQueryParam(redirectionURL, requestParamProvider, "untold")
		if err != nil {
			base.Errorf("error setting untold provider in mock callback URL")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		redirectionURL = uri
	}
	http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
}

// mockAuthHandler mocks the token handling process performed by the OAuth Authorization Server.
// It mocks token response and makes it available as JSON document.
func (s *mockAuthServer) mockTokenHandler(w http.ResponseWriter, r *http.Request) {
	if (s.options.wantErrType == callbackTokenExchangeErr && s.options.grantType == grantTypeAuthCode) ||
		(s.options.wantErrType == refreshTokenExchangeErr && s.options.grantType == grantTypeRefreshToken) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	issuer := s.options.issuer
	claims := jwt.Claims{ID: "id0123456789", Issuer: issuer,
		Audience: jwt.Audience{"aud1", "aud2", "aud3", "baz"},
		IssuedAt: jwt.NewNumericDate(time.Now()), Subject: "noah",
		Expiry: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
	}
	signer, err := base.GetRSASigner()
	if err != nil {
		base.Errorf("Error creating RSA signer: %s", err)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	claimEmail := map[string]interface{}{"email": "noah@foo.com"}
	builder := jwt.Signed(signer).Claims(claims).Claims(claimEmail)
	token, err := builder.CompactSerialize()
	if err != nil {
		base.Errorf("Error serializing token: %s", err)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	response := OIDCTokenResponse{
		IDToken:      token,
		AccessToken:  "7d1d234f5fde713a94454f268833adcd39835fe8",
		RefreshToken: "e08c77351221346153d09ff64c123b24fc4c1905",
		TokenType:    "Bearer",
		Expires:      time.Now().Add(5 * time.Minute).UTC().Second(),
	}
	if (s.options.grantType == grantTypeAuthCode && s.options.wantErrType == callbackNoIDTokenErr) ||
		(s.options.grantType == grantTypeRefreshToken && s.options.wantErrType == refreshNoIDTokenErr) {
		response.IDToken = ""
	}
	s.options.tokenResponse = response
	renderJSON(w, r, http.StatusOK, response)
}

// Verifies OpenID Connect callback URL in redirect link is returned in the Location
// header for both oidc and _oidc_challenge requests.
func TestGetOIDCCallbackURL(t *testing.T) {
	type test struct {
		name         string
		authURL      string
		issuer       string
		wantProvider string
	}
	tests := []test{
		{
			// When multiple providers are defined, default provider is specified and the current provider is
			// not default, then current provider should be added to the generated OpenID Connect callback URL.
			name:         "oidc default provider specified but current provider is not default",
			authURL:      "/db/${path}?provider=bar&offline=true",
			wantProvider: "bar",
			issuer:       "${path}/bar",
		}, {
			// When multiple providers are defined, default provider is specified and the current provider is
			// default, then current provider should NOT be added to the generated OpenID Connect callback URL.
			name:    "oidc default provider specified and current provider is default",
			authURL: "/db/${path}?provider=foo&offline=true",
			issuer:  "${path}/foo",
		}, {
			// When multiple providers are defined, default provider is specified and no current provider is
			// provided, then provider name should NOT be added to the generated OpenID Connect callback URL.
			name:    "oidc default provider specified with no current provider",
			authURL: "/db/${path}?offline=true",
			issuer:  "${path}/foo",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			providers := auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{Name: "foo", ClientID: base.StringPointer("baz"), ValidationKey: base.StringPointer("qux")},
				"bar": &auth.OIDCProvider{Name: "bar", ClientID: base.StringPointer("baz"), ValidationKey: base.StringPointer("qux")}}
			openIDConnectOptions := auth.OIDCOptions{Providers: providers, DefaultProvider: base.StringPointer("foo")}
			rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &openIDConnectOptions}}
			rt := NewRestTester(t, &rtConfig)
			defer rt.Close()

			mockAuthServer := newMockAuthServer()
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()
			refreshProviderConfig(providers, mockAuthServer.URL)
			tc.issuer = strings.ReplaceAll(tc.issuer, "${path}", mockAuthServer.URL)
			mockAuthServer.options.issuer = tc.issuer

			// Check _oidc_challenge behavior
			authURL := strings.ReplaceAll(tc.authURL, "${path}", "_oidc_challenge")
			resp := rt.SendAdminRequest(http.MethodGet, authURL, "")
			require.Equal(t, http.StatusUnauthorized, resp.Code)
			wwwAuthHeader := resp.Header().Get("Www-Authenticate")
			location := regexp.MustCompile(`login="(?P<login>.*?)"`).FindStringSubmatch(wwwAuthHeader)[1]

			require.NotEmpty(t, location, "error extracting location from header")
			locationURL, err := url.Parse(location)
			require.NoError(t, err, "error parsing location URL")
			redirectURI := locationURL.Query().Get(requestParamRedirectURI)
			redirectURL, err := url.Parse(redirectURI)
			require.NoError(t, err, "error parsing redirect_uri URL")
			assert.Equal(t, tc.wantProvider, redirectURL.Query().Get(auth.OIDCAuthProvider))

			// Check _oidc behavior
			authURL = strings.ReplaceAll(tc.authURL, "${path}", "_oidc")
			resp = rt.SendAdminRequest(http.MethodGet, authURL, "")
			require.Equal(t, http.StatusFound, resp.Code)
			location = resp.Header().Get("Location")

			require.NotEmpty(t, location, "error extracting location from header")
			locationURL, err = url.Parse(location)
			require.NoError(t, err, "error parsing location URL")
			redirectURI = locationURL.Query().Get(requestParamRedirectURI)
			redirectURL, err = url.Parse(redirectURI)
			require.NoError(t, err, "error parsing redirect_uri URL")
			assert.Equal(t, tc.wantProvider, redirectURL.Query().Get(auth.OIDCAuthProvider))
		})
	}
}

// Checks End to end OIDC Authorization Code flow.
func TestOpenIDConnectAuth(t *testing.T) {
	type test struct {
		name            string
		providers       auth.OIDCProviderMap
		defaultProvider string
		authURL         string
		wantErrType     wantErrType
	}
	tests := []test{
		{
			// Successful new user authentication when auto registration is enabled and a single provider
			// is configured through providers configuration. Explicitly specified to include access token in
			// token response.
			name: "successful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					ClientID:           base.StringPointer("baz"),
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					IncludeAccessToken: true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Unsuccessful new user authentication when auto registration is enabled and a single provider
			// is configured through providers configuration. Explicitly specified to include access token in
			// token response.
			name: "unsuccessful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     noAutoRegistrationErr,
		}, {
			// Make sure user registration in successful when IncludeAccessToken is false in
			// providers configuration.
			name: "successful user registration against single provider without access token",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
					Register:      true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Make sure user registration and authentication is successful when multiple
			// providers are configured with auto registration enabled.
			name: "successful user registration against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					ClientID:           base.StringPointer("baz"),
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					IncludeAccessToken: true,
				},
				"bar": &auth.OIDCProvider{
					Name:               "bar",
					ClientID:           base.StringPointer("baz"),
					UserPrefix:         "bar",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					IncludeAccessToken: true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Make sure user registration is unsuccessful when a new user authenticates
			// against auto registration option disabled through providers configuration.
			name: "unsuccessful user registration against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
				"bar": &auth.OIDCProvider{
					Name:          "bar",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "bar",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     noAutoRegistrationErr,
		}, {
			// Make sure no access token, token type and expiry time are not included in both
			// authentication response and token refresh response if IncludeAccessToken option
			// is not specified in providers configuration. But both authentication refresh token
			// request should be successful.
			name: "successful user registration against multiple provider without access token",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
					Register:      true,
				},
				"bar": &auth.OIDCProvider{
					Name:          "bar",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "bar",
					ValidationKey: base.StringPointer("qux"),
					Register:      true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Force mock auth server NOT to return no code in the callback URL and
			// make sure authentication is unsuccessful.
			name: "unsuccessful auth code received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     noCodeErr,
		}, {
			// Force the mock auth server to return a callback error and make sure
			// authentication is unsuccessful.
			name: "unsuccessful auth callback error received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     callbackErr,
		}, {
			// Force the mock auth server to return an unknown provider in the callback
			// URL and make sure the authentication request is unsuccessful.
			name: "unsuccessful auth untold provider received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     untoldProviderErr,
		}, {
			// Make sure authentication is unsuccessful when authenticating against a provider
			// which is not configured; i.e., specify a different provider in the auth request URL.
			name: "unsuccessful auth against not configured provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=fred&offline=true", // Configured provider is 'foo', NOT 'fred'.
			wantErrType:     notConfiguredProviderErr,
		}, {
			// Force mock auth server to return an error during provider discovery and make sure
			// the authentication is unsuccessful.
			name: "unsuccessful auth due to provider discovery failure",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     discoveryErr,
		}, {
			// Make sure user registration is successful when access type is offline.
			name: "successful user registration with access type not offline",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					ClientID:           base.StringPointer("baz"),
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					IncludeAccessToken: true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=false",
		}, {
			// Force mock auth server to return an error in callback URL while exchanging auth code
			// for a valid token. Make sure authentication is unsuccessful.
			name: "unsuccessful auth token exchange error",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     callbackTokenExchangeErr,
		}, {
			// Force mock auth server NOT to return an id_token while exchanging auth code
			// for a valid token. Make sure authentication is unsuccessful.
			name: "unsuccessful auth no id token received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:          "foo",
					ClientID:      base.StringPointer("baz"),
					UserPrefix:    "foo",
					ValidationKey: base.StringPointer("qux"),
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     callbackNoIDTokenErr,
		}, {
			// Force mock auth server to return a token exchange error during token refresh.
			// Make sure initial auth request for user registration is successful but subsequent
			// token refresh request is NOT due to exchange error.
			name: "unsuccessful auth token exchange error during token refresh",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					ClientID:           base.StringPointer("baz"),
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					IncludeAccessToken: true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     refreshTokenExchangeErr,
		}, {
			// Force mock auth server not to return an id_token during token refresh.
			// Make sure initial auth request for user registration is successful but subsequent
			// token refresh request is NOT due to no id_token in token response.
			name: "unsuccessful auth no id token received during token refresh",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					ClientID:           base.StringPointer("baz"),
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					IncludeAccessToken: true,
				},
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			wantErrType:     refreshNoIDTokenErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer := newMockAuthServer()
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()

			mockAuthServer.options.issuer = mockAuthServer.URL + "/" + tc.defaultProvider
			mockAuthServer.options.grantType = grantTypeAuthCode
			refreshProviderConfig(tc.providers, mockAuthServer.URL)

			opts := auth.OIDCOptions{Providers: tc.providers, DefaultProvider: &tc.defaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &opts}}
			restTester := NewRestTester(t, &restTesterConfig)
			defer restTester.Close()

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL
			mockAuthServer.options.wantErrType = tc.wantErrType

			// Initiate OpenID Connect Authorization Code flow.
			requestURL := mockSyncGatewayURL + tc.authURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request")
			if assertWantErrType(t, response, tc.wantErrType, grantTypeAuthCode) {
				return
			}
			// Validate received token response
			require.Equal(t, http.StatusOK, response.StatusCode)
			var receivedToken OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, "foo_noah", receivedToken.Username, "name mismatch")

			wantTokenResponse := mockAuthServer.options.tokenResponse
			assert.Equal(t, wantTokenResponse.IDToken, receivedToken.IDToken, "id_token mismatch")
			assert.Equal(t, wantTokenResponse.RefreshToken, receivedToken.RefreshToken, "refresh_token mismatch")
			if tc.providers["foo"].IncludeAccessToken {
				assert.Equal(t, wantTokenResponse.AccessToken, receivedToken.AccessToken, "access_token mismatch")
				assert.Equal(t, wantTokenResponse.TokenType, receivedToken.TokenType, "token_type mismatch")
				assert.True(t, wantTokenResponse.Expires >= receivedToken.Expires, "expires_in mismatch")
			}

			// Query db endpoint with Bearer token
			var responseBody map[string]interface{}
			dbEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", receivedToken.IDToken)
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])

			// Refresh auth token using the refresh token received from OP.
			mockAuthServer.options.grantType = grantTypeRefreshToken
			requestURL = mockSyncGatewayURL + "/db/_oidc_refresh?refresh_token=" + receivedToken.RefreshToken
			request, err = http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request")
			if assertWantErrType(t, response, tc.wantErrType, grantTypeRefreshToken) {
				return
			}
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Validate received token refresh response.
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			wantTokenResponse = mockAuthServer.options.tokenResponse
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, "foo_noah", receivedToken.Username, "name mismatch")
			assert.Equal(t, wantTokenResponse.IDToken, receivedToken.IDToken, "id_token mismatch")
			assert.Equal(t, wantTokenResponse.RefreshToken, receivedToken.RefreshToken, "refresh_token mismatch")
			if tc.providers["foo"].IncludeAccessToken {
				assert.Equal(t, wantTokenResponse.AccessToken, receivedToken.AccessToken, "access_token mismatch")
				assert.Equal(t, wantTokenResponse.TokenType, receivedToken.TokenType, "token_type mismatch")
				assert.True(t, wantTokenResponse.Expires >= receivedToken.Expires, "expires_in mismatch")
			}
			// Query db endpoint with Bearer token
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", receivedToken.IDToken)
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])
		})
	}
}

// The assertHttpResponse asserts the statusCode and bodyText against statusCode and string
// representation of the body bytes from the HTTP response.
func assertHttpResponse(t *testing.T, response *http.Response, statusCode int, bodyText string) {
	bodyBytes, err := ioutil.ReadAll(response.Body)
	require.NoError(t, err, "error reading response body")
	assert.Contains(t, string(bodyBytes), bodyText)
	assert.Equal(t, statusCode, response.StatusCode)
	require.NoError(t, response.Body.Close(), "error closing response body")
}

// refreshProviderConfig updates the issuer URL and sets the discovery endpoint
// in provider configuration.
func refreshProviderConfig(providers auth.OIDCProviderMap, issuer string) {
	for name, provider := range providers {
		provider.Issuer = issuer + "/" + name
		provider.DiscoveryURI = provider.Issuer + auth.OIDCDiscoveryConfigPath
	}
}

// assertWantErrType assert the given wantErrType on the response object. Return true if the wantErrType matches
// with the error in response and false otherwise.
func assertWantErrType(t *testing.T, response *http.Response, wantErrType wantErrType, grantType grantType) (exit bool) {
	switch wantErrType {
	case noAutoRegistrationErr:
		assertHttpResponse(t, response, http.StatusUnauthorized, "Invalid login")
		exit = true
	case noCodeErr:
		assertHttpResponse(t, response, http.StatusBadRequest, "Code must be present on oidc callback")
		exit = true
	case callbackErr:
		assertHttpResponse(t, response, http.StatusUnauthorized, "callback received an error")
		exit = true
	case untoldProviderErr:
		assertHttpResponse(t, response, http.StatusBadRequest, "Unable to identify provider for callback request")
		exit = true
	case notConfiguredProviderErr:
		assertHttpResponse(t, response, http.StatusBadRequest, "OpenID Connect not configured for database db")
		exit = true
	case discoveryErr:
		assertHttpResponse(t, response, http.StatusInternalServerError, "Unable to obtain client for provider")
		exit = true
	case callbackTokenExchangeErr:
		if grantType == grantTypeAuthCode {
			// assertHttpResponse(t, response, http.StatusUnauthorized, "Failed to exchange token")
			assertHttpResponse(t, response, http.StatusInternalServerError, "")
			exit = true
		}
	case callbackNoIDTokenErr:
		if grantType == grantTypeAuthCode {
			// assertHttpResponse(t, response, http.StatusUnauthorized, "No id_token field in oauth2 token")
			assertHttpResponse(t, response, http.StatusInternalServerError, "")
			exit = true
		}
	case refreshTokenExchangeErr:
		if grantType == grantTypeRefreshToken {
			assertHttpResponse(t, response, http.StatusUnauthorized, "Unable to refresh token")
			exit = true
		}
	case refreshNoIDTokenErr:
		if grantType == grantTypeRefreshToken {
			// assertHttpResponse(t, response, http.StatusUnauthorized, "No id_token field in oauth2 token")
			assertHttpResponse(t, response, http.StatusInternalServerError, "")
			exit = true
		}
	default:
		exit = false
	}
	return exit
}

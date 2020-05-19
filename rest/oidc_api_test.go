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

//  A forceError is being used when you want to force an error of type 'forceErrorType'
type forceError struct {
	errorType            forceErrorType // An error type to be forced
	expectedErrorCode    int            // Expected HTTP response code
	expectedErrorMessage string         // Expected HTTP response message
}

// A forceErrorType represents the specific type of forceError to be forced.
type forceErrorType int

const (

	// noCodeErr option forces the /auth API to return a callback URL
	// with no authorization code.
	noCodeErr forceErrorType = iota + 1

	// callbackErr option forces the /auth API to return an auth error
	// in the callback URL with a short error description.
	callbackErr

	// untoldProviderErr option forces the /auth API to return an unknown
	// provider (provider which is not configured) the callback URL.
	untoldProviderErr

	// discoveryErr forces the /.well-known/openid-configuration API to return an
	// error response during custom provider discovery.
	discoveryErr

	// callbackTokenExchangeErr forces the /token API to return an error
	// response while exchanging authorization code for an access token.
	callbackTokenExchangeErr

	// callbackNoIDTokenErr forces the /token API to return no ID token in
	// the response while exchanging authorization code for an access token.
	callbackNoIDTokenErr

	// refreshTokenExchangeErr forces the /token API to return an error
	// response during token refresh.
	refreshTokenExchangeErr

	// refreshNoIDTokenErr forces the /token API to return no ID token in
	// the response during token refresh.
	refreshNoIDTokenErr

	// noAutoRegistrationErr forces SG to return an authentication error when the
	// user tries authenticate through OpenID connect with auto registration is disabled.
	noAutoRegistrationErr

	// notConfiguredProviderErr forces SG to return an authentication error when the
	// user initiate an auth request with a provider which is not configured.
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

	// The options represents a set of custom options to be injected on mock auth
	// server to generate the response on demand.
	options *options
}

// options represents a set of settings to be configured on mock auth server
// to simulate the specific authentication behavior during test execution.
type options struct {

	// OpenID Connect  Provider Issuer URL
	issuer string

	// forceError represents a specific error expected by the relying party.
	forceError forceError

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
	router.HandleFunc("/{provider}"+auth.OIDCDiscoveryConfigPath, s.discoveryHandler).Methods(http.MethodGet)
	router.HandleFunc("/{provider}/auth", s.authHandler).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc("/{provider}/token", s.tokenHandler).Methods(http.MethodPost)
	s.server = httptest.NewServer(router)
	s.URL = s.server.URL
}

// Shutdown shuts down the underlying httptest server and blocks
// until all outstanding requests on that server have completed.
func (s *mockAuthServer) Shutdown() {
	s.server.Close()
}

// discoveryHandler mocks the provider discovery endpoint with a mock response.
// Makes a JSON document available at the path formed by concatenating the string
// /.well-known/openid-configuration to the Issuer.
func (s *mockAuthServer) discoveryHandler(w http.ResponseWriter, r *http.Request) {
	if s.options.forceError.errorType == discoveryErr {
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

// authHandler mocks the authentication process performed by the OAuth Authorization Server.
// The behavior of authHandler can be modified through the options map when needed.
// Clients are redirected to the specified callback URL after successful authentication.
func (s *mockAuthServer) authHandler(w http.ResponseWriter, r *http.Request) {
	var redirectionURL string
	state := r.URL.Query().Get(requestParamState)
	redirect := r.URL.Query().Get(requestParamRedirectURI)
	if redirect == "" {
		base.Errorf("No redirect URL found in auth request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if s.options.forceError.errorType == callbackErr {
		err := "?error=unsupported_response_type&error_description=response_type%20not%20supported"
		redirectionURL = fmt.Sprintf("%s?error=%s", redirect, err)
		http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
	}
	code := base.GenerateRandomSecret()
	if s.options.forceError.errorType == noCodeErr {
		code = ""
	}
	redirectionURL = fmt.Sprintf("%s?code=%s", redirect, code)
	if state != "" {
		redirectionURL = fmt.Sprintf("%s&state=%s", redirectionURL, state)
	}
	if s.options.forceError.errorType == untoldProviderErr {
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

// tokenHandler handles token handling performed by the OAuth Authorization Server.
// It mocks token response and makes it available as JSON document.
func (s *mockAuthServer) tokenHandler(w http.ResponseWriter, r *http.Request) {
	if (s.options.forceError.errorType == callbackTokenExchangeErr && s.options.grantType == grantTypeAuthCode) ||
		(s.options.forceError.errorType == refreshTokenExchangeErr && s.options.grantType == grantTypeRefreshToken) {
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
	if (s.options.grantType == grantTypeAuthCode && s.options.forceError.errorType == callbackNoIDTokenErr) ||
		(s.options.grantType == grantTypeRefreshToken && s.options.forceError.errorType == refreshNoIDTokenErr) {
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
			// Provider should be included in callback URL when the requested provider is not default.
			name:         "requested provider is not default",
			authURL:      "/db/${path}?provider=bar&offline=true",
			wantProvider: "bar",
			issuer:       "${path}/bar",
		}, {
			// Provider should NOT be included in callback URL when the requested provider is not default.
			name:    "requested provider is default",
			authURL: "/db/${path}?provider=foo&offline=true",
			issuer:  "${path}/foo",
		}, {
			// Provider should NOT be included in callback URL when no provider is requested.
			name:    "no provider is requested",
			authURL: "/db/${path}?offline=true",
			issuer:  "${path}/foo",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			providers := auth.OIDCProviderMap{"foo": mockProvider("foo"), "bar": mockProvider("bar")}
			openIDConnectOptions := auth.OIDCOptions{Providers: providers, DefaultProvider: base.StringPtr("foo")}
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

// Returns a new OIDCProvider.
func mockProvider(name string) *auth.OIDCProvider {
	return &auth.OIDCProvider{
		Name:          name,
		ClientID:      base.StringPtr("baz"),
		UserPrefix:    name,
		ValidationKey: base.StringPtr("qux"),
	}
}

// Returns an auto registration enabled provider.
func mockProviderWithRegister(name string) *auth.OIDCProvider {
	return &auth.OIDCProvider{
		Name:          name,
		ClientID:      base.StringPtr("baz"),
		UserPrefix:    name,
		ValidationKey: base.StringPtr("qux"),
		Register:      true,
	}
}

// Returns a new OIDCProvider with Register and IncludeAccessToken flags enabled.
func mockProviderWithRegisterWithAccessToken(name string) *auth.OIDCProvider {
	return &auth.OIDCProvider{
		Name:               name,
		ClientID:           base.StringPtr("baz"),
		UserPrefix:         name,
		ValidationKey:      base.StringPtr("qux"),
		Register:           true,
		IncludeAccessToken: true,
	}
}

// Returns a new OIDCProvider with IncludeAccessToken flags enabled.
func mockProviderWithAccessToken(name string) *auth.OIDCProvider {
	return &auth.OIDCProvider{
		Name:               name,
		ClientID:           base.StringPtr("baz"),
		UserPrefix:         name,
		ValidationKey:      base.StringPtr("qux"),
		IncludeAccessToken: true,
	}
}

// Checks End to end OpenID Connect Authorization Code flow.
func TestOpenIDConnectAuth(t *testing.T) {
	type test struct {
		name                string
		providers           auth.OIDCProviderMap
		defaultProvider     string
		authURL             string
		forceAuthError      forceError
		forceRefreshError   forceError
		requireExistingUser bool
	}
	tests := []test{
		{
			// Successful new user authentication against single provider
			// with auto registration and access token enabled.
			name: "successful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegisterWithAccessToken("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Unsuccessful new user authentication against single provider with
			// auto registration disabled.
			name: "unsuccessful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            noAutoRegistrationErr,
				expectedErrorCode:    http.StatusUnauthorized,
				expectedErrorMessage: "Invalid login",
			},
		}, {
			// Successful new user authentication against single provider with auto
			// registration enabled but access token NOT.
			name: "successful user registration against single provider without access token",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegister("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Successful new user authentication against multiple providers
			// with auto registration and access token enabled.
			name: "successful user registration against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegisterWithAccessToken("foo"),
				"bar": mockProviderWithRegisterWithAccessToken("bar"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Unsuccessful new user authentication against multiple providers with
			// auto registration disabled.
			name: "unsuccessful user registration against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
				"bar": mockProvider("bar"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            noAutoRegistrationErr,
				expectedErrorCode:    http.StatusUnauthorized,
				expectedErrorMessage: "Invalid login",
			},
		}, {
			// Successful new user authentication against multiple providers with auto
			// registration enabled but access token NOT.
			name: "successful user registration against multiple provider without access token",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegister("foo"),
				"bar": mockProviderWithRegister("bar"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Force /auth API NOT to return code in the callback URL and
			// make sure authentication is unsuccessful.
			name: "unsuccessful auth code received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            noCodeErr,
				expectedErrorCode:    http.StatusBadRequest,
				expectedErrorMessage: "Code must be present on oidc callback",
			},
		}, {
			// Force /auth API to return a callback error and make sure
			// authentication is unsuccessful.
			name: "unsuccessful auth callback error received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            callbackErr,
				expectedErrorCode:    http.StatusUnauthorized,
				expectedErrorMessage: "callback received an error",
			},
		}, {
			// Force /auth API to return an unknown provider in the callback
			// URL and make sure the authentication request is unsuccessful.
			name: "unsuccessful auth untold provider received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            untoldProviderErr,
				expectedErrorCode:    http.StatusBadRequest,
				expectedErrorMessage: "Unable to identify provider for callback request",
			},
		}, {
			// Make sure authentication is unsuccessful when authenticating against a provider
			// which is not configured; i.e., specify a different provider in the auth request URL.
			name: "unsuccessful auth against not configured provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=fred&offline=true", // Configured provider is 'foo', NOT 'fred'.
			forceAuthError: forceError{
				errorType:            notConfiguredProviderErr,
				expectedErrorCode:    http.StatusBadRequest,
				expectedErrorMessage: "OpenID Connect not configured for database",
			},
		}, {
			// Force discovery endpoint to return an error during provider discovery and make sure
			// the authentication is unsuccessful.
			name: "unsuccessful auth due to provider discovery failure",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            discoveryErr,
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: "Unable to obtain client for provider",
			},
		}, {
			// Make sure user registration is successful when access type is offline.
			name: "successful user registration with access type not offline",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegisterWithAccessToken("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=false",
		}, {
			// Force /token endpoint to return an error in callback URL while exchanging auth code
			// for a valid token. Make sure authentication is unsuccessful.
			name: "unsuccessful auth token exchange error",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            callbackTokenExchangeErr,
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: "",
			},
		}, {
			// Force /token endpoint NOT to return an id_token while exchanging auth code
			// for a valid token. Make sure authentication is unsuccessful.
			name: "unsuccessful auth no id token received from oauth server",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            callbackNoIDTokenErr,
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: "",
			},
		}, {
			// Force /token endpoint to return a token exchange error during token refresh.
			// Make sure initial auth request for user registration is successful but subsequent
			// token refresh request is NOT due to exchange error.
			name: "unsuccessful auth token exchange error during token refresh",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegisterWithAccessToken("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceRefreshError: forceError{
				errorType:            refreshTokenExchangeErr,
				expectedErrorCode:    http.StatusUnauthorized,
				expectedErrorMessage: "Unable to refresh token",
			},
		}, {
			// Force /token endpoint NOT to return an id_token during token refresh.
			// Make sure initial auth request for user registration is successful but subsequent
			// token refresh request is NOT due to no id_token in token response.
			name: "unsuccessful auth no id token received during token refresh",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithRegisterWithAccessToken("foo"),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceRefreshError: forceError{
				errorType:            refreshNoIDTokenErr,
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: "",
			},
		}, {
			name: "successful registered user authentication against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful registered user authentication against single provider with access token enabled",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithAccessToken("foo"),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful registered user authentication against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
				"bar": mockProvider("bar"),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful registered user authentication against multiple providers with access token enabled",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWithAccessToken("foo"),
				"bar": mockProviderWithAccessToken("bar"),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer := newMockAuthServer()
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()

			mockAuthServer.options.issuer = mockAuthServer.URL + "/" + tc.defaultProvider
			refreshProviderConfig(tc.providers, mockAuthServer.URL)

			opts := auth.OIDCOptions{Providers: tc.providers, DefaultProvider: &tc.defaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &opts}}
			restTester := NewRestTester(t, &restTesterConfig)
			defer restTester.Close()

			// Create the user first if the test requires a registered user.
			if tc.requireExistingUser {
				body := `{"name":"foo_noah", "password":"pass", "admin_channels":["foo"]}`
				userResponse := restTester.SendAdminRequest(http.MethodPut, "/db/_user/foo_noah", body)
				assertStatus(t, userResponse, http.StatusCreated)
				userResponse = restTester.SendAdminRequest(http.MethodGet, "/db/_user/foo_noah", "")
				assertStatus(t, userResponse, http.StatusOK)
			}

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL
			mockAuthServer.options.grantType = grantTypeAuthCode
			mockAuthServer.options.forceError = tc.forceAuthError

			// Initiate OpenID Connect Authorization Code flow.
			requestURL := mockSyncGatewayURL + tc.authURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request")
			if (forceError{}) != tc.forceAuthError {
				assertHttpResponse(t, response, tc.forceAuthError)
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
				assert.Equal(t, wantTokenResponse.Expires, receivedToken.Expires, "expires_in mismatch")
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
			mockAuthServer.options.forceError = tc.forceRefreshError
			mockAuthServer.options.grantType = grantTypeRefreshToken
			requestURL = mockSyncGatewayURL + "/db/_oidc_refresh?refresh_token=" + receivedToken.RefreshToken
			request, err = http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request")
			if (forceError{}) != tc.forceRefreshError {
				assertHttpResponse(t, response, tc.forceRefreshError)
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
				assert.Equal(t, wantTokenResponse.Expires, receivedToken.Expires, "expires_in mismatch")
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

// assertHttpResponse asserts the forceError against HTTP response.
func assertHttpResponse(t *testing.T, response *http.Response, forceError forceError) {
	bodyBytes, err := ioutil.ReadAll(response.Body)
	require.NoError(t, err, "error reading response body")
	assert.Contains(t, string(bodyBytes), forceError.expectedErrorMessage)
	assert.Equal(t, forceError.expectedErrorCode, response.StatusCode)
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

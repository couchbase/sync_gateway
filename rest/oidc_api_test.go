/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
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

	// invalidStateErr forces the /auth API to return an invalid state token in the
	// callback URL.
	invalidStateErr
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

// BearerToken is used for setting JWT token type as well as the
// prefix for the Authorization HTTP header.
const BearerToken = "Bearer"

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

	// signer represents a signer which takes a payload and produces a signed JWS object.
	signer jose.Signer

	// keys represents set of a public keys in JWK format that enable clients to validate
	// a JSON Web Token (JWT) issued by this OpenID Connect Provider.
	keys []jose.JSONWebKey
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

	// claims represents a set of primary and secondary claims to be used to create the ID token.
	claims claimSet
}

// The newMockAuthServer returns a new mock OAuth Server but doesn't start it.
// The caller should call Start when needed, to start it up.
func newMockAuthServer() (*mockAuthServer, error) {
	server := new(mockAuthServer)
	if err := server.setupSignerWithKeys(); err != nil {
		return nil, err
	}
	server.options = new(options)
	return server, nil
}

// setupSignerWithKeys sets up an RSA signer to sign the token and a set of public keys in JWK format
// that enable clients to validate a JSON Web Token (JWT) issued by this OpenID Connect Provider.
// Returns an error if there is any failure in generating RSA key pairs or creating a new RSA signer.
func (s *mockAuthServer) setupSignerWithKeys() error {
	rsaPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	signingKey := jose.SigningKey{Algorithm: jose.RS256, Key: rsaPrivateKey}
	var signerOptions = jose.SignerOptions{}
	signerOptions.WithType("JWT")
	s.signer, err = jose.NewSigner(signingKey, &signerOptions)
	if err != nil {
		return err
	}
	s.keys = []jose.JSONWebKey{{Key: &rsaPrivateKey.PublicKey, KeyID: "kid", Use: "sig"}}
	return nil
}

// Start registers mock handlers and starts the mock OAuth server.
// The caller should call Shutdown when finished, to shut it down.
func (s *mockAuthServer) Start() {
	router := mux.NewRouter()
	router.HandleFunc(auth.GetStandardDiscoveryEndpoint("/{provider}"), s.discoveryHandler).Methods(http.MethodGet)
	router.HandleFunc("/{provider}/auth", s.authHandler).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc("/{provider}/token", s.tokenHandler).Methods(http.MethodPost)
	router.HandleFunc("/{provider}/keys", s.keysHandler).Methods(http.MethodGet)
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
	metadata := auth.ProviderMetadata{
		Issuer:                           issuer,
		TokenEndpoint:                    issuer + "/token",
		JwksUri:                          issuer + "/keys",
		AuthorizationEndpoint:            issuer + "/auth",
		IdTokenSigningAlgValuesSupported: []string{"RS256"},
		TokenEndpointAuthMethodsSupported: []string{
			"client_secret_basic",
			"client_secret_post",
			"client_secret_jwt",
			"private_key_jwt",
			"none",
		},
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
		base.ErrorfCtx(r.Context(), "Error rendering JSON response: %s", err)
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
	if s.options.forceError.errorType == invalidStateErr {
		state = "aW52YWxpZCBzdGF0ZQo=" // Invalid state to simulate CSRF
	}
	redirect := r.URL.Query().Get(requestParamRedirectURI)
	if redirect == "" {
		base.ErrorfCtx(r.Context(), "No redirect URL found in auth request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if s.options.forceError.errorType == callbackErr {
		err := "?error=unsupported_response_type&error_description=response_type%20not%20supported"
		redirectionURL = fmt.Sprintf("%s?error=%s", redirect, err)
		http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
	}
	code, err := base.GenerateRandomSecret()
	if err != nil {
		base.ErrorfCtx(r.Context(), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
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
			base.ErrorfCtx(r.Context(), "error setting untold provider in mock callback URL")
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
	claims := s.options.claims
	if reflect.DeepEqual(s.options.claims, claimSet{}) {
		claims = claimsAuthentic()
	}
	token, err := s.makeToken(claims)
	if err != nil || token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	response := OIDCTokenResponse{
		IDToken:      token,
		AccessToken:  "7d1d234f5fde713a94454f268833adcd39835fe8",
		RefreshToken: "e08c77351221346153d09ff64c123b24fc4c1905",
		TokenType:    BearerToken,
		Expires:      300, // Expires in 5 minutes from when the response was generated.
	}
	if (s.options.grantType == grantTypeAuthCode && s.options.forceError.errorType == callbackNoIDTokenErr) ||
		(s.options.grantType == grantTypeRefreshToken && s.options.forceError.errorType == refreshNoIDTokenErr) {
		response.IDToken = ""
	}
	s.options.tokenResponse = response
	renderJSON(w, r, http.StatusOK, response)
}

// claimSet represents a set of public and private claim values.
type claimSet struct {
	primaryClaims   jwt.Claims             // primaryClaims represents public claim values.
	secondaryClaims map[string]interface{} // secondaryClaims represents private claim values.
}

// makeToken creates a default token with an expiry of 5 minutes.
func (s *mockAuthServer) makeToken(claimSet claimSet) (string, error) {
	primaryClaims := claimSet.primaryClaims
	secondaryClaims := claimSet.secondaryClaims
	if primaryClaims.Issuer == "" {
		primaryClaims.Issuer = s.options.issuer
	}
	builder := jwt.Signed(s.signer).Claims(primaryClaims).Claims(secondaryClaims)
	token, err := builder.CompactSerialize()
	if err != nil {
		base.ErrorfCtx(context.TODO(), "Error serializing token: %s", err)
		return "", err
	}
	return token, nil
}

// claimsAuthentic returns an authentic claim set that contains both primary
// and secondary claims.
func claimsAuthentic() claimSet {
	primaryClaims := jwt.Claims{
		ID:       "id0123456789",
		Subject:  "noah",
		Audience: jwt.Audience{"aud1", "aud2", "aud3", "baz"},
		IssuedAt: jwt.NewNumericDate(time.Now()),
		Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
	}
	secondaryClaims := map[string]interface{}{"email": "noah@foo.com"}
	return claimSet{
		primaryClaims:   primaryClaims,
		secondaryClaims: secondaryClaims,
	}
}

// claimsAuthenticWithUsernameClaim returns returns an authentic claim set after
// setting the given claim key and value as secondary claims.
func claimsAuthenticWithUsernameClaim(key string, value interface{}) claimSet {
	claims := claimsAuthentic()
	claims.secondaryClaims[key] = value
	return claims
}

// keysHandler exposes a set of of a public keys in JWK format that enable clients
// to validate a JSON Web Token (JWT) issued by this OpenID Connect Provider.
func (s *mockAuthServer) keysHandler(w http.ResponseWriter, r *http.Request) {
	renderJSON(w, r, http.StatusOK, jose.JSONWebKeySet{Keys: s.keys})
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
			rtConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{OIDCConfig: &openIDConnectOptions}}}
			rt := NewRestTester(t, &rtConfig)
			defer rt.Close()

			mockAuthServer, err := newMockAuthServer()
			require.NoError(t, err, "Error creating mock oauth2 server")
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

// mockProvider returns a new OIDCProvider.
func mockProvider(name string) *auth.OIDCProvider {
	return &auth.OIDCProvider{
		Name:          name,
		ClientID:      "baz",
		ValidationKey: base.StringPtr("qux"),
	}
}

// mockProviderWith returns an OIDCProvider with the given options applied.
func mockProviderWith(name string, options ...mockProviderOption) *auth.OIDCProvider {
	baseProvider := mockProvider(name)
	for _, opt := range options {
		opt.Apply(baseProvider)
	}
	return baseProvider
}

type mockProviderOption interface {
	Apply(provider *auth.OIDCProvider)
}

type mockProviderRegister struct{}

func (m mockProviderRegister) Apply(provider *auth.OIDCProvider) {
	provider.Register = true
}

type mockProviderIncludeAccessToken struct{}

func (m mockProviderIncludeAccessToken) Apply(provider *auth.OIDCProvider) {
	provider.IncludeAccessToken = true
}

type mockProviderDisableCallbackState struct{}

func (m mockProviderDisableCallbackState) Apply(provider *auth.OIDCProvider) {
	provider.DisableCallbackState = true
}

type mockProviderUsernameClaim struct {
	string
}

func (m mockProviderUsernameClaim) Apply(provider *auth.OIDCProvider) {
	provider.UsernameClaim = m.string
}

type mockProviderUserPrefix struct {
	string
}

func (m mockProviderUserPrefix) Apply(provider *auth.OIDCProvider) {
	provider.UserPrefix = m.string
}

// E2E test that checks OpenID Connect Authorization Code Flow.
func TestOpenIDConnectAuthCodeFlow(t *testing.T) {
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
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
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
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			// Successful new user authentication against multiple providers
			// with auto registration and access token enabled.
			name: "successful user registration against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
				"bar": mockProviderWith("bar", mockProviderRegister{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"bar"}),
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
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderUserPrefix{"foo"}),
				"bar": mockProviderWith("bar", mockProviderRegister{}, mockProviderUserPrefix{"bar"}),
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
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
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
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceRefreshError: forceError{
				errorType:            refreshTokenExchangeErr,
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: "Unable to refresh token",
			},
		}, {
			// Force /token endpoint NOT to return an id_token during token refresh.
			// Make sure initial auth request for user registration is successful but subsequent
			// token refresh request is NOT due to no id_token in token response.
			name: "unsuccessful auth no id token received during token refresh",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
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
				"foo": mockProviderWith("foo", mockProviderUserPrefix{"foo"}),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful registered user authentication against single provider with access token enabled",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful registered user authentication against multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderUserPrefix{"foo"}),
				"bar": mockProviderWith("bar", mockProviderUserPrefix{"bar"}),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful registered user authentication against multiple providers with access token enabled",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
				"bar": mockProviderWith("bar", mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"bar"}),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "successful new user authentication with callback state disabled",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderDisableCallbackState{}, mockProviderIncludeAccessToken{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
		}, {
			name: "unsuccessful new user authentication against csrf attack",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderIncludeAccessToken{}),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            invalidStateErr,
				expectedErrorCode:    http.StatusBadRequest,
				expectedErrorMessage: "State mismatch",
			},
		}, {
			name: "successful registered user authentication with callback state disabled",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderDisableCallbackState{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider:     "foo",
			authURL:             "/db/_oidc?provider=foo&offline=true",
			requireExistingUser: true,
		}, {
			name: "unsuccessful registered user authentication against csrf attack",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderIncludeAccessToken{}),
			},
			defaultProvider: "foo",
			authURL:         "/db/_oidc?provider=foo&offline=true",
			forceAuthError: forceError{
				errorType:            invalidStateErr,
				expectedErrorCode:    http.StatusBadRequest,
				expectedErrorMessage: "State mismatch",
			},
			requireExistingUser: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer, err := newMockAuthServer()
			require.NoError(t, err, "Error creating mock oauth2 server")
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()

			mockAuthServer.options.issuer = mockAuthServer.URL + "/" + tc.defaultProvider
			refreshProviderConfig(tc.providers, mockAuthServer.URL)

			opts := auth.OIDCOptions{Providers: tc.providers, DefaultProvider: &tc.defaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{OIDCConfig: &opts}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			// Create the user first if the test requires a registered user.
			if tc.requireExistingUser {
				createUser(t, restTester, "foo_noah")
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
			jar, err := cookiejar.New(nil)
			require.NoError(t, err, "Error creating new cookie jar")
			client := &http.Client{Jar: jar}
			response, err := client.Do(request)
			require.NoError(t, err, "Error sending request")
			if (forceError{}) != tc.forceAuthError {
				assertHttpResponse(t, response, tc.forceAuthError)
				return
			}
			// Validate received token response
			require.Equal(t, http.StatusOK, response.StatusCode)
			var authResponseActual OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&authResponseActual))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, authResponseActual.SessionID, "session_id doesn't exist")
			assert.Equal(t, "foo_noah", authResponseActual.Username, "name mismatch")

			authResponseExpected := mockAuthServer.options.tokenResponse
			assert.Equal(t, authResponseExpected.IDToken, authResponseActual.IDToken, "id_token mismatch")
			assert.Equal(t, authResponseExpected.RefreshToken, authResponseActual.RefreshToken, "refresh_token mismatch")
			if tc.providers["foo"].IncludeAccessToken {
				assert.Equal(t, authResponseExpected.AccessToken, authResponseActual.AccessToken, "access_token mismatch")
				assert.Equal(t, authResponseExpected.TokenType, authResponseActual.TokenType, "token_type mismatch")
				assert.True(t, (authResponseExpected.Expires-authResponseActual.Expires) <= 30, "expiry is not within 30 seconds of the expected value: expected %d got: %d", authResponseExpected.Expires, authResponseActual.Expires)
			}

			// Query db endpoint with Bearer token
			var responseBody map[string]interface{}
			dbEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", BearerToken+" "+authResponseActual.IDToken)
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])

			// Refresh auth token using the refresh token received from OP.
			mockAuthServer.options.forceError = tc.forceRefreshError
			mockAuthServer.options.grantType = grantTypeRefreshToken
			requestURL = mockSyncGatewayURL + "/db/_oidc_refresh?refresh_token=" + authResponseActual.RefreshToken
			request, err = http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request")
			if (forceError{}) != tc.forceRefreshError {
				assertHttpResponse(t, response, tc.forceRefreshError)
				return
			}
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Validate received token refresh response.
			var refreshResponseActual OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&refreshResponseActual))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			refreshResponseExpected := mockAuthServer.options.tokenResponse
			assert.NotEmpty(t, refreshResponseActual.SessionID, "session_id doesn't exist")
			assert.Equal(t, "foo_noah", refreshResponseActual.Username, "name mismatch")
			assert.Equal(t, refreshResponseExpected.IDToken, refreshResponseActual.IDToken, "id_token mismatch")
			if tc.providers["foo"].IncludeAccessToken {
				assert.Equal(t, refreshResponseExpected.AccessToken, refreshResponseActual.AccessToken, "access_token mismatch")
				assert.Equal(t, refreshResponseExpected.TokenType, refreshResponseActual.TokenType, "token_type mismatch")
				assert.True(t, (refreshResponseExpected.Expires-refreshResponseActual.Expires) <= 30, "expiry is not within 30 seconds of the expected value: expected %d got: %d", refreshResponseExpected.Expires, refreshResponseActual.Expires)
			}
			// Query db endpoint with Bearer token
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", BearerToken+" "+refreshResponseActual.IDToken)
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, json.NewDecoder(response.Body).Decode(&responseBody))
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
		provider.DiscoveryURI = auth.GetStandardDiscoveryEndpoint(provider.Issuer)
	}
}

// getCookie returns the specified cookie by name if it exists in the given
// set of cookies and nil otherwise.
func getCookie(cookies []*http.Cookie, name string) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}

// createUser creates a user with the specified name.
func createUser(t *testing.T, restTester *RestTester, name string) {
	body := fmt.Sprintf(`{"name":"%s", "password":"pass", "email":"%s@couchbase.com"}`, name, name)
	userEndpoint := fmt.Sprintf("/db/_user/%s", url.QueryEscape(name))
	response := restTester.SendAdminRequest(http.MethodPut, userEndpoint, body)
	assertStatus(t, response, http.StatusCreated)
	response = restTester.SendAdminRequest(http.MethodGet, userEndpoint, "")
	assertStatus(t, response, http.StatusOK)
}

// deleteUser deletes the specified user.
func deleteUser(t *testing.T, restTester *RestTester, name string) {
	userEndpoint := fmt.Sprintf("/db/_user/%s", name)
	response := restTester.SendAdminRequest(http.MethodDelete, userEndpoint, "")
	assertStatus(t, response, http.StatusOK)
	response = restTester.SendAdminRequest(http.MethodGet, userEndpoint, "")
	assertStatus(t, response, http.StatusNotFound)
}

// createOIDCRequest creates the request with the sessionEndpoint and token put in.
// sessionEndpoint should end with "/_session".
func createOIDCRequest(t *testing.T, sessionEndpoint string, token string) *http.Request {
	request, err := http.NewRequest(http.MethodPost, sessionEndpoint, strings.NewReader(`{}`))
	require.NoError(t, err, "Error creating new request")
	request.Header.Add("Authorization", BearerToken+" "+token)
	return request
}

// E2E test that checks OpenID Connect Implicit Flow.
func TestOpenIDConnectImplicitFlow(t *testing.T) {
	type test struct {
		name                string
		providers           auth.OIDCProviderMap
		defaultProvider     string
		expectedError       forceError
		requireExistingUser bool
	}
	tests := []test{
		{
			// Successful new user authentication against single provider
			// when auto registration is enabled.
			name: "successful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider: "foo",
		}, {
			// Unsuccessful new user authentication against single provider
			// when auto registration is NOT enabled.
			name: "unsuccessful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProvider("foo"),
			},
			defaultProvider: "foo",
			expectedError: forceError{
				expectedErrorCode:    http.StatusUnauthorized,
				expectedErrorMessage: "Invalid login",
			},
		}, {
			name: "successful registered user authentication against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderUserPrefix{"foo"}),
			},
			defaultProvider:     "foo",
			requireExistingUser: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer, err := newMockAuthServer()
			require.NoError(t, err, "Error creating mock oauth2 server")
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()
			mockAuthServer.options.issuer = mockAuthServer.URL + "/" + tc.defaultProvider
			refreshProviderConfig(tc.providers, mockAuthServer.URL)

			opts := auth.OIDCOptions{Providers: tc.providers, DefaultProvider: &tc.defaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{OIDCConfig: &opts}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			// Create the user first if the test requires a registered user.
			if tc.requireExistingUser {
				createUser(t, restTester, "foo_noah")
			}

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			token, err := mockAuthServer.makeToken(claimsAuthentic())
			require.NoError(t, err, "Error obtaining signed token from OpenID Connect provider")
			require.NotEmpty(t, token, "Empty token retrieved from OpenID Connect provider")
			sessionEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name + "/_session"

			request := createOIDCRequest(t, sessionEndpoint, token)
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")

			if (forceError{}) != tc.expectedError {
				assertHttpResponse(t, response, tc.expectedError)
				return
			}
			checkGoodAuthResponse(t, response, "foo_noah")
		})
	}
}

// checkGoodAuthResponse asserts expected session response values against the given response.
func checkGoodAuthResponse(t *testing.T, response *http.Response, username string) {
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, "application/json", response.Header.Get("Content-Type"))
	var responseBodyActual map[string]interface{}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&responseBodyActual))
	sessionCookie := getCookie(response.Cookies(), auth.DefaultCookieName)
	require.NotNil(t, sessionCookie, "No session cookie found")
	require.NoError(t, response.Body.Close(), "error closing response body")
	responseBodyExpected := map[string]interface{}{
		"authentication_handlers": []interface{}{
			"default", "cookie",
		},
		"ok": true,
		"userCtx": map[string]interface{}{
			"channels": map[string]interface{}{"!": float64(1)},
			"name":     username,
		},
	}
	assert.Equal(t, responseBodyExpected, responseBodyActual, "Session response mismatch")
}

// E2E test that checks OpenID Connect Implicit Flow edge cases.
func TestOpenIDConnectImplicitFlowEdgeCases(t *testing.T) {
	const emailClaim = "email"
	var username = "foo_noah"
	providers := auth.OIDCProviderMap{
		"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderUserPrefix{"foo"}),
	}
	defaultProvider := "foo"
	mockAuthServer, err := newMockAuthServer()
	require.NoError(t, err, "Error creating mock oauth2 server")
	mockAuthServer.Start()
	defer mockAuthServer.Shutdown()
	mockAuthServer.options.issuer = mockAuthServer.URL + "/" + defaultProvider
	refreshProviderConfig(providers, mockAuthServer.URL)

	opts := auth.OIDCOptions{Providers: providers, DefaultProvider: &defaultProvider}
	restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{OIDCConfig: &opts}}}
	restTester := NewRestTester(t, &restTesterConfig)
	require.NoError(t, restTester.SetAdminParty(false))
	defer restTester.Close()

	mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
	defer mockSyncGateway.Close()
	mockSyncGatewayURL := mockSyncGateway.URL
	authenticator := restTester.ServerContext().Database("db").Authenticator(base.TestCtx(t))

	sendAuthRequest := func(claimSet claimSet) (*http.Response, error) {
		token, err := mockAuthServer.makeToken(claimSet)
		require.NoError(t, err, "Error obtaining signed token from OpenID Connect provider")
		require.NotEmpty(t, token, "Empty token retrieved from OpenID Connect provider")
		sessionEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name + "/_session"
		request := createOIDCRequest(t, sessionEndpoint, token)
		return http.DefaultClient.Do(request)
	}

	runBadAuthTest := func(claimSet claimSet) {
		response, err := sendAuthRequest(claimSet)
		require.NoError(t, err, "Error sending request with bearer token")
		expectedAuthError := forceError{
			expectedErrorCode:    http.StatusUnauthorized,
			expectedErrorMessage: "Invalid login",
		}
		assertHttpResponse(t, response, expectedAuthError)
	}

	runGoodAuthTest := func(claimSet claimSet, username string) {
		response, err := sendAuthRequest(claimSet)
		require.NoError(t, err, "Error sending request with bearer token")
		checkGoodAuthResponse(t, response, username)
	}

	t.Run("new user authentication with an expired token", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Expiry = jwt.NewNumericDate(time.Now().Add(-5 * time.Minute))
		runBadAuthTest(claimSet)
	})

	t.Run("registered user authentication with an expired token", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Expiry = jwt.NewNumericDate(time.Now().Add(-5 * time.Minute))
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	t.Run("new user authentication with invalid audience claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Audience = jwt.Audience{"aud1", "aud2", "aud3", "qux"}
		runBadAuthTest(claimSet)
	})

	t.Run("registered user authentication with invalid audience claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Audience = jwt.Audience{"aud1", "aud2", "aud3", "qux"}
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	t.Run("new user authentication with no subject claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Subject = ""
		runBadAuthTest(claimSet)
	})

	t.Run("registered user authentication with no subject claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Subject = ""
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	t.Run("new user authentication with a bad email claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = "foo_noah@"
		runGoodAuthTest(claimSet, username)

		// Bad email shouldn't not be saved on successful authentication.
		user, err := restTester.ServerContext().Database("db").Authenticator(base.TestCtx(t)).GetUser("foo_noah")
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name())
		assert.Empty(t, user.Email(), "Bad email shouldn't be saved")
		deleteUser(t, restTester, username)
	})

	t.Run("registered user authentication with a bad email claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = "foo_noah@"
		runGoodAuthTest(claimSet, username)

		// Bad email shouldn't not be saved on successful authentication.
		user, err := restTester.ServerContext().Database("db").Authenticator(base.TestCtx(t)).GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name())
		assert.Equal(t, "foo_noah@couchbase.com", user.Email(), "Bad email shouldn't be saved")
		deleteUser(t, restTester, "foo_noah")
	})

	t.Run("registered user authentication with a good email claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = "foo_noah@example.com"
		runGoodAuthTest(claimSet, username)

		// Good email should be updated on successful authentication.
		user, err := restTester.ServerContext().Database("db").Authenticator(base.TestCtx(t)).GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name())
		assert.Equal(t, "foo_noah@example.com", user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
	})

	t.Run("new user authentication with bad issuer claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Issuer = "https://login.unknownissuer.com"
		runBadAuthTest(claimSet)
	})

	t.Run("registered user authentication with bad issuer claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.Issuer = "https://login.unknownissuer.com"
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	t.Run("new user authentication with future time nbf claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.NotBefore = jwt.NewNumericDate(time.Now().Add(5 * time.Minute))
		runBadAuthTest(claimSet)
	})

	t.Run("new user authentication with current time nbf claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.NotBefore = jwt.NewNumericDate(time.Now())
		runGoodAuthTest(claimSet, username)
		deleteUser(t, restTester, username)
	})

	t.Run("new user authentication with past time nbf claim", func(t *testing.T) {
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.NotBefore = jwt.NewNumericDate(time.Now().Add(-1 * time.Minute))
		runGoodAuthTest(claimSet, username)
		deleteUser(t, restTester, username)
	})

	t.Run("registered user authentication with future time nbf claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.NotBefore = jwt.NewNumericDate(time.Now().Add(5 * time.Minute))
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	t.Run("registered user authentication with current time nbf claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.NotBefore = jwt.NewNumericDate(time.Now())
		runGoodAuthTest(claimSet, username)
		deleteUser(t, restTester, username)
	})

	t.Run("registered user authentication with past time nbf claim", func(t *testing.T) {
		createUser(t, restTester, username)
		claimSet := claimsAuthentic()
		claimSet.primaryClaims.NotBefore = jwt.NewNumericDate(time.Now().Add(-1 * time.Minute))
		runGoodAuthTest(claimSet, username)
		deleteUser(t, restTester, username)
	})

	// If username_claim is set but user_prefix is not set, use that claim as the Sync Gateway username.
	t.Run("successful new user auth when username_claim is set but user_prefix is not set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		runGoodAuthTest(claimSet, username)
		user, err := authenticator.GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
	})

	t.Run("successful registered user auth when username_claim is set but user_prefix is not set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		createUser(t, restTester, username)
		runGoodAuthTest(claimSet, username)
		user, err := authenticator.GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
	})

	// If username_claim is set and user_prefix is also set, use [user_prefix]_[username_claim] as the Sync Gateway username.
	t.Run("successful new user auth when both username_claim and user_prefix are set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = "foo"
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		usernameExpected := provider.UserPrefix + "_" + username
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		runGoodAuthTest(claimSet, usernameExpected)
		user, err := authenticator.GetUser(usernameExpected)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, usernameExpected)
	})

	t.Run("successful registered user auth when both username_claim and user_prefix are set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = "foo"
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		usernameExpected := provider.UserPrefix + "_" + username
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		createUser(t, restTester, usernameExpected)
		runGoodAuthTest(claimSet, usernameExpected)
		user, err := authenticator.GetUser(usernameExpected)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, usernameExpected)
	})

	// If username_claim is not set and user_prefix is set, use [user_prefix]_[subject] as the Sync Gateway
	// username (existing behaviour).
	t.Run("successful new user auth when username_claim is not set but user_prefix is set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUserPrefix := provider.UserPrefix
		provider.UserPrefix = "foo"
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() { provider.UserPrefix = oldUserPrefix }()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		usernameExpected := provider.UserPrefix + "_" + claimSet.primaryClaims.Subject
		runGoodAuthTest(claimSet, usernameExpected)
		user, err := authenticator.GetUser(usernameExpected)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, usernameExpected)
	})

	t.Run("successful registered user auth when username_claim is not set but user_prefix is set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUserPrefix := provider.UserPrefix
		provider.UserPrefix = "foo"
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() { provider.UserPrefix = oldUserPrefix }()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		usernameExpected := provider.UserPrefix + "_" + claimSet.primaryClaims.Subject
		createUser(t, restTester, usernameExpected)
		runGoodAuthTest(claimSet, usernameExpected)
		user, err := authenticator.GetUser(usernameExpected)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, usernameExpected)
	})

	// If neither username_claim nor user_prefix are set, use [issuer]_[subject] as the Sync Gateway
	// username (existing behaviour).
	t.Run("successful new user auth when neither username_claim nor user_prefix are set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUserPrefix := provider.UserPrefix
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() { provider.UserPrefix = oldUserPrefix }()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		issuerURL, err := url.ParseRequestURI(provider.Issuer)
		require.NoError(t, err, "Error parsing issuer URL")
		usernameExpected := url.QueryEscape(issuerURL.Host+issuerURL.Path) + "_" + claimSet.primaryClaims.Subject
		runGoodAuthTest(claimSet, usernameExpected)
		user, err := authenticator.GetUser(usernameExpected)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		require.NoError(t, authenticator.DeleteUser(user), "Error deleting user %s", user.Name())
	})

	t.Run("successful registered user auth when neither username_claim nor user_prefix are set", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUserPrefix := provider.UserPrefix
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() { provider.UserPrefix = oldUserPrefix }()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username
		issuerURL, err := url.ParseRequestURI(provider.Issuer)
		require.NoError(t, err, "Error parsing issuer URL")
		usernameExpected := url.QueryEscape(issuerURL.Host+issuerURL.Path) + "_" + claimSet.primaryClaims.Subject
		user, err := authenticator.NewUser(usernameExpected, "password", nil)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		runGoodAuthTest(claimSet, usernameExpected)
		user, err = authenticator.GetUser(usernameExpected)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, usernameExpected, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		require.NoError(t, authenticator.DeleteUser(user), "Error deleting user %s", user.Name())
	})

	// If username_claim is set but the specified claim property does not exist in the token, then reject the token.
	t.Run("unsuccessful new user auth when username_claim is set but claim not found in token", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		runBadAuthTest(claimSet)
	})

	t.Run("unsuccessful registered user auth when username_claim is set but claim not found in token", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		createUser(t, restTester, username)
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	// If the username associated with an OIDC subject changes, Sync Gateway does not maintain any connection
	// between the previous user and the current user. If register=true, a new user will be created if it does
	// not exist. If register=false, the user must be created prior to successful authentication with the token.
	t.Run("successful user auth when username_claim is set with different value in token", func(t *testing.T) {
		username := "bar_alice"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		provider.Register = true
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username // Username in token
		createUser(t, restTester, "foo_alice")             // Registered username
		runGoodAuthTest(claimSet, username)
		user, err := authenticator.GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
		deleteUser(t, restTester, "foo_alice")
	})

	t.Run("unsuccessful user auth when username_claim is set with different value in token", func(t *testing.T) {
		username := "bar_alice"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		provider.Register = false
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = username // Username in token
		createUser(t, restTester, "foo_alice")             // Registered username
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, "foo_alice")
	})

	t.Run("unsuccessful new user auth when username_claim is set but claim is of illegal type in token",
		func(t *testing.T) {
			username := "80249751"
			usernameClaim := "gpid"
			email := username + "@example.com"
			provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
			oldUsernameClaim := provider.UsernameClaim
			oldUserPrefix := provider.UserPrefix
			provider.UsernameClaim = usernameClaim
			provider.UserPrefix = ""
			require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
			defer func() {
				provider.UsernameClaim = oldUsernameClaim
				provider.UserPrefix = oldUserPrefix
			}()
			claimSet := claimsAuthentic()
			claimSet.secondaryClaims[emailClaim] = email
			claimSet.secondaryClaims[usernameClaim] = []int{80249751}
			runBadAuthTest(claimSet)
		})

	t.Run("unsuccessful registered user auth when username_claim is set but claim is of illegal type in token", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = []int{80249751}
		createUser(t, restTester, username)
		runBadAuthTest(claimSet)
		deleteUser(t, restTester, username)
	})

	t.Run("successful new user auth when username_claim is set and claim is of legal float64 type in token", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		provider.Register = true
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = float64(80249751)
		runGoodAuthTest(claimSet, username)
		user, err := authenticator.GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
	})

	t.Run("successful registered user auth when username_claim is set and claim is of legal float64 type in token",
		func(t *testing.T) {
			username := "80249751"
			usernameClaim := "gpid"
			email := username + "@example.com"
			provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
			oldUsernameClaim := provider.UsernameClaim
			oldUserPrefix := provider.UserPrefix
			provider.UsernameClaim = usernameClaim
			provider.UserPrefix = ""
			require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
			defer func() {
				provider.UsernameClaim = oldUsernameClaim
				provider.UserPrefix = oldUserPrefix
			}()
			claimSet := claimsAuthentic()
			claimSet.secondaryClaims[emailClaim] = email
			claimSet.secondaryClaims[usernameClaim] = float64(80249751)
			createUser(t, restTester, username)
			runGoodAuthTest(claimSet, username)
			user, err := authenticator.GetUser(username)
			require.NoError(t, err, "Error getting user from db")
			assert.Equal(t, username, user.Name(), "Username mismatch")
			assert.Equal(t, email, user.Email(), "Email is not updated")
			deleteUser(t, restTester, username)
		})

	t.Run("successful new user auth when username_claim is set and claim is of legal json.Number type in token", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		provider.Register = true
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = json.Number("80249751")
		runGoodAuthTest(claimSet, username)
		user, err := authenticator.GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
	})

	t.Run("successful registered user auth when username_claim is set and claim is of legal json.Number type in token", func(t *testing.T) {
		username := "80249751"
		usernameClaim := "gpid"
		email := username + "@example.com"
		provider := restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
		oldUsernameClaim := provider.UsernameClaim
		oldUserPrefix := provider.UserPrefix
		provider.UsernameClaim = usernameClaim
		provider.UserPrefix = ""
		provider.Register = true
		require.NoError(t, provider.InitUserPrefix(base.TestCtx(t)), "Error initializing user_prefix")
		defer func() {
			provider.UsernameClaim = oldUsernameClaim
			provider.UserPrefix = oldUserPrefix
		}()
		claimSet := claimsAuthentic()
		claimSet.secondaryClaims[emailClaim] = email
		claimSet.secondaryClaims[usernameClaim] = json.Number("80249751")
		createUser(t, restTester, username)
		runGoodAuthTest(claimSet, username)
		user, err := authenticator.GetUser(username)
		require.NoError(t, err, "Error getting user from db")
		assert.Equal(t, username, user.Name(), "Username mismatch")
		assert.Equal(t, email, user.Email(), "Email is not updated")
		deleteUser(t, restTester, username)
	})
}

// Checks callback state cookie persistence across requests.
func TestCallbackStateClientCookies(t *testing.T) {
	providers := auth.OIDCProviderMap{
		"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderUserPrefix{"foo"}),
	}
	authURL := "/db/_oidc?provider=foo&offline=true"
	defaultProvider := "foo"
	mockAuthServer, err := newMockAuthServer()
	require.NoError(t, err, "Error creating mock oauth2 server")
	mockAuthServer.Start()
	defer mockAuthServer.Shutdown()
	mockAuthServer.options.issuer = mockAuthServer.URL + "/" + defaultProvider
	refreshProviderConfig(providers, mockAuthServer.URL)

	opts := auth.OIDCOptions{
		Providers:       providers,
		DefaultProvider: &defaultProvider,
	}
	restTesterConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			OIDCConfig: &opts,
		}},
	}
	restTester := NewRestTester(t, &restTesterConfig)
	require.NoError(t, restTester.SetAdminParty(false))
	defer restTester.Close()

	mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
	defer mockSyncGateway.Close()
	mockSyncGatewayURL := mockSyncGateway.URL

	requestURL := mockSyncGatewayURL + authURL
	request, err := http.NewRequest(http.MethodGet, requestURL, nil)
	require.NoError(t, err, "Error creating new request")

	t.Run("unsuccessful auth when callback state enabled with no cookies support from client", func(t *testing.T) {
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err, "Error sending request")
		expectedAuthError := forceError{
			expectedErrorCode:    http.StatusBadRequest,
			expectedErrorMessage: ErrNoStateCookie.Message,
		}
		assertHttpResponse(t, response, expectedAuthError)
	})

	t.Run("successful auth when callback state enabled with cookies support from client", func(t *testing.T) {
		jar, err := cookiejar.New(nil)
		require.NoError(t, err, "Error creating new cookie jar")
		client := &http.Client{Jar: jar}
		response, err := client.Do(request)
		require.NoError(t, err, "Error sending request")
		require.Equal(t, http.StatusOK, response.StatusCode)
		var authResponseActual OIDCTokenResponse
		require.NoError(t, err, json.NewDecoder(response.Body).Decode(&authResponseActual))
		require.NoError(t, response.Body.Close(), "Error closing response body")
		assert.NotEmpty(t, authResponseActual.SessionID, "session_id doesn't exist")
		assert.Equal(t, "foo_noah", authResponseActual.Username, "name mismatch")
	})

	t.Run("successful auth when callback state disabled with no cookies support from client", func(t *testing.T) {
		restTester.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider().DisableCallbackState = true
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err, "Error sending request")
		require.Equal(t, http.StatusOK, response.StatusCode)

		var authResponseActual OIDCTokenResponse
		require.NoError(t, err, json.NewDecoder(response.Body).Decode(&authResponseActual))
		require.NoError(t, response.Body.Close(), "Error closing response body")
		assert.NotEmpty(t, authResponseActual.SessionID, "session_id doesn't exist")
		assert.Equal(t, "foo_noah", authResponseActual.Username, "name mismatch")
	})
}

// E2E test that checks OpenID Connect Authorization Code Flow with the specified username_claim
// as Sync Gateway username.
func TestOpenIDConnectAuthCodeFlowWithUsernameClaim(t *testing.T) {
	var (
		defaultProvider = "foo"
		authURL         = "/db/_oidc?provider=foo&offline=true"
		claimKey        = "uuid"
		claimValue      = "80249751"
	)
	type test struct {
		name                  string
		providers             auth.OIDCProviderMap
		authErrorExpected     forceError
		requireRegisteredUser bool
		// can contain "$issuer", which will be replaced with the test issuer
		registeredUsername string
		claims             claimSet
		// can contain "$issuer", which will be replaced with the test issuer
		usernameExpected string
	}
	tests := []test{
		{
			name: "successful new user auth when username_claim is set but user_prefix is not set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims:           claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected: claimValue,
		}, {
			name: "successful registered user auth when username_claim is set but user_prefix is not set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderUsernameClaim{claimKey}),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected:      claimValue,
			registeredUsername:    claimValue,
			requireRegisteredUser: true,
		}, {
			name: "successful new user auth when both username_claim and user_prefix are set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}, mockProviderUserPrefix{defaultProvider}),
			},
			claims:           claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected: "foo_80249751",
		}, {
			name: "successful registered user auth when both username_claim and user_prefix are set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderUsernameClaim{claimKey}, mockProviderUserPrefix{defaultProvider}),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			requireRegisteredUser: true,
			registeredUsername:    "foo_80249751",
			usernameExpected:      "foo_80249751",
		}, {
			name: "successful new user auth when username_claim is not set but user_prefix is set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUserPrefix{defaultProvider}),
			},
			claims:           claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected: "foo_noah",
		}, {
			name: "successful registered user auth when username_claim is not set but user_prefix is set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderUserPrefix{defaultProvider}),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected:      "foo_noah",
			registeredUsername:    "foo_noah",
			requireRegisteredUser: true,
		}, {
			name: "successful new user auth when neither username_claim nor user_prefix are set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}),
			},
			claims:           claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected: `$issuer_noah`,
		}, {
			name: "successful registered user auth when neither username_claim nor user_prefix are set",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProvider(defaultProvider),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			usernameExpected:      `$issuer_noah`,
			registeredUsername:    `$issuer_noah`,
			requireRegisteredUser: true,
		}, {
			name: "unsuccessful new user auth when username_claim is set but claim not found in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims: claimsAuthentic(),
			authErrorExpected: forceError{
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: `specified claim \"uuid\" not found in id_token`,
			},
		}, {
			name: "unsuccessful registered user auth when username_claim is set but claim not found in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderUsernameClaim{claimKey}),
			},
			claims:                claimsAuthentic(),
			requireRegisteredUser: true,
			registeredUsername:    "foo_noah",
			usernameExpected:      "foo_noah",
			authErrorExpected: forceError{
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: `specified claim \"uuid\" not found in id_token`,
			},
		}, {
			name: "successful user auth when username_claim is set with different value in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			requireRegisteredUser: true,
			registeredUsername:    "foo_noah",
			usernameExpected:      claimValue,
		}, {
			name: "unsuccessful user auth when username_claim is set with different value in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderUsernameClaim{claimKey}),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, claimValue),
			requireRegisteredUser: true,
			registeredUsername:    "foo_noah",
			authErrorExpected: forceError{
				expectedErrorCode:    http.StatusUnauthorized,
				expectedErrorMessage: "Invalid login",
			},
		}, {
			name: "unsuccessful new user auth when username_claim is set but claim is of illegal type in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims: claimsAuthenticWithUsernameClaim(claimKey, []int{80249751}),
			authErrorExpected: forceError{
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: `oidc: can't treat value of type: []interface {} as valid username`,
			},
		}, {
			name: "unsuccessful registered user auth when username_claim is set but claim is of illegal type in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims: claimsAuthenticWithUsernameClaim(claimKey, []int{80249751}),
			authErrorExpected: forceError{
				expectedErrorCode:    http.StatusInternalServerError,
				expectedErrorMessage: `oidc: can't treat value of type: []interface {} as valid username`,
			},
			requireRegisteredUser: true,
			registeredUsername:    "foo_noah",
		}, {
			name: "successful new user auth when username_claim is set but claim is of legal type in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims:           claimsAuthenticWithUsernameClaim(claimKey, 80249751),
			usernameExpected: "80249751",
		}, {
			name: "successful registered user auth when username_claim is set but claim is of legal type in token",
			providers: auth.OIDCProviderMap{
				defaultProvider: mockProviderWith(defaultProvider, mockProviderRegister{}, mockProviderUsernameClaim{claimKey}),
			},
			claims:                claimsAuthenticWithUsernameClaim(claimKey, 80249751),
			requireRegisteredUser: true,
			registeredUsername:    "80249751",
			usernameExpected:      "80249751",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer, err := newMockAuthServer()
			require.NoError(t, err, "Error creating mock oauth2 server")
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()
			mockAuthServer.options.issuer = mockAuthServer.URL + "/" + defaultProvider
			refreshProviderConfig(tc.providers, mockAuthServer.URL)

			opts := auth.OIDCOptions{
				Providers:       tc.providers,
				DefaultProvider: &defaultProvider,
			}
			restTesterConfig := RestTesterConfig{
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					OIDCConfig: &opts,
				}},
			}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			issuerURL, err := url.Parse(mockAuthServer.options.issuer)
			require.NoError(t, err, "invalid issuer URL")

			// Create the user first if the test requires a registered user.
			if tc.requireRegisteredUser {
				registeredUsername := tc.registeredUsername
				if strings.Contains(registeredUsername, "$issuer") {
					// Note: createUser will URL-encode the username again. The double-encoding is intentional, otherwise
					// gorilla/mux will un-escape the slash and 404.
					// The Host+Path concatenation (rather than using the URL verbatim) is to match the behaviour of getOIDCUsername.
					registeredUsername = url.QueryEscape(strings.Replace(registeredUsername, "$issuer", issuerURL.Host+issuerURL.Path, -1))
				}
				createUser(t, restTester, registeredUsername)
			}
			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL
			mockAuthServer.options.claims = tc.claims

			// Initiate OpenID Connect Authorization Code flow.
			requestURL := mockSyncGatewayURL + authURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			jar, err := cookiejar.New(nil)
			require.NoError(t, err, "Error creating new cookie jar")
			client := &http.Client{Jar: jar}
			response, err := client.Do(request)
			require.NoError(t, err, "Error sending request")
			if (forceError{}) != tc.authErrorExpected {
				assertHttpResponse(t, response, tc.authErrorExpected)
				return
			}
			// Validate received token response
			require.Equal(t, http.StatusOK, response.StatusCode)
			var authResponseActual OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&authResponseActual))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, authResponseActual.SessionID, "session_id doesn't exist")
			expectedUsername := tc.usernameExpected
			if strings.Contains(expectedUsername, "$issuer") {
				expectedUsername = url.QueryEscape(strings.Replace(expectedUsername, "$issuer", issuerURL.Host+issuerURL.Path, -1))
			}
			assert.Equal(t, expectedUsername, authResponseActual.Username, "name mismatch")

			authResponseExpected := mockAuthServer.options.tokenResponse
			assert.Equal(t, authResponseExpected.IDToken, authResponseActual.IDToken, "id_token mismatch")
			assert.Equal(t, authResponseExpected.RefreshToken, authResponseActual.RefreshToken, "refresh_token mismatch")

			// Query db endpoint with Bearer token
			var responseBody map[string]interface{}
			dbEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", BearerToken+" "+authResponseActual.IDToken)
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])
		})
	}
}

// CBG-1378 - test when OIDC provider is not reachable, and then becomes reachable
// at a later request
func TestEventuallyReachableOIDCClient(t *testing.T) {
	// Modified copy of TestOpenIDConnectImplicitFlow
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	unreachableAddr := "http://0.0.0.0"
	tests := []struct {
		name                string
		providers           auth.OIDCProviderMap
		defaultProvider     string
		requireExistingUser bool
	}{
		{
			// Successful new user authentication against single provider
			// when auto registration is enabled.
			name: "successful user registration against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderRegister{}, mockProviderUserPrefix{"foo"}),
			},
			defaultProvider: "foo",
		},
		{
			name: "successful registered user authentication against single provider",
			providers: auth.OIDCProviderMap{
				"foo": mockProviderWith("foo", mockProviderUserPrefix{"foo"}),
			},
			defaultProvider:     "foo",
			requireExistingUser: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer, err := newMockAuthServer()
			require.NoError(t, err, "Error creating mock oauth2 server")
			mockAuthServer.Start()
			defer mockAuthServer.Shutdown()
			mockAuthServer.options.issuer = mockAuthServer.URL + "/" + tc.defaultProvider
			refreshProviderConfig(tc.providers, unreachableAddr)

			opts := auth.OIDCOptions{Providers: tc.providers, DefaultProvider: &tc.defaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{OIDCConfig: &opts}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			// Create the user first if the test requires a registered user.
			if tc.requireExistingUser {
				createUser(t, restTester, "foo_noah")
			}

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			token, err := mockAuthServer.makeToken(claimsAuthentic())
			require.NoError(t, err, "Error obtaining signed token from OpenID Connect provider")
			require.NotEmpty(t, token, "Empty token retrieved from OpenID Connect provider")
			sessionEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name + "/_session"

			// Unreachable
			request := createOIDCRequest(t, sessionEndpoint, token)
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			assert.Equal(t, http.StatusUnauthorized, response.StatusCode) // Status code when unreachable

			// Now reachable - success
			refreshProviderConfig(restTester.DatabaseConfig.OIDCConfig.Providers, mockAuthServer.URL)
			request = createOIDCRequest(t, sessionEndpoint, token)
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			checkGoodAuthResponse(t, response, "foo_noah")

			// Unreachable again after being reachable - still success
			refreshProviderConfig(restTester.DatabaseConfig.OIDCConfig.Providers, unreachableAddr)
			request = createOIDCRequest(t, sessionEndpoint, token)
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			checkGoodAuthResponse(t, response, "foo_noah")
		})
	}
}

package rest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
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
	// An empty options map is allocated with enough space to hold the specified
	// number of elements when mock auth server is created.
	options map[string]interface{}
}

// The newMockAuthServer returns a new mock OAuth Server but doesn't start it.
// The caller should call Start when needed, to start it up.
func newMockAuthServer() *mockAuthServer {
	server := new(mockAuthServer)
	server.options = make(map[string]interface{})
	return server
}

// Options returns the options map encapsulated on mock auth server.
// An empty options map is initialized when server is created.
func (s *mockAuthServer) Options() map[string]interface{} {
	return s.options
}

// Option returns the element with the specified key
// (options[key]) from the options map on mock auth server.
// The value may be nil if the specified key doesn't exist.
func (s *mockAuthServer) Option(key string) interface{} {
	return s.options[key]
}

// BoolOption returns the element with the specified key
// (options[key]) as bool from the options map on mock auth server.
// Returns true if specified key exists and false otherwise.
func (s *mockAuthServer) BoolOption(key string) bool {
	val, ok := s.options[key]
	return ok && val.(bool)
}

// StringOption returns the element with the specified key
// (options[key]) as string from the options map on mock auth
// server. The returned option value may be empty, but not nil.
func (s *mockAuthServer) StringOption(key string) string {
	if val, ok := s.options[key]; ok {
		return val.(string)
	}
	return ""
}

// DeleteOption add the element with the specified key
// (options[key]=value) to the options map on mock auth server.
func (s *mockAuthServer) SetOption(k string, v interface{}) {
	s.options[k] = v
}

// DeleteOption deletes the element with the specified key
// (options[key]) from the options map on mock auth server.
// If there is no such element, DeleteOption is a no-op.
func (s *mockAuthServer) DeleteOption(key string) {
	delete(s.options, key)
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
	if s.BoolOption("wantDiscoveryError") {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	issuer := s.StringOption("issuer")
	metadata := auth.ProviderMetadata{
		Issuer:                           issuer,
		TokenEndpoint:                    issuer + "/token",
		JwksUri:                          issuer + "/oauth2/v3/certs",
		AuthorizationEndpoint:            issuer + "/auth",
		IdTokenSigningAlgValuesSupported: []string{"RS256"},
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
	if s.BoolOption("invalidateState") {
		state = "aW52YWxpZCBzdGF0ZQo=" // Invalid state to simulate CSRF
	}
	redirect := r.URL.Query().Get(requestParamRedirectURI)
	if redirect == "" {
		base.Errorf("No redirect URL found in auth request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if s.BoolOption("wantCallbackError") {
		err := "?error=unsupported_response_type&error_description=response_type%20not%20supported"
		redirectionURL = fmt.Sprintf("%s?error=%s", redirect, err)
		http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
	}
	code := base.GenerateRandomSecret()
	if s.BoolOption("wantNoCode") {
		code = ""
	}
	redirectionURL = fmt.Sprintf("%s?code=%s", redirect, code)
	if state != "" {
		redirectionURL = fmt.Sprintf("%s&state=%s", redirectionURL, state)
	}
	if s.BoolOption("wantUntoldProvider") {
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
	if s.BoolOption("wantTokenExchangeError") {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	issuer := s.StringOption("issuer")
	claims := jwt.Claims{ID: "id0123456789", Issuer: issuer,
		Audience: jwt.Audience{"aud1", "aud2", "aud3", "baz"},
		IssuedAt: jwt.NewNumericDate(time.Now()), Subject: "noah",
		Expiry: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
	}
	signer, err := base.GetRSASigner()
	if err != nil {
		base.Errorf("Error creating RSA signer: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
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
	if s.BoolOption("wantNoIDToken") {
		response.IDToken = ""
	}
	s.SetOption("wantTokenResponse", response)
	renderJSON(w, r, http.StatusOK, response)
}

// Verifies OpenID Connect callback URL in redirect link is returned in the Location
// header for both oidc and _oidc_challenge requests.
func TestGetOIDCCallbackURL(t *testing.T) {
	mockAuthServer := newMockAuthServer()
	mockAuthServer.Start()
	defer mockAuthServer.Shutdown()

	issuerFoo := mockAuthServer.URL + "/foo"
	issuerBar := mockAuthServer.URL + "/bar"

	// Default OpenID Connect Provider
	providerFoo := auth.OIDCProvider{
		Name:          "foo",
		Issuer:        issuerFoo,
		ClientID:      "baz",
		ValidationKey: base.StringPointer("qux"),
		DiscoveryURI:  issuerFoo + auth.OIDCDiscoveryConfigPath,
	}

	// Non-default OpenID Connect Provider
	providerBar := auth.OIDCProvider{
		Name:          "bar",
		Issuer:        issuerBar,
		ClientID:      "baz",
		ValidationKey: base.StringPointer("qux"),
		DiscoveryURI:  issuerBar + auth.OIDCDiscoveryConfigPath,
	}

	providers := auth.OIDCProviderMap{"foo": &providerFoo, "bar": &providerBar}
	openIDConnectOptions := auth.OIDCOptions{Providers: providers, DefaultProvider: &providerFoo.Name}
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &openIDConnectOptions}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

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
			authURL:      "/db/_oidc?provider=bar&offline=true",
			wantProvider: "bar",
			issuer:       issuerBar,
		}, {
			name:         "oidc challenge default provider specified but current provider is not default",
			authURL:      "/db/_oidc_challenge?provider=bar&offline=true",
			wantProvider: "bar",
			issuer:       issuerBar,
		}, {
			// When multiple providers are defined, default provider is specified and the current provider is
			// default, then current provider should NOT be added to the generated OpenID Connect callback URL.
			name:    "oidc default provider specified and current provider is default",
			authURL: "/db/_oidc?provider=foo&offline=true",
			issuer:  issuerFoo,
		}, {
			name:    "oidc challenge default provider specified and current provider is default",
			authURL: "/db/_oidc_challenge?provider=foo&offline=true",
			issuer:  issuerFoo,
		}, {
			// When multiple providers are defined, default provider is specified and no current provider is
			// provided, then provider name should NOT be added to the generated OpenID Connect callback URL.
			name:    "oidc default provider specified with no current provider",
			authURL: "/db/_oidc?offline=true",
			issuer:  issuerFoo,
		}, {
			name:    "oidc challenge default provider specified with no current provider",
			authURL: "/db/_oidc_challenge?offline=true",
			issuer:  issuerFoo,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAuthServer.SetOption("issuer", tc.issuer)
			var location string
			if strings.Contains(tc.authURL, "_oidc_challenge") {
				resp := rt.SendAdminRequest(http.MethodGet, tc.authURL, "")
				require.Equal(t, http.StatusUnauthorized, resp.Code)
				wwwAuthHeader := resp.Header().Get("Www-Authenticate")
				location = regexp.MustCompile(`login="(?P<login>.*?)"`).FindStringSubmatch(wwwAuthHeader)[1]
			} else {
				resp := rt.SendAdminRequest(http.MethodGet, tc.authURL, "")
				require.Equal(t, http.StatusFound, resp.Code)
				location = resp.Header().Get(headerLocation)
			}
			require.NotEmpty(t, location, "error extracting location from header")
			locationURL, err := url.Parse(location)
			require.NoError(t, err, "error parsing location URL")
			redirectURI := locationURL.Query().Get(requestParamRedirectURI)
			redirectURL, err := url.Parse(redirectURI)
			require.NoError(t, err, "error parsing redirect_uri URL")
			assert.Equal(t, tc.wantProvider, redirectURL.Query().Get(auth.OIDCAuthProvider))
		})
	}
}

// Checks End to end OIDC Authorization Code flow.
func TestOpenIDConnectAuth(t *testing.T) {
	mockAuthServer := newMockAuthServer()
	mockAuthServer.Start()
	defer mockAuthServer.Shutdown()

	issuerFoo := mockAuthServer.URL + "/foo"
	mockAuthServer.SetOption("issuer", issuerFoo)

	type test struct {
		name                      string
		providers                 auth.OIDCProviderMap
		defaultProvider           string
		authURL                   string
		register                  bool
		includeAccessToken        bool
		notConfiguredProvider     bool
		wantUsername              string
		wantNoCode                bool
		wantCallbackError         bool
		wantUntoldProvider        bool
		wantDiscoveryError        bool
		wantCallbackTokenExgError bool
		wantRefreshTokenExgError  bool
		wantCallbackNoIDToken     bool
		wantRefreshNoIDToken      bool
	}
	tests := []test{
		{
			name: "new user auto registration enabled single provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: true,
		}, {
			name: "new user auto registration disabled single provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           false,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           false,
			includeAccessToken: true,
		}, {
			name: "new user auto registration enabled no access token single provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: false,
		}, {
			name: "new user auto registration enabled multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
				"bar": &auth.OIDCProvider{
					Name:               "bar",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "bar",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: true,
		}, {
			name: "new user auto registration disabled multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           false,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
				"bar": &auth.OIDCProvider{
					Name:               "bar",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "bar",
					ValidationKey:      base.StringPointer("qux"),
					Register:           false,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           false,
			includeAccessToken: true,
		}, {
			name: "new user auto registration disabled no access token multiple providers",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           false,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
				"bar": &auth.OIDCProvider{
					Name:               "bar",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "bar",
					ValidationKey:      base.StringPointer("qux"),
					Register:           false,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           false,
			includeAccessToken: false,
		}, {
			name: "auth code missing in callback URL received from auth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: true,
			wantNoCode:         true,
		}, {
			name: "callback error received from auth server via callback URL",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: true,
			wantCallbackError:  true,
		}, {
			name: "untold provider received from auth server via callback URL",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: true,
			wantUntoldProvider: true,
		}, {
			name: "initiate auth request with not configured provider",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:       "foo",
			wantUsername:          "foo_noah",
			authURL:               "/db/_oidc?provider=fred&offline=true", // Configured provider is 'foo', NOT 'fred'.
			register:              true,
			includeAccessToken:    true,
			notConfiguredProvider: true,
		}, {
			name: "auth request with discovery failure from OAuth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=true",
			register:           true,
			includeAccessToken: true,
			wantDiscoveryError: true,
		}, {
			name: "auth request with access type not offline",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:    "foo",
			wantUsername:       "foo_noah",
			authURL:            "/db/_oidc?provider=foo&offline=false",
			register:           true,
			includeAccessToken: true,
		}, {
			name: "token exchange error from OAuth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:           "foo",
			wantUsername:              "foo_noah",
			authURL:                   "/db/_oidc?provider=foo&offline=true",
			register:                  true,
			includeAccessToken:        true,
			wantCallbackTokenExgError: true,
		}, {
			name: "no ID token received from OAuth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:       "foo",
			wantUsername:          "foo_noah",
			authURL:               "/db/_oidc?provider=foo&offline=true",
			register:              true,
			includeAccessToken:    true,
			wantCallbackNoIDToken: true,
		}, {
			name: "token refresh token exchange error from OAuth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:          "foo",
			wantUsername:             "foo_noah",
			authURL:                  "/db/_oidc?provider=foo&offline=true",
			register:                 true,
			includeAccessToken:       true,
			wantRefreshTokenExgError: true,
		}, {
			name: "token refresh no ID token received from OAuth server",
			providers: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			defaultProvider:      "foo",
			wantUsername:         "foo_noah",
			authURL:              "/db/_oidc?provider=foo&offline=true",
			register:             true,
			includeAccessToken:   true,
			wantRefreshNoIDToken: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := auth.OIDCOptions{Providers: tc.providers, DefaultProvider: &tc.defaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &opts}}
			restTester := NewRestTester(t, &restTesterConfig)
			defer restTester.Close()
			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			if tc.wantNoCode {
				mockAuthServer.SetOption("wantNoCode", true)
				defer mockAuthServer.DeleteOption("wantNoCode")
			}
			if tc.wantCallbackError {
				mockAuthServer.SetOption("wantCallbackError", true)
				defer mockAuthServer.DeleteOption("wantCallbackError")
			}
			if tc.wantUntoldProvider {
				mockAuthServer.SetOption("wantUntoldProvider", true)
				defer mockAuthServer.DeleteOption("wantUntoldProvider")
			}
			if tc.wantDiscoveryError {
				mockAuthServer.SetOption("wantDiscoveryError", true)
				defer mockAuthServer.DeleteOption("wantDiscoveryError")
			}
			if tc.wantCallbackTokenExgError {
				mockAuthServer.SetOption("wantTokenExchangeError", true)
				defer mockAuthServer.DeleteOption("wantTokenExchangeError")
			}
			if tc.wantCallbackNoIDToken {
				mockAuthServer.SetOption("wantNoIDToken", true)
				defer mockAuthServer.DeleteOption("wantNoIDToken")
			}

			// Initiate OpenID Connect Authorization Code flow.
			requestURL := mockSyncGatewayURL + tc.authURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			jar, err := cookiejar.New(nil)
			require.NoError(t, err, "Error creating new cookie jar")
			client := &http.Client{Jar: jar}
			response, err := client.Do(request)
			require.NoError(t, err, "Error sending request")

			if !tc.register {
				assertHttpResponse(t, response, http.StatusUnauthorized, "Invalid login")
				return
			}
			if tc.wantNoCode {
				assertHttpResponse(t, response, http.StatusBadRequest, "Code must be present on oidc callback")
				return
			}
			if tc.wantCallbackError {
				assertHttpResponse(t, response, http.StatusUnauthorized, "oidc callback received an error")
				return
			}
			if tc.wantUntoldProvider {
				assertHttpResponse(t, response, http.StatusBadRequest, "Unable to identify provider for callback request")
				return
			}
			if tc.notConfiguredProvider {
				assertHttpResponse(t, response, http.StatusBadRequest, "OpenID Connect not configured for database db")
				return
			}
			if tc.wantDiscoveryError {
				assertHttpResponse(t, response, http.StatusInternalServerError, "Unable to obtain client for provider")
				return
			}
			if tc.wantCallbackTokenExgError {
				assertHttpResponse(t, response, http.StatusUnauthorized, "Failed to exchange token")
				return
			}
			if tc.wantCallbackNoIDToken {
				assertHttpResponse(t, response, http.StatusInternalServerError, "No id_token field in oauth2 token")
				return
			}

			// Validate received token response
			require.Equal(t, http.StatusOK, response.StatusCode)
			var receivedToken OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, tc.wantUsername, receivedToken.Username, "name mismatch")
			wantTokenResponse := mockAuthServer.Options()["wantTokenResponse"].(OIDCTokenResponse)
			assert.Equal(t, wantTokenResponse.IDToken, receivedToken.IDToken, "id_token mismatch")
			assert.Equal(t, wantTokenResponse.RefreshToken, receivedToken.RefreshToken, "refresh_token mismatch")
			if tc.includeAccessToken {
				assert.Equal(t, wantTokenResponse.AccessToken, receivedToken.AccessToken, "access_token mismatch")
			}
			assert.Equal(t, wantTokenResponse.TokenType, receivedToken.TokenType, "token_type mismatch")
			assert.True(t, wantTokenResponse.Expires >= receivedToken.Expires, "expires_in mismatch")

			// Query db endpoint with Bearer token
			var responseBody map[string]interface{}
			dbEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", receivedToken.IDToken)
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])

			// Token refresh workflow
			if tc.wantRefreshTokenExgError {
				mockAuthServer.SetOption("wantTokenExchangeError", true)
				defer mockAuthServer.DeleteOption("wantTokenExchangeError")
			}
			if tc.wantRefreshNoIDToken {
				mockAuthServer.SetOption("wantNoIDToken", true)
				defer mockAuthServer.DeleteOption("wantNoIDToken")
			}

			// Refresh auth token using refresh token received earlier from OP.
			requestURL = mockSyncGatewayURL + "/db/_oidc_refresh?refresh_token=" + receivedToken.RefreshToken
			request, err = http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request")

			if tc.wantRefreshTokenExgError {
				assertHttpResponse(t, response, http.StatusUnauthorized, "Unable to refresh token")
				return
			}
			if tc.wantRefreshNoIDToken {
				assertHttpResponse(t, response, http.StatusInternalServerError, "No id_token field in oauth2 token")
				return
			}
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Validate received token refresh response.
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			wantTokenResponse = mockAuthServer.Options()["wantTokenResponse"].(OIDCTokenResponse)
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, tc.wantUsername, receivedToken.Username, "name mismatch")
			assert.Equal(t, wantTokenResponse.IDToken, receivedToken.IDToken, "id_token mismatch")
			assert.Equal(t, wantTokenResponse.RefreshToken, receivedToken.RefreshToken, "refresh_token mismatch")
			if tc.includeAccessToken {
				assert.Equal(t, wantTokenResponse.AccessToken, receivedToken.AccessToken, "access_token mismatch")
			}
			assert.Equal(t, wantTokenResponse.TokenType, receivedToken.TokenType, "token_type mismatch")
			assert.True(t, wantTokenResponse.Expires >= receivedToken.Expires, "expires_in mismatch")

			// Query db endpoint with Bearer token
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", receivedToken.IDToken)
			response, err = client.Do(request)
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
	defer func() { require.NoError(t, response.Body.Close(), "error closing response body") }()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	require.NoError(t, err, "error reading response body")
	assert.Contains(t, string(bodyBytes), bodyText)
	assert.Equal(t, statusCode, response.StatusCode)
}

func TestCallbackState(t *testing.T) {
	mockAuthServer := newMockAuthServer()
	mockAuthServer.Start()
	defer mockAuthServer.Shutdown()

	issuerFoo := mockAuthServer.URL + "/foo"
	mockAuthServer.SetOption("issuer", issuerFoo)

	type test struct {
		name                   string
		inputProviders         auth.OIDCProviderMap
		inputDefaultProvider   string
		inputRequestURL        string
		inputChangeStateCookie bool
		wantUsername           string
		invalidateState        bool
	}
	tests := []test{
		{
			name: "callback state enabled by default",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
		},
		{
			name: "callback state enabled by default with HttpOnly cookie",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:                        "foo",
					Issuer:                      issuerFoo,
					ClientID:                    "baz",
					UserPrefix:                  "foo",
					ValidationKey:               base.StringPointer("qux"),
					Register:                    true,
					DiscoveryURI:                issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken:          true,
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
		}, {
			name: "callback state enabled explicitly",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:                 "foo",
					Issuer:               issuerFoo,
					ClientID:             "baz",
					UserPrefix:           "foo",
					ValidationKey:        base.StringPointer("qux"),
					Register:             true,
					DiscoveryURI:         issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken:   true,
					DisableCallbackState: false,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
		},
		{
			name: "callback state enabled explicitly with HttpOnly cookie",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:                        "foo",
					Issuer:                      issuerFoo,
					ClientID:                    "baz",
					UserPrefix:                  "foo",
					ValidationKey:               base.StringPointer("qux"),
					Register:                    true,
					DiscoveryURI:                issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken:          true,
					DisableCallbackState:        false,
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
		}, {
			name: "callback state disabled explicitly",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:                 "foo",
					Issuer:               issuerFoo,
					ClientID:             "baz",
					UserPrefix:           "foo",
					ValidationKey:        base.StringPointer("quz"),
					Register:             true,
					DiscoveryURI:         issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken:   true,
					DisableCallbackState: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
		}, {
			name: "callback state disabled explicitly with HttpOnly cookie",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:                        "foo",
					Issuer:                      issuerFoo,
					ClientID:                    "baz",
					UserPrefix:                  "foo",
					ValidationKey:               base.StringPointer("qux"),
					Register:                    true,
					DiscoveryURI:                issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken:          true,
					DisableCallbackState:        true,
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
		}, {
			name: "mitigation from csrf when callback state enabled",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("qux"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
			invalidateState:      true,
		}, {
			name: "vulnerable to csrf when callback state disabled",
			inputProviders: auth.OIDCProviderMap{
				"foo": &auth.OIDCProvider{
					Name:               "foo",
					Issuer:             issuerFoo,
					ClientID:           "baz",
					UserPrefix:         "foo",
					ValidationKey:      base.StringPointer("quz"),
					Register:           true,
					DiscoveryURI:       issuerFoo + auth.OIDCDiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "foo",
			wantUsername:         "foo_noah",
			inputRequestURL:      "/db/_oidc?provider=foo&offline=true",
			invalidateState:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := auth.OIDCOptions{Providers: tc.inputProviders, DefaultProvider: &tc.inputDefaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &opts}}
			restTester := NewRestTester(t, &restTesterConfig)
			defer restTester.Close()
			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			if tc.invalidateState {
				mockAuthServer.SetOption("invalidateState", true)
				defer mockAuthServer.DeleteOption("invalidateState")
			}

			// Initiate OpenID Connect Authorization Code flow.
			requestURL := mockSyncGatewayURL + tc.inputRequestURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			jar, err := cookiejar.New(nil)
			require.NoError(t, err, "Error creating new cookie jar")
			client := &http.Client{Jar: jar}
			response, err := client.Do(request)
			require.NoError(t, err, "Error sending request")
			defer func() { require.NoError(t, response.Body.Close(), "Error closing response body") }()

			if tc.invalidateState {
				bodyBytes, err := ioutil.ReadAll(response.Body)
				require.NoError(t, err, "error reading response body")
				assert.Contains(t, string(bodyBytes), "State mismatch")
				assert.Equal(t, http.StatusBadRequest, response.StatusCode)
				return
			}
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Validate received token response
			var receivedToken OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, tc.wantUsername, receivedToken.Username, "name mismatch")
			wantTokenResponse := mockAuthServer.Options()["wantTokenResponse"].(OIDCTokenResponse)
			assert.Equal(t, wantTokenResponse.IDToken, receivedToken.IDToken, "id_token mismatch")
			assert.Equal(t, wantTokenResponse.RefreshToken, receivedToken.RefreshToken, "refresh_token mismatch")
			assert.Equal(t, wantTokenResponse.AccessToken, receivedToken.AccessToken, "access_token mismatch")
			assert.Equal(t, wantTokenResponse.TokenType, receivedToken.TokenType, "token_type mismatch")
			assert.True(t, wantTokenResponse.Expires >= receivedToken.Expires, "expires_in mismatch")

			// Query db endpoint with Bearer token
			var responseBody map[string]interface{}
			dbEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", receivedToken.IDToken)
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])

			// Initiate OpenID Connect Authorization Code flow.
			requestURL = mockSyncGatewayURL + "/db/_oidc_refresh?refresh_token=" + receivedToken.RefreshToken
			request, err = http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request")
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Validate received refresh token response
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			wantTokenResponse = mockAuthServer.Options()["wantTokenResponse"].(OIDCTokenResponse)
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, tc.wantUsername, receivedToken.Username, "name mismatch")
			assert.Equal(t, wantTokenResponse.IDToken, receivedToken.IDToken, "id_token mismatch")
			assert.Equal(t, wantTokenResponse.RefreshToken, receivedToken.RefreshToken, "refresh_token mismatch")
			assert.Equal(t, wantTokenResponse.AccessToken, receivedToken.AccessToken, "access_token mismatch")
			assert.Equal(t, wantTokenResponse.TokenType, receivedToken.TokenType, "token_type mismatch")
			assert.True(t, wantTokenResponse.Expires >= receivedToken.Expires, "expires_in mismatch")

			// Query db endpoint with Bearer token
			request, err = http.NewRequest(http.MethodGet, dbEndpoint, nil)
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", receivedToken.IDToken)
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])
		})
	}
}
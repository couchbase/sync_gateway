package rest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2/jwt"
)

var (
	issuerGoogle      = ""
	issuerFacebook    = ""
	wantTokenResponse OIDCTokenResponse
	invalidateState   bool
)

// mockAuthServer mocks a fake OAuth server by registering mock handlers
func mockAuthServer() (*httptest.Server, error) {
	router := mux.NewRouter()
	router.HandleFunc("/{provider}"+auth.DiscoveryConfigPath, mockDiscoveryHandler).Methods(http.MethodGet)
	router.HandleFunc("/google/auth", mockAuthHandler).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc("/google/token", mockTokenHandler).Methods(http.MethodPost)
	return httptest.NewServer(router), nil
}

// mockDiscoveryHandler mocks the provider discovery endpoint with a mock response
func mockDiscoveryHandler(w http.ResponseWriter, r *http.Request) {
	var metadata auth.ProviderMetadata
	vars := mux.Vars(r)
	provider := vars["provider"]
	if provider == "google" {
		metadata = auth.ProviderMetadata{
			Issuer: issuerGoogle, AuthorizationEndpoint: issuerGoogle + "/auth",
			TokenEndpoint: issuerGoogle + "/token", JwksUri: issuerGoogle + "/oauth2/v3/certs",
			IdTokenSigningAlgValuesSupported: []string{"RS256"},
		}
	} else if provider == "facebook" {
		metadata = auth.ProviderMetadata{
			Issuer: issuerFacebook, AuthorizationEndpoint: issuerFacebook + "/auth",
			TokenEndpoint: issuerFacebook + "/token", JwksUri: issuerFacebook + "/oauth2/v3/certs",
			IdTokenSigningAlgValuesSupported: []string{"RS256"},
		}
	} else {
		renderJSON(w, r, http.StatusBadRequest, "Unknown provider")
	}
	renderJSON(w, r, http.StatusOK, metadata)
}

// renderJSON renders the response data as "application/json" with the status code.
func renderJSON(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		base.Errorf("Error rendering JSON response: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// mockAuthHandler mocks the authorization handler
func mockAuthHandler(w http.ResponseWriter, r *http.Request) {
	var redirectionURL string
	state := r.URL.Query().Get(requestParamState)
	if invalidateState {
		state = "aW52YWxpZCBzdGF0ZQo=" // Invalid state to simulate CSRF
	}
	redirect := r.URL.Query().Get(requestParamRedirectURI)
	if redirect == "" {
		base.Errorf("No redirect URL found in auth request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	redirectionURL = fmt.Sprintf("%s?code=%s", redirect, base.GenerateRandomSecret())
	if state != "" {
		redirectionURL = fmt.Sprintf("%s&state=%s", redirectionURL, state)
	}
	http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
}

// mockTokenHandler mocks the token handler for requesting and exchanging auth token.
func mockTokenHandler(w http.ResponseWriter, r *http.Request) {
	claims := jwt.Claims{ID: "id0123456789", Issuer: issuerGoogle,
		Audience: jwt.Audience{"aud1", "aud2", "aud3", "foo"},
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	response := OIDCTokenResponse{
		IDToken:      token,
		AccessToken:  token,
		RefreshToken: token,
		TokenType:    "Bearer",
		Expires:      time.Now().Add(5 * time.Minute).UTC().Second(),
	}
	wantTokenResponse = response
	renderJSON(w, r, http.StatusOK, response)
}

func TestGetOIDCCallbackURL(t *testing.T) {
	mockAuthServer, err := mockAuthServer()
	require.NoError(t, err, "Error mocking fake authorization server")
	defer mockAuthServer.Close()

	issuerGoogle = mockAuthServer.URL + "/google"
	issuerFacebook = mockAuthServer.URL + "/facebook"

	// Default OpenID Connect Provider
	providerGoogle := auth.OIDCProvider{
		Name:          "google",
		Issuer:        issuerGoogle,
		ClientID:      "foo",
		ValidationKey: base.StringPointer("bar"),
		DiscoveryURI:  issuerGoogle + auth.DiscoveryConfigPath,
	}

	// Non-default OpenID Connect Provider
	providerFacebook := auth.OIDCProvider{
		Name:          "facebook",
		Issuer:        issuerFacebook,
		ClientID:      "foo",
		ValidationKey: base.StringPointer("bar"),
		DiscoveryURI:  issuerFacebook + auth.DiscoveryConfigPath,
	}

	providers := auth.OIDCProviderMap{"google": &providerGoogle, "facebook": &providerFacebook}
	openIDConnectOptions := auth.OIDCOptions{Providers: providers, DefaultProvider: &providerGoogle.Name}
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &openIDConnectOptions}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	type test struct {
		name             string
		inputRequestURL  string
		wantProviderName string
	}
	tests := []test{
		{
			// When multiple providers are defined, default provider is specified and the current provider is
			// not default, then current provider should be added to the generated OpenID Connect callback URL.
			name:             "default_specified_but_current_is_not_default",
			inputRequestURL:  "/db/_oidc?provider=facebook&offline=true",
			wantProviderName: "facebook",
		}, {
			// When multiple providers are defined, default provider is specified and the current provider is
			// default, then current provider should NOT be added to the generated OpenID Connect callback URL.
			name:            "default_specified_and_current_is_default",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		}, {
			// When multiple providers are defined, default provider is specified and no current provider is
			// provided, then provider name should NOT be added to the generated OpenID Connect callback URL.
			name:            "default_specified_with_no_current",
			inputRequestURL: "/db/_oidc?offline=true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := rt.SendAdminRequest(http.MethodGet, tc.inputRequestURL, "")
			require.Equal(t, http.StatusFound, resp.Code)
			location := resp.Header().Get(headerLocation)
			require.NotEmpty(t, location, "Location should be available in response header")
			locationURL, err := url.Parse(location)
			require.NoError(t, err, "Location header should be a valid URL")
			redirectURI := locationURL.Query().Get(requestParamRedirectURI)
			require.NotEmpty(t, location, "redirect_uri should be available in auth URL")
			redirectURL, err := url.Parse(redirectURI)
			require.NoError(t, err, "redirect_uri should be a valid URL")
			assert.Equal(t, tc.wantProviderName, redirectURL.Query().Get(auth.OIDCAuthProvider))
		})
	}
}

func TestCallbackState(t *testing.T) {
	mockAuthServer, err := mockAuthServer()
	require.NoError(t, err, "Error mocking fake authorization server")
	defer mockAuthServer.Close()
	issuerGoogle = mockAuthServer.URL + "/google"

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
			name: "callback_state_enabled",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                 "google",
					Issuer:               issuerGoogle,
					ClientID:             "foo",
					UserPrefix:           "google",
					ValidationKey:        base.StringPointer("bar"),
					Register:             true,
					DiscoveryURI:         issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:   true,
					DisableCallbackState: base.BoolPtr(false),
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
		{
			name: "callback_state_enabled_with_HttpOnly",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                        "google",
					Issuer:                      issuerGoogle,
					ClientID:                    "foo",
					UserPrefix:                  "google",
					ValidationKey:               base.StringPointer("bar"),
					Register:                    true,
					DiscoveryURI:                issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:          true,
					DisableCallbackState:        base.BoolPtr(false),
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
		{
			name: "callback_state_enabled_with_invalidateState",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                 "google",
					Issuer:               issuerGoogle,
					ClientID:             "foo",
					UserPrefix:           "google",
					ValidationKey:        base.StringPointer("bar"),
					Register:             true,
					DiscoveryURI:         issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:   true,
					DisableCallbackState: base.BoolPtr(false),
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
			invalidateState: true,
		},
		{
			name: "callback_state_enabled_with_HttpOnly_invalidateState",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                        "google",
					Issuer:                      issuerGoogle,
					ClientID:                    "foo",
					UserPrefix:                  "google",
					ValidationKey:               base.StringPointer("bar"),
					Register:                    true,
					DiscoveryURI:                issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:          true,
					DisableCallbackState:        base.BoolPtr(false),
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
			invalidateState: true,
		},
		{
			name: "callback_state_disabled_implicitly",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:               "google",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "google",
					ValidationKey:      base.StringPointer("bar"),
					Register:           true,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
		{
			name: "callback_state_disabled_implicitly_with_HttpOnly_cookie",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                        "google",
					Issuer:                      issuerGoogle,
					ClientID:                    "foo",
					UserPrefix:                  "google",
					ValidationKey:               base.StringPointer("bar"),
					Register:                    true,
					DiscoveryURI:                issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:          true,
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
		{
			name: "callback_state_disabled_implicitly_with_HttpOnly_cookie_invalidateState",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                        "google",
					Issuer:                      issuerGoogle,
					ClientID:                    "foo",
					UserPrefix:                  "google",
					ValidationKey:               base.StringPointer("bar"),
					Register:                    true,
					DiscoveryURI:                issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:          true,
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
			invalidateState: false,
		},
		{
			name: "callback_state_disabled_explicitly",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                 "google",
					Issuer:               issuerGoogle,
					ClientID:             "foo",
					UserPrefix:           "google",
					ValidationKey:        base.StringPointer("bar"),
					Register:             true,
					DiscoveryURI:         issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:   true,
					DisableCallbackState: base.BoolPtr(true),
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
		{
			name: "callback_state_disabled_explicitly_HttpOnly_cookie",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                        "google",
					Issuer:                      issuerGoogle,
					ClientID:                    "foo",
					UserPrefix:                  "google",
					ValidationKey:               base.StringPointer("bar"),
					Register:                    true,
					DiscoveryURI:                issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:          true,
					DisableCallbackState:        base.BoolPtr(true),
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
		{
			name: "callback_state_disabled_explicitly_HttpOnly_cookie_invalidateState",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:                        "google",
					Issuer:                      issuerGoogle,
					ClientID:                    "foo",
					UserPrefix:                  "google",
					ValidationKey:               base.StringPointer("bar"),
					Register:                    true,
					DiscoveryURI:                issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken:          true,
					DisableCallbackState:        base.BoolPtr(true),
					CallbackStateCookieHTTPOnly: true,
				},
			},
			inputDefaultProvider: "google", wantUsername: "google_noah",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			options := auth.OIDCOptions{Providers: tc.inputProviders, DefaultProvider: &tc.inputDefaultProvider}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DbConfig{OIDCConfig: &options}}
			restTester := NewRestTester(t, &restTesterConfig)
			defer restTester.Close()
			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL
			invalidateState = tc.invalidateState
			defer func() { invalidateState = false }()

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
		})
	}
}

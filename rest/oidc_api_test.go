package rest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2/jwt"
)

// mockOptions keeps options required for mocking
// auth code flow responses.
type mockOptions map[string]interface{}

var (
	optionsOnce sync.Once
	options     mockOptions
)

func NewMockOptions() mockOptions {
	optionsOnce.Do(func() {
		options = make(mockOptions)
	})
	return options
}

func (options mockOptions) Clear() {
	for option := range options {
		delete(options, option)
	}
}

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
		issuerGoogle := options["issuerGoogle"].(string)
		metadata = auth.ProviderMetadata{
			Issuer: issuerGoogle, AuthorizationEndpoint: issuerGoogle + "/auth",
			TokenEndpoint: issuerGoogle + "/token", JwksUri: issuerGoogle + "/oauth2/v3/certs",
			IdTokenSigningAlgValuesSupported: []string{"RS256"},
		}
	} else if provider == "facebook" {
		issuerFacebook := options["issuerFacebook"].(string)
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
	redirect := r.URL.Query().Get(requestParamRedirectURI)
	if redirect == "" {
		base.Errorf("No redirect URL found in auth request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if val, ok := options["wantCallbackError"]; ok && val.(bool) {
		err := "?error=unsupported_response_type&error_description=response_type%20not%20supported"
		redirectionURL = fmt.Sprintf("%s?error=%s", redirect, err)
		http.Redirect(w, r, redirectionURL, http.StatusTemporaryRedirect)
	}
	code := base.GenerateRandomSecret()
	if val, ok := options["wantNoCode"]; ok && val.(bool) {
		code = ""
	}
	redirectionURL = fmt.Sprintf("%s?code=%s", redirect, code)
	if state != "" {
		redirectionURL = fmt.Sprintf("%s&state=%s", redirectionURL, state)
	}
	if val, ok := options["wantUntoldProvider"]; ok && val.(bool) {
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

// mockTokenHandler mocks the token handler for requesting and exchanging auth token.
func mockTokenHandler(w http.ResponseWriter, r *http.Request) {
	issuerGoogle := options["issuerGoogle"].(string)
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
		AccessToken:  "7d1d234f5fde713a94454f268833adcd39835fe8",
		RefreshToken: "e08c77351221346153d09ff64c123b24fc4c1905",
		TokenType:    "Bearer",
		Expires:      time.Now().Add(5 * time.Minute).UTC().Second(),
	}
	options["wantTokenResponse"] = response
	renderJSON(w, r, http.StatusOK, response)
}

func TestGetOIDCCallbackURL(t *testing.T) {
	mockAuthServer, err := mockAuthServer()
	require.NoError(t, err, "Error mocking fake authorization server")
	defer mockAuthServer.Close()

	options := NewMockOptions()
	defer options.Clear()
	options["issuerGoogle"] = mockAuthServer.URL + "/google"
	options["issuerFacebook"] = mockAuthServer.URL + "/facebook"
	issuerGoogle := options["issuerGoogle"].(string)
	issuerFacebook := options["issuerFacebook"].(string)

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
		oidcChallenge    bool
		expectedProvider string
	}
	tests := []test{
		{
			// When multiple providers are defined, default provider is specified and the current provider is
			// not default, then current provider should be added to the generated OpenID Connect callback URL.
			name:             "oidc default provider specified but current provider is not default",
			inputRequestURL:  "/db/_oidc?provider=facebook&offline=true",
			wantProviderName: "facebook",
		}, {
			name:             "oidc challenge default provider specified but current provider is not default",
			inputRequestURL:  "/db/_oidc_challenge?provider=facebook&offline=true",
			wantProviderName: "facebook",
			expectedProvider: "provider%3Dfacebook",
			oidcChallenge:    true,
		}, {
			// When multiple providers are defined, default provider is specified and the current provider is
			// default, then current provider should NOT be added to the generated OpenID Connect callback URL.
			name:            "oidc default provider specified and current provider is default",
			inputRequestURL: "/db/_oidc?provider=google&offline=true",
		}, {
			name:            "oidc challenge default provider specified and current provider is default",
			inputRequestURL: "/db/_oidc_challenge?provider=google&offline=true",
			oidcChallenge:   true,
		}, {
			// When multiple providers are defined, default provider is specified and no current provider is
			// provided, then provider name should NOT be added to the generated OpenID Connect callback URL.
			name:            "oidc default provider specified with no current provider",
			inputRequestURL: "/db/_oidc?offline=true",
		}, {
			name:            "oidc challenge default provider specified with no current provider",
			inputRequestURL: "/db/_oidc_challenge?offline=true",
			oidcChallenge:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.oidcChallenge {
				resp := rt.SendAdminRequest(http.MethodGet, tc.inputRequestURL, "")
				require.Equal(t, http.StatusUnauthorized, resp.Code)
				wwwAuthHeader := resp.Header().Get("Www-Authenticate")
				assert.Contains(t, wwwAuthHeader, tc.expectedProvider)
			} else {
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
			}
		})
	}
}

func TestOpenIDConnectAuth(t *testing.T) {
	mockAuthServer, err := mockAuthServer()
	require.NoError(t, err, "Error mocking fake authorization server")
	defer mockAuthServer.Close()

	options := NewMockOptions()
	defer options.Clear()
	options["issuerGoogle"] = mockAuthServer.URL + "/google"
	issuerGoogle := options["issuerGoogle"].(string)

	type test struct {
		name                 string
		inputProviders       auth.OIDCProviderMap
		inputDefaultProvider string
		inputRequestURL      string
		wantUsername         string
		enableAutoRegNewUser bool
		includeAccessToken   bool
		wantNoCode           bool
		wantCallbackError    bool
		wantUntoldProvider   bool
	}
	tests := []test{
		{
			name: "new user auto registration enabled single provider",
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
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: true,
			includeAccessToken:   true,
		}, {
			name: "new user auto registration disabled single provider",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:               "google",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "google",
					ValidationKey:      base.StringPointer("bar"),
					Register:           false,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: false,
			includeAccessToken:   true,
		}, {
			name: "new user auto registration enabled no access token single provider",
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
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: true,
			includeAccessToken:   false,
		}, {
			name: "new user auto registration enabled multiple providers",
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
				"facebook": &auth.OIDCProvider{
					Name:               "facebook",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "facebook",
					ValidationKey:      base.StringPointer("bar"),
					Register:           true,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: true,
			includeAccessToken:   true,
		}, {
			name: "new user auto registration disabled multiple providers",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:               "google",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "google",
					ValidationKey:      base.StringPointer("bar"),
					Register:           false,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
				"facebook": &auth.OIDCProvider{
					Name:               "facebook",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "facebook",
					ValidationKey:      base.StringPointer("bar"),
					Register:           false,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: false,
			includeAccessToken:   true,
		}, {
			name: "new user auto registration disabled no access token multiple providers",
			inputProviders: auth.OIDCProviderMap{
				"google": &auth.OIDCProvider{
					Name:               "google",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "google",
					ValidationKey:      base.StringPointer("bar"),
					Register:           false,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
				"facebook": &auth.OIDCProvider{
					Name:               "facebook",
					Issuer:             issuerGoogle,
					ClientID:           "foo",
					UserPrefix:         "facebook",
					ValidationKey:      base.StringPointer("bar"),
					Register:           false,
					DiscoveryURI:       issuerGoogle + auth.DiscoveryConfigPath,
					IncludeAccessToken: true,
				},
			},
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: false,
			includeAccessToken:   false,
		}, {
			name: "auth code missing in callback URL received from auth server",
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
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: true,
			includeAccessToken:   true,
			wantNoCode:           true,
		}, {
			name: "callback error received from auth server via callback URL",
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
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: true,
			includeAccessToken:   true,
			wantCallbackError:    true,
		}, {
			name: "untold provider received from auth server via callback URL",
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
			inputDefaultProvider: "google",
			wantUsername:         "google_noah",
			inputRequestURL:      "/db/_oidc?provider=google&offline=true",
			enableAutoRegNewUser: true,
			includeAccessToken:   true,
			wantUntoldProvider:   true,
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

			if tc.wantNoCode {
				options["wantNoCode"] = true
			}
			defer delete(options, "wantNoCode")

			if tc.wantCallbackError {
				options["wantCallbackError"] = true
			}
			defer delete(options, "wantCallbackError")

			if tc.wantUntoldProvider {
				options["wantUntoldProvider"] = true
			}
			defer delete(options, "wantUntoldProvider")

			// Initiate OpenID Connect Authorization Code flow.
			requestURL := mockSyncGatewayURL + tc.inputRequestURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request")
			defer func() { require.NoError(t, response.Body.Close(), "Error closing response body") }()

			if !tc.enableAutoRegNewUser {
				bodyBytes, err := ioutil.ReadAll(response.Body)
				require.NoError(t, err, "error reading response body")
				assert.Contains(t, string(bodyBytes), "auto registration")
				assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
				return
			}
			if tc.wantNoCode {
				bodyBytes, err := ioutil.ReadAll(response.Body)
				require.NoError(t, err, "error reading response body")
				assert.Contains(t, string(bodyBytes), "Code must be present on oidc callback")
				assert.Equal(t, http.StatusBadRequest, response.StatusCode)
				return
			}
			if tc.wantCallbackError {
				bodyBytes, err := ioutil.ReadAll(response.Body)
				require.NoError(t, err, "error reading response body")
				assert.Contains(t, string(bodyBytes), "oidc callback received an error")
				assert.Equal(t, http.StatusUnauthorized, response.StatusCode)
				return
			}
			if tc.wantUntoldProvider {
				bodyBytes, err := ioutil.ReadAll(response.Body)
				require.NoError(t, err, "error reading response body")
				assert.Contains(t, string(bodyBytes), "Unable to identify provider for callback request")
				assert.Equal(t, http.StatusBadRequest, response.StatusCode)
				return
			}
			// Validate received token response
			require.Equal(t, http.StatusOK, response.StatusCode)
			var receivedToken OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, receivedToken.SessionID, "session_id doesn't exist")
			assert.Equal(t, tc.wantUsername, receivedToken.Username, "name mismatch")
			wantTokenResponse := options["wantTokenResponse"].(OIDCTokenResponse)
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
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])

			// Initiate OpenID Connect Authorization Code flow.
			requestURL = mockSyncGatewayURL + "/db/_oidc_refresh?refresh_token=" + receivedToken.RefreshToken
			request, err = http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request")
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Validate received refresh token response
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&receivedToken))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			wantTokenResponse = options["wantTokenResponse"].(OIDCTokenResponse)
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
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&responseBody))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.Equal(t, restTester.DatabaseConfig.Name, responseBody["db_name"])
		})
	}
}

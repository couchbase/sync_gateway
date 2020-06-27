package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2/jwt"
)

type jwtHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Type      string `json:"typ"`
	Version   int    `json:"ver"`
}

func TestCreateJWTToken(t *testing.T) {
	tests := []struct {
		name           string
		idTokenFormat  identityTokenFormat
		headerExpected jwtHeader
	}{{
		name:          "create token in default format",
		idTokenFormat: defaultFormat,
		headerExpected: jwtHeader{
			Algorithm: oidc.RS256,
			Type:      "JWT",
		},
	}, {
		name:          "create token in IBM Cloud App ID format",
		idTokenFormat: ibmCloudAppIDFormat,
		headerExpected: jwtHeader{
			Algorithm: oidc.RS256,
			KeyID:     testProviderKeyIdentifier,
			Type:      "JWT",
			Version:   4,
		},
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject := "alice"
			issueURL := "http://localhost:4984/db/_oidc_testing"
			scopes := make(map[string]struct{})
			scopes["email"] = struct{}{}
			scopes["profile"] = struct{}{}
			authState := AuthState{
				CallbackURL:         "http://localhost:4984/db/_oidc_callback",
				TokenTTL:            5 * time.Minute,
				Scopes:              scopes,
				IdentityTokenFormat: test.idTokenFormat,
			}
			token, err := createJWT(subject, issueURL, authState)
			assert.NoError(t, err, "Couldn't to create JSON Web Token")
			assert.NotEmpty(t, token, "Empty token received")
			tok, err := jwt.ParseSigned(token)
			require.NoError(t, err, "Error parsing signed token")
			claims := &jwt.Claims{}
			customClaims := &CustomClaims{}
			err = tok.UnsafeClaimsWithoutVerification(claims, customClaims)
			require.NoError(t, err, "Error parsing signed token")
			assert.Equal(t, subject, claims.Subject, "Subject mismatch")
			assert.Equal(t, issueURL, claims.Issuer, "Issuer mismatch")
			assert.Equal(t, "slim jim", customClaims.Nickname, "Nickname mismatch")
			assert.Equal(t, subject+"@syncgatewayoidctesting.com", customClaims.Email, "Email mismatch")
			headerBytes, err := base64.StdEncoding.DecodeString(strings.Split(token, ".")[0])
			require.NoError(t, err, "Error extracting header from token")
			var jwtHeaderActual jwtHeader
			require.NoError(t, json.Unmarshal(headerBytes, &jwtHeaderActual), "Header unmarshal error")
			require.True(t, reflect.DeepEqual(test.headerExpected, jwtHeaderActual), "Token header mismatch")
		})
	}
}

func TestExtractSubjectFromRefreshToken(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	// Extract subject from invalid refresh token
	sub, err := extractSubjectFromRefreshToken("invalid_refresh_token")
	require.Error(t, err, "invalid refresh token error")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
	assert.Empty(t, sub, "couldn't extract subject from refresh token")

	// Extract subject from a valid refresh token
	subject := "subject"
	accessToken := base64.StdEncoding.EncodeToString([]byte(subject))
	refreshToken := base64.StdEncoding.EncodeToString([]byte(subject + ":::" + accessToken))
	sub, err = extractSubjectFromRefreshToken(refreshToken)
	require.NoError(t, err, "invalid refresh token error")
	assert.Equal(t, subject, sub)
}

// restTesterConfigWithTestProviderEnabled returns RestTesterConfig with test provider
// enabled single provider configuration.
func restTesterConfigWithTestProviderEnabled() RestTesterConfig {
	providers := auth.OIDCProviderMap{
		"test": &auth.OIDCProvider{
			Register:      true,
			Issuer:        "${baseURL}/db/_oidc_testing",
			Name:          "test",
			ClientID:      "sync_gateway",
			ValidationKey: base.StringPtr("qux"),
			CallbackURL:   base.StringPtr("${baseURL}/db/_oidc_callback"),
		},
	}
	defaultProvider := "test"
	opts := auth.OIDCOptions{
		Providers:       providers,
		DefaultProvider: &defaultProvider,
	}
	restTesterConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			OIDCConfig: &opts,
			Unsupported: db.UnsupportedOptions{
				OidcTestProvider: db.OidcTestProviderOptions{
					Enabled: true,
				},
			},
		},
	}
	return restTesterConfig
}

func TestProviderOIDCAuthWithTlsSkipVerifyEnabled(t *testing.T) {
	restTesterConfig := restTesterConfigWithTestProviderEnabled()
	restTesterConfig.DatabaseConfig.Unsupported.OidcTlsSkipVerify = true
	restTester := NewRestTester(t, &restTesterConfig)
	restTester.SetAdminParty(false)
	defer restTester.Close()
	mockSyncGateway := httptest.NewTLSServer(restTester.TestPublicHandler())
	defer mockSyncGateway.Close()
	mockSyncGatewayURL := mockSyncGateway.URL
	provider := restTesterConfig.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
	provider.Issuer = mockSyncGateway.URL + "/db/_oidc_testing"
	provider.CallbackURL = base.StringPtr(mockSyncGateway.URL + "/db/_oidc_callback")

	// Send OpenID Connect request
	authURL := "/db/_oidc?provider=test&offline=true"
	requestURL := mockSyncGatewayURL + authURL
	request, err := http.NewRequest(http.MethodGet, requestURL, nil)
	require.NoError(t, err, "Error creating new request")
	jar, err := cookiejar.New(nil)
	require.NoError(t, err, "Error creating new cookie jar")

	// Set insecureSkipVerify on the test's HTTP client since both the _oidc and _oidc_testing
	// endpoints are being run with the same TLSServer.
	client := auth.GetHttpClient(true)
	client.Jar = jar
	response, err := client.Do(request)
	require.NoError(t, err, "Error sending request")
	require.Equal(t, http.StatusOK, response.StatusCode)
	bodyBytes, err := ioutil.ReadAll(response.Body)
	require.NoError(t, err, "Error reading response")
	bodyString := string(bodyBytes)
	require.NoError(t, response.Body.Close(), "Error closing response body")

	// Send authentication request
	requestURL = mockSyncGateway.URL + "/db/_oidc_testing/" + parseAuthURL(bodyString)
	form := url.Values{}
	form.Add("username", "alice")
	form.Add("authenticated", "Return a valid authorization code for this user")
	request, err = http.NewRequest(http.MethodPost, requestURL, bytes.NewBufferString(form.Encode()))
	require.NoError(t, err, "Error creating new request")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err = client.Do(request)
	require.NoError(t, err, "Error sending request")
	require.Equal(t, http.StatusOK, response.StatusCode)

	var authResponseActual OIDCTokenResponse
	require.NoError(t, err, json.NewDecoder(response.Body).Decode(&authResponseActual))
	require.NoError(t, response.Body.Close(), "Error closing response body")
	assert.NotEmpty(t, authResponseActual.SessionID, "session_id doesn't exist")
	assert.NotEmpty(t, authResponseActual.Username, "session_id doesn't exist")
	assert.NotEmpty(t, authResponseActual.IDToken, "id_token mismatch")
	assert.NotEmpty(t, authResponseActual.RefreshToken, "refresh_token mismatch")
}

func TestProviderOIDCAuthWithTlsSkipVerifyDisabled(t *testing.T) {
	restTesterConfig := restTesterConfigWithTestProviderEnabled()
	restTesterConfig.DatabaseConfig.Unsupported.OidcTlsSkipVerify = false
	restTester := NewRestTester(t, &restTesterConfig)
	restTester.SetAdminParty(false)
	defer restTester.Close()
	mockSyncGateway := httptest.NewTLSServer(restTester.TestPublicHandler())
	defer mockSyncGateway.Close()
	mockSyncGatewayURL := mockSyncGateway.URL
	provider := restTesterConfig.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
	provider.Issuer = mockSyncGateway.URL + "/db/_oidc_testing"
	provider.CallbackURL = base.StringPtr(mockSyncGateway.URL + "/db/_oidc_callback")

	// Send OpenID Connect request
	authURL := "/db/_oidc?provider=test&offline=true"
	requestURL := mockSyncGatewayURL + authURL
	request, err := http.NewRequest(http.MethodGet, requestURL, nil)
	require.NoError(t, err, "Error creating new request")
	jar, err := cookiejar.New(nil)
	require.NoError(t, err, "Error creating new cookie jar")

	// Set insecureSkipVerify on the test's HTTP client since both the _oidc and _oidc_testing
	// endpoints are being run with the same TLSServer.
	client := auth.GetHttpClient(true)
	client.Jar = jar
	response, err := client.Do(request)
	require.NoError(t, err, "Error sending request")
	require.Equal(t, http.StatusInternalServerError, response.StatusCode)
	bodyBytes, err := ioutil.ReadAll(response.Body)
	require.NoError(t, err, "Error reading response")
	bodyString := string(bodyBytes)
	assert.Contains(t, bodyString, "Unable to obtain client for provider")
	require.NoError(t, response.Body.Close(), "Error closing response body")
}

func TestOpenIDConnectTestProviderWithRealWorldToken(t *testing.T) {
	tests := []struct {
		name           string
		idTokenFormat  identityTokenFormat
		headerExpected jwtHeader
	}{{
		name:          "obtain session with bearer token in default format",
		idTokenFormat: defaultFormat,
		headerExpected: jwtHeader{
			Algorithm: oidc.RS256,
			Type:      "JWT",
		},
	}, {
		name:          "obtain session with bearer token in IBM Cloud App ID format",
		idTokenFormat: ibmCloudAppIDFormat,
		headerExpected: jwtHeader{
			Algorithm: oidc.RS256,
			KeyID:     testProviderKeyIdentifier,
			Type:      "JWT",
			Version:   4,
		},
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			providers := auth.OIDCProviderMap{
				"test": &auth.OIDCProvider{
					Register:      true,
					Issuer:        "${baseURL}/db/_oidc_testing",
					Name:          "test",
					ClientID:      "sync_gateway",
					ValidationKey: base.StringPtr("qux"),
					CallbackURL:   base.StringPtr("${baseURL}/db/_oidc_callback"),
					UserPrefix:    "foo",
				},
			}
			defaultProvider := "test"
			opts := auth.OIDCOptions{Providers: providers, DefaultProvider: &defaultProvider}
			restTesterConfig := RestTesterConfig{
				DatabaseConfig: &DbConfig{
					OIDCConfig: &opts,
					Unsupported: db.UnsupportedOptions{
						OidcTestProvider: db.OidcTestProviderOptions{
							Enabled: true,
						},
					},
				}}
			restTester := NewRestTester(t, &restTesterConfig)
			restTester.SetAdminParty(false)
			defer restTester.Close()

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL
			provider := restTesterConfig.DatabaseConfig.OIDCConfig.Providers.GetDefaultProvider()
			provider.Issuer = mockSyncGateway.URL + "/db/_oidc_testing"
			provider.CallbackURL = base.StringPtr(mockSyncGateway.URL + "/db/_oidc_callback")
			createUser(t, restTester, "foo_noah")

			// Send OpenID Connect request
			authURL := "/db/_oidc?provider=test&offline=true"
			requestURL := mockSyncGatewayURL + authURL
			request, err := http.NewRequest(http.MethodGet, requestURL, nil)
			require.NoError(t, err, "Error creating new request")
			jar, err := cookiejar.New(nil)
			require.NoError(t, err, "Error creating new cookie jar")
			client := http.DefaultClient
			client.Jar = jar
			response, err := client.Do(request)
			require.NoError(t, err, "Error sending request")
			require.Equal(t, http.StatusOK, response.StatusCode)
			bodyBytes, err := ioutil.ReadAll(response.Body)
			require.NoError(t, err, "Error reading response")
			bodyString := string(bodyBytes)
			require.NoError(t, response.Body.Close(), "Error closing response body")

			// Send authentication request
			requestURL = mockSyncGateway.URL + "/db/_oidc_testing/" + parseAuthURL(bodyString)
			form := url.Values{}
			form.Add(formKeyUsername, "noah")
			form.Add(formKeyAuthenticated, "Return a valid authorization code for this user")
			form.Add(formKeyIdTokenFormat, string(test.idTokenFormat))
			request, err = http.NewRequest(http.MethodPost, requestURL, bytes.NewBufferString(form.Encode()))
			require.NoError(t, err, "Error creating new request")
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response, err = client.Do(request)
			require.NoError(t, err, "Error sending request")
			require.Equal(t, http.StatusOK, response.StatusCode)

			var authResponseActual OIDCTokenResponse
			require.NoError(t, err, json.NewDecoder(response.Body).Decode(&authResponseActual))
			require.NoError(t, response.Body.Close(), "Error closing response body")
			assert.NotEmpty(t, authResponseActual.SessionID, "session_id doesn't exist")
			assert.NotEmpty(t, authResponseActual.Username, "session_id doesn't exist")
			assert.NotEmpty(t, authResponseActual.IDToken, "id_token mismatch")
			assert.NotEmpty(t, authResponseActual.RefreshToken, "refresh_token mismatch")

			// Check token header
			headerBytes, err := base64.StdEncoding.DecodeString(strings.Split(authResponseActual.IDToken, ".")[0])
			require.NoError(t, err, "Error extracting header from token")
			var jwtHeaderActual jwtHeader
			require.NoError(t, json.Unmarshal(headerBytes, &jwtHeaderActual), "Header unmarshal error")
			require.True(t, reflect.DeepEqual(test.headerExpected, jwtHeaderActual), "Token header mismatch")

			// Obtain session with Bearer token
			sessionEndpoint := mockSyncGatewayURL + "/" + restTester.DatabaseConfig.Name + "/_session"
			request, err = http.NewRequest(http.MethodPost, sessionEndpoint, strings.NewReader(`{}`))
			require.NoError(t, err, "Error creating new request")
			request.Header.Add("Authorization", BearerToken+" "+authResponseActual.IDToken)
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err, "Error sending request with bearer token")
			checkGoodAuthResponse(t, response)
		})
	}
}

// parseAuthURL returns the authentication URL extracted from user consent form.
func parseAuthURL(html string) string {
	re := regexp.MustCompile(`<form action="(.+)" method`)
	if submatch := re.FindStringSubmatch(html); submatch != nil {
		return submatch[1]
	}
	return ""
}

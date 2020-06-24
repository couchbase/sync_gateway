package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateJWTToken(t *testing.T) {
	subject := "alice"
	issueURL := "http://localhost:4984/default/_oidc_testing"
	ttl := 5 * time.Minute
	scopes := make(map[string]struct{})
	token, err := createJWT(subject, issueURL, ttl, scopes)
	assert.NoError(t, err, "Couldn't to create JSON Web Token for OpenID Connect")
	log.Printf("Token: %s", token)
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

func TestInsecureOpenIDConnectAuth(t *testing.T) {
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
	opts := auth.OIDCOptions{Providers: providers, DefaultProvider: &defaultProvider}
	restTesterConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			OIDCConfig: &opts,
			Unsupported: db.UnsupportedOptions{
				OidcTestProvider: db.OidcTestProviderOptions{
					Enabled: true,
				},
				OidcTlsSkipVerify: true,
			},
		}}
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

// parseAuthURL returns the authentication URL extracted from user consent form.
func parseAuthURL(html string) string {
	re := regexp.MustCompile(`<form action="(.+)" method`)
	if submatch := re.FindStringSubmatch(html); submatch != nil {
		return submatch[1]
	}
	return ""
}

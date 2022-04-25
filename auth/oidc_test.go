//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-oidc"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

func TestOIDCProviderMap_GetDefaultProvider(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAuth)

	cbProvider := OIDCProvider{
		Name: "Couchbase",
	}
	cbProviderDefault := OIDCProvider{
		Name:      "Couchbase",
		IsDefault: true,
	}

	glProvider := OIDCProvider{
		Name: "Gügul",
	}

	fbProvider := OIDCProvider{
		Name: "Fæsbuk",
	}

	tests := []struct {
		Name             string
		ProviderMap      OIDCProviderMap
		ExpectedProvider *OIDCProvider
	}{
		{
			Name:             "Empty OIDCProviderMap",
			ProviderMap:      nil,
			ExpectedProvider: nil,
		},
		{
			Name: "One provider, no default",
			ProviderMap: OIDCProviderMap{
				"cb": &cbProvider,
			},
			ExpectedProvider: nil,
		},
		{
			Name: "One provider, with default",
			ProviderMap: OIDCProviderMap{
				"cb": &cbProviderDefault,
			},
			ExpectedProvider: &cbProviderDefault,
		},
		{
			Name: "Multiple provider, one default",
			ProviderMap: OIDCProviderMap{
				"gl": &glProvider,
				"cb": &cbProviderDefault,
				"fb": &fbProvider,
			},
			ExpectedProvider: &cbProviderDefault,
		},
		// FIXME: Implementation is non-deterministic, because of ranging over the map
		// {
		// 	Name: "Multiple provider, multiple defaults",
		// 	ProviderMap: OIDCProviderMap{
		// 		"cb": &cbProviderDefault,
		// 		"gl": &glProviderDefault,
		// 		"fb": &fbProviderDefault,
		// 	},
		// 	ExpectedProvider: &glProviderDefault,
		// },
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			provider := test.ProviderMap.GetDefaultProvider()
			assert.Equal(tt, test.ExpectedProvider, provider)
		})
	}
}

func TestOIDCProviderMap_GetProviderForIssuer(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAuth)

	clientID := "SGW-TEST"
	cbProvider := OIDCProvider{
		Name:     "Couchbase",
		Issuer:   "http://127.0.0.1:1234",
		ClientID: clientID,
	}
	glProvider := OIDCProvider{
		Name:     "Gügul",
		Issuer:   "http://127.0.0.1:1235",
		ClientID: clientID,
	}
	fbProvider := OIDCProvider{
		Name:     "Fæsbuk",
		Issuer:   "http://127.0.0.1:1236",
		ClientID: clientID,
	}
	providerMap := OIDCProviderMap{
		"gl": &glProvider,
		"cb": &cbProvider,
		"fb": &fbProvider,
	}

	tests := []struct {
		Name             string
		Issuer           string
		Audiences        []string
		ExpectedProvider *OIDCProvider
	}{
		{
			Name:             "No issuer or audiences",
			Issuer:           "",
			Audiences:        []string{},
			ExpectedProvider: nil,
		},
		{
			Name:             "Matched issuer, no audience",
			Issuer:           "http://127.0.0.1:1234",
			Audiences:        []string{},
			ExpectedProvider: nil,
		},
		{
			Name:             "Matched issuer, unmatched audience",
			Issuer:           "http://127.0.0.1:1234",
			Audiences:        []string{"SGW-PROD"},
			ExpectedProvider: nil,
		},
		{
			Name:             "Matched issuer, matched audience",
			Issuer:           "http://127.0.0.1:1234",
			Audiences:        []string{"SGW-TEST"},
			ExpectedProvider: &cbProvider,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			provider := providerMap.GetProviderForIssuer(base.TestCtx(t), test.Issuer, test.Audiences)
			assert.Equal(tt, test.ExpectedProvider, provider)
		})
	}
}

func TestOIDCUsername(t *testing.T) {
	provider := OIDCProvider{
		Name:   "Some_Provider",
		Issuer: "http://www.someprovider.com",
	}

	ctx := base.TestCtx(t)

	err := provider.InitUserPrefix(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "www.someprovider.com", provider.UserPrefix)

	// test username suffix
	identity := Identity{Subject: "bernard"}
	oidcUsername, err := getOIDCUsername(&provider, &identity)
	assert.NoError(t, err, "Error retrieving OpenID Connect username")
	assert.Equal(t, "www.someprovider.com_bernard", oidcUsername)
	assert.Equal(t, true, IsValidPrincipalName(oidcUsername))

	// test char escaping
	identity.Subject = "{bernard}"
	oidcUsername, err = getOIDCUsername(&provider, &identity)
	assert.NoError(t, err, "Error retrieving OpenID Connect username")
	assert.Equal(t, "www.someprovider.com_%7Bbernard%7D", oidcUsername)
	assert.Equal(t, true, IsValidPrincipalName(oidcUsername))

	// test URL with paths
	provider.UserPrefix = ""
	provider.Issuer = "http://www.someprovider.com/extra"
	err = provider.InitUserPrefix(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "www.someprovider.com%2Fextra", provider.UserPrefix)

	// test invalid URL
	provider.UserPrefix = ""
	provider.Issuer = "http//www.someprovider.com"
	err = provider.InitUserPrefix(ctx)
	assert.NoError(t, err)
	// falls back to provider name:
	assert.Equal(t, "Some_Provider", provider.UserPrefix)
}

func TestInitOIDCClient(t *testing.T) {
	defaultWait := OIDCDiscoveryRetryWait
	OIDCDiscoveryRetryWait = 10 * time.Millisecond
	defer func() { OIDCDiscoveryRetryWait = defaultWait }()

	ctx := base.TestCtx(t)

	t.Run("initialize openid connect client with nil provider", func(t *testing.T) {
		provider := &OIDCProvider{}
		err := provider.initOIDCClient(ctx)
		require.Error(t, err, "initialized openid connect client with nil provider")
		assert.Contains(t, err.Error(), "Issuer not defined")
	})

	t.Run("initialize openid connect client with unavailable issuer", func(t *testing.T) {
		provider := &OIDCProvider{
			Issuer:      "http://127.0.0.1:12345/auth",
			CallbackURL: base.StringPtr("http://127.0.0.1:12345/callback"),
		}
		err := provider.initOIDCClient(ctx)
		require.Error(t, err, "openid connect client with unavailable issuer")
		assert.Contains(t, err.Error(), ErrMsgUnableToDiscoverConfig)
	})

	t.Run("initialize openid connect client with valid provider config", func(t *testing.T) {
		provider := &OIDCProvider{
			ClientID:    "foo",
			Issuer:      "https://accounts.google.com",
			CallbackURL: base.StringPtr("http://sgw-test:4984/_callback"),
		}
		err := provider.initOIDCClient(ctx)
		require.NoError(t, err, "openid connect client with unavailable issuer")
		provider.stopDiscoverySync()
	})
}

func TestConcurrentSetConfig(t *testing.T) {
	providerLock := sync.Mutex{}
	provider := &OIDCProvider{
		ClientID:    "foo",
		Issuer:      "https://accounts.google.com",
		CallbackURL: base.StringPtr("http://sgw-test:4984/_callback"),
	}

	ctx := base.TestCtx(t)

	err := provider.initOIDCClient(ctx)
	require.NoError(t, err, "openid connect client initialization failure")
	metadata, verifier, err := provider.DiscoverConfig(ctx)
	require.NoError(t, err, "error discovering provider metadata")

	expectedAuthURL := []string{
		"https://accounts.foo.com/o/oauth2/v2/auth",
		"https://accounts.bar.com/o/oauth2/v2/auth",
	}
	expectedTokenURL := []string{
		"https://oauth2.foo.com/token",
		"https://oauth2.bar.com/token",
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		fooMetadata := metadata
		fooMetadata.AuthorizationEndpoint = expectedAuthURL[0]
		fooMetadata.TokenEndpoint = expectedTokenURL[0]
		providerLock.Lock()
		verifier = provider.generateVerifier(&fooMetadata, base.TestCtx(t))
		require.NotNil(t, verifier, "error generating id token verifier")
		provider.client.SetConfig(verifier, fooMetadata.endpoint())
		providerLock.Unlock()
		wg.Done()
	}()
	go func() {
		barMetadata := metadata
		barMetadata.AuthorizationEndpoint = expectedAuthURL[1]
		barMetadata.TokenEndpoint = expectedTokenURL[1]
		providerLock.Lock()
		verifier = provider.generateVerifier(&barMetadata, base.TestCtx(t))
		require.NotNil(t, verifier, "error generating id token verifier")
		provider.client.SetConfig(verifier, barMetadata.endpoint())
		providerLock.Unlock()
		wg.Done()
	}()
	wg.Wait()
	provider.client.mutex.RLock()
	assert.NotNil(t, provider.client.verifier, "error setting verifier")
	provider.client.mutex.RUnlock()
	assert.Contains(t, expectedAuthURL, provider.client.Config().Endpoint.AuthURL)
	assert.Contains(t, expectedTokenURL, provider.client.Config().Endpoint.TokenURL)
	provider.stopDiscoverySync()
}

func TestFetchCustomProviderConfig(t *testing.T) {
	tests := []struct {
		name            string
		data            string
		trailingSlash   bool
		wantAuthURL     string
		wantTokenURL    string
		wantUserInfoURL string
		wantAlgorithms  []string
		wantErr         bool
	}{{
		name: "basic_case",
		data: `{
				"issuer": "${issuer}",
				"authorization_endpoint": "https://example.com/auth",
				"token_endpoint": "https://example.com/token",
				"jwks_uri": "https://example.com/keys",
				"id_token_signing_alg_values_supported": ["RS256"]
			}`,
		wantAuthURL:    "https://example.com/auth",
		wantTokenURL:   "https://example.com/token",
		wantAlgorithms: []string{"RS256"},
	}, {
		name: "additional_algorithms",
		data: `{
				"issuer": "${issuer}",
				"authorization_endpoint": "https://example.com/auth",
				"token_endpoint": "https://example.com/token",
				"jwks_uri": "https://example.com/keys",
				"id_token_signing_alg_values_supported": ["RS256", "RS384", "ES256"]
			}`,
		wantAuthURL:    "https://example.com/auth",
		wantTokenURL:   "https://example.com/token",
		wantAlgorithms: []string{"RS256", "RS384", "ES256"},
	}, {
		name: "mismatched_issuer",
		data: `{
				"issuer": "https://example.com",
				"authorization_endpoint": "https://example.com/auth",
				"token_endpoint": "https://example.com/token",
				"jwks_uri": "https://example.com/keys",
				"id_token_signing_alg_values_supported": ["RS256"]
			}`,
		wantErr: true,
	}, {
		name: "issuer_with_trailing_slash",
		data: `{
				"issuer": "${issuer}",
				"authorization_endpoint": "https://example.com/auth",
				"token_endpoint": "https://example.com/token",
				"jwks_uri": "https://example.com/keys",
				"id_token_signing_alg_values_supported": ["RS256"]
			}`,
		trailingSlash:  true,
		wantAuthURL:    "https://example.com/auth",
		wantTokenURL:   "https://example.com/token",
		wantAlgorithms: []string{"RS256"},
	}, {
		// Test case taken directly from:
		// https://accounts.google.com/.well-known/openid-configuration
		name:            "google",
		wantAuthURL:     "https://accounts.google.com/o/oauth2/v2/auth",
		wantTokenURL:    "https://oauth2.googleapis.com/token",
		wantUserInfoURL: "https://openidconnect.googleapis.com/v1/userinfo",
		wantAlgorithms:  []string{"RS256"},
		data: `{
				"issuer": "${issuer}",
				"authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
				"device_authorization_endpoint": "https://oauth2.googleapis.com/device/code",
				"token_endpoint": "https://oauth2.googleapis.com/token",
				"userinfo_endpoint": "https://openidconnect.googleapis.com/v1/userinfo",
				"revocation_endpoint": "https://oauth2.googleapis.com/revoke",
				"jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
				"response_types_supported": ["code","token","id_token","code token","code id_token","token id_token","code token id_token","none"],
 				"subject_types_supported": ["public"],
 				"id_token_signing_alg_values_supported": ["RS256"],
 				"scopes_supported": ["openid","email","profile"],
 				"token_endpoint_auth_methods_supported": ["client_secret_post","client_secret_basic"],
                "claims_supported": ["aud","email","email_verified","exp","family_name","given_name","iat","iss","locale","name","picture","sub"],
                 "code_challenge_methods_supported": ["plain","S256"],
                 "grant_types_supported": ["authorization_code","refresh_token","urn:ietf:params:oauth:grant-type:device_code","urn:ietf:params:oauth:grant-type:jwt-bearer"]
            }`,
	},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			var issuer string
			hf := func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != OIDCDiscoveryConfigPath {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, err := io.WriteString(w, strings.ReplaceAll(test.data, "${issuer}", issuer))
				require.NoError(t, err)
			}
			s := httptest.NewServer(http.HandlerFunc(hf))
			defer s.Close()

			issuer = s.URL
			if test.trailingSlash {
				issuer += "/"
			}
			discoveryURL := GetStandardDiscoveryEndpoint(issuer)
			op := &OIDCProvider{Issuer: issuer}
			metadata, _, _, err := op.fetchCustomProviderConfig(base.TestCtx(t), discoveryURL)
			if err != nil {
				assert.True(t, test.wantErr, "Unexpected Error!")
				return
			}
			assert.False(t, test.wantErr, "Expected Error Not Found!")
			assert.Equal(t, test.wantAuthURL, metadata.AuthorizationEndpoint)
			assert.Equal(t, test.wantTokenURL, metadata.TokenEndpoint)
			assert.Equal(t, test.wantUserInfoURL, metadata.UserInfoEndpoint)
			log.Printf("wantAlgorithms: %v, IdTokenSigningAlgValuesSupported: %v", test.wantAlgorithms, metadata.IdTokenSigningAlgValuesSupported)
			assert.True(t, reflect.DeepEqual(test.wantAlgorithms, metadata.IdTokenSigningAlgValuesSupported))
		})
	}
}

func TestGetJWTIssuer(t *testing.T) {
	wantIssuer := "https://accounts.google.com"
	wantAudience := jwt.Audience{"aud1", "aud2"}
	signer, err := getRSASigner()
	require.NoError(t, err, "Failed to create RSA signer")

	claims := jwt.Claims{Issuer: wantIssuer, Audience: wantAudience}
	builder := jwt.Signed(signer).Claims(claims)
	token, err := builder.CompactSerialize()
	require.NoError(t, err, "Failed to serialize JSON Web Token")

	jwt, err := jwt.ParseSigned(token)
	require.NoError(t, err, "Failed to decode raw JSON Web Token")
	issuer, audiences, err := getIssuerWithAudience(jwt)
	assert.Equal(t, wantIssuer, issuer)
	assert.Equal(t, []string(wantAudience), audiences)
}

// getRSASigner creates a signer of type JWT using RS256
func getRSASigner() (signer jose.Signer, err error) {
	rsaPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return signer, err
	}
	signingKey := jose.SigningKey{Algorithm: jose.RS256, Key: rsaPrivateKey}
	var signerOptions = jose.SignerOptions{}
	signerOptions.WithType("JWT")
	signer, err = jose.NewSigner(signingKey, &signerOptions)
	if err != nil {
		return signer, err
	}
	return signer, nil
}

func TestGetSigningAlgorithms(t *testing.T) {
	tests := []struct {
		name                      string
		inputAlgorithms           []string
		wantSupportedAlgorithms   []string
		wantUnsupportedAlgorithms []string
	}{{
		name:                      "Identity Provider supports single algorithm supported by OpenID Connect Library",
		inputAlgorithms:           []string{oidc.RS256},
		wantSupportedAlgorithms:   []string{oidc.RS256},
		wantUnsupportedAlgorithms: nil,
	}, {
		name:                      "OpenID Connect Library supports all signing algorithms supported by Identity Provider",
		inputAlgorithms:           []string{oidc.RS256, oidc.RS384, oidc.RS512, oidc.ES256, oidc.ES384, oidc.ES512, oidc.PS256, oidc.PS384, oidc.PS512},
		wantSupportedAlgorithms:   []string{oidc.RS256, oidc.RS384, oidc.RS512, oidc.ES256, oidc.ES384, oidc.ES512, oidc.PS256, oidc.PS384, oidc.PS512},
		wantUnsupportedAlgorithms: nil,
	}, {
		name:                      "None of the signing algorithms supported by Identity Provider are supported by OpenID Connect Library",
		inputAlgorithms:           []string{"HS256", "HS512", "SHA256", "SHA512"},
		wantSupportedAlgorithms:   nil,
		wantUnsupportedAlgorithms: []string{"HS256", "HS512", "SHA256", "SHA512"},
	}, {
		name:                      "Few of the signing algorithms supported by Identity Provider are supported by OpenID Connect Library",
		inputAlgorithms:           []string{oidc.RS256, oidc.RS384, oidc.RS512, "HS256", "HS512", "SHA256", "SHA512"},
		wantSupportedAlgorithms:   []string{oidc.RS256, oidc.RS384, oidc.RS512},
		wantUnsupportedAlgorithms: []string{"HS256", "HS512", "SHA256", "SHA512"},
	}}
	for _, test := range tests {
		provider := OIDCProvider{}
		t.Run(test.name, func(t *testing.T) {
			metadata := &ProviderMetadata{IdTokenSigningAlgValuesSupported: test.inputAlgorithms}
			signingAlgorithms := provider.getSigningAlgorithms(metadata)
			assert.Equal(t, test.wantSupportedAlgorithms, signingAlgorithms.supportedAlgorithms)
			assert.Equal(t, test.wantUnsupportedAlgorithms, signingAlgorithms.unsupportedAlgorithms)
		})
	}
}

func TestSetURLQueryParam(t *testing.T) {
	var oidcAuthProviderGoogle = "google"
	tests := []struct {
		name             string
		inputCallbackURL string
		inputParamName   string
		inputParamValue  string
		wantCallbackURL  string
		wantError        error
	}{{
		name:             "Add provider to callback URL",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "http://localhost:4984/default/_oidc_callback?provider=google",
	}, {
		name:             "Add provider to callback URL with ? character",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback?",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "http://localhost:4984/default/_oidc_callback?provider=google",
	}, {
		name:             "Add provider to empty callback URL",
		inputCallbackURL: "",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "",
		wantError:        ErrSetURLQueryParam,
	}, {
		name:             "Add empty provider value to callback URL",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  "",
		wantCallbackURL:  "",
		wantError:        ErrSetURLQueryParam,
	}, {
		name:             "Add empty provider name to callback URL",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback",
		inputParamName:   "",
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "",
		wantError:        ErrSetURLQueryParam,
	}, {
		name:             "Update provider in callback URL",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback?provider=facebook",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "http://localhost:4984/default/_oidc_callback?provider=google",
	}, {
		name:             "Add provider to callback URL with illegal value in query param",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback?provider=%%3",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "",
		wantError:        url.EscapeError("%%3"),
	}, {
		name:             "Add provider to callback URL with missing protocol scheme",
		inputCallbackURL: "://localhost:4984/default/_oidc_callback",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  oidcAuthProviderGoogle,
		wantCallbackURL:  "",
		wantError:        &url.Error{Op: "parse", URL: "://localhost:4984/default/_oidc_callback", Err: errors.New("missing protocol scheme")},
	}, {
		name:             "Add provider with non-conforming characters to callback URL",
		inputCallbackURL: "http://localhost:4984/default/_oidc_callback",
		inputParamName:   OIDCAuthProvider,
		inputParamValue:  "test&provider ?",
		wantCallbackURL:  "http://localhost:4984/default/_oidc_callback?provider=test%26provider+%3F",
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			callbackURL, err := SetURLQueryParam(test.inputCallbackURL, test.inputParamName, test.inputParamValue)
			assert.Equal(t, test.wantError, err)
			assert.Equal(t, test.wantCallbackURL, callbackURL)
		})
	}
}

func TestGetExpirationPass(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		wantTTL time.Duration
		wantOK  bool
	}{{
		name: "Expires and Date properly set",
		headers: http.Header{
			"Date":    []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires": []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 10800 * time.Second,
		wantOK:  true,
	}, {
		name: "empty headers",
		headers: http.Header{
			"Date":    []string{""},
			"Expires": []string{""},
		},
		wantOK: false,
	}, {
		name: "lack of Expires short-circuit Date parsing",
		headers: http.Header{
			"Date":    []string{"foo"},
			"Expires": []string{""},
		},
		wantOK: false,
	}, {
		name: "lack of Date short-circuit Expires parsing",
		headers: http.Header{
			"Date":    []string{""},
			"Expires": []string{"foo"},
		},
		wantOK: false,
	}, {
		name: "no Date",
		headers: http.Header{
			"Expires": []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
		},
		wantTTL: 0,
		wantOK:  false,
	}, {
		name: "no Expires",
		headers: http.Header{
			"Date": []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
		},
		wantTTL: 0,
		wantOK:  false,
	}, {
		name: "Expires less than Date",
		headers: http.Header{
			"Date":    []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
			"Expires": []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
		},
		wantTTL: 0,
		wantOK:  false,
	}, {
		name: "Expires is zero",
		headers: http.Header{
			"Date":    []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
			"Expires": []string{"0"},
		},
		wantTTL: 0,
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := getExpiration(tc.headers)
			require.NoError(t, err, "error getting expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestGetExpirationFail(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		wantTTL time.Duration
		wantOK  bool
	}{{
		name: "malformed Date header",
		headers: http.Header{
			"Date":    []string{"foo"},
			"Expires": []string{"Mon, 01 Jun 2020 19:49:09 GMT"},
		},
		wantTTL: 0,
		wantOK:  false,
	}, {
		name: "malformed exp header",
		headers: http.Header{
			"Date":    []string{"Mon, 01 Jun 2020 19:49:09 GMT"},
			"Expires": []string{"bar"},
		},
		wantTTL: 0,
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := getExpiration(tc.headers)
			require.Error(t, err, "No error getting expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestCacheablePass(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		wantTTL time.Duration
		wantOK  bool
	}{{
		name: "valid Cache-Control",
		headers: http.Header{
			"Cache-Control": []string{"max-age=100"},
		},
		wantTTL: 100 * time.Second,
		wantOK:  true,
	}, {
		name: "valid Date and Expires",
		headers: http.Header{
			"Date":    []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires": []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 10800 * time.Second,
		wantOK:  true,
	}, {
		name: "Cache-Control supersedes Date and Expires",
		headers: http.Header{
			"Cache-Control": []string{"max-age=100"},
			"Date":          []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires":       []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 100 * time.Second,
		wantOK:  true,
	}, {
		name:    "no caching headers",
		headers: http.Header{},
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := cacheable(tc.headers)
			require.NoError(t, err, "Error getting expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestCacheableFail(t *testing.T) {
	tests := []struct {
		name    string
		header  http.Header
		wantTTL time.Duration
		wantOK  bool
	}{{
		name: "invalid Cache-Control short-circuits",
		header: http.Header{
			"Cache-Control": []string{"max-age"},
			"Date":          []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires":       []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 0,
		wantOK:  false,
	}, {
		name: "no Cache-Control invalid Expires",
		header: http.Header{
			"Date":    []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires": []string{"boo"},
		},
		wantTTL: 0,
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := cacheable(tc.header)
			require.Error(t, err, "No error checking max age and expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestCacheControlMaxAgePass(t *testing.T) {
	tests := []struct {
		name       string
		header     http.Header
		wantMaxAge time.Duration
		wantOK     bool
	}{{
		name: "Cache-Control header with valid max-age directive",
		header: http.Header{
			"Cache-Control": []string{"max-age=12"},
		},
		wantMaxAge: 12 * time.Second,
		wantOK:     true,
	}, {
		name: "Cache-Control header with invalid max-age directive",
		header: http.Header{
			"Cache-Control": []string{"max-age=-12"},
		},
		wantMaxAge: 0,
		wantOK:     false,
	}, {
		name: "Cache-Control header with zero max-age directive",
		header: http.Header{
			"Cache-Control": []string{"max-age=0"},
		},
		wantMaxAge: 0,
		wantOK:     false,
	}, {
		name: "Cache-Control header with valid max-age set as public",
		header: http.Header{
			"Cache-Control": []string{"public, max-age=12"},
		},
		wantMaxAge: 12 * time.Second,
		wantOK:     true,
	}, {
		name: "Cache-Control header with valid max-age set as public and must-revalidate",
		header: http.Header{
			"Cache-Control": []string{"public, max-age=40192, must-revalidate"},
		},
		wantMaxAge: 40192 * time.Second,
		wantOK:     true,
	}, {
		name: "Cache-Control header with invalid max-age set as public and must-revalidate",
		header: http.Header{
			"Cache-Control": []string{"public, not-max-age=12, must-revalidate"},
		},
		wantMaxAge: time.Duration(0),
		wantOK:     false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			maxAge, ok, err := cacheControlMaxAge(tc.header)
			require.NoError(t, err, "error checking max age and expiry")
			assert.Equal(t, tc.wantMaxAge, maxAge, "max age mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestCacheControlMaxAgeFail(t *testing.T) {
	tests := []struct {
		name       string
		header     http.Header
		wantMaxAge time.Duration
		wantOK     bool
	}{{
		name: "Cache-Control header with max-age directive that has non-integer value",
		header: http.Header{
			"Cache-Control": []string{"max-age=foo"},
		},
		wantMaxAge: 0,
		wantOK:     false,
	}, {
		name: "Cache-Control header with max-age directive that has emnpty value",
		header: http.Header{
			"Cache-Control": []string{"max-age="},
		},
		wantMaxAge: 0,
		wantOK:     false,
	}, {
		name: "Cache-Control header with max-age directive that has no value",
		header: http.Header{
			"Cache-Control": []string{"max-age"},
		},
		wantMaxAge: 0,
		wantOK:     false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			maxAge, ok, err := cacheControlMaxAge(tc.header)
			require.Error(t, err, "No error checking max age and expiry")
			assert.Equal(t, tc.wantMaxAge, maxAge, "max age mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestGetDiscoveryEndpoint(t *testing.T) {
	tests := []struct {
		name                 string
		issuer               string
		expectedDiscoveryURL string
	}{{
		name:                 "http issuer URL",
		issuer:               "http://foo.com",
		expectedDiscoveryURL: "http://foo.com/.well-known/openid-configuration",
	}, {
		name:                 "secure http issuer URL",
		issuer:               "https://foo.com",
		expectedDiscoveryURL: "https://foo.com/.well-known/openid-configuration",
	}, {
		name:                 "http issuer URL with slash suffix",
		issuer:               "http://foo.com/",
		expectedDiscoveryURL: "http://foo.com/.well-known/openid-configuration",
	}, {
		name:                 "secure http issuer URL with slash suffix",
		issuer:               "https://foo.com/",
		expectedDiscoveryURL: "https://foo.com/.well-known/openid-configuration",
	}, {
		name:                 "empty issuer",
		issuer:               "",
		expectedDiscoveryURL: "/.well-known/openid-configuration",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			discoveryURLActual := GetStandardDiscoveryEndpoint(tc.issuer)
			assert.Equal(t, tc.expectedDiscoveryURL, discoveryURLActual)
		})
	}
}

func TestIsStandardDiscovery(t *testing.T) {
	tests := []struct {
		name                        string
		provider                    *OIDCProvider
		isStandardDiscoveryExpected bool
	}{{
		name: "provider with valid discovery URL",
		provider: &OIDCProvider{
			DiscoveryURI: "https://foo.com/.well-known/openid-configuration",
		},
		isStandardDiscoveryExpected: false,
	}, {
		name: "provider with valid discovery URL and config validation disabled",
		provider: &OIDCProvider{
			DiscoveryURI:            "https://foo.com/.well-known/openid-configuration",
			DisableConfigValidation: true,
		},
		isStandardDiscoveryExpected: false,
	}, {
		name: "provider with valid discovery URL and config validation enabled",
		provider: &OIDCProvider{
			DiscoveryURI:            "https://foo.com/.well-known/openid-configuration",
			DisableConfigValidation: false,
		},
		isStandardDiscoveryExpected: false,
	}, {
		name: "provider with no discovery URL and config validation enabled",
		provider: &OIDCProvider{
			DisableConfigValidation: false,
		},
		isStandardDiscoveryExpected: true,
	}, {
		name: "provider with no discovery URL and config validation disabled",
		provider: &OIDCProvider{
			DisableConfigValidation: true,
		},
		isStandardDiscoveryExpected: false,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isStandardDiscoveryActual := tc.provider.isStandardDiscovery()
			assert.Equal(t, tc.isStandardDiscoveryExpected, isStandardDiscoveryActual)
		})
	}
}

func TestFormatUsername(t *testing.T) {
	tests := []struct {
		name             string
		username         interface{}
		usernameExpected string
		errorExpected    error
	}{{
		name:             "format username with valid username of type string",
		username:         "80249751",
		usernameExpected: "80249751",
	}, {
		name:             "format username with valid username of type int",
		username:         80249751,
		usernameExpected: "",
		errorExpected:    errors.New("oidc: can't treat value of type: int as valid username"),
	}, {
		name:             "format username with valid username of type float64",
		username:         float64(80249751),
		usernameExpected: "80249751",
	}, {
		name:             "format username with valid username of type json.Number",
		username:         json.Number("80249751"),
		usernameExpected: "80249751",
	}, {
		name:             "format username with valid username of type nil",
		username:         nil,
		usernameExpected: "",
		errorExpected:    errors.New("oidc: can't treat value of type: <nil> as valid username"),
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			username, err := formatUsername(tc.username)
			assert.Equal(t, tc.errorExpected, err)
			assert.Equal(t, tc.usernameExpected, username)
		})
	}
}

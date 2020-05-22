//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
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
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAuth)()

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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAuth)()

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
			provider := providerMap.GetProviderForIssuer(test.Issuer, test.Audiences)
			assert.Equal(tt, test.ExpectedProvider, provider)
		})
	}
}

func TestOIDCUsername(t *testing.T) {

	provider := OIDCProvider{
		Name:   "Some_Provider",
		Issuer: "http://www.someprovider.com",
	}

	err := provider.InitUserPrefix()
	assert.NoError(t, err)
	assert.Equal(t, "www.someprovider.com", provider.UserPrefix)

	// test username suffix
	oidcUsername := GetOIDCUsername(&provider, "bernard")
	assert.Equal(t, "www.someprovider.com_bernard", oidcUsername)
	assert.Equal(t, true, IsValidPrincipalName(oidcUsername))

	// test char escaping
	oidcUsername = GetOIDCUsername(&provider, "{bernard}")
	assert.Equal(t, "www.someprovider.com_%7Bbernard%7D", oidcUsername)
	assert.Equal(t, true, IsValidPrincipalName(oidcUsername))

	// test URL with paths
	provider.UserPrefix = ""
	provider.Issuer = "http://www.someprovider.com/extra"
	err = provider.InitUserPrefix()
	assert.NoError(t, err)
	assert.Equal(t, "www.someprovider.com%2Fextra", provider.UserPrefix)

	// test invalid URL
	provider.UserPrefix = ""
	provider.Issuer = "http//www.someprovider.com"
	err = provider.InitUserPrefix()
	assert.NoError(t, err)
	// falls back to provider name:
	assert.Equal(t, "Some_Provider", provider.UserPrefix)

}

func TestOIDCProvider_InitOIDCClient(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAuth)()

	clientID := "SGW-TEST"
	callbackURL := "http://sgw-test:4984/_callback"

	tests := []struct {
		Name             string
		Provider         *OIDCProvider
		ErrContains      string
		ExpectOIDCClient bool
	}{
		{
			Name:        "nil provider",
			ErrContains: ErrMsgNilProvider,
		},
		{
			Name:        "empty provider",
			Provider:    &OIDCProvider{},
			ErrContains: "Issuer not defined",
		},
		{
			Name: "unavailable",
			Provider: &OIDCProvider{
				Issuer: "http://127.0.0.1:12345/auth",
			},
			ErrContains: ErrMsgUnableToDiscoverConfig,
		},
		{
			Name: "valid provider",
			Provider: &OIDCProvider{
				ClientID:    clientID,
				Issuer:      "https://accounts.google.com",
				CallbackURL: &callbackURL,
			},
			ExpectOIDCClient: true,
		},
	}

	defaultWait := OIDCDiscoveryRetryWait
	OIDCDiscoveryRetryWait = 10 * time.Millisecond
	defer func() {
		OIDCDiscoveryRetryWait = defaultWait
	}()

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Provider.InitOIDCClient()
			if test.ErrContains != "" {
				assert.Error(t, err)
				assert.Contains(tt, err.Error(), test.ErrContains)
			} else {
				assert.NoError(t, err)
			}

			if test.Provider != nil {
				client := test.Provider.GetClient(func(string, bool) string { return "" })
				if test.ExpectOIDCClient {
					assert.NotEqual(tt, (*OIDCClient)(nil), client)
				} else {
					assert.Equal(tt, (*OIDCClient)(nil), client)
				}
			}
		})
	}
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
			discoveryURL := strings.TrimSuffix(issuer, "/") + OIDCDiscoveryConfigPath
			op := &OIDCProvider{Issuer: issuer}
			metadata, err := op.FetchCustomProviderConfig(discoveryURL)
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
	issuer, audiences, err := GetIssuerWithAudience(jwt)
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
			signingAlgorithms := provider.GetSigningAlgorithms(metadata)
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

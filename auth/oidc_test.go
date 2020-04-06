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
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
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
		ClientID: &clientID,
	}
	glProvider := OIDCProvider{
		Name:     "Gügul",
		Issuer:   "http://127.0.0.1:1235",
		ClientID: &clientID,
	}
	fbProvider := OIDCProvider{
		Name:     "Fæsbuk",
		Issuer:   "http://127.0.0.1:1236",
		ClientID: &clientID,
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
			ErrContains: "nil provider",
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
			ErrContains: "connection refused",
		},
		{
			Name: "valid provider",
			Provider: &OIDCProvider{
				ClientID:    &clientID,
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
				client := test.Provider.GetClient(func() string { return "" })
				if test.ExpectOIDCClient {
					assert.NotEqual(tt, (*OIDCClient)(nil), client)
				} else {
					assert.Equal(tt, (*OIDCClient)(nil), client)
				}
			}
		})
	}

}

func TestNewProvider(t *testing.T) {
	tests := []struct {
		name            string
		data            string
		trailingSlash   bool
		wantAuthURL     string
		wantTokenURL    string
		wantUserInfoURL string
		wantAlgorithms  []string
		wantErr         bool
	}{
		{
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
		},
		{
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
		},
		{
			name: "unsupported_algorithms",
			data: `{
				"issuer": "${issuer}",
				"authorization_endpoint": "https://example.com/auth",
				"token_endpoint": "https://example.com/token",
				"jwks_uri": "https://example.com/keys",
				"id_token_signing_alg_values_supported": [
					"RS256", "RS384", "ES256", "HS256", "none"
				]
			}`,
			wantAuthURL:    "https://example.com/auth",
			wantTokenURL:   "https://example.com/token",
			wantAlgorithms: []string{"RS256", "RS384", "ES256"},
		},
		{
			name: "mismatched_issuer",
			data: `{
				"issuer": "https://example.com",
				"authorization_endpoint": "https://example.com/auth",
				"token_endpoint": "https://example.com/token",
				"jwks_uri": "https://example.com/keys",
				"id_token_signing_alg_values_supported": ["RS256"]
			}`,
			wantErr: true,
		},
		{
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
		},
		{
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var issuer string
			hf := func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != discoveryConfigPath {
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
			wellKnown := strings.TrimSuffix(issuer, "/") + discoveryConfigPath
			p, err := NewProvider(ctx, issuer, wellKnown)
			if err != nil {
				assert.True(t, test.wantErr, "Unexpected Error!")
				return
			}
			assert.False(t, test.wantErr, "Expected Error Not Found!")
			assert.Equal(t, test.wantAuthURL, reflect.ValueOf(*p).FieldByName("authURL").String())
			assert.Equal(t, test.wantTokenURL, reflect.ValueOf(*p).FieldByName("tokenURL").String())
			assert.Equal(t, test.wantUserInfoURL, reflect.ValueOf(*p).FieldByName("userInfoURL").String())
			assert.True(t, !reflect.DeepEqual(test.wantAlgorithms, reflect.ValueOf(*p).FieldByName("algorithms")))
		})
	}
}

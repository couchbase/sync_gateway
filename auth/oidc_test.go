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
	"testing"
	"time"

	"github.com/coreos/go-oidc/oidc"
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
			ErrContains: "unable to discover config",
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
					assert.NotEqual(tt, (*oidc.Client)(nil), client)
				} else {
					assert.Equal(tt, (*oidc.Client)(nil), client)
				}
			}
		})
	}

}

// This test verifies that common OpenIDConnect providers return configurations that
// don't cause any errors in the Sync Gateway processing, for example if the URL parsing fails.
// If any errors are found from provider, these should be dealt with appropriately.  As new
// OIDC providers are tested and supported, their discovery URL should be added to this list.
// See https://github.com/couchbase/sync_gateway/issues/3065
func TestFetchCustomProviderConfig(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test is only enabled in integration test mode due to remote webserver dependencies")
	}

	providerDiscoveryUrls := []string{
		"https://accounts.google.com/.well-known/openid-configuration",
		"https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration",
	}

	for _, discoveryUrl := range providerDiscoveryUrls {
		oidcProvider := OIDCProvider{}
		_, err := oidcProvider.FetchCustomProviderConfig(discoveryUrl)
		assert.True(t, err == nil)
	}

}

// Check fetching custom provider configuration with blank or empty provider
// discovery URL. If discovery URL is empty, it must use the standard discovery
// URL. Fetching  custom provider configuration should fail while sending HTTP
// request. Error message should contain 'unsupported protocol scheme'.
func TestFetchCustomProviderConfigWithEmptyURL(t *testing.T) {
	provider := OIDCProvider{}
	_, err := provider.FetchCustomProviderConfig("")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unsupported protocol scheme")
}

// Check fetching custom provider configuration with bad or unknown provider
// discovery URL. Fetching  custom provider configuration should fail while
// sending HTTP request. Error message should contain 'no such host'.
func TestFetchCustomProviderConfigWithBadURL(t *testing.T) {
	provider := OIDCProvider{}
	_, err := provider.FetchCustomProviderConfig("https://accounts.unknown.com")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such host")
}

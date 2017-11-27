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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func TestOIDCUsername(t *testing.T) {

	provider := OIDCProvider{
		Issuer: "http://www.someprovider.com",
	}

	err := provider.InitUserPrefix()
	assert.Equals(t, err, nil)
	assert.Equals(t, provider.UserPrefix, "www.someprovider.com")

	oidcUsername := GetOIDCUsername(&provider, "bernard")
	assert.Equals(t, oidcUsername, "www.someprovider.com_bernard")
	assert.Equals(t, IsValidPrincipalName(oidcUsername), true)

	oidcUsername = GetOIDCUsername(&provider, "{bernard}")
	assert.Equals(t, oidcUsername, "www.someprovider.com_%7Bbernard%7D")
	assert.Equals(t, IsValidPrincipalName(oidcUsername), true)

	provider.UserPrefix = ""
	provider.Issuer = "http://www.someprovider.com/extra"
	err = provider.InitUserPrefix()
	assert.Equals(t, err, nil)
	assert.Equals(t, provider.UserPrefix, "www.someprovider.com%2Fextra")
	assert.Equals(t, IsValidPrincipalName(oidcUsername), true)

}

// This test verifies that common OpenIDConnect providers return configurations that
// don't cause any errors in the Sync Gateway processing, for example if the URL parsing fails.
// If any errors are found from provider, these should be dealt with appropriately.  As new
// OIDC providers are tested and supported, their discovery URL should be added to this list.
// See https://github.com/couchbase/sync_gateway/issues/3065
func TestFetchCustomProviderConfig(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
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

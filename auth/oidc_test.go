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

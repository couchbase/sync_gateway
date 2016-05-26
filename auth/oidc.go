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
	"errors"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/oidc"
	"github.com/couchbase/sync_gateway/base"
)

// Options for OpenID Connect
type OIDCOptions struct {
	JWTOptions
	Issuer         *string `json:"issuer,omitempty"`          // OIDC Issuer
	AuthorizeURL   *string `json:"authorize_url,omitempty"`   // OIDC OP authorize endpoint.
	TokenURL       *string `json:"token_url,omitempty"`       // OIDC OP token endpoint.
	Register       bool    `json:"register"`                  // If true, server will register new user accounts
	ClientID       *string `json:"client_id,omitempty"`       // Client ID
	ValidationKey  *string `json:"validation_key,omitempty"`  // Client secret
	CallbackURL    *string `json:"callback_url,omitempty"`    // Sync Gateway redirect URL.  Needs to be specified to handle load balancer endpoints?  Or can we lazy load on first client use, based on request
	DisableSession bool    `json:"disable_session,omitempty"` // Disable Sync Gateway session creation on successful OIDC authentication
}

func CreateOIDCClient(options *OIDCOptions) (*oidc.Client, error) {

	var config oidc.ProviderConfig
	var err error
	if options.Issuer == nil {
		return nil, errors.New("Issuer must be defined for OpenID Connect")
	}
	base.LogTo("OIDC", "Attempting to fetch provider config from discovery endpoint for issuer %s...", *options.Issuer)
	retryCount := 5
	for i := 1; i <= 5; i++ {
		config, err = oidc.FetchProviderConfig(http.DefaultClient, *options.Issuer)
		if err == nil {
			break
		}
		base.LogTo("OIDC", "Unable to fetch provider config from discovery endpoint for %s (attempt %v/%v): %v",
			options.Issuer, i, retryCount, err)
		time.Sleep(1 * time.Second)
	}

	// TODO: If discovery endpoint not present, attempt to build from lower level components?
	/*
		if config == nil {
			if options.Issuer == nil || options.AuthorizeURL == nil || options.TokenURL == nil {
				return nil, errors.New("Issuer, authorize_url and token_url must be present if discovery endpoint is unavailable")
			}
			config := &oidc.ProviderConfig{


		}
	*/

	clientCredentials := oidc.ClientCredentials{
		ID:     *options.ClientID,
		Secret: *options.ValidationKey,
	}

	clientConfig := oidc.ClientConfig{
		ProviderConfig: config,
		Credentials:    clientCredentials,
		RedirectURL:    *options.CallbackURL,
	}

	clientConfig.Scope = []string{"openid", "email"}

	client, err := oidc.NewClient(clientConfig)
	if err != nil {
		return nil, err
	}

	// Start process for ongoing sync of the provider config
	client.SyncProviderConfig(*options.Issuer)

	return client, nil
}

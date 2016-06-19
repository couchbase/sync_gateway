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
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/go-oidc/oauth2"
	"github.com/coreos/go-oidc/oidc"
	"github.com/couchbase/sync_gateway/base"
)

// Options for OpenID Connect
type OIDCOptions struct {
	Providers       OIDCProviderMap `json:"providers,omitempty"`        // List of OIDC issuers
	DefaultProvider *string         `json:"default_provider,omitempty"` // Issuer used when not specified by client
}

type OIDCProvider struct {
	JWTOptions
	Issuer             string   `json:"issuer"`                    // OIDC Issuer
	Register           bool     `json:"register"`                  // If true, server will register new user accounts
	ClientID           *string  `json:"client_id,omitempty"`       // Client ID
	ValidationKey      *string  `json:"validation_key,omitempty"`  // Client secret
	CallbackURL        *string  `json:"callback_url,omitempty"`    // Sync Gateway redirect URL.  Needs to be specified to handle load balancer endpoints?  Or can we lazy load on first client use, based on request
	DisableSession     bool     `json:"disable_session,omitempty"` // Disable Sync Gateway session creation on successful OIDC authentication
	Scope              []string `json:"scope,omitempty"`           // Scope sent for openid request
	IncludeAccessToken bool     `json:"include_access,omitempty"`  // Whether the _oidc_callback response should include OP access token and associated fields (token_type, expires_in)
	OIDCClient         *oidc.Client
	OIDCClientOnce     sync.Once
	IsDefault          bool
	Name               string
}

type OIDCProviderMap map[string]*OIDCProvider

func (opm OIDCProviderMap) GetDefaultProvider() *OIDCProvider {
	for _, provider := range opm {
		if provider.IsDefault {
			return provider
		}
	}
	return nil
}

func (opm OIDCProviderMap) GetProviderForIssuer(issuer, audience string) *OIDCProvider {
	for _, provider := range opm {
		if provider.Issuer == issuer && *provider.ClientID == audience {
			return provider
		}
	}
	return nil
}

func (op *OIDCProvider) GetClient() *oidc.Client {
	// Initialize the client on first request
	op.OIDCClientOnce.Do(func() {
		var err error
		if err = op.InitOIDCClient(); err != nil {
			base.Warn("Unable to initialize OIDC client: %v", err)
		}
	})
	return op.OIDCClient
}

func (op *OIDCProvider) InitOIDCClient() error {

	var config oidc.ProviderConfig
	var err error
	if op.Issuer == "" {
		return fmt.Errorf("Issuer not defined for OpenID Connect provider %+v", op)
	}
	base.LogTo("OIDC", "Attempting to fetch provider config from discovery endpoint for issuer %s...", op.Issuer)
	retryCount := 5
	var providerLoaded bool
	for i := 1; i <= 5; i++ {
		config, err = oidc.FetchProviderConfig(http.DefaultClient, op.Issuer)
		if err == nil {
			providerLoaded = true
			break
		}
		base.LogTo("OIDC", "Unable to fetch provider config from discovery endpoint for %s (attempt %v/%v): %v",
			op.Issuer, i, retryCount, err)
		time.Sleep(1 * time.Second)
	}

	if !providerLoaded {
		return fmt.Errorf("Unable to fetch provider - OIDC unavailable")
	}

	clientCredentials := oidc.ClientCredentials{
		ID:     *op.ClientID,
		Secret: *op.ValidationKey,
	}

	clientConfig := oidc.ClientConfig{
		ProviderConfig: config,
		Credentials:    clientCredentials,
		RedirectURL:    *op.CallbackURL,
	}

	if op.Scope != nil || len(op.Scope) > 0 {
		clientConfig.Scope = op.Scope
	} else {
		clientConfig.Scope = []string{"openid", "email"}
	}

	op.OIDCClient, err = oidc.NewClient(clientConfig)
	if err != nil {
		return err
	}

	// Start process for ongoing sync of the provider config
	op.OIDCClient.SyncProviderConfig(op.Issuer)

	return nil
}

// Converts an OpenID Connect / OAuth2 error to an HTTP error
func OIDCToHTTPError(err error) error {
	if oauthErr, ok := err.(*oauth2.Error); ok {
		status := 400
		switch oauthErr.Type {
		case oauth2.ErrorAccessDenied,
			oauth2.ErrorUnauthorizedClient,
			oauth2.ErrorInvalidClient,
			oauth2.ErrorInvalidGrant,
			oauth2.ErrorInvalidRequest:
			status = 401
		case oauth2.ErrorServerError:
			status = 502
		case oauth2.ErrorUnsupportedGrantType,
			oauth2.ErrorUnsupportedResponseType:
			status = 400
		}
		err = base.HTTPErrorf(status, "OpenID Connect error: %s (%s)",
			oauthErr.Description, oauthErr.Type)
	}
	return err
}

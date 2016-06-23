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
	"crypto/md5"
	"fmt"
	"net/http"
	"net/url"
	"strings"
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
	UserPrefix         string   `json:"user_prefix,omitempty"`     // Username prefix for users created for this provider
	OIDCClient         *oidc.Client
	OIDCClientOnce     sync.Once
	IsDefault          bool
	Name               string
}

type OIDCProviderMap map[string]*OIDCProvider

type OIDCCallbackURLFunc func() string

func (opm OIDCProviderMap) GetDefaultProvider() *OIDCProvider {
	for _, provider := range opm {
		if provider.IsDefault {
			return provider
		}
	}
	return nil
}

func (opm OIDCProviderMap) GetProviderForIssuer(issuer string, audiences []string) *OIDCProvider {
	for _, provider := range opm {
		if provider.Issuer == issuer && provider.ClientID != nil {
			// Iterate over the audiences looking for a match
			for _, aud := range audiences {
				if *provider.ClientID == aud {
					return provider
				}
			}
		}
	}
	return nil
}

func (op *OIDCProvider) GetClient(buildCallbackURLFunc OIDCCallbackURLFunc) *oidc.Client {
	// Initialize the client on first request.  If the callback URL isn't defined for the provider,
	// uses buildCallbackURLFunc to construct (based on current request)
	op.OIDCClientOnce.Do(func() {
		var err error
		// If the redirect URL is not defined for the provider generate it from the
		// handler request and set it on the provider
		if op.CallbackURL == nil || *op.CallbackURL == "" {
			callbackURL := buildCallbackURLFunc()
			if callbackURL != "" {
				op.CallbackURL = &callbackURL
			}
		}
		if err = op.InitOIDCClient(); err != nil {
			base.Warn("Unable to initialize OIDC client: %v", err)
		}
	})

	return op.OIDCClient
}

// To support multiple providers referencing the same issuer, the user prefix used to build the SG usernames for
// a provider is based on the issuer
func (op *OIDCProvider) InitUserPrefix() error {

	// If the user prefix has been explicitly defined, skip calculation
	if op.UserPrefix != "" {
		return nil
	}

	issuerURL, err := url.ParseRequestURI(op.Issuer)
	if err != nil {
		base.Warn("Unable to parse issuer URI when initializing user prefix - using provider name")
		op.UserPrefix = op.Name
		return nil
	}
	op.UserPrefix = issuerURL.Host + issuerURL.Path

	// If the prefix contains forward slash or underscore, it's not valid as-is for a username: forward slash
	// breaks the REST API, underscore breaks uniqueness of "[prefix]_[sub]".  MD5 hash the prefix in this
	// scenario.
	if strings.ContainsAny(op.UserPrefix, "/_") {
		op.UserPrefix = fmt.Sprintf("%x", md5.Sum([]byte(op.UserPrefix)))
	}
	return nil
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
		ID: *op.ClientID,
	}
	if op.ValidationKey != nil {
		clientCredentials.Secret = *op.ValidationKey
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

	// Initialize the prefix for users created for this provider
	if err = op.InitUserPrefix(); err != nil {
		return err
	}

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

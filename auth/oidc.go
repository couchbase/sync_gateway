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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/coreos/go-oidc"
	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"gopkg.in/square/go-jose.v2"
)

const (
	discoveryConfigPath = "/.well-known/openid-configuration"
)

var OIDCDiscoveryRetryWait = 500 * time.Millisecond

// Options for OpenID Connect
type OIDCOptions struct {
	Providers       OIDCProviderMap `json:"providers,omitempty"`        // List of OIDC issuers
	DefaultProvider *string         `json:"default_provider,omitempty"` // Issuer used when not specified by client
}

type OIDCClient struct {
	Provider *oidc.Provider
	Config   oauth2.Config
	Context  context.Context
}

type OIDCProvider struct {
	JWTOptions
	Issuer                  string   `json:"issuer"`                           // OIDC Issuer
	Register                bool     `json:"register"`                         // If true, server will register new user accounts
	ClientID                *string  `json:"client_id,omitempty"`              // Client ID
	ValidationKey           *string  `json:"validation_key,omitempty"`         // Client secret
	CallbackURL             *string  `json:"callback_url,omitempty"`           // Sync Gateway redirect URL.  Needs to be specified to handle load balancer endpoints?  Or can we lazy load on first client use, based on request
	DisableSession          bool     `json:"disable_session,omitempty"`        // Disable Sync Gateway session creation on successful OIDC authentication
	Scope                   []string `json:"scope,omitempty"`                  // Scope sent for openid request
	IncludeAccessToken      bool     `json:"include_access,omitempty"`         // Whether the _oidc_callback response should include OP access token and associated fields (token_type, expires_in)
	UserPrefix              string   `json:"user_prefix,omitempty"`            // Username prefix for users created for this provider
	DiscoveryURI            string   `json:"discovery_url,omitempty"`          // Non-standard discovery endpoints
	DisableConfigValidation bool     `json:"disable_cfg_validation,omitempty"` // Bypasses config validation based on the OIDC spec.  Required for some OPs that don't strictly adhere to spec (eg. Yahoo)
	OIDCClient              *OIDCClient
	OIDCClientOnce          sync.Once
	IsDefault               bool
	Name                    string
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
	base.Debugf(base.KeyAuth, "GetProviderForIssuer with issuer: %v, audiences: %+v", base.UD(issuer), base.UD(audiences))
	for _, provider := range opm {
		if provider.Issuer == issuer && provider.ClientID != nil {
			// Iterate over the audiences looking for a match
			for _, aud := range audiences {
				if *provider.ClientID == aud {
					base.Debugf(base.KeyAuth, "Provider matches, returning")
					return provider
				}
			}
		}
	}
	base.Debugf(base.KeyAuth, "No provider match found")
	return nil
}

func (op *OIDCProvider) GetClient(buildCallbackURLFunc OIDCCallbackURLFunc) *OIDCClient {
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
			base.Errorf("Unable to initialize OIDC client: %v", err)
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
		base.Warnf("Unable to parse issuer URI when initializing user prefix - using provider name")
		op.UserPrefix = op.Name
		return nil
	}
	op.UserPrefix = issuerURL.Host + issuerURL.Path

	// If the prefix contains forward slash or underscore, it's not valid as-is for a username: forward slash
	// breaks the REST API, underscore breaks uniqueness of "[prefix]_[sub]".  URL encode the prefix to cover
	// this scenario

	op.UserPrefix = url.QueryEscape(op.UserPrefix)

	return nil
}

func (op *OIDCProvider) InitOIDCClient() error {
	if op == nil {
		return fmt.Errorf("nil provider")
	}

	if op.Issuer == "" {
		return base.RedactErrorf("Issuer not defined for OpenID Connect provider %+v", base.UD(op))
	}

	provider, context, _, err := op.DiscoverConfig()
	if err != nil || provider == nil {
		return pkgerrors.Wrap(err, "unable to discover config")
	}

	config := oauth2.Config{
		ClientID:    *op.ClientID,
		Endpoint:    provider.Endpoint(),
		RedirectURL: *op.CallbackURL,
	}

	if op.ValidationKey != nil {
		config.ClientSecret = *op.ValidationKey
	}

	if op.Scope != nil || len(op.Scope) > 0 {
		config.Scopes = op.Scope
	} else {
		config.Scopes = []string{oidc.ScopeOpenID, "email"}
	}

	// Initialize the prefix for users created for this provider
	if err = op.InitUserPrefix(); err != nil {
		return err
	}
	op.OIDCClient = &OIDCClient{
		Provider: provider,
		Config:   config,
		Context:  context,
	}

	return nil
}

func (op *OIDCProvider) DiscoverConfig() (config *oidc.Provider, ctx context.Context, shouldSync bool, err error) {
	ctx = context.Background()
	// If discovery URI is explicitly defined, use it instead of the standard issuer-based discovery.
	if op.DiscoveryURI != "" || op.DisableConfigValidation {
		base.Infof(base.KeyAuth, "Fetching provider config from explicitly defined discovery endpoint: %s", base.UD(op.DiscoveryURI))
		config, err = NewProvider(ctx, op.Issuer, op.DiscoveryURI)
		shouldSync = false
	} else {
		wellKnown := strings.TrimSuffix(op.Issuer, "/") + discoveryConfigPath
		base.Infof(base.KeyAuth, "Fetching provider config from standard issuer-based discovery endpoint: %s", base.UD(wellKnown))
		maxRetryAttempts := 5
		for i := 1; i <= maxRetryAttempts; i++ {
			config, err = oidc.NewProvider(ctx, op.Issuer)
			if err == nil {
				shouldSync = true
				break
			}
			base.Debugf(base.KeyAuth, "Unable to fetch provider config from discovery endpoint for %s (attempt %v/%v): %v",
				base.UD(op.Issuer), i, maxRetryAttempts, err)
			time.Sleep(OIDCDiscoveryRetryWait)
		}
	}

	return config, ctx, shouldSync, err
}

func GetOIDCUsername(provider *OIDCProvider, subject string) string {
	return fmt.Sprintf("%s_%s", provider.UserPrefix, url.QueryEscape(subject))
}

type jsonTime time.Time

type idToken struct {
	Issuer       string                 `json:"iss"`
	Subject      string                 `json:"sub"`
	Audience     audience               `json:"aud"`
	Expiry       jsonTime               `json:"exp"`
	IssuedAt     jsonTime               `json:"iat"`
	NotBefore    *jsonTime              `json:"nbf"`
	Nonce        string                 `json:"nonce"`
	AtHash       string                 `json:"at_hash"`
	ClaimNames   map[string]string      `json:"_claim_names"`
	ClaimSources map[string]claimSource `json:"_claim_sources"`
}

type claimSource struct {
	Endpoint    string `json:"endpoint"`
	AccessToken string `json:"access_token"`
}

type audience []string

func GetIDToken(rawIDToken string) (*oidc.IDToken, error) {
	_, err := jose.ParseSigned(rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("oidc: malformed jwt: %v", err)
	}

	// Throw out tokens with invalid claims before trying to verify the token.
	// This lets us do cheap checks before possibly re-syncing keys.
	payload, err := parseJWT(rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("oidc: malformed jwt: %v", err)
	}
	var token idToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return nil, fmt.Errorf("oidc: failed to unmarshal claims: %v", err)
	}

	idToken := &oidc.IDToken{
		Issuer:          token.Issuer,
		Subject:         token.Subject,
		Audience:        []string(token.Audience),
		Expiry:          time.Time(token.Expiry),
		IssuedAt:        time.Time(token.IssuedAt),
		Nonce:           token.Nonce,
		AccessTokenHash: token.AtHash,
	}
	return idToken, nil
}

// NewProvider uses the OpenID Connect discovery mechanism to construct a Provider.
//
// The issuer is the URL identifier for the service. For example: "https://accounts.google.com"
// or "https://login.salesforce.com".
func NewProvider(ctx context.Context, issuer, wellKnown string) (*oidc.Provider, error) {
	req, err := http.NewRequest(http.MethodGet, wellKnown, nil)
	if err != nil {
		return nil, err
	}
	resp, err := doRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, body)
	}

	var p providerJSON
	err = unmarshalResp(resp, body, &p)
	if err != nil {
		return nil, fmt.Errorf("oidc: failed to decode provider discovery object: %v", err)
	}

	if p.Issuer != issuer {
		return nil, fmt.Errorf("oidc: issuer did not match the issuer returned by provider, expected %q got %q", issuer, p.Issuer)
	}
	var algs []string
	for _, a := range p.Algorithms {
		if supportedAlgorithms[a] {
			algs = append(algs, a)
		}
	}

	provider := &oidc.Provider{}
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("issuer"), p.Issuer)
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("authURL"), p.AuthURL)
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("tokenURL"), p.TokenURL)
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("userInfoURL"), p.UserInfoURL)
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("algorithms"), algs)
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("rawClaims"), body)
	SetUnexportedField(reflect.ValueOf(provider).Elem().FieldByName("remoteKeySet"), oidc.NewRemoteKeySet(ctx, p.JWKSURL))

	return provider, nil
}

// List of algorithms explicitly supported by this go-oidc library.
// If a provider supports other algorithms, such as HS256 or none,
// those values won't be passed to the IDTokenVerifier.
var supportedAlgorithms = map[string]bool{
	oidc.RS256: true,
	oidc.RS384: true,
	oidc.RS512: true,
	oidc.ES256: true,
	oidc.ES384: true,
	oidc.ES512: true,
	oidc.PS256: true,
	oidc.PS384: true,
	oidc.PS512: true,
}

func doRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	client := http.DefaultClient
	if c, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok {
		client = c
	}
	return client.Do(req.WithContext(ctx))
}

type providerJSON struct {
	Issuer      string   `json:"issuer"`
	AuthURL     string   `json:"authorization_endpoint"`
	TokenURL    string   `json:"token_endpoint"`
	JWKSURL     string   `json:"jwks_uri"`
	UserInfoURL string   `json:"userinfo_endpoint"`
	Algorithms  []string `json:"id_token_signing_alg_values_supported"`
}

func unmarshalResp(r *http.Response, body []byte, v interface{}) error {
	err := json.Unmarshal(body, &v)
	if err == nil {
		return nil
	}
	ct := r.Header.Get("Content-Type")
	mediaType, _, parseErr := mime.ParseMediaType(ct)
	if parseErr == nil && mediaType == "application/json" {
		return fmt.Errorf("got Content-Type = application/json, but could not unmarshal as JSON: %v", err)
	}
	return fmt.Errorf("expected Content-Type = application/json, got %q: %v", ct, err)
}

func SetUnexportedField(field reflect.Value, value interface{}) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

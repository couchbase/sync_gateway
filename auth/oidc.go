//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Sync Gateway uses the github.com/coreos/go-oidc package for oidc support, which also depends on the
// gopkg.in/square/go-jose.v2/jwt package for verifying and validating ID Tokens.
// The go-oidc library doesn't support retrieval of OIDC provider configurations from non-standard locations.
// Support for this in Sync Gateway requires cloning some unexported functionality from go-oidc.
// The following methods should be reviewed for consistency when updating the commit of go-oidc being used:
//
// parseClaim: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/verify.go#L174
// VerifyClaims: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/verify.go#L208
// supportedAlgorithms: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/oidc.go#L97
// audience: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/oidc.go#L360
// audience.UnmarshalJSON: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/oidc.go#L362
// jsonTime: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/oidc.go#L376
// jsonTime.UnmarshalJSON: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/oidc.go#L378
// issuerGoogleAccounts: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/verify.go#L20
// issuerGoogleAccountsNoScheme: https://github.com/coreos/go-oidc/blob/8d771559cf6e5111c9b9159810d0e4538e7cdc82/verify.go#L21

package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc"
	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
	"golang.org/x/oauth2"
	"gopkg.in/square/go-jose.v2/jwt"
)

const (
	discoveryConfigPath = "/.well-known/openid-configuration"
)

var OIDCDiscoveryRetryWait = 500 * time.Millisecond

// Request parameter to specify the OpenID Connect provider to be used for authentication,
// from the list of providers defined in the Sync Gateway configuration.
var OIDCAuthProvider = "provider"

// Error code returned by failures to set parameters to URL query string.
var ErrSetURLQueryParam = errors.New("URL, parameter name and value must not be empty")

// Options for OpenID Connect
type OIDCOptions struct {
	Providers       OIDCProviderMap `json:"providers,omitempty"`        // List of OIDC issuers
	DefaultProvider *string         `json:"default_provider,omitempty"` // Issuer used when not specified by client
}

// OIDCClient represents client configurations to authenticate end-users
// with an OpenID Connect provider.
type OIDCClient struct {

	// Config describes a typical OAuth2 flow, with both the client
	// application information and the server's endpoint URLs.
	Config oauth2.Config

	// Verifier provides verification for ID Tokens.
	Verifier *oidc.IDTokenVerifier
}

type OIDCProvider struct {
	Issuer                  string   `json:"issuer"`                           // OIDC Issuer
	Register                bool     `json:"register"`                         // If true, server will register new user accounts
	ClientID                string   `json:"client_id,omitempty"`              // Client ID
	ValidationKey           *string  `json:"validation_key,omitempty"`         // Client secret
	CallbackURL             *string  `json:"callback_url,omitempty"`           // Sync Gateway redirect URL.  Needs to be specified to handle load balancer endpoints?  Or can we lazy load on first client use, based on request
	DisableSession          bool     `json:"disable_session,omitempty"`        // Disable Sync Gateway session creation on successful OIDC authentication
	Scope                   []string `json:"scope,omitempty"`                  // Scope sent for openid request
	IncludeAccessToken      bool     `json:"include_access,omitempty"`         // Whether the _oidc_callback response should include OP access token and associated fields (token_type, expires_in)
	UserPrefix              string   `json:"user_prefix,omitempty"`            // Username prefix for users created for this provider
	DiscoveryURI            string   `json:"discovery_url,omitempty"`          // Non-standard discovery endpoints
	DisableConfigValidation bool     `json:"disable_cfg_validation,omitempty"` // Bypasses config validation based on the OIDC spec.  Required for some OPs that don't strictly adhere to spec (eg. Yahoo)

	// DisableCallbackState determines whether or not to maintain state between the
	// Authentication (OAuth 2.0 Authorization) request and the callback. The default
	// value of DisableCallbackState is true, which means no state is maintained between
	// the request and the callback. It can be enabled through provider configuration
	// by setting property "disable_callback_state": false. Enabling callback state is
	// recommended to mitigate from Cross-Site Request Forgery (CSRF, XSRF).
	DisableCallbackState *bool `json:"disable_callback_state,omitempty"`

	// client represents client configurations to authenticate end-users
	// with an OpenID Connect provider. It must not be accessed directly,
	// use the accessor method GetClient() instead.
	client *OIDCClient

	// clientOnce synchronises access to the GetClient() and ensures that
	// the OpenID Connect client only gets initialized exactly once.
	clientOnce sync.Once

	// IsDefault indicates whether this OpenID Connect provider (the current
	// instance of OIDCProvider is explicitly specified as default provider
	// in providers configuration.
	IsDefault bool

	// Name represents the name of this OpenID Connect provider.
	Name string
}

type OIDCProviderMap map[string]*OIDCProvider

type OIDCCallbackURLFunc func(string, bool) string

func (opm OIDCProviderMap) GetDefaultProvider() *OIDCProvider {
	for _, provider := range opm {
		if provider.IsDefault {
			return provider
		}
	}
	return nil
}

// Returns the 'provider' and  a boolean value 'true' when there only a single
// provider is defined. A boolean 'true' indicates there is one and only provider
// and 'false' indicates multiple providers or no provider.
func (opm OIDCProviderMap) getProviderWhenSingle() (*OIDCProvider, bool) {
	if len(opm) == 1 {
		for _, value := range opm {
			return value, true
		}
	}
	return nil, false
}

func (opm OIDCProviderMap) GetProviderForIssuer(issuer string, audiences []string) *OIDCProvider {
	base.Debugf(base.KeyAuth, "GetProviderForIssuer with issuer: %v, audiences: %+v", base.UD(issuer), base.UD(audiences))
	for _, provider := range opm {
		if provider.Issuer == issuer && provider.ClientID != "" {
			// Iterate over the audiences looking for a match
			for _, aud := range audiences {
				if provider.ClientID == aud {
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
	op.clientOnce.Do(func() {
		var err error
		// If the redirect URL is not defined for the provider generate it from the
		// handler request and set it on the provider
		if op.CallbackURL == nil || *op.CallbackURL == "" {
			callbackURL := buildCallbackURLFunc(op.Name, op.IsDefault)
			if callbackURL != "" {
				op.CallbackURL = &callbackURL
			}
		}
		if err = op.InitOIDCClient(); err != nil {
			base.Errorf("Unable to initialize OIDC client: %v", err)
		}
	})

	return op.client
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

	verifier, endpoint, err := op.DiscoverConfig()
	if err != nil || verifier == nil {
		return pkgerrors.Wrap(err, "unable to discover config")
	}

	config := oauth2.Config{
		ClientID:    op.ClientID,
		Endpoint:    *endpoint,
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

	op.client = &OIDCClient{Config: config, Verifier: verifier}

	return nil
}

func (op *OIDCProvider) DiscoverConfig() (verifier *oidc.IDTokenVerifier, endpoint *oauth2.Endpoint, err error) {
	// If discovery URI is explicitly defined, use it instead of the standard issuer-based discovery.
	if op.DiscoveryURI != "" || op.DisableConfigValidation {
		discoveryURL := op.DiscoveryURI

		// If the end-user has opted out for config validation and the discovery URI is not defined
		// in provider config, construct the discovery URI based on standard issuer-based discovery.
		if op.DisableConfigValidation && discoveryURL == "" {
			discoveryURL = strings.TrimSuffix(op.Issuer, "/") + discoveryConfigPath
		}
		base.Infof(base.KeyAuth, "Fetching provider config from explicitly defined discovery endpoint: %s", base.UD(op.DiscoveryURI))
		metadata, err := op.FetchCustomProviderConfig(discoveryURL)
		if err != nil {
			return nil, nil, err
		}
		verifier = op.GenerateVerifier(metadata, context.Background())
		endpoint = &oauth2.Endpoint{AuthURL: metadata.AuthorizationEndpoint, TokenURL: metadata.TokenEndpoint}
	} else {
		base.Infof(base.KeyAuth, "Fetching provider config from standard issuer-based discovery endpoint, issuer: %s", base.UD(op.Issuer))
		var provider *oidc.Provider
		maxRetryAttempts := 5
		for i := 1; i <= maxRetryAttempts; i++ {
			provider, err = oidc.NewProvider(context.Background(), op.Issuer)
			if err == nil && provider != nil {
				providerEndpoint := provider.Endpoint()
				endpoint = &providerEndpoint
				verifier = provider.Verifier(&oidc.Config{ClientID: op.ClientID})
				break
			}
			base.Debugf(base.KeyAuth, "Unable to fetch provider config from discovery endpoint for %s (attempt %v/%v): %v", base.UD(op.Issuer), i, maxRetryAttempts, err)
			time.Sleep(OIDCDiscoveryRetryWait)
		}
	}

	return verifier, endpoint, err
}

func GetOIDCUsername(provider *OIDCProvider, subject string) string {
	return fmt.Sprintf("%s_%s", provider.UserPrefix, url.QueryEscape(subject))
}

func (op *OIDCProvider) FetchCustomProviderConfig(discoveryURL string) (*ProviderMetadata, error) {
	if discoveryURL == "" {
		return nil, errors.New("URL must not be empty for custom identity provider discovery")
	}

	base.Debugf(base.KeyAuth, "Fetching custom provider config from %s", base.UD(discoveryURL))
	req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
	if err != nil {
		base.Debugf(base.KeyAuth, "Error building new request for URL %s: %v", base.UD(discoveryURL), err)
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		base.Debugf(base.KeyAuth, "Error invoking calling discovery URL %s: %v", base.UD(discoveryURL), err)
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, resp.Body)
	}
	var metadata ProviderMetadata
	if err := base.JSONDecoder(resp.Body).Decode(&metadata); err != nil {
		base.Debugf(base.KeyAuth, "Error parsing body %s: %v", base.UD(discoveryURL), err)
		return nil, err
	}

	if metadata.Issuer != op.Issuer {
		return nil, fmt.Errorf("oidc: issuer did not match the issuer returned by provider, expected %q got %q", op.Issuer, metadata.Issuer)
	}

	base.Debugf(base.KeyAuth, "Returning config: %v", base.UD(metadata))
	return &metadata, nil
}

// GenerateVerifier returns a verifier manually constructed from a key set and issuer URL.
func (op *OIDCProvider) GenerateVerifier(metadata *ProviderMetadata, ctx context.Context) *oidc.IDTokenVerifier {
	signingAlgorithms := op.GetSigningAlgorithms(metadata)
	if len(signingAlgorithms.unsupportedAlgorithms) > 0 {
		base.Infof(base.KeyAuth, "Found algorithms not supported by underlying OpenID Connect library: %v", signingAlgorithms.unsupportedAlgorithms)
	}
	config := &oidc.Config{ClientID: op.ClientID}
	if len(signingAlgorithms.supportedAlgorithms) > 0 {
		config.SupportedSigningAlgs = signingAlgorithms.supportedAlgorithms
	}
	return oidc.NewVerifier(metadata.Issuer, oidc.NewRemoteKeySet(ctx, metadata.JwksUri), config)
}

// SigningAlgorithms contains the signing algorithms which are supported
// and not supported by the underlying github.com/coreos/go-oidc package.
type SigningAlgorithms struct {
	supportedAlgorithms   []string
	unsupportedAlgorithms []string
}

// GetSigningAlgorithms returns the list of supported and unsupported signing algorithms.
func (op *OIDCProvider) GetSigningAlgorithms(metadata *ProviderMetadata) (signingAlgorithms SigningAlgorithms) {
	for _, algorithm := range metadata.IdTokenSigningAlgValuesSupported {
		if supportedAlgorithms[algorithm] {
			signingAlgorithms.supportedAlgorithms = append(signingAlgorithms.supportedAlgorithms, algorithm)
		} else {
			signingAlgorithms.unsupportedAlgorithms = append(signingAlgorithms.unsupportedAlgorithms, algorithm)
		}
	}
	return signingAlgorithms
}

// supportedAlgorithms is list of signing algorithms explicitly supported
// by github.com/coreos/go-oidc package. If a provider supports other algorithms,
// such as HS256 or none, those values won't be passed to the IDTokenVerifier.
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

// ProviderMetadata describes the configuration of an OpenID Connect Provider.
//
// It doesn't contain all the values used by OpenID Connect, but holds the minimum and necessary configuration
// values to manually construct an ID Token verifier to support OpenID Connect authentication flows that requires
// retrieving provider's configuration information from custom endpoints/locations instead.
//
// See: https://openid.net/specs/openid-connect-discovery-1_0.html to get the complete provider's configuration.
type ProviderMetadata struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	JwksUri                           string   `json:"jwks_uri"`
	UserInfoEndpoint                  string   `json:"userinfo_endpoint"`
	IdTokenSigningAlgValuesSupported  []string `json:"id_token_signing_alg_values_supported"`
	ResponseTypesSupported            []string `json:"response_types_supported,omitempty"`
	SubjectTypesSupported             []string `json:"subject_types_supported,omitempty"`
	ScopesSupported                   []string `json:"scopes_supported,omitempty"`
	ClaimsSupported                   []string `json:"claims_supported,omitempty"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported,omitempty"`
}

// GetIssuerWithAudience returns "issuer" and "audiences" claims from the given JSON Web Token.
// Returns malformed oidc token error when issuer/audience doesn't exist in token.
func GetIssuerWithAudience(token *jwt.JSONWebToken) (issuer string, audiences []string, err error) {
	claims := &jwt.Claims{}
	err = token.UnsafeClaimsWithoutVerification(claims)
	if err != nil {
		return issuer, audiences, pkgerrors.Wrapf(err, "failed to parse JWT claims")
	}
	if claims.Issuer == "" {
		return issuer, audiences, fmt.Errorf("malformed oidc token %v, issuer claim doesn't exist", token)
	}
	if claims.Audience == nil || len(claims.Audience) == 0 {
		return issuer, audiences, fmt.Errorf("malformed oidc token %v, audience claim doesn't exist", token)
	}
	return claims.Issuer, claims.Audience, err
}

// VerifyJWT parses a raw ID Token, verifies it's been signed by the provider
// and returns the payload. It uses the ID Token Verifier to verify the token.
func (client *OIDCClient) VerifyJWT(token string) (*oidc.IDToken, error) {
	return client.Verifier.Verify(context.Background(), token)
}

func SetURLQueryParam(strURL, name, value string) (string, error) {
	if strURL == "" || name == "" || value == "" {
		return "", ErrSetURLQueryParam
	}
	uri, err := url.Parse(strURL)
	if err != nil {
		return "", err
	}
	rawQuery, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return "", err
	}
	rawQuery.Set(name, value)
	uri.RawQuery = rawQuery.Encode()
	return uri.String(), nil
}

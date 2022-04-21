//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
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
	// OIDCDiscoveryConfigPath represents a predefined string value to be used to construct
	// the well-known endpoint. The well-known endpoint can always be reached by adding this
	// string value to the end of OP's issuer URI.
	OIDCDiscoveryConfigPath = "/.well-known/openid-configuration"

	// Request parameter to specify the OpenID Connect provider to be used for authentication,
	// from the list of providers defined in the Sync Gateway configuration.
	OIDCAuthProvider = "provider"

	// MaxProviderConfigSyncInterval is used as the duration between now and next provider
	// metadata sync time when the metadata expiry is zero or half of the duration between
	// provider metadata expiry and now is grater than 24 hours.
	MaxProviderConfigSyncInterval = 24 * time.Hour

	// MinProviderConfigSyncInterval is used as the duration between now and next provider
	// metadata sync time when half of the duration between provider metadata expiry and now is
	// grater than 1 minute.
	MinProviderConfigSyncInterval = time.Minute
)

// OIDCDiscoveryRetryWait represents the wait time between provider discovery retries.
// SG periodically retries a failed provider discovery request with a 500 milliseconds
// delay between requests upto max retries of 5 times and back-off after that.
var OIDCDiscoveryRetryWait = 500 * time.Millisecond

var (
	// Error code returned by failures to set parameters to URL query string.
	ErrSetURLQueryParam = errors.New("URL, parameter name and value must not be empty")

	// ErrEmptyDiscoveryURL error is returned if the discovery URL is empty during non-standard discovery.
	ErrEmptyDiscoveryURL = errors.New("URL must not be empty for custom identity provider discovery")
)

const (
	// Error message returned by failures to initialize OIDCClient due to nil provider reference.
	ErrMsgNilProvider = "nil provider"

	// Error message returned by failures to initialize OIDCClient due provider discovery error.
	ErrMsgUnableToDiscoverConfig = "unable to discover config"

	// Error message returned by failures to initialize OIDCClient due error generating ID token verifier.
	ErrMsgUnableToGenerateVerifier = "unable to generate id token verifier"
)

// Options for OpenID Connect
type OIDCOptions struct {
	Providers       OIDCProviderMap `json:"providers,omitempty"`        // List of OIDC issuers
	DefaultProvider *string         `json:"default_provider,omitempty"` // Issuer used when not specified by client
}

// OIDCClient represents client configurations to authenticate end-users
// with an OpenID Connect provider.
type OIDCClient struct {

	// mutex prevents the ID token verifier and OAuth2 configurations from resetting
	// while there is an active ID token verification. It is held for read during ID
	// token verification and exclusively during provider metadata discovery sync.
	mutex sync.RWMutex

	// Config describes a typical OAuth2 flow, with both the client
	// application information and the server's endpoint URLs.
	config *oauth2.Config

	// verifier provides verification for ID Tokens.
	verifier *oidc.IDTokenVerifier
}

// Config returns the OAuth2 configurations.
func (client *OIDCClient) Config() *oauth2.Config {
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	return client.config
}

// SetConfig sets the ID Token verifier and token endpoint URLs on OIDCClient
func (client *OIDCClient) SetConfig(verifier *oidc.IDTokenVerifier, endpoint oauth2.Endpoint) {
	client.mutex.Lock()
	client.verifier = verifier
	client.config = &oauth2.Config{
		ClientID:     client.config.ClientID,
		ClientSecret: client.config.ClientSecret,
		Endpoint:     endpoint,
		RedirectURL:  client.config.RedirectURL,
		Scopes:       client.config.Scopes,
	}
	client.mutex.Unlock()
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

	// DisableCallbackState determines whether or not to maintain state between the "/_oidc" and
	// "/_oidc_callback" endpoints. The default value of DisableCallbackState is false, which means
	// state is maintained between auth request and the callback. It can be disabled through provider
	// configuration by setting property "disable_callback_state": true. Disabling callback state is
	// vulnerable to Cross-Site Request Forgery (CSRF, XSRF) and NOT recommended.
	DisableCallbackState bool `json:"disable_callback_state,omitempty"`

	// UsernameClaim allows to specify a claim other than subject to use as the Sync Gateway username.
	// The specified claim must be a string - numeric claims may be unmarshalled inconsistently between
	// Sync Gateway and the underlying OIDC library.
	UsernameClaim string `json:"username_claim"`

	// AllowUnsignedProviderTokens allows users to opt-in to accepting unsigned tokens from providers.
	AllowUnsignedProviderTokens bool `json:"allow_unsigned_provider_tokens"`

	// client represents client configurations to authenticate end-users
	// with an OpenID Connect provider. It must not be accessed directly,
	// use the accessor method GetClient() instead.
	client *OIDCClient

	// clientInitLock synchronises access to the GetClient() and ensures that
	// the OpenID Connect client only gets initialized exactly once when
	// the client has been successfully initialized.
	clientInitLock sync.Mutex

	// clientInit tracks whether the client has been successfully initialized or not
	clientInit base.AtomicBool

	// IsDefault indicates whether this OpenID Connect provider (the current
	// instance of OIDCProvider is explicitly specified as default provider
	// in providers configuration.
	IsDefault bool

	// Name represents the name of this OpenID Connect provider.
	Name string

	// terminator ensures termination of async goroutines for provider
	// metadata sync. Closed during DatabaseContext.close().
	terminator chan struct{}

	// metadata describes the configuration of an OpenID Connect Provider.
	metadata ProviderMetadata

	// InsecureSkipVerify determines whether the TLS certificate verification
	// should be disabled for this provider. TLS certificate verification is
	// enabled by default.
	InsecureSkipVerify bool
}

type OIDCProviderMap map[string]*OIDCProvider

type OIDCCallbackURLFunc func(string, bool) string

// GetDefaultProvider returns the default OpenID Connect provider.
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

func (opm OIDCProviderMap) GetProviderForIssuer(ctx context.Context, issuer string, audiences []string) *OIDCProvider {
	base.DebugfCtx(ctx, base.KeyAuth, "GetProviderForIssuer with issuer: %v, audiences: %+v", base.UD(issuer), base.UD(audiences))
	for _, provider := range opm {
		if provider.Issuer == issuer && provider.ClientID != "" {
			// Iterate over the audiences looking for a match
			for _, aud := range audiences {
				if provider.ClientID == aud {
					base.DebugfCtx(ctx, base.KeyAuth, "Provider matches, returning")
					return provider
				}
			}
		}
	}
	base.DebugfCtx(ctx, base.KeyAuth, "No provider match found")
	return nil
}

// Stop stops all the provider discovery sync task by terminating the async goroutines.
func (opm OIDCProviderMap) Stop() {
	for _, provider := range opm {
		provider.stopDiscoverySync()
	}
}

// GetClient initializes the client on first successful request.
func (op *OIDCProvider) GetClient(ctx context.Context, buildCallbackURLFunc OIDCCallbackURLFunc) (*OIDCClient, error) {
	// If the callback URL isn't defined for the provider, uses buildCallbackURLFunc to construct (based on current request)

	if op.clientInit.IsTrue() {
		return op.client, nil
	}
	op.clientInitLock.Lock()
	defer op.clientInitLock.Unlock()

	// Check again to see if the previous lock holder initialized the client
	if op.clientInit.IsTrue() {
		return op.client, nil
	}

	// If the redirect URL is not defined for the provider generate it from the
	// handler request and set it on the provider
	if op.CallbackURL == nil || *op.CallbackURL == "" {
		callbackURL := buildCallbackURLFunc(op.Name, op.IsDefault)
		if callbackURL != "" {
			op.CallbackURL = &callbackURL
		}
	}
	if err := op.initOIDCClient(ctx); err != nil {
		return nil, err
	}
	op.clientInit.Set(true)
	return op.client, nil
}

// To support multiple providers referencing the same issuer, the user prefix used to build the SG usernames for
// a provider is based on the issuer
func (op *OIDCProvider) InitUserPrefix(ctx context.Context) error {

	// If the user prefix has been explicitly defined, skip calculation
	if op.UserPrefix != "" || op.UsernameClaim != "" {
		return nil
	}

	issuerURL, err := url.ParseRequestURI(op.Issuer)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to parse issuer URI when initializing user prefix - using provider name")
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

// initOIDCClient initializes the OpenID Connect client with the specified configurations by performing
// an initial provider metadata discovery and initiates the discovery sync in the background.
func (op *OIDCProvider) initOIDCClient(ctx context.Context) error {
	if op == nil {
		return fmt.Errorf(ErrMsgNilProvider)
	}

	if op.Issuer == "" {
		return base.RedactErrorf("Issuer not defined for OpenID Connect provider %+v", base.UD(op))
	}

	config := oauth2.Config{
		ClientID:    op.ClientID,
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
	if err := op.InitUserPrefix(ctx); err != nil {
		return err
	}

	metadata, verifier, err := op.DiscoverConfig(ctx)
	if err != nil {
		return pkgerrors.Wrap(err, ErrMsgUnableToDiscoverConfig)
	}

	verifier = op.generateVerifier(&metadata, GetOIDCClientContext(op.InsecureSkipVerify))
	if verifier == nil {
		return pkgerrors.Wrap(err, ErrMsgUnableToGenerateVerifier)
	}

	op.client = &OIDCClient{config: &config}
	op.client.SetConfig(verifier, metadata.endpoint())
	op.metadata = metadata

	discoveryURL := op.getDiscoveryEndpoint()
	if err := op.startDiscoverySync(ctx, discoveryURL); err != nil {
		base.ErrorfCtx(ctx, "failed to start OpenID Connect discovery sync: %v", err)
	}

	return nil
}

// isStandardDiscovery returns true if both discovery_url and disable_cfg_validation
// options are not defined in provider configuration and false otherwise.
func (op *OIDCProvider) isStandardDiscovery() bool {
	if op.DiscoveryURI != "" || op.DisableConfigValidation {
		return false
	}
	return true
}

// getDiscoveryEndpoint returns the OpenID Connect provider metadata discovery endpoint.
// If the end-user has opted out for config validation and the discovery URI is not defined
// in provider config, constructs the discovery URI based on standard issuer-based discovery.
func (op *OIDCProvider) getDiscoveryEndpoint() string {
	if !op.isStandardDiscovery() {
		if op.DisableConfigValidation && op.DiscoveryURI == "" {
			return GetStandardDiscoveryEndpoint(op.Issuer)
		}
		return op.DiscoveryURI
	}
	return GetStandardDiscoveryEndpoint(op.Issuer)
}

// GetStandardDiscoveryEndpoint returns the standard issuer based provider metadata discovery
// endpoint by concatenating the string /.well-known/openid-configuration to the issuer.
func GetStandardDiscoveryEndpoint(issuer string) string {
	return strings.TrimSuffix(issuer, "/") + OIDCDiscoveryConfigPath
}

// DiscoverConfig initiates the initial metadata provider discovery while initializing the OpenID Connect client.
func (op *OIDCProvider) DiscoverConfig(ctx context.Context) (metadata ProviderMetadata, verifier *oidc.IDTokenVerifier, err error) {
	discoveryURL := op.getDiscoveryEndpoint()
	if !op.isStandardDiscovery() {
		base.InfofCtx(ctx, base.KeyAuth, "Fetching provider config from explicitly defined discovery endpoint: %s", base.UD(discoveryURL))
		metadata, _, _, err = op.fetchCustomProviderConfig(ctx, discoveryURL)
		if err != nil {
			return ProviderMetadata{}, nil, err
		}
		verifier = op.generateVerifier(&metadata, GetOIDCClientContext(op.InsecureSkipVerify))
	} else {
		metadata, verifier, err = op.standardDiscovery(ctx, discoveryURL)
	}
	return metadata, verifier, err
}

// standardDiscovery uses the standard OIDC discovery endpoint (issuer + /.well-known/openid-configuration) to
// look up the OIDC provider configuration and returns provider metadata and ID token verifier. Provided discoveryURL
// must not be empty.
func (op *OIDCProvider) standardDiscovery(ctx context.Context, discoveryURL string) (metadata ProviderMetadata, verifier *oidc.IDTokenVerifier, err error) {
	base.InfofCtx(ctx, base.KeyAuth, "Fetching provider config from standard issuer-based discovery endpoint: %s", base.UD(discoveryURL))
	var provider *oidc.Provider
	maxRetryAttempts := 5
	for i := 1; i <= maxRetryAttempts; i++ {
		provider, err = oidc.NewProvider(GetOIDCClientContext(op.InsecureSkipVerify), op.Issuer)
		if err == nil && provider != nil {
			verifier = provider.Verifier(&oidc.Config{ClientID: op.ClientID})
			if err = provider.Claims(&metadata); err != nil {
				base.ErrorfCtx(ctx, "Error caching metadata from standard issuer-based discovery endpoint: %s", base.UD(discoveryURL))
			}
			break
		}
		base.DebugfCtx(ctx, base.KeyAuth, "Unable to fetch provider config from discovery endpoint for %s (attempt %v/%v): %v", base.UD(op.Issuer), i, maxRetryAttempts, err)
		time.Sleep(OIDCDiscoveryRetryWait)
	}
	return metadata, verifier, err
}

// getOIDCUsername returns the username to be used as the Sync Gateway username.
func getOIDCUsername(provider *OIDCProvider, identity *Identity) (username string, err error) {
	if provider.UsernameClaim != "" {
		value, ok := identity.Claims[provider.UsernameClaim]
		if !ok {
			return "", fmt.Errorf("oidc: specified claim %q not found in id_token, identity: %v", provider.UsernameClaim, identity)
		}
		if username, err = formatUsername(value); err != nil {
			return "", err
		}
		if provider.UserPrefix == "" {
			return url.QueryEscape(username), nil
		}
		return fmt.Sprintf("%s_%s", provider.UserPrefix, url.QueryEscape(username)), nil
	}
	return fmt.Sprintf("%s_%s", provider.UserPrefix, url.QueryEscape(identity.Subject)), nil
}

// formatUsername returns the string representation of the given username value.
func formatUsername(value interface{}) (string, error) {
	switch valueType := value.(type) {
	case string:
		return valueType, nil
	case json.Number:
		return valueType.String(), nil
	case float64:
		return strconv.FormatFloat(valueType, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("oidc: can't treat value of type: %T as valid username", valueType)
	}
}

// fetchCustomProviderConfig collects the provider configuration from the given discovery endpoint and determines
// whether the cached configuration needs to be refreshed by comparing the new metadata with the cached value
func (op *OIDCProvider) fetchCustomProviderConfig(ctx context.Context, discoveryURL string) (metadata ProviderMetadata, ttl time.Duration, refresh bool, err error) {
	if discoveryURL == "" {
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, ErrEmptyDiscoveryURL
	}
	base.DebugfCtx(ctx, base.KeyAuth, "Fetching custom provider config from %s", base.UD(discoveryURL))
	req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
	if err != nil {
		base.DebugfCtx(ctx, base.KeyAuth, "Error building new request for URL %s: %v", base.UD(discoveryURL), err)
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, err
	}
	client := base.GetHttpClient(op.InsecureSkipVerify)
	resp, err := client.Do(req)
	if err != nil {
		base.DebugfCtx(ctx, base.KeyAuth, "Error invoking calling discovery URL %s: %v", base.UD(discoveryURL), err)
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, err
	}

	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("unsuccessful response and could not read returned response body: %w", err)
		} else {
			err = fmt.Errorf("unsuccessful response: %v", body)
		}
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, err
	}

	ttl, _, err = cacheable(resp.Header)
	if err != nil {
		base.InfofCtx(ctx, base.KeyAuth, "Failed to determine whether provider metadata can be cached, error: %v", err)
	}

	// If the metadata expiry is zero or greater than 24 hours, the next sync should start in 24 hours.
	if ttl == 0 || ttl > MaxProviderConfigSyncInterval {
		ttl = MaxProviderConfigSyncInterval
	}
	// If the expiry is less than 1 minute, the next sync should start in the next minute.
	if ttl < MinProviderConfigSyncInterval {
		ttl = MinProviderConfigSyncInterval
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, err
	}

	if err := base.JSONUnmarshal(bodyBytes, &metadata); err != nil {
		base.DebugfCtx(ctx, base.KeyAuth, "Error parsing body during discovery sync: %v", err)
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, err
	}

	if reflect.DeepEqual(op.metadata, metadata) {
		base.DebugfCtx(ctx, base.KeyAuth, "No change in discovery config detected at this time, next sync will be after %v", ttl)
		return metadata, ttl, false, nil
	}

	if metadata.Issuer != op.Issuer {
		return ProviderMetadata{}, MaxProviderConfigSyncInterval, false, fmt.Errorf("oidc: issuer did not match the issuer returned by provider, expected %q got %q", op.Issuer, metadata.Issuer)
	}

	base.DebugfCtx(ctx, base.KeyAuth, "Returning config: %v", base.UD(metadata))
	return metadata, ttl, true, nil
}

// runDiscoverySync runs the discovery sync by fetching the provider metadata by calling
// fetchCustomProviderConfig. If fetchCustomProviderConfig detects any change in provider
// config, it sets the verifier, OAuth endpoints and returns the time interval between now
// and next discovery sync.
func (op *OIDCProvider) runDiscoverySync(ctx context.Context, discoveryURL string) (time.Duration, error) {
	metadata, ttl, refresh, err := op.fetchCustomProviderConfig(ctx, discoveryURL)
	if !refresh || err != nil {
		return ttl, err
	}
	if refresh && !op.isStandardDiscovery() {
		verifier := op.generateVerifier(&metadata, context.Background())
		op.client.SetConfig(verifier, metadata.endpoint())
		op.metadata = metadata
	}
	if refresh && op.isStandardDiscovery() {
		metadata, verifier, err := op.standardDiscovery(ctx, discoveryURL)
		if err != nil || verifier == nil {
			return 0, pkgerrors.Wrap(err, ErrMsgUnableToDiscoverConfig)
		}
		op.client.SetConfig(verifier, metadata.endpoint())
		op.metadata = metadata
	}
	return ttl, nil
}

// generateVerifier returns a verifier manually constructed from a key set and issuer URL.
func (op *OIDCProvider) generateVerifier(metadata *ProviderMetadata, ctx context.Context) *oidc.IDTokenVerifier {
	signingAlgorithms := op.getSigningAlgorithms(metadata)
	if len(signingAlgorithms.unsupportedAlgorithms) > 0 {
		base.InfofCtx(ctx, base.KeyAuth, "Found algorithms not supported by underlying OpenID Connect library: %v", signingAlgorithms.unsupportedAlgorithms)
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

// getSigningAlgorithms returns the list of supported and unsupported signing algorithms.
func (op *OIDCProvider) getSigningAlgorithms(metadata *ProviderMetadata) (signingAlgorithms SigningAlgorithms) {
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

// endpoint returns the OAuth2 auth and token endpoints for the given provider.
func (metadata *ProviderMetadata) endpoint() oauth2.Endpoint {
	return oauth2.Endpoint{
		AuthURL:  metadata.AuthorizationEndpoint,
		TokenURL: metadata.TokenEndpoint,
	}
}

// getIssuerWithAudience returns "issuer" and "audiences" claims from the given JSON Web Token.
// Returns malformed oidc token error when issuer/audience doesn't exist in token.
func getIssuerWithAudience(token *jwt.JSONWebToken) (issuer string, audiences []string, err error) {
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

// verifyJWT parses a raw ID Token, verifies it's been signed by the provider
// and returns the payload. It uses the ID Token Verifier to verify the token.
func (client *OIDCClient) verifyJWT(token string) (*oidc.IDToken, error) {
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	return client.verifier.Verify(context.Background(), token)
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

// startDiscoverySync starts the provider metadata discovery sync task in background.
func (op *OIDCProvider) startDiscoverySync(ctx context.Context, discoveryURL string) error {
	op.terminator = make(chan struct{})
	duration, err := op.runDiscoverySync(ctx, discoveryURL)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-time.After(duration):
				duration, err = op.runDiscoverySync(ctx, discoveryURL)
				if err != nil {
					base.WarnfCtx(ctx, "OpenID Connect provider discovery sync ends up in error: %v, next retry in %v", err, duration)
				}
			case <-op.terminator:
				base.DebugfCtx(ctx, base.KeyAll, "Terminating OpenID Connect provider discovery sync")
				return
			}
		}
	}()
	return nil
}

// stopDiscoverySync stops the currently running metadata discovery sync of this provider.
func (op *OIDCProvider) stopDiscoverySync() {
	if op.terminator != nil {
		close(op.terminator)
	}
}

// Returns the value of max-age directive from the Cache-Control HTTP header, i.e. the maximum
// amount of time in seconds that the fetched responses are allowed to be used again (from the
// time when a request is made). The second value (ok) is true if max-age exists, and false if not.
func cacheControlMaxAge(header http.Header) (maxAge time.Duration, ok bool, err error) {
	for _, field := range strings.Split(header.Get("Cache-Control"), ",") {
		parts := strings.SplitN(strings.TrimSpace(field), "=", 2)
		k := strings.ToLower(strings.TrimSpace(parts[0]))
		if k != "max-age" {
			continue
		}
		if len(parts) == 1 {
			return 0, false, errors.New("max-age has no value")
		}
		v := strings.TrimSpace(parts[1])
		if v == "" {
			return 0, false, errors.New("max-age has empty value")
		}
		age, err := strconv.Atoi(v)
		if err != nil {
			return 0, false, err
		}
		if age <= 0 {
			return 0, false, nil
		}
		return time.Duration(age) * time.Second, true, nil
	}
	return 0, false, nil
}

// getExpiration calculates the freshness lifetime by computing the number of seconds difference
// between the Expires value and the Date value on the response header. The second value (ok) is
// true if the calculated freshness lifetime is greater than zero and false if not. The getExpiration
// is used when the response doesn't contain a Cache-Control: max-age header.
func getExpiration(header http.Header) (ttl time.Duration, ok bool, err error) {
	date := header.Get("Date")
	if date == "" {
		return 0, false, nil
	}
	expires := header.Get("Expires")
	if expires == "" || expires == "0" {
		return 0, false, nil
	}
	te, err := time.Parse(time.RFC1123, expires)
	if err != nil {
		return 0, false, err
	}
	td, err := time.Parse(time.RFC1123, date)
	if err != nil {
		return 0, false, err
	}
	ttl = te.Sub(td)
	if ttl <= 0 {
		return 0, false, nil
	}
	return ttl, true, nil
}

// cacheable determines whether the provider discovery response can be cached.
// Returns the invalidate interval with a value true when the response is cacheable
// and zero interval with a value false otherwise.
func cacheable(header http.Header) (ttl time.Duration, ok bool, err error) {
	ttl, ok, err = cacheControlMaxAge(header)
	if err != nil || ok {
		return ttl, ok, err
	}
	return getExpiration(header)
}

// GetOIDCClientContext returns a new Context that carries the provided HTTP client
// with TLS certificate verification disabled when insecureSkipVerify is true and
// enabled otherwise.
func GetOIDCClientContext(insecureSkipVerify bool) context.Context {
	client := base.GetHttpClient(insecureSkipVerify)
	return oidc.ClientContext(context.Background(), client)
}

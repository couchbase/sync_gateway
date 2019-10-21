package auth

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Check parsing valid URL. It should return URL with no error.
func TestParseURIWithValidURL(t *testing.T) {
	const (
		httpPort    = "8086"
		httpsPort   = "433"
		httpScheme  = "http"
		httpsScheme = "https"
		hostname    = "accounts.couchbase.com"
	)
	httpURL := fmt.Sprintf("%s://%s:%s", httpScheme, hostname, httpPort)
	httpsURL := fmt.Sprintf("%s://%s:%s", httpsScheme, hostname, httpsPort)
	httpHost := fmt.Sprintf("%s:%s", hostname, httpPort)
	httpsHost := fmt.Sprintf("%s:%s", hostname, httpsPort)

	config := &OidcProviderConfiguration{}

	url, err := config.parseURI(httpURL)
	assert.Nil(t, err)
	assert.Equal(t, httpURL, url.String())
	assert.Equal(t, httpScheme, url.Scheme)
	assert.Equal(t, httpHost, url.Host)
	assert.Equal(t, hostname, url.Hostname())
	assert.Equal(t, httpPort, url.Port())

	url, err = config.parseURI(httpsURL)
	assert.Nil(t, err)
	assert.Equal(t, httpsURL, url.String())
	assert.Equal(t, httpsScheme, url.Scheme)
	assert.Equal(t, httpsHost, url.Host)
	assert.Equal(t, hostname, url.Hostname())
	assert.Equal(t, httpsPort, url.Port())
}

func TestParseURIWithBadURIs(t *testing.T) {
	config := &OidcProviderConfiguration{}
	tests := []struct {
		input string
		text  string
		want  interface{}
	}{
		// Simulate Error parsing URI use case; Unicode U+237E (⍾), "graphic for bell"
		{input: "⍾://accounts.unknown.com/", text: "Error parsing URI", want: nil},
		// HTTP URL without hostname
		{input: "http://", text: "Host required in URI", want: nil},
		// HTTPS URL without hostname
		{input: "https://", text: "Host required in URI", want: nil},
		// HTTPS URL without a valid scheme.
		{input: "sftp://accounts.unknown.com", text: "Invalid URI scheme", want: nil},
		// Blank or empty URL
		{input: "", text: "", want: nil},
	}

	for _, tc := range tests {
		got, err := config.parseURI(tc.input)
		if tc.input == "" {
			assert.Nil(t, got)
			assert.Nil(t, err)
			continue
		}
		if !assert.Error(t, err) || (assert.Error(t, err) && !assert.Contains(t, err.Error(), tc.text)) {
			t.Fatalf("expected: %v, got: %v", tc.want, got)
		}
	}
}

// Constants for checking provider configuration method
const (
	goodIssuer               = "https://accounts.google.com"
	badIssuer                = "sftp://accounts.google.com"
	goodAuthEndpoint         = "https://accounts.google.com/o/oauth2/v2/auth"
	badAuthEndpoint          = "sftp://accounts.google.com/o/oauth2/v2/auth"
	goodTokenEndpoint        = "https://oauth2.googleapis.com/token"
	badTokenEndpoint         = "sftp://oauth2.googleapis.com/token"
	goodUserInfoEndpoint     = "https://openidconnect.googleapis.com/v1/userinfo"
	badUserInfoEndpoint      = "sftp://openidconnect.googleapis.com/v1/userinfo"
	goodJwksUri              = "https://www.googleapis.com/oauth2/v3/certs"
	badJwksUri               = "sftp://www.googleapis.com/oauth2/v3/certs"
	goodRegistrationEndpoint = "https://accounts.google.com/clients/"
	badRegistrationEndpoint  = "sftp://accounts.google.com/clients/"
	goodPolicy               = "https://accounts.google.com/policy/"
	badPolicy                = "sftp://accounts.google.com/policy/"
	goodTermsOfService       = "https://accounts.google.com/termsofservice/"
	badTermsOfService        = "sftp://accounts.google.com/termsofservice/"
	goodServiceDocs          = "https://accounts.google.com/servicedocs/"
	badServiceDocs           = "sftp://accounts.google.com/servicedocs/"
)

func TestAsProviderConfigWithBadIssuer(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer: badIssuer,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
}

func TestAsProviderConfigWithBadAuthEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:       goodIssuer,
		AuthEndpoint: badAuthEndpoint,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
}

func TestAsProviderConfigWithBadTokenEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:        goodIssuer,
		AuthEndpoint:  goodAuthEndpoint,
		TokenEndpoint: badTokenEndpoint,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
}

func TestAsProviderConfigWithBadUserInfoEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:           goodIssuer,
		AuthEndpoint:     goodAuthEndpoint,
		TokenEndpoint:    goodTokenEndpoint,
		UserInfoEndpoint: badUserInfoEndpoint,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
	assert.Nil(t, pc.UserInfoEndpoint)
}

func TestAsProviderConfigWithBadJwksUri(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:           goodIssuer,
		AuthEndpoint:     goodAuthEndpoint,
		TokenEndpoint:    goodTokenEndpoint,
		UserInfoEndpoint: goodUserInfoEndpoint,
		JwksUri:          badJwksUri,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
	assert.Nil(t, pc.UserInfoEndpoint)
	assert.Nil(t, pc.KeysEndpoint)
}

func TestAsProviderConfigWithBadRegistrationEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:               goodIssuer,
		AuthEndpoint:         goodAuthEndpoint,
		TokenEndpoint:        goodTokenEndpoint,
		UserInfoEndpoint:     goodUserInfoEndpoint,
		JwksUri:              goodJwksUri,
		RegistrationEndpoint: badRegistrationEndpoint,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
	assert.Nil(t, pc.UserInfoEndpoint)
	assert.Nil(t, pc.KeysEndpoint)
	assert.Nil(t, pc.RegistrationEndpoint)
}

func TestAsProviderConfigWithBadPolicy(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:               goodIssuer,
		AuthEndpoint:         goodAuthEndpoint,
		TokenEndpoint:        goodTokenEndpoint,
		UserInfoEndpoint:     goodUserInfoEndpoint,
		JwksUri:              goodJwksUri,
		RegistrationEndpoint: goodRegistrationEndpoint,
		Policy:               badPolicy,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
	assert.Nil(t, pc.UserInfoEndpoint)
	assert.Nil(t, pc.KeysEndpoint)
	assert.Nil(t, pc.RegistrationEndpoint)
	assert.Nil(t, pc.Policy)
}

func TestAsProviderConfigWithBadTermsOfService(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:               goodIssuer,
		AuthEndpoint:         goodAuthEndpoint,
		TokenEndpoint:        goodTokenEndpoint,
		UserInfoEndpoint:     goodUserInfoEndpoint,
		JwksUri:              goodJwksUri,
		RegistrationEndpoint: goodRegistrationEndpoint,
		Policy:               goodPolicy,
		TermsOfService:       badTermsOfService,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
	assert.Nil(t, pc.UserInfoEndpoint)
	assert.Nil(t, pc.KeysEndpoint)
	assert.Nil(t, pc.RegistrationEndpoint)
	assert.Nil(t, pc.Policy)
	assert.Nil(t, pc.TermsOfService)
}

func TestAsProviderConfigWithBadServiceDocs(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:               goodIssuer,
		AuthEndpoint:         goodAuthEndpoint,
		TokenEndpoint:        goodTokenEndpoint,
		UserInfoEndpoint:     goodUserInfoEndpoint,
		JwksUri:              goodJwksUri,
		RegistrationEndpoint: goodRegistrationEndpoint,
		Policy:               goodPolicy,
		TermsOfService:       goodTermsOfService,
		ServiceDocs:          badServiceDocs,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
	assert.Nil(t, pc.UserInfoEndpoint)
	assert.Nil(t, pc.KeysEndpoint)
	assert.Nil(t, pc.RegistrationEndpoint)
	assert.Nil(t, pc.Policy)
	assert.Nil(t, pc.TermsOfService)
	assert.Nil(t, pc.ServiceDocs)
}

func TestAsProviderConfig(t *testing.T) {
	const (
		httpsScheme              = "https"
		accountsGoogle           = "accounts.google.com"
		apisGoogle               = "www.googleapis.com"
		openIdConnectGoogle      = "openidconnect.googleapis.com"
		oauth2ApisGoogle         = "oauth2.googleapis.com"
		oauth2AuthAPath          = "/o/oauth2/v2/auth"
		serviceDocsPath          = "/servicedocs/"
		termsOfServicePath       = "/termsofservice/"
		policyPath               = "/policy/"
		registrationEndpointPath = "/clients/"
		keysEndpointPath         = "/oauth2/v3/certs"
		userInfoEndpointPath     = "/v1/userinfo"
		tokenEndpointPath        = "/token"
	)
	opc := &OidcProviderConfiguration{
		Issuer:               goodIssuer,
		AuthEndpoint:         goodAuthEndpoint,
		TokenEndpoint:        goodTokenEndpoint,
		UserInfoEndpoint:     goodUserInfoEndpoint,
		JwksUri:              goodJwksUri,
		RegistrationEndpoint: goodRegistrationEndpoint,
		Policy:               goodPolicy,
		TermsOfService:       goodTermsOfService,
		ServiceDocs:          goodServiceDocs,
	}
	// Make provider configuration from Open ID Connect provider configuration.
	pc, err := opc.AsProviderConfig()
	assert.NoError(t, err)

	// Check Issuer details
	assert.Equal(t, httpsScheme, pc.Issuer.Scheme)
	assert.Empty(t, pc.Issuer.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.Issuer.User)
	assert.Equal(t, accountsGoogle, pc.Issuer.Host)
	assert.Empty(t, pc.Issuer.Path)
	assert.Empty(t, pc.Issuer.RawPath)
	assert.False(t, pc.Issuer.ForceQuery)
	assert.Empty(t, pc.Issuer.RawQuery)
	assert.Empty(t, pc.Issuer.Fragment)

	// Check AuthEndpoint details
	assert.Equal(t, httpsScheme, pc.AuthEndpoint.Scheme)
	assert.Empty(t, pc.AuthEndpoint.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.AuthEndpoint.User)
	assert.Equal(t, accountsGoogle, pc.AuthEndpoint.Host)
	assert.Equal(t, oauth2AuthAPath, pc.AuthEndpoint.Path)
	assert.Empty(t, pc.AuthEndpoint.RawPath)
	assert.False(t, pc.AuthEndpoint.ForceQuery)
	assert.Empty(t, pc.AuthEndpoint.RawQuery)
	assert.Empty(t, pc.AuthEndpoint.Fragment)

	// Check TokenEndpoint details
	assert.Equal(t, httpsScheme, pc.TokenEndpoint.Scheme)
	assert.Empty(t, pc.TokenEndpoint.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.TokenEndpoint.User)
	assert.Equal(t, oauth2ApisGoogle, pc.TokenEndpoint.Host)
	assert.Equal(t, tokenEndpointPath, pc.TokenEndpoint.Path)
	assert.Empty(t, pc.TokenEndpoint.RawPath)
	assert.False(t, pc.TokenEndpoint.ForceQuery)
	assert.Empty(t, pc.TokenEndpoint.RawQuery)
	assert.Empty(t, pc.TokenEndpoint.Fragment)

	// Check UserInfoEndpoint details
	assert.Equal(t, httpsScheme, pc.UserInfoEndpoint.Scheme)
	assert.Empty(t, pc.UserInfoEndpoint.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.UserInfoEndpoint.User)
	assert.Equal(t, openIdConnectGoogle, pc.UserInfoEndpoint.Host)
	assert.Equal(t, userInfoEndpointPath, pc.UserInfoEndpoint.Path)
	assert.Empty(t, pc.UserInfoEndpoint.RawPath)
	assert.False(t, pc.UserInfoEndpoint.ForceQuery)
	assert.Empty(t, pc.UserInfoEndpoint.RawQuery)
	assert.Empty(t, pc.UserInfoEndpoint.Fragment)

	// Check KeysEndpoint details
	assert.Equal(t, httpsScheme, pc.KeysEndpoint.Scheme)
	assert.Empty(t, pc.KeysEndpoint.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.KeysEndpoint.User)
	assert.Equal(t, apisGoogle, pc.KeysEndpoint.Host)
	assert.Equal(t, keysEndpointPath, pc.KeysEndpoint.Path)
	assert.Empty(t, pc.KeysEndpoint.RawPath)
	assert.False(t, pc.KeysEndpoint.ForceQuery)
	assert.Empty(t, pc.KeysEndpoint.RawQuery)
	assert.Empty(t, pc.KeysEndpoint.Fragment)

	// Check RegistrationEndpoint details
	assert.Equal(t, httpsScheme, pc.RegistrationEndpoint.Scheme)
	assert.Empty(t, pc.RegistrationEndpoint.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.RegistrationEndpoint.User)
	assert.Equal(t, accountsGoogle, pc.RegistrationEndpoint.Host)
	assert.Equal(t, registrationEndpointPath, pc.RegistrationEndpoint.Path)
	assert.Empty(t, pc.RegistrationEndpoint.RawPath)
	assert.False(t, pc.RegistrationEndpoint.ForceQuery)
	assert.Empty(t, pc.RegistrationEndpoint.RawQuery)
	assert.Empty(t, pc.RegistrationEndpoint.Fragment)

	// Check Policy details
	assert.Equal(t, httpsScheme, pc.Policy.Scheme)
	assert.Empty(t, pc.Policy.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.Policy.User)
	assert.Equal(t, accountsGoogle, pc.Policy.Host)
	assert.Equal(t, policyPath, pc.Policy.Path)
	assert.Empty(t, pc.Policy.RawPath)
	assert.False(t, pc.Policy.ForceQuery)
	assert.Empty(t, pc.Policy.RawQuery)
	assert.Empty(t, pc.Policy.Fragment)

	// Check TermsOfService details
	assert.Equal(t, httpsScheme, pc.TermsOfService.Scheme)
	assert.Empty(t, pc.TermsOfService.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.TermsOfService.User)
	assert.Equal(t, accountsGoogle, pc.TermsOfService.Host)
	assert.Equal(t, termsOfServicePath, pc.TermsOfService.Path)
	assert.Empty(t, pc.TermsOfService.RawPath)
	assert.False(t, pc.TermsOfService.ForceQuery)
	assert.Empty(t, pc.TermsOfService.RawQuery)
	assert.Empty(t, pc.TermsOfService.Fragment)

	// Check ServiceDocs details
	assert.Equal(t, httpsScheme, pc.ServiceDocs.Scheme)
	assert.Empty(t, pc.ServiceDocs.Opaque)
	assert.Equal(t, (*url.Userinfo)(nil), pc.ServiceDocs.User)
	assert.Equal(t, accountsGoogle, pc.ServiceDocs.Host)
	assert.Equal(t, serviceDocsPath, pc.ServiceDocs.Path)
	assert.Empty(t, pc.ServiceDocs.RawPath)
	assert.False(t, pc.ServiceDocs.ForceQuery)
	assert.Empty(t, pc.ServiceDocs.RawQuery)
	assert.Empty(t, pc.ServiceDocs.Fragment)
}

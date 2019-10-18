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

// Check parsing an empty URL instead of a valid HTTP/HTTPS URL.
// It should not return any URL or Error message.
func TestParseURIWithEmptyURL(t *testing.T) {
	config := &OidcProviderConfiguration{}
	url, err := config.parseURI("")
	assert.Nil(t, url)
	assert.Nil(t, err)
}

// Check parsing a non HTTP/HTTPS URL. It should should return an error
// stating "Invalid URI scheme".
func TestParseURIWithNonHttpURL(t *testing.T) {
	config := &OidcProviderConfiguration{}
	url, err := config.parseURI("sftp://accounts.unknown.com")
	assert.Nil(t, url)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid URI scheme")
}

// Check parsing an HTTP/HTTPS URL with no Host. It should should return
// an error stating "Host required in URI".
func TestParseURIWithNoHost(t *testing.T) {
	config := &OidcProviderConfiguration{}
	url, err := config.parseURI("http://") // HTTP URL with No hostname
	assert.Nil(t, url)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Host required in URI")

	url, err = config.parseURI("https://") // HTTPS URL with No hostname
	assert.Nil(t, url)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Host required in URI")
}

// Simulate Error parsing URI use case
// Unicode U+237E (⍾), "graphic for bell"
func TestParseURIWithBadHttpURL(t *testing.T) {
	config := &OidcProviderConfiguration{}
	url, err := config.parseURI("⍾://accounts.unknown.com/")
	assert.Nil(t, url)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error parsing URI")
}

// Constants for checking provider configuration method
const (
	GoodIssuer               = "https://accounts.google.com"
	BadIssuer                = "sftp://accounts.google.com"
	GoodAuthEndpoint         = "https://accounts.google.com/o/oauth2/v2/auth"
	BadAuthEndpoint          = "sftp://accounts.google.com/o/oauth2/v2/auth"
	GoodTokenEndpoint        = "https://oauth2.googleapis.com/token"
	BadTokenEndpoint         = "sftp://oauth2.googleapis.com/token"
	GoodUserInfoEndpoint     = "https://openidconnect.googleapis.com/v1/userinfo"
	BadUserInfoEndpoint      = "sftp://openidconnect.googleapis.com/v1/userinfo"
	GoodJwksUri              = "https://www.googleapis.com/oauth2/v3/certs"
	BadJwksUri               = "sftp://www.googleapis.com/oauth2/v3/certs"
	GoodRegistrationEndpoint = "https://accounts.google.com/clients/"
	BadRegistrationEndpoint  = "sftp://accounts.google.com/clients/"
	GoodPolicy               = "https://accounts.google.com/policy/"
	BadPolicy                = "https://accounts.google.com/policy/"
	GoodTermsOfService       = "https://accounts.google.com/termsofservice/"
	BadTermsOfService        = "sftp://accounts.google.com/termsofservice/"
	GoodServiceDocs          = "https://accounts.google.com/servicedocs/"
	BadServiceDocs           = "sftp://accounts.google.com/servicedocs/"
)

func TestAsProviderConfigWithBadIssuer(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer: BadIssuer,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
}

func TestAsProviderConfigWithBadAuthEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:       GoodIssuer,
		AuthEndpoint: BadAuthEndpoint,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
}

func TestAsProviderConfigWithBadTokenEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:        GoodIssuer,
		AuthEndpoint:  GoodAuthEndpoint,
		TokenEndpoint: BadTokenEndpoint,
	}
	pc, err := opc.AsProviderConfig()
	assert.Error(t, err)
	assert.Nil(t, pc.Issuer)
	assert.Nil(t, pc.AuthEndpoint)
	assert.Nil(t, pc.TokenEndpoint)
}

func TestAsProviderConfigWithBadUserInfoEndpoint(t *testing.T) {
	opc := &OidcProviderConfiguration{
		Issuer:           GoodIssuer,
		AuthEndpoint:     GoodAuthEndpoint,
		TokenEndpoint:    GoodTokenEndpoint,
		UserInfoEndpoint: BadUserInfoEndpoint,
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
		Issuer:           GoodIssuer,
		AuthEndpoint:     GoodAuthEndpoint,
		TokenEndpoint:    GoodTokenEndpoint,
		UserInfoEndpoint: GoodUserInfoEndpoint,
		JwksUri:          BadJwksUri,
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
		Issuer:               GoodIssuer,
		AuthEndpoint:         GoodAuthEndpoint,
		TokenEndpoint:        GoodTokenEndpoint,
		UserInfoEndpoint:     GoodUserInfoEndpoint,
		JwksUri:              GoodJwksUri,
		RegistrationEndpoint: BadRegistrationEndpoint,
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
		Issuer:               GoodIssuer,
		AuthEndpoint:         GoodAuthEndpoint,
		TokenEndpoint:        GoodTokenEndpoint,
		UserInfoEndpoint:     GoodUserInfoEndpoint,
		JwksUri:              GoodJwksUri,
		RegistrationEndpoint: GoodRegistrationEndpoint,
		Policy:               BadPolicy,
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
		Issuer:               GoodIssuer,
		AuthEndpoint:         GoodAuthEndpoint,
		TokenEndpoint:        GoodTokenEndpoint,
		UserInfoEndpoint:     GoodUserInfoEndpoint,
		JwksUri:              GoodJwksUri,
		RegistrationEndpoint: GoodRegistrationEndpoint,
		Policy:               GoodPolicy,
		TermsOfService:       BadTermsOfService,
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
		Issuer:               GoodIssuer,
		AuthEndpoint:         GoodAuthEndpoint,
		TokenEndpoint:        GoodTokenEndpoint,
		UserInfoEndpoint:     GoodUserInfoEndpoint,
		JwksUri:              GoodJwksUri,
		RegistrationEndpoint: GoodRegistrationEndpoint,
		Policy:               GoodPolicy,
		TermsOfService:       GoodTermsOfService,
		ServiceDocs:          BadServiceDocs,
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
		Issuer:               GoodIssuer,
		AuthEndpoint:         GoodAuthEndpoint,
		TokenEndpoint:        GoodTokenEndpoint,
		UserInfoEndpoint:     GoodUserInfoEndpoint,
		JwksUri:              GoodJwksUri,
		RegistrationEndpoint: GoodRegistrationEndpoint,
		Policy:               GoodPolicy,
		TermsOfService:       GoodTermsOfService,
		ServiceDocs:          GoodServiceDocs,
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

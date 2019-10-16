package auth

import (
	"fmt"
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

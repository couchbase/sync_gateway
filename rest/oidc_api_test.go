package rest

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddCallbackURLQueryParam(t *testing.T) {
	var oidcAuthProviderGoogle = "google"
	tests := []struct {
		name            string
		inputURL        string
		inputParamName  string
		inputParamValue string
		wantURL         string
		wantError       error
	}{{
		name:            "Add provider parameter to callback URL",
		inputURL:        "https://accounts.google.com/o/oauth2/v2/auth?client_id=EADGBE&redirect_uri=http%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback&response_type=code&scope=openid+email&state=GDCEm",
		inputParamName:  oidcAuthProvider,
		inputParamValue: oidcAuthProviderGoogle,
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?client_id=EADGBE&redirect_uri=http%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback%3Fprovider%3Dgoogle&response_type=code&scope=openid+email&state=GDCEm",
	}, {
		name:           "Add provider parameter with empty value to callback URL",
		inputURL:       "https://accounts.google.com/o/oauth2/v2/auth?client_id=EADGBE&redirect_uri=http%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback&response_type=code&scope=openid+email&state=GDCEm",
		inputParamName: oidcAuthProvider,
		wantURL:        "https://accounts.google.com/o/oauth2/v2/auth?client_id=EADGBE&redirect_uri=http%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback%3Fprovider%3D&response_type=code&scope=openid+email&state=GDCEm",
	}, {
		name:            "Add provider parameter to callback URL which doesn't have redirect_uri",
		inputURL:        "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent",
		inputParamName:  oidcAuthProvider,
		inputParamValue: oidcAuthProviderGoogle,
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent",
		wantError:       ErrNoRedirectURI,
	}, {
		name:            "Add provider parameter to callback URL which has invalid redirect_uri",
		inputURL:        "https://accounts.google.com/o/oauth2/v2/auth?client_id=EADGBE&redirect_uri=http%%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback&response_type=code&scope=openid+email&state=GDCEm",
		inputParamName:  oidcAuthProvider,
		inputParamValue: oidcAuthProviderGoogle,
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?client_id=EADGBE&redirect_uri=http%%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback&response_type=code&scope=openid+email&state=GDCEm",
		wantError:       url.EscapeError("%%3"),
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputURL, err := url.Parse(test.inputURL)
			require.NoError(t, err, "Couldn't parse URL")
			err = addCallbackURLQueryParam(inputURL, test.inputParamName, test.inputParamValue)
			assert.Equal(t, test.wantError, err)
			assert.Equal(t, test.wantURL, inputURL.String())
		})
	}
}

func TestAddCallbackURLQueryParamNoURL(t *testing.T) {
	var oidcAuthProviderGoogle = "google"
	err := addCallbackURLQueryParam(nil, oidcAuthProvider, oidcAuthProviderGoogle)
	assert.Equal(t, ErrBadCallbackURL, err)
}

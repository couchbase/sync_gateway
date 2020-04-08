//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package rest

import (
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddCallbackURLQueryParam(t *testing.T) {
	t.Parallel()
	toURL := func(strURL string) *url.URL {
		uri, err := url.Parse(strURL)
		require.NoError(t, err, "Couldn't parse URL")
		return uri
	}
	tests := []struct {
		name            string
		inputURL        *url.URL
		inputParamName  string
		inputParamValue string
		wantURL         string
		wantError       error
	}{{
		name:            "Add provider parameter to callback URL",
		inputURL:        toURL("https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent&redirect_uri=http://localhost:4984/default/_oidc_callback&response_type=code&scope=openid email&state="),
		inputParamName:  "provider",
		inputParamValue: "google",
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent&redirect_uri=http%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback%26provider%3Dgoogle&response_type=code&scope=openid+email&state=",
	}, {
		name:            "Add provider parameter to nil callback URL",
		inputURL:        nil,
		inputParamName:  "provider",
		inputParamValue: "google",
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?provider=google",
		wantError:       errors.New("URL must not be nil"),
	}, {
		name:            "Add empty provider parameter to callback URL",
		inputURL:        toURL("https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent&redirect_uri=http://localhost:4984/default/_oidc_callback&response_type=code&scope=openid email&state="),
		inputParamValue: "google",
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent&redirect_uri=http://localhost:4984/default/_oidc_callback&response_type=code&scope=openid email&state=",
		wantError:       errors.New("parameter name must not be empty"),
	}, {
		name:           "Add provider parameter with empty value to callback URL",
		inputURL:       toURL("https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent&redirect_uri=http://localhost:4984/default/_oidc_callback&response_type=code&scope=openid email&state="),
		inputParamName: "provider",
		wantURL:        "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent&redirect_uri=http%3A%2F%2Flocalhost%3A4984%2Fdefault%2F_oidc_callback%26provider%3D&response_type=code&scope=openid+email&state=",
		wantError:      errors.New("parameter name must not be empty"),
	}, {
		name:            "Add provider parameter to callback URL which doesn't have redirect_uri",
		inputURL:        toURL("https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent"),
		inputParamName:  "provider",
		inputParamValue: "google",
		wantURL:         "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&client_id=client123&prompt=consent",
		wantError:       errors.New("no " + requestParamRedirectURI + " parameter found in URL"),
	}}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := addCallbackURLQueryParam(tt.inputURL, tt.inputParamName, tt.inputParamValue)
			if err != nil {
				assert.Equal(t, tt.wantError, err)
				if tt.inputURL != nil {
					assert.Equal(t, tt.wantURL, tt.inputURL.String())
				}
				return
			}
			require.NoError(t, err, "Couldn't add query param to URL")
			assert.Equal(t, tt.wantURL, tt.inputURL.String())
		})
	}
}

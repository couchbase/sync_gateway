//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/couchbase/sync_gateway/base"
)

// Response from a Persona assertion verification.
// VerifyPersona will never return a response whose status is not "okay"; returns nil instead.
type PersonaResponse struct {
	Status   string // "okay" or "failure"
	Reason   string // On failure, the error message
	Email    string // The verified email address
	Audience string // The audience value; expected to be your own website URL
	Expires  uint64 // Date assertion expires, as milliseconds since 1/1/1970
	Issuer   string // Hostname of the identity provider that issued the assertion
}

// Verifies a Persona/BrowserID assertion received from a client, returning either the verification
// response (which includes the verified email address) or an error.
// The 'audience' parameter must be the same as the 'origin' parameter the client used when
// requesting the assertion, i.e. the root URL of this website.
func VerifyPersona(assertion string, audience string) (*PersonaResponse, error) {
	// See <https://developer.mozilla.org/en-US/docs/Persona/Remote_Verification_API>
	res, err := http.PostForm("https://verifier.login.persona.org/verify",
		url.Values{"assertion": {assertion}, "audience": {audience}})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Persona verification server status %d", res.StatusCode)
	}
	responseBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway, "Invalid response from Persona verifier")
	}
	var response PersonaResponse
	if err = json.Unmarshal(responseBody, &response); err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway, "Invalid response from Persona verifier")
	}
	if response.Status != "okay" {
		return nil, base.HTTPErrorf(http.StatusUnauthorized, response.Reason)
	}
	return &response, nil
}

func (h *handler) PersonaEnabled() bool {
	return h.server.config.Persona != nil
}

// POST /_persona creates a browserID-based login session and sets its cookie.
// It's API-compatible with the CouchDB plugin: <https://github.com/iriscouch/browserid_couchdb/>
func (h *handler) handlePersonaPOST() error {
	// CORS not allowed for login #115 #762
	originHeader := h.rq.Header["Origin"]
	if len(originHeader) > 0 {
		matched := matchedOrigin(h.server.config.CORS.LoginOrigin, originHeader)
		if matched == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
		}
	}
	var params struct {
		Assertion string `json:"assertion"`
	}
	err := h.readJSONInto(&params)
	if err != nil {
		return err
	}

	origin := h.server.config.Persona.Origin
	if origin == "" {
		base.Warn("Can't accept Persona logins: Server URL not configured")
		return base.HTTPErrorf(http.StatusInternalServerError, "Server url not configured")
	}

	// OK, now verify it:
	base.Logf("Persona: Verifying assertion %q for %q", params.Assertion, origin)
	verifiedInfo, err := VerifyPersona(params.Assertion, origin)
	if err != nil {
		base.Logf("Persona: Failed verify: %v", err)
		return err
	}
	base.Logf("Persona: Logged in %q!", verifiedInfo.Email)

	createUserIfNeeded := h.server.config.Persona.Register
	return h.makeSessionFromEmail(verifiedInfo.Email, createUserIfNeeded)

}

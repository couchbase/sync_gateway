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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
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
		return nil, &base.HTTPError{http.StatusBadGateway,
			fmt.Sprintf("Persona verification server status %d", res.Status)}
	}
	responseBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Invalid response from Persona verifier"}
	}
	var response PersonaResponse
	if err = json.Unmarshal(responseBody, &response); err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Invalid response from Persona verifier"}
	}
	if response.Status != "okay" {
		return nil, &base.HTTPError{http.StatusUnauthorized, response.Reason}
	}
	return &response, nil
}

func (h *handler) PersonaEnabled() bool {
	return h.server.config.Persona != nil
}

// Registers a new user account based on a Persona verified assertion.
// Username will be the same as the verified email address. Password will be random.
// The user will have access to no channels.
func (h *handler) registerPersonaUser(verifiedInfo *PersonaResponse) (auth.User, error) {
	user, err := h.context.auth.NewUser(verifiedInfo.Email, base.GenerateRandomSecret(), channels.Set{})
	if err != nil {
		return nil, err
	}
	user.SetEmail(verifiedInfo.Email)
	err = h.context.auth.Save(user)
	if err != nil {
		return nil, err
	}
	return user, err
}

// POST /_persona creates a browserID-based login session and sets its cookie.
// It's API-compatible with the CouchDB plugin: <https://github.com/iriscouch/browserid_couchdb/>
func (h *handler) handlePersonaPOST() error {
	var params struct {
		Assertion string `json:"assertion"`
	}
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &params)
	if err != nil {
		return err
	}

	origin := h.server.config.Persona.Origin
	if origin == "" {
		base.Warn("Can't accept Persona logins: Server URL not configured")
		return &base.HTTPError{http.StatusInternalServerError, "Server url not configured"}
	}

	// OK, now verify it:
	base.Log("Persona: Verifying assertion %q for %q", params.Assertion, origin)
	verifiedInfo, err := VerifyPersona(params.Assertion, origin)
	if err != nil {
		base.Log("Persona: Failed verify: %v", err)
		return err
	}
	base.Log("Persona: Logged in %q!", verifiedInfo.Email)

	// Email is verified. Look up the user and make a login session for her:
	user, err := h.context.auth.GetUserByEmail(verifiedInfo.Email)
	if err != nil {
		return err
	}
	if user == nil {
		// The email address is authentic but we have no user account for it.
		// Create a User for this session, with the given email address but no
		// channel access and a random password.
		user, err = h.registerPersonaUser(verifiedInfo)
		if err != nil {
			return err
		}
	}
	return h.makeSession(user)
}

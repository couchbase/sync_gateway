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
	"log"
	"net/http"
	"net/url"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

// Response from a BrowserID assertion verification.
// VerifyBrowserID will never return a response whose status is not "okay"; returns nil instead.
type BrowserIDResponse struct {
	Status   string // "okay" or "failure"
	Reason   string // On failure, the error message
	Email    string // The verified email address
	Audience string // The audience value; expected to be your own website URL
	Expires  uint64 // Date assertion expires, as milliseconds since 1/1/1970
	Issuer   string // Hostname of the identity provider that issued the assertion
}

// Verifies a BrowserID/Persona assertion received from a client, returning either the verification
// response (which includes the verified email address) or an error.
// The 'audience' parameter must be the same as the 'origin' parameter the client used when
// requesting the assertion, i.e. the root URL of this website.
func VerifyBrowserID(assertion string, audience string) (*BrowserIDResponse, error) {
	// See <https://developer.mozilla.org/en-US/docs/Persona/Remote_Verification_API>
	res, err := http.PostForm("https://verifier.login.persona.org/verify",
		url.Values{"assertion": {assertion}, "audience": {audience}})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return nil, &base.HTTPError{http.StatusBadGateway,
			fmt.Sprintf("BrowserID verification server status %d", res.Status)}
	}
	responseBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Invalid response from BrowserID verifier"}
	}
	var response BrowserIDResponse
	if err = json.Unmarshal(responseBody, &response); err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Invalid response from BrowserID verifier"}
	}
	if response.Status != "okay" {
		return nil, &base.HTTPError{http.StatusUnauthorized, response.Reason}
	}
	return &response, nil
}

func (h *handler) BrowserIDEnabled() bool {
	return h.context.serverURL != ""
}

// POST /_browserid creates a browserID-based login session and sets its cookie.
// It's API-compatible with the CouchDB plugin: <https://github.com/iriscouch/browserid_couchdb/>
func (h *handler) handleBrowserIDPOST() error {
	var params struct {
		Assertion string `json:"assertion"`
	}
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &params)
	if err != nil {
		return err
	}
	if h.context.serverURL == "" {
		return &base.HTTPError{http.StatusInternalServerError, "Server url not configured"}
	}

	// OK, now verify it:
	log.Printf("BrowserID: Verifying assertion %q for %q", params.Assertion, h.context.serverURL)
	verifiedInfo, err := VerifyBrowserID(params.Assertion, h.context.serverURL)
	if err != nil {
		log.Printf("BrowserID: Failed verify: %v", err)
		return err
	}
	log.Printf("BrowserID: Logged in %q!", verifiedInfo.Email)

	// Email is verified. Look up the user and make a login session for her:
	auth := h.context.auth
	user, err := auth.GetUserByEmail(verifiedInfo.Email)
	if err != nil {
		return err
	}
	return h.makeSession(user)
}

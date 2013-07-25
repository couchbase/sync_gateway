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
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
	"net/http"
	"net/url"
)

const kFacebookOpenGraphURL = "https://graph.facebook.com"

type FacebookResponse struct {
	Id    string
	Name  string
	Email string
}

// POST /_persona creates a browserID-based login session and sets its cookie.
// It's API-compatible with the CouchDB plugin: <https://github.com/iriscouch/browserid_couchdb/>
func (h *handler) handleFacebookPOST() error {

	var params struct {
		AccessToken string `json:"access_token"`
	}
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &params)
	if err != nil {
		return err
	}

	facebookResponse, err := verifyFacebook(kFacebookOpenGraphURL, params.AccessToken)
	if err != nil {
		return err
	}

	createUserIfNeeded := h.server.config.Facebook.Register
	return h.makeSessionFromEmail(facebookResponse.Email, createUserIfNeeded)

}

func verifyFacebook(fbUrl, accessToken string) (*FacebookResponse, error) {

	params := url.Values{"fields": []string{"id,name,email"}, "access_token": []string{accessToken}}
	destUrl := fbUrl + "/me?" + params.Encode()

	res, err := http.Get(destUrl)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		return nil, &base.HTTPError{http.StatusBadGateway,
			fmt.Sprintf("Facebook verification server status %d", res.Status)}
	}

	decoder := json.NewDecoder(res.Body)

	var response FacebookResponse
	err = decoder.Decode(&response)
	if err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Invalid response from Facebook verifier"}
	}

	return &response, nil

}

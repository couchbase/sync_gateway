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
	"github.com/couchbase/sync_gateway/base"
	"net/http"
	"net/url"
)

const kFacebookOpenGraphURL = "https://graph.facebook.com"

type FacebookResponse struct {
	Id    string
	Name  string
	Email string
}

// POST /_facebook creates a facebook-based login session and sets its cookie.
func (h *handler) handleFacebookPOST() error {
	// CORS not allowed for login #115 #762
	originHeader := h.rq.Header["Origin"]
	if len(originHeader) > 0 {
		matched := matchedOrigin(h.server.config.CORS.LoginOrigin, originHeader)
		if matched == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
		}
	}
	var params struct {
		AccessToken string `json:"access_token"`
	}
	err := h.readJSONInto(&params)
	if err != nil {
		return err
	}

	facebookResponse, err := verifyFacebook(kFacebookOpenGraphURL, params.AccessToken)
	if err != nil {
		return err
	}

	createUserIfNeeded := h.server.config.Facebook.Register
	return h.makeSessionFromNameAndEmail(facebookResponse.Id, facebookResponse.Email, createUserIfNeeded)

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
		return nil, base.HTTPErrorf(http.StatusUnauthorized,
			"Facebook verification server status %d", res.StatusCode)
	}

	decoder := json.NewDecoder(res.Body)

	var response FacebookResponse
	err = decoder.Decode(&response)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway, "Invalid response from Facebook verifier")
	}

	return &response, nil

}

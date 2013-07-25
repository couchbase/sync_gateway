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
	"io/ioutil"
	"net/http"
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
	base.Log("handleFacebookPOST called")

	var params struct {
		AccessToken string `json:"access_token"`
	}
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &params)
	if err != nil {
		return err
	}

	base.Log("params: %v", params.AccessToken)

	facebookResponse, err := verifyFacebook(kFacebookOpenGraphURL, params.AccessToken)
	if err != nil {
		return err
	}

	base.Log("facebookRespose: %v", facebookResponse)

	createUserIfNeeded := true // TODO! get this from a config var
	return h.makeSessionFromEmail(facebookResponse.Email, createUserIfNeeded)

}

func verifyFacebook(fbUrl string, accessToken string) (*FacebookResponse, error) {

	destUrl := fmt.Sprintf("%s/me?fields=id,name,email&access_token=%s", fbUrl, accessToken)
	res, err := http.Get(destUrl)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		return nil, &base.HTTPError{http.StatusBadGateway,
			fmt.Sprintf("Facebook verification server status %d", res.Status)}
	}

	responseBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Invalid response from Facebook verifier"}
	}

	var response FacebookResponse
	if err = json.Unmarshal(responseBody, &response); err != nil {
		return nil, &base.HTTPError{http.StatusBadGateway, "Unable to umarshal response from Facebook verifier."}
	}

	base.Log("fb response: %v", response)

	return &response, nil

}

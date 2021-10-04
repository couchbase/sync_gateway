//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"net/url"

	"github.com/couchbase/sync_gateway/base"
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
		matched := matchedOrigin(h.server.config.API.CORS.LoginOrigin, originHeader)
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

	createUserIfNeeded := h.server.config.DeprecatedConfig.Facebook.Register
	return h.makeSessionFromNameAndEmail(facebookResponse.Id, facebookResponse.Email, createUserIfNeeded)

}

func verifyFacebook(fbUrl, accessToken string) (*FacebookResponse, error) {

	params := url.Values{"fields": []string{"id,name,email"}, "access_token": []string{accessToken}}
	destUrl := fbUrl + "/me?" + params.Encode()

	res, err := http.Get(destUrl)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusGatewayTimeout, "Unable to send request to Facebook API: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode >= 300 {
		return nil, base.HTTPErrorf(http.StatusUnauthorized,
			"Facebook verification server status %d", res.StatusCode)
	}

	decoder := base.JSONDecoder(res.Body)

	var response FacebookResponse
	err = decoder.Decode(&response)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway, "Invalid response from Facebook verifier")
	}

	return &response, nil

}

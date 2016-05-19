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
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

const (
	OIDC_AUTH_RESPONSE_TYPE    = "response_type"
	OIDC_AUTH_CLIENT_ID        = "client_id"
	OIDC_AUTH_SCOPE            = "scope"
	OIDC_AUTH_REDIRECT_URI     = "redirect_uri"
	OIDC_AUTH_STATE            = "state"


	OIDC_RESPONSE_TYPE_CODE     = "code"
	OIDC_RESPONSE_TYPE_IMPLICIT = "id_token%20token"
)

func (h *handler) handleOIDC() error {

	scope := h.getQuery("scope")

	// Validate scope is openid
	if scope == nil {
		return errors.New("Scope must be defined in oidc request")
	}
	if !strings.HasPrefix(scope, "openid") {
		return errors.New("Invalid scope - only openid scopes are supported")
	}

	authParams := make(map[string]string)

	// If scope requests offline access, handle as implicit flow.
	if strings.Contains(scope, "offline_access") {
		authParams["response_type"] = OIDC_RESPONSE_TYPE_IMPLICIT
	} else {
		authParams["response_type"] = OIDC_RESPONSE_TYPE_CODE
	}

	authParams[]

	return nil
}

func (h *handler) handleOIDCCallback() error {
	base.LogTo("Oidc", "handleOidcCallback() called")
	return nil
}

func (h *handler) handleOIDCRefresh() error {
	base.LogTo("Oidc", "handleOidcRefresh() called")
	return nil
}

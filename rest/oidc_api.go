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
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/oauth2"
	"github.com/coreos/go-oidc/oidc"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

const (
	OIDC_AUTH_RESPONSE_TYPE = "response_type"
	OIDC_AUTH_CLIENT_ID     = "client_id"
	OIDC_AUTH_SCOPE         = "scope"
	OIDC_AUTH_REDIRECT_URI  = "redirect_uri"
	OIDC_AUTH_STATE         = "state"

	OIDC_RESPONSE_TYPE_CODE     = "code"
	OIDC_RESPONSE_TYPE_IMPLICIT = "id_token%20token"
)

type OIDCTokenResponse struct {
	IDToken      string `json:"id_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

func (h *handler) handleOIDC() error {

	client, err := h.getOIDCClient()
	if err != nil {
		return err
	}

	oac, err := client.OAuthClient()
	if err != nil {
		return err
	}

	state := "1234"
	accessType := ""
	prompt := ""

	// TODO: Should we support direct pass-through of access_type and prompt from the caller?
	offline := h.getBoolQuery("offline")
	if offline {
		accessType = "offline"
		prompt = "consent"
	}

	redirectURL, err := url.Parse(oac.AuthCodeURL(state, accessType, prompt))
	if err != nil {
		return err
	}

	http.Redirect(h.response, h.rq, redirectURL.String(), http.StatusFound)

	return nil
}

func (h *handler) handleOIDCCallback() error {
	code := h.getQuery("code")
	if code == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Code must be present on oidc callback")
	}

	client, err := h.getOIDCClient()
	if err != nil {
		return err
	}

	oac, err := client.OAuthClient()
	if err != nil {
		return err
	}

	tokenResponse, err := oac.RequestToken(oauth2.GrantTypeAuthCode, code)
	if err != nil {
		return err
	}

	callbackResponse := &OIDCTokenResponse{
		IDToken:      tokenResponse.IDToken,
		RefreshToken: tokenResponse.RefreshToken,
	}

	// Create a Sync Gateway session
	if err = h.createSessionForIdToken(tokenResponse.IDToken, client); err != nil {
		return err
	}

	h.writeJSON(callbackResponse)
	return nil
}

func (h *handler) handleOIDCRefresh() error {

	refreshToken := h.getQuery("refresh_token")
	if refreshToken == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Refresh token must be present for oidc refresh")
	}

	client, err := h.getOIDCClient()
	if err != nil {
		return err
	}

	oac, err := client.OAuthClient()
	if err != nil {
		return err
	}

	tokenResponse, err := oac.RequestToken(oauth2.GrantTypeRefreshToken, refreshToken)
	if err != nil {
		base.LogTo("OIDC", "Unsuccessful token refresh: %v", err)
		return base.HTTPErrorf(http.StatusUnauthorized, "Unable to refresh token.")
		return err
	}

	if err = h.createSessionForIdToken(tokenResponse.IDToken, client); err != nil {
		return err
	}

	refreshResponse := &OIDCTokenResponse{
		IDToken: tokenResponse.IDToken,
	}

	h.writeJSON(refreshResponse)

	return nil
}

func (h *handler) createSessionForIdToken(idToken string, client *oidc.Client) error {
	if !h.db.Options.OIDCOptions.DisableSession {
		user, jwt, err := h.db.Authenticator().AuthenticateJWT(idToken, client, h.db.Options.OIDCOptions.Register)
		if err != nil {
			return err
		}
		tokenExpiryTime, err := auth.GetJWTExpiry(jwt)
		if err != nil {
			return err
		}
		sessionTTL := tokenExpiryTime.Sub(time.Now())
		err = h.makeSessionWithTTL(user, sessionTTL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *handler) getOIDCClient() (*oidc.Client, error) {
	client := h.db.GetOIDCClient()
	if client == nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, fmt.Sprintf("OpenID Connect not configured for database %v", h.db.Name))
	}
	return client, nil
}

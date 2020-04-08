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
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/oauth2"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

const (
	OIDC_AUTH_RESPONSE_TYPE = "response_type"
	OIDC_AUTH_CLIENT_ID     = "client_id"
	OIDC_AUTH_SCOPE         = "scope"
	OIDC_AUTH_REDIRECT_URI  = "redirect_uri"
	OIDC_AUTH_STATE         = "state"

	// Request parameter to specify the OpenID Connect provider to be used for authentication,
	// from the list of providers defined in the Sync Gateway configuration.
	requestParamProvider = "provider"

	// Request parameter to specify the URL to which you want the end-user to be redirected
	// after the authorization is complete.
	requestParamRedirectURI = "redirect_uri"

	OIDC_RESPONSE_TYPE_CODE     = "code"
	OIDC_RESPONSE_TYPE_IMPLICIT = "id_token%20token"
)

type OIDCTokenResponse struct {
	IDToken      string `json:"id_token"`                // ID token, from OP
	RefreshToken string `json:"refresh_token,omitempty"` // Refresh token, from OP
	SessionID    string `json:"session_id,omitempty"`    // Sync Gateway session ID
	Username     string `json:"name,omitempty"`          // Sync Gateway user name
	AccessToken  string `json:"access_token,omitempty"`  // Access token, from OP
	TokenType    string `json:"token_type,omitempty"`    // Access token type, from OP
	Expires      int    `json:"expires_in,omitempty"`    // Access token expiry, from OP
}

func (h *handler) handleOIDC() error {

	redirectURL, err := h.handleOIDCCommon()
	if err != nil {
		return err
	}
	http.Redirect(h.response, h.rq, redirectURL, http.StatusFound)
	return nil
}

func (h *handler) handleOIDCChallenge() error {
	redirectURL, err := h.handleOIDCCommon()
	if err != nil {
		return err
	}

	authHeader := fmt.Sprintf("OIDC login=%q", redirectURL)
	h.setHeader("WWW-Authenticate", authHeader)

	return base.HTTPErrorf(http.StatusUnauthorized, "Login Required")
}

func (h *handler) handleOIDCCommon() (redirectURLString string, err error) {

	redirectURLString = ""

	providerName := h.getQuery("provider")
	base.Infof(base.KeyAuth, "Getting provider for name %v", base.UD(providerName))
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return redirectURLString, err
	}

	client := provider.GetClient(h.getOIDCCallbackURL)
	if client == nil {
		return redirectURLString, base.HTTPErrorf(http.StatusInternalServerError, fmt.Sprintf("Unable to obtain client for provider:%s", providerName))
	}
	oac, err := client.OAuthClient()
	if err != nil {
		return redirectURLString, err
	}

	state := ""
	accessType := ""
	prompt := ""

	// TODO: Is there a use case where we need to support direct pass-through of access_type and prompt from the caller?
	offline := h.getBoolQuery("offline")
	if offline {
		accessType = "offline"
		prompt = "consent"
	}

	redirectURL, err := url.Parse(oac.AuthCodeURL(state, accessType, prompt))
	if err != nil {
		return redirectURLString, err
	}

	if !provider.IsDefault {
		base.Debugf(base.KeyAuth, "Adding provider (%v) to callback URL", base.UD(provider.Name))
		if err = addCallbackURLQueryParam(redirectURL, requestParamProvider, provider.Name); err != nil {
			base.Errorf("Failed to add provider to callback URL, err: %v", err)
		}
		base.Debugf(base.KeyAuth, "Callback URL: %s", redirectURL.String())
	}

	return redirectURL.String(), nil
}

func addCallbackURLQueryParam(uri *url.URL, name, value string) error {
	if uri == nil {
		return errors.New("URL must not be nil")
	}
	if name == "" {
		return errors.New("parameter name must not be empty")
	}
	rawQuery, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return err
	}
	redirectURL := rawQuery.Get(requestParamRedirectURI)
	if redirectURL == "" {
		return errors.New("no " + requestParamRedirectURI + " parameter found in URL")
	}
	redirectURL += "&" + name + "=" + value
	rawQuery.Set(requestParamRedirectURI, redirectURL)
	uri.RawQuery = rawQuery.Encode()
	return nil
}

func (h *handler) handleOIDCCallback() error {
	callbackError := h.getQuery("error")
	if callbackError != "" {
		errorDescription := h.getQuery("error_description")
		return base.HTTPErrorf(http.StatusUnauthorized, "oidc_callback received an error: %v", errorDescription)
	}

	code := h.getQuery("code")
	if code == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Code must be present on oidc callback")
	}

	providerName := h.getQuery("provider")
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Unable to identify provider for callback request")
	}

	oac, err := provider.GetClient(h.getOIDCCallbackURL).OAuthClient()
	if err != nil {
		return err
	}

	tokenResponse, err := oac.RequestToken(oauth2.GrantTypeAuthCode, code)
	if err != nil {
		return err
	}

	// Create a Sync Gateway session
	username, sessionID, err := h.createSessionForTrustedIdToken(tokenResponse.IDToken, provider)
	if err != nil {
		return err
	}

	callbackResponse := &OIDCTokenResponse{
		IDToken:      tokenResponse.IDToken,
		RefreshToken: tokenResponse.RefreshToken,
		SessionID:    sessionID,
		Username:     username,
	}

	if provider.IncludeAccessToken {
		callbackResponse.AccessToken = tokenResponse.AccessToken
		callbackResponse.Expires = tokenResponse.Expires
		callbackResponse.TokenType = tokenResponse.TokenType
	}

	h.writeJSON(callbackResponse)
	return nil
}

func (h *handler) handleOIDCRefresh() error {

	refreshToken := h.getQuery("refresh_token")
	if refreshToken == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Refresh token must be present for oidc refresh")
	}

	providerName := h.getQuery("provider")
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Unable to identify provider for callback request")
	}

	oac, err := provider.GetClient(h.getOIDCCallbackURL).OAuthClient()
	if err != nil {
		return err
	}

	tokenResponse, err := oac.RequestToken(oauth2.GrantTypeRefreshToken, refreshToken)
	if err != nil {
		base.Infof(base.KeyAuth, "Unsuccessful token refresh: %v", err)
		return base.HTTPErrorf(http.StatusUnauthorized, "Unable to refresh token.")
	}

	username, sessionID, err := h.createSessionForTrustedIdToken(tokenResponse.IDToken, provider)
	if err != nil {
		return err
	}

	refreshResponse := &OIDCTokenResponse{
		IDToken:   tokenResponse.IDToken,
		SessionID: sessionID,
		Username:  username,
	}

	if provider.IncludeAccessToken {
		refreshResponse.AccessToken = tokenResponse.AccessToken
		refreshResponse.Expires = tokenResponse.Expires
		refreshResponse.TokenType = tokenResponse.TokenType
	}

	h.writeJSON(refreshResponse)

	return nil
}

func (h *handler) createSessionForTrustedIdToken(idToken string, provider *auth.OIDCProvider) (username string, sessionID string, err error) {

	user, jwt, err := h.db.Authenticator().AuthenticateTrustedJWT(idToken, provider, h.getOIDCCallbackURL)
	if err != nil {
		return "", "", err
	}

	if !provider.DisableSession {
		tokenExpiryTime, err := auth.GetJWTExpiry(jwt)
		if err != nil {
			return "", "", err
		}
		sessionTTL := tokenExpiryTime.Sub(time.Now())
		sessionID, err := h.makeSessionWithTTL(user, sessionTTL)
		return user.Name(), sessionID, err
	}
	return user.Name(), "", nil
}

func (h *handler) getOIDCProvider(providerName string) (*auth.OIDCProvider, error) {
	provider, err := h.db.GetOIDCProvider(providerName)
	if provider == nil || err != nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, fmt.Sprintf("OpenID Connect not configured for database %v", h.db.Name))
	}
	return provider, nil
}

// Builds the OIDC callback based on the current request and database. Used during OIDC Client lazy initialization.  Needs to pass
// in dbName, as it's not necessarily initialized on the request yet.
func (h *handler) getOIDCCallbackURL() string {
	scheme := "http"
	if h.rq.TLS != nil {
		scheme = "https"
	}
	if dbName := h.PathVar("db"); dbName == "" {
		base.Warnf("Can't calculate OIDC callback URL without DB in path.")
		return ""
	} else {
		return fmt.Sprintf("%s://%s/%s/%s", scheme, h.rq.Host, dbName, "_oidc_callback")
	}
}

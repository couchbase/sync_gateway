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
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/oauth2"
)

const (
	requestParamCode         = "code"
	requestParamProvider     = "provider"
	requestParamOffline      = "offline"
	requestParamError        = "error"
	requestParamErrorDesc    = "error_description"
	requestParamRefreshToken = "refresh_token"
	requestParamState        = "state"
	requestParamScope        = "scope"
	requestParamRedirectURI  = "redirect_uri"

	// stateCookieName is the name of state cookie to prevent cross-site request forgery (CSRF).
	stateCookieName = "sg-oidc-state"

	// stateCookieTimeout represents the duration of state cookie; state
	// cookie expires within 5 minutes.
	stateCookieTimeout = time.Minute * 5
)

var (
	// ErrNoStateCookie is returned by handler's handleOIDCCallback method when
	// the state cookie is not found during OpenID Connect Auth callback.
	ErrNoStateCookie = base.HTTPErrorf(http.StatusBadRequest, "OIDC Auth Failure: No state cookie found")

	// ErrStateMismatch is returned by handler's handleOIDCCallback method when
	// the state cookie value doesn't match with state param in the callback URL
	// during OpenID Connect Auth callback.
	ErrStateMismatch = base.HTTPErrorf(http.StatusBadRequest, "OIDC Auth Failure: State mismatch")

	// ErrReadStateCookie is returned by handler's handleOIDCCallback method when
	// there is failure reading state cookie value during OpenID Connect Auth callback.
	ErrReadStateCookie = base.HTTPErrorf(http.StatusBadRequest, "OIDC Auth Failure: Couldn't read state")
)

const (
	keyIDToken = "id_token"
)

type OIDCTokenResponse struct {
	IDToken      string `json:"id_token,omitempty"`      // ID token, from OP
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
	providerName := h.getQuery(requestParamProvider)
	base.Infof(base.KeyAuth, "Getting provider for name %v", base.UD(providerName))
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return redirectURLString, err
	}

	client := provider.GetClient(h.getOIDCCallbackURL)
	if client == nil {
		return redirectURLString, base.HTTPErrorf(
			http.StatusInternalServerError, fmt.Sprintf("Unable to obtain client for provider:%s", providerName))
	}

	var redirectURL *url.URL
	state := ""

	// Set state to prevent cross-site request forgery (CSRF) when DisableCallbackState is not enabled.
	if !provider.DisableCallbackState {
		state = base.GenerateRandomSecret()
		stateCookie := h.makeStateCookie(state, time.Now().Add(stateCookieTimeout))
		http.SetCookie(h.response, stateCookie)
	}

	// TODO: Is there a use case where we need to support direct pass-through of access_type and prompt from the caller?
	offline := h.getBoolQuery(requestParamOffline)
	if offline {
		// Set access type to offline and prompt to consent in auth code request URL.
		redirectURL, err = url.Parse(client.Config.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce))
	} else {
		redirectURL, err = url.Parse(client.Config.AuthCodeURL(state))
	}

	if err != nil {
		return redirectURLString, err
	}

	return redirectURL.String(), nil
}

func (h *handler) handleOIDCCallback() error {
	callbackError := h.getQuery(requestParamError)
	if callbackError != "" {
		errorDescription := h.getQuery(requestParamErrorDesc)
		return base.HTTPErrorf(http.StatusUnauthorized, "oidc callback received an error: %v", errorDescription)
	}

	code := h.getQuery(requestParamCode)
	if code == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Code must be present on oidc callback")
	}

	providerName := h.getQuery(requestParamProvider)
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Unable to identify provider for callback request")
	}

	// Validate state parameter to prevent cross-site request forgery (CSRF) when callback state is enabled.
	if !provider.DisableCallbackState {
		stateCookie, err := h.rq.Cookie(stateCookieName)

		if err == http.ErrNoCookie || stateCookie == nil {
			return ErrNoStateCookie
		}

		if err != nil {
			base.Warnf("Unexpected error attempting to read OIDC state cookie: %v", err)
			return ErrReadStateCookie
		}

		stateParam := h.rq.URL.Query().Get(requestParamState)
		if stateParam != stateCookie.Value {
			return ErrStateMismatch
		}

		// Delete the state cookie on successful validation.
		stateCookie = h.makeStateCookie("", time.Unix(0, 0))
		http.SetCookie(h.response, stateCookie)
	}

	client := provider.GetClient(h.getOIDCCallbackURL)
	if client == nil {
		return err
	}

	// Converts the authorization code into a token.
	token, err := client.Config.Exchange(context.Background(), code)
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Failed to exchange token: "+err.Error())
	}

	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		return base.HTTPErrorf(http.StatusInternalServerError, "No id_token field in oauth2 token.")
	}
	base.Infof(base.KeyAuth, "Obtained token from Authorization Server: %v", rawIDToken)

	// Create a Sync Gateway session
	username, sessionID, err := h.createSessionForTrustedIdToken(rawIDToken, provider)
	if err != nil {
		return err
	}

	callbackResponse := &OIDCTokenResponse{
		IDToken:      rawIDToken,
		RefreshToken: token.RefreshToken,
		SessionID:    sessionID,
		Username:     username,
	}

	if provider.IncludeAccessToken {
		callbackResponse.AccessToken = token.AccessToken
		callbackResponse.Expires = int(token.Expiry.Sub(time.Now()).Seconds())
		callbackResponse.TokenType = token.TokenType
	}

	h.writeJSON(callbackResponse)
	return nil
}

func (h *handler) handleOIDCRefresh() error {
	refreshToken := h.getQuery(requestParamRefreshToken)
	if refreshToken == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Refresh token must be present for oidc refresh")
	}

	providerName := h.getQuery(requestParamProvider)
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Unable to identify provider for callback request")
	}

	client := provider.GetClient(h.getOIDCCallbackURL)
	if client == nil {
		return err
	}

	token, err := client.Config.TokenSource(context.Background(), &oauth2.Token{RefreshToken: refreshToken}).Token()
	if err != nil {
		base.Infof(base.KeyAuth, "Unsuccessful token refresh: %v", err)
		return base.HTTPErrorf(http.StatusInternalServerError, "Unable to refresh token.")
	}

	rawIDToken, ok := token.Extra(keyIDToken).(string)
	if !ok {
		return base.HTTPErrorf(http.StatusInternalServerError, "No id_token field in oauth2 token.")
	}
	base.Infof(base.KeyAuth, "Obtained token from Authorization Server: %v", rawIDToken)

	username, sessionID, err := h.createSessionForTrustedIdToken(rawIDToken, provider)
	if err != nil {
		return err
	}

	refreshResponse := &OIDCTokenResponse{
		IDToken:   rawIDToken,
		SessionID: sessionID,
		Username:  username,
	}

	if provider.IncludeAccessToken {
		refreshResponse.AccessToken = token.AccessToken
		refreshResponse.Expires = int(token.Expiry.Sub(time.Now()).Seconds())
		refreshResponse.TokenType = token.TokenType
	}

	h.writeJSON(refreshResponse)
	return nil
}

func (h *handler) createSessionForTrustedIdToken(rawIDToken string, provider *auth.OIDCProvider) (username string, sessionID string, err error) {
	user, tokenExpiryTime, err := h.db.Authenticator().AuthenticateTrustedJWT(rawIDToken, provider)
	if err != nil {
		return "", "", err
	}
	if user == nil {
		return "", "", base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
	}

	if !provider.DisableSession {
		sessionTTL := tokenExpiryTime.Sub(time.Now())
		sessionID, err := h.makeSessionWithTTL(user, sessionTTL)
		return user.Name(), sessionID, err
	}
	return user.Name(), "", nil
}

func (h *handler) getOIDCProvider(providerName string) (*auth.OIDCProvider, error) {
	provider, err := h.db.GetOIDCProvider(providerName)
	if provider == nil || err != nil {
		return nil, base.HTTPErrorf(
			http.StatusBadRequest, fmt.Sprintf("OpenID Connect not configured for database %v", h.db.Name))
	}
	return provider, nil
}

// Builds the OIDC callback based on the current request. Used during OIDC Client lazy initialization.
// Need to pass providerName and isDefault for the requested provider to determine whether we need to append it to the callback URL or not.
func (h *handler) getOIDCCallbackURL(providerName string, isDefault bool) string {
	dbName := h.PathVar("db")
	if dbName == "" {
		base.Warnf("Can't calculate OIDC callback URL without DB in path.")
		return ""
	}

	scheme := "http"
	if h.rq.TLS != nil {
		scheme = "https"
	}

	callbackURL := scheme + "://" + h.rq.Host + "/" + dbName + "/_oidc_callback"
	if isDefault || providerName == "" {
		return callbackURL
	}

	callbackURL, err := auth.SetURLQueryParam(callbackURL, auth.OIDCAuthProvider, providerName)
	if err != nil {
		base.Warnf("Failed to add provider %q to OIDC callback URL (%s): %v", base.UD(providerName), callbackURL, err)
	}
	return callbackURL
}

// makeStateCookie returns a new state cookie with the value and expiry provided.
func (h *handler) makeStateCookie(value string, expiry time.Time) *http.Cookie {
	cookie := &http.Cookie{
		Name:     stateCookieName,
		Value:    value,
		HttpOnly: true,
		Expires:  expiry,
	}
	if h.rq.TLS != nil {
		cookie.Secure = true
	}
	base.AddDbPathToCookie(h.rq, cookie)
	return cookie
}

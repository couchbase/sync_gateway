//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
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

	// stateCookieMaxAge represents the number of seconds until the cookie expires.
	// The state cookie expires in 5 minutes from when the response was generated.
	stateCookieMaxAge = 300
)

var (
	// ErrNoStateCookie is returned by handler's handleOIDCCallback method when
	// the state cookie is not found during OpenID Connect Auth callback.
	ErrNoStateCookie = base.HTTPErrorf(http.StatusBadRequest, "OIDC Auth Failure: No state cookie found, client needs to support cookies when OIDC callback state is enabled")

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
	base.InfofCtx(h.ctx(), base.KeyAuth, "Getting provider for name %v", base.UD(providerName))
	provider, err := h.getOIDCProvider(providerName)
	if err != nil || provider == nil {
		return redirectURLString, err
	}

	client, err := provider.GetClient(h.db.Ctx, h.getOIDCCallbackURL)
	if err != nil {
		return redirectURLString, base.HTTPErrorf(
			http.StatusInternalServerError, fmt.Sprintf("Unable to obtain client for provider: %s - %v", providerName, err))
	}

	var redirectURL *url.URL
	state := ""

	// Set state to prevent cross-site request forgery (CSRF) when DisableCallbackState is not enabled.
	if !provider.DisableCallbackState {
		state, err = base.GenerateRandomSecret()
		if err != nil {
			return redirectURLString, err
		}
		stateCookie := h.makeStateCookie(state, stateCookieMaxAge)
		http.SetCookie(h.response, stateCookie)
	}

	// TODO: Is there a use case where we need to support direct pass-through of access_type and prompt from the caller?
	offline := h.getBoolQuery(requestParamOffline)
	if offline {
		// Set access type to offline and prompt to consent in auth code request URL.
		redirectURL, err = url.Parse(client.Config().AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce))
	} else {
		redirectURL, err = url.Parse(client.Config().AuthCodeURL(state))
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
			base.WarnfCtx(h.ctx(), "Unexpected error attempting to read OIDC state cookie: %v", err)
			return ErrReadStateCookie
		}

		stateParam := h.rq.URL.Query().Get(requestParamState)
		if stateParam != stateCookie.Value {
			return ErrStateMismatch
		}

		// Delete the state cookie on successful validation.
		stateCookie = h.makeStateCookie("", -1)
		http.SetCookie(h.response, stateCookie)
	}

	client, err := provider.GetClient(h.db.Ctx, h.getOIDCCallbackURL)
	if err != nil {
		return fmt.Errorf("OIDC initialization error: %w", err)
	}

	// Converts the authorization code into a token.
	context := auth.GetOIDCClientContext(provider.InsecureSkipVerify)
	token, err := client.Config().Exchange(context, code)
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Failed to exchange token: "+err.Error())
	}

	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		return base.HTTPErrorf(http.StatusInternalServerError, "No id_token field in oauth2 token.")
	}
	base.InfofCtx(h.ctx(), base.KeyAuth, "Obtained token from Authorization Server: %v", rawIDToken)

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

	client, err := provider.GetClient(h.db.Ctx, h.getOIDCCallbackURL)
	if err != nil {
		return fmt.Errorf("OIDC initialization error: %w", err)
	}

	context := auth.GetOIDCClientContext(provider.InsecureSkipVerify)
	token, err := client.Config().TokenSource(context, &oauth2.Token{RefreshToken: refreshToken}).Token()
	if err != nil {
		base.InfofCtx(h.ctx(), base.KeyAuth, "Unsuccessful token refresh: %v", err)
		return base.HTTPErrorf(http.StatusInternalServerError, "Unable to refresh token.")
	}

	rawIDToken, ok := token.Extra(keyIDToken).(string)
	if !ok {
		return base.HTTPErrorf(http.StatusInternalServerError, "No id_token field in oauth2 token.")
	}
	base.InfofCtx(h.ctx(), base.KeyAuth, "Obtained token from Authorization Server: %v", rawIDToken)

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
	user, tokenExpiryTime, err := h.db.Authenticator(h.db.Ctx).AuthenticateTrustedJWT(rawIDToken, provider, h.getOIDCCallbackURL)
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
		base.WarnfCtx(h.ctx(), "Can't calculate OIDC callback URL without DB in path.")
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
		base.WarnfCtx(h.ctx(), "Failed to add provider %q to OIDC callback URL (%s): %v", base.UD(providerName), callbackURL, err)
	}
	return callbackURL
}

// makeStateCookie creates a new state cookie with the specified value and Max-Age.
// Max-Age has precedence whilst determining the state cookie expiration even though
// both Expires and Max-Age are set.
func (h *handler) makeStateCookie(value string, maxAge int) *http.Cookie {
	cookie := &http.Cookie{
		Name:     stateCookieName,
		Value:    value,
		HttpOnly: true,
		MaxAge:   maxAge,
	}
	if h.rq.TLS != nil {
		cookie.Secure = true
	}
	if maxAge > 0 {
		cookie.Expires = time.Now().Add(time.Duration(maxAge) * time.Second)
	}
	base.AddDbPathToCookie(h.rq, cookie)
	return cookie
}

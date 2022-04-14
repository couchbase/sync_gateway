//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

const kDefaultSessionTTL = 24 * time.Hour

// Respond with a JSON struct containing info about the current login session
func (h *handler) respondWithSessionInfo() error {

	response := h.formatSessionResponse(h.user)

	h.writeJSON(response)
	return nil
}

// GET /_session returns info about the current user
func (h *handler) handleSessionGET() error {
	return h.respondWithSessionInfo()
}

// POST /_session creates a login session and sets its cookie
func (h *handler) handleSessionPOST() error {
	// CORS not allowed for login #115 #762
	originHeader := h.rq.Header["Origin"]
	if len(originHeader) > 0 {
		matched := ""
		if h.server.config.API.CORS != nil {
			matched = matchedOrigin(h.server.config.API.CORS.LoginOrigin, originHeader)
		}
		if matched == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
		}
	}

	// NOTE: handleSessionPOST doesn't handle creating users from OIDC - checkAuth calls out into AuthenticateUntrustedJWT.
	// Therefore, if by this point `h.user` is guest, this isn't creating a session from OIDC.
	if h.db.Options.DisablePasswordAuthentication && (h.user == nil || h.user.Name() == "") {
		return base.HTTPErrorf(http.StatusUnauthorized, "Password authentication is disabled")
	}
	user, err := h.getUserFromSessionRequestBody()

	// If we fail to get a user from the body and we've got a non-GUEST authenticated user, create the session based on that user
	if user == nil && h.user != nil && h.user.Name() != "" {
		return h.makeSession(h.user)
	} else {
		if err != nil {
			return err
		}
		return h.makeSession(user)
	}

}

func (h *handler) getUserFromSessionRequestBody() (auth.User, error) {

	var params struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	err := h.readJSONInto(&params)
	if err != nil {
		return nil, err
	}

	var user auth.User
	user, err = h.db.Authenticator(h.db.Ctx).GetUser(params.Name)
	if err != nil {
		return nil, err
	}

	if user != nil && !user.Authenticate(params.Password) {
		user = nil
	}
	return user, err
}

// DELETE /_session logs out the current session
func (h *handler) handleSessionDELETE() error {
	// CORS not allowed for login #115 #762
	originHeader := h.rq.Header["Origin"]
	if len(originHeader) > 0 {
		matched := ""
		if h.server.config.API.CORS != nil {
			matched = matchedOrigin(h.server.config.API.CORS.LoginOrigin, originHeader)
		}
		if matched == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
		}
	}

	cookie := h.db.Authenticator(h.db.Ctx).DeleteSessionForCookie(h.rq)
	if cookie == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no session")
	}
	http.SetCookie(h.response, cookie)
	return nil
}

func (h *handler) makeSession(user auth.User) error {

	_, err := h.makeSessionWithTTL(user, kDefaultSessionTTL)
	if err != nil {
		return err
	}
	return h.respondWithSessionInfo()
}

// Creates a session with TTL and adds to the response.  Does NOT return the session info response.
func (h *handler) makeSessionWithTTL(user auth.User, expiry time.Duration) (sessionID string, err error) {
	if user == nil {
		return "", base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
	}
	h.user = user
	auth := h.db.Authenticator(h.db.Ctx)
	session, err := auth.CreateSession(user.Name(), expiry)
	if err != nil {
		return "", err
	}
	cookie := auth.MakeSessionCookie(session, h.db.Options.SecureCookieOverride, h.db.Options.SessionCookieHttpOnly)
	base.AddDbPathToCookie(h.rq, cookie)
	http.SetCookie(h.response, cookie)
	return session.ID, nil
}

// MakeSessionFromUserAndEmail first attempts to find the user by username.  If found, updates the users's
// email if different. If no match for username, attempts to find by the user by email.
// If not found, and createUserIfNeeded=true, creates a new user based on username, email.
func (h *handler) makeSessionFromNameAndEmail(username, email string, createUserIfNeeded bool) error {

	// First attempt lookup by username and make a login session for her.
	user, err := h.db.Authenticator(h.db.Ctx).GetUser(username)
	if err != nil {
		return err
	}

	// Attempt email updates/lookups if an email is provided.
	if len(email) > 0 {
		if user != nil {
			// User found, check whether the email needs to be updated
			// (e.g. user has changed email in external auth system)
			if email != user.Email() {
				if err = h.db.Authenticator(h.db.Ctx).UpdateUserEmail(user, email); err != nil {
					// Failure to update email during session creation is non-critical, log and continue.
					base.InfofCtx(h.ctx(), base.KeyAuth, "Unable to update email for user %s during session creation.  Session will still be created. Error:%v,", base.UD(username), err)
				}
			}
		} else {
			// User not found by username. Attempt user lookup by email. This provides backward
			// compatibility for users that were originally created with id = email
			if user, err = h.db.Authenticator(h.db.Ctx).GetUserByEmail(email); err != nil {
				return err
			}
		}
	}

	// Couldn't find existing user.
	if user == nil {
		if !createUserIfNeeded {
			return base.HTTPErrorf(http.StatusUnauthorized, "No such user")
		}

		// Create a User with the given username, email address, and a random password.
		// CAS mismatch indicates the user has been created by another request underneath us, can continue with session creation
		user, err = h.db.Authenticator(h.db.Ctx).RegisterNewUser(username, email)
		if err != nil && !base.IsCasMismatch(err) {
			return err
		}
	}

	return h.makeSession(user)
}

// ADMIN API: Generates a login session for a user and returns the session ID and cookie name.
func (h *handler) createUserSession() error {
	h.assertAdminOnly()
	var params struct {
		Name string `json:"name"`
		TTL  int    `json:"ttl"`
	}
	params.TTL = int(kDefaultSessionTTL / time.Second)
	err := h.readJSONInto(&params)
	if err != nil {
		return err
	} else if params.Name == "" || params.Name == base.GuestUsername || !auth.IsValidPrincipalName(params.Name) {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid or missing user name")
	} else if user, err := h.db.Authenticator(h.db.Ctx).GetUser(params.Name); user == nil {
		if err == nil {
			err = base.HTTPErrorf(http.StatusNotFound, "No such user %q", params.Name)
		}
		return err
	}

	ttl := time.Duration(params.TTL) * time.Second
	if ttl < 1.0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid or missing ttl")
	}

	authenticator := h.db.Authenticator(h.db.Ctx)
	session, err := authenticator.CreateSession(params.Name, ttl)
	if err != nil {
		return err
	}
	var response struct {
		SessionID  string `json:"session_id"`
		Expires    string `json:"expires"`
		CookieName string `json:"cookie_name"`
	}
	response.SessionID = session.ID
	response.Expires = session.Expiration.UTC().Format(time.RFC3339)
	response.CookieName = authenticator.SessionCookieName
	h.writeJSON(response)
	return nil
}

func (h *handler) getUserSession() error {

	h.assertAdminOnly()
	session, err := h.db.Authenticator(h.db.Ctx).GetSession(h.PathVar("sessionid"))

	if session == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}

	return h.respondWithSessionInfoForSession(session)
}

// ADMIN API: Deletes a specified session.  If username is present on the request, validates
// that the session being deleted is associated with the user.
func (h *handler) deleteUserSession() error {
	h.assertAdminOnly()
	userName := h.PathVar("name")
	if userName != "" {
		return h.deleteUserSessionWithValidation(h.PathVar("sessionid"), userName)
	} else {
		return h.db.Authenticator(h.db.Ctx).DeleteSession(h.PathVar("sessionid"))
	}
}

// ADMIN API: Deletes all sessions for a user
func (h *handler) deleteUserSessions() error {
	h.assertAdminOnly()
	userName := h.PathVar("name")
	return h.db.DeleteUserSessions(h.db.Ctx, userName)
}

// Delete a session if associated with the user provided
func (h *handler) deleteUserSessionWithValidation(sessionId string, userName string) error {

	// Validate that the session being deleted belongs to the user.  This adds some
	// overhead - for user-agnostic session deletion should use deleteSession
	session, getErr := h.db.Authenticator(h.db.Ctx).GetSession(sessionId)
	if session == nil {
		if getErr == nil {
			getErr = kNotFoundError
		}
		return getErr
	}

	if getErr == nil {
		if session.Username == userName {
			delErr := h.db.Authenticator(h.db.Ctx).DeleteSession(sessionId)
			if delErr != nil {
				return delErr
			}
		} else {
			return kNotFoundError
		}
	}
	return nil
}

// Respond with a JSON struct containing info about the current login session
func (h *handler) respondWithSessionInfoForSession(session *auth.LoginSession) error {

	user, err := h.db.Authenticator(h.db.Ctx).GetUser(session.Username)

	// let the empty user case succeed
	if err != nil {
		return err
	}

	response := h.formatSessionResponse(user)
	if response != nil {
		h.writeJSON(response)
	}
	return nil
}

// Formats session response similar to what is returned by CouchDB
func (h *handler) formatSessionResponse(user auth.User) db.Body {

	var name *string
	allChannels := channels.TimedSet{}

	if user != nil {
		userName := user.Name()
		if userName != "" {
			name = &userName
		}
		allChannels = user.Channels()
	}

	// Return a JSON struct similar to what CouchDB returns:
	userCtx := db.Body{"name": name, "channels": allChannels}
	handlers := []string{"default", "cookie"}
	response := db.Body{"ok": true, "userCtx": userCtx, "authentication_handlers": handlers}
	return response

}

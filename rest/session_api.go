//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
		if h.server.config.CORS != nil {
			matched = matchedOrigin(h.server.config.CORS.LoginOrigin, originHeader)
		}
		if matched == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
		}
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
	user, err = h.db.Authenticator().GetUser(params.Name)
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
	if len(h.rq.Header["Origin"]) > 0 {
		// CORS not allowed for login #115
		return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
	}
	cookie := h.db.Authenticator().DeleteSessionForCookie(h.rq)
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
	auth := h.db.Authenticator()
	session, err := auth.CreateSession(user.Name(), expiry)
	if err != nil {
		return "", err
	}
	cookie := auth.MakeSessionCookie(session)
	base.AddDbPathToCookie(h.rq, cookie)
	http.SetCookie(h.response, cookie)
	return session.ID, nil
}

func (h *handler) makeSessionFromEmail(email string, createUserIfNeeded bool) error {

	// Email is verified. Look up the user and make a login session for her:
	user, err := h.db.Authenticator().GetUserByEmail(email)
	if err != nil {
		return err
	}
	if user == nil {
		// The email address is authentic but we have no user account for it.
		if !createUserIfNeeded {
			return base.HTTPErrorf(http.StatusUnauthorized, "No such user")
		}

		if len(email) < 1 {
			return base.HTTPErrorf(http.StatusBadRequest, "Cannot register new user: email is missing")
		}

		// Create a User with the given email address as username and a random password.
		user, err = h.db.Authenticator().RegisterNewUser(email, email)
		if err != nil {
			return err
		}
	}
	return h.makeSession(user)

}

// MakeSessionFromUserAndEmail first attempts to find the user by username.  If found, updates the users's
// email if different. If no match for username, attempts to find by the user by email.
// If not found, and createUserIfNeeded=true, creates a new user based on username, email.
func (h *handler) makeSessionFromNameAndEmail(username, email string, createUserIfNeeded bool) error {

	// Username and email are verified. Look up the user and make a login session for her - first
	// attempt lookup by name
	user, err := h.db.Authenticator().GetUser(username)
	if err != nil {
		return err
	}

	// If user found, check whether the email needs to be updated (e.g. user has changed email in
	// external auth system)
	if user != nil {
		if email != user.Email() {
			if err := user.SetEmail(email); err == nil {
				h.db.Authenticator().Save(user)
			}
		}
	} else {
		// User not found by name.  Attempt user lookup by email.  This provides backward
		// compatibility for users that were originally created with id = email
		user, err = h.db.Authenticator().GetUserByEmail(email)
		if err != nil {
			return err
		}
	}
	if user == nil {
		// The user/email are validated, but we don't have a user for either
		if !createUserIfNeeded {
			return base.HTTPErrorf(http.StatusUnauthorized, "No such user")
		}

		if len(email) < 1 {
			return base.HTTPErrorf(http.StatusBadRequest, "Cannot register new user: email is missing")
		}

		// Create a User with the given email address as username and a random password.
		user, err = h.db.Authenticator().RegisterNewUser(username, email)
		if err != nil {
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
	} else if user, err := h.db.Authenticator().GetUser(params.Name); user == nil {
		if err == nil {
			err = base.HTTPErrorf(http.StatusNotFound, "No such user %q", params.Name)
		}
		return err
	}

	ttl := time.Duration(params.TTL) * time.Second
	if ttl < 1.0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid or missing ttl")
	}

	session, err := h.db.Authenticator().CreateSession(params.Name, ttl)
	if err != nil {
		return err
	}
	var response struct {
		SessionID  string    `json:"session_id"`
		Expires    time.Time `json:"expires"`
		CookieName string    `json:"cookie_name"`
	}
	response.SessionID = session.ID
	response.Expires = session.Expiration
	response.CookieName = auth.CookieName
	h.writeJSON(response)
	return nil
}

func (h *handler) getUserSession() error {

	h.assertAdminOnly()
	session, err := h.db.Authenticator().GetSession(h.PathVar("sessionid"))

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
		return h.db.Authenticator().DeleteSession(h.PathVar("sessionid"))
	}
}

// ADMIN API: Deletes all sessions for a user
func (h *handler) deleteUserSessions() error {
	h.assertAdminOnly()

	userName := h.PathVar("name")
	return h.db.DeleteUserSessions(userName)
}

// Delete a session if associated with the user provided
func (h *handler) deleteUserSessionWithValidation(sessionId string, userName string) error {

	// Validate that the session being deleted belongs to the user.  This adds some
	// overhead - for user-agnostic session deletion should use deleteSession
	session, getErr := h.db.Authenticator().GetSession(sessionId)
	if session == nil {
		if getErr == nil {
			getErr = kNotFoundError
		}
		return getErr
	}

	if getErr == nil {
		if session.Username == userName {
			delErr := h.db.Authenticator().DeleteSession(sessionId)
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

	user, err := h.db.Authenticator().GetUser(session.Username)

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

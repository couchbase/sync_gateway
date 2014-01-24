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

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

const kDefaultSessionTTL = 24 * time.Hour

// Respond with a JSON struct containing info about the current login session
func (h *handler) respondWithSessionInfo() error {
	var name *string
	allChannels := channels.TimedSet{}
	if h.user != nil {
		userName := h.user.Name()
		if userName != "" {
			name = &userName
		}
		allChannels = h.user.Channels()
	}
	// Return a JSON struct similar to what CouchDB returns:
	userCtx := db.Body{"name": name, "channels": allChannels}
	handlers := []string{"default", "cookie"}
	if h.PersonaEnabled() {
		handlers = append(handlers, "persona")
	}
	response := db.Body{"ok": true, "userCtx": userCtx, "authentication_handlers": handlers}
	h.writeJSON(response)
	return nil
}

// GET /_session returns info about the current user
func (h *handler) handleSessionGET() error {
	return h.respondWithSessionInfo()
}

// POST /_session creates a login session and sets its cookie
func (h *handler) handleSessionPOST() error {
	var params struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	err := h.readJSONInto(&params)
	if err != nil {
		return err
	}
	var user auth.User
	user, err = h.db.Authenticator().GetUser(params.Name)
	if err != nil {
		return err
	}
	if !user.Authenticate(params.Password) {
		user = nil
	}
	return h.makeSession(user)
}

func (h *handler) makeSession(user auth.User) error {
	if user == nil {
		return base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
	}
	h.user = user
	auth := h.db.Authenticator()
	session, err := auth.CreateSession(user.Name(), kDefaultSessionTTL)
	if err != nil {
		return err
	}
	cookie := auth.MakeSessionCookie(session)
	cookie.Path = "/" + h.db.Name + "/"
	http.SetCookie(h.response, cookie)
	return h.respondWithSessionInfo()
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
		// Create a User with the given email address as username and a random password.
		user, err = h.registerNewUser(email)
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
	} else if params.Name == "" || params.Name == "GUEST" || !auth.IsValidPrincipalName(params.Name) {
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

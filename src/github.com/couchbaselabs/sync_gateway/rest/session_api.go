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
	"io/ioutil"
	"encoding/json"

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
	h.checkAuth() // ignore result; this URL is always accessible
	return h.respondWithSessionInfo()
}

// POST /_session creates a login session and sets its cookie
func (h *handler) handleSessionPOST() error {
	var params struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &params)
	if err != nil {
		return err
	}
	var user auth.User
	user, err = h.context.auth.GetUser(params.Name)
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
		return &base.HTTPError{http.StatusUnauthorized, "Invalid login"}
	}
	h.user = user
	auth := h.context.auth
	session, err := auth.CreateSession(user.Name(), kDefaultSessionTTL)
	if err != nil {
		return err
	}
	cookie := auth.MakeSessionCookie(session)
	cookie.Path = "/" + h.context.dbcontext.Name + "/"
	http.SetCookie(h.response, cookie)
	return h.respondWithSessionInfo()
}

// ADMIN API: Generates a login session for a user and returns the session ID and cookie name.
func (h *handler) createUserSession() error {
	h.assertAdminOnly()
	body, err := ioutil.ReadAll(h.rq.Body)
	if err != nil {
		return err
	}
	var params struct {
		Name string `json:"name"`
		TTL  int    `json:"ttl"`
	}
	err = json.Unmarshal(body, &params)
	if err != nil {
		return err
	}
	ttl := time.Duration(params.TTL) * time.Second
	if params.Name == "" || ttl < 1.0 {
		return &base.HTTPError{http.StatusBadRequest, "Invalid name or ttl"}
	}
	session, err := h.context.auth.CreateSession(params.Name, ttl)
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
	bytes, _ := json.Marshal(response)
	h.response.Header().Set("Content-Type", "application/json")
	h.response.Write(bytes)
	return nil
}


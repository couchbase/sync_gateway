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

	"github.com/couchbaselabs/basecouch/auth"
	"github.com/couchbaselabs/basecouch/base"
	"github.com/couchbaselabs/basecouch/db"
)

const kDefaultSessionTTL = 24 * time.Hour

// Respond with a JSON struct containing info about the current login session
func (h *handler) respondWithSessionInfo() error {
	var name *string
	channels := []string{}
	if h.user != nil {
		if h.user.Name != "" {
			name = &h.user.Name
		}
		channels = h.user.Channels
	}
	// Return a JSON struct similar to what CouchDB returns:
	userCtx := db.Body{"name": name, "channels": channels}
	handlers := []string{"default", "cookie"}
	if h.BrowserIDEnabled() {
		handlers = append(handlers, "browserid")
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
		Name string			`json:"name"`
		Password string		`json:"password"`
	}
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &params)
	if err != nil {
		return err
	}
	var user *auth.User
	user, err = h.context.auth.GetUser(params.Name)
	if err != nil {
		return err
	}
	if !user.Authenticate(params.Password) {
		user = nil
	}
	return h.makeSession(user)
}

func (h *handler) makeSession(user *auth.User) error {
	if user == nil {
		return &base.HTTPError{http.StatusUnauthorized, "Invalid login"}
	}
	h.user = user
	auth := h.context.auth
	session := auth.CreateSession(user.Name, kDefaultSessionTTL)
	http.SetCookie(h.response, auth.MakeSessionCookie(session))
	return h.respondWithSessionInfo()
}

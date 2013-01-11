//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"net/http"
	"time"

	"github.com/couchbaselabs/basecouch/base"
)

// A user login session (used with cookie-based auth.)
type LoginSession struct {
	ID string
	username string
	Expiration time.Time
}

const CookieName = "BaseCouchSession"

func (auth *Authenticator) AuthenticateCookie(rq *http.Request) (*User, error) {
	cookie, _ := rq.Cookie(CookieName)
	if cookie == nil {
		return nil, nil
	}

	auth.lock.Lock()
	defer auth.lock.Unlock()

	session, found := auth.sessions[cookie.Value]
	if found && session.Expiration.Before(time.Now()) {
		delete(auth.sessions, cookie.Value)
		found = false
	}
	if !found {
		return nil, &base.HTTPError{http.StatusUnauthorized, "Invalid session cookie"}
	}
	return auth.GetUser(session.username)
}

func (auth *Authenticator) CreateSession(username string, ttl time.Duration) *LoginSession {
	auth.lock.Lock()
	defer auth.lock.Unlock()

	// Create a random unused session ID:
	var sessionID string
	for {
		sessionID = base.GenerateRandomSecret()
		if _, found := auth.sessions[sessionID]; !found {
			break
		}
	}

	expiration := time.Now().Add(ttl)
	session := &LoginSession{
		ID: sessionID,
		username: username,
		Expiration: expiration,
	}
	auth.sessions[sessionID] = session
	return session
}

func (auth *Authenticator) MakeSessionCookie(session *LoginSession) *http.Cookie {
	if session == nil {
		return nil
	}
	return &http.Cookie{
		Name: CookieName,
		Value: session.ID,
		Expires: session.Expiration,
	}
}

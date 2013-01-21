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

	"github.com/couchbaselabs/sync_gateway/base"
)

// A user login session (used with cookie-based auth.)
type LoginSession struct {
	ID         string       `json:"id"`
	Username   string       `json:"username"`
	Expiration time.Time	`json:"expiration"`
}

const CookieName = "SyncGatewaySession"

func (auth *Authenticator) AuthenticateCookie(rq *http.Request) (*User, error) {
	cookie, _ := rq.Cookie(CookieName)
	if cookie == nil {
		return nil, nil
	}

	var session LoginSession
	err := auth.bucket.Get(docIDForSession(cookie.Value), &session)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return nil, err
	}
	// Don't need to check session.Expiration, because Couchbase will have nuked the document.
	return auth.GetUser(session.Username)
}

func (auth *Authenticator) CreateSession(username string, ttl time.Duration) (*LoginSession, error) {
	session := &LoginSession{
		ID:         base.GenerateRandomSecret(),
		Username:   username,
		Expiration: time.Now().Add(ttl),
	}
	if err := auth.bucket.Set(docIDForSession(session.ID), int(ttl.Seconds()), session); err != nil {
		return nil, err
	}
	return session, nil
}

func (auth *Authenticator) MakeSessionCookie(session *LoginSession) *http.Cookie {
	if session == nil {
		return nil
	}
	return &http.Cookie{
		Name:    CookieName,
		Value:   session.ID,
		Expires: session.Expiration,
	}
}

func docIDForSession(sessionID string) string {
	return "session:" + sessionID
}


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

	"github.com/couchbase/sync_gateway/base"
)

const kDefaultSessionTTL = 24 * time.Hour

// A user login session (used with cookie-based auth.)
type LoginSession struct {
	ID         string        `json:"id"`
	Username   string        `json:"username"`
	Expiration time.Time     `json:"expiration"`
	Ttl        time.Duration `json:"ttl"`
}

const CookieName = "SyncGatewaySession"

const SessionKeyPrefix = "_sync:session:"

func (auth *Authenticator) AuthenticateCookie(rq *http.Request, response http.ResponseWriter) (User, error) {

	cookie, _ := rq.Cookie(CookieName)
	if cookie == nil {
		return nil, nil
	}

	var session LoginSession
	_, err := auth.bucket.Get(docIDForSession(cookie.Value), &session)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return nil, err
	}
	// Don't need to check session.Expiration, because Couchbase will have nuked the document.
	//update the session Expiration if 10% or more of the current expiration time has elapsed
	//if the session does not contain a Ttl (probably created prior to upgrading SG), use
	//default value of 24Hours
	if session.Ttl == 0 {
		session.Ttl = kDefaultSessionTTL
	}
	duration := session.Ttl
	sessionTimeElapsed := int((time.Now().Add(duration).Sub(session.Expiration)).Seconds())
	tenPercentOfTtl := int(duration.Seconds()) / 10
	if sessionTimeElapsed > tenPercentOfTtl {
		session.Expiration = time.Now().Add(duration)
		ttlSec := int(duration.Seconds())
		if err = auth.bucket.Set(docIDForSession(session.ID), ttlSec, session); err != nil {
			return nil, err
		}
		base.AddDbPathToCookie(rq, cookie)
		cookie.Expires = session.Expiration
		http.SetCookie(response, cookie)
	}

	user, err := auth.GetUser(session.Username)
	if user != nil && user.Disabled() {
		user = nil
	}
	return user, err
}

func (auth *Authenticator) CreateSession(username string, ttl time.Duration) (*LoginSession, error) {
	ttlSec := int(ttl.Seconds())
	if ttlSec <= 0 {
		return nil, base.HTTPErrorf(400, "Invalid session time-to-live")
	}

	session := &LoginSession{
		ID:         base.GenerateRandomSecret(),
		Username:   username,
		Expiration: time.Now().Add(ttl),
		Ttl:        ttl,
	}
	if err := auth.bucket.Set(docIDForSession(session.ID), base.DurationToCbsExpiry(ttl), session); err != nil {
		return nil, err
	}
	return session, nil
}

func (auth *Authenticator) GetSession(sessionid string) (*LoginSession, error) {
	var session LoginSession
	_, err := auth.bucket.Get(docIDForSession(sessionid), &session)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return nil, err
	}
	return &session, nil
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

func (auth Authenticator) DeleteSessionForCookie(rq *http.Request) *http.Cookie {
	cookie, _ := rq.Cookie(CookieName)
	if cookie == nil {
		return nil
	}
	auth.bucket.Delete(docIDForSession(cookie.Value))

	newCookie := *cookie
	newCookie.Value = ""
	newCookie.Expires = time.Now()
	return &newCookie
}

func (auth Authenticator) DeleteSession(sessionid string) error {

	return auth.bucket.Delete(docIDForSession(sessionid))

}

func docIDForSession(sessionID string) string {
	return SessionKeyPrefix + sessionID
}

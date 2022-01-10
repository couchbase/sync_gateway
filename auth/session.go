//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

const DefaultCookieName = "SyncGatewaySession"

func (auth *Authenticator) AuthenticateCookie(rq *http.Request, response http.ResponseWriter) (User, error) {

	cookie, _ := rq.Cookie(auth.SessionCookieName)
	if cookie == nil {
		return nil, nil
	}

	var session LoginSession
	_, err := auth.bucket.Get(DocIDForSession(cookie.Value), &session)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			return nil, base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
		}
		return nil, err
	}
	// Don't need to check session.Expiration, because Couchbase will have nuked the document.
	// update the session Expiration if 10% or more of the current expiration time has elapsed
	// if the session does not contain a Ttl (probably created prior to upgrading SG), use
	// default value of 24Hours
	if session.Ttl == 0 {
		session.Ttl = kDefaultSessionTTL
	}
	duration := session.Ttl

	// SessionTimeElapsed and tenPercentOfTtl use Nanoseconds for more precision when converting to int
	sessionTimeElapsed := int((time.Now().Add(duration).Sub(session.Expiration)).Nanoseconds())
	tenPercentOfTtl := int(duration.Nanoseconds()) / 10
	if sessionTimeElapsed > tenPercentOfTtl {
		session.Expiration = time.Now().Add(duration)
		if err = auth.bucket.Set(DocIDForSession(session.ID), base.DurationToCbsExpiry(duration), nil, session); err != nil {
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

	secret, err := base.GenerateRandomSecret()
	if err != nil {
		return nil, err
	}

	session := &LoginSession{
		ID:         secret,
		Username:   username,
		Expiration: time.Now().Add(ttl),
		Ttl:        ttl,
	}
	if err := auth.bucket.Set(DocIDForSession(session.ID), base.DurationToCbsExpiry(ttl), nil, session); err != nil {
		return nil, err
	}
	return session, nil
}

func (auth *Authenticator) GetSession(sessionID string) (*LoginSession, error) {
	var session LoginSession
	_, err := auth.bucket.Get(DocIDForSession(sessionID), &session)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return nil, err
	}
	return &session, nil
}

func (auth *Authenticator) MakeSessionCookie(session *LoginSession, secureCookie bool, httpOnly bool) *http.Cookie {
	if session == nil {
		return nil
	}
	return &http.Cookie{
		Name:     auth.SessionCookieName,
		Value:    session.ID,
		Expires:  session.Expiration,
		Secure:   secureCookie,
		HttpOnly: httpOnly,
	}
}

func (auth Authenticator) DeleteSessionForCookie(rq *http.Request) *http.Cookie {
	cookie, _ := rq.Cookie(auth.SessionCookieName)
	if cookie == nil {
		return nil
	}

	if err := auth.bucket.Delete(DocIDForSession(cookie.Value)); err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error while deleting session for cookie %s, Error: %v", base.UD(cookie.Value), err)
	}

	newCookie := *cookie
	newCookie.Value = ""
	newCookie.Expires = time.Now()
	return &newCookie
}

func (auth Authenticator) DeleteSession(sessionID string) error {
	return auth.bucket.Delete(DocIDForSession(sessionID))
}

func DocIDForSession(sessionID string) string {
	return base.SessionPrefix + sessionID
}

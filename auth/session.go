//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"context"
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const kDefaultSessionTTL = 24 * time.Hour

// A user login session (used with cookie-based auth.)
type LoginSession struct {
	ID          string        `json:"id"`
	Username    string        `json:"username"`
	Expiration  time.Time     `json:"expiration"`
	Ttl         time.Duration `json:"ttl"`
	SessionUUID string        `json:"session_uuid"` // marker of when the user object changes, to match with session docs to determine if they are valid
	OneTime     *bool         `json:"one_time,omitempty"`
}

const DefaultCookieName = "SyncGatewaySession"

func (auth *Authenticator) AuthenticateCookie(rq *http.Request, response http.ResponseWriter) (User, error) {

	cookie, _ := rq.Cookie(auth.SessionCookieName)
	if cookie == nil {
		return nil, nil
	}

	var session LoginSession
	_, err := auth.datastore.Get(auth.DocIDForSession(cookie.Value), &session)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			base.InfofCtx(auth.LogCtx, base.KeyAuth, "Session not found: %s", base.UD(cookie.Value))
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
		if err = auth.datastore.Set(auth.DocIDForSession(session.ID), base.DurationToCbsExpiry(duration), nil, session); err != nil {
			return nil, err
		}
		base.AddDbPathToCookie(rq, cookie)
		cookie.Expires = session.Expiration
		http.SetCookie(response, cookie)
	}

	user, err := auth.GetUser(session.Username)
	if err != nil {
		return nil, err
	}

	if session.SessionUUID != user.GetSessionUUID() {
		base.InfofCtx(auth.LogCtx, base.KeyAuth, "Session no longer valid for user %s", base.UD(session.Username))
		return nil, base.HTTPErrorf(http.StatusUnauthorized, "Session no longer valid for user")
	}
	return user, err
}

// AuthenticateOneTimeSession authenticates a session and deletes it upon successful authentication if it was marked as
// a one time sesssion. If it is a one time session, delete the session.
func (auth *Authenticator) AuthenticateOneTimeSession(ctx context.Context, sessionID string) (User, error) {
	session, user, err := auth.GetSession(sessionID)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
	}

	if session.OneTime != nil && *session.OneTime {
		err = auth.datastore.Delete(auth.DocIDForSession(sessionID))
		if err != nil {
			// If doc is not found, it probably means someone else is simultaneously using the one-time session, error.
			// If the delete error comes from another source, still treat this as an error, expecting the client to retry
			// due to a temporary KV issue.
			if !base.IsDocNotFoundError(err) {
				base.InfofCtx(ctx, base.KeyAuth, "Error deleting one-time session %s. Not allowing login: %v", base.UD(sessionID), err)
			}
			return nil, base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
		}
	}
	return user, nil
}

// CreateSession creates a new login session for the specified user with the specified TTL. If oneTime is true, the
// session is marked as a one-time session and will be removed with a successful authentication.
func (auth *Authenticator) CreateSession(ctx context.Context, user User, ttl time.Duration, oneTime bool) (*LoginSession, error) {
	ttlSec := int(ttl.Seconds())
	if ttlSec <= 0 {
		return nil, base.HTTPErrorf(400, "Invalid session time-to-live")
	}

	secret, err := base.GenerateRandomSecret()
	if err != nil {
		return nil, err
	}

	if user != nil && user.Disabled() {
		return nil, base.HTTPErrorf(400, "User is disabled")
	} else if err != nil {
		return nil, err
	}

	session := &LoginSession{
		ID:          secret,
		Username:    user.Name(),
		Expiration:  time.Now().Add(ttl),
		Ttl:         ttl,
		SessionUUID: user.GetSessionUUID(),
	}
	// only serialize one_time if set
	if oneTime {
		session.OneTime = &oneTime
	}
	if err := auth.datastore.Set(auth.DocIDForSession(session.ID), base.DurationToCbsExpiry(ttl), nil, session); err != nil {
		return nil, err
	}
	base.Audit(ctx, base.AuditIDPublicUserSessionCreated, base.AuditFields{
		base.AuditFieldSessionID: session.ID,
		base.AuditFieldUserName:  user.Name(),
	})

	return session, nil
}

// GetSession returns a session by ID. Return a not found error if the session is not found, or is invalid.
func (auth *Authenticator) GetSession(sessionID string) (*LoginSession, User, error) {
	var session LoginSession
	_, err := auth.datastore.Get(auth.DocIDForSession(sessionID), &session)
	if err != nil {
		return nil, nil, err
	}
	user, err := auth.GetUser(session.Username)
	if err != nil {
		return nil, nil, err
	}
	if user == nil {
		return nil, nil, base.ErrNotFound
	}
	if session.SessionUUID != user.GetSessionUUID() {
		return nil, nil, base.ErrNotFound
	}

	return &session, user, nil
}

func (auth *Authenticator) MakeSessionCookie(session *LoginSession, secureCookie bool, httpOnly bool, sameSite http.SameSite) *http.Cookie {
	if session == nil {
		return nil
	}
	return &http.Cookie{
		Name:     auth.SessionCookieName,
		Value:    session.ID,
		Expires:  session.Expiration,
		Secure:   secureCookie,
		HttpOnly: httpOnly,
		// as of go 1.25, http.SameSiteDefaultMode will omit SameSite attribute from the cookie
		SameSite: sameSite,
	}
}

func (auth Authenticator) DeleteSessionForCookie(ctx context.Context, rq *http.Request) *http.Cookie {
	cookie, _ := rq.Cookie(auth.SessionCookieName)
	if cookie == nil {
		return nil
	}

	if err := auth.DeleteSession(ctx, cookie.Value, ""); err != nil {
		base.InfofCtx(auth.LogCtx, base.KeyAuth, "Error while deleting session for cookie %s, Error: %v", base.UD(cookie.Value), err)
	}

	newCookie := *cookie
	newCookie.Value = ""
	newCookie.Expires = time.Now()
	return &newCookie
}

func (auth Authenticator) DeleteSession(ctx context.Context, sessionID string, username string) error {
	err := auth.datastore.Delete(auth.DocIDForSession(sessionID))
	if err == nil {
		auditFields := base.AuditFields{base.AuditFieldSessionID: sessionID}
		if username != "" {
			auditFields[base.AuditFieldUserName] = username
		}
		base.Audit(ctx, base.AuditIDPublicUserSessionDeleted, auditFields)
	}
	return err
}

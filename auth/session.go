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
	"errors"
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

// errSessionRevoked is returned by getValidSession when the session exists but no longer matches its user.
var errSessionRevoked = errors.New("session no longer valid for user")

// AuthenticateCookie authenticates the request using its session cookie, returning a nil user if there is no cookie.
// The session's username is returned whenever the session was found, including when it is rejected, so that callers
// can attribute failed logins.
func (auth *Authenticator) AuthenticateCookie(rq *http.Request, response http.ResponseWriter) (user User, sessionUsername string, err error) {

	cookie, _ := rq.Cookie(auth.SessionCookieName)
	if cookie == nil {
		return nil, "", nil
	}

	// Validate the session before refreshing its expiry, so that requests rejected here can't keep extending a session
	// that is no longer usable.
	session, user, err := auth.getValidSession(cookie.Value)
	if err != nil {
		if errors.Is(err, errSessionRevoked) {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Session no longer valid for user %s", base.UD(session.Username))
			return nil, session.Username, base.HTTPErrorf(http.StatusUnauthorized, "Session no longer valid for user")
		}
		if base.IsDocNotFoundError(err) {
			base.InfofCtx(auth.LogCtx, base.KeyAuth, "Session not found: %s", base.UD(cookie.Value))
			return nil, "", base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
		}
		return nil, "", err
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
	// One-time sessions must not refresh the cookie — they will be deleted on this request.
	if sessionTimeElapsed > tenPercentOfTtl && (session.OneTime == nil || !*session.OneTime) {
		session.Expiration = time.Now().Add(duration)
		if err = auth.datastore.Set(auth.LogCtx, auth.DocIDForSession(session.ID), base.DurationToCbsExpiry(duration), nil, session); err != nil {
			return nil, session.Username, err
		}
		base.AddDbPathToCookie(rq, cookie)
		cookie.Expires = session.Expiration
		http.SetCookie(response, cookie)
	}

	err = auth.deleteOneTimeSession(auth.LogCtx, session)
	if err != nil {
		return nil, session.Username, err
	}

	return user, session.Username, nil
}

// AuthenticateOneTimeSession authenticates a session and deletes it upon successful authentication if it was marked as
// a one time sesssion. If it is a one time session, delete the session. As with AuthenticateCookie, the session's
// username is returned whenever the session was found.
func (auth *Authenticator) AuthenticateOneTimeSession(ctx context.Context, sessionID string) (user User, sessionUsername string, err error) {
	session, user, err := auth.getValidSession(sessionID)
	if err != nil {
		if errors.Is(err, errSessionRevoked) {
			return nil, session.Username, base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
		}
		if base.IsDocNotFoundError(err) {
			return nil, "", base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
		}
		// Other errors (e.g. a temporary KV failure) don't mean the token is bad, and it hasn't been used up, so they
		// are returned as they are to let the client retry with the same token.
		return nil, "", err
	}

	err = auth.deleteOneTimeSession(ctx, session)
	if err != nil {
		return nil, session.Username, err
	}
	return user, session.Username, nil
}

// deleteOneTimeSession deletes the session if it is marked as a one-time session. If the session can be not deleted, or
// was already deleted an error is returned.
func (auth *Authenticator) deleteOneTimeSession(ctx context.Context, session *LoginSession) error {
	if session.OneTime == nil || !*session.OneTime {
		return nil
	}
	err := auth.datastore.Delete(auth.LogCtx, auth.DocIDForSession(session.ID))
	if err != nil {
		// If doc is not found, it probably means someone else is simultaneously using the one-time session, error.
		// If the delete error comes from another source, still treat this as an error, expecting the client to retry
		// due to a temporary KV issue.
		if !base.IsDocNotFoundError(err) {
			base.InfofCtx(ctx, base.KeyAuth, "Unable to delete one-time session %s. Not allowing login: %v", base.UD(session.ID), err)
		}
		return base.HTTPErrorf(http.StatusUnauthorized, "Session Invalid")
	}
	return nil
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
	if err := auth.datastore.Set(auth.LogCtx, auth.DocIDForSession(session.ID), base.DurationToCbsExpiry(ttl), nil, session); err != nil {
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
	session, user, err := auth.getValidSession(sessionID)
	if errors.Is(err, errSessionRevoked) {
		return nil, nil, base.ErrNotFound
	} else if err != nil {
		return nil, nil, err
	}
	return session, user, nil
}

// getValidSession returns a session by ID along with its user. If the session exists but its user has been deleted or
// its session UUID has changed (on password change, disable, or deleting all sessions), the session is returned along
// with errSessionRevoked so that callers can still identify whose session it was.
func (auth *Authenticator) getValidSession(sessionID string) (*LoginSession, User, error) {
	var session LoginSession
	_, err := auth.datastore.Get(auth.LogCtx, auth.DocIDForSession(sessionID), &session)
	if err != nil {
		return nil, nil, err
	}
	user, err := auth.GetUser(session.Username)
	if err != nil {
		return nil, nil, err
	}
	if user == nil || session.SessionUUID != user.GetSessionUUID() {
		return &session, nil, errSessionRevoked
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
	err := auth.datastore.Delete(auth.LogCtx, auth.DocIDForSession(sessionID))
	if err == nil {
		auditFields := base.AuditFields{base.AuditFieldSessionID: sessionID}
		if username != "" {
			auditFields[base.AuditFieldUserName] = username
		}
		base.Audit(ctx, base.AuditIDPublicUserSessionDeleted, auditFields)
	}
	return err
}

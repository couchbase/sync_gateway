/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSession(t *testing.T) {
	const username = "Alice"
	const invalidSessionTTLError = "400 Invalid session time-to-live"
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))

	user, err := auth.NewUser(username, "password", base.Set{})
	require.NoError(t, err)
	require.NotNil(t, user)
	require.NoError(t, auth.Save(user))

	// Create session with a username and valid TTL of 2 hours.
	session, err := auth.CreateSession(ctx, user, 2*time.Hour)
	require.NoError(t, err)

	assert.Equal(t, username, session.Username)
	assert.Equal(t, 2*time.Hour, session.Ttl)
	assert.NotEmpty(t, session.ID)
	assert.NotEmpty(t, session.Expiration)

	// Once the session is created, the details should be persisted on the bucket
	// and it must be accessible anytime later within the session expiration time.
	session, err = auth.GetSession(session.ID)
	assert.NoError(t, err)

	assert.Equal(t, username, session.Username)
	assert.Equal(t, 2*time.Hour, session.Ttl)
	assert.NotEmpty(t, session.ID)
	assert.NotEmpty(t, session.Expiration)

	// Session must not be created with zero TTL; it's illegal.
	session, err = auth.CreateSession(ctx, user, time.Duration(0))
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), invalidSessionTTLError)

	// Session must not be created with negative TTL; it's illegal.
	session, err = auth.CreateSession(ctx, user, time.Duration(-1))
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), invalidSessionTTLError)
}

func TestDeleteSession(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	ctx := base.TestCtx(t)
	const username = "Alice"
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))

	id, err := base.GenerateRandomSecret()
	require.NoError(t, err)

	mockSession := &LoginSession{
		ID:         id,
		Username:   username,
		Expiration: time.Now().Add(2 * time.Hour),
		Ttl:        24 * time.Hour,
	}
	const noSessionExpiry = 0
	assert.NoError(t, dataStore.Set(auth.DocIDForSession(mockSession.ID), noSessionExpiry, nil, mockSession))
	assert.NoError(t, auth.DeleteSession(ctx, mockSession.ID, ""))

	session, err := auth.GetSession(mockSession.ID)
	assert.Nil(t, session)
	base.RequireDocNotFoundError(t, err)
}

// Coverage for MakeSessionCookie. The MakeSessionCookie should create a cookie
// using the sessionID, username, expiration and TTL from LoginSession provided.
// If nil is provided instead of valid login session, nil must be returned.
func TestMakeSessionCookie(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))

	sessionID, err := base.GenerateRandomSecret()
	require.NoError(t, err)
	mockSession := &LoginSession{
		ID:         sessionID,
		Username:   "Alice",
		Expiration: time.Now().Add(2 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	cookie := auth.MakeSessionCookie(mockSession, false, false)
	assert.Equal(t, DefaultCookieName, cookie.Name)
	assert.Equal(t, sessionID, cookie.Value)
	assert.NotEmpty(t, cookie.Expires)

	// Cookies should not be created with uninitialized session
	mockSession = nil
	cookie = auth.MakeSessionCookie(mockSession, false, false)
	assert.Empty(t, cookie)
}

func TestMakeSessionCookieProperties(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))

	sessionID, err := base.GenerateRandomSecret()
	require.NoError(t, err)
	mockSession := &LoginSession{
		ID:         sessionID,
		Username:   "jrascagneres",
		Expiration: time.Now().Add(2 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	unsecuredCookie := auth.MakeSessionCookie(mockSession, false, false)
	assert.False(t, unsecuredCookie.Secure)

	securedCookie := auth.MakeSessionCookie(mockSession, true, false)
	assert.True(t, securedCookie.Secure)

	httpOnlyFalseCookie := auth.MakeSessionCookie(mockSession, false, false)
	assert.False(t, httpOnlyFalseCookie.HttpOnly)

	httpOnlyCookie := auth.MakeSessionCookie(mockSession, false, true)
	assert.True(t, httpOnlyCookie.HttpOnly)
}

// Coverage for DeleteSessionForCookie. Mock a fake cookie with default cookie name,
// sessionID and expiration; Try to delete the session for the cookie. DocID for session
// must be deleted from the Couchbase bucket and a new cookie must be returned with no
// sessionID against SyncGatewaySession and Now as the expiration value. If the cookie in
// the request is unknown, Nil would be returned from DeleteSessionForCookie.
func TestDeleteSessionForCookie(t *testing.T) {
	const defaultEndpoint = "http://localhost/"
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))

	sessionID, err := base.GenerateRandomSecret()
	require.NoError(t, err)
	body := strings.NewReader("?")
	request, _ := http.NewRequest(http.MethodPost, defaultEndpoint, body)

	cookie := &http.Cookie{
		Name:    DefaultCookieName,
		Value:   sessionID,
		Expires: time.Now().Add(time.Duration(10)),
	}

	request.AddCookie(cookie)
	newCookie := auth.DeleteSessionForCookie(ctx, request)

	assert.NotEmpty(t, newCookie.Name)
	assert.Empty(t, newCookie.Value)
	assert.NotEmpty(t, newCookie.Expires)

	// Check delete session for cookie request with unknown cookie.
	// No new cookie must be returned from DeleteSessionForCookie; Nil.
	request, _ = http.NewRequest(http.MethodPost, defaultEndpoint, body)
	cookie = &http.Cookie{
		Name:    "Unknown",
		Value:   sessionID,
		Expires: time.Now().Add(time.Duration(10)),
	}

	request.AddCookie(cookie)
	newCookie = auth.DeleteSessionForCookie(ctx, request)
	assert.Nil(t, newCookie)
}

func TestCreateSessionChangePassword(t *testing.T) {
	testCases := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "guestuser",
			username: "",
			password: "",
		},

		{
			name:     "emptypassword",
			username: "Alice",
			password: "",
		},
		{
			name:     "realpassword",
			username: "Alice",
			password: "password",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			ctx := base.TestCtx(t)
			testBucket := base.GetTestBucket(t)
			defer testBucket.Close(ctx)
			dataStore := testBucket.GetSingleDataStore()
			auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))

			user, err := auth.NewUser(test.username, test.password, base.Set{})
			require.NoError(t, err)
			require.NotNil(t, user)
			require.NoError(t, auth.Save(user))

			// Create session with a username and valid TTL of 2 hours.
			session, err := auth.CreateSession(ctx, user, 2*time.Hour)
			require.NoError(t, err)

			session, err = auth.GetSession(session.ID)
			require.NoError(t, err)

			request, err := http.NewRequest(http.MethodGet, "", nil)
			require.NoError(t, err)
			request.AddCookie(auth.MakeSessionCookie(session, true, true))

			recorder := httptest.NewRecorder()
			_, err = auth.AuthenticateCookie(request, recorder)
			require.NoError(t, err)

			require.NoError(t, user.SetPassword("someotherpassword"))
			require.NoError(t, auth.Save(user))

			recorder = httptest.NewRecorder()
			_, err = auth.AuthenticateCookie(request, recorder)
			require.Error(t, err)
			require.Equal(t, err.(*base.HTTPError).Status, http.StatusUnauthorized)
		})
	}

}

// TestUserWithoutSessionUUID tests users that existed before we stamped SessionUUID into user docs
func TestUserWithoutSessionUUID(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))
	const username = "Alice"
	user, err := auth.NewUser(username, "password", base.Set{})
	require.NoError(t, err)
	require.NotNil(t, user)
	require.NoError(t, auth.Save(user))

	var rawUser map[string]interface{}
	_, err = auth.datastore.Get(user.DocID(), &rawUser)
	require.NoError(t, err)

	sessionUUIDKey := "session_uuid"
	_, exists := rawUser[sessionUUIDKey]
	require.True(t, exists)
	delete(rawUser, sessionUUIDKey)

	err = auth.datastore.Set(user.DocID(), 0, nil, rawUser)
	require.NoError(t, err)

	user, err = auth.GetUser(username)
	require.NoError(t, err)
	require.NotNil(t, user)

	// Create session with a username and valid TTL of 2 hours.
	session, err := auth.CreateSession(ctx, user, 2*time.Hour)
	require.NoError(t, err)

	session, err = auth.GetSession(session.ID)
	require.NoError(t, err)

	request, err := http.NewRequest(http.MethodGet, "", nil)
	require.NoError(t, err)
	request.AddCookie(auth.MakeSessionCookie(session, true, true))

	recorder := httptest.NewRecorder()
	_, err = auth.AuthenticateCookie(request, recorder)
	require.NoError(t, err)

}

// TestUserDeleteAllSessions changes the session UUID on a user such that existing sessions should not be usable.
func TestUserDeleteAllSessions(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)
	dataStore := testBucket.GetSingleDataStore()
	auth := NewTestAuthenticator(t, dataStore, nil, DefaultAuthenticatorOptions(ctx))
	const username = "Alice"
	user, err := auth.NewUser(username, "password", base.Set{})
	require.NoError(t, err)
	require.NotNil(t, user)
	require.NoError(t, auth.Save(user))

	// Create session with a username and valid TTL of 2 hours.
	session, err := auth.CreateSession(ctx, user, 2*time.Hour)
	require.NoError(t, err)

	session, err = auth.GetSession(session.ID)
	require.NoError(t, err)

	request, err := http.NewRequest(http.MethodGet, "", nil)
	require.NoError(t, err)
	request.AddCookie(auth.MakeSessionCookie(session, true, true))
	recorder := httptest.NewRecorder()

	_, err = auth.AuthenticateCookie(request, recorder)
	require.NoError(t, err)

	// h.deleteUserSessions() equivalent
	user.UpdateSessionUUID()
	err = auth.Save(user)
	require.NoError(t, err)

	_, err = auth.AuthenticateCookie(request, recorder)
	require.EqualError(t, err, "401 Session no longer valid for user")
}

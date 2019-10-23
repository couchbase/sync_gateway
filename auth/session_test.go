package auth

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestCreateSession(t *testing.T) {
	var username string = "Alice"
	const invalidSessionTTLError = "400 Invalid session time-to-live"
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	// Create session with a username and valid TTL of 2 hours.
	session, err := auth.CreateSession(username, 2*time.Hour)
	assert.NoError(t, err)

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
	session, err = auth.CreateSession(username, time.Duration(0))
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), invalidSessionTTLError)

	// Session must not be created with negative TTL; it's illegal.
	session, err = auth.CreateSession(username, time.Duration(-1))
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), invalidSessionTTLError)
}

func TestDeleteSession(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	var username string = "Alice"
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	mockSession := &LoginSession{
		ID:         base.GenerateRandomSecret(),
		Username:   username,
		Expiration: time.Now().Add(2 * time.Hour),
		Ttl:        24 * time.Hour,
	}
	const noSessionExpiry = 0
	assert.NoError(t, bucket.Set(DocIDForSession(mockSession.ID), noSessionExpiry, mockSession))
	assert.NoError(t, auth.DeleteSession(mockSession.ID))

	// Just to verify the session has been deleted gracefully.
	session, err := auth.GetSession(mockSession.ID)
	assert.Nil(t, session)
	assert.NoError(t, err)
}

// Coverage for MakeSessionCookie. The MakeSessionCookie should create a cookie
// using the sessionID, username, expiration and TTL from LoginSession provided.
// If nil is provided instead of valid login session, nil must be returned.
func TestMakeSessionCookie(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)
	var username string = "Alice"

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	sessionID := base.GenerateRandomSecret()
	mockSession := &LoginSession{
		ID:         sessionID,
		Username:   username,
		Expiration: time.Now().Add(2 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	cookie := auth.MakeSessionCookie(mockSession)
	assert.Equal(t, DefaultCookieName, cookie.Name)
	assert.Equal(t, sessionID, cookie.Value)
	assert.NotEmpty(t, cookie.Expires)

	// Cookies should not be created with uninitialized session
	mockSession = nil
	cookie = auth.MakeSessionCookie(mockSession)
	assert.Empty(t, cookie)
}

// Coverage for DeleteSessionForCookie. Mock a fake cookie with default cookie name,
// sessionID and expiration; Try to delete the session for the cookie. DocID for session
// must be deleted from the Couchbase bucket and a new cookie must be returned with no
// sessionID against SyncGatewaySession and Now as the expiration value. If the cookie in
// the request is unknown, Nil would be returned from DeleteSessionForCookie.
func TestDeleteSessionForCookie(t *testing.T) {
	const defaultEndpoint = "http://localhost/"
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	sessionID := base.GenerateRandomSecret()
	body := strings.NewReader("?")
	request, _ := http.NewRequest(http.MethodPost, defaultEndpoint, body)

	cookie := &http.Cookie{
		Name:    DefaultCookieName,
		Value:   sessionID,
		Expires: time.Now().Add(time.Duration(10)),
	}

	request.AddCookie(cookie)
	newCookie := auth.DeleteSessionForCookie(request)

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
	newCookie = auth.DeleteSessionForCookie(request)
	assert.Nil(t, newCookie)
}

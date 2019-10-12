package auth

import (
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

const (
	Username         = "Alice"
	TwoHours         = 2 * time.Hour
	TwentyFourHours  = 24 * time.Hour
	NoExpiry         = 0
	NegativeTtl      = -1
	NoTtl            = 0
	ErrMsgIllegalTtl = "400 Invalid session time-to-live"
	DefaultURL       = "http://localhost/"
)

func TestDocIDForSession(t *testing.T) {
	sessionID := base.GenerateRandomSecret()
	docIDForSession := DocIDForSession(sessionID)
	assert.Equal(t, base.SessionPrefix+sessionID, docIDForSession)
}

func TestCreateSession(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	// Create session with a username and valid TTL of 2 hours.
	session, err := auth.CreateSession(Username, TwoHours)
	assert.NoError(t, err)
	log.Printf("Session: %v", session)

	assert.Equal(t, Username, session.Username)
	assert.Equal(t, TwoHours, session.Ttl)
	assert.NotEmpty(t, session.ID)
	assert.NotEmpty(t, session.Expiration)

	// Once the session is created, the details should be persisted on the bucket
	// and it must be accessible anytime later within the session expiration time.
	session, err = auth.GetSession(session.ID)
	assert.NoError(t, err)
	log.Printf("Session: %v", session)

	assert.Equal(t, Username, session.Username)
	assert.Equal(t, TwoHours, session.Ttl)
	assert.NotEmpty(t, session.ID)
	assert.NotEmpty(t, session.Expiration)

	// Session must not be created with zero TTL; it's illegal.
	session, err = auth.CreateSession(Username, NoTtl)
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), ErrMsgIllegalTtl)

	// Session must not be created with negative TTL; it's illegal.
	session, err = auth.CreateSession(Username, NegativeTtl)
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), ErrMsgIllegalTtl)
}

func TestDeleteSession(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	mockedSession := &LoginSession{
		ID:         base.GenerateRandomSecret(),
		Username:   Username,
		Expiration: time.Now().Add(TwoHours),
		Ttl:        TwentyFourHours,
	}

	assert.NoError(t, bucket.Set(DocIDForSession(mockedSession.ID), NoExpiry, mockedSession))
	log.Printf("Mocked session: %v", mockedSession)
	assert.NoError(t, auth.DeleteSession(mockedSession.ID))
}

// Coverage for MakeSessionCookie. The MakeSessionCookie should create a cookie
// using the sessionID, username, expiration and TTL from LoginSession provided.
// If nil is provided instead of valid login session, nil must be returned.
func TestMakeSessionCookie(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	sessionID := base.GenerateRandomSecret()
	mockedSession := &LoginSession{
		ID:         sessionID,
		Username:   Username,
		Expiration: time.Now().Add(TwoHours),
		Ttl:        TwentyFourHours,
	}

	cookie := auth.MakeSessionCookie(mockedSession)
	log.Printf("cookie: %v", cookie)

	assert.Equal(t, DefaultCookieName, cookie.Name)
	assert.Equal(t, sessionID, cookie.Value)
	assert.NotEmpty(t, cookie.Expires)

	cookie = auth.MakeSessionCookie(nil)
	log.Printf("cookie: %v", cookie)
	assert.Empty(t, cookie)
}

// Coverage for DeleteSessionForCookie. Mock a fake cookie with default cookie name,
// sessionID and expiration; Try to delete the session for the cookie. DocID for session
// must be deleted from the Couchbase bucket and a new cookie must be returned with no
// sessionID against SyncGatewaySession and Now as the expiration value. If the cookie in
// the request is unknown, Nil would be returned from DeleteSessionForCookie.
func TestDeleteSessionForCookie(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)

	defer testBucket.Close()
	bucket := testBucket.Bucket
	auth := NewAuthenticator(bucket, nil)

	sessionID := base.GenerateRandomSecret()
	body := strings.NewReader("?")
	request, _ := http.NewRequest(http.MethodPost, DefaultURL, body)

	cookie := &http.Cookie{
		Name:    DefaultCookieName,
		Value:   sessionID,
		Expires: time.Now().Add(time.Duration(10)),
	}

	request.AddCookie(cookie)
	newCookie := auth.DeleteSessionForCookie(request)
	log.Printf("newCookie: %v", newCookie)

	assert.NotEmpty(t, newCookie.Name)
	assert.Empty(t, newCookie.Value)
	assert.NotEmpty(t, newCookie.Expires)

	// Check delete session for cookie request with unknown cookie.
	// No new cookie must be returned from DeleteSessionForCookie; Nil.
	request, _ = http.NewRequest(http.MethodPost, DefaultURL, body)
	cookie = &http.Cookie{
		Name:    "Unknown",
		Value:   sessionID,
		Expires: time.Now().Add(time.Duration(10)),
	}

	request.AddCookie(cookie)
	newCookie = auth.DeleteSessionForCookie(request)
	log.Printf("newCookie: %v", newCookie)
	assert.Nil(t, newCookie)
}

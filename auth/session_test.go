package auth

import (
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

const (
	Username               = "Alice"
	TwoHours               = 2 * time.Hour
	TwentyFourHours        = 24 * time.Hour
	NoExpiry               = 0
	NegativeTtl            = -1
	NoTtl                  = 0
	ErrorMessageIllegalTtl = "400 Invalid session time-to-live"
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
	assert.Contains(t, err.Error(), ErrorMessageIllegalTtl)

	// Session must not be created with negative TTL; it's illegal.
	session, err = auth.CreateSession(Username, NegativeTtl)
	assert.Nil(t, session)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), ErrorMessageIllegalTtl)
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

// +build !race
// The tests in this file are time-sensitive and disabled when running with the -race flag.  See SG #3055

package auth

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// Test that multiple authentications of the same user/password are fast.
// This is an important check because the underlying bcrypt algorithm used to verify passwords
// is _extremely_ slow (~100ms!) so we use a cache to speed it up (see password_hash.go).
func TestAuthenticationSpeed(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)
	user, _ := auth.NewUser("me", "goIsKewl", nil)
	assert.True(t, user.Authenticate("goIsKewl"))

	start := time.Now()
	for i := 0; i < 1000; i++ {
		assert.True(t, user.Authenticate("goIsKewl"))
	}
	durationPerAuth := time.Since(start) / 1000
	if durationPerAuth > time.Millisecond {
		t.Errorf("user.Authenticate is too slow: %v", durationPerAuth)
	}
}

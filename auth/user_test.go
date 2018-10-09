package auth

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	assert "github.com/couchbaselabs/go.assert"
	"golang.org/x/crypto/bcrypt"
)

func TestUserAuthenticatePasswordHashUpgrade(t *testing.T) {
	const (
		username      = "alice"
		oldPassword   = "hunter2"
		newBcryptCost = 12
	)

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	bucket := gTestBucket.Bucket

	// Reset bcrypt cost after test
	defer SetBcryptCost(bcrypt.DefaultCost)

	// Create user
	auth := NewAuthenticator(bucket, nil)
	u, err := auth.NewUser(username, oldPassword, base.Set{})
	assert.Equals(t, err, nil)
	assert.NotEquals(t, u, nil)

	user := u.(*userImpl)
	oldHash := user.PasswordHash_

	// Make sure their password was hashed with the desired cost
	cost, err := bcrypt.Cost(user.PasswordHash_)
	assert.Equals(t, err, nil)
	assert.Equals(t, cost, bcrypt.DefaultCost)

	// Try to auth with an incorrect password
	assert.False(t, u.Authenticate("test"))

	// Make sure the hash has not changed
	newHash := user.PasswordHash_
	assert.Equals(t, string(newHash), string(oldHash))

	// Authenticate correctly
	assert.True(t, u.Authenticate(oldPassword))

	// Make sure the hash has still not changed (we've not changed the cost yet)
	newHash = user.PasswordHash_
	assert.Equals(t, string(newHash), string(oldHash))

	// Check the cost is still the old value
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.Equals(t, err, nil)
	assert.Equals(t, cost, bcrypt.DefaultCost)

	// Now bump the global bcrypt cost
	err = SetBcryptCost(newBcryptCost)
	assert.Equals(t, err, nil)

	// Authenticate incorrectly again
	assert.False(t, u.Authenticate("test"))

	// Make sure the hash has still not changed (cost has been bumped, but auth was not successful)
	newHash = user.PasswordHash_
	assert.Equals(t, string(newHash), string(oldHash))

	// Authenticate correctly
	assert.True(t, u.Authenticate(oldPassword))

	// Hash should've changed, as the above authenticate was successful
	newHash = user.PasswordHash_
	assert.NotEquals(t, string(newHash), string(oldHash))

	// Cost should now match newBcryptCost
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.Equals(t, err, nil)
	assert.Equals(t, cost, newBcryptCost)
}

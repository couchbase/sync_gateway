package auth

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/bcrypt"
)

func TestUserAuthenticateDisabled(t *testing.T) {
	const (
		username    = "alice"
		oldPassword = "hunter2"
	)

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	bucket := gTestBucket.Bucket

	// Create user
	auth := NewAuthenticator(bucket, nil)
	u, err := auth.NewUser(username, oldPassword, base.Set{})
	assert.NoError(t, err)
	goassert.NotEquals(t, u, nil)

	goassert.False(t, u.Disabled())
	// Correct password, activated account
	goassert.True(t, u.Authenticate(oldPassword))
	// Incorrect password, activated account
	goassert.False(t, u.Authenticate("test"))

	// Disable account
	u.SetDisabled(true)

	goassert.True(t, u.Disabled())
	// Correct password, disabled account
	goassert.False(t, u.Authenticate(oldPassword))
	// Incorrect password, disabled account
	goassert.False(t, u.Authenticate("test"))

}

func TestUserAuthenticatePasswordHashUpgrade(t *testing.T) {
	const (
		username      = "alice"
		oldPassword   = "hunter2"
		newBcryptCost = 12
	)

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	bucket := gTestBucket.Bucket

	// Reset bcrypt cost after test
	defer SetBcryptCost(bcryptDefaultCost)

	// Create user
	auth := NewAuthenticator(bucket, nil)
	u, err := auth.NewUser(username, oldPassword, base.Set{})
	assert.NoError(t, err)
	goassert.NotEquals(t, u, nil)

	user := u.(*userImpl)
	oldHash := user.PasswordHash_

	// Make sure their password was hashed with the desired cost
	cost, err := bcrypt.Cost(user.PasswordHash_)
	assert.NoError(t, err)
	goassert.Equals(t, cost, bcryptDefaultCost)

	// Try to auth with an incorrect password
	goassert.False(t, u.Authenticate("test"))

	// Make sure the hash has not changed
	newHash := user.PasswordHash_
	goassert.Equals(t, string(newHash), string(oldHash))

	// Authenticate correctly
	goassert.True(t, u.Authenticate(oldPassword))

	// Make sure the hash has still not changed (we've not changed the cost yet)
	newHash = user.PasswordHash_
	goassert.Equals(t, string(newHash), string(oldHash))

	// Check the cost is still the old value
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.NoError(t, err)
	goassert.Equals(t, cost, bcryptDefaultCost)

	// Now bump the global bcrypt cost
	err = SetBcryptCost(newBcryptCost)
	assert.NoError(t, err)

	// Authenticate incorrectly again
	goassert.False(t, u.Authenticate("test"))

	// Make sure the hash has still not changed (cost has been bumped, but auth was not successful)
	newHash = user.PasswordHash_
	goassert.Equals(t, string(newHash), string(oldHash))

	// Authenticate correctly
	goassert.True(t, u.Authenticate(oldPassword))

	// Hash should've changed, as the above authenticate was successful
	newHash = user.PasswordHash_
	goassert.NotEquals(t, string(newHash), string(oldHash))

	// Cost should now match newBcryptCost
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.NoError(t, err)
	goassert.Equals(t, cost, newBcryptCost)
}

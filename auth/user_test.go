package auth

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	assert "github.com/couchbaselabs/go.assert"
	"golang.org/x/crypto/bcrypt"
)

func TestUserPasswordHashUpgrade(t *testing.T) {
	const (
		username      = "alice"
		oldPassword   = "hunter2"
		newPassword   = "correct horse battery staple"
		newBcryptCost = 12
	)

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAuth)

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

	// Now authenticate
	assert.False(t, u.Authenticate("test"))
	assert.True(t, u.Authenticate(oldPassword))

	// Make sure their password was hashed with the desired cost
	user := u.(*userImpl)
	cost, err := bcrypt.Cost(user.PasswordHash_)
	assert.Equals(t, err, nil)
	assert.Equals(t, cost, bcrypt.DefaultCost)

	// Now bump the global bcrypt cost
	err = SetBcryptCost(newBcryptCost)
	assert.Equals(t, err, nil)

	// Cost is still default... Password hash was created before change
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.Equals(t, err, nil)
	assert.Equals(t, cost, bcrypt.DefaultCost)

	// Make sure the user can still authenticate
	assert.True(t, u.Authenticate(oldPassword))

	// Reset password
	u.SetPassword(newPassword)
	// Make sure old password doesn't work anymore
	assert.True(t, u.Authenticate(newPassword))
	assert.False(t, u.Authenticate(oldPassword))
	// Now check the bcrypt cost was upgraded
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.Equals(t, err, nil)
	assert.Equals(t, cost, newBcryptCost)
}

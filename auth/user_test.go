package auth

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.NotNil(t, u)

	assert.False(t, u.Disabled())
	// Correct password, activated account
	assert.True(t, u.Authenticate(oldPassword))
	// Incorrect password, activated account
	assert.False(t, u.Authenticate("test"))

	// Disable account
	u.SetDisabled(true)

	assert.True(t, u.Disabled())
	// Correct password, disabled account
	assert.False(t, u.Authenticate(oldPassword))
	// Incorrect password, disabled account
	assert.False(t, u.Authenticate("test"))

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
	assert.NotNil(t, u)

	user := u.(*userImpl)
	oldHash := user.PasswordHash_

	// Make sure their password was hashed with the desired cost
	cost, err := bcrypt.Cost(user.PasswordHash_)
	assert.NoError(t, err)
	assert.Equal(t, bcryptDefaultCost, cost)

	// Try to auth with an incorrect password
	assert.False(t, u.Authenticate("test"))

	// Make sure the hash has not changed
	newHash := user.PasswordHash_
	assert.Equal(t, string(oldHash), string(newHash))

	// Authenticate correctly
	assert.True(t, u.Authenticate(oldPassword))

	// Make sure the hash has still not changed (we've not changed the cost yet)
	newHash = user.PasswordHash_
	assert.Equal(t, string(oldHash), string(newHash))

	// Check the cost is still the old value
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.NoError(t, err)
	assert.Equal(t, bcryptDefaultCost, cost)

	// Now bump the global bcrypt cost
	err = SetBcryptCost(newBcryptCost)
	assert.NoError(t, err)

	// Authenticate incorrectly again
	assert.False(t, u.Authenticate("test"))

	// Make sure the hash has still not changed (cost has been bumped, but auth was not successful)
	newHash = user.PasswordHash_
	assert.Equal(t, string(oldHash), string(newHash))

	// Authenticate correctly
	assert.True(t, u.Authenticate(oldPassword))

	// Hash should've changed, as the above authenticate was successful
	newHash = user.PasswordHash_
	assert.NotEqual(t, string(oldHash), string(newHash))

	// Cost should now match newBcryptCost
	cost, err = bcrypt.Cost(user.PasswordHash_)
	assert.NoError(t, err)
	assert.Equal(t, newBcryptCost, cost)
}

func TestIsValidEmail(t *testing.T) {
	// Valid Email Addresses
	assert.True(t, IsValidEmail("alice@couchbase.com"))       // Valid Email Address Check
	assert.True(t, IsValidEmail("a1ice@couchbase.com"))       // Numbers and letters in the address field
	assert.True(t, IsValidEmail("alice.bob@couchbase.com"))   // Email contains dot in the address field
	assert.True(t, IsValidEmail("alice@couchbase.lab.com"))   // Email contains dot with sub-domain
	assert.True(t, IsValidEmail("alice+bob@couchbase.com"))   // Plus sign is considered valid character
	assert.True(t, IsValidEmail("alice@127.0.0.1"))           // Domain is valid IP address
	assert.True(t, IsValidEmail("1234567890@couchbase.com"))  // Digits in address are valid
	assert.True(t, IsValidEmail("alice@couchbase-com"))       // Dash in domain name is valid
	assert.True(t, IsValidEmail("_______@couchbase-com"))     // Underscore in the address field is valid
	assert.True(t, IsValidEmail("alice@couchbase.name"))      // .name is valid Top Level Domain name
	assert.True(t, IsValidEmail("alice@couchbase.co.jp"))     // Dot in Top Level Domain name is considered valid
	assert.True(t, IsValidEmail("alice-bob@couchbase.co.jp")) // Dash in address field is valid

	// Invalid Email Addresses
	assert.False(t, IsValidEmail("aliceatcouchbasedotcom"))          // Missing @ sign and domain
	assert.False(t, IsValidEmail("#@%^%#$@#$@#.com"))                // Garbage value
	assert.False(t, IsValidEmail("@couchbase.com"))                  // Missing username
	assert.False(t, IsValidEmail("Alice Bob <alice@couchbase.com>")) // Encoded html within email is invalid
	assert.False(t, IsValidEmail("email.couchbase.com"))             // Missing @
	assert.False(t, IsValidEmail("alice@couchbase@couchbase.com"))   // Two @ sign
	assert.False(t, IsValidEmail("áĺíćé@couchbase.com"))             // Unicode char as address
	assert.False(t, IsValidEmail("alice@couchbase.com (Alice Bob)")) // Text followed email is not allowed
	assert.False(t, IsValidEmail("alice@-couchbase.com"))            // Leading dash in front of domain is invalid
}

func TestCanSeeChannelSince(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket.Bucket, nil)
	freeChannels := base.SetFromArray([]string{"ESPN", "HBO", "FX", "AMC"})
	user, err := auth.NewUser("user", "password", freeChannels)
	assert.Nil(t, err)

	role, err := auth.NewRole("music", channels.SetOf(t, "Spotify", "Youtube"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	role, err = auth.NewRole("video", channels.SetOf(t, "Netflix", "Hulu"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	user.(*userImpl).setRolesSince(channels.TimedSet{
		"music": channels.NewVbSimpleSequence(1),
		"video": channels.NewVbSimpleSequence(1)})

	for channel := range freeChannels {
		assert.Equal(t, uint64(1), user.CanSeeChannelSince(channel))
	}
	assert.Equal(t, uint64(0), user.CanSeeChannelSince("unknown"))
}

func TestGetAddedChannels(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket.Bucket, nil)

	role, err := auth.NewRole("music", channels.SetOf(t, "Spotify", "Youtube"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	role, err = auth.NewRole("video", channels.SetOf(t, "Netflix", "Hulu"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	user, err := auth.NewUser("alice", "password", channels.SetOf(t, "ESPN", "HBO", "FX", "AMC"))
	assert.Nil(t, err)
	require.NoError(t, user.SetEmail("alice@couchbase.com"))

	user.(*userImpl).setRolesSince(channels.TimedSet{
		"music": channels.NewVbSimpleSequence(0x5),
		"video": channels.NewVbSimpleSequence(0x6)})

	addedChannels := user.(*userImpl).GetAddedChannels(channels.TimedSet{
		"ESPN": channels.NewVbSimpleSequence(0x5),
		"HBO":  channels.NewVbSimpleSequence(0x6)})

	expectedChannels := channels.SetOf(t, "!", "AMC", "FX", "Hulu", "Netflix", "Spotify", "Youtube")
	log.Printf("Added Channels: %v", addedChannels)
	assert.Equal(t, expectedChannels, addedChannels)
}

// Needless to say; must not authenticate with nil user reference;
func TestUserAuthenticateWithNilUserReference(t *testing.T) {
	var nouser *userImpl
	assert.False(t, nouser.Authenticate("password"))
}

// Must not authenticate if the user account is disabled.
func TestUserAuthenticateWithDisabledUserAccount(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	user.SetDisabled(true)
	assert.True(t, user.Disabled())
	assert.False(t, user.Authenticate(password))
}

// Must not authenticate if old hash is present.
// Password must be reset to use new (bcrypt) password hash.
func TestUserAuthenticateWithOldPasswordHash(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	passwordHash := user.(*userImpl).PasswordHash_
	user.(*userImpl).OldPasswordHash_ = passwordHash
	assert.False(t, user.Authenticate(password))
}

// Must not authenticate with bad password hash
func TestUserAuthenticateWithBadPasswordHash(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	user.SetPassword("hunter3")
	assert.False(t, user.Authenticate(password))
}

// Must authenticate but the password gets rehashed if the password
// was correct if the bcryptCost is different.
func TestUserAuthenticateWithNewBcryptCost(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	const (
		username      = "alice"
		password      = "hunter2"
		newBcryptCost = 12
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	defer SetBcryptCost(bcryptDefaultCost)
	cost, err := bcrypt.Cost(user.(*userImpl).PasswordHash_)
	assert.NoError(t, err)
	assert.Equal(t, bcryptDefaultCost, cost)

	err = SetBcryptCost(newBcryptCost)
	assert.NoError(t, err)
	user.SetPassword(password)

	cost, err = bcrypt.Cost(user.(*userImpl).PasswordHash_)
	assert.NoError(t, err)
	assert.Equal(t, newBcryptCost, cost)
	assert.True(t, user.Authenticate(password))
}

// Must not authenticate if No hash, but (incorrect) password provided.
func TestUserAuthenticateWithNoHashAndBadPassword(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	user.(*userImpl).OldPasswordHash_ = nil
	assert.False(t, user.Authenticate("hunter3"))
}

func TestUserVBHashFunction(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	user, err := auth.NewUser("alice", "password", channels.SetOf(t, "user"))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NotNil(t, user)

	vbHashFunction := func(str string) uint32 {
		return hash(str)
	}
	assert.Equal(t, uint16(0xe760), user.getVbNo(vbHashFunction))
}

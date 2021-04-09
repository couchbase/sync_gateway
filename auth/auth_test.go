//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"encoding/base64"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/square/go-jose.v2/jwt"
)

func canSeeAllChannels(princ Principal, channels base.Set) bool {
	for channel := range channels {
		if !princ.CanSeeChannel(channel) {
			return false
		}
	}
	return true
}

func TestValidateGuestUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.NewUser("invalid:name", "", nil)
	assert.Equal(t, user, (User)(nil))
	assert.True(t, err != nil)
	user, err = auth.NewUser("ValidName", "", nil)
	assert.True(t, user != nil)
	assert.Equal(t, err, nil)
	user, err = auth.NewUser("ValidName", "letmein", nil)
	assert.True(t, user != nil)
	assert.Equal(t, err, nil)
}

func TestValidateRole(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	role, err := auth.NewRole("invalid:name", nil)
	assert.Equal(t, (User)(nil), role)
	assert.True(t, err != nil)
	role, err = auth.NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equal(t, nil, err)
	role, err = auth.NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equal(t, nil, err)
}

func TestValidateUserEmail(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	badEmails := []string{"", "foo", "foo@", "@bar", "foo@bar@buzz"}
	for _, e := range badEmails {
		assert.False(t, IsValidEmail(e))
	}
	goodEmails := []string{"foo@bar", "foo.99@bar.com", "f@bar.exampl-3.com."}
	for _, e := range goodEmails {
		assert.True(t, IsValidEmail(e))
	}
	user, _ := auth.NewUser("ValidName", "letmein", nil)
	assert.False(t, user.SetEmail("foo") == nil)
	assert.Equal(t, nil, user.SetEmail("foo@example.com"))
}

func TestUserPasswords(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, _ := auth.NewUser("me", "letmein", nil)
	assert.True(t, user.Authenticate("letmein"))
	assert.False(t, user.Authenticate("password"))
	assert.False(t, user.Authenticate(""))

	guest, _ := auth.NewUser("", "", nil)
	assert.True(t, guest.Authenticate(""))
	assert.False(t, guest.Authenticate("123456"))

	// Create a second user with the same password
	user2, _ := auth.NewUser("me", "letmein", nil)
	assert.True(t, user2.Authenticate("letmein"))
	assert.False(t, user2.Authenticate("password"))
	assert.True(t, user.Authenticate("letmein"))
	assert.False(t, user.Authenticate("password"))
}

func TestSerializeUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, _ := auth.NewUser("me", "letmein", ch.SetOf(t, "me", "public"))
	require.NoError(t, user.SetEmail("foo@example.com"))
	encoded, _ := base.JSONMarshal(user)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled User as: %s", encoded)

	resu := &userImpl{}
	err := base.JSONUnmarshal(encoded, resu)
	assert.True(t, err == nil)
	goassert.DeepEquals(t, resu.Name(), user.Name())
	goassert.DeepEquals(t, resu.Email(), user.Email())
	goassert.DeepEquals(t, resu.ExplicitChannels(), user.ExplicitChannels())
	assert.True(t, resu.Authenticate("letmein"))
	assert.False(t, resu.Authenticate("123456"))
}

func TestSerializeRole(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	role, _ := auth.NewRole("froods", ch.SetOf(t, "hoopy", "public"))
	encoded, _ := base.JSONMarshal(role)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled Role as: %s", encoded)
	elor := &roleImpl{}
	err := base.JSONUnmarshal(encoded, elor)

	assert.True(t, err == nil)
	goassert.DeepEquals(t, elor.Name(), role.Name())
	goassert.DeepEquals(t, elor.ExplicitChannels(), role.ExplicitChannels())
}

func TestUserAccess(t *testing.T) {

	// User with no access:
	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, _ := auth.NewUser("foo", "password", nil)
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "*")), ch.SetOf(t, "!"))
	assert.False(t, user.CanSeeChannel("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "*")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf(t)) == nil)

	// User with access to one channel:
	user.setChannels(ch.AtSequence(ch.SetOf(t, "x"), 1))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "*")), ch.SetOf(t, "x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf(t, "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf(t)) == nil)

	// User with access to one channel and one derived channel:
	user.setChannels(ch.AtSequence(ch.SetOf(t, "x", "z"), 1))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "*")), ch.SetOf(t, "x", "z"))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "x")), ch.SetOf(t, "x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)

	// User with access to two channels:
	user.setChannels(ch.AtSequence(ch.SetOf(t, "x", "z"), 1))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "*")), ch.SetOf(t, "x", "z"))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "x")), ch.SetOf(t, "x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)

	user.setChannels(ch.AtSequence(ch.SetOf(t, "x", "y"), 1))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "*")), ch.SetOf(t, "x", "y"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y", "z")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)

	// User with wildcard access:
	user.setChannels(ch.AtSequence(ch.SetOf(t, "*", "q"), 1))
	goassert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf(t, "*")), ch.SetOf(t, "*", "q"))
	assert.True(t, user.CanSeeChannel("*"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf(t, "x")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf(t, "*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf(t)) == nil)
}

func TestGetMissingUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.GetUser("noSuchUser")
	assert.Equal(t, nil, err)
	assert.True(t, user == nil)
	user, err = auth.GetUserByEmail("noreply@example.com")
	assert.Equal(t, nil, err)
	assert.True(t, user == nil)
}

func TestGetMissingRole(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	role, err := auth.GetRole("noSuchRole")
	assert.Equal(t, nil, err)
	assert.True(t, role == nil)
}

func TestGetGuestUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.GetUser("")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, user, auth.defaultGuestUser())
}

func TestSaveUsers(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf(t, "test"))
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, user2, user)
}

func TestSaveRoles(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	role, _ := auth.NewRole("testRole", ch.SetOf(t, "test"))
	err := auth.Save(role)
	assert.Equal(t, nil, err)

	role2, err := auth.GetRole("testRole")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, role2, role)
}

type mockComputer struct {
	channels     ch.TimedSet
	roles        ch.TimedSet
	roleChannels ch.TimedSet
	err          error
}

func (self *mockComputer) ComputeChannelsForPrincipal(p Principal) (ch.TimedSet, error) {
	switch p.(type) {
	case User:
		return self.channels, self.err
	case Role:
		return self.roleChannels, self.err
	default:
		return nil, self.err
	}
}

func (self *mockComputer) ComputeRolesForUser(User) (ch.TimedSet, error) {
	return self.roles, self.err
}

func (self *mockComputer) UseGlobalSequence() bool {
	return true
}

func TestRebuildUserChannels(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{channels: ch.AtSequence(ch.SetOf(t, "derived1", "derived2"), 1)}
	auth := NewAuthenticator(bucket, &computer)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf(t, "explicit1"))
	user.SetChannelInvaliSeq(2)
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf(t, "explicit1", "derived1", "derived2", "!"), 1))
}

func TestRebuildRoleChannels(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{roleChannels: ch.AtSequence(ch.SetOf(t, "derived1", "derived2"), 1)}
	auth := NewAuthenticator(bucket, &computer)
	role, err := auth.NewRole("testRole", ch.SetOf(t, "explicit1"))
	assert.NoError(t, err)
	err = auth.InvalidateChannels(role, 1)
	assert.Equal(t, nil, err)

	role2, err := auth.GetRole("testRole")
	assert.Equal(t, nil, err)
	assert.Equal(t, ch.AtSequence(ch.SetOf(t, "explicit1", "derived1", "derived2", "!"), 1), role2.Channels())
}

func TestRebuildChannelsError(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{}
	auth := NewAuthenticator(bucket, &computer)
	role, err := auth.NewRole("testRole2", ch.SetOf(t, "explicit1"))
	assert.NoError(t, err)
	assert.Equal(t, nil, auth.InvalidateChannels(role, 1))

	computer.err = errors.New("I'm sorry, Dave.")

	role2, err := auth.GetRole("testRole2")
	assert.Nil(t, role2)
	assert.Equal(t, computer.err, err)
}

func TestRebuildUserRoles(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{roles: ch.AtSequence(base.SetOf("role1", "role2"), 3)}
	auth := NewAuthenticator(bucket, &computer)
	user, _ := auth.NewUser("testUser", "letmein", nil)
	user.SetExplicitRoles(ch.TimedSet{"role3": ch.NewVbSimpleSequence(1), "role1": ch.NewVbSimpleSequence(1)})
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	// Retrieve the user, triggers initial build of roles
	user1, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	expected := ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	goassert.DeepEquals(t, user1.RoleNames(), expected)

	// Invalidate the roles, triggers rebuild
	err = auth.InvalidateRoles(user1, 1)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	expected = ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	goassert.DeepEquals(t, user2.RoleNames(), expected)
}

func TestRoleInheritance(t *testing.T) {
	// Create some roles:
	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil)
	role, _ := auth.NewRole("square", ch.SetOf(t, "dull", "duller", "dullest"))
	assert.Equal(t, nil, auth.Save(role))
	role, _ = auth.NewRole("frood", ch.SetOf(t, "hoopy", "hoopier", "hoopiest"))
	assert.Equal(t, nil, auth.Save(role))

	user, _ := auth.NewUser("arthur", "password", ch.SetOf(t, "britain"))
	user.(*userImpl).setRolesSince(ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	goassert.DeepEquals(t, user.RoleNames(), ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	require.NoError(t, auth.Save(user))

	user2, err := auth.GetUser("arthur")
	assert.Equal(t, nil, err)
	log.Printf("Channels = %s", user2.Channels())
	goassert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf(t, "!", "britain"), 1))
	goassert.DeepEquals(t, user2.InheritedChannels(),
		ch.TimedSet{"!": ch.NewVbSimpleSequence(0x1), "britain": ch.NewVbSimpleSequence(0x1), "dull": ch.NewVbSimpleSequence(0x3), "duller": ch.NewVbSimpleSequence(0x3), "dullest": ch.NewVbSimpleSequence(0x3), "hoopy": ch.NewVbSimpleSequence(0x4), "hoopier": ch.NewVbSimpleSequence(0x4), "hoopiest": ch.NewVbSimpleSequence(0x4)})
	assert.True(t, user2.CanSeeChannel("britain"))
	assert.True(t, user2.CanSeeChannel("duller"))
	assert.True(t, user2.CanSeeChannel("hoopy"))
	assert.Equal(t, nil, user2.AuthorizeAllChannels(ch.SetOf(t, "britain", "dull", "hoopiest")))
}

func TestRegisterUser(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Register user based on name, email
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.RegisterNewUser("ValidName", "foo@example.com")
	require.NoError(t, err)
	assert.Equal(t, "ValidName", user.Name())
	assert.Equal(t, "foo@example.com", user.Email())

	// verify retrieval by username
	user, err = auth.GetUser("ValidName")
	require.NoError(t, err)
	assert.Equal(t, "ValidName", user.Name())

	// verify retrieval by email
	user, err = auth.GetUserByEmail("foo@example.com")
	require.NoError(t, err)
	assert.Equal(t, "ValidName", user.Name())

	// Register user based on email, retrieve based on username, email
	user, err = auth.RegisterNewUser("bar@example.com", "bar@example.com")
	require.NoError(t, err)
	assert.Equal(t, "bar@example.com", user.Name())
	assert.Equal(t, "bar@example.com", user.Email())

	user, err = auth.GetUser("UnknownName")
	require.NoError(t, err)
	assert.Equal(t, nil, user)

	user, err = auth.GetUserByEmail("bar@example.com")
	require.NoError(t, err)
	assert.Equal(t, "bar@example.com", user.Name())

	// Register user without an email address
	user, err = auth.RegisterNewUser("01234567890", "")
	require.NoError(t, err)
	assert.Equal(t, "01234567890", user.Name())
	assert.Equal(t, "", user.Email())
	// Get above user by username.
	user, err = auth.GetUser("01234567890")
	require.NoError(t, err)
	assert.Equal(t, "01234567890", user.Name())
	assert.Equal(t, "", user.Email())
	// Make sure we can't retrieve 01234567890 by supplying empty email.
	user, err = auth.GetUserByEmail("")
	require.NoError(t, err)
	assert.Equal(t, nil, user)

	// Try to register a user based on invalid email
	user, err = auth.RegisterNewUser("foo", "bar")
	require.NoError(t, err)
	assert.Equal(t, "foo", user.Name())
	assert.Equal(t, "", user.Email()) // skipped due to invalid email

}

func TestConcurrentUserWrites(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	username := "foo"
	password := "password"
	email := "foo@bar.org"

	// Modify the bcrypt cost to test rehashPassword properly below
	require.Error(t, SetBcryptCost(5))

	// Create user
	auth := NewAuthenticator(bucket, nil)
	user, _ := auth.NewUser(username, password, ch.SetOf(t, "123", "456"))
	user.SetExplicitRoles(ch.TimedSet{"role1": ch.NewVbSimpleSequence(1), "role2": ch.NewVbSimpleSequence(1)})
	createErr := auth.Save(user)
	if createErr != nil {
		t.Errorf("Error creating user: %v", createErr)
	}

	// Retrieve user to trigger initial calculation of roles, channels
	user, getErr := auth.GetUser(username)
	if getErr != nil {
		t.Errorf("Error retrieving user: %v", getErr)
	}

	require.NoError(t, SetBcryptCost(bcryptDefaultCost))
	// Reset bcryptCostChanged state after test runs
	defer func() {
		bcryptCostChanged = false
	}()

	var wg sync.WaitGroup
	wg.Add(4)
	// Update user email, password hash, and invalidate user channels, roles concurrently
	go func() {
		user, getErr := auth.GetUser(username)
		if getErr != nil {
			t.Errorf("Error retrieving user: %v", getErr)
		}
		if user == nil {
			t.Errorf("User is nil prior to invalidate channels, error: %v", getErr)
		}

		invalidateErr := auth.InvalidateChannels(user, 1)
		if invalidateErr != nil {
			t.Errorf("Error invalidating user's channels: %v", invalidateErr)
		}
		wg.Done()
	}()

	go func() {
		user, getErr := auth.GetUser(username)
		if getErr != nil {
			t.Errorf("Error retrieving user: %v", getErr)
		}
		if user == nil {
			t.Errorf("User is nil prior to email update, error: %v", getErr)
		}

		updateErr := auth.UpdateUserEmail(user, email)
		if updateErr != nil {
			t.Errorf("Error updating user email: %v", updateErr)
		}
		wg.Done()
	}()

	go func() {
		user, getErr := auth.GetUser(username)
		if getErr != nil {
			t.Errorf("Error retrieving user: %v", getErr)
		}
		if user == nil {
			t.Errorf("User is nil prior to invalidate roles, error: %v", getErr)
		}

		updateErr := auth.InvalidateRoles(user, 1)
		if updateErr != nil {
			t.Errorf("Error invalidating roles: %v", updateErr)
		}
		wg.Done()
	}()

	go func() {
		user, getErr := auth.GetUser(username)
		if getErr != nil {
			t.Errorf("Error retrieving user: %v", getErr)
		}
		if user == nil {
			t.Errorf("User is nil prior to invalidate roles, error: %v", getErr)
		}

		rehashErr := auth.rehashPassword(user, password)
		if rehashErr != nil {
			t.Errorf("Error rehashing password: %v", rehashErr)
		}
		wg.Done()
	}()

	wg.Wait()

	// Get the user, validate channels and email
	user, getErr = auth.GetUser(username)
	require.NoError(t, getErr)

	assert.Equal(t, email, user.Email())
	require.Len(t, user.Channels(), 3)
	require.Len(t, user.RoleNames(), 2)

	// Check the password hash bcrypt cost
	userImpl := user.(*userImpl)
	cost, _ := bcrypt.Cost(userImpl.PasswordHash_)
	assert.Equal(t, cost, bcryptDefaultCost)
}

func TestAuthenticateTrustedJWT(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket, nil)

	var callbackURLFunc OIDCCallbackURLFunc
	callbackURL := base.StringPtr("http://comcast:4984/_callback")
	providerGoogle := &OIDCProvider{
		Name:        "Google",
		ClientID:    "aud1",
		Issuer:      issuerGoogleAccounts,
		CallbackURL: callbackURL,
	}

	// Make an RSA signer for signing tokens
	signer, err := getRSASigner()
	require.NoError(t, err, "Failed to create RSA signer")

	t.Run("malformed token with bad header no payload", func(t *testing.T) {
		user, expiry, err := auth.AuthenticateTrustedJWT("DmBb9C5", providerGoogle, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("malformed token with bad header bad payload", func(t *testing.T) {
		user, expiry, err := auth.AuthenticateTrustedJWT("DmBb9C5.C#m7G#7", providerGoogle, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("malformed token with bad header bad base64 payload", func(t *testing.T) {
		token := "DmBb9C5." + ToBase64String(`{"unknown":"value"}`)
		user, expiry, err := auth.AuthenticateTrustedJWT(token, providerGoogle, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("token with issuer but no clientID config", func(t *testing.T) {
		builder := jwt.Signed(signer).Claims(jwt.Claims{Issuer: issuerGoogleAccounts})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		provider := &OIDCProvider{
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			CallbackURL: providerGoogle.CallbackURL}
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Error checking clientID config")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("issuer mismatch google provider valid token", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:                    true,
			CallbackURL:                 providerGoogle.CallbackURL,
			Name:                        providerGoogle.Name,
			Issuer:                      issuerGoogleAccounts,
			ClientID:                    "aud1",
			AllowUnsignedProviderTokens: true,
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccountsNoScheme, // Different issuer returned from Google OP but it is valid.
			Subject:  "sub0123456789",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}
		wantUsername, err := getOIDCUsername(provider, &Identity{Subject: claims.Subject})
		assert.NoError(t, err, "Error retrieving OpenID Connect username")
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.NoError(t, err, "Error authenticating with trusted JWT")
		assert.Equal(t, wantUsername, user.Name())
		assert.Equal(t, claims.Expiry.Time(), expiry)
	})

	t.Run("issuer mismatch google provider invalid token", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud1",
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   "invalid.issuer.google.com",
			Subject:  "sub0123456789",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Error verifying issuer claim from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("token with audience mismatch", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud4",
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   "invalid.issuer.google.com",
			Subject:  "sub0123456789",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"}, // aud4 doesn't exist in claims
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Error verifying audience claim from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("token with no audience claim", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud4",
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   "invalid.issuer.google.com",
			Subject:  "sub0123456789",
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Error verifying audience claim from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("authenticate with expired token", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud1",
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Subject:  "sub0123456789",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(-1 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Can't authenticate with expired token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("token with nbf (not before) claim", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:                    true,
			CallbackURL:                 providerGoogle.CallbackURL,
			Name:                        providerGoogle.Name,
			Issuer:                      issuerGoogleAccounts,
			ClientID:                    "aud1",
			AllowUnsignedProviderTokens: true,
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:        "id0123456789",
			Issuer:    issuerGoogleAccounts,
			Subject:   "sub0123456789",
			Audience:  jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Expiry:    jwt.NewNumericDate(time.Now().Add(1 * time.Minute)),
			NotBefore: jwt.NewNumericDate(time.Now().Add(1 * time.Minute)),
		}
		wantUsername, err := getOIDCUsername(provider, &Identity{Subject: claims.Subject})
		assert.NoError(t, err, "Error retrieving OpenID Connect username")
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.NoError(t, err, "Error authenticating with trusted token")
		assert.Equal(t, wantUsername, user.Name())
		assert.Equal(t, claims.Expiry.Time(), expiry)
	})

	t.Run("token with expired nbf (not before) claim", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud1",
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:        "id0123456789",
			Issuer:    issuerGoogleAccounts,
			Subject:   "sub0123456789",
			Audience:  jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Expiry:    jwt.NewNumericDate(time.Now().Add(1 * time.Minute)),
			NotBefore: jwt.NewNumericDate(time.Now().Add(2 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Token with expired nbf (not before) time")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("token with no subject claim", func(t *testing.T) {
		provider := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud1",
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.Error(t, err, "Token must contain valid subject claim")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
		assert.Equal(t, time.Time{}, expiry, "Expiry should be zero time instant")
	})

	t.Run("registered user with valid token and new email", func(t *testing.T) {
		wantUsername := strings.ToLower(providerGoogle.Name) + "_layla"
		wantUserEmail := "layla@example.com"
		wantUser, err := auth.RegisterNewUser(wantUsername, wantUserEmail)
		require.NoError(t, err, "User registration failure")
		assert.Equal(t, wantUsername, wantUser.Name())
		assert.Equal(t, wantUserEmail, wantUser.Email())

		provider := &OIDCProvider{
			Register:                    true,
			CallbackURL:                 providerGoogle.CallbackURL,
			Name:                        providerGoogle.Name,
			Issuer:                      issuerGoogleAccounts,
			ClientID:                    "aud1",
			UserPrefix:                  strings.ToLower(providerGoogle.Name),
			AllowUnsignedProviderTokens: true,
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Subject:  "layla",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}

		wantUserEmail = "layla@couchbase.com"
		claimEmail := map[string]interface{}{"email": wantUserEmail}
		builder := jwt.Signed(signer).Claims(claims).Claims(claimEmail)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.NoError(t, err, "Error authenticating with trusted JWT")
		assert.Equal(t, wantUsername, user.Name())
		assert.Equal(t, claims.Expiry.Time(), expiry)
		assert.Equal(t, wantUserEmail, user.Email())
	})

	t.Run("registered user with valid token and same email", func(t *testing.T) {
		wantUsername := strings.ToLower(providerGoogle.Name) + "_noah"
		wantUserEmail := "noah@example.com"
		wantUser, err := auth.RegisterNewUser(wantUsername, wantUserEmail)
		require.NoError(t, err, "User registration failure")
		assert.Equal(t, wantUsername, wantUser.Name())
		assert.Equal(t, wantUserEmail, wantUser.Email())
		provider := &OIDCProvider{
			Register:                    true,
			CallbackURL:                 providerGoogle.CallbackURL,
			Name:                        providerGoogle.Name,
			Issuer:                      issuerGoogleAccounts,
			ClientID:                    "aud1",
			UserPrefix:                  strings.ToLower(providerGoogle.Name),
			AllowUnsignedProviderTokens: true,
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Subject:  "noah",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}

		claimEmail := map[string]interface{}{"email": wantUserEmail}
		builder := jwt.Signed(signer).Claims(claims).Claims(claimEmail)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.NoError(t, err, "Error authenticating with trusted JWT")
		assert.Equal(t, wantUsername, user.Name())
		assert.Equal(t, claims.Expiry.Time(), expiry)
		assert.Equal(t, wantUserEmail, user.Email())
	})

	t.Run("registered user with valid token and invalid email", func(t *testing.T) {
		wantUsername := strings.ToLower(providerGoogle.Name) + "_emily"
		wantUserEmail := "emily@example.com"
		wantUser, err := auth.RegisterNewUser(wantUsername, wantUserEmail)
		require.NoError(t, err, "User registration failure")
		assert.Equal(t, wantUsername, wantUser.Name())
		assert.Equal(t, wantUserEmail, wantUser.Email())

		provider := &OIDCProvider{
			Register:                    true,
			CallbackURL:                 providerGoogle.CallbackURL,
			Name:                        providerGoogle.Name,
			Issuer:                      issuerGoogleAccounts,
			ClientID:                    "aud1",
			UserPrefix:                  strings.ToLower(providerGoogle.Name),
			AllowUnsignedProviderTokens: true,
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Subject:  "emily",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}

		claimEmail := map[string]interface{}{"email": "emily@"}
		builder := jwt.Signed(signer).Claims(claims).Claims(claimEmail)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.NoError(t, err, "Error authenticating with trusted JWT")
		assert.Equal(t, wantUsername, user.Name())
		assert.Equal(t, claims.Expiry.Time(), expiry)
		assert.Equal(t, wantUserEmail, user.Email())
	})

	t.Run("new user with valid token and invalid email", func(t *testing.T) {
		wantUsername := strings.ToLower(providerGoogle.Name) + "_layla"
		provider := &OIDCProvider{
			Register:                    true,
			CallbackURL:                 providerGoogle.CallbackURL,
			Name:                        providerGoogle.Name,
			Issuer:                      issuerGoogleAccounts,
			ClientID:                    "aud1",
			UserPrefix:                  strings.ToLower(providerGoogle.Name),
			AllowUnsignedProviderTokens: true,
		}
		err = provider.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Subject:  "layla",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}

		claimEmail := map[string]interface{}{"email": "layla@"}
		builder := jwt.Signed(signer).Claims(claims).Claims(claimEmail)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, expiry, err := auth.AuthenticateTrustedJWT(token, provider, callbackURLFunc)
		assert.NoError(t, err, "Error authenticating with trusted JWT")
		assert.Equal(t, wantUsername, user.Name())
		assert.Empty(t, "", user.Email(), "Should skip updating invalid email from token")
		assert.Equal(t, claims.Expiry.Time(), expiry)
	})

}

func TestGetPrincipal(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket, nil)

	const (
		channelRead       = "read"
		channelWrite      = "write"
		channelExecute    = "execute"
		channelCreate     = "create"
		channelUpdate     = "update"
		channelDelete     = "delete"
		roleRoot          = "root"
		roleUser          = "user"
		username          = "batman"
		password          = "YmF0bWFu"
		accessViewKeyRoot = "role:root"
		accessViewKeyUser = "role:user"
	)

	// Create a new role named root with access to read, write and execute channels
	role, _ := auth.NewRole(roleRoot, ch.SetOf(t, channelRead, channelWrite, channelExecute))
	assert.Equal(t, nil, auth.Save(role))

	// Create another role named user with access to read and execute channels; no write channel access.
	role, _ = auth.NewRole(roleUser, ch.SetOf(t, channelRead, channelExecute))
	assert.Equal(t, nil, auth.Save(role))

	// Get the principal against root role and verify the details.
	principal, err := auth.GetPrincipal(roleRoot, false)
	assert.NoError(t, err)
	assert.Equal(t, roleRoot, principal.Name())
	assert.Equal(t, accessViewKeyRoot, principal.accessViewKey())
	assert.True(t, principal.CanSeeChannel(channelRead))
	assert.True(t, principal.CanSeeChannel(channelWrite))
	assert.True(t, principal.CanSeeChannel(channelExecute))

	// Get the principal against user role and verify the details.
	principal, err = auth.GetPrincipal(roleUser, false)
	assert.NoError(t, err)
	assert.Equal(t, roleUser, principal.Name())
	assert.Equal(t, accessViewKeyUser, principal.accessViewKey())
	assert.True(t, principal.CanSeeChannel(channelRead))
	assert.False(t, principal.CanSeeChannel(channelWrite))
	assert.True(t, principal.CanSeeChannel(channelExecute))

	// Create a new user with new set of channels and assign user role to the user.
	user, err := auth.NewUser(username, password, ch.SetOf(
		t, channelCreate, channelRead, channelUpdate, channelDelete))
	user.(*userImpl).setRolesSince(ch.TimedSet{roleUser: ch.NewVbSimpleSequence(0x3)})
	require.NoError(t, auth.Save(user))

	// Get the principal of user and verify the details
	principal, err = auth.GetPrincipal(username, true)
	assert.NoError(t, err)
	assert.Equal(t, username, principal.Name())
	assert.Equal(t, username, principal.accessViewKey())
	assert.True(t, principal.CanSeeChannel(channelRead))
	assert.False(t, principal.CanSeeChannel(channelWrite))
	assert.True(t, principal.CanSeeChannel(channelExecute))
	assert.True(t, principal.CanSeeChannel(channelCreate))
	assert.True(t, principal.CanSeeChannel(channelUpdate))
	assert.True(t, principal.CanSeeChannel(channelDelete))
}

// Encode the given string to base64.
func ToBase64String(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key))
}

func TestAuthenticateUntrustedJWT(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket, nil)

	issuerFacebookAccounts := "https://accounts.facebook.com"
	issuerAmazonAccounts := "https://accounts.amazon.com"
	callbackURL := base.StringPtr("http://comcast:4984/_callback")
	var callbackURLFunc OIDCCallbackURLFunc
	providerGoogle := &OIDCProvider{
		Name:        "Google",
		ClientID:    "aud1",
		Issuer:      issuerGoogleAccounts,
		CallbackURL: callbackURL,
	}
	providerFacebook := &OIDCProvider{
		Name:        "Facebook",
		ClientID:    "aud1",
		Issuer:      issuerFacebookAccounts,
		CallbackURL: callbackURL,
	}

	// Make an RSA signer for signing tokens
	signer, err := getRSASigner()
	require.NoError(t, err, "Failed to create RSA signer")

	t.Run("no provider malformed token with bad header no payload", func(t *testing.T) {
		var providers OIDCProviderMap
		user, err := auth.AuthenticateUntrustedJWT("DmBb9C5", providers, callbackURLFunc)
		assert.Error(t, err, "No provider found to authenticate token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("single provider malformed token with bad header no payload", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle}
		user, err := auth.AuthenticateUntrustedJWT("DmBb9C5", providers, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers malformed token with bad header no payload", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		user, err := auth.AuthenticateUntrustedJWT("DmBb9C5", providers, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers malformed token with bad header bad payload", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		user, err := auth.AuthenticateUntrustedJWT("DmBb9C5.C#m7G#7", providers, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers malformed token with bad header bad base64 payload", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		token := "DmBb9C5." + ToBase64String(`{"unknown":"value"}`)
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		assert.Error(t, err, "Error parsing malformed token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers token with no issuer no audience", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		builder := jwt.Signed(signer).Claims(jwt.Claims{})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		require.Error(t, err, "Error getting issuer and audience from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers token with issuer no audience", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		builder := jwt.Signed(signer).Claims(jwt.Claims{Issuer: issuerGoogleAccounts})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		require.Error(t, err, "Error getting issuer and audience from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers token with issuer empty audience", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		builder := jwt.Signed(signer).Claims(jwt.Claims{Issuer: issuerGoogleAccounts, Audience: jwt.Audience{}})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		require.Error(t, err, "Error getting issuer and audience from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers token with audience no issuer", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		builder := jwt.Signed(signer).Claims(jwt.Claims{Audience: jwt.Audience{"aud1", "aud2", "aud3"}})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		require.Error(t, err, "Error getting issuer and audience from token")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers token with issuer mismatch", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		builder := jwt.Signed(signer).Claims(jwt.Claims{Issuer: issuerAmazonAccounts, Audience: jwt.Audience{"aud1"}})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		require.Error(t, err, "No provider found against the configured issuer")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers token with audience mismatch", func(t *testing.T) {
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		builder := jwt.Signed(signer).Claims(jwt.Claims{Issuer: issuerGoogleAccounts, Audience: jwt.Audience{"aud2"}})
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		require.Error(t, err, "No provider found against the configured issuer")
		assert.Nil(t, user, "User shouldn't be created or retrieved")
	})

	t.Run("multiple providers with valid token signature verification failure", func(t *testing.T) {
		providerGoogle := &OIDCProvider{
			Register:    true,
			CallbackURL: providerGoogle.CallbackURL,
			Name:        providerGoogle.Name,
			Issuer:      issuerGoogleAccounts,
			ClientID:    "aud1",
		}
		providers := OIDCProviderMap{providerGoogle.Name: providerGoogle, providerFacebook.Name: providerFacebook}
		err = providerGoogle.InitUserPrefix()
		assert.NoError(t, err, "Error initializing user prefix")
		claims := jwt.Claims{
			ID:       "id0123456789",
			Issuer:   issuerGoogleAccounts,
			Subject:  "sub0123456789",
			Audience: jwt.Audience{"aud1", "aud2", "aud3"},
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Expiry:   jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}
		builder := jwt.Signed(signer).Claims(claims)
		token, err := builder.CompactSerialize()
		require.NoError(t, err, "Error serializing token using compact serialization format")
		user, err := auth.AuthenticateUntrustedJWT(token, providers, callbackURLFunc)
		assert.Error(t, err, "Error authenticating with trusted JWT")
		assert.Nil(t, user, "User shouldn't be returned without signature verification")
	})
}

type mockComputerV2 struct {
	channels     map[string]ch.TimedSet
	roles        map[string]ch.TimedSet
	roleChannels map[string]ch.TimedSet
	err          error
}

func (m mockComputerV2) ComputeChannelsForPrincipal(principal Principal) (ch.TimedSet, error) {
	if user, ok := principal.(User); ok {
		return m.channels[user.Name()].Copy(), nil
	} else {
		return m.roleChannels[principal.Name()].Copy(), nil
	}
}

func (m mockComputerV2) ComputeRolesForUser(user User) (ch.TimedSet, error) {
	return m.roles[user.Name()].Copy(), nil
}

func (m mockComputerV2) addRoleChannels(t *testing.T, auth *Authenticator, princ Principal, roleName, channelName string, invalSeq uint64) {
	if _, ok := m.roleChannels[roleName]; !ok {
		m.roleChannels[roleName] = ch.TimedSet{}
	}

	m.roleChannels[roleName].Add(ch.AtSequence(ch.SetOf(t, channelName), invalSeq))
	err := auth.InvalidateChannels(princ, invalSeq)
	assert.NoError(t, err)
	err = auth.Save(princ)
	assert.NoError(t, err)
}

func (m mockComputerV2) removeRoleChannel(t *testing.T, auth *Authenticator, princ Principal, roleName, channelName string, invalSeq uint64) {
	delete(m.roleChannels[roleName], channelName)
	err := auth.InvalidateChannels(princ, invalSeq)
	assert.NoError(t, err)
	err = auth.Save(princ)
	assert.NoError(t, err)
}

func (m mockComputerV2) addRole(t *testing.T, auth *Authenticator, user User, userName, roleName string, invalSeq uint64) {
	if _, ok := m.roles[userName]; !ok {
		m.roles[userName] = ch.TimedSet{}
	}

	m.roles[userName].Add(ch.AtSequence(ch.SetOf(t, roleName), invalSeq))
	err := auth.InvalidateRoles(user, invalSeq)
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)
}

func (m mockComputerV2) removeRole(t *testing.T, auth *Authenticator, user User, userName, roleName string, invalSeq uint64) {
	delete(m.roles[userName], roleName)
	err := auth.InvalidateRoles(user, invalSeq)
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)
}

// =======================================================================================================
// The below 'TestRevocationScenario' tests refer to scenarios in the following Google Sheet
// https://docs.google.com/spreadsheets/d/1pTLyJqrSdde-dAxDfMkKGXi0ttWF4GVCOBFJg7hRY0U/edit?usp=sharing
// =======================================================================================================

func TestRevocationScenario1(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	// Get Principals / Rebuild Seq 40
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 75, EndSeq: 85}, channelHistory.Entries[0])
}

func TestRevocationScenario2(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)

	// Get Principals / Rebuild Seq 50
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, TimeSetHistoryEntry{Seq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok = aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[1])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 75, EndSeq: 85}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario3(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	// Rebuild seq 60
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 20, EndSeq: 45}, userRoleHistory.Entries[0])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(user.ChannelHistory()))

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	// Rebuild seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 1, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 1, len(fooPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(user.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))

	userRoleHistory, ok = aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[1])

	channelHistory, ok = fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)

	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, TimeSetHistoryEntry{Seq: 75, EndSeq: 85}, channelHistory.Entries[1])

	assert.Equal(t, 0, len(user.ChannelHistory()))
}

func TestRevocationScenario4(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)

	// Get Principals / Rebuild Seq 70
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 1, len(fooPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	channelHistory, ok = fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, TimeSetHistoryEntry{Seq: 75, EndSeq: 85}, channelHistory.Entries[1])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario5(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 75, EndSeq: 85}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario6(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)

	// Rebuild seq 90
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 100
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])

	channelHistory, ok = fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario7(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllChannels())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 100
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 20, EndSeq: 45}, userRoleHistory.Entries[0])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))

	assert.Equal(t, 1, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 1, len(fooPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario8(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)

	// Get Principals / Rebuild Seq 50
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario9(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	// Get Principals / Rebuild Seq 60
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
}

func TestRevocationScenario10(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)

	// Get Principals / Rebuild Seq 70
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
}

func TestRevocationScenario11(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))

	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 75, EndSeq: 85}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
}

func TestRevocationScenario12(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)

	// Get Principals / Rebuild Seq 90
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, TimeSetHistoryEntry{Seq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
}

func TestRevocationScenario13(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	getPrincipals := func(auth *Authenticator) (*userImpl, Principal) {
		principal, err := auth.GetPrincipal("alice", true)
		assert.NoError(t, err)
		userPrincipal, ok := principal.(*userImpl)
		assert.True(t, ok)
		rolePrincipal, err := auth.GetPrincipal("foo", false)
		assert.NoError(t, err)
		return userPrincipal, rolePrincipal
	}

	auth := NewAuthenticator(testBucket, &testMockComputer)

	user, err := auth.NewUser("alice", "password", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", ch.SetOf(t))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	alicePrincipal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	aliceUserPrincipal, ok := alicePrincipal.(*userImpl)
	assert.True(t, ok)
	fooPrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)

	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, aliceUserPrincipal, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, fooPrincipal, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, fooPrincipal, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, aliceUserPrincipal, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 100
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
}

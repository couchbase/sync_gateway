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
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/coreos/go-oidc/jose"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	bucket := gTestBucket.Bucket
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	badEmails := []string{"", "foo", "foo@", "@bar", "foo @bar", "foo@.bar"}
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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
	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, err := auth.GetUser("noSuchUser")
	assert.Equal(t, nil, err)
	assert.True(t, user == nil)
	user, err = auth.GetUserByEmail("noreply@example.com")
	assert.Equal(t, nil, err)
	assert.True(t, user == nil)
}

func TestGetMissingRole(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	role, err := auth.GetRole("noSuchRole")
	assert.Equal(t, nil, err)
	assert.True(t, role == nil)
}

func TestGetGuestUser(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, err := auth.GetUser("")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, user, auth.defaultGuestUser())

}

func TestSaveUsers(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf(t, "test"))
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, user2, user)
}

func TestSaveRoles(t *testing.T) {
	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	computer := mockComputer{channels: ch.AtSequence(ch.SetOf(t, "derived1", "derived2"), 1)}
	auth := NewAuthenticator(gTestBucket.Bucket, &computer)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf(t, "explicit1"))
	user.setChannels(nil)
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	goassert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf(t, "explicit1", "derived1", "derived2", "!"), 1))
}

func TestRebuildRoleChannels(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	computer := mockComputer{roleChannels: ch.AtSequence(ch.SetOf(t, "derived1", "derived2"), 1)}
	auth := NewAuthenticator(gTestBucket.Bucket, &computer)
	role, err := auth.NewRole("testRole", ch.SetOf(t, "explicit1"))
	assert.NoError(t, err)
	err = auth.InvalidateChannels(role)
	assert.Equal(t, nil, err)

	role2, err := auth.GetRole("testRole")
	assert.Equal(t, nil, err)
	assert.Equal(t, ch.AtSequence(ch.SetOf(t, "explicit1", "derived1", "derived2", "!"), 1), role2.Channels())
}

func TestRebuildChannelsError(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	computer := mockComputer{}
	auth := NewAuthenticator(gTestBucket.Bucket, &computer)
	role, err := auth.NewRole("testRole2", ch.SetOf(t, "explicit1"))
	assert.NoError(t, err)
	assert.Equal(t, nil, auth.InvalidateChannels(role))

	computer.err = errors.New("I'm sorry, Dave.")

	role2, err := auth.GetRole("testRole2")
	assert.Nil(t, role2)
	assert.Equal(t, computer.err, err)
}

func TestRebuildUserRoles(t *testing.T) {

	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	computer := mockComputer{roles: ch.AtSequence(base.SetOf("role1", "role2"), 3)}
	auth := NewAuthenticator(gTestBucket.Bucket, &computer)
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
	err = auth.InvalidateRoles(user1)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	expected = ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	goassert.DeepEquals(t, user2.RoleNames(), expected)
}

func TestRoleInheritance(t *testing.T) {
	// Create some roles:
	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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
	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()

	// Register user based on name, email
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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
	gTestBucket := base.GetTestBucket(t)
	defer gTestBucket.Close()

	username := "foo"
	password := "password"
	email := "foo@bar.org"

	// Modify the bcrypt cost to test rehashPassword properly below
	SetBcryptCost(5)

	// Create user
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
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

	SetBcryptCost(bcryptDefaultCost)
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

		invalidateErr := auth.InvalidateChannels(user)
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

		updateErr := auth.InvalidateRoles(user)
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
	if getErr != nil {
		t.Errorf("Error retrieving user: %v", getErr)
	}

	assert.Equal(t, email, user.Email())
	assert.Equal(t, 3, len(user.Channels()))
	assert.Equal(t, 2, len(user.RoleNames()))

	// Check the password hash bcrypt cost
	userImpl := user.(*userImpl)
	cost, _ := bcrypt.Cost(userImpl.PasswordHash_)
	assert.Equal(t, cost, bcryptDefaultCost)
}

func TestAuthenticateTrustedJWTWithBadToken(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	clientID := "comcast"
	callbackURL := "http://comcast:4984/_callback"

	provider := &OIDCProvider{
		ClientID:    &clientID,
		Issuer:      "https://accounts.google.com",
		CallbackURL: &callbackURL,
	}

	user, jws, err := auth.AuthenticateTrustedJWT(GetBadBearerToken(), provider, nil)
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

func TestAuthenticateTrustedJWTWithBadClaim(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	clientID := "comcast"
	callbackURL := "http://comcast:4984/_callback"

	provider := &OIDCProvider{
		ClientID:    &clientID,
		Issuer:      "https://accounts.google.com",
		CallbackURL: &callbackURL,
	}

	user, jws, err := auth.AuthenticateTrustedJWT(TokenWithBadClaim, provider, nil)
	log.Printf("%v", err.Error())
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

func TestAuthenticateTrustedJWT(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	clientID := "comcast"
	callbackURL := "http://comcast:4984/_callback"

	provider := &OIDCProvider{
		ClientID:    &clientID,
		Issuer:      "https://accounts.google.com",
		CallbackURL: &callbackURL,
	}

	user, jws, err := auth.AuthenticateTrustedJWT(Token, provider, nil)

	assert.NoError(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

func TestAuthenticateJWTWithNoClaim(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	// Parse the mocked JWS token.
	jws, err := jose.ParseJWS(Token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(Token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader: jws.RawHeader,
		Header:    jws.Header,
		Signature: jws.Signature}

	clientID := "comcast"
	callbackURL := "http://comcast:4984/_callback"

	provider := &OIDCProvider{
		ClientID:    &clientID,
		Issuer:      "https://accounts.google.com",
		CallbackURL: &callbackURL,
	}
	usr, jwtNew, err := auth.authenticateJWT(jwt, provider)
	assert.Error(t, err)
	assert.Nil(t, usr)
	assert.Equal(t, jwt, jwtNew)
}

func TestGetPrincipal(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

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

// Get a random bad bearer token.
func GetBadBearerToken() string {
	header := ToBase64String(base.GenerateRandomSecret())
	payload := ToBase64String(base.GenerateRandomSecret())
	signature := ToBase64String(base.GenerateRandomSecret())
	return fmt.Sprintf("%s.%s.%s", header, payload, signature)
}

// Check untrusted JWT authentication method with a bad bearer token.
// The step which parse JWT (needed to determine issuer/provider) should fail.
func TestAuthenticateUnTrustedJWTWithBadToken(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	googleClientID := "google"
	googleCallbackURL := "https://accounts.google.com/_callback"

	const (
		googleName   = "Google LLC"
		googleIssuer = "https://accounts.google.com"
	)

	providerGoogle := &OIDCProvider{
		Name:        googleName,
		ClientID:    &googleClientID,
		Issuer:      googleIssuer,
		CallbackURL: &googleCallbackURL,
	}

	providers := map[string]*OIDCProvider{
		googleName: providerGoogle,
	}

	user, jws, err := auth.AuthenticateUntrustedJWT(GetBadBearerToken(), providers, nil)
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

func TestAuthenticateUnTrustedJWTWithNoIssAud(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	googleClientID := "google"
	googleCallbackURL := "https://accounts.google.com/_callback"

	const (
		googleName   = "Google LLC"
		googleIssuer = "https://accounts.google.com"
	)

	providerGoogle := &OIDCProvider{
		Name:        googleName,
		ClientID:    &googleClientID,
		Issuer:      googleIssuer,
		CallbackURL: &googleCallbackURL,
	}

	providers := map[string]*OIDCProvider{
		googleName: providerGoogle,
	}

	user, jws, err := auth.AuthenticateUntrustedJWT(TokenWithNoIssuer, providers, nil)
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

func TestAuthenticateUnTrustedJWTNoProviderForIssuer(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	// Different clientID from token; "aud": ["ebay", "comcast", "linkedin"]
	clientID := "google"
	callbackURL := "https://accounts.google.com/_callback"

	const (
		googleName   = "Google LLC"
		googleIssuer = "https://accounts.google.com"
	)

	providerGoogle := &OIDCProvider{
		Name:        googleName,
		ClientID:    &clientID,
		Issuer:      googleIssuer,
		CallbackURL: &callbackURL,
	}

	providers := map[string]*OIDCProvider{
		googleName: providerGoogle,
	}

	user, jws, err := auth.AuthenticateUntrustedJWT(Token, providers, nil)
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

func TestAuthenticateUnTrustedJWTWithNoClient(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil)

	clientID := "comcast"
	callbackURL := "https://accounts.google.com/_callback"

	const (
		googleName   = "Google LLC"
		googleIssuer = "https://accounts.google.com"
	)

	providerGoogle := &OIDCProvider{
		Name:        googleName,
		ClientID:    &clientID,
		Issuer:      googleIssuer,
		CallbackURL: &callbackURL,
	}

	providers := map[string]*OIDCProvider{
		googleName: providerGoogle,
	}

	user, jws, err := auth.AuthenticateUntrustedJWT(Token, providers, nil)
	assert.Error(t, err)
	assert.Nil(t, user)
	assert.NotNil(t, jws)
}

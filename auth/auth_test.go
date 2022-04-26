//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	user, err := auth.NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	user, _ := auth.NewUser("me", "letmein", ch.SetOf(t, "me", "public"))
	require.NoError(t, user.SetEmail("foo@example.com"))
	encoded, _ := base.JSONMarshal(user)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled User as: %s", encoded)

	resu := &userImpl{auth: auth}
	err := base.JSONUnmarshal(encoded, resu)
	assert.True(t, err == nil)
	assert.Equal(t, user.Name(), resu.Name())
	assert.Equal(t, user.Email(), resu.Email())
	assert.Equal(t, user.ExplicitChannels(), resu.ExplicitChannels())
	assert.True(t, resu.Authenticate("letmein"))
	assert.False(t, resu.Authenticate("123456"))
}

func TestSerializeRole(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	role, _ := auth.NewRole("froods", ch.SetOf(t, "hoopy", "public"))
	encoded, _ := base.JSONMarshal(role)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled Role as: %s", encoded)
	elor := &roleImpl{}
	err := base.JSONUnmarshal(encoded, elor)

	assert.True(t, err == nil)
	assert.Equal(t, role.Name(), elor.Name())
	assert.Equal(t, role.ExplicitChannels(), elor.ExplicitChannels())
}

func TestUserAccess(t *testing.T) {

	// User with no access:
	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	user, _ := auth.NewUser("foo", "password", nil)
	assert.Equal(t, ch.SetOf(t, "!"), user.ExpandWildCardChannel(ch.SetOf(t, "*")))
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
	assert.Equal(t, ch.SetOf(t, "x"), user.ExpandWildCardChannel(ch.SetOf(t, "*")))
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
	assert.Equal(t, ch.SetOf(t, "x", "z"), user.ExpandWildCardChannel(ch.SetOf(t, "*")))
	assert.Equal(t, ch.SetOf(t, "x"), user.ExpandWildCardChannel(ch.SetOf(t, "x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)

	// User with access to two channels:
	user.setChannels(ch.AtSequence(ch.SetOf(t, "x", "z"), 1))
	assert.Equal(t, ch.SetOf(t, "x", "z"), user.ExpandWildCardChannel(ch.SetOf(t, "*")))
	assert.Equal(t, ch.SetOf(t, "x"), user.ExpandWildCardChannel(ch.SetOf(t, "x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)

	user.setChannels(ch.AtSequence(ch.SetOf(t, "x", "y"), 1))
	assert.Equal(t, ch.SetOf(t, "x", "y"), user.ExpandWildCardChannel(ch.SetOf(t, "*")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t)))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf(t, "x", "y", "z")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf(t, "*")) == nil)

	// User with wildcard access:
	user.setChannels(ch.AtSequence(ch.SetOf(t, "*", "q"), 1))
	assert.Equal(t, ch.SetOf(t, "*", "q"), user.ExpandWildCardChannel(ch.SetOf(t, "*")))
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
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
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	role, err := auth.GetRole("noSuchRole")
	assert.Equal(t, nil, err)
	assert.True(t, role == nil)
}

func TestGetGuestUser(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	user, err := auth.GetUser("")
	assert.Equal(t, nil, err)
	assert.Equal(t, auth.defaultGuestUser(), user)
}

func TestSaveUsers(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	user, _ := auth.NewUser("testUser", "password", ch.SetOf(t, "test"))
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	assert.Equal(t, user, user2)
}

func TestSaveRoles(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	role, _ := auth.NewRole("testRole", ch.SetOf(t, "test"))
	err := auth.Save(role)
	assert.Equal(t, nil, err)

	role2, err := auth.GetRole("testRole")
	assert.Equal(t, nil, err)
	assert.Equal(t, role, role2)
}

type mockComputer struct {
	channels     ch.TimedSet
	roles        ch.TimedSet
	roleChannels ch.TimedSet
	err          error
}

func (self *mockComputer) ComputeChannelsForPrincipal(ctx context.Context, p Principal) (ch.TimedSet, error) {
	switch p.(type) {
	case User:
		return self.channels, self.err
	case Role:
		return self.roleChannels, self.err
	default:
		return nil, self.err
	}
}

func (self *mockComputer) ComputeRolesForUser(context.Context, User) (ch.TimedSet, error) {
	return self.roles, self.err
}

func (self *mockComputer) UseGlobalSequence() bool {
	return true
}

func TestRebuildUserChannels(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{channels: ch.AtSequence(ch.SetOf(t, "derived1", "derived2"), 1)}
	auth := NewAuthenticator(bucket, &computer, DefaultAuthenticatorOptions())
	user, _ := auth.NewUser("testUser", "password", ch.SetOf(t, "explicit1"))
	err := auth.Save(user)
	assert.NoError(t, err)

	err = auth.InvalidateChannels("testUser", true, 2)
	assert.NoError(t, err)

	user2, err := auth.GetUser("testUser")
	assert.NoError(t, err)
	assert.Equal(t, ch.AtSequence(ch.SetOf(t, "explicit1", "derived1", "derived2", "!"), 1), user2.Channels())
}

func TestRebuildRoleChannels(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{roleChannels: ch.AtSequence(ch.SetOf(t, "derived1", "derived2"), 1)}
	auth := NewAuthenticator(bucket, &computer, DefaultAuthenticatorOptions())
	role, err := auth.NewRole("testRole", ch.SetOf(t, "explicit1"))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	err = auth.InvalidateChannels("testRole", false, 1)
	assert.Equal(t, nil, err)

	role2, err := auth.GetRole("testRole")
	assert.Equal(t, nil, err)
	assert.Equal(t, ch.AtSequence(ch.SetOf(t, "explicit1", "derived1", "derived2", "!"), 1), role2.Channels())
}

func TestRebuildChannelsError(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{}
	auth := NewAuthenticator(bucket, &computer, DefaultAuthenticatorOptions())
	role, err := auth.NewRole("testRole2", ch.SetOf(t, "explicit1"))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	assert.Equal(t, nil, auth.InvalidateChannels("testRole2", false, 1))

	computer.err = errors.New("I'm sorry, Dave.")

	role2, err := auth.GetRole("testRole2")
	assert.Nil(t, role2)
	assert.Equal(t, computer.err, err)
}

func TestRebuildUserRoles(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	computer := mockComputer{roles: ch.AtSequence(base.SetOf("role1", "role2"), 3)}
	auth := NewAuthenticator(bucket, &computer, DefaultAuthenticatorOptions())
	user, _ := auth.NewUser("testUser", "letmein", nil)
	user.SetExplicitRoles(ch.TimedSet{"role3": ch.NewVbSimpleSequence(1), "role1": ch.NewVbSimpleSequence(1)}, 1)
	err := auth.Save(user)
	assert.Equal(t, nil, err)

	// Retrieve the user, triggers initial build of roles
	user1, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	expected := ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	assert.Equal(t, expected, user1.RoleNames())

	// Invalidate the roles, triggers rebuild
	err = auth.InvalidateRoles("testUser", 1)
	assert.Equal(t, nil, err)

	user2, err := auth.GetUser("testUser")
	assert.Equal(t, nil, err)
	expected = ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	assert.Equal(t, expected, user2.RoleNames())
}

func TestRoleInheritance(t *testing.T) {
	// Create some roles:
	bucket := base.GetTestBucket(t)
	defer bucket.Close()
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	role, _ := auth.NewRole("square", ch.SetOf(t, "dull", "duller", "dullest"))
	assert.Equal(t, nil, auth.Save(role))
	role, _ = auth.NewRole("frood", ch.SetOf(t, "hoopy", "hoopier", "hoopiest"))
	assert.Equal(t, nil, auth.Save(role))

	user, _ := auth.NewUser("arthur", "password", ch.SetOf(t, "britain"))
	user.(*userImpl).setRolesSince(ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	assert.Equal(t, ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)}, user.RoleNames())
	require.NoError(t, auth.Save(user))

	user2, err := auth.GetUser("arthur")
	assert.Equal(t, nil, err)
	log.Printf("Channels = %s", user2.Channels())
	assert.Equal(t, ch.AtSequence(ch.SetOf(t, "!", "britain"), 1), user2.Channels())
	assert.Equal(t, ch.TimedSet{"!": ch.NewVbSimpleSequence(0x1), "britain": ch.NewVbSimpleSequence(0x1), "dull": ch.NewVbSimpleSequence(0x3), "duller": ch.NewVbSimpleSequence(0x3), "dullest": ch.NewVbSimpleSequence(0x3), "hoopy": ch.NewVbSimpleSequence(0x4), "hoopier": ch.NewVbSimpleSequence(0x4), "hoopiest": ch.NewVbSimpleSequence(0x4)}, user2.InheritedChannels())

	assert.True(t, user2.CanSeeChannel("britain"))
	assert.True(t, user2.CanSeeChannel("duller"))
	assert.True(t, user2.CanSeeChannel("hoopy"))
	assert.Equal(t, nil, user2.AuthorizeAllChannels(ch.SetOf(t, "britain", "dull", "hoopiest")))
}

func TestRegisterUser(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Register user based on name, email
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
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

func TestCASUpdatePrincipal(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Create user
	username := "foo"
	password := "password"
	email := "foo@bar.org"

	// Create user
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())

	// Modify the bcrypt cost to test rehashPassword properly below
	require.Error(t, auth.SetBcryptCost(5))

	user, _ := auth.NewUser(username, password, ch.SetOf(t, "123", "456"))
	user.SetExplicitRoles(ch.TimedSet{"role1": ch.NewVbSimpleSequence(1), "role2": ch.NewVbSimpleSequence(1)}, 1)
	createErr := auth.Save(user)
	if createErr != nil {
		t.Errorf("Error creating user: %v", createErr)
	}
	user, getErr := auth.GetUser(username)
	if getErr != nil {
		t.Errorf("Error retrieving user: %v", getErr)
	}
	if user == nil {
		t.Errorf("User is nil prior to invalidate channels, error: %v", getErr)
	}

	// updateEmailWithConflictCallback causes CAS failure three times
	updateCount := uint64(0)
	updateEmailWithConflictCallback := func(currentPrincipal Principal) (updatedPrincipal Principal, err error) {
		currentUser, ok := currentPrincipal.(User)
		if !ok {
			return nil, base.ErrUpdateCancel
		}

		log.Printf("attempting update with CAS:%v", currentUser.Cas())
		if updateCount < 3 {
			// Update principal externally to trigger CAS error three times
			concurrentUser, err := auth.GetUser(username)
			assert.NoError(t, err)
			log.Printf("setting explicit channels to %v", updateCount)
			concurrentUser.SetExplicitChannels(ch.TimedSet{"ch1": ch.NewVbSimpleSequence(updateCount)}, updateCount)
			updateErr := auth.Save(concurrentUser)
			assert.NoError(t, updateErr)

			updatedConcurrentUser, err := auth.GetUser(username)
			assert.NoError(t, err)
			log.Printf("Forcing cas failure, updated CAS: %v", updatedConcurrentUser.Cas())

			updateCount++
		}

		err = currentUser.SetEmail(email)
		if err != nil {
			return nil, err
		}
		return currentUser, nil
	}

	updateErr := auth.casUpdatePrincipal(user, updateEmailWithConflictCallback)
	assert.NoError(t, updateErr)

	updatedUser, err := auth.GetUser(username)
	assert.NoError(t, err)
	assert.Equal(t, email, updatedUser.Email())
}

func TestConcurrentUserWrites(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	username := "foo"
	password := "password"
	email := "foo@bar.org"

	// Create user
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())

	// Modify the bcrypt cost to test rehashPassword properly below
	require.Error(t, auth.SetBcryptCost(5))

	user, _ := auth.NewUser(username, password, ch.SetOf(t, "123", "456"))
	user.SetExplicitRoles(ch.TimedSet{"role1": ch.NewVbSimpleSequence(1), "role2": ch.NewVbSimpleSequence(1)}, 1)
	createErr := auth.Save(user)
	if createErr != nil {
		t.Errorf("Error creating user: %v", createErr)
	}

	// Retrieve user to trigger initial calculation of roles, channels
	user, getErr := auth.GetUser(username)
	if getErr != nil {
		t.Errorf("Error retrieving user: %v", getErr)
	}

	require.NoError(t, auth.SetBcryptCost(DefaultBcryptCost))
	// Reset bcryptCostChanged state after test runs
	defer func() {
		auth.bcryptCostChanged = false
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

		invalidateErr := auth.InvalidateChannels(username, true, 1)
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

		updateErr := auth.InvalidateRoles(username, 1)
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
	assert.Equal(t, cost, DefaultBcryptCost)
}

func TestAuthenticateTrustedJWT(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	ctx := base.TestCtx(t)

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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
		err = provider.InitUserPrefix(ctx)
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
	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

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
	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

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
		err = providerGoogle.InitUserPrefix(base.TestCtx(t))
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

func (m mockComputerV2) ComputeChannelsForPrincipal(ctx context.Context, principal Principal) (ch.TimedSet, error) {
	if user, ok := principal.(User); ok {
		return m.channels[user.Name()].Copy(), nil
	} else {
		return m.roleChannels[principal.Name()].Copy(), nil
	}
}

func (m mockComputerV2) ComputeRolesForUser(ctx context.Context, user User) (ch.TimedSet, error) {
	return m.roles[user.Name()].Copy(), nil
}

func (m mockComputerV2) addRoleChannels(t *testing.T, auth *Authenticator, roleName, channelName string, invalSeq uint64) {
	if _, ok := m.roleChannels[roleName]; !ok {
		m.roleChannels[roleName] = ch.TimedSet{}
	}

	m.roleChannels[roleName].Add(ch.AtSequence(ch.SetOf(t, channelName), invalSeq))
	err := auth.InvalidateChannels(roleName, false, invalSeq)
	assert.NoError(t, err)
}

func (m mockComputerV2) removeRoleChannel(t *testing.T, auth *Authenticator, roleName, channelName string, invalSeq uint64) {
	delete(m.roleChannels[roleName], channelName)
	err := auth.InvalidateChannels(roleName, false, invalSeq)
	assert.NoError(t, err)
}

func (m mockComputerV2) addRole(t *testing.T, auth *Authenticator, userName, roleName string, invalSeq uint64) {
	if _, ok := m.roles[userName]; !ok {
		m.roles[userName] = ch.TimedSet{}
	}

	m.roles[userName].Add(ch.AtSequence(ch.SetOf(t, roleName), invalSeq))
	err := auth.InvalidateRoles(userName, invalSeq)
	assert.NoError(t, err)
}

func (m mockComputerV2) removeRole(t *testing.T, auth *Authenticator, userName, roleName string, invalSeq uint64) {
	delete(m.roles[userName], roleName)
	err := auth.InvalidateRoles(userName, invalSeq)
	assert.NoError(t, err)
}

// Obtains principals for test scenarios
// This triggers rebuild of the principals if invalid, intended to simulate an auth on _changes
func getPrincipals(t *testing.T, auth *Authenticator) (*userImpl, Principal) {
	principal, err := auth.GetPrincipal("alice", true)
	assert.NoError(t, err)
	userPrincipal, ok := principal.(*userImpl)
	assert.True(t, ok)
	rolePrincipal, err := auth.GetPrincipal("foo", false)
	assert.NoError(t, err)
	return userPrincipal, rolePrincipal
}

// Initializes principals and returns them for scenarios
func initializeScenario(t *testing.T, auth *Authenticator) (*userImpl, Principal) {
	user, err := auth.NewUser("alice", "password", nil)
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	role, err := auth.NewRole("foo", nil)
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get principals to do initial ops on
	return getPrincipals(t, auth)
}

// =======================================================================================================
// The below 'TestRevocationScenario' tests refer to scenarios in the following Google Sheet
// https://docs.google.com/spreadsheets/d/1pTLyJqrSdde-dAxDfMkKGXi0ttWF4GVCOBFJg7hRY0U/edit?usp=sharing
// =======================================================================================================

// Scenario 1
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// No changes
//  - Changes Request - Seq 40 - Has channel 1 access, no history
// Role revoke, role channel revoke then role re-grant and role channel re-grant
//  - Changes Request - Seq 80 - Has channel 1 access, no history
// Role revoke, role channel revoke
//  - Changes Request - Seq 110 - Doesn't have channel access, history added for both role and channel
func TestRevocationScenario1(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	// Get Principals / Rebuild Seq 40
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(40, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 75, EndSeq: 85}, channelHistory.Entries[0])
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(80, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(85), revokedChannelsCombined["ch1"])
}

// Scenario 2
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// Revoke role
//  - Changes Request - Seq 50 - Doesn't have channel access, role history added
// Revoke channel, re-grant role, re-grant channel
//  - Changes Request - Seq 80 - Has channel access, retains role history
// Role revoke, role channel revoke
//  - Changes Request - Seq 110 - Doesn't have channel access, history added for both role and channel
func TestRevocationScenario2(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)

	// Get Principals / Rebuild Seq 50
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(45), revokedChannelsCombined["ch1"])

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)
	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(50, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok = aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[1])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 75, EndSeq: 85}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(80, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(85), revokedChannelsCombined["ch1"])
}

// Scenario 3
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// Revoke role, revoke role channel
//  - Changes Request - Seq 60 - Doesn't have channel access, history added for both role and channel
// Grant role channel and role
//  - Changes Request - Seq 80 - Has channel access, retains history
// Role revoke, role channel revoke
//  - Changes Request - Seq 110 - Doesn't have channel access, history added for both role and channel
func TestRevocationScenario3(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(55, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	// Rebuild seq 60
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(45), revokedChannelsCombined["ch1"])

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	// Rebuild seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 1, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 1, len(fooPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(60, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))

	userRoleHistory, ok = aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[1])

	channelHistory, ok = fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)

	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 75, EndSeq: 85}, channelHistory.Entries[1])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(80, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(85), revokedChannelsCombined["ch1"])
}

// Scenario 4
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// Revoke role, revoke role channel, re-grant role
//  - Changes Request - Seq 70 - Doesn't have channel access, history added for role channel
// Grant role
//  - Changes Request - Seq 80 - Has channel access, retains history
// Role revoke, role channel revoke
//  - Changes Request - Seq 110 - Doesn't have channel access, history added for both role and channel
func TestRevocationScenario4(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)

	// Get Principals / Rebuild Seq 70
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(55), revokedChannelsCombined["ch1"])

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 1, len(fooPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(70, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	channelHistory, ok = fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 75, EndSeq: 85}, channelHistory.Entries[1])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(80, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(85), revokedChannelsCombined["ch1"])
}

// Scenario 5
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// Revoke role, revoke role channel, re-grant role, re-grant channel
//  - Changes Request - Seq 80 - Has channel 1 access, no history
// Revoke role and role channel
//  - Changes Request - Seq 110 - Doesn't have channel access, history added for both role and channel
func TestRevocationScenario5(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 75, EndSeq: 85}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(80, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(85), revokedChannelsCombined["ch1"])
}

// Scenario 6
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// Revoke role, revoke role channel, re-grant role, re-grant channel, re-revoke channel
//  - Changes Request - Seq 90 - Doesn't have channel 1 access, history added for role channel
// Revoke role
//  - Changes Request - Seq 110 - Doesn't have channel access, history added for role
func TestRevocationScenario6(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	require.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)

	// Rebuild seq 90
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(55), revokedChannelsCombined["ch1"])

	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 100
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])

	channelHistory, ok = fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	assert.Len(t, revokedChannelsCombined, 1)
	assert.Equal(t, uint64(55), revokedChannelsCombined["ch1"])
}

// Scenario 7
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel 1 access, no history
// Revoke role, revoke role channel, re-grant role, re-grant channel, re-revoke channel, re-revoke role
//  - Changes Request - Seq 100 - Doesn't have channel 1 access, history added for role channel and role
// No Change
//  - Changes Request - Seq 110 - Doesn't have channel access, history retained
func TestRevocationScenario7(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.ElementsMatch(t, []string{"!", "ch1"}, fooPrincipal.Channels().AllKeys())
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 100
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(25, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(45), revokedChannelsCombined["ch1"])

	// Get Principals / Rebuild Seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))

	assert.Equal(t, 1, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 1, len(fooPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(100, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)
}

// Scenario 8
// Initiate user and role
// Grant role channel and role, revoke role
//  - Changes Request - Seq 50 - Doesn't have channel 1 access, no history
// Revoke role channel, re-grant role, re-grant channel, re-revoke channel, re-revoke role
//  - Changes Request - Seq 110 - Doesn't have channel 1 access, no history
func TestRevocationScenario8(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)

	// Get Principals / Rebuild Seq 50
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(50, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 5, EndSeq: 55}, channelHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(50, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)
}

// Scenario 9
// Initiate user and role
// Grant role channel and role, revoke role and role channel
//  - Changes Request - Seq 60 - Doesn't have channel 1 access, no history
// Re-grant role, re-grant channel, re-revoke channel, re-revoke role
//  - Changes Request - Seq 110 - Doesn't have channel 1 access, no history
func TestRevocationScenario9(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	// Get Principals / Rebuild Seq 60
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(60, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)
}

// Scenario 10
// Initiate user and role
// Grant role channel and role, revoke role and role channel, re-grant role
//  - Changes Request - Seq 70 - Doesn't have channel 1 access, no history
// Re-grant channel, re-revoke channel, re-revoke role
//  - Changes Request - Seq 110 - Doesn't have channel 1 access, no history
func TestRevocationScenario10(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)

	// Get Principals / Rebuild Seq 70
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(70, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)
}

// Scenario 11
// Initiate user and role
// Grant role channel and role, revoke role and role channel, re-grant role, re-grant channel
//  - Changes Request - Seq 80 - Has channel 1 access, no history
// Revoke channel, revoke role
//  - Changes Request - Seq 110 - Doesn't have channel 1 access, adds role and channel history
func TestRevocationScenario11(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	// Get Principals / Rebuild Seq 80
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user can see ch1 (via role)
	// Verify history
	assert.True(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))

	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])

	channelHistory, ok := fooPrincipal.ChannelHistory()["ch1"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 75, EndSeq: 85}, channelHistory.Entries[0])

	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))

	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(80, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(85), revokedChannelsCombined["ch1"])
}

// Scenario 12
// Initiate user and role
// Grant role channel and role, revoke role and role channel, re-grant role, re-grant channel, re-revoke channel
//  - Changes Request - Seq 90 - Doesn't have channel 1 access, no history
// Revoke role
//  - Changes Request - Seq 110 - Doesn't have channel 1 access, no history
func TestRevocationScenario12(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)

	// Get Principals / Rebuild Seq 90
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 65, EndSeq: 95}, userRoleHistory.Entries[0])
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(90, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)
}

// Scenario 13
// Initiate user and role
// Grant role channel and role, revoke role and role channel, re-grant role, re-grant channel, re-revoke channel, re-revoke role
//  - Changes Request - Seq 100 - Doesn't have channel 1 access, no history
// No changes
//  - Changes Request - Seq 110 - Doesn't have channel 1 access, no history
func TestRevocationScenario13(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, fooPrincipal := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)
	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 55)

	testMockComputer.addRole(t, auth, "alice", "foo", 65)
	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 75)

	testMockComputer.removeRoleChannel(t, auth, "foo", "ch1", 85)
	testMockComputer.removeRole(t, auth, "alice", "foo", 95)

	// Get Principals / Rebuild Seq 100
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(5, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

	// Rebuild seq 110
	aliceUserPrincipal, fooPrincipal = getPrincipals(t, auth)

	// Ensure user cannot see ch1 (via role)
	// Verify history
	assert.False(t, aliceUserPrincipal.CanSeeChannel("ch1"))
	assert.Equal(t, 0, len(aliceUserPrincipal.RoleHistory()))
	assert.Equal(t, 0, len(aliceUserPrincipal.ChannelHistory()))
	assert.Equal(t, 0, len(fooPrincipal.ChannelHistory()))
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(100, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)
}

// Scenario 14
// Initiate user and role
// Grant role channel and role
//  - Changes Request - Seq 25 - Has channel access no history
// Revoke role
//  - Changes Request Seq 45 since 25. Ensure revocation.
// 	- Changes Request Seq 45 since 45 same seq as revocation. Ensure no revocation message.
func TestRevocationScenario14(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testMockComputer := mockComputerV2{
		roles:        map[string]ch.TimedSet{},
		channels:     map[string]ch.TimedSet{},
		roleChannels: map[string]ch.TimedSet{},
	}

	auth := NewAuthenticator(testBucket, &testMockComputer, DefaultAuthenticatorOptions())
	aliceUserPrincipal, _ := initializeScenario(t, auth)

	testMockComputer.addRoleChannels(t, auth, "foo", "ch1", 5)
	testMockComputer.addRole(t, auth, "alice", "foo", 20)

	// Get Principals / Rebuild Seq 25
	aliceUserPrincipal, _ = getPrincipals(t, auth)

	testMockComputer.removeRole(t, auth, "alice", "foo", 45)

	// Get Principals / Rebuild Seq 45
	aliceUserPrincipal, _ = getPrincipals(t, auth)
	userRoleHistory, ok := aliceUserPrincipal.RoleHistory()["foo"]
	require.True(t, ok)
	assert.Equal(t, GrantHistorySequencePair{StartSeq: 20, EndSeq: 45}, userRoleHistory.Entries[0])

	// Ensure that a since 25 shows the revocation
	revokedChannelsCombined := aliceUserPrincipal.RevokedChannels(25, 0, 0)
	require.Contains(t, revokedChannelsCombined, "ch1")
	assert.Equal(t, uint64(45), revokedChannelsCombined["ch1"])

	// Ensure that the user cannot see the channel at this point (after 45)
	aliceUserPrincipal.CanSeeChannel("ch1")

	// Ensure a pull from 45 (same seq as revocation) wouldn't send message
	revokedChannelsCombined = aliceUserPrincipal.RevokedChannels(45, 0, 0)
	assert.Len(t, revokedChannelsCombined, 0)

}

func TestRoleSoftDelete(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	const roleName = "role"

	// Instantiate role
	role, err := auth.NewRole(roleName, ch.SetOf(t, "channel"))
	assert.NoError(t, err)
	assert.NotNil(t, role)

	// Save role to bucket
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get role - ensure accessible
	role, err = auth.GetRole(roleName)
	assert.NoError(t, err)
	assert.NotNil(t, role)
	assert.Equal(t, 2, len(role.Channels().AllKeys()))
	assert.True(t, role.Channels().Contains("channel"))

	// Delete role
	err = auth.DeleteRole(role, false, 2)
	assert.NoError(t, err)

	// Delete again
	err = auth.DeleteRole(role, false, 2)
	assert.NoError(t, err)

	expectedChannelHistory := GrantHistorySequencePair{StartSeq: 1, EndSeq: 2}

	role, err = auth.GetRoleIncDeleted(roleName)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(role.ChannelHistory()))
	assert.Equal(t, 0, len(role.Channels().AllKeys()))
	require.Equal(t, 1, len(role.ChannelHistory()["channel"].Entries))
	require.Equal(t, 1, len(role.ChannelHistory()["!"].Entries))
	assert.Equal(t, expectedChannelHistory, role.ChannelHistory()["channel"].Entries[0])
	assert.Equal(t, expectedChannelHistory, role.ChannelHistory()["!"].Entries[0])

	// Get role - ensure its not accessible
	role, err = auth.GetRole(roleName)
	assert.NoError(t, err)
	assert.Nil(t, role)

	// Re-create role
	role, err = auth.NewRole(roleName, ch.SetOf(t, "channel2"))
	assert.NoError(t, err)
	assert.NotNil(t, role)

	// Save role to bucket
	err = auth.Save(role)
	assert.NoError(t, err)

	// Get role - ensure its accessible
	role, err = auth.GetRole(roleName)
	assert.NoError(t, err)
	assert.NotNil(t, role)
	assert.Equal(t, 2, len(role.Channels().AllKeys()))
	assert.False(t, role.Channels().Contains("channel"))
	assert.True(t, role.Channels().Contains("channel2"))
	assert.Equal(t, 2, len(role.ChannelHistory()))
	require.Equal(t, 1, len(role.ChannelHistory()["channel"].Entries))
	require.Equal(t, 1, len(role.ChannelHistory()["!"].Entries))
	assert.Equal(t, expectedChannelHistory, role.ChannelHistory()["channel"].Entries[0])
	assert.Equal(t, expectedChannelHistory, role.ChannelHistory()["!"].Entries[0])
}

func TestObtainChannelsForDeletedRole(t *testing.T) {
	testcases := []struct {
		Name     string
		TestFunc func(auth *Authenticator, role Role, t *testing.T)
	}{{
		"GetUserThenDelete",
		func(auth *Authenticator, role Role, t *testing.T) {
			// Get user
			user, err := auth.GetUser("user")
			assert.NoError(t, err)

			// Role deleted
			err = auth.DeleteRole(role, true, 2)
			assert.NoError(t, err)

			// Successfully able to get inherited channels even though role is missing
			assert.Equal(t, []string{"!"}, user.InheritedChannels().AllKeys())
		},
	},
		{
			"DeleteThenGetUser",
			func(auth *Authenticator, role Role, t *testing.T) {
				// Role deleted
				err := auth.DeleteRole(role, true, 2)
				assert.NoError(t, err)

				// Get user
				user, err := auth.GetUser("user")
				assert.NoError(t, err)

				// Successfully able to get inherited channels even though role is missing
				assert.Equal(t, []string{"!"}, user.InheritedChannels().AllKeys())
			},
		},
	}

	for _, testCase := range testcases {
		t.Run("name", func(t *testing.T) {
			testMockComputer := mockComputerV2{
				roles:        map[string]ch.TimedSet{},
				channels:     map[string]ch.TimedSet{},
				roleChannels: map[string]ch.TimedSet{},
			}

			testBucket := base.GetTestBucket(t)
			defer testBucket.Close()
			auth := NewAuthenticator(testBucket, testMockComputer, DefaultAuthenticatorOptions())

			const roleName = "role"

			// Instantiate role
			role, err := auth.NewRole(roleName, ch.SetOf(t, "channel"))
			assert.NoError(t, err)
			assert.NotNil(t, role)

			// Save role to bucket
			err = auth.Save(role)
			assert.NoError(t, err)

			// Instantiate user
			user, err := auth.NewUser("user", "", nil)
			assert.NoError(t, err)

			// Save user to bucket
			err = auth.Save(user)
			assert.NoError(t, err)

			// Add channel to role and role to user
			testMockComputer.addRoleChannels(t, auth, "role", "chan", 1)
			testMockComputer.addRole(t, auth, "user", "role", 1)

			testCase.TestFunc(auth, role, t)
		})
	}
}

func TestInvalidateRoles(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	leakyBucket := base.NewLeakyBucket(testBucket, base.LeakyBucketConfig{})

	auth := NewAuthenticator(leakyBucket, nil, DefaultAuthenticatorOptions())

	// Invalidate role on non-existent user and ensure no error
	err := auth.InvalidateRoles("user", 0)
	assert.NoError(t, err)

	// Create user
	user, err := auth.NewUser("user", "pass", nil)
	assert.NoError(t, err)
	err = auth.Save(user)
	assert.NoError(t, err)

	enableRetry := false
	leakyBucket.SetUpdateCallback(func(key string) {
		if enableRetry {
			enableRetry = false
			err = auth.InvalidateRoles("user", 5)
			assert.NoError(t, err)
		}
	})

	// Invalidate roles at invalSeq but cause cas retry by setting to 5
	enableRetry = true
	err = auth.InvalidateRoles("user", 10)
	assert.NoError(t, err)

	// Ensure the inval seq was set to 5 (raw get to avoid rebuild)
	var userOut userImpl
	_, err = leakyBucket.Get(docIDForUser("user"), &userOut)
	assert.NoError(t, err)

	var expectedValue uint64
	if leakyBucket.IsSupported(sgbucket.DataStoreFeatureSubdocOperations) {
		expectedValue = 10
	} else {
		expectedValue = 5
	}

	assert.Equal(t, expectedValue, userOut.GetRoleInvalSeq())

	// Invalidate again and ensure existing value remains
	err = auth.InvalidateRoles("user", 20)
	assert.NoError(t, err)

	_, err = leakyBucket.Get(docIDForUser("user"), &userOut)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, userOut.GetRoleInvalSeq())
}

func TestInvalidateChannels(t *testing.T) {
	testCases := []struct {
		name   string
		isUser bool
	}{
		{
			name:   "User",
			isUser: true,
		},
		{
			name:   "Role",
			isUser: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			base.ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

				leakyBucket := base.NewLeakyBucket(bucket, base.LeakyBucketConfig{})

				auth := NewAuthenticator(leakyBucket, nil, DefaultAuthenticatorOptions())

				// Invalidate channels on non-existent user / role and ensure no error
				err := auth.InvalidateChannels(testCase.name, testCase.isUser, 0)
				assert.NoError(t, err)

				// Create user / role
				var princ Principal
				if testCase.isUser {
					princ, err = auth.NewUser(testCase.name, "pass", nil)
				} else {
					princ, err = auth.NewRole(testCase.name, nil)
				}
				assert.NoError(t, err)

				err = auth.Save(princ)
				assert.NoError(t, err)

				// Invalidate channels at invalSeq but cause cas retry by setting to 5
				enableRetry := false
				// If subdoc operations are supported, perform an initial invalidation at seq 5
				if leakyBucket.IsSupported(sgbucket.DataStoreFeatureSubdocOperations) {
					err := auth.InvalidateChannels(testCase.name, testCase.isUser, 5)
					assert.NoError(t, err)

				} else {
					// If subdoc ops aren't supported, use leakyBucket to invalidate
					leakyBucket.SetUpdateCallback(func(key string) {
						if enableRetry {
							enableRetry = false
							err = auth.InvalidateChannels(testCase.name, testCase.isUser, 5)
							assert.NoError(t, err)
						}
					})
				}

				enableRetry = true
				err = auth.InvalidateChannels(testCase.name, testCase.isUser, 10)
				assert.NoError(t, err)

				// Ensure the inval seq was set to 5 (raw get to avoid rebuild)
				var princCheck Principal
				var docID string
				if testCase.isUser {
					princCheck = &userImpl{}
					docID = docIDForUser(testCase.name)
				} else {
					princCheck = &roleImpl{}
					docID = docIDForRole(testCase.name)
				}
				_, err = leakyBucket.Get(docID, &princCheck)
				assert.NoError(t, err)

				expectedValue := uint64(5)
				assert.Equal(t, expectedValue, princCheck.GetChannelInvalSeq())

				// Invalidate again and ensure existing value remains
				err = auth.InvalidateChannels(testCase.name, testCase.isUser, 20)
				assert.NoError(t, err)

				_, err = leakyBucket.Get(docID, &princCheck)
				assert.NoError(t, err)
				assert.Equal(t, expectedValue, princCheck.GetChannelInvalSeq())
			})
		})
	}
}

func TestCalculateMaxHistoryEntriesPerGrant(t *testing.T) {
	testCases := []struct {
		Name                     string
		HistoryLengthInput       int
		MaxEntriesExpectedOutput int
	}{
		{
			Name:                     "Extreme history length",
			HistoryLengthInput:       1000000,
			MaxEntriesExpectedOutput: 1,
		},
		{
			Name:                     "Upper Bound",
			HistoryLengthInput:       3972,
			MaxEntriesExpectedOutput: 1,
		},
		{
			Name:                     "Large History Length",
			HistoryLengthInput:       3330,
			MaxEntriesExpectedOutput: 4,
		},
		{
			Name:                     "Median history length",
			HistoryLengthInput:       3250,
			MaxEntriesExpectedOutput: 5,
		},
		{
			Name:                     "Small history length",
			HistoryLengthInput:       3010,
			MaxEntriesExpectedOutput: 7,
		},
		{
			Name:                     "Lower bound",
			HistoryLengthInput:       2680,
			MaxEntriesExpectedOutput: 10,
		},
		{
			Name:                     "Tiny history length",
			HistoryLengthInput:       1,
			MaxEntriesExpectedOutput: 10,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			assert.Equal(t, testCase.MaxEntriesExpectedOutput, CalculateMaxHistoryEntriesPerGrant(testCase.HistoryLengthInput))
		})
	}
}

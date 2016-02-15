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
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

//const kTestURL = "http://localhost:8091"
const kTestURL = "walrus:"

var gTestBucket base.Bucket

func init() {
	var err error
	gTestBucket, err = base.GetBucket(base.BucketSpec{
		Server:     kTestURL,
		BucketName: "sync_gateway_tests"}, nil)
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	if err != nil {
		log.Fatalf("Couldn't install design doc: %v", err)
	}
}

func canSeeAllChannels(princ Principal, channels base.Set) bool {
	for channel := range channels {
		if !princ.CanSeeChannel(channel) {
			return false
		}
	}
	return true
}

func TestValidateGuestUser(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, err := auth.NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, err := auth.NewUser("invalid:name", "", nil)
	assert.Equals(t, user, (User)(nil))
	assert.True(t, err != nil)
	user, err = auth.NewUser("ValidName", "", nil)
	assert.True(t, user != nil)
	assert.Equals(t, err, nil)
	user, err = auth.NewUser("ValidName", "letmein", nil)
	assert.True(t, user != nil)
	assert.Equals(t, err, nil)
}

func TestValidateRole(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	role, err := auth.NewRole("invalid:name", nil)
	assert.Equals(t, role, (User)(nil))
	assert.True(t, err != nil)
	role, err = auth.NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equals(t, err, nil)
	role, err = auth.NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equals(t, err, nil)
}

func TestValidateUserEmail(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
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
	assert.Equals(t, user.SetEmail("foo@example.com"), nil)
}

func TestUserPasswords(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
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

// Test that multiple authentications of the same user/password are fast.
// This is an important check because the underlying bcrypt algorithm used to verify passwords
// is _extremely_ slow (~100ms!) so we use a cache to speed it up (see password_hash.go).
func TestAuthenticationSpeed(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
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

func TestSerializeUser(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, _ := auth.NewUser("me", "letmein", ch.SetOf("me", "public"))
	user.SetEmail("foo@example.com")
	encoded, _ := json.Marshal(user)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled User as: %s", encoded)

	resu := &userImpl{}
	err := json.Unmarshal(encoded, resu)
	assert.True(t, err == nil)
	assert.DeepEquals(t, resu.Name(), user.Name())
	assert.DeepEquals(t, resu.Email(), user.Email())
	assert.DeepEquals(t, resu.ExplicitChannels(), user.ExplicitChannels())
	assert.True(t, resu.Authenticate("letmein"))
	assert.False(t, resu.Authenticate("123456"))
}

func TestSerializeRole(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	role, _ := auth.NewRole("froods", ch.SetOf("hoopy", "public"))
	encoded, _ := json.Marshal(role)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled Role as: %s", encoded)
	elor := &roleImpl{}
	err := json.Unmarshal(encoded, elor)

	assert.True(t, err == nil)
	assert.DeepEquals(t, elor.Name(), role.Name())
	assert.DeepEquals(t, elor.ExplicitChannels(), role.ExplicitChannels())
}

func TestUserAccess(t *testing.T) {
	// User with no access:
	auth := NewAuthenticator(gTestBucket, nil)
	user, _ := auth.NewUser("foo", "password", nil)
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("!"))
	assert.False(t, user.CanSeeChannel("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("*")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf()) == nil)

	// User with access to one channel:
	user.setChannels(ch.AtSequence(ch.SetOf("x"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf("y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf()) == nil)

	// User with access to one channel and one derived channel:
	user.setChannels(ch.AtSequence(ch.SetOf("x", "z"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("x")), ch.SetOf("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with access to two channels:
	user.setChannels(ch.AtSequence(ch.SetOf("x", "z"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("x")), ch.SetOf("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	user.setChannels(ch.AtSequence(ch.SetOf("x", "y"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "y"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y", "z")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with wildcard access:
	user.setChannels(ch.AtSequence(ch.SetOf("*", "q"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("*", "q"))
	assert.True(t, user.CanSeeChannel("*"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf("x")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf("*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf()) == nil)
}

func TestGetMissingUser(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, err := auth.GetUser("noSuchUser")
	assert.Equals(t, err, nil)
	assert.True(t, user == nil)
	user, err = auth.GetUserByEmail("noreply@example.com")
	assert.Equals(t, err, nil)
	assert.True(t, user == nil)
}

func TestGetMissingRole(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	role, err := auth.GetRole("noSuchRole")
	assert.Equals(t, err, nil)
	assert.True(t, role == nil)
}

func TestGetGuestUser(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, err := auth.GetUser("")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user, auth.defaultGuestUser())

}

func TestSaveUsers(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf("test"))
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user2, user)
}

func TestSaveRoles(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	role, _ := auth.NewRole("testRole", ch.SetOf("test"))
	err := auth.Save(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, role2, role)
}

type mockComputer struct {
	channels ch.TimedSet
	roles    ch.TimedSet
	err      error
}

func (self *mockComputer) ComputeChannelsForPrincipal(Principal) (ch.TimedSet, error) {
	return self.channels, self.err
}

func (self *mockComputer) ComputeRolesForUser(User) (ch.TimedSet, error) {
	return self.roles, self.err
}

func (self *mockComputer) UseGlobalSequence() bool {
	return true
}

func TestRebuildUserChannels(t *testing.T) {
	computer := mockComputer{channels: ch.AtSequence(ch.SetOf("derived1", "derived2"), 1)}
	auth := NewAuthenticator(gTestBucket, &computer)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf("explicit1"))
	user.setChannels(nil)
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf("explicit1", "derived1", "derived2", "!"), 1))
}

func TestRebuildRoleChannels(t *testing.T) {
	computer := mockComputer{channels: ch.AtSequence(ch.SetOf("derived1", "derived2"), 1)}
	auth := NewAuthenticator(gTestBucket, &computer)
	role, _ := auth.NewRole("testRole", ch.SetOf("explicit1"))
	err := auth.InvalidateChannels(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, role2.Channels(), ch.AtSequence(ch.SetOf("explicit1", "derived1", "derived2", "!"), 1))
}

func TestRebuildChannelsError(t *testing.T) {
	computer := mockComputer{}
	auth := NewAuthenticator(gTestBucket, &computer)
	role, err := auth.NewRole("testRole2", ch.SetOf("explicit1"))
	assert.Equals(t, err, nil)
	assert.Equals(t, auth.InvalidateChannels(role), nil)

	computer.err = fmt.Errorf("I'm sorry, Dave.")

	role2, err := auth.GetRole("testRole2")
	assert.Equals(t, role2, nil)
	assert.DeepEquals(t, err, computer.err)
}

func TestRebuildUserRoles(t *testing.T) {
	computer := mockComputer{roles: ch.AtSequence(base.SetOf("role1", "role2"), 3)}
	auth := NewAuthenticator(gTestBucket, &computer)
	user, _ := auth.NewUser("testUser", "letmein", nil)
	user.SetExplicitRoles(ch.TimedSet{"role3": ch.NewVbSimpleSequence(1), "role1": ch.NewVbSimpleSequence(1)})
	err := auth.InvalidateRoles(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	expected := ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	assert.DeepEquals(t, user2.RoleNames(), expected)
}

func TestRoleInheritance(t *testing.T) {
	// Create some roles:
	auth := NewAuthenticator(gTestBucket, nil)
	role, _ := auth.NewRole("square", ch.SetOf("dull", "duller", "dullest"))
	assert.Equals(t, auth.Save(role), nil)
	role, _ = auth.NewRole("frood", ch.SetOf("hoopy", "hoopier", "hoopiest"))
	assert.Equals(t, auth.Save(role), nil)

	user, _ := auth.NewUser("arthur", "password", ch.SetOf("britain"))
	user.(*userImpl).setRolesSince(ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	assert.DeepEquals(t, user.RoleNames(), ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	auth.Save(user)

	user2, err := auth.GetUser("arthur")
	assert.Equals(t, err, nil)
	log.Printf("Channels = %s", user2.Channels())
	assert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf("!", "britain"), 1))
	assert.DeepEquals(t, user2.InheritedChannels(),
		ch.TimedSet{"!": ch.NewVbSimpleSequence(0x1), "britain": ch.NewVbSimpleSequence(0x1), "dull": ch.NewVbSimpleSequence(0x3), "duller": ch.NewVbSimpleSequence(0x3), "dullest": ch.NewVbSimpleSequence(0x3), "hoopy": ch.NewVbSimpleSequence(0x4), "hoopier": ch.NewVbSimpleSequence(0x4), "hoopiest": ch.NewVbSimpleSequence(0x4)})
	assert.True(t, user2.CanSeeChannel("britain"))
	assert.True(t, user2.CanSeeChannel("duller"))
	assert.True(t, user2.CanSeeChannel("hoopy"))
	assert.Equals(t, user2.AuthorizeAllChannels(ch.SetOf("britain", "dull", "hoopiest")), nil)
}

func TestRegisterUser(t *testing.T) {
	// Register user based on name, email
	auth := NewAuthenticator(gTestBucket, nil)
	user, err := auth.RegisterNewUser("ValidName", "foo@example.com")
	assert.Equals(t, user.Name(), "ValidName")
	assert.Equals(t, user.Email(), "foo@example.com")
	assert.Equals(t, err, nil)

	// verify retrieval by username
	user, err = auth.GetUser("ValidName")
	assert.Equals(t, user.Name(), "ValidName")
	assert.Equals(t, err, nil)

	// verify retrieval by email
	user, err = auth.GetUserByEmail("foo@example.com")
	assert.Equals(t, user.Name(), "ValidName")
	assert.Equals(t, err, nil)

	// Register user based on email, retrieve based on username, email
	user, err = auth.RegisterNewUser("bar@example.com", "bar@example.com")
	assert.Equals(t, user.Name(), "bar@example.com")
	assert.Equals(t, user.Email(), "bar@example.com")
	assert.Equals(t, err, nil)

	user, err = auth.GetUser("UnknownName")
	assert.Equals(t, user, nil)
	assert.Equals(t, err, nil)

	user, err = auth.GetUserByEmail("bar@example.com")
	assert.Equals(t, user.Name(), "bar@example.com")
	assert.Equals(t, err, nil)
}

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

	"github.com/sdegutis/go.assert"

	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

//const kTestURL = "http://localhost:8091"
const kTestURL = "walrus:"

var gTestBucket base.Bucket

func init() {
	var err error
	gTestBucket, err = base.GetBucket(kTestURL, "default", "sync_gateway_tests")
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	err = InstallDesignDoc(gTestBucket)
	if err != nil {
		log.Fatalf("Couldn't install design doc: %v", err)
	}
}

func TestValidateGuestUser(t *testing.T) {
	user, err := NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {
	user, err := NewUser("invalid:name", "", nil)
	assert.Equals(t, user, (User)(nil))
	assert.True(t, err != nil)
	user, err = NewUser("ValidName", "", nil)
	assert.True(t, user != nil)
	assert.Equals(t, err, nil)
	user, err = NewUser("ValidName", "letmein", nil)
	assert.True(t, user != nil)
	assert.Equals(t, err, nil)
}

func TestValidateRole(t *testing.T) {
	role, err := NewRole("invalid:name", nil)
	assert.Equals(t, role, (User)(nil))
	assert.True(t, err != nil)
	role, err = NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equals(t, err, nil)
	role, err = NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equals(t, err, nil)
}

func TestValidateUserEmail(t *testing.T) {
	badEmails := []string{"", "foo", "foo@", "@bar", "foo @bar", "foo@.bar"}
	for _, e := range badEmails {
		assert.False(t, IsValidEmail(e))
	}
	goodEmails := []string{"foo@bar", "foo.99@bar.com", "f@bar.exampl-3.com."}
	for _, e := range goodEmails {
		assert.True(t, IsValidEmail(e))
	}
	user, _ := NewUser("ValidName", "letmein", nil)
	assert.False(t, user.SetEmail("foo") == nil)
	assert.Equals(t, user.SetEmail("foo@example.com"), nil)
}

func TestUserPasswords(t *testing.T) {
	user, _ := NewUser("me", "letmein", nil)
	assert.True(t, user.Authenticate("letmein"))
	assert.False(t, user.Authenticate("password"))
	assert.False(t, user.Authenticate(""))
	user, _ = NewUser("", "", nil)
	assert.True(t, user.Authenticate(""))
	assert.False(t, user.Authenticate("123456"))
}

func TestSerializeUser(t *testing.T) {
	user, _ := NewUser("me", "letmein", ch.SetOf("me", "public"))
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
	role, _ := NewRole("froods", ch.SetOf("hoopy", "public"))
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
	user, _ := NewUser("foo", "password", nil)
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf())
	assert.False(t, user.CanSeeChannel("x"))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf()))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("x")))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("x", "y")))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("*")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with access to one channel:
	user.setChannels(ch.SetOf("x"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x"))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf()))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x")))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with access to one channel and one derived channel:
	user.setChannels(ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("x")), ch.SetOf("x"))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf()))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x")))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with access to two channels:
	user.setChannels(ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("x")), ch.SetOf("x"))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf()))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x")))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	user.setChannels(ch.SetOf("x", "y"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "y"))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf()))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x")))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x", "y")))
	assert.False(t, user.CanSeeAllChannels(ch.SetOf("x", "y", "z")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with wildcard access:
	user.setChannels(ch.SetOf("*", "q"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("*", "q"))
	assert.True(t, user.CanSeeChannel("*"))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf()))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x")))
	assert.True(t, user.CanSeeAllChannels(ch.SetOf("x", "y")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
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
	assert.DeepEquals(t, user, defaultGuestUser())

}

func TestSaveUsers(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	user, _ := NewUser("testUser", "password", ch.SetOf("test"))
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user2, user)
}

func TestSaveRoles(t *testing.T) {
	auth := NewAuthenticator(gTestBucket, nil)
	role, _ := NewRole("testRole", ch.SetOf("test"))
	err := auth.Save(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, role2, role)
}

type mockComputer struct {
	channels ch.Set
	err      error
}

func (self *mockComputer) ComputeChannelsForPrincipal(Principal) (ch.Set, error) {
	return self.channels, self.err
}

func TestRebuildUserChannels(t *testing.T) {
	computer := mockComputer{channels: ch.SetOf("derived1", "derived2")}
	auth := NewAuthenticator(gTestBucket, &computer)
	user, _ := NewUser("testUser", "password", ch.SetOf("explicit1"))
	user.setChannels(nil)
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	if user2 != nil {
		log.Printf("Channels = %s", user2.Channels())
		assert.DeepEquals(t, user2.Channels(), ch.SetOf("explicit1", "derived1", "derived2"))
	}
}

func TestRebuildRoleChannels(t *testing.T) {
	computer := mockComputer{channels: ch.SetOf("derived1", "derived2")}
	auth := NewAuthenticator(gTestBucket, &computer)
	role, _ := NewRole("testRole", ch.SetOf("explicit1"))
	err := auth.InvalidateChannels(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole")
	assert.Equals(t, err, nil)
	if role2 != nil {
		assert.DeepEquals(t, role2.Channels(), ch.SetOf("explicit1", "derived1", "derived2"))
	}
}

func TestRebuildChannelsError(t *testing.T) {
	intendedError := fmt.Errorf("I'm sorry, Dave.")
	computer := mockComputer{err: intendedError}
	auth := NewAuthenticator(gTestBucket, &computer)
	role, _ := NewRole("testRole2", ch.SetOf("explicit1"))
	err := auth.InvalidateChannels(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole2")
	assert.Equals(t, role2, nil)
	assert.DeepEquals(t, err, intendedError)
}

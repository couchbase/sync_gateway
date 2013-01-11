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
	//"log"
	"github.com/sdegutis/go.assert"
	"testing"
)

func TestValidateGuestUser(t *testing.T) {
	user, err := NewUser("", "letmein", nil)
	assert.True(t, user == nil)
	assert.True(t, err != nil)
	user, err = NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {
	user, _ := NewUser("invalid:name", "", nil)
	assert.Equals(t, user, (*User)(nil))
	user, _ = NewUser("ValidName", "", nil)
	assert.Equals(t, user, (*User)(nil))
	user, _ = NewUser("ValidName", "letmein", nil)
	assert.True(t, user != nil)
}

func TestValidateUserEmail(t *testing.T) {
	badEmails := []string{"", "foo", "foo@", "@bar", "foo @bar", "foo@.bar"}
	for _,e := range(badEmails) {
		assert.False(t, IsValidEmail(e))
	}
	goodEmails := []string{"foo@bar", "foo.99@bar.com", "f@bar.exampl-3.com."}
	for _,e := range(goodEmails) {
		assert.True(t, IsValidEmail(e))
	}
	user, _ := NewUser("ValidName", "letmein", nil)
	user.Email = "foo"
	assert.False(t, user.Validate() == nil)
	user.Email = "foo@example.com"
	assert.True(t, user.Validate() == nil)
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
	user, _ := NewUser("me", "letmein", []string{"me", "public"})
	user.Email = "foo@example.com"
	encoded, _ := json.Marshal(user)
	assert.True(t, encoded != nil)

	var resu User
	err := json.Unmarshal(encoded, &resu)
	assert.True(t, err == nil)
	assert.DeepEquals(t, resu.Name, user.Name)
	assert.DeepEquals(t, resu.Email, user.Email)
	assert.DeepEquals(t, resu.PasswordHash, user.PasswordHash)
	assert.DeepEquals(t, resu.Channels, user.Channels)
	assert.True(t, resu.Authenticate("letmein"))
	assert.False(t, resu.Authenticate("123456"))
}

func TestUserAccess(t *testing.T) {
	// User with no access:
	user, _ := NewUser("foo", "password", nil)
	assert.False(t, user.CanSeeChannel("x"))
	assert.True(t, user.CanSeeAllChannels([]string{}))
	assert.False(t, user.CanSeeAllChannels([]string{"x"}))
	assert.False(t, user.CanSeeAllChannels([]string{"x", "y"}))
	assert.True(t, user.CanSeeAllChannels([]string{"*"}))
	assert.False(t, user.CanSeeAnyChannels([]string{}))
	assert.False(t, user.CanSeeAnyChannels([]string{"x"}))
	assert.False(t, user.CanSeeAnyChannels([]string{"x", "y"}))
	assert.True(t, user.CanSeeAnyChannels([]string{"*"}))

	// User with access to one channel:
	user.Channels = []string{"x"}
	assert.True(t, user.CanSeeAllChannels([]string{}))
	assert.True(t, user.CanSeeAllChannels([]string{"x"}))
	assert.False(t, user.CanSeeAllChannels([]string{"x", "y"}))
	assert.False(t, user.CanSeeAnyChannels([]string{}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x"}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x", "y"}))
	assert.False(t, user.AuthorizeAllChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.AuthorizeAnyChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.CanSeeAnyChannels([]string{"*"}))

	// User with access to two channels:
	user.Channels = []string{"x", "z"}
	assert.True(t, user.CanSeeAllChannels([]string{}))
	assert.True(t, user.CanSeeAllChannels([]string{"x"}))
	assert.False(t, user.CanSeeAllChannels([]string{"x", "y"}))
	assert.False(t, user.CanSeeAnyChannels([]string{}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x"}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x", "y"}))
	assert.False(t, user.AuthorizeAllChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.AuthorizeAnyChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.CanSeeAnyChannels([]string{"*"}))

	user.Channels = []string{"x", "y"}
	assert.True(t, user.CanSeeAllChannels([]string{}))
	assert.True(t, user.CanSeeAllChannels([]string{"x"}))
	assert.True(t, user.CanSeeAllChannels([]string{"x", "y"}))
	assert.False(t, user.CanSeeAnyChannels([]string{}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x"}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x", "y"}))
	assert.True(t, user.AuthorizeAllChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.AuthorizeAnyChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.CanSeeAnyChannels([]string{"*"}))

	// User with wildcard access:
	user.Channels = []string{"*", "q"}
	assert.True(t, user.CanSeeAllChannels([]string{}))
	assert.True(t, user.CanSeeAllChannels([]string{"x"}))
	assert.True(t, user.CanSeeAllChannels([]string{"x", "y"}))
	assert.True(t, user.CanSeeAnyChannels([]string{}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x"}))
	assert.True(t, user.CanSeeAnyChannels([]string{"x", "y"}))
	assert.True(t, user.AuthorizeAllChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.AuthorizeAnyChannels([]string{"x", "y"}) == nil)
	assert.True(t, user.CanSeeAnyChannels([]string{"*"}))
}

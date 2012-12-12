// auth_test.go

package channelsync

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
	encoded, _ := json.Marshal(user)
	assert.True(t, encoded != nil)

	var resu User
	err := json.Unmarshal(encoded, &resu)
	assert.True(t, err == nil)
	assert.DeepEquals(t, resu.Name, user.Name)
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

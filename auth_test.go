// auth_test.go

package channelsync

import (
	"github.com/sdegutis/go.assert"
	//"net/http"
	"testing"
)

func TestValidateGuestUser(t *testing.T) {
	user := User{"", "password", nil}
	assert.False(t, user.Validate() == nil)
	user.Password = ""
	assert.True(t, user.Validate() == nil)
}

func TestValidateUser(t *testing.T) {
	user := User{"invalid:name", "password", nil}
	assert.False(t, user.Validate() == nil)
	user.Name = "validName1"
	assert.True(t, user.Validate() == nil)
	user.Password = ""
	assert.False(t, user.Validate() == nil)
}

func TestUserAccess(t *testing.T) {
	// User with no access:
	user := User{"foo", "password", nil}
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

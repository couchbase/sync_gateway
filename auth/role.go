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
	"fmt"
	"net/http"
	"regexp"

	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

/** A group that users can belong to, with associated channel permisisons. */
type Role struct {
	Name          string `json:"name,omitempty"`
	AdminChannels ch.Set `json:"admin_channels"`
	AllChannels   ch.Set `json:"all_channels,omitempty"`
}

var kValidNameRegexp *regexp.Regexp

func init() {
	var err error
	kValidNameRegexp, err = regexp.Compile(`^[-+.@\w]*$`)
	if err != nil {
		panic("Bad kValidNameRegexp")
	}
}

func (role *Role) initRole(name string, channels ch.Set) error {
	channels = channels.ExpandingStar()
	role.Name = name
	role.AdminChannels = channels
	role.AllChannels = channels
	return role.Validate()
}

// Creates a new Role object.
func NewRole(name string, channels ch.Set) (*Role, error) {
	role := &Role{}
	return role, role.initRole(name, channels)
}

// Checks whether this User object contains valid data; if not, returns an error.
func (role *Role) Validate() error {
	if !kValidNameRegexp.MatchString(role.Name) {
		return &base.HTTPError{http.StatusBadRequest, fmt.Sprintf("Invalid name %q", role.Name)}
	}
	return nil
}

//////// CHANNEL AUTHORIZATION:

// If a channel list contains a wildcard ("*"), replace it with all the role's accessible channels.
// Do this before calling any of the CanSee or Authorize methods below, as they interpret a
// channel named "*" as, literally, the wildcard channel that contains all documents.
func (role *Role) ExpandWildCardChannel(channels ch.Set) ch.Set {
	if channels.Contains("*") {
		channels = role.AllChannels
		if channels == nil {
			channels = ch.Set{}
		}
	}
	return channels
}

func (role *Role) UnauthError(message string) error {
	if role.Name == "" {
		return &base.HTTPError{http.StatusUnauthorized, "login required"}
	}
	return &base.HTTPError{http.StatusForbidden, message}
}

// Returns true if the Role is allowed to access the channel.
// A nil Role means access control is disabled, so the function will return true.
func (role *Role) CanSeeChannel(channel string) bool {
	return role == nil || role.AllChannels.Contains(channel) || role.AllChannels.Contains("*")
}

// Returns true if the Role is allowed to access all of the given channels.
// A nil Role means access control is disabled, so the function will return true.
func (role *Role) CanSeeAllChannels(channels ch.Set) bool {
	if channels != nil {
		for channel, _ := range channels {
			if !role.CanSeeChannel(channel) {
				return false
			}
		}
	}
	return true
}

// Returns an HTTP 403 error if the Role is not allowed to access all the given channels.
// A nil Role means access control is disabled, so the function will return nil.
func (role *Role) AuthorizeAllChannels(channels ch.Set) error {
	var forbidden []string
	for channel, _ := range channels {
		if !role.CanSeeChannel(channel) {
			if forbidden == nil {
				forbidden = make([]string, 0, len(channels))
			}
			forbidden = append(forbidden, channel)
		}
	}
	if forbidden != nil {
		return role.UnauthError(fmt.Sprintf("You are not allowed to see channels %v", forbidden))
	}
	return nil
}

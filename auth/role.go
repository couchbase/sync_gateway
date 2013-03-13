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
type roleImpl struct {
	Name_             string `json:"name,omitempty"`
	ExplicitChannels_ ch.Set `json:"admin_channels"`
	Channels_         ch.Set `json:"all_channels,omitempty"`
}

var kValidNameRegexp *regexp.Regexp

func init() {
	var err error
	kValidNameRegexp, err = regexp.Compile(`^[-+.@\w]*$`)
	if err != nil {
		panic("Bad kValidNameRegexp")
	}
}

func (role *roleImpl) initRole(name string, channels ch.Set) error {
	channels = channels.ExpandingStar()
	role.Name_ = name
	role.ExplicitChannels_ = channels
	role.Channels_ = channels
	return role.validate()
}

// Creates a new Role object.
func NewRole(name string, channels ch.Set) (Role, error) {
	role := &roleImpl{}
	if err := role.initRole(name, channels); err != nil {
		return nil, err
	}
	return role, nil
}

func docIDForRole(name string) string {
	return "role:" + name
}

func (role *roleImpl) docID() string {
	return docIDForRole(role.Name_)
}

// Key used in 'access' view (not same meaning as doc ID)
func (role *roleImpl) accessViewKey() string {
	return "role:" + role.Name_
}

//////// ACCESSORS:

func (role *roleImpl) Name() string {
	return role.Name_
}

func (role *roleImpl) Channels() ch.Set {
	return role.Channels_
}

func (role *roleImpl) setChannels(channels ch.Set) {
	role.Channels_ = channels
}

func (role *roleImpl) ExplicitChannels() ch.Set {
	return role.ExplicitChannels_
}

// Checks whether this role object contains valid data; if not, returns an error.
func (role *roleImpl) validate() error {
	if !kValidNameRegexp.MatchString(role.Name_) {
		return &base.HTTPError{http.StatusBadRequest, fmt.Sprintf("Invalid name %q", role.Name_)}
	}
	return nil
}

//////// CHANNEL AUTHORIZATION:

// If a channel list contains a wildcard ("*"), replace it with all the role's accessible channels.
// Do this before calling any of the CanSee or Authorize methods below, as they interpret a
// channel named "*" as, literally, the wildcard channel that contains all documents.
func (role *roleImpl) ExpandWildCardChannel(channels ch.Set) ch.Set {
	if channels.Contains("*") {
		channels = role.Channels_
		if channels == nil {
			channels = ch.Set{}
		}
	}
	return channels
}

func (role *roleImpl) UnauthError(message string) error {
	if role.Name_ == "" {
		return &base.HTTPError{http.StatusUnauthorized, "login required"}
	}
	return &base.HTTPError{http.StatusForbidden, message}
}

// Returns true if the Role is allowed to access the channel.
// A nil Role means access control is disabled, so the function will return true.
func (role *roleImpl) CanSeeChannel(channel string) bool {
	return role == nil || role.Channels_.Contains(channel) || role.Channels_.Contains("*")
}

// Returns true if the Role is allowed to access all of the given channels.
// A nil Role means access control is disabled, so the function will return true.
func (role *roleImpl) CanSeeAllChannels(channels ch.Set) bool {
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
func (role *roleImpl) AuthorizeAllChannels(channels ch.Set) error {
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

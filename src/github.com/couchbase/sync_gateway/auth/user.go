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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

	"golang.org/x/crypto/bcrypt"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

const kBcryptCostFactor = bcrypt.DefaultCost

// Actual implementation of User interface
type userImpl struct {
	roleImpl // userImpl "inherits from" Role
	userImplBody
	auth  *Authenticator
	roles []Role
}

// Marshalable data is stored in separate struct from userImpl,
// to work around limitations of JSON marshaling.
type userImplBody struct {
	Email_           string      `json:"email,omitempty"`
	Disabled_        bool        `json:"disabled,omitempty"`
	PasswordHash_    []byte      `json:"passwordhash_bcrypt,omitempty"`
	OldPasswordHash_ interface{} `json:"passwordhash,omitempty"` // For pre-beta compatibility
	ExplicitRoles_   ch.TimedSet `json:"explicit_roles,omitempty"`
	RolesSince_      ch.TimedSet `json:"rolesSince"`

	OldExplicitRoles_ []string `json:"admin_roles,omitempty"` // obsolete; declared for migration
}

var kValidEmailRegexp *regexp.Regexp

func init() {
	var err error
	kValidEmailRegexp, err = regexp.Compile(`^[-+.\w]+@\w[-.\w]+$`)
	if err != nil {
		panic("Bad kValidEmailRegexp")
	}
}

func IsValidEmail(email string) bool {
	return kValidEmailRegexp.MatchString(email)
}

func (auth *Authenticator) defaultGuestUser() User {
	user := &userImpl{
		roleImpl: roleImpl{
			ExplicitChannels_: ch.AtSequence(ch.SetOf(), 1),
		},
		userImplBody: userImplBody{
			Disabled_: true,
		},
		auth: auth,
	}
	user.Channels_ = user.ExplicitChannels_.Copy()
	return user
}

// Creates a new User object.
func (auth *Authenticator) NewUser(username string, password string, channels base.Set) (User, error) {
	user := &userImpl{
		auth:         auth,
		userImplBody: userImplBody{RolesSince_: ch.TimedSet{}},
	}
	if err := user.initRole(username, channels); err != nil {
		return nil, err
	}
	if err := auth.rebuildChannels(user); err != nil {
		return nil, err
	}

	if err := auth.rebuildRoles(user); err != nil {
		return nil, err
	}

	user.SetPassword(password)
	return user, nil
}

func (auth *Authenticator) UnmarshalUser(data []byte, defaultName string, defaultSequence uint64) (User, error) {
	user := &userImpl{auth: auth}
	if err := json.Unmarshal(data, user); err != nil {
		return nil, err
	}
	if user.Name_ == "" {
		user.Name_ = defaultName
	}
	for channel, seq := range user.ExplicitChannels_ {
		if seq == 0 {
			user.ExplicitChannels_[channel] = defaultSequence
		}
	}
	if err := user.validate(); err != nil {
		return nil, err
	}
	return user, nil
}

// Checks whether this userImpl object contains valid data; if not, returns an error.
func (user *userImpl) validate() error {
	if err := (&user.roleImpl).validate(); err != nil {
		return err
	} else if user.Email_ != "" && !IsValidEmail(user.Email_) {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid email address")
	} else if user.OldPasswordHash_ != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Obsolete password hash present")
	}
	for roleName, _ := range user.ExplicitRoles_ {
		if !IsValidPrincipalName(roleName) {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid role name %q", roleName)
		}
	}
	return nil
}

// Key prefix reserved for user documents in the bucket
const UserKeyPrefix = "_sync:user:"

func docIDForUser(username string) string {
	return UserKeyPrefix + username
}

func (user *userImpl) docID() string {
	return docIDForUser(user.Name_)
}

// Key used in 'access' view (not same meaning as doc ID)
func (user *userImpl) accessViewKey() string {
	return user.Name_
}

func (user *userImpl) Disabled() bool {
	return user.Disabled_
}

func (user *userImpl) SetDisabled(disabled bool) {
	user.Disabled_ = disabled
}

func (user *userImpl) Email() string {
	return user.Email_
}

func (user *userImpl) SetEmail(email string) error {
	if email != "" && !IsValidEmail(email) {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid email address")
	}
	user.Email_ = email
	return nil
}

func (user *userImpl) RoleNames() ch.TimedSet {
	return user.RolesSince_
}

func (user *userImpl) setRolesSince(rolesSince ch.TimedSet) {
	user.RolesSince_ = rolesSince
	user.roles = nil // invalidate in-memory cache list of Role objects
}

func (user *userImpl) ExplicitRoles() ch.TimedSet {
	return user.ExplicitRoles_
}

func (user *userImpl) SetExplicitRoles(roles ch.TimedSet) {
	user.ExplicitRoles_ = roles
	user.setRolesSince(nil) // invalidate persistent cache of role names
}

// Returns true if the given password is correct for this user, and the account isn't disabled.
func (user *userImpl) Authenticate(password string) bool {
	if user == nil {
		return false
	} else if user.OldPasswordHash_ != nil {
		base.Warn("User account %q still has pre-beta password hash; need to reset password", user.Name_)
		return false // Password must be reset to use new (bcrypt) password hash
	} else if user.PasswordHash_ == nil {
		if password != "" {
			return false
		}
	} else if !compareHashAndPassword(user.PasswordHash_, []byte(password)) {
		return false
	}
	return !user.Disabled_
}

// Changes a user's password to the given string.
func (user *userImpl) SetPassword(password string) {
	if password == "" {
		user.PasswordHash_ = nil
	} else {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), kBcryptCostFactor)
		if err != nil {
			panic(fmt.Sprintf("Error hashing password: %v", err))
		}
		user.PasswordHash_ = hash
	}
}

//////// CHANNEL ACCESS:

func (user *userImpl) GetRoles() []Role {
	if user.roles == nil {
		roles := make([]Role, 0, len(user.RolesSince_))
		for name, _ := range user.RolesSince_ {
			role, err := user.auth.GetRole(name)
			//base.LogTo("Access", "User %s role %q = %v", user.Name_, name, role)
			if err != nil {
				panic(fmt.Sprintf("Error getting user role %q: %v", name, err))
			} else if role != nil {
				roles = append(roles, role)
			}
		}
		user.roles = roles
	}
	return user.roles
}

func (user *userImpl) CanSeeChannel(channel string) bool {
	if user.roleImpl.CanSeeChannel(channel) {
		return true
	}
	for _, role := range user.GetRoles() {
		if role.CanSeeChannel(channel) {
			return true
		}
	}
	return false
}

func (user *userImpl) CanSeeChannelSince(channel string) uint64 {
	minSeq := user.roleImpl.CanSeeChannelSince(channel)
	for _, role := range user.GetRoles() {
		if seq := role.CanSeeChannelSince(channel); seq > 0 && (seq < minSeq || minSeq == 0) {
			minSeq = seq
		}
	}
	return minSeq
}

func (user *userImpl) AuthorizeAllChannels(channels base.Set) error {
	return authorizeAllChannels(user, channels)
}

func (user *userImpl) AuthorizeAnyChannel(channels base.Set) error {
	return authorizeAnyChannel(user, channels)
}

func (user *userImpl) InheritedChannels() ch.TimedSet {
	channels := user.Channels().Copy()
	for _, role := range user.GetRoles() {
		roleSince := user.RolesSince_[role.Name()]
		channels.AddAtSequence(role.Channels(), roleSince)
	}
	return channels
}

// If a channel list contains the all-channel wildcard, replace it with all the user's accessible channels.
func (user *userImpl) ExpandWildCardChannel(channels base.Set) base.Set {
	if channels.Contains(ch.AllChannelWildcard) {
		channels = user.InheritedChannels().AsSet()
	}
	return channels
}

func (user *userImpl) FilterToAvailableChannels(channels base.Set) ch.TimedSet {
	output := ch.TimedSet{}
	for channel, _ := range channels {
		if channel == ch.AllChannelWildcard {
			return user.InheritedChannels().Copy()
		}
		output.AddChannel(channel, user.CanSeeChannelSince(channel))
	}
	return output
}

func (user *userImpl) GetAddedChannels(channels ch.TimedSet) base.Set {
	output := base.Set{}
	for userChannel, _ := range user.InheritedChannels() {
		_, found := channels[userChannel]
		if !found {
			output[userChannel] = struct{}{}
		}
	}
	return output
}

//////// MARSHALING:

// JSON encoding/decoding -- these functions are ugly hacks to work around the current
// Go 1.0.3 limitation that the JSON package won't traverse into unnamed/embedded
// fields of structs (i.e. the Role).

func (user *userImpl) MarshalJSON() ([]byte, error) {
	var err error
	data, err := json.Marshal(user.roleImpl)
	if err != nil {
		return nil, err
	}
	userData, err := json.Marshal(user.userImplBody)
	if err != nil {
		return nil, err
	}
	if len(userData) > 2 {
		// Splice the two JSON bodies together if the user data is not just "{}"
		data = data[0 : len(data)-1]
		userData = userData[1:len(userData)]
		data, err = bytes.Join([][]byte{data, userData}, []byte(",")), nil
	}
	return data, err
}

func (user *userImpl) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &user.userImplBody); err != nil {
		return err
	} else if err := json.Unmarshal(data, &user.roleImpl); err != nil {
		return err
	}

	// Migrate "admin_roles" field:
	if user.OldExplicitRoles_ != nil {
		user.ExplicitRoles_ = ch.AtSequence(base.SetFromArray(user.OldExplicitRoles_), 1)
		user.OldExplicitRoles_ = nil
	}

	return nil
}

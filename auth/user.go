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

	"github.com/dchest/passwordhash"

	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

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
	Email_        string                     `json:"email,omitempty"`
	Disabled_     bool                       `json:"disabled,omitempty"`
	PasswordHash_ *passwordhash.PasswordHash `json:"passwordhash,omitempty"`
	Password_     *string                    `json:"password,omitempty"`
	RoleNames_    []string                   `json:"roles,omitempty"`
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
	return &userImpl{
		roleImpl: roleImpl{
			ExplicitChannels_: ch.SetOf("*"),
			Channels_:         ch.SetOf("*"),
		},
		auth: auth,
	}
}

// Creates a new User object.
func (auth *Authenticator) NewUser(username string, password string, channels ch.Set) (User, error) {
	user := &userImpl{auth: auth}
	if err := user.initRole(username, channels); err != nil {
		return nil, err
	}
	user.SetPassword(password)
	return user, nil
}

func (auth *Authenticator) UnmarshalUser(data []byte, defaultName string) (User, error) {
	user := &userImpl{auth: auth}
	if err := json.Unmarshal(data, user); err != nil {
		return nil, err
	}
	if user.Name_ == "" {
		user.Name_ = defaultName
	}
	if user.Password_ != nil {
		user.SetPassword(*user.Password_)
		user.Password_ = nil
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
		return &base.HTTPError{http.StatusBadRequest, "Invalid email address"}
	} else if (user.Name_ == "") != (user.PasswordHash_ == nil) {
		// Real user must have a password; anon user must not have a password
		return &base.HTTPError{http.StatusBadRequest, "Invalid password"}
	}
	for _, roleName := range user.RoleNames_ {
		if !kValidNameRegexp.MatchString(roleName) {
			return &base.HTTPError{http.StatusBadRequest, fmt.Sprintf("Invalid role name %q", roleName)}
		}
	}
	return nil
}

func docIDForUser(username string) string {
	return "user:" + username
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

func (user *userImpl) Email() string {
	return user.Email_
}

func (user *userImpl) SetEmail(email string) error {
	if email != "" && !IsValidEmail(email) {
		return &base.HTTPError{http.StatusBadRequest, "Invalid email address"}
	}
	user.Email_ = email
	return nil
}

func (user *userImpl) RoleNames() []string {
	return user.RoleNames_
}

func (user *userImpl) SetRoleNames(names []string) {
	user.RoleNames_ = names
	user.roles = nil // invalidate cache
}

// Returns true if the given password is correct for this user.
func (user *userImpl) Authenticate(password string) bool {
	if user == nil {
		return false
	}
	if user.PasswordHash_ == nil {
		if password != "" {
			return false
		}
	} else if !user.PasswordHash_.EqualToPassword(password) {
		return false
	}
	return !user.Disabled_
}

// Changes a user's password to the given string.
func (user *userImpl) SetPassword(password string) {
	if password == "" {
		user.PasswordHash_ = nil
	} else {
		user.PasswordHash_ = passwordhash.New(password)
	}
}

//////// CHANNEL ACCESS:

func (user *userImpl) GetRoles() []Role {
	if user.roles == nil {
		roles := make([]Role, 0, len(user.RoleNames_))
		for _, name := range user.RoleNames_ {
			role, err := user.auth.GetRole(name)
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

func (user *userImpl) AuthorizeAllChannels(channels ch.Set) error {
	return authorizeAllChannels(user, channels)
}

func (user *userImpl) InheritedChannels() ch.Set {
	channels := user.Channels()
	// This could be optimized to do less copying.
	for _, role := range user.GetRoles() {
		channels = channels.Union(role.Channels())
	}
	return channels
}

// If a channel list contains a wildcard ("*"), replace it with all the user's accessible channels.
func (user *userImpl) ExpandWildCardChannel(channels ch.Set) ch.Set {
	if channels.Contains("*") {
		channels = user.InheritedChannels()
		if channels == nil {
			channels = ch.Set{}
		}
	}
	return channels
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
	// Splice the two JSON bodies together:
	data = data[0 : len(data)-1]
	userData = userData[1:len(userData)]
	return bytes.Join([][]byte{data, userData}, []byte(",")), nil
}

func (user *userImpl) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &user.userImplBody); err != nil {
		return err
	} else if err := json.Unmarshal(data, &user.roleImpl); err != nil {
		return err
	}
	return nil
}

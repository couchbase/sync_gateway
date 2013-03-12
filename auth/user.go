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
	"net/http"
	"regexp"

	"github.com/dchest/passwordhash"

	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

/** Persistent information about a user. */
type User struct {
	Role                                    // User "inherits from" Role
	Email        string                     `json:"email,omitempty"`
	Disabled     bool                       `json:"disabled,omitempty"`
	PasswordHash *passwordhash.PasswordHash `json:"passwordhash,omitempty"`
	Password     *string                    `json:"password,omitempty"`
	RoleNames    []string                   `json:"roles,omitempty"`
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

func defaultGuestUser() *User {
	return &User{
		Role: Role{
			AdminChannels: ch.SetOf("*"),
			AllChannels:   ch.SetOf("*"),
		},
	}
}

// Creates a new User object.
func NewUser(username string, password string, channels ch.Set) (*User, error) {
	user := &User{}
	if err := user.initRole(username, channels); err != nil {
		return nil, err
	}
	user.SetPassword(password)
	return user, nil
}

// Checks whether this User object contains valid data; if not, returns an error.
func (user *User) Validate() error {
	if err := (&user.Role).Validate(); err != nil {
		return err
	} else if user.Email != "" && !IsValidEmail(user.Email) {
		return &base.HTTPError{http.StatusBadRequest, "Invalid email address"}
	}
	return nil
}

// Returns true if the given password is correct for this user.
func (user *User) Authenticate(password string) bool {
	if user == nil {
		return false
	}
	if user.PasswordHash == nil {
		if password != "" {
			return false
		}
	} else if !user.PasswordHash.EqualToPassword(password) {
		return false
	}
	return !user.Disabled
}

// Changes a user's password to the given string.
func (user *User) SetPassword(password string) {
	if password == "" {
		user.PasswordHash = nil
	} else {
		user.PasswordHash = passwordhash.New(password)
	}
}

// JSON encoding/decoding -- these functions are ugly hacks to work around the current
// Go 1.0.3 limitation that the JSON package won't traverse into unnamed/embedded
// fields of structs (i.e. the Role).

func (user *User) Marshal() ([]byte, error) {
	var err error
	data, err := json.Marshal(user.Role)
	if err != nil {
		return nil, err
	}
	userData, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}
	data = data[0 : len(data)-1]
	userData = userData[1:len(userData)]
	return bytes.Join([][]byte{data, userData}, []byte(",")), nil
}

func UnmarshalUser(data []byte) (*User, error) {
	user := &User{}
	if err := json.Unmarshal(data, user); err != nil {
		return nil, err
	} else if err := json.Unmarshal(data, &user.Role); err != nil {
		return nil, err
	}
	return user, nil
}

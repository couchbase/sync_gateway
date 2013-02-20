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
	"net/http"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

/** Manages user authentication for a database. */
type Authenticator struct {
	bucket *couchbase.Bucket
}

type userByEmailInfo struct {
	Username string
}

// Creates a new Authenticator that stores user info in the given Bucket.
func NewAuthenticator(bucket *couchbase.Bucket) *Authenticator {
	return &Authenticator{
		bucket: bucket,
	}
}

func docIDForUser(username string) string {
	return "user:" + username
}

func docIDForUserEmail(email string) string {
	return "useremail:" + email
}

// Looks up the information for a user.
// If the username is "" it will return the default (guest) User object, not nil.
// By default the guest User has access to everything, i.e. Admin Party! This can
// be changed by altering its list of channels and saving the changes via SetUser.
func (auth *Authenticator) GetUser(username string) (*User, error) {
	var user *User
	err := auth.bucket.Get(docIDForUser(username), &user)
	if err != nil && !base.IsDocNotFoundError(err) {
		return nil, err
	}
	if user == nil && username == "" {
		// Default value of 'guest' User:
		user = &User{
			Name:          username,
			AdminChannels: []string{"*"},
			AllChannels:   []string{"*"},
		}
	}
	if user == nil {
		return nil, nil
	}
	if user.AllChannels == nil {
		// Channel list has been invalidated by a doc update -- rebuild from view:
		opts := map[string]interface{}{"stale": false, "key": user.Name}
		vres := couchbase.ViewResult{}
		if verr := auth.bucket.ViewCustom("sync_gateway", "access", opts, &vres); verr != nil {
			return nil, verr
		}

		allChannels := ch.SetFromArray(user.AdminChannels)
		for _, row := range vres.Rows {
			value := row.Value.([]interface{})
			for _, item := range value {
				allChannels[item.(string)] = true
			}
		}
		user.AllChannels = allChannels.ToArray()

		//FIX: This update needs to be done as a CAS
		if err := auth.SaveUser(user); err != nil {
			return nil, err
		}
	}
	return user, nil
}

// Looks up a User by email address.
func (auth *Authenticator) GetUserByEmail(email string) (*User, error) {
	var info userByEmailInfo
	err := auth.bucket.Get(docIDForUserEmail(email), &info)
	if base.IsDocNotFoundError(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return auth.GetUser(info.Username)
}

// Saves the information for a user.
func (auth *Authenticator) SaveUser(user *User) error {
	user.AdminChannels = ch.SimplifyChannels(user.AdminChannels, true)
	if user.Password != nil {
		user.SetPassword(*user.Password)
		user.Password = nil
	}
	if err := user.Validate(); err != nil {
		return err
	}
	if (user.Name == "") != (user.PasswordHash == nil) {
		// Real user must have a password; anon user must not have a password
		return &base.HTTPError{http.StatusBadRequest, "Invalid password"}
	}

	if err := auth.bucket.Set(docIDForUser(user.Name), 0, user); err != nil {
		return err
	}
	if user.Email != "" {
		info := userByEmailInfo{user.Name}
		if err := auth.bucket.Set(docIDForUserEmail(user.Email), 0, info); err != nil {
			return err
		}
		//FIX: Fail if email address is already registered to another user
		//FIX: Unregister old email address if any
	}
	return nil
}

// Invalidates the channel list of a user by saving its AllChannels property as nil.
func (auth *Authenticator) InvalidateUserChannels(name string) error {
	user, err := auth.GetUser(name)
	if err != nil {
		return err
	}
	if user != nil && user.AllChannels != nil {
		user.AllChannels = nil
		if err := auth.SaveUser(user); err != nil {
			return err
		}
	}
	return nil
}

// Deletes a user.
func (auth *Authenticator) DeleteUser(user *User) error {
	if user.Email != "" {
		auth.bucket.Delete(docIDForUserEmail(user.Email))
	}
	return auth.bucket.Delete(docIDForUser(user.Name))
}

// Authenticates a user given the username and password.
// If the username and password are both "", it will return a default empty User object, not nil.
func (auth *Authenticator) AuthenticateUser(username string, password string) *User {
	user, _ := auth.GetUser(username)
	if user == nil || !user.Authenticate(password) {
		return nil
	}
	return user
}

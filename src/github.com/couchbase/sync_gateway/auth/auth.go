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

	"github.com/couchbase/go-couchbase"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

/** Manages user authentication for a database. */
type Authenticator struct {
	bucket          base.Bucket
	channelComputer ChannelComputer
}

// Interface for deriving the set of channels and roles a User/Role has access to.
// The instantiator of an Authenticator must provide an implementation.
type ChannelComputer interface {
	ComputeChannelsForPrincipal(Principal) (ch.TimedSet, error)
	ComputeRolesForUser(User) (ch.TimedSet, error)
}

type userByEmailInfo struct {
	Username string
}

// Creates a new Authenticator that stores user info in the given Bucket.
func NewAuthenticator(bucket base.Bucket, channelComputer ChannelComputer) *Authenticator {
	return &Authenticator{
		bucket:          bucket,
		channelComputer: channelComputer,
	}
}

func docIDForUserEmail(email string) string {
	return "_sync:useremail:" + email
}

func (auth *Authenticator) UnmarshalPrincipal(data []byte, defaultName string, defaultSeq uint64, isUser bool) (Principal, error) {
	if isUser {
		return auth.UnmarshalUser(data, defaultName, defaultSeq)
	}
	return auth.UnmarshalRole(data, defaultName, defaultSeq)
}

func (auth *Authenticator) GetPrincipal(name string, isUser bool) (Principal, error) {
	if isUser {
		return auth.GetUser(name)
	}
	return auth.GetRole(name)
}

// Looks up the information for a user.
// If the username is "" it will return the default (guest) User object, not nil.
// By default the guest User has access to everything, i.e. Admin Party! This can
// be changed by altering its list of channels and saving the changes via SetUser.
func (auth *Authenticator) GetUser(name string) (User, error) {
	princ, err := auth.getPrincipal(docIDForUser(name), func() Principal { return &userImpl{} })
	if err != nil {
		return nil, err
	} else if princ == nil {
		if name == "" {
			princ = auth.defaultGuestUser()
		} else {
			return nil, nil
		}
	}
	princ.(*userImpl).auth = auth
	return princ.(User), err
}

// Looks up the information for a role.
func (auth *Authenticator) GetRole(name string) (Role, error) {
	princ, err := auth.getPrincipal(docIDForRole(name), func() Principal { return &roleImpl{} })
	role, _ := princ.(Role)
	return role, err
}

// Common implementation of GetUser and GetRole. factory() parameter returns a new empty instance.
func (auth *Authenticator) getPrincipal(docID string, factory func() Principal) (Principal, error) {
	var princ Principal

	err := auth.bucket.Update(docID, 0, func(currentValue []byte) ([]byte, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if currentValue == nil {
			princ = nil
			return nil, couchbase.UpdateCancel
		}
		princ = factory()
		if err := json.Unmarshal(currentValue, princ); err != nil {
			return nil, err
		}
		changed := false
		if princ.Channels() == nil {
			// Channel list has been invalidated by a doc update -- rebuild it:
			if err := auth.rebuildChannels(princ); err != nil {
				return nil, err
			}
			changed = true
		}
		if user, ok := princ.(User); ok {
			if user.RoleNames() == nil {
				if err := auth.rebuildRoles(user); err != nil {
					return nil, err
				}
				changed = true
			}
		}

		if changed {
			// Save the updated doc:
			return json.Marshal(princ)
		} else {
			// Principal is valid, so stop the update
			return nil, couchbase.UpdateCancel
		}
	})

	if err != nil && err != couchbase.UpdateCancel {
		return nil, err
	}
	return princ, nil
}

func (auth *Authenticator) rebuildChannels(princ Principal) error {
	channels := princ.ExplicitChannels().Copy()
	if auth.channelComputer != nil {
		set, err := auth.channelComputer.ComputeChannelsForPrincipal(princ)
		if err != nil {
			base.Warn("channelComputer.ComputeChannelsForPrincipal failed on %s: %v", princ, err)
			return err
		}
		channels.Add(set)
	}
	// always grant access to the public document channel
	channels.AddChannel(ch.DocumentStarChannel, 1)

	base.LogTo("Access", "Computed channels for %q: %s", princ.Name(), channels)
	princ.setChannels(channels)
	return nil
}

func (auth *Authenticator) rebuildRoles(user User) error {
	var roles ch.TimedSet
	if auth.channelComputer != nil {
		var err error
		roles, err = auth.channelComputer.ComputeRolesForUser(user)
		if err != nil {
			base.Warn("channelComputer.ComputeRolesForUser failed on user %s: %v", user.Name(), err)
			return err
		}
	}
	if roles == nil {
		roles = ch.TimedSet{} // it mustn't be nil; nil means it's unknown
	}

	if explicit := user.ExplicitRoles(); explicit != nil {
		roles.Add(explicit)
	}

	base.LogTo("Access", "Computed roles for %q: %s", user.Name(), roles)
	user.setRolesSince(roles)
	return nil
}

// Looks up a User by email address.
func (auth *Authenticator) GetUserByEmail(email string) (User, error) {
	var info userByEmailInfo
	err := auth.bucket.Get(docIDForUserEmail(email), &info)
	if base.IsDocNotFoundError(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return auth.GetUser(info.Username)
}

// Saves the information for a user/role.
func (auth *Authenticator) Save(p Principal) error {
	if err := p.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if err := auth.bucket.SetRaw(p.docID(), 0, data); err != nil {
		return err
	}
	if user, ok := p.(User); ok {
		if user.Email() != "" {
			info := userByEmailInfo{user.Name()}
			if err := auth.bucket.Set(docIDForUserEmail(user.Email()), 0, info); err != nil {
				return err
			}
			//FIX: Fail if email address is already registered to another user
			//FIX: Unregister old email address if any
		}
	}
	base.LogTo("Auth", "Saved %s: %s", p.docID(), data)
	return nil
}

// Invalidates the channel list of a user/role by saving its Channels() property as nil.
func (auth *Authenticator) InvalidateChannels(p Principal) error {
	if p != nil && p.Channels() != nil {
		base.LogTo("Access", "Invalidate access of %q", p.Name())
		p.setChannels(nil)
		if err := auth.Save(p); err != nil {
			return err
		}
	}
	return nil
}

// Invalidates the role list of a user by saving its Roles() property as nil.
func (auth *Authenticator) InvalidateRoles(user User) error {
	if user != nil && user.Channels() != nil {
		base.LogTo("Access", "Invalidate roles of %q", user.Name())
		user.setRolesSince(nil)
		if err := auth.Save(user); err != nil {
			return err
		}
	}
	return nil
}

// Deletes a user/role.
func (auth *Authenticator) Delete(p Principal) error {
	if user, ok := p.(User); ok {
		if user.Email() != "" {
			auth.bucket.Delete(docIDForUserEmail(user.Email()))
		}
	}
	return auth.bucket.Delete(p.docID())
}

// Authenticates a user given the username and password.
// If the username and password are both "", it will return a default empty User object, not nil.
func (auth *Authenticator) AuthenticateUser(username string, password string) User {
	user, _ := auth.GetUser(username)
	if user == nil || !user.Authenticate(password) {
		return nil
	}
	return user
}

// Registers a new user account based on the given verified email address.
// Username will be the same as the verified email address. Password will be random.
// The user will have access to no channels.
func (auth *Authenticator) RegisterNewUser(username, email string) (User, error) {
	user, err := auth.NewUser(username, base.GenerateRandomSecret(), base.Set{})
	if err != nil {
		return nil, err
	}
	user.SetEmail(email)
	err = auth.Save(user)
	if err != nil {
		return nil, err
	}
	return user, err
}

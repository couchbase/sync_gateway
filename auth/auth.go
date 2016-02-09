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
	UseGlobalSequence() bool
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

	// Changes for vbucket sequence management.  We can't determine relative ordering of sequences
	// across vbuckets.  To avoid redundant channel backfills during changes processing, we maintain
	// the previous vb/seq for a channel in PreviousChannels.  If that channel is still present during
	// this rebuild, we reuse the vb/seq from PreviousChannels (using UpdateIfPresent).  If PreviousChannels
	// is nil, reverts to normal sequence handling.

	previousChannels := princ.PreviousChannels().Copy()
	if previousChannels != nil {
		channels.UpdateIfPresent(previousChannels)
	}

	if auth.channelComputer != nil {
		viewChannels, err := auth.channelComputer.ComputeChannelsForPrincipal(princ)
		if err != nil {
			base.Warn("channelComputer.ComputeChannelsForPrincipal failed on %v: %v", princ, err)
			return err
		}
		if previousChannels != nil {
			viewChannels.UpdateIfPresent(previousChannels)
		}
		channels.Add(viewChannels)
	}
	// always grant access to the public document channel
	channels.AddChannel(ch.DocumentStarChannel, 1)

	base.LogTo("Access", "Computed channels for %q: %s", princ.Name(), channels)
	princ.SetPreviousChannels(nil)
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
	_, err := auth.bucket.Get(docIDForUserEmail(email), &info)
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
	if err := auth.bucket.SetRaw(p.DocID(), 0, data); err != nil {
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
	base.LogTo("Auth", "Saved %s: %s", p.DocID(), data)
	return nil
}

// Invalidates the channel list of a user/role by saving its Channels() property as nil.
func (auth *Authenticator) InvalidateChannels(p Principal) error {
	if p != nil && p.Channels() != nil {
		base.LogTo("Access", "Invalidate access of %q", p.Name())
		if auth.channelComputer != nil && !auth.channelComputer.UseGlobalSequence() {
			p.SetPreviousChannels(p.Channels())
		}
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
	return auth.bucket.Delete(p.DocID())
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

func (auth *Authenticator) UpdateRoleVbucketSequences(docID string, sequence uint64) error {
	return auth.updateVbucketSequences(docID, func() Principal { return &roleImpl{} }, sequence)
}

func (auth *Authenticator) UpdateUserVbucketSequences(docID string, sequence uint64) error {
	return auth.updateVbucketSequences(docID, func() Principal { return &userImpl{} }, sequence)
}

// Updates any entries in the admin (explicit) channel set that have
// their sequence set to zero, to the provided sequence, and invalidates the user channels.
// Used during distributed index processing, where the sequence value is the vb sequence, and
// isn't known at initial write time. Using bucket.Update for cas handling, similar to updatePrincipal.
func (auth *Authenticator) updateVbucketSequences(docID string, factory func() Principal, seq uint64) error {

	sequence := ch.NewVbSimpleSequence(seq)
	err := auth.bucket.Update(docID, 0, func(currentValue []byte) ([]byte, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if currentValue == nil {
			return nil, couchbase.UpdateCancel
		}
		princ := factory()
		if err := json.Unmarshal(currentValue, princ); err != nil {
			return nil, err
		}
		channelsChanged := false
		for channel, vbSeq := range princ.ExplicitChannels() {
			seq := vbSeq.Sequence
			if seq == 0 {
				switch p := princ.(type) {
				case *roleImpl:
					p.ExplicitChannels_[channel] = sequence
				case *userImpl:
					p.ExplicitChannels_[channel] = sequence
				}
				channelsChanged = true
			}
		}
		// Invalidate calculated channels if changed.
		if channelsChanged {
			princ.setChannels(nil)
		}

		// If user, also check for explicit roles.
		rolesChanged := false
		if userPrinc, ok := princ.(*userImpl); ok {
			for role, vbSeq := range userPrinc.ExplicitRoles() {
				seq := vbSeq.Sequence
				if seq == 0 {
					userPrinc.ExplicitRoles_[role] = sequence
					rolesChanged = true
				}
			}
			// Invalidate calculated roles if changed.
			if rolesChanged {
				userPrinc.setRolesSince(nil)
			}
		}

		princ.SetSequence(seq)

		if channelsChanged || rolesChanged {
			// Save the updated principal doc.
			return json.Marshal(princ)
		} else {
			// No entries found requiring update, so cancel update.
			return nil, couchbase.UpdateCancel
		}
	})

	if err != nil && err != couchbase.UpdateCancel {
		return err
	}
	return nil
}

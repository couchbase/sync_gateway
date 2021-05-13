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
	"fmt"
	"net/http"
	"regexp"

	"golang.org/x/crypto/bcrypt"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

// Actual implementation of User interface
type userImpl struct {
	roleImpl // userImpl "inherits from" Role
	userImplBody
	auth  *Authenticator
	roles []Role
}

// Marshallable data is stored in separate struct from userImpl,
// to work around limitations of JSON marshaling.
type userImplBody struct {
	Email_           string          `json:"email,omitempty"`
	Disabled_        bool            `json:"disabled,omitempty"`
	PasswordHash_    []byte          `json:"passwordhash_bcrypt,omitempty"`
	OldPasswordHash_ interface{}     `json:"passwordhash,omitempty"` // For pre-beta compatibility
	ExplicitRoles_   ch.TimedSet     `json:"explicit_roles,omitempty"`
	RolesSince_      ch.TimedSet     `json:"rolesSince"`
	RoleInvalSeq     uint64          `json:"role_inval_seq,omitempty"`
	RoleHistory_     TimedSetHistory `json:"role_history,omitempty"`

	OldExplicitRoles_ []string `json:"admin_roles,omitempty"` // obsolete; declared for migration
}

// Permissive email validation (accepts ALL valid emails, but does not reject all invalid emails)
// Regexp in English: Allow one, and only one @ symbol between any two things
var kValidEmailRegexp = regexp.MustCompile(`^[^@]+@[^@]+$`)

type VBHashFunction func(string) uint32

func IsValidEmail(email string) bool {
	return kValidEmailRegexp.MatchString(email)
}

func (auth *Authenticator) defaultGuestUser() User {
	user := &userImpl{
		roleImpl: roleImpl{
			ExplicitChannels_: ch.AtSequence(make(base.Set, 0), 1),
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

// Checks whether this userImpl object contains valid data; if not, returns an error.
func (user *userImpl) validate() error {
	if err := (&user.roleImpl).validate(); err != nil {
		return err
	} else if user.Email_ != "" && !IsValidEmail(user.Email_) {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid email address")
	} else if user.OldPasswordHash_ != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Obsolete password hash present")
	}
	for roleName := range user.ExplicitRoles_ {
		if !IsValidPrincipalName(roleName) {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid role name %q", roleName)
		}
	}
	return nil
}

func docIDForUser(username string) string {
	return base.UserPrefix + username
}

func (user *userImpl) DocID() string {
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
	if user.RoleInvalSeq != 0 {
		return nil
	}
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

func (user *userImpl) GetRoleInvalSeq() uint64 {
	return user.RoleInvalSeq
}

func (user *userImpl) SetRoleInvalSeq(invalSeq uint64) {
	user.RoleInvalSeq = invalSeq
}

func (user *userImpl) InvalidatedRoles() ch.TimedSet {
	if user.RoleInvalSeq != 0 {
		return user.RolesSince_
	}
	return nil
}

func (user *userImpl) SetRoleHistory(history TimedSetHistory) {
	user.RoleHistory_ = history
}

func (user *userImpl) RoleHistory() TimedSetHistory {
	return user.RoleHistory_
}

// Type is used to store a pair of channel names to triggered by sequences
// If there already exists the channel in the map it'll only update its sequence if the sequence being added is greater
type RevokedChannels map[string]uint64

func (revokedChannels RevokedChannels) add(chanName string, triggeredBy uint64) {
	if currentVal, ok := revokedChannels[chanName]; !ok || triggeredBy > currentVal {
		revokedChannels[chanName] = triggeredBy
	}
}

// RevokedChannels returns a map of revoked channels => most recent sequence at which access to that channel was lost
// Steps:
// Get revoked roles and for each:
// 	- Revoke the current channels if the role is deleted
//  - Revoke the role revoked channels
// Get current roles and for each:
//  - Revoke the role revoked channels
// Get user:
//  - Revoke users revoked channels
func (user *userImpl) RevokedChannels(since uint64) RevokedChannels {
	accessibleChannels := user.InheritedChannels()

	// Get revoked roles
	rolesToRevoke := map[string]uint64{}
	roleHistory := user.RoleHistory()
	for roleName, history := range roleHistory {
		if !user.RoleNames().Contains(roleName) {
			for _, entry := range history.Entries {
				if entry.StartSeq <= since && entry.EndSeq > since {
					mostRecentEndSeq := history.Entries[len(history.Entries)-1]
					rolesToRevoke[roleName] = mostRecentEndSeq.EndSeq
				}
			}
		}
	}

	// Store revoked channels to return
	// addToCombined adds to this return map or updates if required based on requirement to have largest triggeredBy val
	combinedRevokedChannels := RevokedChannels{}

	// revokeChannelHistoryProcessing iterates over a principals channel history and if not accessible add to combined
	revokeChannelHistoryProcessing := func(princ Principal) {
		for chanName, history := range princ.ChannelHistory() {
			if !accessibleChannels.Contains(chanName) {
				for _, entry := range history.Entries {
					if entry.StartSeq <= since && entry.EndSeq > since {
						mostRecentEndSeq := history.Entries[len(history.Entries)-1]
						combinedRevokedChannels.add(chanName, mostRecentEndSeq.EndSeq)
					}
				}
			}
		}
	}

	// Iterate over revoked roles and revoke ALL channels (current and previous) from revoked roles that we don't have
	// from another grant
	for roleName, roleRevokeSeq := range rolesToRevoke {
		role, err := user.auth.GetRoleIncDeleted(roleName)
		if err != nil || role == nil {
			base.Warnf("unable to obtain role %s to calculate channel revocation: %v. Will continue", base.UD(roleName), err)
			continue
		}

		// First check 'current channels' if role isn't deleted
		// Current roles should be invalidated on deleted anyway but for safety
		if !role.IsDeleted() {
			for _, chanName := range role.Channels().AllChannels() {
				if !accessibleChannels.Contains(chanName) {
					combinedRevokedChannels.add(chanName, roleRevokeSeq)
				}
			}
		}

		// Second check the channel history and add any revoked channels
		for chanName, history := range role.ChannelHistory() {
			if !accessibleChannels.Contains(chanName) {
				for _, channelEntry := range history.Entries {
					if channelEntry.StartSeq <= since && channelEntry.EndSeq > since {
						// If triggeredBy falls in channel history grant period then revocation actually caused by role
						// revocation. So use triggeredBy.
						// Otherwise this was a channel revocation whilst role was still assigned. So use end seq.
						if channelEntry.EndSeq > roleRevokeSeq {
							combinedRevokedChannels.add(chanName, roleRevokeSeq)
						} else {
							mostRecentEndSeq := history.Entries[len(history.Entries)-1]
							combinedRevokedChannels.add(chanName, mostRecentEndSeq.EndSeq)
						}
					}
				}
			}
		}

	}

	// Iterate over current roles and revoke any revoked channels inside role provided that channel isn't accessible
	// from another grant
	for _, role := range user.GetRoles() {
		revokeChannelHistoryProcessing(role)
	}

	// Lastly get the revoked channels based off of channel history on the user itself
	revokeChannelHistoryProcessing(user)

	return combinedRevokedChannels
}

// Returns true if the given password is correct for this user, and the account isn't disabled.
func (user *userImpl) Authenticate(password string) bool {
	if user == nil {
		return false
	}

	// exit early for disabled user accounts
	if user.Disabled_ {
		return false
	}

	// exit early if old hash is present
	if user.OldPasswordHash_ != nil {
		base.Warnf("User account %q still has pre-beta password hash; need to reset password", base.UD(user.Name_))
		return false // Password must be reset to use new (bcrypt) password hash
	}

	// bcrypt hash present
	if user.PasswordHash_ != nil {
		if !compareHashAndPassword(cachedHashes, user.PasswordHash_, []byte(password)) {
			// incorrect password
			return false
		}

		// password was correct, we'll rehash the password if required
		// e.g: in the case of bcryptCost changes
		if err := user.auth.rehashPassword(user, password); err != nil {
			// rehash is best effort, just log a warning on error.
			base.Warnf("Error when rehashing password for user %s: %v", base.UD(user.Name()), err)
		}
	} else {
		// no hash, but (incorrect) password provided
		if password != "" {
			return false
		}
	}

	return true
}

// Changes a user's password to the given string.
func (user *userImpl) SetPassword(password string) {
	if password == "" {
		user.PasswordHash_ = nil
	} else {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
		if err != nil {
			panic(fmt.Sprintf("Error hashing password: %v", err))
		}
		user.PasswordHash_ = hash
	}
}

//////// CHANNEL ACCESS:

func (user *userImpl) GetRoles() []Role {
	if user.roles == nil {
		roles := make([]Role, 0, len(user.RoleNames()))
		for name := range user.RoleNames() {
			role, err := user.auth.GetRole(name)
			//base.Infof(base.KeyAccess, "User %s role %q = %v", base.UD(user.Name_), base.UD(name), base.UD(role))
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
		roleSince := user.RoleNames()[role.Name()]
		channels.AddAtSequence(role.Channels(), roleSince.Sequence)
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
	for channel := range channels {
		if channel == ch.AllChannelWildcard {
			return user.InheritedChannels().Copy()
		}
		output.AddChannel(channel, user.CanSeeChannelSince(channel))
	}
	return output
}

func (user *userImpl) GetAddedChannels(channels ch.TimedSet) base.Set {
	output := base.Set{}
	for userChannel := range user.InheritedChannels() {
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
	data, err := base.JSONMarshal(user.roleImpl)
	if err != nil {
		return nil, err
	}
	userData, err := base.JSONMarshal(user.userImplBody)
	if err != nil {
		return nil, err
	}
	if len(userData) > 2 {
		// Splice the two JSON bodies together if the user data is not just "{}"
		data = data[0 : len(data)-1]
		userData = userData[1:]
		data, err = bytes.Join([][]byte{data, userData}, []byte(",")), nil
	}
	return data, err
}

func (user *userImpl) UnmarshalJSON(data []byte) error {
	if err := base.JSONUnmarshal(data, &user.userImplBody); err != nil {
		return err
	} else if err := base.JSONUnmarshal(data, &user.roleImpl); err != nil {
		return err
	}

	// Migrate "admin_roles" field:
	if user.OldExplicitRoles_ != nil {
		user.ExplicitRoles_ = ch.AtSequence(base.SetFromArray(user.OldExplicitRoles_), 1)
		user.OldExplicitRoles_ = nil
	}

	return nil
}

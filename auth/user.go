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

type VBHashFunction func(string) uint32

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
	defaultVbSequence := ch.NewVbSimpleSequence(defaultSequence)
	for channel, seq := range user.ExplicitChannels_ {
		if seq.Sequence == 0 {
			user.ExplicitChannels_[channel] = defaultVbSequence
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
	for roleName := range user.ExplicitRoles_ {
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

// Returns the sequence number since which the user has been able to access the channel, else zero.  Sets the vb
// for an admin channel grant, if needed.
func (user *userImpl) CanSeeChannelSinceVbSeq(channel string, hashFunction VBHashFunction) (base.VbSeq, bool) {
	seq, ok := user.Channels_[channel]
	if !ok {
		seq, ok = user.Channels_[ch.UserStarChannel]
		if !ok {
			return base.VbSeq{}, false
		}
	}
	if seq.VbNo == nil {
		userDocVbno := user.getVbNo(hashFunction)
		seq.VbNo = &userDocVbno
	}
	return base.VbSeq{*seq.VbNo, seq.Sequence}, true
}

func (user *userImpl) getVbNo(hashFunction VBHashFunction) uint16 {
	if user.vbNo == nil {
		calculatedVbNo := uint16(hashFunction(user.DocID()))
		user.vbNo = &calculatedVbNo
	}
	return *user.vbNo
}

func (user *userImpl) ValidateGrant(vbSeq *ch.VbSequence, hashFunction VBHashFunction) bool {

	// If the sequence is zero, this is an admin grant that hasn't been updated by accel - ignore
	if vbSeq.Sequence == 0 {
		return false
	}

	// If vbSeq is nil, this is an admin grant.  Set the vb to the vb of the user doc
	if vbSeq.VbNo == nil {
		calculatedVbNo := user.getVbNo(hashFunction)
		vbSeq.VbNo = &calculatedVbNo
	}
	return true
}

//////// CHANNEL ACCESS:

func (user *userImpl) GetRoles() []Role {
	if user.roles == nil {
		roles := make([]Role, 0, len(user.RolesSince_))
		for name := range user.RolesSince_ {
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

/////////// Support for vb.seq based comparison of channel, role, and role channel grants

// If a channel list contains the all-channel wildcard, replace it with all the user's accessible channels.
func (user *userImpl) ExpandWildCardChannelSince(channels base.Set, since base.SequenceClock) base.Set {
	if channels.Contains(ch.AllChannelWildcard) {
		channelSet, _ := user.InheritedChannelsForClock(since)
		channels = channelSet.AsSet()
	}
	return channels
}

// FilterToAvailableChannelsForSince is used for clock-based changes, and gives priority to vb.seq values
// earlier than the provided since clock when returning results (because that means the channel doesn't require
// backfill).
func (user *userImpl) FilterToAvailableChannelsForSince(channels base.Set, since base.SequenceClock) (ch.TimedSet, ch.TimedSet) {
	output := ch.TimedSet{}
	secondaryTriggers := ch.TimedSet{}
	for channel := range channels {
		if channel == ch.AllChannelWildcard {
			return user.InheritedChannelsForClock(since)
		}
		baseVbSeq, secondaryTriggerSeq, ok := user.CanSeeChannelSinceForClock(channel, since)
		if ok {
			output[channel] = ch.NewVbSequence(baseVbSeq.Vb, baseVbSeq.Seq)
			if secondaryTriggerSeq.Seq > 0 {
				secondaryTriggers[channel] = ch.NewVbSequence(secondaryTriggerSeq.Vb, secondaryTriggerSeq.Seq)
			}
		}
	}
	return output, secondaryTriggers
}

func (user *userImpl) InheritedChannelsForClock(sinceClock base.SequenceClock) (channels ch.TimedSet, secondaryTriggers ch.TimedSet) {

	channels = make(ch.TimedSet, 0)
	secondaryTriggers = make(ch.TimedSet, 0)
	hashFunction := user.auth.bucket.VBHash

	// Initialize to the user channel timed set, regardless of since value
	for channelName, channelGrant := range user.Channels() {
		if ok := user.ValidateGrant(&channelGrant, hashFunction); ok {
			channels[channelName] = channelGrant
		}
	}
	// For each role, evaluate role grant vbseq and role channel grant vbseq.
	for _, role := range user.GetRoles() {
		roleSince := user.RolesSince_[role.Name()]
		roleGrantValid := user.ValidateGrant(&roleSince, hashFunction)
		if !roleGrantValid {
			continue
		}

		for channelName, roleChannelSince := range role.Channels() {
			if roleChannelGrantValid := role.ValidateGrant(&roleChannelSince, hashFunction); !roleChannelGrantValid {
				continue
			}

			rolePreSince, grantSeq, secondarySeq := CalculateRoleChannelGrant(roleSince.AsVbSeq(), roleChannelSince.AsVbSeq(), sinceClock)
			roleGrantingSeq := ch.VbSequence{&grantSeq.Vb, grantSeq.Seq}
			secondaryTriggerSeq := ch.VbSequence{}
			if secondarySeq.Seq > 0 {
				secondaryTriggerSeq.VbNo = &secondarySeq.Vb
				secondaryTriggerSeq.Sequence = secondarySeq.Seq
			}

			// If the user doesn't already have channel access through user or a previous role, add to set
			if existingValue, ok := channels[channelName]; !ok {
				channels[channelName] = roleGrantingSeq
				secondaryTriggers[channelName] = secondaryTriggerSeq
			} else {
				// Otherwise update the value in the set if the role channel depending on comparisons to the since value
				if existingValue.IsLTEClock(sinceClock) {
					// If both are pre-clock, use the role channel since if it's an earlier vbseq
					if rolePreSince && roleGrantingSeq.CompareTo(existingValue) == base.CompareLessThan {
						channels[channelName] = roleGrantingSeq
						secondaryTriggers[channelName] = secondaryTriggerSeq
					}
				} else {
					// If existing is post-clock and the role channel is pre-clock, use the role channel
					if rolePreSince {
						channels[channelName] = roleGrantingSeq
						secondaryTriggers[channelName] = secondaryTriggerSeq
					} else {
						// If both are post-clock, use the role channel if it's an earlier vbseq
						if roleChannelSince.CompareTo(existingValue) == base.CompareLessThan {
							channels[channelName] = roleGrantingSeq
							secondaryTriggers[channelName] = secondaryTriggerSeq
						}
					}
				}
			}

		}
	}
	return channels, secondaryTriggers
}

// CanSeeChannelSinceForClock returns the vbseq at which the user was first granted access to the channel, with the following
// method for comparing vb/seq across multiple grants:
//   - Find the earliest vbseq among all grants earlier than the since clock using vbseq.Compare (minPreSince)
//   - Find the earliest vbseq among all grants later than the since clock using vbseq.Compare (minPostSince)
func (user *userImpl) CanSeeChannelSinceForClock(channel string, sinceClock base.SequenceClock) (channelSince base.VbSeq, secondaryTrigger base.VbSeq, ok bool) {

	minPreSince := base.VbSeq{}
	minPostSince := base.VbSeq{}
	secondaryTrigger = base.VbSeq{}
	hashFunction := user.auth.bucket.VBHash
	// Check for the channel in the user's grants
	userChannelSince, ok := user.CanSeeChannelSinceVbSeq(channel, hashFunction)
	if ok {
		if userChannelSince.LessThanOrEqualsClock(sinceClock) {
			minPreSince = userChannelSince
		} else {
			minPostSince = userChannelSince
		}
	}
	// Check for the channel in the user's roles
	for _, role := range user.GetRoles() {
		roleChannelSince, ok := role.CanSeeChannelSinceVbSeq(channel, hashFunction)
		if ok {
			roleGrant := user.RolesSince_[role.Name()]
			isValid := user.ValidateGrant(&roleGrant, hashFunction)
			if !isValid {
				continue
			}
			roleGrantSince := base.VbSeq{*roleGrant.VbNo, roleGrant.Sequence}
			preSince, grantingSeq, roleSecondaryTrigger := CalculateRoleChannelGrant(roleGrantSince, roleChannelSince, sinceClock)
			if preSince {
				minPreSince.UpdateIfEarlier(grantingSeq)
			} else {
				if minPostSince.UpdateIfEarlier(grantingSeq) {
					secondaryTrigger = roleSecondaryTrigger
				}
			}
		}
	}

	// If minPreSince exists, it gets priority over minPostSince, because it means the user already had access to
	// the channel before the specified sinceClock.
	if minPreSince.Seq != 0 {
		return minPreSince, base.VbSeq{}, true
	}
	if minPostSince.Seq != 0 {
		return minPostSince, secondaryTrigger, true
	}
	// Neither exist - the user can't see this channel.
	return base.VbSeq{}, base.VbSeq{}, false
}

// Identifies whether the specified role grant or roleChannelGrant are new to the user (occured after the specified since clock).
// If only one is post-since, returns that value
// If both are pre-since or both are post-since, returns the higher vb.seq
func CalculateRoleChannelGrant(roleGrant base.VbSeq, roleChannelGrant base.VbSeq, sinceClock base.SequenceClock) (preSinceGrant bool, grantSeq base.VbSeq, secondaryTrigger base.VbSeq) {

	preSinceGrant = false
	if roleGrant.LessThanOrEqualsClock(sinceClock) {
		if roleChannelGrant.LessThanOrEqualsClock(sinceClock) {
			preSinceGrant = true
			// Existing role grant, existing channel grant, update with the later vb.seq value
			if base.CompareVbSequence(roleChannelGrant, roleGrant) == base.CompareGreaterThan {
				grantSeq = roleChannelGrant
			} else {
				grantSeq = roleGrant
			}
		} else {
			// Existing role grant, new channel grant
			grantSeq = roleChannelGrant
		}
	} else {
		if roleChannelGrant.LessThanOrEqualsClock(sinceClock) {
			// New role grant, existing role channel grant
			grantSeq = roleGrant
		} else {
			// New role grant, new channel grant, update with the later vb.seq value
			if base.CompareVbSequence(roleChannelGrant, roleGrant) == base.CompareGreaterThan {
				grantSeq = roleChannelGrant
				secondaryTrigger = roleGrant
			} else {
				grantSeq = roleGrant
				secondaryTrigger = roleChannelGrant
			}
		}
	}
	return preSinceGrant, grantSeq, secondaryTrigger
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
		userData = userData[1:]
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

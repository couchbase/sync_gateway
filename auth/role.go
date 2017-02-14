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
	"fmt"
	"net/http"
	"regexp"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

/** A group that users can belong to, with associated channel permisisons. */
type roleImpl struct {
	Name_             string      `json:"name,omitempty"`
	ExplicitChannels_ ch.TimedSet `json:"admin_channels,omitempty"`
	Channels_         ch.TimedSet `json:"all_channels"`
	Sequence_         uint64      `json:"sequence"`
	PreviousChannels_ ch.TimedSet `json:"previous_channels,omitempty"`
	vbNo              *uint16
}

var kValidNameRegexp *regexp.Regexp

func init() {
	var err error
	kValidNameRegexp, err = regexp.Compile(`^[-+.@%\w]*$`)
	if err != nil {
		panic("Bad kValidNameRegexp")
	}
}

func (role *roleImpl) initRole(name string, channels base.Set) error {
	channels = ch.ExpandingStar(channels)
	role.Name_ = name
	role.ExplicitChannels_ = ch.AtSequence(channels, 1)
	return role.validate()
}

// Is this string a valid name for a User/Role? (Valid chars are alphanumeric and any of "_-+.@")
func IsValidPrincipalName(name string) bool {
	return kValidNameRegexp.Copy().MatchString(name)
}

// Creates a new Role object.
func (auth *Authenticator) NewRole(name string, channels base.Set) (Role, error) {
	role := &roleImpl{}
	if err := role.initRole(name, channels); err != nil {
		return nil, err
	}
	if err := auth.rebuildChannels(role); err != nil {
		return nil, err
	}
	return role, nil
}

func (auth *Authenticator) UnmarshalRole(data []byte, defaultName string, defaultSeq uint64) (Role, error) {
	role := &roleImpl{}
	if err := json.Unmarshal(data, role); err != nil {
		return nil, err
	}
	if role.Name_ == "" {
		role.Name_ = defaultName
	}

	defaultVbSeq := ch.NewVbSimpleSequence(defaultSeq)
	for channel, seq := range role.ExplicitChannels_ {
		if seq.Sequence == 0 {
			role.ExplicitChannels_[channel] = defaultVbSeq
		}
	}
	if err := role.validate(); err != nil {
		return nil, err
	}

	return role, nil
}

// Key prefix reserved for role documents in the bucket
const RoleKeyPrefix = "_sync:role:"

func docIDForRole(name string) string {
	return RoleKeyPrefix + name
}

func (role *roleImpl) DocID() string {
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

func (role *roleImpl) Sequence() uint64 {
	return role.Sequence_
}
func (role *roleImpl) SetSequence(sequence uint64) {
	role.Sequence_ = sequence
}

func (role *roleImpl) Channels() ch.TimedSet {
	return role.Channels_
}

func (role *roleImpl) setChannels(channels ch.TimedSet) {
	role.Channels_ = channels
}

func (role *roleImpl) ExplicitChannels() ch.TimedSet {
	return role.ExplicitChannels_
}

func (role *roleImpl) SetExplicitChannels(channels ch.TimedSet) {
	role.ExplicitChannels_ = channels
	role.setChannels(nil)
}

func (role *roleImpl) PreviousChannels() ch.TimedSet {
	return role.PreviousChannels_
}

func (role *roleImpl) SetPreviousChannels(channels ch.TimedSet) {
	role.PreviousChannels_ = channels
}

// Checks whether this role object contains valid data; if not, returns an error.
func (role *roleImpl) validate() error {
	if !IsValidPrincipalName(role.Name_) {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid name %q", role.Name_)
	}
	return role.ExplicitChannels_.Validate()
}

//////// CHANNEL AUTHORIZATION:

func (role *roleImpl) UnauthError(message string) error {
	if role.Name_ == "" {
		return base.HTTPErrorf(http.StatusUnauthorized, "login required: "+message)
	}
	return base.HTTPErrorf(http.StatusForbidden, message)
}

// Returns true if the Role is allowed to access the channel.
// A nil Role means access control is disabled, so the function will return true.
func (role *roleImpl) CanSeeChannel(channel string) bool {
	return role == nil || role.Channels_.Contains(channel) || role.Channels_.Contains(ch.UserStarChannel)
}

// Returns the sequence number since which the Role has been able to access the channel, else zero.
func (role *roleImpl) CanSeeChannelSince(channel string) uint64 {
	seq := role.Channels_[channel]
	if seq.Sequence == 0 {
		seq = role.Channels_[ch.UserStarChannel]
	}
	return seq.Sequence
}

// Returns the sequence number since which the Role has been able to access the channel, else zero.  Sets the vb
// for an admin channel grant, if needed.
func (role *roleImpl) CanSeeChannelSinceVbSeq(channel string, hashFunction VBHashFunction) (base.VbSeq, bool) {
	seq, ok := role.Channels_[channel]
	if !ok {
		seq, ok = role.Channels_[ch.UserStarChannel]
		if !ok {
			return base.VbSeq{}, false
		}
	}
	if seq.VbNo == nil {
		roleDocVbNo := role.getVbNo(hashFunction)
		seq.VbNo = &roleDocVbNo
	}
	return base.VbSeq{*seq.VbNo, seq.Sequence}, true
}

func (role *roleImpl) AuthorizeAllChannels(channels base.Set) error {
	return authorizeAllChannels(role, channels)
}

func (role *roleImpl) AuthorizeAnyChannel(channels base.Set) error {
	return authorizeAnyChannel(role, channels)
}

func (role *roleImpl) ValidateGrant(vbSeq *ch.VbSequence, hashFunction VBHashFunction) bool {

	// If the sequence is zero, this is an admin grant that hasn't been updated by accel - ignore
	if vbSeq.Sequence == 0 {
		return false
	}

	// If vbSeq is nil, this is an admin grant.  Set the vb to the vb of the user doc
	if vbSeq.VbNo == nil {
		calculatedVbNo := role.getVbNo(hashFunction)
		vbSeq.VbNo = &calculatedVbNo
	}
	return true
}

func (role *roleImpl) getVbNo(hashFunction VBHashFunction) uint16 {
	if role.vbNo == nil {
		calculatedVbNo := uint16(hashFunction(role.DocID()))
		role.vbNo = &calculatedVbNo
	}
	return *role.vbNo
}

// Returns an HTTP 403 error if the Principal is not allowed to access all the given channels.
// A nil Principal means access control is disabled, so the function will return nil.
func authorizeAllChannels(princ Principal, channels base.Set) error {
	var forbidden []string
	for channel := range channels {
		if !princ.CanSeeChannel(channel) {
			if forbidden == nil {
				forbidden = make([]string, 0, len(channels))
			}
			forbidden = append(forbidden, channel)
		}
	}
	if forbidden != nil {
		return princ.UnauthError(fmt.Sprintf("You are not allowed to see channels %v", forbidden))
	}
	return nil
}

// Returns an HTTP 403 error if the Principal is not allowed to access any of the given channels.
// A nil Role means access control is disabled, so the function will return nil.
func authorizeAnyChannel(princ Principal, channels base.Set) error {
	if len(channels) > 0 {
		for channel := range channels {
			if princ.CanSeeChannel(channel) {
				return nil
			}
		}
	} else if princ.Channels().Contains(ch.UserStarChannel) {
		return nil
	}
	return princ.UnauthError("You are not allowed to see this")
}

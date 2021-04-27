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
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

/** A group that users can belong to, with associated channel permissions. */
type roleImpl struct {
	Name_             string          `json:"name,omitempty"`
	ExplicitChannels_ ch.TimedSet     `json:"admin_channels,omitempty"`
	Channels_         ch.TimedSet     `json:"all_channels"`
	Sequence_         uint64          `json:"sequence"`
	ChannelHistory_   TimedSetHistory `json:"channel_history,omitempty"`
	ChannelInvalSeq   uint64          `json:"channel_inval_seq"`
	Deleted           bool            `json:"deleted,omitempty"`
	vbNo              *uint16
	cas               uint64
}

type TimedSetHistory map[string]GrantHistory

type GrantHistory struct {
	UpdatedAt int64                      `json:"updated_at"` // Timestamp at which history was last updated, allows for pruning
	Entries   []GrantHistorySequencePair `json:"entries"`    // Entry for a specific grant period
}

// Struct is for ease of internal use
// Bucket store has each entry as a string "seq-endSeq"
type GrantHistorySequencePair struct {
	StartSeq uint64 // Sequence at which a grant was performed to give access to a role / channel. Only populated once endSeq is available.
	EndSeq   uint64 // Sequence when access to a role / channel was revoked.
}

// MarshalJSON will handle conversion from having a seq / endSeq struct to the bucket format of "seq-endSeq"
func (pair *GrantHistorySequencePair) MarshalJSON() ([]byte, error) {
	var stringPair string
	if pair.EndSeq != 0 {
		stringPair = fmt.Sprintf("%d-%d", pair.StartSeq, pair.EndSeq)
	} else {
		stringPair = fmt.Sprintf("%d-", pair.StartSeq)
	}
	return base.JSONMarshal(stringPair)
}

// UnmarshalJSON will handle conversion from the bucket format of "seq-endSeq" to the internal struct containing
// seq / endSeq elements
func (pair *GrantHistorySequencePair) UnmarshalJSON(data []byte) error {
	var stringPair string
	err := json.Unmarshal(data, &stringPair)
	if err != nil {
		return err
	}

	splitPair := strings.Split(stringPair, "-")
	if len(splitPair) != 2 {
		return fmt.Errorf("unexpected sequence pair length")
	}

	pair.StartSeq, err = strconv.ParseUint(splitPair[0], 10, 64)
	if err != nil {
		return err
	}

	// If no endSeq
	if splitPair[1] == "" {
		return nil
	}

	pair.EndSeq, err = strconv.ParseUint(splitPair[1], 10, 64)
	if err != nil {
		return err
	}

	return nil
}

var kValidNameRegexp = regexp.MustCompile(`^[-+.@%\w]*$`)

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
	existingRole, err := auth.GetRoleIncDeleted(name)
	if err != nil {
		return nil, err
	}

	if existingRole != nil && existingRole.IsDeleted() {
		role.SetCas(existingRole.Cas())
		role.SetChannelHistory(existingRole.ChannelHistory())
	}

	if err := role.initRole(name, channels); err != nil {
		return nil, err
	}
	if err := auth.rebuildChannels(role); err != nil {
		return nil, err
	}
	return role, nil
}

func docIDForRole(name string) string {
	return base.RolePrefix + name
}

func (role *roleImpl) DocID() string {
	return docIDForRole(role.Name_)
}

// Key used in 'access' view (not same meaning as doc ID)
func (role *roleImpl) accessViewKey() string {
	return ch.RoleAccessPrefix + role.Name_
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

func (role *roleImpl) Cas() uint64 {
	return role.cas
}
func (role *roleImpl) SetCas(cas uint64) {
	role.cas = cas
}

func (role *roleImpl) setDeleted(deleted bool) {
	role.Deleted = deleted
}

func (role *roleImpl) IsDeleted() bool {
	return role.Deleted
}

func (role *roleImpl) Channels() ch.TimedSet {
	if role.ChannelInvalSeq != 0 {
		return nil
	}
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

func (role *roleImpl) GetChannelInvalSeq() uint64 {
	return role.ChannelInvalSeq
}

func (role *roleImpl) SetChannelInvalSeq(invalSeq uint64) {
	role.ChannelInvalSeq = invalSeq
}

func (role *roleImpl) InvalidatedChannels() ch.TimedSet {
	if role.ChannelInvalSeq != 0 {
		return role.Channels_
	}
	return nil
}

func (role *roleImpl) SetChannelHistory(history TimedSetHistory) {
	role.ChannelHistory_ = history
}

func (role *roleImpl) ChannelHistory() TimedSetHistory {
	return role.ChannelHistory_
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
	return role == nil || role.Channels().Contains(channel) || role.Channels().Contains(ch.UserStarChannel)
}

// Returns the sequence number since which the Role has been able to access the channel, else zero.
func (role *roleImpl) CanSeeChannelSince(channel string) uint64 {
	seq := role.Channels()[channel]
	if seq.Sequence == 0 {
		seq = role.Channels()[ch.UserStarChannel]
	}
	return seq.Sequence
}

func (role *roleImpl) AuthorizeAllChannels(channels base.Set) error {
	return authorizeAllChannels(role, channels)
}

func (role *roleImpl) AuthorizeAnyChannel(channels base.Set) error {
	return authorizeAnyChannel(role, channels)
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

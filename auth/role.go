//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

/** A group that users can belong to, with associated channel permissions. */
type roleImpl struct {
	Name_             string                                  `json:"name,omitempty"`
	ExplicitChannels_ ch.TimedSet                             `json:"admin_channels,omitempty"`
	Channels_         ch.TimedSet                             `json:"all_channels"`
	Sequence_         uint64                                  `json:"sequence"`
	ChannelHistory_   TimedSetHistory                         `json:"channel_history,omitempty"`   // Added to when a previously granted channel is revoked. Calculated inside of RebuildChannels.
	ChannelInvalSeq   uint64                                  `json:"channel_inval_seq,omitempty"` // Sequence at which the channels were invalidated. Data remains in Channels_ for history calculation.
	Deleted           bool                                    `json:"deleted,omitempty"`
	CollectionsAccess map[string]map[string]*CollectionAccess `json:"collection_access,omitempty"` // Nested maps of CollectionAccess, indexed by scope and collection name
	UpdatedAt         time.Time                               `json:"updated_at"`
	CreatedAt         time.Time                               `json:"created_at"`
	cas               uint64
	docID             string // key used to store the roleImpl
}

type TimedSetHistory map[string]GrantHistory

func (timedSet TimedSetHistory) PruneHistory(partitionWindow time.Duration) []string {
	prunedChannelHistory := make([]string, 0)
	for chanName, grantHistory := range timedSet {
		grantTime := time.Unix(grantHistory.UpdatedAt, 0)
		if time.Since(grantTime) > partitionWindow {
			delete(timedSet, chanName)
			prunedChannelHistory = append(prunedChannelHistory, chanName)
		}
	}
	return prunedChannelHistory
}

type GrantHistory struct {
	UpdatedAt int64                      `json:"updated_at"` // Timestamp at which history was last updated, allows for pruning
	Entries   []GrantHistorySequencePair `json:"entries"`    // Entry for a specific grant period
}

// Struct is for ease of internal use
// Bucket store has each entry as a string "seq-endSeq"
type GrantHistorySequencePair struct {
	StartSeq  uint64 // Sequence at which a grant was performed to give access to a role / channel. Only populated once endSeq is available.
	EndSeq    uint64 // Sequence when access to a role / channel was revoked.
	Compacted bool
}

// MarshalJSON will handle conversion from having a seq / endSeq struct to the bucket format of "seq-endSeq" and "seq~endSeq" if the pair was compacted
func (pair *GrantHistorySequencePair) MarshalJSON() ([]byte, error) {
	var stringPair string
	if pair.Compacted {
		stringPair = fmt.Sprintf("%d~%d", pair.StartSeq, pair.EndSeq)
	} else {
		stringPair = fmt.Sprintf("%d-%d", pair.StartSeq, pair.EndSeq)
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
		// try again with compacted sequence pair format
		splitPair = strings.Split(stringPair, "~")
		if len(splitPair) != 2 {
			return fmt.Errorf("unexpected sequence pair length")
		}
		pair.Compacted = true
	}

	pair.StartSeq, err = strconv.ParseUint(splitPair[0], 10, 64)
	if err != nil {
		return err
	}

	pair.EndSeq, err = strconv.ParseUint(splitPair[1], 10, 64)
	if err != nil {
		return err
	}

	return nil
}

func (role *roleImpl) initRole(name string, channels base.Set, collections map[string]map[string]struct{}) error {
	role.Name_ = name
	// Grant the user the specified channels for all specified collections.  If none are
	// specified, grant to the default collection.
	if len(collections) == 0 {
		if err := role.initChannels(base.DefaultScope, base.DefaultCollection, channels); err != nil {
			return err
		}
	} else {
		for scopeName, scope := range collections {
			for collectionName, _ := range scope {
				if err := role.initChannels(scopeName, collectionName, channels); err != nil {
					return err
				}
			}
		}
	}
	return role.validate()
}

// IsValidPrincipalName checks if the given user/role name would be valid. Valid names must be valid UTF-8, containing
// at least one alphanumeric (except for the guest user), and no colons, commas, backticks, or slashes.
func IsValidPrincipalName(name string) bool {
	namelen := len(name)
	if namelen == 0 {
		return true // guest user
	}
	if namelen > base.MaxPrincipalNameLen {
		return false
	}
	if !utf8.ValidString(name) {
		return false
	}
	seenAnAlphanum := false
	for _, char := range name {
		// Reasons for forbidding each of these:
		// colons: basic authentication uses them to separate usernames from passwords
		// commas: fails channels.IsValidChannel, which channels.compileAccessMap uses via SetFromArray
		// slashes: would need to make many (possibly breaking) changes to routing
		// backticks: MB-50619
		if char == '/' || char == ':' || char == ',' || char == '`' {
			return false
		}
		if !seenAnAlphanum && (unicode.IsLetter(char) || unicode.IsNumber(char)) {
			seenAnAlphanum = true
		}
	}
	return seenAnAlphanum
}

// ValidatePrincipalName performs the same checks as IsValidPrincipalName, but adds length check and returns a more
// verbose error message.  This function is slower than IsValidPrincipalName, and should be used only for user
// and role creation.  Names should have a max length of 239 chars, to account for SG prefixes.  All validation
// errors are concatenated and returned as one error message.
func ValidatePrincipalName(name string) error {
	namelen := len(name)
	if namelen == 0 {
		return nil // guest user
	}

	const validationMsg = "invalid name: "
	msgs := make([]string, 0, 4)

	if namelen > base.MaxPrincipalNameLen {
		const msg = "length exceeds 239" // leaving as const to avoid fmt performance (21% slower)
		msgs = append(msgs, msg)
	}

	if !utf8.ValidString(name) {
		const msg = "non UTF-8 encoding"
		msgs = append(msgs, msg)
	}

	seenAnInvalid := false
	seenAnAlphanum := false
	for _, char := range name {
		// Reasons for forbidding each of these:
		// colons: basic authentication uses them to separate usernames from passwords
		// commas: fails channels.IsValidChannel, which channels.compileAccessMap uses via SetFromArray
		// slashes: would need to make many (possibly breaking) changes to routing
		// backticks: MB-50619
		if (char == '/' || char == ':' || char == ',' || char == '`') && !seenAnInvalid {
			seenAnInvalid = true
			const msg = "contains '/', ':', ',', or '`'"
			msgs = append(msgs, msg)
			if seenAnAlphanum {
				break
			}
		}
		if !seenAnAlphanum && (unicode.IsLetter(char) || unicode.IsNumber(char)) {
			seenAnAlphanum = true
			if seenAnInvalid {
				break
			}
		}
	}

	if !seenAnAlphanum {
		const msg = "must contain alphanumeric"
		msgs = append(msgs, msg)
	}

	if len(msgs) > 0 {
		return errors.New(validationMsg + strings.Join(msgs, "; "))
	}
	return nil
}

// Creates a new Role object.
func (auth *Authenticator) NewRole(name string, channels base.Set) (Role, error) {
	role := &roleImpl{
		docID: auth.DocIDForRole(name),
	}
	existingRole, err := auth.GetRoleIncDeleted(name)
	if err != nil {
		return nil, err
	}

	if existingRole != nil && existingRole.IsDeleted() {
		role.SetCas(existingRole.Cas())
		role.SetChannelHistory(existingRole.ChannelHistory())
	}
	if err := role.initRole(name, channels, auth.Collections); err != nil {
		return nil, err
	}
	if _, err := auth.RebuildChannels(role); err != nil {
		return nil, err
	}
	return role, nil
}

// Creates a new Role object.
func (auth *Authenticator) NewRoleNoChannels(name string) (Role, error) {
	role := &roleImpl{
		docID: auth.DocIDForRole(name),
	}
	existingRole, err := auth.GetRoleIncDeleted(name)
	if err != nil {
		return nil, err
	}

	if existingRole != nil && existingRole.IsDeleted() {
		role.SetCas(existingRole.Cas())
		role.SetChannelHistory(existingRole.ChannelHistory())
	}

	if err := role.initRole(name, nil, nil); err != nil {
		return nil, err
	}
	if _, err := auth.RebuildChannels(role); err != nil {
		return nil, err
	}
	return role, nil
}

func (role *roleImpl) DocID() string {
	return role.docID
}

// Key used in 'access' view (not same meaning as doc ID)
func (role *roleImpl) accessViewKey() string {
	return ch.RoleAccessPrefix + role.Name_
}

//////// ACCESSORS:

func (role *roleImpl) Name() string {
	return role.Name_
}

func (role *roleImpl) SetUpdatedAt() {
	role.UpdatedAt = time.Now().UTC()
}

func (role *roleImpl) SetCreatedAt(t time.Time) {
	role.CreatedAt = t
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

// Retrieves Channels for the default collection
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

func (role *roleImpl) SetExplicitChannels(channels ch.TimedSet, invalSeq uint64) {
	role.ExplicitChannels_ = channels
	role.SetChannelInvalSeq(invalSeq)
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
		return base.HTTPErrorf(http.StatusUnauthorized, "login required: %s", message)
	}
	return base.NewHTTPError(http.StatusForbidden, message)
}

// Returns true if the Role is allowed to access the channel.
// A nil Role means access control is disabled, so the function will return true.
func (role *roleImpl) canSeeChannel(channel string) bool {
	return role == nil || role.Channels().Contains(channel) || role.Channels().Contains(ch.UserStarChannel)
}

// Returns the sequence number since which the Role has been able to access the channel, else zero.
func (role *roleImpl) canSeeChannelSince(channel string) uint64 {
	seq := role.Channels()[channel]
	if seq.Sequence == 0 {
		seq = role.Channels()[ch.UserStarChannel]
	}
	return seq.Sequence
}

func (role *roleImpl) authorizeAllChannels(channels base.Set) error {
	return authorizeAllChannels(role, channels)
}

func (role *roleImpl) authorizeAnyChannel(channels base.Set) error {
	return authorizeAnyChannel(role, channels)
}

// Returns an HTTP 403 error if the Principal is not allowed to access all the given channels.
// A nil Principal means access control is disabled, so the function will return nil.
func authorizeAllChannels(princ Principal, channels base.Set) error {
	var forbidden []string
	for channel := range channels {
		if !princ.canSeeChannel(channel) {
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
			if princ.canSeeChannel(channel) {
				return nil
			}
		}
	} else if princ.Channels().Contains(ch.UserStarChannel) {
		return nil
	}
	return princ.UnauthError("You are not allowed to see this")
}

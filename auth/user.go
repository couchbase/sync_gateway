//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sync"

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

	// warnChanThresholdOnce ensures that the check for channels
	// per user threshold is only performed exactly once.
	warnChanThresholdOnce sync.Once
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
	RoleInvalSeq     uint64          `json:"role_inval_seq,omitempty"` // Sequence at which the roles were invalidated. Data remains in RolesSince_ for history calculation.
	RoleHistory_     TimedSetHistory `json:"role_history,omitempty"`   // Added to when a previously granted role is revoked. Calculated inside of rebuildRoles.

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

	err := user.SetPassword(password)
	if err != nil {
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

func (user *userImpl) SetExplicitRoles(roles ch.TimedSet, invalSeq uint64) {
	user.ExplicitRoles_ = roles
	user.SetRoleInvalSeq(invalSeq)
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
func (user *userImpl) RevokedChannels(since uint64, lowSeq uint64, triggeredBy uint64) RevokedChannels {
	// checkSeq represents the value that we use to 'diff' against ie. What channels did the user have at checkSeq but
	// no longer has.
	// In the event we have a lowSeq that will be used.
	// In the event we do not have a lowSeq but have a triggeredBy that will be used.
	// In the event we have neither lowSeq or triggeredBy the 'regular' seq value will be used.
	var checkSeq uint64
	if lowSeq > 0 {
		checkSeq = lowSeq
	} else if triggeredBy > 0 {
		checkSeq = triggeredBy
	} else {
		checkSeq = since
	}

	// Note
	// entry.EndSeq > checkSeq || entry.EndSeq == triggeredBy
	// The above check is used in a number of places and represents:
	// If there has been a revocation somewhere after the since value or we're in an interrupted revocation backfill
	// at the point a revocation occurred we should return this as a channel to revoke.

	accessibleChannels := user.InheritedChannels()
	// Get revoked roles
	rolesToRevoke := map[string]uint64{}
	roleHistory := user.RoleHistory()
	for roleName, history := range roleHistory {
		if !user.RoleNames().Contains(roleName) {
			for _, entry := range history.Entries {
				if entry.EndSeq > checkSeq || entry.EndSeq == triggeredBy {
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
					if entry.EndSeq > checkSeq || entry.EndSeq == triggeredBy {
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
			base.WarnfCtx(user.auth.LogCtx, "unable to obtain role %s to calculate channel revocation: %v. Will continue", base.UD(roleName), err)
			continue
		}

		// First check 'current channels' if role isn't deleted
		// Current roles should be invalidated on deleted anyway but for safety
		if !role.IsDeleted() {
			for _, chanName := range role.Channels().AllKeys() {
				if !accessibleChannels.Contains(chanName) {
					combinedRevokedChannels.add(chanName, roleRevokeSeq)
				}
			}
		}

		// Second check the channel history and add any revoked channels
		for chanName, history := range role.ChannelHistory() {
			if !accessibleChannels.Contains(chanName) {
				for _, channelEntry := range history.Entries {
					if channelEntry.EndSeq > checkSeq || channelEntry.EndSeq == triggeredBy {
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

// Calculate periods where user had access to the given channel either directly or via a role
func (user *userImpl) ChannelGrantedPeriods(chanName string) ([]GrantHistorySequencePair, error) {
	var resultPairs []GrantHistorySequencePair

	// Grab user history and use this to begin the resultPairs
	userChannelHistory, ok := user.ChannelHistory()[chanName]
	if ok {
		resultPairs = userChannelHistory.Entries
	}

	// Iterate over current user channels and add to resultPairs
	for channelName, chanInfo := range user.Channels() {
		if channelName != chanName {
			continue
		}
		resultPairs = append(resultPairs, GrantHistorySequencePair{
			StartSeq: chanInfo.Sequence,
			EndSeq:   math.MaxUint64,
		})
	}

	// Small function which takes the two start seqs and two end seqs and calculates the intersection
	compareAndAddPair := func(startSeq1, startSeq2, endSeq1, endSeq2 uint64) {
		start := base.MaxUint64(startSeq1, startSeq2)
		end := base.MinUint64(endSeq1, endSeq2)
		if start < end {
			resultPairs = append(resultPairs, GrantHistorySequencePair{
				StartSeq: start,
				EndSeq:   end,
			})
		}
	}

	// Iterate over current roles
	for _, currentRole := range user.GetRoles() {

		// Grab pairs from channel history on current roles
		roleChannelHistory, ok := currentRole.ChannelHistory()[chanName]
		if ok {
			for _, roleChannelHistoryEntry := range roleChannelHistory.Entries {
				roleGrantedAt := user.RolesSince_[currentRole.Name()].Sequence
				compareAndAddPair(roleChannelHistoryEntry.StartSeq, roleGrantedAt, roleChannelHistoryEntry.EndSeq, math.MaxUint64)
			}
		}

		// Grab pairs from current channels on current roles
		if currentRole.Channels().Contains(chanName) {
			for _, channelInfo := range currentRole.Channels() {
				resultPairs = append(resultPairs, GrantHistorySequencePair{
					StartSeq: channelInfo.Sequence,
					EndSeq:   math.MaxUint64,
				})
			}
		}
	}

	// Iterate over previous roles the user has had
	for roleName, historyEntry := range user.RoleHistory() {
		role, err := user.auth.GetRoleIncDeleted(roleName)
		if err != nil {
			return nil, err
		}

		// Iterate over channel history on old roles
		roleChannelHistory, ok := role.ChannelHistory()[chanName]
		if ok {
			for _, roleChannelHistoryEntry := range roleChannelHistory.Entries {
				for _, roleHistoryEntry := range historyEntry.Entries {
					compareAndAddPair(roleChannelHistoryEntry.StartSeq, roleHistoryEntry.StartSeq, roleChannelHistoryEntry.EndSeq, roleHistoryEntry.EndSeq)
				}
			}
		}

		// Iterate over current channels on old roles
		if role.Channels().Contains(chanName) {
			for _, activeChannel := range role.Channels() {
				for _, roleHistoryEntry := range historyEntry.Entries {
					compareAndAddPair(activeChannel.Sequence, roleHistoryEntry.StartSeq, math.MaxUint64, roleHistoryEntry.EndSeq)
				}
			}
		}
	}

	return resultPairs, nil
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
		base.WarnfCtx(user.auth.LogCtx, "User account %q still has pre-beta password hash; need to reset password", base.UD(user.Name_))
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
			base.WarnfCtx(user.auth.LogCtx, "Error when rehashing password for user %s: %v", base.UD(user.Name()), err)
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
func (user *userImpl) SetPassword(password string) error {
	if password == "" {
		user.PasswordHash_ = nil
	} else {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), user.auth.BcryptCost)
		if err != nil {
			return fmt.Errorf("error hashing password: %w", err)
		}
		user.PasswordHash_ = hash
	}
	return nil
}

// ////// CHANNEL ACCESS:

func (user *userImpl) GetRoles() []Role {
	if user.roles == nil {
		roles := make([]Role, 0, len(user.RoleNames()))
		for name := range user.RoleNames() {
			role, err := user.auth.GetRole(name)
			// base.InfofCtx(user.auth.LogCtx, base.KeyAccess, "User %s role %q = %v", base.UD(user.Name_), base.UD(name), base.UD(role))
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

func (user *userImpl) InitializeRoles() {
	_ = user.GetRoles()
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

	user.warnChanThresholdOnce.Do(func() {
		if channelsPerUserThreshold := user.auth.ChannelsWarningThreshold; channelsPerUserThreshold != nil {
			channelCount := len(channels)
			if uint32(channelCount) >= *channelsPerUserThreshold {
				base.WarnfCtx(user.auth.LogCtx, "User ID: %v channel count: %d exceeds %d for channels per user warning threshold",
					base.UD(user.Name()), channelCount, *channelsPerUserThreshold)
			}
		}
	})

	return channels
}

// If a channel list contains the all-channel wildcard, replace it with all the user's accessible channels.
func (user *userImpl) ExpandWildCardChannel(channels base.Set) base.Set {
	if channels.Contains(ch.AllChannelWildcard) {
		channels = user.InheritedChannels().AsSet()
	}
	return channels
}

func (user *userImpl) FilterToAvailableChannels(channels base.Set) (filtered ch.TimedSet, removed []string) {
	filtered = ch.TimedSet{}
	for channel := range channels {
		if channel == ch.AllChannelWildcard {
			return user.InheritedChannels().Copy(), nil
		}
		added := filtered.AddChannel(channel, user.CanSeeChannelSince(channel))
		if !added {
			removed = append(removed, channel)
		}
	}
	return filtered, removed
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

// ////// MARSHALING:

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

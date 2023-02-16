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
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	"github.com/google/uuid"
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
	JWTRoles_        ch.TimedSet     `json:"jwt_roles,omitempty"`
	JWTChannels_     ch.TimedSet     `json:"jwt_channels,omitempty"`
	JWTIssuer_       string          `json:"jwt_issuer,omitempty"`
	JWTLastUpdated_  time.Time       `json:"jwt_last_updated,omitempty"`
	RolesSince_      ch.TimedSet     `json:"rolesSince"`
	RoleInvalSeq     uint64          `json:"role_inval_seq,omitempty"` // Sequence at which the roles were invalidated. Data remains in RolesSince_ for history calculation.
	RoleHistory_     TimedSetHistory `json:"role_history,omitempty"`   // Added to when a previously granted role is revoked. Calculated inside of rebuildRoles.
	SessionUUID_     string          `json:"session_uuid"`             // marker of when the user object changes, to match with session docs to determine if they are valid

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
	return user
}

// Creates a new User object. Intended for test usage, for simplified channel assignment.
// The specified channels are granted to the user as admin grants for
// all collections defined in the authenticator, at sequence 1.  If no collections are defined, the channels
// are granted for the default collection.
func (auth *Authenticator) NewUser(username string, password string, channels base.Set) (User, error) {
	user := &userImpl{
		auth:         auth,
		userImplBody: userImplBody{RolesSince_: ch.TimedSet{}},
	}
	if err := user.initRole(username, channels, auth.Collections); err != nil {
		return nil, err
	}

	if _, err := auth.rebuildChannels(user); err != nil {
		return nil, err
	}

	if err := auth.rebuildRoles(user); err != nil {
		return nil, err
	}

	err := user.setPassword(password)
	if err != nil {
		return nil, err
	}

	return user, nil
}

// NewUserNoChannels initializes a user object with no admin channel grants.  Should be used for
// non-test user creation.
func (auth *Authenticator) NewUserNoChannels(username string, password string) (User, error) {
	user := &userImpl{
		auth:         auth,
		userImplBody: userImplBody{RolesSince_: ch.TimedSet{}},
	}
	if err := user.initRole(username, nil, nil); err != nil {
		return nil, err
	}
	if _, err := auth.rebuildChannels(user); err != nil {
		return nil, err
	}

	if err := auth.rebuildRoles(user); err != nil {
		return nil, err
	}

	err := user.setPassword(password)
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

func (user *userImpl) JWTRoles() ch.TimedSet {
	return user.JWTRoles_
}

func (user *userImpl) SetJWTRoles(channels ch.TimedSet, invalSeq uint64) {
	user.JWTRoles_ = channels
	// change to OIDC roles means roles need to be recomputed
	user.SetRoleInvalSeq(invalSeq)
}

func (user *userImpl) JWTChannels() ch.TimedSet {
	return user.JWTChannels_
}

func (user *userImpl) SetJWTChannels(channels ch.TimedSet, invalSeq uint64) {
	user.JWTChannels_ = channels
	// change to JWT channels means channels need to be recomputed
	user.SetChannelInvalSeq(invalSeq)
}

func (user *userImpl) JWTIssuer() string {
	return user.JWTIssuer_
}

func (user *userImpl) SetJWTIssuer(val string) {
	user.JWTIssuer_ = val
}

func (user *userImpl) JWTLastUpdated() time.Time {
	return user.JWTLastUpdated_
}

func (user *userImpl) SetJWTLastUpdated(val time.Time) {
	user.JWTLastUpdated_ = val
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

func (user *userImpl) revokedChannels(since uint64, lowSeq uint64, triggeredBy uint64) RevokedChannels {
	return user.RevokedCollectionChannels(base.DefaultScope, base.DefaultCollection, since, lowSeq, triggeredBy)
}

// RevokedCollectionChannels returns a map of revoked channels for the collection => most recent sequence at which access to that channel was lost
// Steps:
// Get revoked roles and for each:
//   - Revoke the current channels if the role is deleted
//   - Revoke the role revoked channels
//
// Get current roles and for each:
//   - Revoke the role revoked channels
//
// Get user:
//   - Revoke users revoked channels
func (user *userImpl) RevokedCollectionChannels(scope string, collection string, since uint64, lowSeq uint64, triggeredBy uint64) RevokedChannels {
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

	accessibleChannels := user.InheritedCollectionChannels(scope, collection)
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
		for chanName, history := range princ.CollectionChannelHistory(scope, collection) {
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
			for _, chanName := range role.CollectionChannels(scope, collection).AllKeys() {
				if !accessibleChannels.Contains(chanName) {
					combinedRevokedChannels.add(chanName, roleRevokeSeq)
				}
			}
		}

		// Second check the channel history and add any revoked channels
		for chanName, history := range role.CollectionChannelHistory(scope, collection) {
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

func (user *userImpl) channelGrantedPeriods(chanName string) ([]GrantHistorySequencePair, error) {
	return user.CollectionChannelGrantedPeriods(base.DefaultScope, base.DefaultCollection, chanName)
}

// Calculate periods where user had access to the given channel either directly or via a role
func (user *userImpl) CollectionChannelGrantedPeriods(scope, collection, chanName string) ([]GrantHistorySequencePair, error) {
	var resultPairs []GrantHistorySequencePair

	// Grab user history and use this to begin the resultPairs
	userChannelHistory, ok := user.CollectionChannelHistory(scope, collection)[chanName]
	if ok {
		resultPairs = userChannelHistory.Entries
	}

	// Iterate over current user channels and add to resultPairs
	for channelName, chanInfo := range user.CollectionChannels(scope, collection) {
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
		start := base.Max(startSeq1, startSeq2)
		end := base.Min(endSeq1, endSeq2)
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
		roleChannelHistory, ok := currentRole.CollectionChannelHistory(scope, collection)[chanName]
		if ok {
			for _, roleChannelHistoryEntry := range roleChannelHistory.Entries {
				roleGrantedAt := user.RolesSince_[currentRole.Name()].Sequence
				compareAndAddPair(roleChannelHistoryEntry.StartSeq, roleGrantedAt, roleChannelHistoryEntry.EndSeq, math.MaxUint64)
			}
		}

		// Grab pairs from current channels on current roles
		if currentRole.CollectionChannels(scope, collection).Contains(chanName) {
			for _, channelInfo := range currentRole.CollectionChannels(scope, collection) {
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
		if role == nil {
			// In most cases, this means the role never existed (and therefore didn't affect access).
			// However, it's also possible (though rare) that the role used to exist but was explicitly purged, and thus
			// could have granted access to channels before it was purged - but we can't determine what those channels were.
			// We can't distinguish these cases, so log to be safe.
			base.WarnfCtx(user.auth.LogCtx, "Unable to determine complete access history for user %v because role %v doesn't exist or has been purged (for channel %v).", base.UD(user.Name()), base.UD(roleName), base.UD(chanName))
			continue
		}

		// Iterate over channel history on old roles
		roleChannelHistory, ok := role.CollectionChannelHistory(scope, collection)[chanName]
		if ok {
			for _, roleChannelHistoryEntry := range roleChannelHistory.Entries {
				for _, roleHistoryEntry := range historyEntry.Entries {
					compareAndAddPair(roleChannelHistoryEntry.StartSeq, roleHistoryEntry.StartSeq, roleChannelHistoryEntry.EndSeq, roleHistoryEntry.EndSeq)
				}
			}
		}

		// Iterate over current channels on old roles
		if role.CollectionChannels(scope, collection).Contains(chanName) {
			for _, activeChannel := range role.CollectionChannels(scope, collection) {
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

// GetSessionUUID returns the UUID that a session to match to be a valid session.
func (user *userImpl) GetSessionUUID() string {
	return user.SessionUUID_
}

// UpdateSessionUUID creates a new UUID for a session.
func (user *userImpl) UpdateSessionUUID() {
	user.SessionUUID_ = uuid.NewString()
}

// SetPassword changes a user's password to the given string. This needs to be called from external functions, so we invalidate sessions.
func (user *userImpl) SetPassword(password string) error {
	user.UpdateSessionUUID()
	return user.setPassword(password)
}

// setPassword to the given string. It should only be called from constructors, since it will avoid invalidating existing sessions.
func (user *userImpl) setPassword(password string) error {
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

func (user *userImpl) canSeeChannel(channel string) bool {
	if user.roleImpl.canSeeChannel(channel) {
		return true
	}
	for _, role := range user.GetRoles() {
		if role.canSeeChannel(channel) {
			return true
		}
	}
	return false
}

func (user *userImpl) canSeeChannelSince(channel string) uint64 {
	minSeq := user.roleImpl.canSeeChannelSince(channel)
	for _, role := range user.GetRoles() {
		if seq := role.canSeeChannelSince(channel); seq > 0 && (seq < minSeq || minSeq == 0) {
			minSeq = seq
		}
	}
	return minSeq
}

func (user *userImpl) authorizeAllChannels(channels base.Set) error {
	return authorizeAllChannels(user, channels)
}

func (user *userImpl) authorizeAnyChannel(channels base.Set) error {
	return authorizeAnyChannel(user, channels)
}

func (user *userImpl) inheritedChannels() ch.TimedSet {
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
func (user *userImpl) expandWildCardChannel(channels base.Set) base.Set {
	return user.expandCollectionWildCardChannel(base.DefaultScope, base.DefaultCollection, channels)
}

func (user *userImpl) expandCollectionWildCardChannel(scope, collection string, channels base.Set) base.Set {
	if channels.Contains(ch.AllChannelWildcard) {
		channels = user.InheritedCollectionChannels(scope, collection).AsSet()
	}
	return channels
}

func (user *userImpl) filterToAvailableChannels(channelNames base.Set) (filtered ch.TimedSet, removed []string) {
	return user.FilterToAvailableCollectionChannels(base.DefaultScope, base.DefaultCollection, channelNames)
}

func (user *userImpl) FilterToAvailableCollectionChannels(scope, collection string, channelNames base.Set) (filtered ch.TimedSet, removed []string) {
	filtered = ch.TimedSet{}
	for channelName, _ := range channelNames {
		if channelName == ch.AllChannelWildcard {
			return user.InheritedCollectionChannels(scope, collection).Copy(), nil
		}
		added := filtered.AddChannel(channelName, user.canSeeCollectionChannelSince(scope, collection, channelName))
		if !added {
			removed = append(removed, channelName)
		}
	}
	return filtered, removed
}

func (user *userImpl) GetAddedChannels(channels ch.TimedSet) base.Set {
	output := base.Set{}
	for userChannel := range user.inheritedChannels() {
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

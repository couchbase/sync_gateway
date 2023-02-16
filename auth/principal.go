//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

// A Principal is an abstract object that can have access to channels.
type Principal interface {
	// The Principal's identifier.
	Name() string

	// The database sequence at which this Principal last changed
	Sequence() uint64
	SetSequence(sequence uint64)

	// Returns true if the Principal has access to the given channel.
	canSeeChannel(channel string) bool

	// If the Principal has access to the given channel, returns the sequence number at which
	// access was granted; else returns zero.
	canSeeChannelSince(channel string) uint64

	// Returns an error if the Principal does not have access to all the channels in the set.
	authorizeAllChannels(channels base.Set) error

	// Returns an error if the Principal does not have access to any of the channels in the set.
	authorizeAnyChannel(channels base.Set) error

	// Returns an appropriate HTTPError for unauthorized access -- a 401 if the receiver is
	// the guest user, else 403.
	UnauthError(message string) error

	DocID() string
	accessViewKey() string
	validate() error

	// Cas value for the associated principal document in the bucket
	Cas() uint64
	SetCas(cas uint64)

	setDeleted(bool)
	IsDeleted() bool

	// Principal includes the PrincipalCollectionAccess interface for operations against
	// the _default._default collection (stored directly on the principal for backward
	// compatibility)
	PrincipalCollectionAccess

	// Principals implement the CollectionChannelAPI for collection-scoped operations
	CollectionChannelAPI
}

// Role is basically the same as Principal, just concrete. Users can inherit channels from Roles.
type Role interface {
	Principal
}

// A User is a Principal that can log in and have multiple Roles.
type User interface {
	Principal

	// The user's email address.
	Email() string

	// Sets the user's email address.
	SetEmail(string) error

	// If true, the user is unable to authenticate.
	Disabled() bool

	// Sets the disabled property
	SetDisabled(bool)

	// Authenticates the user's password.
	Authenticate(password string) bool

	// GetSessionUUID returns the UUID that a session to match to be a valid session.
	GetSessionUUID() string

	// UpdateSessionUUID will invalidate all earlier sessions associated with this user.
	UpdateSessionUUID()

	// Changes the user's password.
	SetPassword(password string) error

	// The set of Roles the user belongs to (including ones given to it by the sync function and by OIDC/JWT)
	// Returns nil if invalidated
	RoleNames() ch.TimedSet

	// The roles the user was explicitly granted access to thru the admin API.
	ExplicitRoles() ch.TimedSet

	// Sets the explicit roles the user belongs to.
	SetExplicitRoles(ch.TimedSet, uint64)

	JWTRoles() ch.TimedSet
	SetJWTRoles(ch.TimedSet, uint64)
	JWTIssuer() string
	SetJWTIssuer(string)
	JWTLastUpdated() time.Time
	SetJWTLastUpdated(time.Time)

	GetRoleInvalSeq() uint64

	SetRoleInvalSeq(uint64)

	// The set of invalidated roles
	// Returns nil if not invalidated
	InvalidatedRoles() ch.TimedSet

	SetRoleHistory(history TimedSetHistory)

	RoleHistory() TimedSetHistory

	InitializeRoles()

	revokedChannels(since uint64, lowSeq uint64, triggeredBy uint64) RevokedChannels

	// Obtains the period over which the user had access to the given channel. Either directly or via a role.
	channelGrantedPeriods(chanName string) ([]GrantHistorySequencePair, error)

	// Every channel the user has access to, including those inherited from Roles.
	inheritedChannels() ch.TimedSet

	// If the input set contains the wildcard "*" channel, returns the user's InheritedChannels;
	// else returns the input channel list unaltered.
	expandWildCardChannel(channels base.Set) base.Set

	// Returns a TimedSet containing only the channels from the input set that the user has access
	// to, annotated with the sequence number at which access was granted.
	// Returns a string array containing any channels filtered out due to the user not having access
	// to them.
	filterToAvailableChannels(channels base.Set) (filtered ch.TimedSet, removed []string)

	setRolesSince(ch.TimedSet)

	UserCollectionChannelAPI

	UserCollectionAccess
}

// PrincipalConfig represents a user/role as a JSON object.
// Used to define a user/role within DbConfig, and structures the request/response body in the admin REST API
// for /db/_user/*
type PrincipalConfig struct {
	Name             *string                                       `json:"name,omitempty"`
	ExplicitChannels base.Set                                      `json:"admin_channels,omitempty"`
	CollectionAccess map[string]map[string]*CollectionAccessConfig `json:"collection_access,omitempty"`
	// Fields below only apply to Users, not Roles:
	Email             *string  `json:"email,omitempty"`
	Disabled          *bool    `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames base.Set `json:"admin_roles,omitempty"`
	// Fields below are read-only
	Channels       base.Set   `json:"all_channels,omitempty"`
	RoleNames      []string   `json:"roles,omitempty"`
	JWTIssuer      *string    `json:"jwt_issuer,omitempty"`
	JWTRoles       base.Set   `json:"jwt_roles,omitempty"`
	JWTChannels    base.Set   `json:"jwt_channels,omitempty"`
	JWTLastUpdated *time.Time `json:"jwt_last_updated,omitempty"`
}

type CollectionAccessConfig struct {
	ExplicitChannels_ base.Set `json:"admin_channels,omitempty"`
	// read-only
	Channels_      base.Set   `json:"all_channels,omitempty"`
	JWTChannels_   base.Set   `json:"jwt_channels,omitempty"` // TODO: JWT properties should only be populated for user but would like to share scope/collection map
	JWTLastUpdated *time.Time `json:"jwt_last_updated,omitempty"`
}

// IsPasswordValid checks if the passwords in this PrincipalConfig is valid.  Only allows
// empty passwords if allowEmptyPass is true.
func (u PrincipalConfig) IsPasswordValid(allowEmptyPass bool) (isValid bool, reason string) {
	// if it's an anon user, they should not have a password
	if u.Name == nil {
		if u.Password != nil {
			return false, "Anonymous users should not have a password"
		} else {
			return true, ""
		}
	}

	/*
		if allowEmptyPass && ( u.Password == nil || len(*u.Password) == 0) {
			return true, ""
		}

		if u.Password == nil || (u.Password != nil && len(*u.Password) < 3) {
			return false, "Passwords must be at least three 3 characters"
		}
	*/

	if u.Password == nil || len(*u.Password) == 0 {
		if !allowEmptyPass {
			return false, "Empty passwords are not allowed "
		}
	} else if len(*u.Password) < 3 {
		return false, "Passwords must be at least three 3 characters"
	}

	return true, ""
}

// Merge returns a new PrincipalConfig that represents the combination of both this and other's changes.
// If any changes conflict, those of the other take precedence.
func (u PrincipalConfig) Merge(other PrincipalConfig) PrincipalConfig {
	return PrincipalConfig{
		Name:              base.Coalesce(other.Name, u.Name),
		ExplicitChannels:  base.CoalesceSets(other.ExplicitChannels, u.ExplicitChannels),
		Email:             base.Coalesce(other.Email, u.Email),
		Password:          base.Coalesce(other.Password, u.Password),
		Disabled:          base.Coalesce(other.Disabled, u.Disabled),
		ExplicitRoleNames: base.CoalesceSets(other.ExplicitRoleNames, u.ExplicitRoleNames),
		JWTIssuer:         base.Coalesce(other.JWTIssuer, u.JWTIssuer),
		JWTRoles:          base.CoalesceSets(other.JWTRoles, u.JWTRoles),
		JWTChannels:       base.CoalesceSets(other.JWTChannels, u.JWTChannels),
		JWTLastUpdated:    base.Coalesce(other.JWTLastUpdated, u.JWTLastUpdated),
	}
}

// Helper function to set explicit channels for a collection
func (u *PrincipalConfig) SetExplicitChannels(scopeName, collectionName string, channels ...string) {
	channelSet := base.SetFromArray(channels)
	if u.CollectionAccess == nil {
		u.CollectionAccess = map[string]map[string]*CollectionAccessConfig{
			scopeName: {
				collectionName: {
					ExplicitChannels_: channelSet,
				},
			},
		}
		return
	}
	if scope, ok := u.CollectionAccess[scopeName]; !ok {
		u.CollectionAccess[scopeName] = map[string]*CollectionAccessConfig{
			collectionName: {
				ExplicitChannels_: channelSet,
			},
		}
	} else {
		if collection, ok := scope[collectionName]; !ok {
			scope[collectionName] = &CollectionAccessConfig{
				ExplicitChannels_: channelSet,
			}
		} else {
			collection.ExplicitChannels_ = channelSet
		}
	}
}

func (u *PrincipalConfig) GetExplicitChannels(scopeName, collectionName string) base.Set {
	if base.IsDefaultCollection(scopeName, collectionName) {
		return u.ExplicitChannels
	}
	return u.CollectionAccess[scopeName][collectionName].ExplicitChannels_
}

func (u *PrincipalConfig) GetChannels(scopeName, collectionName string) base.Set {
	if base.IsDefaultCollection(scopeName, collectionName) {
		return u.Channels
	}
	return u.CollectionAccess[scopeName][collectionName].Channels_
}

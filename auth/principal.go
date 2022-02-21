//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
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

	// The set of channels the Principal belongs to, and what sequence access was granted.
	// Returns nil if invalidated
	Channels() ch.TimedSet

	// The channels the Principal was explicitly granted access to thru the admin API.
	ExplicitChannels() ch.TimedSet

	// Sets the explicit channels the Principal has access to.
	SetExplicitChannels(ch.TimedSet, uint64)

	GetChannelInvalSeq() uint64

	SetChannelInvalSeq(uint64)

	// The set of invalidated channels
	// Returns nil if not invalidated
	InvalidatedChannels() ch.TimedSet

	ChannelHistory() TimedSetHistory

	SetChannelHistory(history TimedSetHistory)

	// Returns true if the Principal has access to the given channel.
	CanSeeChannel(channel string) bool

	// If the Principal has access to the given channel, returns the sequence number at which
	// access was granted; else returns zero.
	CanSeeChannelSince(channel string) uint64

	// Returns an error if the Principal does not have access to all the channels in the set.
	AuthorizeAllChannels(channels base.Set) error

	// Returns an error if the Principal does not have access to any of the channels in the set.
	AuthorizeAnyChannel(channels base.Set) error

	// Returns an appropriate HTTPError for unauthorized access -- a 401 if the receiver is
	// the guest user, else 403.
	UnauthError(message string) error

	DocID() string
	accessViewKey() string
	validate() error
	setChannels(ch.TimedSet)

	// Cas value for the associated principal document in the bucket
	Cas() uint64
	SetCas(cas uint64)

	setDeleted(bool)
	IsDeleted() bool
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

	// Changes the user's password.
	SetPassword(password string) error

	// The set of Roles the user belongs to (including ones given to it by the sync function)
	// Returns nil if invalidated
	RoleNames() ch.TimedSet

	// The roles the user was explicitly granted access to thru the admin API.
	ExplicitRoles() ch.TimedSet

	// Sets the explicit roles the user belongs to.
	SetExplicitRoles(ch.TimedSet, uint64)

	GetRoleInvalSeq() uint64

	SetRoleInvalSeq(uint64)

	// The set of invalidated roles
	// Returns nil if not invalidated
	InvalidatedRoles() ch.TimedSet

	SetRoleHistory(history TimedSetHistory)

	RoleHistory() TimedSetHistory

	InitializeRoles()

	RevokedChannels(since uint64, lowSeq uint64, triggeredBy uint64) RevokedChannels

	// Obtains the period over which the user had access to the given channel. Either directly or via a role.
	ChannelGrantedPeriods(chanName string) ([]GrantHistorySequencePair, error)

	// Every channel the user has access to, including those inherited from Roles.
	InheritedChannels() ch.TimedSet

	// If the input set contains the wildcard "*" channel, returns the user's InheritedChannels;
	// else returns the input channel list unaltered.
	ExpandWildCardChannel(channels base.Set) base.Set

	// Returns a TimedSet containing only the channels from the input set that the user has access
	// to, annotated with the sequence number at which access was granted.
	// Returns a string array containing any channels filtered out due to the user not having access
	// to them.
	FilterToAvailableChannels(channels base.Set) (filtered ch.TimedSet, removed []string)

	setRolesSince(ch.TimedSet)
}

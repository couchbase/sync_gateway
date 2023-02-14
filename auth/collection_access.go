// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package auth

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

// CollectionChannelAPI defines helper functions for interacting with a principal's CollectionAccess set using
// scope and collection name.  Mirrors the default collection functionality defined on Principal
type CollectionChannelAPI interface {

	// Retrieve all channels for a collection
	CollectionChannels(scope, collection string) ch.TimedSet

	// Sets all channels for a collection.
	setCollectionChannels(scope, collection string, channels ch.TimedSet)

	// Retrieve admin-granted channels for a collection
	CollectionExplicitChannels(scope, collection string) ch.TimedSet

	// Set admin-granted channels for a collection
	SetCollectionExplicitChannels(scope, collection string, channels ch.TimedSet, seq uint64)

	// Retrieve channel history for a collection
	CollectionChannelHistory(scope, collection string) TimedSetHistory

	// Set channel history for a collection
	SetCollectionChannelHistory(scope, collection string, history TimedSetHistory)

	// Returns true if the Principal has access to the given channel.
	CanSeeCollectionChannel(scope, collection, channel string) bool

	// Retrieve invalidation sequence for a collection
	getCollectionChannelInvalSeq(scope, collection string) uint64

	// Set invalidation sequence for a collection
	setCollectionChannelInvalSeq(scope, collection string, seq uint64)

	// The set of invalidated channels for a collection
	// Returns nil if not invalidated
	collectionInvalidatedChannels(scope, collection string) ch.TimedSet

	// If the Principal has access to the given collection's channel, returns the sequence number at which
	// access was granted; else returns zero.
	canSeeCollectionChannelSince(scope, collection, channel string) uint64

	// Returns an error if the Principal does not have access to all the channels in the set, for the specified collection.
	authorizeAllCollectionChannels(scope, collection string, channels base.Set) error

	// Returns an error if the Principal does not have access to any of the channels in the set, for the specified collection
	AuthorizeAnyCollectionChannel(scope, collection string, channels base.Set) error

	// Retrieves or creates collection access entry on principal for specified scope and collection
	getOrCreateCollectionAccess(scope, collection string) *CollectionAccess

	// Returns the CollectionAccess map
	GetCollectionsAccess() map[string]map[string]*CollectionAccess
}

// UserCollectionChannelAPI defines the interface for managing channel access that is supported by users but not roles.
type UserCollectionChannelAPI interface {
	// Retrieves JWT channels for a collection
	CollectionJWTChannels(scope, collection string) ch.TimedSet

	// Sets JWT channels for a collection
	SetCollectionJWTChannels(scope, collection string, channels ch.TimedSet, seq uint64)

	// Retrieves revoked channels for a collection, based on the given since value
	RevokedCollectionChannels(scope, collection string, since uint64, lowSeq uint64, triggeredBy uint64) RevokedChannels

	// Obtains the period over which the user had access to the given collection's channel. Either directly or via a role.
	CollectionChannelGrantedPeriods(scope, collection, chanName string) ([]GrantHistorySequencePair, error)

	// Every channel the user has access to in the collection, including those inherited from Roles.
	InheritedCollectionChannels(scope, collection string) ch.TimedSet

	// Returns a TimedSet containing only the channels from the input set that the user has access
	// to for the collection, annotated with the sequence number at which access was granted.
	// Returns a string array containing any channels filtered out due to the user not having access
	// to them.
	FilterToAvailableCollectionChannels(scope, collection string, channels base.Set) (filtered ch.TimedSet, removed []string)

	// If the input set contains the wildcard "*" channel, returns the user's inheritedChannels for the collection;
	// else returns the input channel list unaltered.
	expandCollectionWildCardChannel(scope, collection string, channels base.Set) base.Set
}

// PrincipalCollectionAccess defines a common interface for principal access control.  This interface is
// implemented by CollectionAccess for named collection access,
// and by roleImpl for the _default._default collection.
type PrincipalCollectionAccess interface {
	// The set of channels the Principal belongs to for the channel, and what sequence access was granted.
	// Returns nil if invalidated.
	// For both roles and users, the set of channels is the union of ExplicitChannels, JWTChannels, and any channels
	// they are granted through a sync function.
	//
	// NOTE: channels a user has access to through a role are *not* included in Channels(), so the user could have
	// access to more documents than included in Channels. canSeeChannel will also check against the user's roles.
	Channels() ch.TimedSet

	// Sets the explicit channels the Principal has access to.
	SetExplicitChannels(ch.TimedSet, uint64)

	// The channels the Principal was explicitly granted access to thru the admin API.
	ExplicitChannels() ch.TimedSet

	// sets the computed channels for the collection
	setChannels(ch.TimedSet)

	// The set of invalidated channels
	// Returns nil if not invalidated
	InvalidatedChannels() ch.TimedSet

	GetChannelInvalSeq() uint64
	SetChannelInvalSeq(invalSeq uint64)
	ChannelHistory() TimedSetHistory
	SetChannelHistory(h TimedSetHistory)
}

// UserCollectionAccess functions the same as PrincipalCollectionAccess, but for user-specific properties.
type UserCollectionAccess interface {
	JWTChannels() ch.TimedSet
	SetJWTChannels(ch.TimedSet, uint64)
}

// Defines channel grants and history for a single collection
type CollectionAccess struct {
	ExplicitChannels_ ch.TimedSet     `json:"admin_channels,omitempty"`
	Channels_         ch.TimedSet     `json:"all_channels,omitempty"`
	ChannelHistory_   TimedSetHistory `json:"channel_history,omitempty"`   // Added to when a previously granted channel is revoked. Calculated inside of rebuildChannels.
	ChannelInvalSeq   uint64          `json:"channel_inval_seq,omitempty"` // Sequence at which the channels were invalidated. Data remains in Channels_ for history calculation.
	JWTChannels_      ch.TimedSet     `json:"jwt_channels,omitempty"`      // TODO: JWT properties should only be populated for user but would like to share scope/collection map
	JWTLastUpdated    *time.Time      `json:"jwt_last_updated,omitempty"`
}

func (ca *CollectionAccess) ExplicitChannels() ch.TimedSet {
	return ca.ExplicitChannels_
}

func (ca *CollectionAccess) SetExplicitChannels(channels ch.TimedSet, invalSeq uint64) {
	ca.ExplicitChannels_ = channels
	ca.SetChannelInvalSeq(invalSeq)
}

func (ca *CollectionAccess) JWTChannels() ch.TimedSet {
	return ca.JWTChannels_
}

func (ca *CollectionAccess) GetChannelInvalSeq() uint64 {
	return ca.ChannelInvalSeq
}

func (ca *CollectionAccess) InvalidatedChannels() ch.TimedSet {
	if ca.ChannelInvalSeq != 0 {
		return ca.Channels_
	}
	return nil
}

func (ca *CollectionAccess) ChannelHistory() TimedSetHistory {
	return ca.ChannelHistory_
}

func (ca *CollectionAccess) SetChannelHistory(history TimedSetHistory) {
	ca.ChannelHistory_ = history
}

func (ca *CollectionAccess) SetChannelInvalSeq(invalSeq uint64) {
	ca.ChannelInvalSeq = invalSeq
}

func (ca *CollectionAccess) setChannels(channels ch.TimedSet) {
	ca.Channels_ = channels
}

func (ca *CollectionAccess) CanSeeChannel(channel string) bool {
	return ca.Channels().Contains(channel) || ca.Channels().Contains(ch.UserStarChannel)
}

func (ca *CollectionAccess) Channels() ch.TimedSet {
	if ca.ChannelInvalSeq != 0 {
		return nil
	}
	return ca.Channels_
}

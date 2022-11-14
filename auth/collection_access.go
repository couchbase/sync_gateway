package auth

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

// PrincipalCollectionAccess defines the common interface for managing channel access for a role or user for
// a single collection.
type PrincipalCollectionAccess interface {

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
	authorizeAnyCollectionChannel(scope, collection string, channels base.Set) error
}

// UserCollection defines the interface for managing channel access that is supported by users but not roles.
type UserCollectionAccess interface {
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
	FilterToAvailableCollectionChannels(scope, collection string, channels ch.Set) (filtered ch.TimedSet, removed []string)

	// If the input set contains the wildcard "*" channel, returns the user's InheritedChannels for the collection;
	// else returns the input channel list unaltered.
	expandCollectionWildCardChannel(scope, collection string, channels base.Set) base.Set
}

// Defines channel grants and history for a single collection
type CollectionAccess struct {
	ExplicitChannels_ ch.TimedSet     `json:"admin_channels,omitempty"`
	Channels_         ch.TimedSet     `json:"all_channels,omitempty"`
	ChannelHistory_   TimedSetHistory `json:"channel_history,omitempty"`   // Added to when a previously granted channel is revoked. Calculated inside of rebuildChannels.
	ChannelInvalSeq   uint64          `json:"channel_inval_seq,omitempty"` // Sequence at which the channels were invalidated. Data remains in Channels_ for history calculation.
	JWTChannels       ch.TimedSet     `json:"jwt_channels,omitempty"`      // TODO: JWT properties should only be populated for user but would like to share scope/collection map
	JWTLastUpdated    *time.Time      `json:"jwt_last_updated,omitempty"`
}

func (ca *CollectionAccess) CanSeeChannel(channel string) bool {
	return ca.Channels().Contains(channel) || ca.Channels().Contains(ch.UserStarChannel)
}

package auth

import (
	"fmt"
	
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

var _ PrincipalCollectionAccess = &roleImpl{}

// getCollectionAccess retrieves collection-specific access information for the roleImpl
func (role *roleImpl) getCollectionAccess(scope, collection string) (*CollectionAccess, bool) {
	ca, ok := role.CollectionsAccess[scope][collection]
	return ca, ok
}

// Gets collection-specific access for the roleImpl.  Creates if not found
func (role *roleImpl) getOrCreateCollectionAccess(scope, collection string) *CollectionAccess {

	ca, ok := role.getCollectionAccess(scope, collection)
	if ok {
		return ca
	}

	// Get or create Scope map
	var scopeAccess map[string]*CollectionAccess
	scopeAccess, ok = role.CollectionsAccess[scope]
	if !ok {
		if role.CollectionsAccess == nil {
			role.CollectionsAccess = make(map[string]map[string]*CollectionAccess)
		}
		scopeAccess = make(map[string]*CollectionAccess)
		role.CollectionsAccess[scope] = scopeAccess
	}

	newCollectionAccess := &CollectionAccess{}
	scopeAccess[collection] = newCollectionAccess
	return newCollectionAccess
}

// Collection-aware role handlers
func (role *roleImpl) CollectionChannels(scope, collection string) ch.TimedSet {
	if base.IsDefaultCollection(scope, collection) {
		return role.Channels()
	}

	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		if cc.ChannelInvalSeq != 0 {
			return nil
		}
		return cc.Channels_
	}
	// Access always granted to public channel, even if channels aren't otherwise defined for the collection
	// always grant access to the public document channel
	return ch.TimedSet{ch.DocumentStarChannel: ch.NewVbSimpleSequence(1)}
}

func (role *roleImpl) CollectionExplicitChannels(scope, collection string) ch.TimedSet {
	if base.IsDefaultCollection(scope, collection) {
		return role.ExplicitChannels()
	}

	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		return cc.ExplicitChannels_
	}
	return nil
}

func (role *roleImpl) SetCollectionExplicitChannels(scope, collection string, channels ch.TimedSet, invalSeq uint64) {
	if base.IsDefaultCollection(scope, collection) {
		role.SetExplicitChannels(channels, invalSeq)
		return
	}

	cc := role.getOrCreateCollectionAccess(scope, collection)
	cc.ExplicitChannels_ = channels
	cc.ChannelInvalSeq = invalSeq
}

func (role *roleImpl) setCollectionChannels(scope, collection string, channels ch.TimedSet) {
	if base.IsDefaultCollection(scope, collection) {
		role.setChannels(channels)
		return
	}
	cc := role.getOrCreateCollectionAccess(scope, collection)
	cc.Channels_ = channels
}

func (role *roleImpl) getCollectionChannelInvalSeq(scope, collection string) uint64 {
	if base.IsDefaultCollection(scope, collection) {
		return role.GetChannelInvalSeq()
	}

	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		return cc.ChannelInvalSeq
	}
	return 0
}

func (role *roleImpl) setCollectionChannelInvalSeq(scope, collection string, invalSeq uint64) {
	if base.IsDefaultCollection(scope, collection) {
		role.SetChannelInvalSeq(invalSeq)
		return
	}

	cc := role.getOrCreateCollectionAccess(scope, collection)
	cc.ChannelInvalSeq = invalSeq
}

func (role *roleImpl) collectionInvalidatedChannels(scope, collection string) ch.TimedSet {
	if base.IsDefaultCollection(scope, collection) {
		return role.InvalidatedChannels()
	}

	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		if cc.ChannelInvalSeq != 0 {
			return cc.Channels_
		}
	}
	return nil
}

func (role *roleImpl) CollectionChannelHistory(scope, collection string) TimedSetHistory {
	if base.IsDefaultCollection(scope, collection) {
		return role.ChannelHistory()
	}

	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		return cc.ChannelHistory_
	}
	return nil
}

func (role *roleImpl) SetCollectionChannelHistory(scope, collection string, history TimedSetHistory) {
	if base.IsDefaultCollection(scope, collection) {
		role.SetChannelHistory(history)
		return
	}

	cc := role.getOrCreateCollectionAccess(scope, collection)
	cc.ChannelHistory_ = history
}

// Returns true if the Role is allowed to access the channel.
// A nil Role means access control is disabled, so the function will return true.
func (role *roleImpl) CanSeeCollectionChannel(scope, collection, channel string) bool {
	if base.IsDefaultCollection(scope, collection) {
		return role.CanSeeChannel(channel)
	}

	if role == nil {
		return true
	}
	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		return cc.Channels().Contains(channel) || cc.Channels().Contains(ch.UserStarChannel)
	}
	return false
}

// Returns the sequence number since which the Role has been able to access the channel, else zero.
func (role *roleImpl) canSeeCollectionChannelSince(scope, collection, channel string) uint64 {
	if base.IsDefaultCollection(scope, collection) {
		return role.CanSeeChannelSince(channel)
	}

	if cc, ok := role.getCollectionAccess(scope, collection); ok {
		seq := cc.Channels()[channel]
		if seq.Sequence == 0 {
			seq = cc.Channels()[ch.UserStarChannel]
		}
	}
	return 0
}

func (role *roleImpl) authorizeAllCollectionChannels(scope, collection string, channels base.Set) error {
	if base.IsDefaultCollection(scope, collection) {
		return role.AuthorizeAllChannels(channels)
	}

	if ca, ok := role.getCollectionAccess(scope, collection); ok {
		var forbidden []string
		for channel := range channels {
			if !ca.CanSeeChannel(channel) {
				if forbidden == nil {
					forbidden = make([]string, 0, len(channels))
				}
				forbidden = append(forbidden, channel)
			}
		}
		if forbidden != nil {
			return role.UnauthError(fmt.Sprintf("You are not allowed to see channels %v", forbidden))
		}
		return nil
	}
	return role.UnauthError(fmt.Sprintf("Unauthorized to see channels %v", channels))
}

// Returns an error if the Principal does not have access to any of the channels in the set.
func (role *roleImpl) authorizeAnyCollectionChannel(scope, collection string, channels base.Set) error {
	if base.IsDefaultCollection(scope, collection) {
		return role.AuthorizeAnyChannel(channels)
	}

	if ca, ok := role.getCollectionAccess(scope, collection); ok {
		if len(channels) > 0 {
			for channel := range channels {
				if ca.CanSeeChannel(channel) {
					return nil
				}
			}
		} else if ca.Channels().Contains(ch.UserStarChannel) {
			return nil
		}
	}
	return role.UnauthError("You are not allowed to see this")
}

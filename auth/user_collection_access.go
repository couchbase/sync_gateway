package auth

import (
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

var _ UserCollectionAccess = &userImpl{}

func (user *userImpl) CollectionJWTChannels(scope, collection string) ch.TimedSet {
	if base.IsDefaultCollection(scope, collection) {
		return user.JWTChannels()
	}

	if cc, ok := user.getCollectionAccess(scope, collection); ok {
		return cc.JWTChannels
	}
	return nil
}

func (user *userImpl) SetCollectionJWTChannels(scope, collection string, channels ch.TimedSet, invalSeq uint64) {
	if base.IsDefaultCollection(scope, collection) {
		user.SetJWTChannels(channels, invalSeq)
		return
	}

	cc := user.getOrCreateCollectionAccess(scope, collection)
	cc.JWTChannels = channels
	cc.ChannelInvalSeq = invalSeq
}

func (user *userImpl) CanSeeCollectionChannel(scope, collection, channel string) bool {
	if user.roleImpl.CanSeeCollectionChannel(scope, collection, channel) {
		return true
	}
	for _, role := range user.GetRoles() {
		if role.CanSeeCollectionChannel(scope, collection, channel) {
			return true
		}
	}
	return false
}

func (user *userImpl) InheritedCollectionChannels(scope, collection string) ch.TimedSet {

	channels := user.CollectionChannels(scope, collection).Copy()
	for _, role := range user.GetRoles() {
		roleSince := user.RoleNames()[role.Name()]
		channels.AddAtSequence(role.CollectionChannels(scope, collection), roleSince.Sequence)
	}

	// TODO: should warning threshold be per-collection, or cross-collection?  If the latter, hard to compute, as
	//  we lazily fetch channels per collection
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

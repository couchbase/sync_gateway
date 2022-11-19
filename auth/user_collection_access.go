// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
		return cc.JWTChannels()
	}
	return nil
}

func (user *userImpl) SetCollectionJWTChannels(scope, collection string, channels ch.TimedSet, invalSeq uint64) {
	if base.IsDefaultCollection(scope, collection) {
		user.SetJWTChannels(channels, invalSeq)
		return
	}

	cc := user.getOrCreateCollectionAccess(scope, collection)
	cc.JWTChannels_ = channels
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

	// Warning threshold is per-collection, as we lazily load per-collection channel information
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

//  Checks for user access to any channel in the set, including access inherited via roles
func (user *userImpl) AuthorizeAnyCollectionChannel(scope, collection string, channels base.Set) error {

	if base.IsDefaultCollection(scope, collection) {
		return user.AuthorizeAnyChannel(channels)
	}

	// User access
	if ca, ok := user.getCollectionAccess(scope, collection); ok {
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

	// Inherited role access
	for _, role := range user.GetRoles() {
		if role.AuthorizeAnyCollectionChannel(scope, collection, channels) == nil {
			return nil
		}
	}

	return user.UnauthError("You are not allowed to see this")
}

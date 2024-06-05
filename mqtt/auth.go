//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"context"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Highly-simplified subset of auth.User;
// contains just the methods this package needs, for easier unit testing.
type User interface {
	Name() string
	RoleNames() channels.TimedSet
	CanSeeCollectionChannel(scope, collection, channel string) bool
}

//======== AUTH:

// Finds a TopicConfig in BrokerConfig.Allow whose topic filter matches a topic name.
// Returns the TopicConfig and the list of matches against any wildcards in its filter.
func (bc *BrokerConfig) Authorize(user User, topic string, write bool) bool {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.allowFilters == nil {
		var filters TopicMap[*TopicConfig]
		for _, tc := range bc.Allow {
			if err := filters.AddFilter(tc.Topic, tc); err != nil {
				return false // should have been caught in validation
			}
		}
		bc.allowFilters = &filters
	}

	if topicConfig, match := bc.allowFilters.Match(topic); topicConfig == nil {
		return false
	} else if write {
		return topicConfig.Publish.Authorize(user, *match)
	} else {
		return topicConfig.Subscribe.Authorize(user, *match)
	}
}

// Returns true if the ACLConfig allows this user based on username, role, or channel membership.
// `$1` variables will be expanded based on the TopicMatch.
func (acl *ACLConfig) Authorize(user User, topic TopicMatch) bool {
	// Check if the username is allowed:
	username := user.Name()
	for _, userPattern := range acl.Users {
		if userPattern == "*" {
			return true // "*" matches anyone
		} else if userPattern == "!" && username != "" {
			return true // "!" matches any non-guest
		} else if expanded, err := topic.ExpandPattern(userPattern); err != nil {
			base.WarnfCtx(context.Background(), "MQTT: invalid username pattern %q", userPattern)
		} else if expanded == user.Name() {
			return true // User name is in the allowed list
		}
	}

	// Check if the user is in one of the allowed roles.
	userRoles := user.RoleNames()
	for _, rolePattern := range acl.Roles {
		if role, err := topic.ExpandPattern(rolePattern); err != nil {
			base.WarnfCtx(context.Background(), "MQTT: invalid role pattern %q", rolePattern)
		} else if userRoles.Contains(role) {
			return true // User has one of the allowed roles
		}
	}

	// Check if the user has access to one of the allowed channels.
	for _, channelPattern := range acl.Channels {
		if channelPattern == channels.AllChannelWildcard {
			return true
		} else if channel, err := topic.ExpandPattern(channelPattern); err != nil {
			base.WarnfCtx(context.Background(), "MQTT: invalid channel pattern %q", channelPattern)
		} else if user.CanSeeCollectionChannel(base.DefaultScope, base.DefaultCollection, channel) {
			return true // User has access to one of the allowed channels
		}
	}

	return false
}

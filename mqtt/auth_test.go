//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/require"
)

//======== MOCK USER

// Mock implementatin of User
type MockUser struct {
	name      string
	roleNames channels.TimedSet
	channels  base.Set
}

func MakeMockUser(name string, roleNames []string, channelNames []string) *MockUser {
	return &MockUser{
		name:      name,
		roleNames: channels.AtSequence(base.SetFromArray(roleNames), 1234),
		channels:  base.SetFromArray(channelNames),
	}
}

func (user *MockUser) Name() string { return user.name }

func (user *MockUser) RoleNames() channels.TimedSet { return user.roleNames }

func (user *MockUser) CanSeeCollectionChannel(scope, collection, channel string) bool {
	return scope == base.DefaultScope && collection == base.DefaultCollection && user.channels.Contains(channel)
}

var _ User = &MockUser{}

//======== TESTS

func TestAuth(t *testing.T) {
	config := BrokerConfig{
		Allow: []*TopicConfig{
			{Topic: "anyone",
				Publish: ACLConfig{
					Users: []string{"*"},
				},
			},
			{Topic: "personal/+/#",
				Publish: ACLConfig{
					Users: []string{"$1"},
				},
				Subscribe: ACLConfig{
					Users: []string{"$1"},
					Roles: []string{"high priest"},
				},
			},
			{Topic: "stocks",
				Publish: ACLConfig{
					Channels: []string{"NBC"},
				},
				Subscribe: ACLConfig{
					Channels: []string{"ABC", "NBC"},
				},
			},
			{Topic: "network/+",
				Publish: ACLConfig{
					Channels: []string{"$1"},
				},
			},
		},
	}

	require.NoError(t, config.Validate())

	jeff := MakeMockUser("jeff", []string{"minion"}, []string{"ESPN"})
	amy := MakeMockUser("amy", []string{"high priest"}, []string{"ABC", "PBS"})

	require.False(t, config.Authorize(jeff, "missingTopic", false))
	require.True(t, config.Authorize(jeff, "anyone", true))

	require.True(t, config.Authorize(jeff, "personal/jeff/howdy", false))
	require.True(t, config.Authorize(jeff, "personal/jeff/howdy", true))
	require.True(t, config.Authorize(amy, "personal/jeff/howdy", false))
	require.False(t, config.Authorize(amy, "personal/jeff/howdy", true))

	require.True(t, config.Authorize(amy, "stocks", false))
	require.False(t, config.Authorize(amy, "stocks", true))

	require.True(t, config.Authorize(amy, "network/PBS", true))
	require.False(t, config.Authorize(amy, "network/ESPN", true))

}

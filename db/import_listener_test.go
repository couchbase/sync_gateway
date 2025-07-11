// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// TestImportFeedEventRecover will make sure panics are recoverable importListener.ProcessFeedEvent
func TestImportFeedEventRecover(t *testing.T) {
	// test the ability for ProcessFeedEvent
	listener := importListener{
		collections: map[uint32]DatabaseCollectionWithUser{
			0: {}, // this is an invalid entry, because we expect this code to panic and recover
		},
	}
	startWarnCount := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	// assert false to indicate that this checkpoint will not be incremented
	require.False(t, listener.ProcessFeedEvent(sgbucket.FeedEvent{
		Key:    []byte("example-doc"),
		Opcode: sgbucket.FeedOpMutation,
	}))
	require.Equal(t, startWarnCount+1, base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value())
}

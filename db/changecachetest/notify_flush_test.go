/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestManualNotifyDeliversInline(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	collection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, database.Options.JavascriptTimeout)
	expected := channels.NewID("zero", collection.GetCollectionID())

	// Capacity 1 with a non-blocking send: further matching notifications must never block the caching feed.
	notified := make(chan struct{}, 1)
	cache := database.ChangeCacheForTest(t)
	cache.SetManualNotifyChangeFuncForTest(t, func(_ context.Context, chans channels.Set) {
		if !chans.Contains(expected) {
			return
		}
		select {
		case notified <- struct{}{}:
		default:
		}
	})

	_, _, err := collection.Put(ctx, "manualNotifyDoc", db.Body{"channels": []string{"zero"}})
	require.NoError(t, err)

	// The caching feed notifies inline: DocChanged calls the notify callback as soon as processEntry has
	// cached the entry, so the channel arrives without a flush.
	select {
	case <-notified:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for notify")
	}
}

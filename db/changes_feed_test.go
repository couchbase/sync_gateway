//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// failingSecondPageCache returns one page of changes, then cancels the changes request and fails the next query.
type failingSecondPageCache struct {
	SingleChannelCache
	channelID     channels.ID
	cancelChanges context.CancelCauseFunc
	calls         int
}

func (c *failingSecondPageCache) ChannelID() channels.ID {
	return c.channelID
}

func (c *failingSecondPageCache) GetChanges(_ context.Context, _ ChangesOptions) ([]*LogEntry, error) {
	c.calls++
	if c.calls == 1 {
		return []*LogEntry{{DocID: "doc1", RevID: "1-a", Sequence: 1}}, nil
	}
	c.cancelChanges(errors.New("client disconnected"))
	return nil, errors.New("query failed")
}

// countLabeledGoroutines returns the number of goroutines carrying the pprof label key=value.
func countLabeledGoroutines(t require.TestingT, key, value string) int {
	var profile strings.Builder
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&profile, 1))
	label := fmt.Sprintf("%q:%q", key, value)
	count := 0
	// debug=1 output groups identical goroutines into blank-line separated blocks headed by "<count> @ <pcs>".
	for _, group := range strings.Split(profile.String(), "\n\n") {
		if !strings.Contains(group, "# labels: ") || !strings.Contains(group, label) {
			continue
		}
		var n int
		_, err := fmt.Sscanf(group, "%d @", &n)
		require.NoError(t, err)
		count += n
	}
	return count
}

// TestChangesFeedQueryErrorAfterChangesCancelled verifies that the channel feed goroutine exits when a query fails
// after the client stops reading.
func TestChangesFeedQueryErrorAfterChangesCancelled(t *testing.T) {
	const labelKey = "test"

	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelQueryLimit = 1
	db, ctx := SetupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	changesCtx, cancelChanges := context.WithCancelCause(ctx)
	defer cancelChanges(nil)
	cache := &failingSecondPageCache{
		channelID:     channels.NewID("ABC", collection.GetCollectionID()),
		cancelChanges: cancelChanges,
	}

	// The feed goroutine inherits the label, which separates it from goroutines started by other tests. Nothing reads
	// the feed until the goroutine exits, so the first page fills the feed's buffer.
	var feed <-chan *ChangeEntry
	pprof.Do(ctx, pprof.Labels(labelKey, t.Name()), func(ctx context.Context) {
		feed = collection.changesFeed(ctx, cache, ChangesOptions{ChangesCtx: changesCtx}, "")
	})
	base.RequireChanClosed(t, changesCtx.Done(), "second query never ran")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Zero(c, countLabeledGoroutines(c, labelKey, t.Name()))
	}, 10*time.Second, 10*time.Millisecond, "changesFeed goroutine did not exit after the changes request was cancelled")

	entry := base.RequireChanRecv(t, feed, "first page entry was not sent")
	require.Equal(t, "doc1", entry.ID)
	base.RequireChanClosed(t, feed, "feed was not closed")
}

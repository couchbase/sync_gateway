// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestTimestampsOfflineOnline makes sure that a database can be taken offline and back online, and that coming
// back online records the current sequence straight away.
func TestTimestampsOfflineOnline(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	ctx := rt.Context()

	rt.CreateTestDoc("doc1")
	lastSeq, err := rt.GetDatabase().LastSequence(ctx)
	require.NoError(t, err)

	rt.TakeDbOffline()
	assert.Nil(t, rt.GetDatabase().TimestampsManager, "an offline database must not run a timestamps manager")

	// make the newest entry older than the update frequency, so that coming online adds one
	key := rt.GetDatabase().MetadataKeys.TimestampsSeqKey()
	stale := db.TimestampsSeqArray{Timestamps: []db.TimestampsSeqPair{{Seq: 0, Timestamp: time.Now().Add(-24 * time.Hour).Unix()}}}
	require.NoError(t, rt.MetadataStore().Set(ctx, key, 0, nil, stale))

	beforeOnline := time.Now().Unix()
	rt.TakeDbOnline()

	var tsDoc db.TimestampsSeqArray
	_, err = rt.MetadataStore().Get(ctx, key, &tsDoc)
	require.NoError(t, err)
	require.NotEmpty(t, tsDoc.Timestamps)
	newest := tsDoc.Timestamps[len(tsDoc.Timestamps)-1]
	assert.GreaterOrEqual(t, newest.Seq, lastSeq)
	assert.GreaterOrEqual(t, newest.Timestamp, beforeOnline)
}

// TestTimestampsMissingDocRecreatedOnline makes sure that a timestamps document deleted while the database is online is
// created again with an empty array once the database is taken offline and back online.
func TestTimestampsMissingDocRecreatedOnline(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	ctx := rt.Context()

	rt.CreateTestDoc("doc1")

	key := rt.GetDatabase().MetadataKeys.TimestampsSeqKey()
	require.NoError(t, rt.MetadataStore().Delete(ctx, key))

	rt.TakeDbOffline()
	rt.TakeDbOnline()

	var tsDoc db.TimestampsSeqArray
	_, err := rt.MetadataStore().Get(ctx, key, &tsDoc)
	require.NoError(t, err)
	require.Len(t, tsDoc.Timestamps, 0)
}

// TestTimestampsNonPositiveClientPartitionWindow makes sure that with client_partition_window_secs of zero or less,
// bringing a database online trims the timestamps document by DefaultClientPartitionWindow rather than keeping every
// entry ever added.
func TestTimestampsNonPositiveClientPartitionWindow(t *testing.T) {
	const day = 24 * time.Hour
	for _, windowSecs := range []int{0, -5} {
		t.Run(strconv.Itoa(windowSecs), func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{ClientPartitionWindowSecs: new(windowSecs)}},
			})
			defer rt.Close()
			ctx := rt.Context()
			require.Equal(t, time.Duration(windowSecs)*time.Second, rt.GetDatabase().ClientPartitionWindow())

			rt.TakeDbOffline()

			// one entry a day, starting 10 days before the default window
			now := time.Now()
			start := now.Add(-base.DefaultClientPartitionWindow - 10*day)
			var history db.TimestampsSeqArray
			for ts := start; !ts.After(now.Add(-day)); ts = ts.Add(day) {
				history.Timestamps = append(history.Timestamps, db.TimestampsSeqPair{Timestamp: ts.Unix()})
			}
			key := rt.GetDatabase().MetadataKeys.TimestampsSeqKey()
			require.NoError(t, rt.MetadataStore().Set(ctx, key, 0, nil, history))

			// the newest entry is a day old, older than the update frequency, so coming online adds one
			rt.TakeDbOnline()

			var tsDoc db.TimestampsSeqArray
			_, err := rt.MetadataStore().Get(ctx, key, &tsDoc)
			require.NoError(t, err)
			// the 10 entries older than the start of the window are dropped, and a new entry is added
			require.Len(t, tsDoc.Timestamps, len(history.Timestamps)-10+1)
			assert.Equal(t, history.Timestamps[10:], tsDoc.Timestamps[:len(tsDoc.Timestamps)-1])
		})
	}
}

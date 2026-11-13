//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestTimestampsDocCreation makes sure that Start creates the timestamps document in the metadata store.
func TestTimestampsDocCreation(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	mgrOpts := TimestampsMgrOpts{
		ctx:           ctx,
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
	}

	tm := NewTimestampsManager(mgrOpts)
	require.NotNil(t, tm, "NewTimestampsManager must return a TimestampsManager")

	err := tm.Start()
	require.NoError(t, err)
	assert.NotNil(t, tm.timestampsSeqArray.Load())

	tsDocBytes, _, err := metadataStore.GetRaw(ctx, metaKeys.TimestampsSeqKey())
	assert.NoError(t, err)
	assert.NotNil(t, tsDocBytes)

	var tsDoc TimestampsSeqArray
	err = base.JSONUnmarshal(tsDocBytes, &tsDoc)
	require.NoError(t, err)
	assert.Len(t, tsDoc.Timestamps, 0)

}

// TestTimestampsSeqArrayIsEmpty makes sure that isEmpty is true only when the array has no entries.
func TestTimestampsSeqArrayIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		array    TimestampsSeqArray
		expected bool
	}{
		{name: "zero value", array: TimestampsSeqArray{}, expected: true},
		{name: "empty slice", array: TimestampsSeqArray{Timestamps: []TimestampsSeqPair{}}, expected: true},
		{name: "one entry", array: TimestampsSeqArray{Timestamps: []TimestampsSeqPair{{Seq: 1, Timestamp: 100}}}, expected: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.array.isEmpty())
		})
	}
}

// TestTimestampsSeqArrayOldestTimestamp makes sure that oldestTimestamp returns the first entry, or 0 for an empty array.
func TestTimestampsSeqArrayOldestTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		array    TimestampsSeqArray
		expected int64
	}{
		{name: "empty array", array: TimestampsSeqArray{}, expected: 0},
		{name: "one entry", array: TimestampsSeqArray{Timestamps: []TimestampsSeqPair{{Seq: 1, Timestamp: 100}}}, expected: 100},
		{
			name: "many entries",
			array: TimestampsSeqArray{Timestamps: []TimestampsSeqPair{
				{Seq: 1, Timestamp: 100},
				{Seq: 5, Timestamp: 200},
				{Seq: 9, Timestamp: 300},
			}},
			expected: 100,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.array.oldestTimestamp())
		})
	}
}

// TestTimestampsSeqArrayNewestTimestamp makes sure that newestTimestamp returns the last entry, or 0 for an empty array.
func TestTimestampsSeqArrayNewestTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		array    TimestampsSeqArray
		expected int64
	}{
		{name: "empty array", array: TimestampsSeqArray{}, expected: 0},
		{name: "one entry", array: TimestampsSeqArray{Timestamps: []TimestampsSeqPair{{Seq: 1, Timestamp: 100}}}, expected: 100},
		{
			name: "many entries",
			array: TimestampsSeqArray{Timestamps: []TimestampsSeqPair{
				{Seq: 1, Timestamp: 100},
				{Seq: 5, Timestamp: 200},
				{Seq: 9, Timestamp: 300},
			}},
			expected: 300,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.array.newestTimestamp())
		})
	}
}

// TestTimestampsSeqArrayRemoveInvalidEntries makes sure that removeInvalidEntries drops only entries with a sequence above the given value.
func TestTimestampsSeqArrayRemoveInvalidEntries(t *testing.T) {
	entries := []TimestampsSeqPair{
		{Seq: 1, Timestamp: 100},
		{Seq: 5, Timestamp: 200},
		{Seq: 9, Timestamp: 300},
	}
	tests := []struct {
		name     string
		entries  []TimestampsSeqPair
		seq      uint64
		expected []TimestampsSeqPair
		removed  []TimestampsSeqPair
	}{
		{name: "empty array", entries: nil, seq: 5, expected: nil, removed: nil},
		{name: "no entries above seq", entries: entries, seq: 9, expected: entries, removed: nil},
		{name: "seq equal to an entry keeps it", entries: entries, seq: 5, expected: entries[:2], removed: entries[2:]},
		{name: "some entries above seq", entries: entries, seq: 6, expected: entries[:2], removed: entries[2:]},
		{name: "all entries above seq", entries: entries, seq: 0, expected: []TimestampsSeqPair{}, removed: entries},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			array := TimestampsSeqArray{Timestamps: slices.Clone(test.entries)}
			removed := array.removeInvalidEntries(test.seq)
			assert.Equal(t, test.removed, removed, "expected removed %+v, got %+v", test.removed, removed)
			assert.Equal(t, len(test.expected), len(array.Timestamps))
			assert.True(t, slices.Equal(test.expected, array.Timestamps), "expected %+v, got %+v", test.expected, array.Timestamps)
		})
	}
}

// TestTimestampsSeqArrayAppend makes sure that append adds the new pair at the end and keeps the existing entries.
func TestTimestampsSeqArrayAppend(t *testing.T) {
	tests := []struct {
		name     string
		entries  []TimestampsSeqPair
		seq      uint64
		now      int64
		expected []TimestampsSeqPair
	}{
		{
			name:     "empty array",
			entries:  nil,
			seq:      1,
			now:      100,
			expected: []TimestampsSeqPair{{Seq: 1, Timestamp: 100}},
		},
		{
			name:    "existing entries",
			entries: []TimestampsSeqPair{{Seq: 1, Timestamp: 100}, {Seq: 5, Timestamp: 200}},
			seq:     9,
			now:     300,
			expected: []TimestampsSeqPair{
				{Seq: 1, Timestamp: 100},
				{Seq: 5, Timestamp: 200},
				{Seq: 9, Timestamp: 300},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			array := TimestampsSeqArray{Timestamps: slices.Clone(test.entries)}
			array.append(test.seq, test.now)
			assert.Equal(t, test.expected, array.Timestamps, "expected %+v, got %+v", test.expected, array.Timestamps)
			assert.Equal(t, test.now, array.newestTimestamp())
		})
	}
}

// TestTimestampsSeqArrayTrim makes sure that trim keeps the newest entry at or before the purge timestamp, and every entry after it.
func TestTimestampsSeqArrayTrim(t *testing.T) {
	now := time.Now()
	// Entries are generated with constant gaps, so the expected array is the same generator started at the purge timestamp.
	tests := []struct {
		name           string
		entries        []TimestampsSeqPair
		purgeTimestamp int64
		expected       []TimestampsSeqPair
		purge          bool
	}{
		{
			// a single 12 hour gap: entries older than day -30 are trimmed
			name: "timespan=1month;duration:12hours;duration_change=false",
			entries: func() []TimestampsSeqPair {
				return GenerateTimestampsSeqPairs(now.Add(-31*24*time.Hour), now, time.Hour*12, 0)
			}(),
			purgeTimestamp: now.Add(-30 * 24 * time.Hour).Unix(),
			expected: func() []TimestampsSeqPair {
				return GenerateTimestampsSeqPairs(now.Add(-30*24*time.Hour), now, time.Hour*12, 0)
			}(),
			purge: true,
		},
		{
			// the gap changes from 12 hours to 24 hours at day -30: the purge timestamp falls before the change
			name: "timespan=1.5month;duration:12hours/24hours;duration_change=true",
			entries: func() []TimestampsSeqPair {
				tsPair := GenerateTimestampsSeqPairs(now.Add(-60.5*24*time.Hour), now.Add(-30*24*time.Hour), time.Hour*12, 0)
				tsPair2 := GenerateTimestampsSeqPairs(now.Add(-29*24*time.Hour), now, time.Hour*24, 0)
				tsPair = append(tsPair, tsPair2...)
				return tsPair
			}(),
			purgeTimestamp: now.Add(-60 * 24 * time.Hour).Unix(),
			expected: func() []TimestampsSeqPair {
				tsPair := GenerateTimestampsSeqPairs(now.Add(-60*24*time.Hour), now.Add(-30*24*time.Hour), time.Hour*12, 0)
				tsPair2 := GenerateTimestampsSeqPairs(now.Add(-29*24*time.Hour), now, time.Hour*24, 0)
				tsPair = append(tsPair, tsPair2...)
				return tsPair
			}(),
			purge: true,
		},
		{
			name:           "purge_timestamp older and no trim",
			entries:        GenerateTimestampsSeqPairs(now.Add(-25*24*time.Hour), now, time.Hour*12, 0),
			purgeTimestamp: now.Add(-30 * 24 * time.Hour).Unix(),
			expected:       GenerateTimestampsSeqPairs(now.Add(-25*24*time.Hour), now, time.Hour*12, 0),
			purge:          false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.True(t, slices.IsSortedFunc(test.entries, func(a, b TimestampsSeqPair) int {
				return cmp.Compare(a.Timestamp, b.Timestamp)
			}), "entries must be sorted by timestamp: %+v", test.entries)
			array := TimestampsSeqArray{Timestamps: slices.Clone(test.entries)}
			array.trim(test.purgeTimestamp)
			assert.Equal(t, len(test.expected), len(array.Timestamps), "expected %d, got %d", len(test.expected), len(array.Timestamps))
			assert.Equal(t, test.expected, array.Timestamps)
			if test.purge {
				assert.LessOrEqual(t, array.oldestTimestamp(), test.purgeTimestamp, "expected %s, got %s", time.Unix(test.purgeTimestamp, 0).UTC().Format(time.RFC3339), time.Unix(array.oldestTimestamp(), 0).UTC().Format(time.RFC3339))
			}
		})
	}
}

// TestTimestampsManagerEntryDue makes sure that entryDue is true for an empty array or once the newest entry is older than the update frequency.
func TestTimestampsManagerEntryDue(t *testing.T) {
	now := time.Now()
	// entryAge returns a single entry that is the given age old
	entryAge := func(age time.Duration) []TimestampsSeqPair {
		return []TimestampsSeqPair{{Seq: 1, Timestamp: now.Add(-age).Unix()}}
	}
	tests := []struct {
		name          string
		purgeInterval time.Duration
		entries       []TimestampsSeqPair
		expected      bool
	}{
		// a purge interval of NumTimestampsEntries hours gives an update frequency of one hour
		{name: "empty array", purgeInterval: NumTimestampsEntries * time.Hour, entries: nil, expected: true},
		{name: "newest entry within update frequency", purgeInterval: NumTimestampsEntries * time.Hour, entries: entryAge(30 * time.Minute), expected: false},
		{name: "newest entry older than update frequency", purgeInterval: NumTimestampsEntries * time.Hour, entries: entryAge(90 * time.Minute), expected: true},
		// a purge interval of one hour would give one minute, so the update frequency is raised to the 10 minute minimum
		{name: "minimum update frequency, entry within it", purgeInterval: time.Hour, entries: entryAge(5 * time.Minute), expected: false},
		{name: "minimum update frequency, entry older than it", purgeInterval: time.Hour, entries: entryAge(15 * time.Minute), expected: true},
		{
			name:          "only the newest entry counts",
			purgeInterval: NumTimestampsEntries * time.Hour,
			entries: []TimestampsSeqPair{
				{Seq: 1, Timestamp: now.Add(-5 * time.Hour).Unix()},
				{Seq: 2, Timestamp: now.Add(-10 * time.Minute).Unix()},
			},
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tm := NewTimestampsManager(TimestampsMgrOpts{
				purgeIntervalFunc: func() time.Duration { return test.purgeInterval },
			})
			tm.timestampsSeqArray.Store(&TimestampsSeqArray{Timestamps: test.entries})
			assert.Equal(t, test.expected, tm.entryDue())
		})
	}
}

// TestTimestampsManagerAddEntry makes sure that addEntry adds an entry to the stored timestamps document and trims it by the purge interval.
func TestTimestampsManagerAddEntry(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	mgrOpts := TimestampsMgrOpts{
		ctx:           ctx,
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
		getSequenceFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil
		},
	}

	tm := NewTimestampsManager(mgrOpts)
	require.NotNil(t, tm)

	err := tm.Start()
	require.NoError(t, err)

	// cleanupFunc resets the document to an empty array, so that subtests do not depend on each other
	cleanupFunc := func() {
		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: []TimestampsSeqPair{}})
	}

	t.Run("empty array", func(t *testing.T) {
		defer cleanupFunc()
		// an empty array is always due
		err := tm.addEntry()
		require.NoError(t, err)
		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Len(t, tsDoc.Timestamps, 1)
	})

	t.Run("complete array", func(t *testing.T) {
		defer cleanupFunc()
		// Entries are an hour apart and the newest is older than the update frequency, so an entry is due.
		// The two oldest entries are at or before the purge timestamp, so adding one entry trims one.
		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-61*time.Hour), now.Add(-2*time.Hour), time.Hour, 0)
		require.Len(t, pairs, NumTimestampsEntries)

		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		require.NoError(t, tm.addEntry())

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		require.Len(t, tsDoc.Timestamps, NumTimestampsEntries)
		assert.Equal(t, pairs[1], tsDoc.Timestamps[0], "oldest entry must be trimmed")
		assert.GreaterOrEqual(t, tsDoc.newestTimestamp(), now.Unix(), "new entry must be added at the end")
		assert.Len(t, tm.timestampsSeqArray.Load().Timestamps, NumTimestampsEntries)
	})

	t.Run("entries within purge interval are not trimmed", func(t *testing.T) {
		defer cleanupFunc()
		const day = 24 * time.Hour
		// A 60 day purge interval gives an update frequency of one day, so the entry at day -1 is old enough for a new one.
		longPurgeMgr := NewTimestampsManager(TimestampsMgrOpts{
			ctx:               ctx,
			metadataStore:     metadataStore,
			metaKeys:          metaKeys,
			purgeIntervalFunc: func() time.Duration { return 60 * day },
			getSequenceFunc:   mgrOpts.getSequenceFunc,
		})

		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-59*day), now.Add(-30*day), 12*time.Hour, 0)
		pairs = append(pairs, GenerateTimestampsSeqPairs(now.Add(-29*day), now.Add(-1*day), day, 0)...)

		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		require.NoError(t, longPurgeMgr.addEntry())

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		require.Len(t, tsDoc.Timestamps, len(pairs)+1)
		assert.Equal(t, pairs, tsDoc.Timestamps[:len(pairs)], "existing entries must not be trimmed")
		assert.GreaterOrEqual(t, tsDoc.newestTimestamp(), now.Unix(), "new entry must be added at the end")
	})

	t.Run("multiple calls within update frequency", func(t *testing.T) {
		defer cleanupFunc()
		// The first call adds an entry to the empty array. The newest entry is then within the update frequency,
		// so later calls cancel the update and add nothing.
		require.NoError(t, tm.addEntry())
		for i := range 4 {
			require.NoError(t, tm.addEntry(), "call %d", i+2)
		}

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Len(t, tm.timestampsSeqArray.Load().Timestamps, 1)
	})

	t.Run("remove invalid entries", func(t *testing.T) {
		defer cleanupFunc()

		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-4*24*time.Hour), now.Add(-24*time.Hour), time.Hour*24, 50)
		require.Len(t, pairs, 4)
		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		// the sequence has rolled back to 51, so the entries above it are invalid
		rollbackOpts := mgrOpts
		rollbackOpts.getSequenceFunc = func(_ context.Context) (uint64, error) {
			return 51, nil
		}
		rollbackMgr := NewTimestampsManager(rollbackOpts)
		require.NoError(t, rollbackMgr.addEntry())
		assert.Len(t, rollbackMgr.timestampsSeqArray.Load().Timestamps, 2)
	})

	t.Run("rollback with no entry due", func(t *testing.T) {
		defer cleanupFunc()

		// the purge interval gives an update frequency of one hour, and the newest entry is 10 minutes old, so no entry is due
		now := time.Now()
		pairs := []TimestampsSeqPair{
			{Seq: 10, Timestamp: now.Add(-3 * time.Hour).Unix()},
			{Seq: 20, Timestamp: now.Add(-2 * time.Hour).Unix()},
			{Seq: 30, Timestamp: now.Add(-30 * time.Minute).Unix()},
			{Seq: 40, Timestamp: now.Add(-10 * time.Minute).Unix()},
		}
		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		// the sequence has rolled back to 20, so the last two entries are invalid
		rollbackOpts := mgrOpts
		rollbackOpts.getSequenceFunc = func(_ context.Context) (uint64, error) {
			return 20, nil
		}
		rollbackMgr := NewTimestampsManager(rollbackOpts)
		require.NoError(t, rollbackMgr.addEntry())

		// the invalid entries are removed, and no new entry is appended
		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Equal(t, pairs[:2], tsDoc.Timestamps)
		assert.Equal(t, tsDoc, *rollbackMgr.timestampsSeqArray.Load())
	})

	t.Run("getSequenceFunc error", func(t *testing.T) {
		defer cleanupFunc()

		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-4*24*time.Hour), now.Add(-24*time.Hour), time.Hour*24, 50)
		require.Len(t, pairs, 4)
		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		errSequence := errors.New("sequence unavailable")
		errOpts := mgrOpts
		errOpts.getSequenceFunc = func(_ context.Context) (uint64, error) {
			return 0, errSequence
		}
		errMgr := NewTimestampsManager(errOpts)
		errMgr.updateTimestampsArray(TimestampsSeqArray{Timestamps: slices.Clone(pairs)})

		require.ErrorIs(t, errMgr.addEntry(), errSequence)

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Equal(t, pairs, tsDoc.Timestamps)
		assert.Equal(t, pairs, errMgr.timestampsSeqArray.Load().Timestamps)
	})

	t.Run("document is not valid JSON", func(t *testing.T) {
		defer cleanupFunc()
		invalidDoc := []byte("not json")
		require.NoError(t, metadataStore.Set(ctx, metaKeys.TimestampsSeqKey(), 0, nil, invalidDoc))
		memoryBefore := tm.timestampsSeqArray.Load()

		require.Error(t, tm.addEntry())

		// the document is left as it was, and the in-memory array is not replaced
		stored, _, err := metadataStore.GetRaw(ctx, metaKeys.TimestampsSeqKey())
		require.NoError(t, err)
		assert.Equal(t, invalidDoc, stored)
		assert.Same(t, memoryBefore, tm.timestampsSeqArray.Load())
	})

	t.Run("newest entry ahead of the clock", func(t *testing.T) {
		defer cleanupFunc()

		// another node with a faster clock added an entry that is still in the future for this node
		pairs := []TimestampsSeqPair{{Seq: 0, Timestamp: time.Now().Add(2 * time.Hour).Unix()}}
		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		// the update is cancelled without an error, and the stored document is loaded into memory
		require.NoError(t, tm.addEntry())

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Equal(t, pairs, tsDoc.Timestamps)
		assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
	})

	t.Run("non-positive purge interval", func(t *testing.T) {
		defer cleanupFunc()

		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-40*24*time.Hour), now.Add(-24*time.Hour), 12*time.Hour, 0)
		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		// the default purge interval is used, so entries older than it are trimmed
		zeroPurgeOpts := mgrOpts
		zeroPurgeOpts.purgeIntervalFunc = func() time.Duration { return 0 }
		zeroPurgeMgr := NewTimestampsManager(zeroPurgeOpts)
		require.NoError(t, zeroPurgeMgr.addEntry())

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		require.Less(t, len(tsDoc.Timestamps), len(pairs)+1, "document must be trimmed")
		purgeTimestamp := now.Add(-base.DefaultClientPartitionWindow).Unix()
		assert.LessOrEqual(t, tsDoc.oldestTimestamp(), purgeTimestamp)
		assert.Greater(t, tsDoc.Timestamps[1].Timestamp, purgeTimestamp, "only one entry at or before the purge timestamp is kept")
	})

	t.Run("document deleted", func(t *testing.T) {
		defer cleanupFunc()
		require.NoError(t, tm.addEntry())
		require.NoError(t, metadataStore.Delete(ctx, metaKeys.TimestampsSeqKey()))

		// the document is recreated from the in-memory array, which still holds the entry added above
		require.NoError(t, tm.addEntry())

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
	})
}

// TestConcurrentAddEntry makes sure that concurrent addEntry calls on an empty document add exactly one entry, and none of them fail.
func TestConcurrentAddEntry(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	tm := NewTimestampsManager(TimestampsMgrOpts{
		ctx:           ctx,
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
		getSequenceFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil
		},
	})
	require.NoError(t, tm.Start())

	const numGoroutines = 20
	errs := make(chan error, numGoroutines)
	// start is closed once every goroutine exists, so that the calls overlap as much as possible
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range numGoroutines {
		wg.Go(func() {
			<-start
			errs <- tm.addEntry()
		})
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		assert.NoError(t, err)
	}

	// Whichever call wins the write adds the entry. The rest see it within the update frequency and add nothing.
	var tsDoc TimestampsSeqArray
	_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
	require.NoError(t, err)
	assert.Len(t, tsDoc.Timestamps, 1)
	assert.Len(t, tm.timestampsSeqArray.Load().Timestamps, 1)
}

// TestConcurrentAddEntryMultipleManagers makes sure that managers on several nodes calling addEntry at the same time
// add exactly one entry to a new document, none of them fail, and every node ends up with the stored document in memory.
func TestConcurrentAddEntryMultipleManagers(t *testing.T) {
	const (
		numNodes   = 5
		iterations = 100
	)
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()

	for i := range iterations {
		// a new document for each iteration, so that every iteration starts with an empty array
		metaKeys := base.NewMetadataKeys(fmt.Sprintf("%s_%d", t.Name(), i))
		opts := TimestampsMgrOpts{
			ctx:           ctx,
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			purgeIntervalFunc: func() time.Duration {
				return NumTimestampsEntries * time.Hour
			},
			getSequenceFunc: func(ctx context.Context) (uint64, error) {
				return 0, nil
			},
		}
		managers := make([]*TimestampsManager, numNodes)
		for n := range managers {
			managers[n] = NewTimestampsManager(opts)
		}
		// the first manager creates the document, so that the concurrent calls all race to add the first entry
		require.NoError(t, managers[0].Start())

		errs := make(chan error, numNodes)
		// start is closed once every goroutine exists, so that the calls overlap as much as possible
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, tm := range managers {
			wg.Go(func() {
				<-start
				errs <- tm.addEntry()
			})
		}
		close(start)
		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err, "iteration %d", i)
		}

		var tsDoc TimestampsSeqArray
		_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		require.Len(t, tsDoc.Timestamps, 1, "iteration %d", i)
		for n, tm := range managers {
			require.Equal(t, tsDoc, *tm.timestampsSeqArray.Load(), "iteration %d, manager %d", i, n)
		}
	}
}

// TestTimestampsManagerJoinExistingCluster makes sure that Start on a node joining a cluster leaves the existing
// timestamps document untouched and loads it into memory, whether the document exists before Start or is
// created by another node while Start is running.
func TestTimestampsManagerJoinExistingCluster(t *testing.T) {
	newOpts := func(ctx context.Context, metadataStore base.DataStore, metaKeys *base.MetadataKeys) TimestampsMgrOpts {
		return TimestampsMgrOpts{
			ctx:           ctx,
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			purgeIntervalFunc: func() time.Duration {
				return NumTimestampsEntries * time.Hour
			},
			getSequenceFunc: func(ctx context.Context) (uint64, error) {
				return 0, nil
			},
		}
	}
	// requireJoinedNode asserts the document still holds node 1's single entry, and that node 2 mirrors it in memory.
	requireJoinedNode := func(t *testing.T, ctx context.Context, metadataStore base.DataStore, metaKeys *base.MetadataKeys, tm2 *TimestampsManager) {
		var tsDoc TimestampsSeqArray
		_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		require.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *tm2.timestampsSeqArray.Load())
		assert.False(t, tm2.entryDue())
	}

	t.Run("document exists before Start", func(t *testing.T) {
		testBucket := base.GetTestBucket(t)
		ctx := base.TestCtx(t)
		defer testBucket.Close(ctx)
		metadataStore := testBucket.GetMetadataStore()
		metaKeys := base.NewMetadataKeys(t.Name())

		tm1 := NewTimestampsManager(newOpts(ctx, metadataStore, metaKeys))
		require.NoError(t, tm1.Start())
		require.NoError(t, tm1.addEntry())

		tm2 := NewTimestampsManager(newOpts(ctx, metadataStore, metaKeys))
		require.NoError(t, tm2.Start())
		requireJoinedNode(t, ctx, metadataStore, metaKeys, tm2)
	})

	t.Run("document created while Start is running", func(t *testing.T) {
		testBucket := base.GetTestBucket(t)
		ctx := base.TestCtx(t)
		defer testBucket.Close(ctx)
		metadataStore := testBucket.GetMetadataStore()
		metaKeys := base.NewMetadataKeys(t.Name())

		tm1 := NewTimestampsManager(newOpts(ctx, metadataStore, metaKeys))
		// The callback runs after node 2 sees no document but before it writes, so node 1 wins the race and
		// node 2's write fails on CAS and retries against node 1's document.
		var once atomic.Bool
		leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
			UpdateCallback: func(key string) {
				if key == metaKeys.TimestampsSeqKey() && once.CompareAndSwap(false, true) {
					require.NoError(t, tm1.Start())
					require.NoError(t, tm1.addEntry())
				}
			},
		})

		tm2 := NewTimestampsManager(newOpts(ctx, leakyBucket.GetMetadataStore(), metaKeys))
		require.NoError(t, tm2.Start())
		requireJoinedNode(t, ctx, metadataStore, metaKeys, tm2)
	})
}

// TestTimestampsManagerCancelledAddEntryLoadsOtherNodeEntry makes sure that an addEntry on node 2 that is cancelled
// because node 1 already added an entry replaces node 2's stale in-memory array with the stored document.
func TestTimestampsManagerCancelledAddEntryLoadsOtherNodeEntry(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	opts := TimestampsMgrOpts{
		ctx:           ctx,
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
		getSequenceFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil
		},
	}

	tm1 := NewTimestampsManager(opts)
	require.NoError(t, tm1.Start())
	require.NoError(t, tm1.addEntry())

	// node 2 only knows about an old entry, so from its point of view an entry is due
	tm2 := NewTimestampsManager(opts)
	tm2.updateTimestampsArray(TimestampsSeqArray{Timestamps: []TimestampsSeqPair{
		{Seq: 0, Timestamp: time.Now().Add(-2 * time.Hour).Unix()},
	}})
	require.True(t, tm2.entryDue())

	// node 1's entry is within the update frequency, so node 2 adds nothing and loads node 1's entry
	require.NoError(t, tm2.addEntry())

	var tsDoc TimestampsSeqArray
	_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
	require.NoError(t, err)
	assert.Len(t, tsDoc.Timestamps, 1)
	assert.Equal(t, tsDoc, *tm2.timestampsSeqArray.Load())
	assert.False(t, tm2.entryDue())
}

// clockStart is the fixed time at which the clocks of the clock tests start.
var clockStart = time.Unix(1_700_000_000, 0)

// testClock is a manually controlled clock, so that tests can move time forward and backward.
type testClock struct {
	unix atomic.Int64
}

func newTestClock(start time.Time) *testClock {
	c := &testClock{}
	c.Set(start)
	return c
}

// Now returns the current time of the clock.
func (c *testClock) Now() time.Time { return time.Unix(c.unix.Load(), 0) }

// Set moves the clock to t, which can be before the current time.
func (c *testClock) Set(t time.Time) { c.unix.Store(t.Unix()) }

// Advance moves the clock by d, which can be negative.
func (c *testClock) Advance(d time.Duration) { c.unix.Add(int64(d / time.Second)) }

// newClockTestManager returns a manager that reads the time from clock. A nil purgeInterval gives a purge interval of
// NumTimestampsEntries hours, and so an update frequency of one hour.
func newClockTestManager(ctx context.Context, metadataStore base.DataStore, metaKeys *base.MetadataKeys, clock *testClock, purgeInterval PurgeIntervalFunc) *TimestampsManager {
	if purgeInterval == nil {
		purgeInterval = func() time.Duration { return NumTimestampsEntries * time.Hour }
	}
	return NewTimestampsManager(TimestampsMgrOpts{
		ctx:               ctx,
		metadataStore:     metadataStore,
		metaKeys:          metaKeys,
		purgeIntervalFunc: purgeInterval,
		getSequenceFunc:   func(context.Context) (uint64, error) { return 0, nil },
		nowFunc:           clock.Now,
	})
}

// getTimestampsDoc returns the stored timestamps document, and checks that its entries are sorted by timestamp.
func getTimestampsDoc(t *testing.T, ctx context.Context, metadataStore base.DataStore, metaKeys *base.MetadataKeys) TimestampsSeqArray {
	t.Helper()
	var tsDoc TimestampsSeqArray
	_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
	require.NoError(t, err)
	require.True(t, slices.IsSortedFunc(tsDoc.Timestamps, func(a, b TimestampsSeqPair) int {
		return cmp.Compare(a.Timestamp, b.Timestamp)
	}), "entries must be sorted by timestamp: %+v", tsDoc.Timestamps)
	return tsDoc
}

// TestTimestampsManagerUpdateFrequencyBoundary makes sure that an entry is added exactly when the newest entry is one
// update frequency old, and that entryDue agrees with addEntry at that boundary.
func TestTimestampsManagerUpdateFrequencyBoundary(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	// a purge interval of NumTimestampsEntries hours gives an update frequency of one hour
	const updateFrequency = time.Hour
	tests := []struct {
		name        string
		elapsed     time.Duration
		due         bool
		expectedLen int
	}{
		{name: "one second before the boundary", elapsed: updateFrequency - time.Second, due: false, expectedLen: 1},
		{name: "exactly at the boundary", elapsed: updateFrequency, due: true, expectedLen: 2},
		{name: "one second after the boundary", elapsed: updateFrequency + time.Second, due: true, expectedLen: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newTestClock(clockStart)
			tm := newClockTestManager(ctx, metadataStore, metaKeys, clock, nil)
			newest := []TimestampsSeqPair{{Seq: 0, Timestamp: clockStart.Unix()}}
			tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: newest})
			tm.updateTimestampsArray(TimestampsSeqArray{Timestamps: slices.Clone(newest)})
			clock.Advance(test.elapsed)

			assert.Equal(t, test.due, tm.entryDue())
			require.NoError(t, tm.addEntry())
			assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, test.expectedLen)
		})
	}
}

// TestTimestampsManagerSteadyStateSize makes sure that the document grows to its steady-state size and stays there
// across several purge intervals, with an entry added every update frequency.
func TestTimestampsManagerSteadyStateSize(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	const (
		purgeInterval   = NumTimestampsEntries * time.Hour
		updateFrequency = time.Hour
		purgeIntervals  = 3
	)
	clock := newTestClock(clockStart)
	tm := newClockTestManager(ctx, metadataStore, metaKeys, clock, nil)
	require.NoError(t, tm.Start())

	// The entry at or before the purge timestamp is kept, so a full document holds NumTimestampsEntries+1 entries.
	for i := range purgeIntervals * NumTimestampsEntries {
		require.NoError(t, tm.addEntry(), "call %d", i)
		expectedLen := min(i+1, NumTimestampsEntries+1)
		require.Len(t, tm.timestampsSeqArray.Load().Timestamps, expectedLen, "call %d", i)
		clock.Advance(updateFrequency)
	}

	tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
	assert.Len(t, tsDoc.Timestamps, NumTimestampsEntries+1)
	assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
	// the clock is one update frequency past the newest entry, which is the last call
	purgeTimestamp := clock.Now().Add(-updateFrequency - purgeInterval).Unix()
	assert.Equal(t, purgeTimestamp, tsDoc.oldestTimestamp(), "the oldest entry is the one at the purge timestamp")
}

// TestTimestampsManagerPurgeIntervalShrinks makes sure that when the purge interval shrinks, the next addEntry trims
// the document to the new interval, and entries are added at the new, higher rate.
func TestTimestampsManagerPurgeIntervalShrinks(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	var purgeInterval atomic.Int64
	purgeInterval.Store(int64(NumTimestampsEntries * time.Hour))
	clock := newTestClock(clockStart)
	tm := newClockTestManager(ctx, metadataStore, metaKeys, clock, func() time.Duration {
		return time.Duration(purgeInterval.Load())
	})
	require.NoError(t, tm.Start())

	// an entry every hour for more than a purge interval, which leaves a full document
	for range NumTimestampsEntries + 10 {
		require.NoError(t, tm.addEntry())
		clock.Advance(time.Hour)
	}
	require.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, NumTimestampsEntries+1)
	newest := tm.timestampsSeqArray.Load().newestTimestamp()

	// A purge interval of 10 hours gives an update frequency of 10 minutes, the minimum.
	purgeInterval.Store(int64(10 * time.Hour))
	clock.Set(time.Unix(newest, 0).Add(10 * time.Minute))
	require.NoError(t, tm.addEntry())

	// Entries newer than the purge timestamp are kept with the one before them, and the new entry is added.
	// The entries are an hour apart, so these are the 10 entries newest entries, the one before them, and the new one.
	tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
	purgeTimestamp := clock.Now().Add(-10 * time.Hour).Unix()
	assert.Len(t, tsDoc.Timestamps, 12)
	assert.LessOrEqual(t, tsDoc.oldestTimestamp(), purgeTimestamp)
	assert.Greater(t, tsDoc.Timestamps[1].Timestamp, purgeTimestamp)

	// the entry is due again after the new, shorter update frequency
	clock.Advance(10 * time.Minute)
	require.NoError(t, tm.addEntry())
	assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 13)
}

// TestTimestampsManagerClockSkew makes sure that nodes with clocks that differ, or that move backward, never break
// the order of the entries and never fail, and that a node with a fast clock only holds back the others for as long as
// its entry is within the update frequency of their clocks.
func TestTimestampsManagerClockSkew(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()

	// a purge interval of NumTimestampsEntries hours gives an update frequency of one hour
	const updateFrequency = time.Hour

	t.Run("node clock behind the newest entry", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clockA, clockB := newTestClock(clockStart), newTestClock(clockStart)
		nodeA := newClockTestManager(ctx, metadataStore, metaKeys, clockA, nil)
		nodeB := newClockTestManager(ctx, metadataStore, metaKeys, clockB, nil)
		require.NoError(t, nodeA.Start())
		require.NoError(t, nodeA.addEntry())

		// node B is two hours behind node A, so the newest entry is in its future
		clockB.Set(clockStart.Add(-2 * time.Hour))
		require.NoError(t, nodeB.addEntry())

		tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *nodeB.timestampsSeqArray.Load())

		// once the clock of node B passes the newest entry by the update frequency, it adds its entry
		clockB.Set(clockStart.Add(updateFrequency))
		require.NoError(t, nodeB.addEntry())
		assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2)
	})

	t.Run("node clock ahead holds back the other nodes", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clockA, clockB := newTestClock(clockStart), newTestClock(clockStart)
		nodeA := newClockTestManager(ctx, metadataStore, metaKeys, clockA, nil)
		nodeB := newClockTestManager(ctx, metadataStore, metaKeys, clockB, nil)
		require.NoError(t, nodeA.Start())
		require.NoError(t, nodeA.addEntry())

		// node B is two hours ahead of node A, and adds an entry stamped in the future of node A
		clockB.Set(clockStart.Add(2 * time.Hour))
		require.NoError(t, nodeB.addEntry())
		require.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2)

		// node A adds nothing while its clock is before that entry, or within the update frequency after it
		for _, elapsed := range []time.Duration{updateFrequency, 2 * time.Hour, 2*time.Hour + updateFrequency - time.Second} {
			clockA.Set(clockStart.Add(elapsed))
			require.NoError(t, nodeA.addEntry(), "elapsed %s", elapsed)
			assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2, "elapsed %s", elapsed)
		}
		assert.Equal(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys), *nodeA.timestampsSeqArray.Load())

		// then node A adds its entry, after the entry of node B
		clockA.Set(clockStart.Add(2*time.Hour + updateFrequency))
		require.NoError(t, nodeA.addEntry())
		assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 3)
	})

	t.Run("clock steps backward on the same node", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clock := newTestClock(clockStart)
		node := newClockTestManager(ctx, metadataStore, metaKeys, clock, nil)
		require.NoError(t, node.Start())
		require.NoError(t, node.addEntry())

		// the clock is stepped back by a day, then moves forward again, one update frequency at a time
		clock.Advance(-24 * time.Hour)
		for range 3 {
			require.NoError(t, node.addEntry())
			assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 1)
			clock.Advance(8 * time.Hour)
		}
		// the clock is now back at the time of the entry, plus one update frequency after the next step
		clock.Set(clockStart.Add(updateFrequency))
		require.NoError(t, node.addEntry())
		assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2)
	})

	t.Run("both clocks move in opposite directions", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clockA, clockB := newTestClock(clockStart), newTestClock(clockStart)
		nodeA := newClockTestManager(ctx, metadataStore, metaKeys, clockA, nil)
		nodeB := newClockTestManager(ctx, metadataStore, metaKeys, clockB, nil)
		require.NoError(t, nodeA.Start())

		// node A jumps forward and adds an entry, while node B jumps backward and adds nothing
		clockA.Set(clockStart.Add(5 * time.Hour))
		require.NoError(t, nodeA.addEntry())
		clockB.Set(clockStart.Add(-5 * time.Hour))
		require.NoError(t, nodeB.addEntry())
		tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *nodeB.timestampsSeqArray.Load())

		// node B jumps forward past node A, and now node A is the one that adds nothing
		clockB.Set(clockStart.Add(7 * time.Hour))
		require.NoError(t, nodeB.addEntry())
		clockA.Set(clockStart.Add(4 * time.Hour))
		require.NoError(t, nodeA.addEntry())
		tsDoc = getTimestampsDoc(t, ctx, metadataStore, metaKeys)
		assert.Len(t, tsDoc.Timestamps, 2)
		assert.Equal(t, tsDoc, *nodeA.timestampsSeqArray.Load())
	})

	t.Run("rollback is cleaned up when every clock is behind the newest entry", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		var currentSeq atomic.Uint64
		newNode := func(clock *testClock) *TimestampsManager {
			return NewTimestampsManager(TimestampsMgrOpts{
				ctx:               ctx,
				metadataStore:     metadataStore,
				metaKeys:          metaKeys,
				purgeIntervalFunc: func() time.Duration { return NumTimestampsEntries * time.Hour },
				getSequenceFunc:   func(context.Context) (uint64, error) { return currentSeq.Load(), nil },
				nowFunc:           clock.Now,
			})
		}
		// both clocks are an hour behind the oldest entry, so they are behind the newest entry at every step
		nodeA := newNode(newTestClock(clockStart.Add(-time.Hour)))
		nodeB := newNode(newTestClock(clockStart.Add(-time.Hour)))
		entries := []TimestampsSeqPair{
			{Seq: 10, Timestamp: clockStart.Unix()},
			{Seq: 20, Timestamp: clockStart.Add(time.Hour).Unix()},
			{Seq: 30, Timestamp: clockStart.Add(2 * time.Hour).Unix()},
		}
		currentSeq.Store(30)
		nodeA.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: entries})

		// the sequence rolls back to 10, so the last two entries are ahead of it
		currentSeq.Store(10)
		for name, node := range map[string]*TimestampsManager{"node A": nodeA, "node B": nodeB} {
			require.NoError(t, node.addEntry(), name)
			tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
			assert.Equal(t, entries[:1], tsDoc.Timestamps, name)
			assert.Equal(t, tsDoc, *node.timestampsSeqArray.Load(), name)
		}
	})
}

// CreateTSDocForTest writes tsArray to the timestamps document, replacing any existing content.
func (tm *TimestampsManager) CreateTSDocForTest(t *testing.T, tsArray TimestampsSeqArray) {
	t.Helper()
	require.NoError(t, tm.metadataStore.Set(tm.ctx, tm.metaKeys.TimestampsSeqKey(), 0, nil, tsArray))
}

// GenerateTimestampsSeqPairs returns one pair every duration from start up to and including end. If startSeq is
// non-zero, the sequences start at startSeq and increase by one per pair, otherwise every pair has sequence 0.
// It returns nil if duration is not positive, or if start is after end.
func GenerateTimestampsSeqPairs(start, end time.Time, duration time.Duration, startSeq uint64) []TimestampsSeqPair {
	var pairs []TimestampsSeqPair
	seq := startSeq
	for ts := start; duration > 0 && !ts.After(end); ts = ts.Add(duration) {
		pairs = append(pairs, TimestampsSeqPair{Seq: seq, Timestamp: ts.Unix()})
		if startSeq != 0 {
			seq++
		}
	}
	return pairs
}

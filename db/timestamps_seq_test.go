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

// TestTimestampsDocCreation makes sure that Start creates the timestamps document in the metadata store, and adds an
// entry for the current sequence without waiting for the background task.
func TestTimestampsDocCreation(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	mgrOpts := TimestampsMgrOpts{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
		getSequenceFunc: func(ctx context.Context) (uint64, error) {
			return 5, nil
		},
	}

	tm := NewTimestampsManager(mgrOpts)
	require.NotNil(t, tm, "NewTimestampsManager must return a TimestampsManager")

	err := tm.Start(ctx)
	require.NoError(t, err)
	defer tm.Stop(ctx)
	assert.NotNil(t, tm.timestampsSeqArray.Load())

	var tsDoc TimestampsSeqArray
	_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
	require.NoError(t, err)
	require.Len(t, tsDoc.Timestamps, 0)
	assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
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
		{name: "newest entry exactly update frequency old", purgeInterval: NumTimestampsEntries * time.Hour, entries: entryAge(time.Hour), expected: true},
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

// TestTimestampsManagerUpdateFrequency makes sure that the update frequency splits the purge interval across
// NumTimestampsEntries with a 10 minute minimum, and that a test override replaces it.
func TestTimestampsManagerUpdateFrequency(t *testing.T) {
	tests := []struct {
		name          string
		purgeInterval time.Duration
		override      *time.Duration
		expected      time.Duration
	}{
		{name: "default window", purgeInterval: base.DefaultClientPartitionWindow, expected: 12 * time.Hour},
		{name: "one hour per entry", purgeInterval: NumTimestampsEntries * time.Hour, expected: time.Hour},
		// one hour split across NumTimestampsEntries is one minute, below the minimum
		{name: "minimum", purgeInterval: time.Hour, expected: 10 * time.Minute},
		{name: "zero window uses the default window", purgeInterval: 0, expected: 12 * time.Hour},
		{name: "negative window uses the default window", purgeInterval: -5 * time.Second, expected: 12 * time.Hour},
		{name: "override ignores the minimum", purgeInterval: base.DefaultClientPartitionWindow, override: new(50 * time.Millisecond), expected: 50 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tm := NewTimestampsManager(TimestampsMgrOpts{
				purgeIntervalFunc:           func() time.Duration { return test.purgeInterval },
				testUpdateFrequencyOverride: test.override,
			})
			assert.Equal(t, test.expected, tm.updateFrequency())
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
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
		getSequenceFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil
		},
		dbName: t.Name(),
	}

	tm := NewTimestampsManager(mgrOpts)
	require.NotNil(t, tm)

	err := tm.Start(ctx)
	require.NoError(t, err)
	defer tm.Stop(ctx)

	// cleanupFunc resets the document to an empty array, so that subtests do not depend on each other
	cleanupFunc := func() {
		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: []TimestampsSeqPair{}})
	}
	// Start has already added an entry
	cleanupFunc()

	t.Run("empty array", func(t *testing.T) {
		defer cleanupFunc()
		// an empty array is always due
		err := tm.addEntry(ctx)
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

		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		require.NoError(t, tm.addEntry(ctx))

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
			metadataStore:     metadataStore,
			metaKeys:          metaKeys,
			purgeIntervalFunc: func() time.Duration { return 60 * day },
			getSequenceFunc:   mgrOpts.getSequenceFunc,
		})

		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-59*day), now.Add(-30*day), 12*time.Hour, 0)
		pairs = append(pairs, GenerateTimestampsSeqPairs(now.Add(-29*day), now.Add(-1*day), day, 0)...)

		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		require.NoError(t, longPurgeMgr.addEntry(ctx))

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
		require.NoError(t, tm.addEntry(ctx))
		for i := range 4 {
			require.NoError(t, tm.addEntry(ctx), "call %d", i+2)
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
		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		// the sequence has rolled back to 51, so the entries above it are invalid
		rollbackOpts := mgrOpts
		rollbackOpts.getSequenceFunc = func(_ context.Context) (uint64, error) {
			return 51, nil
		}
		rollbackMgr := NewTimestampsManager(rollbackOpts)
		require.NoError(t, rollbackMgr.addEntry(ctx))
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
		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		// the sequence has rolled back to 20, so the last two entries are invalid
		rollbackOpts := mgrOpts
		rollbackOpts.getSequenceFunc = func(_ context.Context) (uint64, error) {
			return 20, nil
		}
		rollbackMgr := NewTimestampsManager(rollbackOpts)
		require.NoError(t, rollbackMgr.addEntry(ctx))

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
		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		errSequence := errors.New("sequence unavailable")
		errOpts := mgrOpts
		errOpts.getSequenceFunc = func(_ context.Context) (uint64, error) {
			return 0, errSequence
		}
		errMgr := NewTimestampsManager(errOpts)
		errMgr.updateTimestampsArray(TimestampsSeqArray{Timestamps: slices.Clone(pairs)})

		require.ErrorIs(t, errMgr.addEntry(ctx), errSequence)

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Equal(t, pairs, tsDoc.Timestamps)
		assert.Equal(t, pairs, errMgr.timestampsSeqArray.Load().Timestamps)
	})

	t.Run("document is not valid JSON", func(t *testing.T) {
		defer cleanupFunc()
		// an entry in memory, so that replacing the in-memory copy can be seen
		require.NoError(t, tm.addEntry(ctx))
		require.Len(t, tm.timestampsSeqArray.Load().Timestamps, 1)
		require.NoError(t, metadataStore.Set(ctx, metaKeys.TimestampsSeqKey(), 0, nil, []byte("not json")))

		require.NoError(t, tm.addEntry(ctx))

		// the document is reset to an empty array, and the in-memory copy is replaced with it
		var tsDoc TimestampsSeqArray
		_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err, "the document must be reset to valid JSON")
		assert.Empty(t, tsDoc.Timestamps)
		assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
	})

	t.Run("newest entry ahead of the clock", func(t *testing.T) {
		defer cleanupFunc()

		// another node with a faster clock added an entry that is still in the future for this node
		pairs := []TimestampsSeqPair{{Seq: 0, Timestamp: time.Now().Add(2 * time.Hour).Unix()}}
		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		// the update is cancelled without an error, and the stored document is loaded into memory
		require.NoError(t, tm.addEntry(ctx))

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
		tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

		// the default purge interval is used, so entries older than it are trimmed
		zeroPurgeOpts := mgrOpts
		zeroPurgeOpts.purgeIntervalFunc = func() time.Duration { return 0 }
		zeroPurgeMgr := NewTimestampsManager(zeroPurgeOpts)
		require.NoError(t, zeroPurgeMgr.addEntry(ctx))

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
		require.NoError(t, tm.addEntry(ctx))
		require.NoError(t, metadataStore.Delete(ctx, metaKeys.TimestampsSeqKey()))

		// the document is recreated from the in-memory array, which still holds the entry added above
		require.NoError(t, tm.addEntry(ctx))

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
	})

	t.Run("missing document", func(t *testing.T) {
		defer cleanupFunc()
		require.NoError(t, tm.addEntry(ctx))
		inMemory := tm.timestampsSeqArray.Load()
		require.Len(t, inMemory.Timestamps, 1)

		require.NoError(t, metadataStore.Delete(ctx, metaKeys.TimestampsSeqKey()))
		require.NoError(t, tm.addEntry(ctx), "timestamps_seq should be recreated")
		assert.Equal(t, inMemory, tm.timestampsSeqArray.Load(), "a missing document should not change the in memory copy's value")

		_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &TimestampsSeqArray{})
		assert.NoError(t, err, "addEntry must create the document, got %v", err)
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
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		purgeIntervalFunc: func() time.Duration {
			return NumTimestampsEntries * time.Hour
		},
		getSequenceFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil
		},
	})
	require.NoError(t, tm.Start(ctx))
	defer tm.Stop(ctx)
	// Start has already added an entry, so empty the array for the calls to race over
	tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: []TimestampsSeqPair{}})

	const numGoroutines = 20
	errs := make(chan error, numGoroutines)
	// start is closed once every goroutine exists, so that the calls overlap as much as possible
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range numGoroutines {
		wg.Go(func() {
			<-start
			errs <- tm.addEntry(ctx)
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
		require.NoError(t, managers[0].Start(ctx))

		errs := make(chan error, numNodes)
		// start is closed once every goroutine exists, so that the calls overlap as much as possible
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, tm := range managers {
			wg.Go(func() {
				<-start
				errs <- tm.addEntry(ctx)
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
	newOpts := func(t *testing.T, ctx context.Context, metadataStore base.DataStore, metaKeys *base.MetadataKeys, dbName string) TimestampsMgrOpts {
		return TimestampsMgrOpts{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			purgeIntervalFunc: func() time.Duration {
				return NumTimestampsEntries * time.Hour
			},
			getSequenceFunc: func(ctx context.Context) (uint64, error) {
				return 0, nil
			},
			dbName: dbName,
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

		tm1 := NewTimestampsManager(newOpts(t, ctx, metadataStore, metaKeys, t.Name()))
		require.NoError(t, tm1.Start(ctx))
		require.NoError(t, tm1.addEntry(ctx))
		defer tm1.Stop(ctx)

		tm2 := NewTimestampsManager(newOpts(t, ctx, metadataStore, metaKeys, t.Name()))
		require.NoError(t, tm2.Start(ctx))
		defer tm2.Stop(ctx)
		requireJoinedNode(t, ctx, metadataStore, metaKeys, tm2)
	})

	t.Run("document created while Start is running", func(t *testing.T) {
		testBucket := base.GetTestBucket(t)
		ctx := base.TestCtx(t)
		defer testBucket.Close(ctx)
		metadataStore := testBucket.GetMetadataStore()
		metaKeys := base.NewMetadataKeys(t.Name())

		tm1 := NewTimestampsManager(newOpts(t, ctx, metadataStore, metaKeys, t.Name()))
		// The callback runs after node 2 sees no document but before it writes, so node 1 wins the race and
		// node 2's write fails on CAS and retries against node 1's document.
		var once atomic.Bool
		leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
			UpdateCallback: func(key string) {
				if key == metaKeys.TimestampsSeqKey() && once.CompareAndSwap(false, true) {
					require.NoError(t, tm1.Start(ctx))
					require.NoError(t, tm1.addEntry(ctx))
				}
			},
		})

		tm2 := NewTimestampsManager(newOpts(t, ctx, leakyBucket.GetMetadataStore(), metaKeys, t.Name()))
		require.NoError(t, tm2.Start(ctx))
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
	require.NoError(t, tm1.Start(ctx))
	require.NoError(t, tm1.addEntry(ctx))

	// node 2 only knows about an old entry, so from its point of view an entry is due
	tm2 := NewTimestampsManager(opts)
	tm2.updateTimestampsArray(TimestampsSeqArray{Timestamps: []TimestampsSeqPair{
		{Seq: 0, Timestamp: time.Now().Add(-2 * time.Hour).Unix()},
	}})
	require.True(t, tm2.entryDue())

	// node 1's entry is within the update frequency, so node 2 adds nothing and loads node 1's entry
	require.NoError(t, tm2.addEntry(ctx))

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
func newClockTestManager(ctx context.Context, metadataStore base.DataStore, metaKeys *base.MetadataKeys, clock *testClock, purgeInterval PurgeIntervalFunc, dbName string) *TimestampsManager {
	if purgeInterval == nil {
		purgeInterval = func() time.Duration { return NumTimestampsEntries * time.Hour }
	}
	return NewTimestampsManager(TimestampsMgrOpts{
		metadataStore:     metadataStore,
		metaKeys:          metaKeys,
		purgeIntervalFunc: purgeInterval,
		getSequenceFunc:   func(context.Context) (uint64, error) { return 0, nil },
		nowFunc:           clock.Now,
		dbName:            dbName,
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
			tm := newClockTestManager(ctx, metadataStore, metaKeys, clock, nil, t.Name())
			newest := []TimestampsSeqPair{{Seq: 0, Timestamp: clockStart.Unix()}}
			tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: newest})
			tm.updateTimestampsArray(TimestampsSeqArray{Timestamps: slices.Clone(newest)})
			clock.Advance(test.elapsed)

			assert.Equal(t, test.due, tm.entryDue())
			require.NoError(t, tm.addEntry(ctx))
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
	tm := newClockTestManager(ctx, metadataStore, metaKeys, clock, nil, t.Name())
	require.NoError(t, tm.Start(ctx))

	// The entry at or before the purge timestamp is kept, so a full document holds NumTimestampsEntries+1 entries.
	for i := range purgeIntervals * NumTimestampsEntries {
		require.NoError(t, tm.addEntry(ctx), "call %d", i)
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
	}, t.Name())
	require.NoError(t, tm.Start(ctx))

	// an entry every hour for more than a purge interval, which leaves a full document
	for range NumTimestampsEntries + 10 {
		require.NoError(t, tm.addEntry(ctx))
		clock.Advance(time.Hour)
	}
	require.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, NumTimestampsEntries+1)
	newest := tm.timestampsSeqArray.Load().newestTimestamp()

	// A purge interval of 10 hours gives an update frequency of 10 minutes, the minimum.
	purgeInterval.Store(int64(10 * time.Hour))
	clock.Set(time.Unix(newest, 0).Add(10 * time.Minute))
	require.NoError(t, tm.addEntry(ctx))

	// Entries newer than the purge timestamp are kept with the one before them, and the new entry is added.
	// The entries are an hour apart, so these are the 10 entries newest entries, the one before them, and the new one.
	tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
	purgeTimestamp := clock.Now().Add(-10 * time.Hour).Unix()
	assert.Len(t, tsDoc.Timestamps, 12)
	assert.LessOrEqual(t, tsDoc.oldestTimestamp(), purgeTimestamp)
	assert.Greater(t, tsDoc.Timestamps[1].Timestamp, purgeTimestamp)

	// the entry is due again after the new, shorter update frequency
	clock.Advance(10 * time.Minute)
	require.NoError(t, tm.addEntry(ctx))
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
		nodeA := newClockTestManager(ctx, metadataStore, metaKeys, clockA, nil, t.Name())
		nodeB := newClockTestManager(ctx, metadataStore, metaKeys, clockB, nil, t.Name())
		require.NoError(t, nodeA.Start(ctx))
		require.NoError(t, nodeA.addEntry(ctx))

		// node B is two hours behind node A, so the newest entry is in its future
		clockB.Set(clockStart.Add(-2 * time.Hour))
		require.NoError(t, nodeB.addEntry(ctx))

		tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *nodeB.timestampsSeqArray.Load())

		// once the clock of node B passes the newest entry by the update frequency, it adds its entry
		clockB.Set(clockStart.Add(updateFrequency))
		require.NoError(t, nodeB.addEntry(ctx))
		assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2)
	})

	t.Run("node clock ahead holds back the other nodes", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clockA, clockB := newTestClock(clockStart), newTestClock(clockStart)
		nodeA := newClockTestManager(ctx, metadataStore, metaKeys, clockA, nil, t.Name())
		nodeB := newClockTestManager(ctx, metadataStore, metaKeys, clockB, nil, t.Name())
		require.NoError(t, nodeA.Start(ctx))
		require.NoError(t, nodeA.addEntry(ctx))

		// node B is two hours ahead of node A, and adds an entry stamped in the future of node A
		clockB.Set(clockStart.Add(2 * time.Hour))
		require.NoError(t, nodeB.addEntry(ctx))
		require.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2)

		// node A adds nothing while its clock is before that entry, or within the update frequency after it
		for _, elapsed := range []time.Duration{updateFrequency, 2 * time.Hour, 2*time.Hour + updateFrequency - time.Second} {
			clockA.Set(clockStart.Add(elapsed))
			require.NoError(t, nodeA.addEntry(ctx), "elapsed %s", elapsed)
			assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2, "elapsed %s", elapsed)
		}
		assert.Equal(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys), *nodeA.timestampsSeqArray.Load())

		// then node A adds its entry, after the entry of node B
		clockA.Set(clockStart.Add(2*time.Hour + updateFrequency))
		require.NoError(t, nodeA.addEntry(ctx))
		assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 3)
	})

	t.Run("clock steps backward on the same node", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clock := newTestClock(clockStart)
		node := newClockTestManager(ctx, metadataStore, metaKeys, clock, nil, t.Name())
		require.NoError(t, node.Start(ctx))
		require.NoError(t, node.addEntry(ctx))

		// the clock is stepped back by a day, then moves forward again, one update frequency at a time
		clock.Advance(-24 * time.Hour)
		for range 3 {
			require.NoError(t, node.addEntry(ctx))
			assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 1)
			clock.Advance(8 * time.Hour)
		}
		// the clock is now back at the time of the entry, plus one update frequency after the next step
		clock.Set(clockStart.Add(updateFrequency))
		require.NoError(t, node.addEntry(ctx))
		assert.Len(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys).Timestamps, 2)
	})

	t.Run("both clocks move in opposite directions", func(t *testing.T) {
		metaKeys := base.NewMetadataKeys(t.Name())
		clockA, clockB := newTestClock(clockStart), newTestClock(clockStart)
		nodeA := newClockTestManager(ctx, metadataStore, metaKeys, clockA, nil, t.Name())
		nodeB := newClockTestManager(ctx, metadataStore, metaKeys, clockB, nil, t.Name())
		require.NoError(t, nodeA.Start(ctx))

		// node A jumps forward and adds an entry, while node B jumps backward and adds nothing
		clockA.Set(clockStart.Add(5 * time.Hour))
		require.NoError(t, nodeA.addEntry(ctx))
		clockB.Set(clockStart.Add(-5 * time.Hour))
		require.NoError(t, nodeB.addEntry(ctx))
		tsDoc := getTimestampsDoc(t, ctx, metadataStore, metaKeys)
		assert.Len(t, tsDoc.Timestamps, 1)
		assert.Equal(t, tsDoc, *nodeB.timestampsSeqArray.Load())

		// node B jumps forward past node A, and now node A is the one that adds nothing
		clockB.Set(clockStart.Add(7 * time.Hour))
		require.NoError(t, nodeB.addEntry(ctx))
		clockA.Set(clockStart.Add(4 * time.Hour))
		require.NoError(t, nodeA.addEntry(ctx))
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

// GenerateTimestampsSeqPairs returns one pair every duration from start up to and including end. If startSeq is
// non-zero, the sequences start at startSeq and increase by one per pair, otherwise every pair has sequence 0.
// It returns nil if duration is not positive, or if start is after end.
func TestTimestampsManagerStop(t *testing.T) {
	ctx := t.Context()
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	tm := NewTimestampsManager(TimestampsMgrOpts{
		metadataStore:     testBucket.GetMetadataStore(),
		metaKeys:          base.NewMetadataKeys(t.Name()),
		purgeIntervalFunc: func() time.Duration { return NumTimestampsEntries * time.Hour },
		getSequenceFunc:   func(context.Context) (uint64, error) { return 0, nil },
		dbName:            t.Name(),
	})
	require.NoError(t, tm.Start(ctx))
	require.Len(t, tm.backgroundTasks, 1)
	doneChan := tm.backgroundTasks[0].doneChan

	select {
	case <-doneChan:
		t.Fatal("background task finished before Stop")
	default:
	}

	tm.Stop(ctx)

	select {
	case <-doneChan:
	default:
		t.Fatal("Stop returned before the background task terminated")
	}

	// a second Stop must be a no-op rather than panic on a double close
	require.NotPanics(t, func() { tm.Stop(ctx) })
}

// TestTimestampsManagerStartCorruptDoc makes sure that Start resets a timestamps document that can't be parsed to an
// empty array, replaces the in-memory copy with it, and starts the background task, so that recording heals by itself.
func TestTimestampsManagerStartCorruptDoc(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	// a JSON document whose timestamps field is not an array of entries
	require.NoError(t, metadataStore.Set(ctx, metaKeys.TimestampsSeqKey(), 0, nil, map[string]any{"timestamps": "not an array"}))
	_, err := metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &TimestampsSeqArray{})
	require.Error(t, err, "the seeded document must not parse")

	tm := NewTimestampsManager(TimestampsMgrOpts{
		metadataStore:     metadataStore,
		metaKeys:          metaKeys,
		purgeIntervalFunc: func() time.Duration { return NumTimestampsEntries * time.Hour },
		getSequenceFunc:   func(context.Context) (uint64, error) { return 0, nil },
		dbName:            t.Name(),
	})
	require.NoError(t, tm.Start(ctx))
	defer tm.Stop(ctx)
	assert.Len(t, tm.backgroundTasks, 1, "the background task must start after the document is reset")

	var tsDoc TimestampsSeqArray
	_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
	require.NoError(t, err, "the document must be reset to a parseable array")
	assert.Empty(t, tsDoc.Timestamps)
	assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load(), "the in-memory copy must match the reset document")

	// an empty array is always due, so the next call records an entry again
	require.NoError(t, tm.addEntry(ctx))
	_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
	require.NoError(t, err)
	assert.Len(t, tsDoc.Timestamps, 1)
	assert.Equal(t, tsDoc, *tm.timestampsSeqArray.Load())
}

// TestTimestampsDatabaseRecordsSequence makes sure that a database's timestamps manager records the sequence of the
// documents written to it, in the stored document and in memory.
func TestTimestampsDatabaseRecordsSequence(t *testing.T) {
	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	key := db.MetadataKeys.TimestampsSeqKey()

	for i := range 3 {
		_, doc, err := collection.Put(ctx, fmt.Sprintf("doc%d", i), Body{"val": i})
		require.NoError(t, err)

		// move the existing entries back a day, so that the next entry is due
		var before TimestampsSeqArray
		_, err = db.MetadataStore.Get(ctx, key, &before)
		require.NoError(t, err)
		for j := range before.Timestamps {
			before.Timestamps[j].Timestamp -= int64((24 * time.Hour).Seconds())
		}
		db.TimestampsManager.CreateTSDocForTest(t, ctx, before)

		require.NoError(t, db.TimestampsManager.addEntry(ctx))

		var tsDoc TimestampsSeqArray
		_, err = db.MetadataStore.Get(ctx, key, &tsDoc)
		require.NoError(t, err)
		// Start creates the document without an entry, so there is one entry per document
		require.Len(t, tsDoc.Timestamps, i+1)
		assert.GreaterOrEqual(t, tsDoc.Timestamps[i].Seq, doc.Sequence)
		assert.True(t, slices.IsSortedFunc(tsDoc.Timestamps, func(a, b TimestampsSeqPair) int {
			return cmp.Compare(a.Seq, b.Seq)
		}), "sequences must not decrease: %+v", tsDoc.Timestamps)
		assert.Equal(t, tsDoc, *db.TimestampsManager.timestampsSeqArray.Load())
	}
}

// TestTimestampsQuietNodeKeepsOtherNodesEntries makes sure that a node which has written fewer documents than another
// node records the cluster's sequence, and keeps the other node's entries.
func TestTimestampsQuietNodeKeepsOtherNodesEntries(t *testing.T) {
	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))

	// two database contexts on one bucket share the sequence counter and the timestamps document, like two nodes
	busyDB, busyCtx := SetupTestDBForBucketWithOptions(t, tb, DatabaseContextOptions{})
	defer busyDB.Close(busyCtx)
	quietDB, quietCtx := SetupTestDBForBucketWithOptions(t, tb.NoCloseClone(), DatabaseContextOptions{})
	defer quietDB.Close(quietCtx)
	busyCollection, busyCtx := GetSingleDatabaseCollectionWithUser(busyCtx, t, busyDB)
	quietCollection, quietCtx := GetSingleDatabaseCollectionWithUser(quietCtx, t, quietDB)
	key := busyDB.MetadataKeys.TimestampsSeqKey()

	// once the quiet node has written a document, its last sequence is its own rather than the counter's
	_, _, err := quietCollection.Put(quietCtx, "quietDoc", Body{"val": 0})
	require.NoError(t, err)
	var busyDoc *Document
	for i := range 10 {
		_, busyDoc, err = busyCollection.Put(busyCtx, fmt.Sprintf("busyDoc%d", i), Body{"val": i})
		require.NoError(t, err)
	}

	// makeEntryDue moves the stored entries back a day, so that the next addEntry adds one
	makeEntryDue := func() {
		var tsDoc TimestampsSeqArray
		_, err := busyDB.MetadataStore.Get(busyCtx, key, &tsDoc)
		require.NoError(t, err)
		for i := range tsDoc.Timestamps {
			tsDoc.Timestamps[i].Timestamp -= int64((24 * time.Hour).Seconds())
		}
		busyDB.TimestampsManager.CreateTSDocForTest(t, busyCtx, tsDoc)
	}

	makeEntryDue()
	require.NoError(t, busyDB.TimestampsManager.addEntry(busyCtx))
	busyEntries := busyDB.TimestampsManager.timestampsSeqArray.Load().Timestamps
	busySeq := busyEntries[len(busyEntries)-1].Seq
	require.GreaterOrEqual(t, busySeq, busyDoc.Sequence)

	makeEntryDue()
	require.NoError(t, quietDB.TimestampsManager.addEntry(quietCtx))

	var tsDoc TimestampsSeqArray
	_, err = busyDB.MetadataStore.Get(busyCtx, key, &tsDoc)
	require.NoError(t, err)
	assert.True(t, slices.ContainsFunc(tsDoc.Timestamps, func(pair TimestampsSeqPair) bool { return pair.Seq == busySeq }),
		"the busy node's entry at sequence %d must be kept: %+v", busySeq, tsDoc.Timestamps)
	assert.GreaterOrEqual(t, tsDoc.Timestamps[len(tsDoc.Timestamps)-1].Seq, busySeq, "the quiet node must record the cluster's sequence")
}

// TestTimestampsNonPositiveClientPartitionWindow makes sure that a client partition window of zero or less is replaced
// by DefaultClientPartitionWindow, rather than keeping every entry ever added.
func TestTimestampsNonPositiveClientPartitionWindow(t *testing.T) {
	const day = 24 * time.Hour
	for _, window := range []time.Duration{0, -5 * time.Second} {
		t.Run(window.String(), func(t *testing.T) {
			db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{ClientPartitionWindow: new(window)})
			defer db.Close(ctx)
			require.Equal(t, window, db.ClientPartitionWindow())
			require.Equal(t, base.DefaultClientPartitionWindow, db.TimestampsManager.purgeInterval())

			// one entry a day, starting 10 days before the default window
			now := time.Now()
			pairs := GenerateTimestampsSeqPairs(now.Add(-base.DefaultClientPartitionWindow-10*day), now.Add(-day), day, 0)
			db.TimestampsManager.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: pairs})

			require.NoError(t, db.TimestampsManager.addEntry(ctx))

			var tsDoc TimestampsSeqArray
			_, err := db.MetadataStore.Get(ctx, db.MetadataKeys.TimestampsSeqKey(), &tsDoc)
			require.NoError(t, err)
			// the 10 entries older than the start of the window are dropped, and a new entry is added
			require.Len(t, tsDoc.Timestamps, len(pairs)-10+1)
			assert.Equal(t, pairs[10:], tsDoc.Timestamps[:len(tsDoc.Timestamps)-1])
		})
	}
}

// TestTimestampsEntryGapAfterStart makes sure that when a node starts shortly after another node added an entry, the
// node's first background entry comes within one check interval of that entry becoming due.
func TestTimestampsEntryGapAfterStart(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	const freq = 5 * time.Second
	tm := NewTimestampsManager(TimestampsMgrOpts{
		metadataStore:               testBucket.GetMetadataStore(),
		metaKeys:                    base.NewMetadataKeys(t.Name()),
		purgeIntervalFunc:           func() time.Duration { return NumTimestampsEntries * time.Hour },
		getSequenceFunc:             func(context.Context) (uint64, error) { return 0, nil },
		testUpdateFrequencyOverride: new(freq),
		testCheckIntervalOverride:   new(100 * time.Millisecond),
	})
	// another node added an entry 3s ago, so the entry Start would add is not due yet
	tm.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: []TimestampsSeqPair{{Timestamp: time.Now().Add(-3 * time.Second).Unix()}}})
	require.NoError(t, tm.Start(ctx))
	defer tm.Stop(ctx)
	require.Len(t, tm.timestampsSeqArray.Load().Timestamps, 1)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, tm.timestampsSeqArray.Load().Timestamps, 2)
	}, 3*freq, 100*time.Millisecond)

	entries := tm.timestampsSeqArray.Load().Timestamps
	gap := entries[1].Timestamp - entries[0].Timestamp
	assert.LessOrEqual(t, gap, int64(freq/time.Second)+1, "entries should be about one update frequency (%v) apart", freq)
}

// TestTimestampsEntryGapAfterNodeStops makes sure that a running node whose in-memory copy is out of date, because another
// node added the newest entry and stopped, adds no duplicate entry and adds the next one an update frequency later.
func TestTimestampsEntryGapAfterNodeStops(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	// each node has its own clock, so that node B's checker stays idle until node A has stopped
	newNode := func(clock *testClock) *TimestampsManager {
		return NewTimestampsManager(TimestampsMgrOpts{
			metadataStore:             metadataStore,
			metaKeys:                  metaKeys,
			purgeIntervalFunc:         func() time.Duration { return NumTimestampsEntries * time.Hour },
			getSequenceFunc:           func(context.Context) (uint64, error) { return 0, nil },
			nowFunc:                   clock.Now,
			dbName:                    t.Name(),
			testCheckIntervalOverride: new(10 * time.Millisecond),
		})
	}

	clockB := newTestClock(clockStart)
	nodeB := newNode(clockB)
	freq := nodeB.updateFrequency()
	// the seeded entry is due half an update frequency after clockStart, so node B adds nothing until its clock gets there
	seed := clockStart.Add(-freq / 2)
	dueAt := seed.Add(freq)
	nodeB.CreateTSDocForTest(t, ctx, TimestampsSeqArray{Timestamps: []TimestampsSeqPair{{Timestamp: seed.Unix()}}})
	require.NoError(t, nodeB.Start(ctx))
	defer nodeB.Stop(ctx)

	nodeA := newNode(newTestClock(dueAt))
	require.NoError(t, nodeA.Start(ctx))
	nodeA.Stop(ctx)
	require.Len(t, nodeA.timestampsSeqArray.Load().Timestamps, 2)
	require.Len(t, nodeB.timestampsSeqArray.Load().Timestamps, 1, "node B's in-memory copy must still show only the seeded entry")

	// node B's out-of-date copy shows an entry is due but the stored document doesn't, so node B only loads it
	clockB.Set(dueAt)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, nodeB.timestampsSeqArray.Load().Timestamps, 2)
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, getTimestampsDoc(t, ctx, metadataStore, metaKeys), *nodeB.timestampsSeqArray.Load())

	clockB.Advance(freq)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, nodeB.timestampsSeqArray.Load().Timestamps, 3)
	}, 5*time.Second, 10*time.Millisecond)

	entries := nodeB.timestampsSeqArray.Load().Timestamps
	assert.Equal(t, int64(freq/time.Second), entries[2].Timestamp-entries[1].Timestamp, "node B must add the next entry one update frequency after node A's")
}

// TestTimestampsRecorderSurvivesError makes sure that one failed check does not stop the background task from
// adding entries on later checks.
func TestTimestampsRecorderSurvivesError(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	var failNext atomic.Bool
	var seq atomic.Uint64
	tm := NewTimestampsManager(TimestampsMgrOpts{
		metadataStore:     testBucket.GetMetadataStore(),
		metaKeys:          base.NewMetadataKeys(t.Name()),
		purgeIntervalFunc: func() time.Duration { return NumTimestampsEntries * time.Hour },
		getSequenceFunc: func(context.Context) (uint64, error) {
			if failNext.CompareAndSwap(true, false) {
				return 0, errors.New("transient error")
			}
			return seq.Add(1), nil
		},
		// entries are stored in whole seconds, so below a second every check is due
		testUpdateFrequencyOverride: new(50 * time.Millisecond),
		testCheckIntervalOverride:   new(10 * time.Millisecond),
	})
	require.NoError(t, tm.Start(ctx))
	defer tm.Stop(ctx)
	doneChan := tm.backgroundTasks[0].doneChan

	failNext.Store(true)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, failNext.Load(), "a check must hit the error")
	}, time.Second, 10*time.Millisecond)

	seqAfterError := seq.Load()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Greater(c, seq.Load(), seqAfterError+2, "later checks must keep adding entries")
	}, time.Second, 10*time.Millisecond)

	select {
	case <-doneChan:
		t.Fatal("background task stopped after a single error")
	default:
	}
}

// CreateTSDocForTest writes tsArray to the timestamps document, replacing any existing content.
func (tm *TimestampsManager) CreateTSDocForTest(t *testing.T, ctx context.Context, tsArray TimestampsSeqArray) {
	t.Helper()
	require.NoError(t, tm.metadataStore.Set(ctx, tm.metaKeys.TimestampsSeqKey(), 0, nil, tsArray))
}

// GenerateTimestampsSeqPairs returns one pair every duration from start up to and including end, each with sequence 0.
// It returns nil if duration is not positive.
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

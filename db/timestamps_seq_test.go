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
	"slices"
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
	}

	tm := NewTimestampsManager(mgrOpts)
	require.NotNil(t, tm, "NewTimestampsManager must return a TimestampsManager")

	err := tm.Start()
	require.NoError(t, err)

	tsDoc, _, err := metadataStore.GetRaw(ctx, metaKeys.TimestampsSeqKey())
	assert.NoError(t, err)
	assert.NotNil(t, tsDoc)
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
	}{
		{name: "empty array", entries: nil, seq: 5, expected: nil},
		{name: "no entries above seq", entries: entries, seq: 9, expected: entries},
		{name: "seq equal to an entry keeps it", entries: entries, seq: 5, expected: entries[:2]},
		{name: "some entries above seq", entries: entries, seq: 6, expected: entries[:2]},
		{name: "all entries above seq", entries: entries, seq: 0, expected: []TimestampsSeqPair{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			array := TimestampsSeqArray{Timestamps: slices.Clone(test.entries)}
			array.removeInvalidEntries(ctx, test.seq)
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
	}{
		{
			// a single 12 hour gap: entries older than day -30 are trimmed
			name: "timespan=1month;duration:12hours;duration_change=false",
			entries: func() []TimestampsSeqPair {
				return GenerateTimestampsSeqPairs(now.Add(-31*24*time.Hour), now, time.Hour*12)
			}(),
			purgeTimestamp: now.Add(-30 * 24 * time.Hour).Unix(),
			expected: func() []TimestampsSeqPair {
				return GenerateTimestampsSeqPairs(now.Add(-30*24*time.Hour), now, time.Hour*12)
			}(),
		},
		{
			// the gap changes from 12 hours to 24 hours at day -30: the purge timestamp falls before the change
			name: "timespan=1.5month;duration:12hours/24hours;duration_change=true",
			entries: func() []TimestampsSeqPair {
				tsPair := GenerateTimestampsSeqPairs(now.Add(-60.5*24*time.Hour), now.Add(-30*24*time.Hour), time.Hour*12)
				tsPair2 := GenerateTimestampsSeqPairs(now.Add(-30*24*time.Hour-time.Hour-12), now, time.Hour*24)
				tsPair = append(tsPair, tsPair2...)
				return tsPair
			}(),
			purgeTimestamp: now.Add(-60 * 24 * time.Hour).Unix(),
			expected: func() []TimestampsSeqPair {
				tsPair := GenerateTimestampsSeqPairs(now.Add(-60*24*time.Hour), now.Add(-30*24*time.Hour), time.Hour*12)
				tsPair2 := GenerateTimestampsSeqPairs(now.Add(-30*24*time.Hour-time.Hour-12), now, time.Hour*24)
				tsPair = append(tsPair, tsPair2...)
				return tsPair
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			array := TimestampsSeqArray{Timestamps: slices.Clone(test.entries)}
			array.trim(test.purgeTimestamp)
			assert.Equal(t, len(test.expected), len(array.Timestamps), "expected %d, got %d", len(test.expected), len(array.Timestamps))
			assert.Equal(t, test.expected, array.Timestamps)
			assert.LessOrEqual(t, array.oldestTimestamp(), test.purgeTimestamp, "expected %d, got %d", time.Unix(test.purgeTimestamp, 0).UTC().Format(time.RFC3339), time.Unix(array.oldestTimestamp(), 0).UTC().Format(time.RFC3339))
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
	cleanupFunc := func(key string) {
		_, err := tm.metadataStore.Update(ctx, key, 0, func(current []byte) ([]byte, *uint32, bool, error) {
			val := TimestampsSeqArray{
				Timestamps: []TimestampsSeqPair{},
			}
			valBytes, err := base.JSONMarshal(val)
			require.NoError(t, err)
			return valBytes, nil, false, nil
		})
		require.NoError(t, err)
	}

	t.Run("empty array", func(t *testing.T) {
		defer cleanupFunc(metaKeys.TimestampsSeqKey())
		// an empty array is always due
		err := tm.addEntry()
		require.NoError(t, err)
		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		assert.Len(t, tsDoc.Timestamps, 1)
	})

	t.Run("complete array", func(t *testing.T) {
		defer cleanupFunc(metaKeys.TimestampsSeqKey())
		// Entries are an hour apart and the newest is older than the update frequency, so an entry is due.
		// The two oldest entries are at or before the purge timestamp, so adding one entry trims one.
		now := time.Now()
		pairs := GenerateTimestampsSeqPairs(now.Add(-61*time.Hour), now.Add(-2*time.Hour), time.Hour)
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
		defer cleanupFunc(metaKeys.TimestampsSeqKey())
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
		pairs := GenerateTimestampsSeqPairs(now.Add(-59*day), now.Add(-30*day), 12*time.Hour)
		pairs = append(pairs, GenerateTimestampsSeqPairs(now.Add(-29*day), now.Add(-1*day), day)...)

		tm.CreateTSDocForTest(t, TimestampsSeqArray{Timestamps: pairs})

		require.NoError(t, longPurgeMgr.addEntry())

		var tsDoc TimestampsSeqArray
		_, err = metadataStore.Get(ctx, metaKeys.TimestampsSeqKey(), &tsDoc)
		require.NoError(t, err)
		require.Len(t, tsDoc.Timestamps, len(pairs)+1)
		assert.Equal(t, pairs, tsDoc.Timestamps[:len(pairs)], "existing entries must not be trimmed")
		assert.GreaterOrEqual(t, tsDoc.newestTimestamp(), now.Unix(), "new entry must be added at the end")
	})
}

// GenerateTimestampsSeqPairs returns one pair every duration from start up to and including end, each with sequence 0.
// It returns nil if duration is not positive.
func GenerateTimestampsSeqPairs(start, end time.Time, duration time.Duration) []TimestampsSeqPair {
	var pairs []TimestampsSeqPair
	for ts := start; duration > 0 && !ts.After(end); ts = ts.Add(duration) {
		pairs = append(pairs, TimestampsSeqPair{Seq: 0, Timestamp: ts.Unix()})
	}
	return pairs
}

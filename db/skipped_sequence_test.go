/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPushSingleSkippedSequence:
//   - Populate 10 single skipped sequence items in the slice
//   - Assert that each one is added in the correct order
//   - Assert that timestamp is increasing from the last entry (or equal to)
//   - Add contiguous sequence to slice and assert that it extends the last element with a range
func TestPushSingleSkippedSequence(t *testing.T) {
	skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

	for i := 0; i < 10; i++ {
		skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
	}

	var prevTime int64 = 0
	for j := 0; j < 10; j++ {
		assert.Equal(t, uint64(j*2), skippedSlice.list[j].getStartSeq())
		assert.GreaterOrEqual(t, skippedSlice.list[j].getTimestamp(), prevTime)
		prevTime = skippedSlice.list[j].getTimestamp()
	}
	// add a new single entry that is contiguous with end of the slice which should replace last
	// single entry with a range
	skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(19))
	// grab last entry in list
	index := len(skippedSlice.list) - 1
	entry := skippedSlice.list[index]

	// assert last entry is range entry and start + end sequence on range is as expected
	assert.False(t, entry.singleEntry())
	assert.Equal(t, uint64(18), entry.getStartSeq())
	assert.Equal(t, uint64(19), entry.getLastSeq())
}

// TestPushSkippedSequenceRange:
//   - Create slice of range sequence entries and assert contents of slice are as expected
//   - Attempt to add a new range that is contiguous with entry at end of slice
//   - Assert that the last entry of the slice is expended to include the new range
func TestPushSkippedSequenceRange(t *testing.T) {
	skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

	for i := 0; i < 10; i++ {
		start := i * 10
		skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(uint64(start), uint64(start+5)))
	}

	var prevTime int64 = 0
	for j := 0; j < 10; j++ {
		start := j * 10
		end := start + 5
		assert.Equal(t, uint64(start), skippedSlice.list[j].getStartSeq())
		assert.Equal(t, uint64(end), skippedSlice.list[j].getLastSeq())
		assert.GreaterOrEqual(t, skippedSlice.list[j].getTimestamp(), prevTime)
		prevTime = skippedSlice.list[j].getTimestamp()
	}

	// add a new range entry that is contiguous with end of the slice which should alter range last element in list
	skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(96, 110))
	// grab last entry in list
	index := len(skippedSlice.list) - 1
	entry := skippedSlice.list[index]

	// assert last entry is range entry and start + end sequence on range is as expected
	assert.False(t, entry.singleEntry())
	assert.Equal(t, uint64(90), entry.getStartSeq())
	assert.Equal(t, uint64(110), entry.getLastSeq())

	// add new single entry that is not contiguous with last element on slice
	skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(500))

	// add new range that is contiguous with the single entry on the last element of the slice + garbage timestamp
	// for later assertion
	newTimeStamp := time.Now().Unix() + 10000
	skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(501, 510, newTimeStamp))

	index = len(skippedSlice.list) - 1
	entry = skippedSlice.list[index]
	// assert that last element in list is a range and holds sequences we expect + timestamp
	// is what the new pushed range above holds
	assert.False(t, entry.singleEntry())
	assert.Equal(t, uint64(500), entry.getStartSeq())
	assert.Equal(t, uint64(510), entry.getLastSeq())
	assert.Equal(t, newTimeStamp, entry.getTimestamp())
}

func BenchmarkPushSkippedSequenceEntry(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
	}{
		{name: "single_entries", rangeEntries: false},
		{name: "range_entries", rangeEntries: true},
	}
	for _, bm := range benchmarks {
		skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if !bm.rangeEntries {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				} else {
					skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(uint64(i*10), uint64(i*10)+5))
				}
			}
		})
	}
}

// TestIsSequenceSkipped:
//   - Create a skipped slice
//   - Test each sequence added returns true for Contains
//   - For range entries, assert that each boundary of the range returns true, in addition to a sequence that
//     is in the middle of the range
//   - Assert that Contains returns false for a sequence that doesn't exist in the slice
//   - Then add this sequence and search again for it, asserting Contains returns true now
func TestIsSequenceSkipped(t *testing.T) {
	testCases := []struct {
		name       string
		rangeItems bool
		inputList  []uint64
	}{
		{
			name:      "list_full_single_items",
			inputList: []uint64{2, 6, 100, 200, 500},
		},
		{
			name:       "list_full_range_items",
			inputList:  []uint64{5, 15, 25, 35, 45},
			rangeItems: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input))
				}
				for _, v := range testCase.inputList {
					assert.True(t, skippedSlice.Contains(v))
				}
			} else {
				for _, input := range testCase.inputList {
					skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input, input+5))
				}
				for _, v := range testCase.inputList {
					assert.True(t, skippedSlice.Contains(v))
					assert.True(t, skippedSlice.Contains(v+5))
					assert.True(t, skippedSlice.Contains(v+2))
				}
			}

			// try a currently non-existent sequence
			assert.False(t, skippedSlice.Contains(550))

			// push this sequence and assert Contains returns true after
			skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(550))
			assert.True(t, skippedSlice.Contains(550))

			// push another range much higher, assert Contains works as expected
			skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(60000, 70000))
			assert.True(t, skippedSlice.Contains(60000))
			assert.True(t, skippedSlice.Contains(70000))
			assert.True(t, skippedSlice.Contains(65000))
		})
	}
}

func BenchmarkContainsFunction(b *testing.B) {
	benchmarks := []struct {
		name       string
		inputSlice *SkippedSequenceSlice
	}{
		{name: "single_entries_large_slice", inputSlice: setupBenchmark(true, false, DefaultClipCapacityHeadroom)},
		{name: "single_entries_small_slice", inputSlice: setupBenchmark(false, false, DefaultClipCapacityHeadroom)},
		{name: "range_entries_large_slice", inputSlice: setupBenchmark(true, true, DefaultClipCapacityHeadroom)},
		{name: "range_entries_small_slice", inputSlice: setupBenchmark(false, true, DefaultClipCapacityHeadroom)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bm.inputSlice.Contains(uint64(i * 2))
			}
		})
	}
}

// TestRemoveSeqFromSkipped:
//   - Create skipped sequence slice
//   - Remove a sequence from that slice and assert the resulting slice is as expected
//   - Assert on timestamps being preserved throughout the slice
//   - Attempt to remove a non-existent sequence and assert it returns an error
//   - Test features edge cases where we remove start or end seq on a range. Then another edge case of removing a
//     sequence that is startSeq+1 or lastSeq-1 thus altering existing range and inserting a new single sequence in place
func TestRemoveSeqFromSkipped(t *testing.T) {
	testCases := []struct {
		name        string
		inputList   [][]uint64
		expected    [][]uint64
		remove      uint64
		errorRemove uint64
		rangeItems  bool
	}{
		{
			name:        "list_full_single_items",
			inputList:   [][]uint64{{2}, {6}, {100}, {200}, {500}},
			expected:    [][]uint64{{2, 2}, {6, 6}, {200, 200}, {500, 500}},
			remove:      100,
			errorRemove: 150,
		},
		{
			name:        "list_full_range_items",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 26}, {28, 30}, {35, 40}, {45, 50}},
			remove:      27,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_startSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {26, 30}, {35, 40}, {45, 50}},
			remove:      25,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_endSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 39}, {45, 50}},
			remove:      40,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_startSeq+1",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 35}, {37, 40}, {45, 50}},
			remove:      36,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_endSeq-1",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 38}, {40, 40}, {45, 50}},
			remove:      39,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_with_length_1_range_removal",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {22}, {25, 30}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}},
			remove:      22,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_with_length_2_range_removal_startSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {22, 23}, {26, 27}, {35, 40}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {23, 23}, {26, 27}, {35, 40}},
			remove:      22,
			errorRemove: 500,
			rangeItems:  true,
		},
		{
			name:        "list_with_length_2_range_removal_lastSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {22, 23}, {26, 27}, {35, 40}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {22, 23}, {26, 26}, {35, 40}},
			remove:      27,
			errorRemove: 500,
			rangeItems:  true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
				}
			} else {
				for _, input := range testCase.inputList {
					if len(input) == 1 {
						skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
					} else {
						skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input[0], input[1]))
					}
				}
			}

			err := skippedSlice.removeSeq(testCase.remove)
			require.NoError(t, err)

			for i := 0; i < len(skippedSlice.list); i++ {
				// if we have expected entry of just {{x, x}}, then we expect this entry to be single skipped entry
				if testCase.expected[i][0] == testCase.expected[i][1] {
					assert.True(t, skippedSlice.list[i].singleEntry())
				} else {
					assert.False(t, skippedSlice.list[i].singleEntry())
				}
				assert.Equal(t, testCase.expected[i][0], skippedSlice.list[i].getStartSeq())
				assert.Equal(t, testCase.expected[i][1], skippedSlice.list[i].getLastSeq())
			}

			// assert on timestamps being preserved, all timestamps must be increasing or equal as we iterate through
			// the slice proving after insertion of new elements in middle of slice timestamps are preserved
			var prevTime int64
			for _, v := range skippedSlice.list {
				assert.GreaterOrEqual(t, v.getTimestamp(), prevTime)
				prevTime = v.getTimestamp()
			}

			// attempt remove on non existent sequence
			err = skippedSlice.removeSeq(testCase.errorRemove)
			require.Error(t, err)
		})
	}
}

// TestRemoveSeqFromThreeSequenceRange:
//   - Create slice of ranges with a range of just three in there too
//   - Grab timestamp of the three sequence range entry
//   - Attempt to remove middle sequence from the three sequence range
//   - Assert that there are two single items inserted in the middle of the slice to preserve the order
//   - Assert that timestamp is preserved
//   - Add two more three sequence ranges and remove the start/last seq from those respectively
//   - Assert the resulting slice is as expected
func TestRemoveSeqFromThreeSequenceRange(t *testing.T) {

	skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
	inputList := [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}}
	expected := [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {60, 60}, {62, 62}, {70, 75}}

	for _, v := range inputList {
		skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(v[0], v[1]))
	}
	skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(60, 62))
	skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(70, 75))

	i, found := skippedSlice._findSequence(60)
	assert.True(t, found)

	// grab timestamp from range that is getting split
	timestampAtSequence := skippedSlice.list[i].getTimestamp()

	// remove seq in middle of above range
	err := skippedSlice.removeSeq(61)
	require.NoError(t, err)

	skippedLen := len(skippedSlice.list)
	for i := 0; i < skippedLen; i++ {
		if expected[i][0] == expected[i][1] {
			assert.True(t, skippedSlice.list[i].singleEntry())
		} else {
			assert.False(t, skippedSlice.list[i].singleEntry())
		}
		assert.Equal(t, expected[i][0], skippedSlice.list[i].getStartSeq())
		assert.Equal(t, expected[i][1], skippedSlice.list[i].getLastSeq())
	}

	// assert that items second and third from last are single items now
	assert.True(t, skippedSlice.list[skippedLen-3].singleEntry())
	assert.True(t, skippedSlice.list[skippedLen-2].singleEntry())
	// assert that items second and third from last timestamps are preserved
	assert.Equal(t, timestampAtSequence, skippedSlice.list[skippedLen-3].getTimestamp())
	assert.Equal(t, timestampAtSequence, skippedSlice.list[skippedLen-2].getTimestamp())

	// push two new three seq ranges and remove the start seq from one of those ranges,
	// then last seq from the other range
	expected = [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {60, 60}, {62, 62}, {70, 75}, {81, 82}, {85, 86}}
	skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(80, 82))

	skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(85, 87))

	// remove start seq from range 80-32
	err = skippedSlice.removeSeq(80)
	require.NoError(t, err)
	// remove last seq from range 85-87
	err = skippedSlice.removeSeq(87)
	require.NoError(t, err)

	skippedLen = len(skippedSlice.list)
	for j := 0; j < skippedLen; j++ {
		if expected[j][0] == expected[j][1] {
			assert.True(t, skippedSlice.list[j].singleEntry())
		} else {
			assert.False(t, skippedSlice.list[j].singleEntry())
		}
		assert.Equal(t, expected[j][0], skippedSlice.list[j].getStartSeq())
		assert.Equal(t, expected[j][1], skippedSlice.list[j].getLastSeq())
	}
}

func BenchmarkRemoveSeqFromSkippedList(b *testing.B) {
	benchmarks := []struct {
		name       string
		inputSlice *SkippedSequenceSlice
	}{
		{name: "single_entries_large_slice", inputSlice: setupBenchmark(true, false, DefaultClipCapacityHeadroom)},
		{name: "single_entries_small_slice", inputSlice: setupBenchmark(false, false, DefaultClipCapacityHeadroom)},
		{name: "range_entries_large_slice", inputSlice: setupBenchmark(true, true, DefaultClipCapacityHeadroom)},
		{name: "range_entries_small_slice", inputSlice: setupBenchmark(false, true, DefaultClipCapacityHeadroom)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = bm.inputSlice.removeSeq(uint64(i * 2))
			}
		})
	}
}

// TestInsertItemInSlice:
//   - Create skipped sequence slice
//   - Insert a new value in the slice at index specified to maintain order
//   - Assert the resulting slice is correct
//   - Assert on resulting slice
func TestInsertItemInSlice(t *testing.T) {
	testCases := []struct {
		name       string
		inputList  [][]uint64
		expected   [][]uint64
		insert     uint64
		index      int
		rangeItems bool
	}{
		{
			name:      "single_items",
			inputList: [][]uint64{{2}, {6}, {100}, {200}, {500}},
			expected:  [][]uint64{{2, 2}, {6, 6}, {70, 70}, {100, 100}, {200, 200}, {500, 500}},
			insert:    70,
			index:     2,
		},
		{
			name:       "range_items",
			inputList:  [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {32, 32}, {35, 40}, {45, 50}},
			insert:     32,
			index:      3,
			rangeItems: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
				}
			} else {
				for _, input := range testCase.inputList {
					skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input[0], input[1]))
				}
			}

			// attempt to insert at test case index to keep order
			skippedSlice._insert(testCase.index, NewSingleSkippedSequenceEntry(testCase.insert))

			for i := 0; i < len(skippedSlice.list); i++ {
				if testCase.expected[i][0] == testCase.expected[i][1] {
					assert.True(t, skippedSlice.list[i].singleEntry())
				} else {
					assert.False(t, skippedSlice.list[i].singleEntry())
				}
				// if we expect range at this index, assert on it
				assert.Equal(t, testCase.expected[i][0], skippedSlice.list[i].getStartSeq())
				assert.Equal(t, testCase.expected[i][1], skippedSlice.list[i].getLastSeq())
			}

		})
	}
}

func BenchmarkInsertSkippedItem(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		inputSlice   *SkippedSequenceSlice
	}{
		{name: "single_entries_large_slice", rangeEntries: false, inputSlice: setupBenchmark(true, false, DefaultClipCapacityHeadroom)},
		{name: "single_entries_small_slice", rangeEntries: false, inputSlice: setupBenchmark(false, false, DefaultClipCapacityHeadroom)},
		{name: "range_entries_large_slice", rangeEntries: true, inputSlice: setupBenchmark(true, true, DefaultClipCapacityHeadroom)},
		{name: "range_entries_small_slice", rangeEntries: true, inputSlice: setupBenchmark(false, true, DefaultClipCapacityHeadroom)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			sequenceNum := 40000
			for i := 0; i < b.N; i++ {
				if !bm.rangeEntries {
					bm.inputSlice._insert(i, NewSingleSkippedSequenceEntry(uint64(sequenceNum*i)))
				} else {
					bm.inputSlice._insert(i, NewSkippedSequenceRangeEntry(uint64(sequenceNum*i), uint64(sequenceNum*i)+5))
				}
			}
		})
	}
}

// TestCompactSkippedList:
//   - Create skipped sequence slice with old timestamp
//   - Push new entry with future timestamp
//   - Run compact and assert that each item is compacted apart from the last added item
//   - Assert on number sequences removed
func TestCompactSkippedList(t *testing.T) {
	testCases := []struct {
		name       string
		inputList  [][]uint64
		expected   [][]uint64
		numRemoved int64
		rangeItems bool
	}{
		{
			name:       "single_items",
			inputList:  [][]uint64{{2}, {6}, {100}, {200}, {500}},
			expected:   [][]uint64{{600, 600}},
			numRemoved: 5,
		},
		{
			name:       "range_items",
			inputList:  [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {55, 60}},
			expected:   [][]uint64{{600, 605}},
			numRemoved: 36,
			rangeItems: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			inputTime := time.Now().Unix() - 1000

			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					// add single entries with old timestamps for compaction
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(input[0], inputTime))
				}
			} else {
				for _, input := range testCase.inputList {
					// add range entries with old timestamps for compaction
					skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(input[0], input[1], inputTime))
				}
			}

			// alter timestamp so we don't compact this entry
			var entry *SkippedSequenceListEntry
			futureTime := time.Now().Unix() + 10000
			if !testCase.rangeItems {
				entry = NewSingleSkippedSequenceEntryAt(600, futureTime)
			} else {
				entry = NewSkippedSequenceRangeEntryAt(600, 605, futureTime)
			}
			skippedSlice.PushSkippedSequenceEntry(entry)

			numRemoved := skippedSlice.SkippedSequenceCompact(base.TestCtx(t), 1)

			require.Len(t, skippedSlice.list, 1)
			assert.Equal(t, testCase.expected[0][0], skippedSlice.list[0].getStartSeq())
			assert.Equal(t, testCase.expected[0][1], skippedSlice.list[0].getLastSeq())

			// assert on num sequences removed
			assert.Equal(t, testCase.numRemoved, numRemoved)
		})
	}
}

// TestCompactSkippedListClipHandling:
//   - Create new skipped sequence slice with old timestamps + clip headroom defined at 100
//   - Push new future entry on the slice
//   - Run compact and assert that the capacity of the slice is correct after _clip is run
func TestCompactSkippedListClipHandling(t *testing.T) {
	testCases := []struct {
		name        string
		expectedCap int
		rangeItems  bool
	}{
		{
			name:        "single_items",
			expectedCap: 100,
		},
		{
			name:        "range_items",
			expectedCap: 100,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// define clip headroom at 100 for test
			skippedSlice := NewSkippedSequenceSlice(100)
			inputTime := time.Now().Unix() - 1000

			if !testCase.rangeItems {
				for i := 0; i < 100; i++ {
					// add single entries with old timestamps for compaction
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(uint64(i*2), inputTime))
				}
			} else {
				for i := 0; i < 100; i++ {
					// add range entries with old timestamps for compaction
					skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(uint64(i*2), uint64(i*2)+5, inputTime))
				}
			}
			// alter timestamp so we don't compact this entry
			var entry *SkippedSequenceListEntry
			futureTime := time.Now().Unix() + 10000
			if !testCase.rangeItems {
				entry = NewSingleSkippedSequenceEntryAt(600, futureTime)
			} else {
				entry = NewSkippedSequenceRangeEntryAt(600, 605, futureTime)
			}
			skippedSlice.PushSkippedSequenceEntry(entry)

			skippedSlice.SkippedSequenceCompact(base.TestCtx(t), 1)

			assert.Equal(t, testCase.expectedCap, cap(skippedSlice.list))
		})
	}
}

func BenchmarkCompactSkippedList(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		inputSlice   *SkippedSequenceSlice
	}{
		{name: "single_entries_large_slice", rangeEntries: false, inputSlice: setupBenchmarkToCompact(true, false, 100)},
		{name: "single_entries_small_slice", rangeEntries: false, inputSlice: setupBenchmarkToCompact(false, false, 100)},
		{name: "range_entries_large_slice", rangeEntries: true, inputSlice: setupBenchmarkToCompact(true, true, 100)},
		{name: "range_entries_small_slice", rangeEntries: true, inputSlice: setupBenchmarkToCompact(false, true, 100)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bm.inputSlice.SkippedSequenceCompact(base.TestCtx(b), 100)
			}
		})
	}
}

// TestGetOldestSkippedSequence:
//   - Create slice of no items, single items and range items
//   - Assert that getOldest() correctly returns the oldest sequence in the slice, 0 if the slice is empty
func TestGetOldestSkippedSequence(t *testing.T) {
	testCases := []struct {
		name       string
		inputList  [][]uint64
		expected   uint64
		empty      bool
		rangeItems bool
	}{
		{
			name:      "single_items",
			inputList: [][]uint64{{2}, {6}, {100}, {200}, {500}},
			expected:  2,
		},
		{
			name:     "empty_slice",
			empty:    true,
			expected: 0,
		},
		{
			name:      "range_items",
			inputList: [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:  5,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			if !testCase.empty {
				if !testCase.rangeItems {
					for _, v := range testCase.inputList {
						skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(v[0]))
					}
				} else {
					for _, v := range testCase.inputList {
						skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(v[0], v[1]))
					}
				}
			}
			assert.Equal(t, testCase.expected, skippedSlice.getOldest())
		})
	}
}

// setupBenchmark sets up a skipped sequence slice for benchmark tests
func setupBenchmark(largeSlice bool, rangeEntries bool, clipHeadroom int) *SkippedSequenceSlice {
	skippedSlice := NewSkippedSequenceSlice(clipHeadroom)
	if largeSlice {
		for i := 0; i < 10000; i++ {
			if rangeEntries {
				skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(uint64(i*2), uint64(i*2)+5))
			} else {
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
			}
		}
	} else {
		for i := 0; i < 100; i++ {
			if rangeEntries {
				skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(uint64(i*2), uint64(i*2)+5))
			} else {
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
			}
		}
	}
	return skippedSlice
}

// setupBenchmarkToCompact sets up a skipped sequence slice for compaction based benchmark tests
func setupBenchmarkToCompact(largeSlice bool, rangeEntries bool, clipHeadroom int) *SkippedSequenceSlice {
	skippedSlice := NewSkippedSequenceSlice(clipHeadroom)
	inputTime := time.Now().Unix() - 1000
	if largeSlice {
		for i := 0; i < 10000; i++ {
			if rangeEntries {
				// add range entries with old timestamps for compaction
				skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(uint64(i*2), uint64(i*2)+5, inputTime))
			} else {
				// add single entries with old timestamps for compaction
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(uint64(i*2), inputTime))
			}
		}
	} else {
		for i := 0; i < 100; i++ {
			if rangeEntries {
				// add range entries with old timestamps for compaction
				skippedSlice.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(uint64(i*2), uint64(i*2)+5, inputTime))
			} else {
				// add single entries with old timestamps for compaction
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(uint64(i*2), inputTime))
			}
		}
	}
	// have one entry to not be compacted
	skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(60000))
	return skippedSlice
}

// singleEntry returns true if the entry is a single sequence entry, false if not. Used for testing purposes
func (s *SkippedSequenceListEntry) singleEntry() bool {
	// if no star and end seq equal then it's a single entry
	return s.start == s.end
}

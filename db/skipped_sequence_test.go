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
	skiplist "github.com/couchbasedeps/fast-skiplist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const multiplier = 8 // used to multiply sequence numbers in benchmarks

// TestPushSingleSkippedSequence:
//   - Populate 10 single skipped sequence items in the list
//   - Assert that each one is added in the correct order
//   - Assert that timestamp is increasing from the last entry (or equal to)
func TestPushSingleSkippedSequence(t *testing.T) {
	skippedList := NewSkippedSequenceSkiplist()

	for i := range 10 {
		err := skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
		require.NoError(t, err)
	}

	var prevTime int64 = 0
	j := 0
	for c := skippedList.list.Front(); c != nil; c = c.Next() {
		assert.Equal(t, uint64(j*2), c.Key().Start)
		assert.Equal(t, uint64(j*2), c.Key().End)
		assert.GreaterOrEqual(t, c.Key().Timestamp, prevTime)
		prevTime = c.Key().Timestamp
		j++
	}

	// add a new single entry that is contiguous with end of the slice which should replace last
	// single entry with a range
	err := skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(19))
	require.NoError(t, err)

	// grab last entry in list
	elem := skippedList.list.GetLastElement()
	// assert last entry is range entry and start + end sequence on range is as expected
	assert.False(t, isSingleEntry(elem.Key()))
	assert.Equal(t, uint64(18), elem.Key().Start)
	assert.Equal(t, uint64(19), elem.Key().End)
}

// TestPushSkippedSequenceRange:
//   - Create list of range sequence entries and assert contents of list are as expected
//   - Attempt to add a new range that is contiguous with entry at end of list and assert that item is as expected
func TestPushSkippedSequenceRange(t *testing.T) {
	skippedList := NewSkippedSequenceSkiplist()

	for i := range 10 {
		start := i * 10
		err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(uint64(start), uint64(start+5)))
		require.NoError(t, err)
	}

	var prevTime int64 = 0
	j := 0
	for c := skippedList.list.Front(); c != nil; c = c.Next() {
		start := j * 10
		end := start + 5
		assert.Equal(t, uint64(start), c.Key().Start)
		assert.Equal(t, uint64(end), c.Key().End)
		assert.GreaterOrEqual(t, c.Key().Timestamp, prevTime)
		prevTime = c.Key().Timestamp
		j++
	}

	// add a new range entry that is contiguous with end of the slice which should alter range last element in list
	err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(96, 110))
	require.NoError(t, err)
	// grab last entry in list
	elem := skippedList.list.GetLastElement()
	// assert last entry is range entry and start + end sequence on range is as expected
	assert.False(t, isSingleEntry(elem.Key()))
	assert.Equal(t, uint64(90), elem.Key().Start)
	assert.Equal(t, uint64(110), elem.Key().End)

	// add new single entry that is not contiguous with last element on list
	err = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(500))
	require.NoError(t, err)

	// add new range that is contiguous with the single entry on the last element of the list + garbage timestamp
	// for later assertion
	newTimeStamp := time.Now().Unix() + 10000
	err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(501, 510, newTimeStamp))
	require.NoError(t, err)

	elem = skippedList.list.GetLastElement()

	// assert that last element in list is a range and holds sequences we expect + timestamp
	// is what the new pushed range above holds
	assert.Equal(t, uint64(500), elem.Key().Start)
	assert.Equal(t, uint64(510), elem.Key().End)
	assert.Equal(t, newTimeStamp, elem.Key().Timestamp)
}

func BenchmarkPushSkippedSequenceEntryLargeList(b *testing.B) {
	skippedList := setupBenchmark(true, false)
	i := uint64(240_000_005)
	if testing.Short() {
		i = uint64(240)
	}
	for b.Loop() {
		_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(i*2, 0))
		i++
	}
}

func BenchmarkPushSkippedSequenceEntryLargeListContiguous(b *testing.B) {
	skippedList := setupBenchmark(true, false)
	i := uint64(240000005)
	if testing.Short() {
		i = uint64(240)
	}
	for b.Loop() {
		_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(i, 0))
		i++
	}
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
		b.Run(bm.name, func(b *testing.B) {
			skippedList := NewSkippedSequenceSkiplist()
			i := uint64(0)
			for b.Loop() {
				if !bm.rangeEntries {
					_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(i*2, 0))
				} else {
					_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(i*10, (i*10)+5, 0))
				}
				i++
			}
		})
	}
}

// TestIsSequenceSkipped:
//   - Create a skipped list
//   - Test each sequence added returns true for Contains
//   - For range entries, assert that each boundary of the range returns true, in addition to a sequence that
//     is in the middle of the range
//   - Assert that Contains returns false for a sequence that doesn't exist in the list
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
			skippedList := NewSkippedSequenceSkiplist()

			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					err := skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input))
					require.NoError(t, err)
				}
				for _, v := range testCase.inputList {
					assert.True(t, skippedList.Contains(v))
				}
			} else {
				for _, input := range testCase.inputList {
					err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input, input+5))
					require.NoError(t, err)
				}
				for _, v := range testCase.inputList {
					assert.True(t, skippedList.Contains(v))
					assert.True(t, skippedList.Contains(v+5))
					assert.True(t, skippedList.Contains(v+2))
				}
			}

			// try a currently non-existent sequence
			assert.False(t, skippedList.Contains(550))

			// push this sequence and assert Contains returns true after
			err := skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(550))
			require.NoError(t, err)
			assert.True(t, skippedList.Contains(550))

			// push another range much higher, assert Contains works as expected
			err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(60000, 70000))
			require.NoError(t, err)
			assert.True(t, skippedList.Contains(60000))
			assert.True(t, skippedList.Contains(70000))
			assert.True(t, skippedList.Contains(65000))
		})
	}
}

func BenchmarkContainsFunction(b *testing.B) {
	benchmarks := []struct {
		name      string
		inputList *SkippedSequenceSkiplist
	}{
		{name: "single_entries_large_slice", inputList: setupBenchmark(true, false)},
		{name: "single_entries_small_slice", inputList: setupBenchmark(false, false)},
		{name: "range_entries_large_slice", inputList: setupBenchmark(true, true)},
		{name: "range_entries_small_slice", inputList: setupBenchmark(false, true)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			loopNum := uint64(1)
			for b.Loop() {
				bm.inputList.Contains(loopNum * multiplier)
				loopNum++
			}
		})
	}
}

// TestRemoveSeqFromSkipped:
//   - Create skipped sequence list
//   - Remove a sequence from that list and assert the resulting ist is as expected
//   - Assert on timestamps being preserved throughout the list
//   - Attempt to remove a non-existent sequence and assert it returns an error
//   - Test features edge cases where we remove start or end seq on a range. Then another edge case of removing a
//     sequence that is startSeq+1 or lastSeq-1 thus altering existing range and inserting a new single sequence in place
func TestRemoveSeqFromSkipped(t *testing.T) {
	testCases := []struct {
		name        string
		inputList   [][]uint64
		expected    [][]uint64
		remove      skiplist.SkippedSequenceEntry
		errorRemove skiplist.SkippedSequenceEntry
		rangeItems  bool
	}{
		{
			name:        "list_full_single_items",
			inputList:   [][]uint64{{2}, {6}, {100}, {200}, {500}},
			expected:    [][]uint64{{2, 2}, {6, 6}, {200, 200}, {500, 500}},
			remove:      NewSingleSkippedSequenceEntry(100),
			errorRemove: NewSingleSkippedSequenceEntry(150),
		},
		{
			name:        "list_full_range_items",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 26}, {28, 30}, {35, 40}, {45, 50}},
			remove:      NewSingleSkippedSequenceEntry(27),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_startSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {26, 30}, {35, 40}, {45, 50}},
			remove:      NewSingleSkippedSequenceEntry(25),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_endSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 39}, {45, 50}},
			remove:      NewSingleSkippedSequenceEntry(40),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_startSeq+1",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 35}, {37, 40}, {45, 50}},
			remove:      NewSingleSkippedSequenceEntry(36),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_full_range_items_remove_endSeq-1",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 38}, {40, 40}, {45, 50}},
			remove:      NewSingleSkippedSequenceEntry(39),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_with_length_1_range_removal",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {22}, {25, 30}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {25, 30}},
			remove:      NewSingleSkippedSequenceEntry(22),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_with_length_2_range_removal_startSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {22, 23}, {26, 27}, {35, 40}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {23, 23}, {26, 27}, {35, 40}},
			remove:      NewSingleSkippedSequenceEntry(22),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
		{
			name:        "list_with_length_2_range_removal_lastSeq",
			inputList:   [][]uint64{{5, 10}, {15, 20}, {22, 23}, {26, 27}, {35, 40}},
			expected:    [][]uint64{{5, 10}, {15, 20}, {22, 23}, {26, 26}, {35, 40}},
			remove:      NewSingleSkippedSequenceEntry(27),
			errorRemove: NewSingleSkippedSequenceEntry(500),
			rangeItems:  true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedList := NewSkippedSequenceSkiplist()
			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					err := skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
					require.NoError(t, err)
				}
			} else {
				var err error
				for _, input := range testCase.inputList {
					if len(input) == 1 {
						err = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
						require.NoError(t, err)
					} else {
						err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input[0], input[1]))
						require.NoError(t, err)
					}
				}
			}

			elem, _, err := skippedList.list.Remove(testCase.remove)
			require.NoError(t, err)
			require.NotNil(t, elem)

			i := 0
			var prevTime int64
			for c := skippedList.list.Front(); c != nil; c = c.Next() {
				// if we have expected entry of just {{x, x}}, then we expect this entry to be single skipped entry
				if testCase.expected[i][0] == testCase.expected[i][1] {
					assert.True(t, isSingleEntry(c.Key()))
				} else {
					assert.False(t, isSingleEntry(c.Key()))
				}
				assert.Equal(t, testCase.expected[i][0], c.Key().Start)
				assert.Equal(t, testCase.expected[i][1], c.Key().End)
				assert.GreaterOrEqual(t, c.Key().Timestamp, prevTime)
				prevTime = c.Key().Timestamp
				i++
			}

			// attempt remove on non existent sequence
			elem, _, err = skippedList.list.Remove(testCase.errorRemove)
			require.Error(t, err)
			require.Nil(t, elem)
		})
	}
}

// TestRemoveSeqFromThreeSequenceRange:
//   - Create list of ranges with a range of just three in there too
//   - Grab timestamp of the three sequence range entry
//   - Attempt to remove middle sequence from the three sequence range
//   - Assert that there are two single items inserted in the middle of the list to preserve the order
//   - Assert that timestamp is preserved
//   - Add two more three sequence ranges and remove the start/last seq from those respectively
//   - Assert the resulting list is as expected
func TestRemoveSeqFromThreeSequenceRange(t *testing.T) {

	skippedList := NewSkippedSequenceSkiplist()
	inputList := [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}}
	expected := [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {60, 60}, {62, 62}, {70, 75}}

	for _, v := range inputList {
		err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(v[0], v[1]))
		require.NoError(t, err)
	}
	err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(60, 62))
	require.NoError(t, err)
	err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(70, 75))
	require.NoError(t, err)

	elem := skippedList.list.Get(NewSingleSkippedSequenceEntry(60))
	assert.NotNil(t, elem)

	// grab timestamp from range that is getting split
	timestampAtSequence := elem.Key().Timestamp

	// remove seq in middle of above range
	elem, _, err = skippedList.list.Remove(NewSingleSkippedSequenceEntry(61))
	require.NoError(t, err)
	require.NotNil(t, elem)

	i := 0
	for c := skippedList.list.Front(); c != nil; c = c.Next() {
		if expected[i][0] == expected[i][1] {
			assert.True(t, isSingleEntry(c.Key()))
		} else {
			assert.False(t, isSingleEntry(c.Key()))
		}
		assert.Equal(t, expected[i][0], c.Key().Start)
		assert.Equal(t, expected[i][1], c.Key().End)
		i++
	}

	// assert that items second and third from last timestamps are preserved
	elem = skippedList.list.Get(NewSingleSkippedSequenceEntry(60))
	assert.Equal(t, timestampAtSequence, elem.Key().Timestamp)
	elem = skippedList.list.Get(NewSingleSkippedSequenceEntry(62))
	assert.Equal(t, timestampAtSequence, elem.Key().Timestamp)

	// push two new three seq ranges and remove the start seq from one of those ranges,
	// then last seq from the other range
	expected = [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {60, 60}, {62, 62}, {70, 75}, {81, 82}, {85, 86}}
	err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(80, 82))
	require.NoError(t, err)

	err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(85, 87))
	require.NoError(t, err)

	// remove start seq from range 80-32
	elem, _, err = skippedList.list.Remove(NewSingleSkippedSequenceEntry(80))
	require.NoError(t, err)
	require.NotNil(t, elem)
	// remove last seq from range 85-87
	elem, _, err = skippedList.list.Remove(NewSingleSkippedSequenceEntry(87))
	require.NoError(t, err)
	require.NotNil(t, elem)

	j := 0
	for c := skippedList.list.Front(); c != nil; c = c.Next() {
		if expected[j][0] == expected[j][1] {
			assert.True(t, isSingleEntry(c.Key()))
		} else {
			assert.False(t, isSingleEntry(c.Key()))
		}
		assert.Equal(t, expected[j][0], c.Key().Start)
		assert.Equal(t, expected[j][1], c.Key().End)
		j++
	}
}

func BenchmarkRemoveSeqFromSkippedList(b *testing.B) {
	benchmarks := []struct {
		name      string
		inputList *SkippedSequenceSkiplist
	}{
		{name: "single_entries_large_slice", inputList: setupBenchmark(true, false)},
		{name: "single_entries_small_slice", inputList: setupBenchmark(false, false)},
		{name: "range_entries_large_slice", inputList: setupBenchmark(true, true)},
		{name: "range_entries_small_slice", inputList: setupBenchmark(false, true)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			i := uint64(1)
			for b.Loop() {
				_, _, _ = bm.inputList.list.Remove(NewSingleSkippedSequenceEntryAt(multiplier*i, 0))
				i++
			}
		})
	}
}

func BenchmarkRemoveSeqRangeFromSkippedList(b *testing.B) {
	skipedList := setupBenchmark(true, true)
	i := uint64(1)
	for b.Loop() {
		_, _, _ = skipedList.list.Remove(NewSkippedSequenceRangeEntryAt(i*multiplier, (i*multiplier)+1, 0))
		i++
	}
}

// TestInsertItemInSlice:
//   - Create skipped sequence list
//   - Insert a new value in the list at index specified to maintain order
//   - Assert the resulting list is correct
//   - Assert on resulting list
func TestInsertItemInSkipped(t *testing.T) {
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
			skippedList := NewSkippedSequenceSkiplist()
			var err error

			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					err = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
					require.NoError(t, err)
				}
			} else {
				for _, input := range testCase.inputList {
					err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input[0], input[1]))
					require.NoError(t, err)
				}
			}

			// attempt to insert at test case index to keep order
			elem, err := skippedList.list.Set(NewSingleSkippedSequenceEntry(testCase.insert))
			require.NoError(t, err)
			require.NotNil(t, elem)

			i := 0
			for c := skippedList.list.Front(); c != nil; c = c.Next() {
				if testCase.expected[i][0] == testCase.expected[i][1] {
					assert.True(t, isSingleEntry(c.Key()))
				} else {
					assert.False(t, isSingleEntry(c.Key()))
				}
				// if we expect range at this index, assert on it
				assert.Equal(t, testCase.expected[i][0], c.Key().Start)
				assert.Equal(t, testCase.expected[i][1], c.Key().End)
				i++
			}
		})
	}
}

func BenchmarkInsertSkippedItem(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		inputSlice   *SkippedSequenceSkiplist
	}{
		{name: "single_entries_large_slice", rangeEntries: false, inputSlice: setupBenchmarkForInsert(true, false)},
		{name: "single_entries_small_slice", rangeEntries: false, inputSlice: setupBenchmarkForInsert(false, false)},
		{name: "range_entries_large_slice", rangeEntries: true, inputSlice: setupBenchmarkForInsert(true, true)},
		{name: "range_entries_small_slice", rangeEntries: true, inputSlice: setupBenchmarkForInsert(false, true)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			i := uint64(2)
			for b.Loop() {
				if !bm.rangeEntries {
					_, _ = bm.inputSlice.list.Set(NewSingleSkippedSequenceEntryAt(multiplier*i, 0))
				} else {
					_, _ = bm.inputSlice.list.Set(NewSkippedSequenceRangeEntryAt(multiplier*i, (multiplier*i)+2, 0))
				}
				i++
			}
		})
	}
}

// TestCompactSkippedList:
//   - Create skipped sequence list with old timestamp
//   - Push new entry with future timestamp
//   - Run compact and assert that each item is compacted apart from the last added item
//   - Assert on number sequences removed
func TestCompactSkippedList(t *testing.T) {

	testCases := []struct {
		name       string
		inputList  [][]uint64
		expected   [][]uint64
		numRemoved int64
		numLeft    int64
		rangeItems bool
	}{
		{
			name:       "single_items",
			inputList:  [][]uint64{{2}, {6}, {100}, {200}, {500}},
			expected:   [][]uint64{{600, 600}},
			numLeft:    1,
			numRemoved: 5,
		},
		{
			name:       "range_items",
			inputList:  [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {55, 60}},
			expected:   [][]uint64{{600, 605}},
			numRemoved: 36,
			numLeft:    6,
			rangeItems: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedList := NewSkippedSequenceSkiplist()
			inputTime := time.Now().Unix() - 1000
			var err error

			if !testCase.rangeItems {
				for _, input := range testCase.inputList {
					// add single entries with old timestamps for compaction
					err = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(input[0], inputTime))
					require.NoError(t, err)
				}
			} else {
				for _, input := range testCase.inputList {
					// add range entries with old timestamps for compaction
					err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(input[0], input[1], inputTime))
					require.NoError(t, err)
				}
			}

			// alter timestamp so we don't compact this entry
			var entry skiplist.SkippedSequenceEntry
			futureTime := time.Now().Unix() + 10000
			if !testCase.rangeItems {
				entry = NewSingleSkippedSequenceEntryAt(600, futureTime)
			} else {
				entry = NewSkippedSequenceRangeEntryAt(600, 605, futureTime)
			}
			err = skippedList.PushSkippedSequenceEntry(entry)
			require.NoError(t, err)

			numRemoved, numSeqsLeft := skippedList.SkippedSequenceCompact(base.TestCtx(t), 1)

			require.Equal(t, skippedList.list.GetLength(), 1)
			assert.Equal(t, testCase.numLeft, numSeqsLeft)
			assert.Equal(t, testCase.expected[0][0], skippedList.list.Front().Key().Start)
			assert.Equal(t, testCase.expected[0][1], skippedList.list.Front().Key().End)

			// assert on num sequences removed
			assert.Equal(t, testCase.numRemoved, numRemoved)
		})
	}
}

func BenchmarkCompactSkippedList(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		inputList    *SkippedSequenceSkiplist
	}{
		{name: "single_entries_large_slice", rangeEntries: false, inputList: setupBenchmarkToCompact(true, false)},
		{name: "single_entries_small_slice", rangeEntries: false, inputList: setupBenchmarkToCompact(false, false)},
		{name: "range_entries_large_slice", rangeEntries: true, inputList: setupBenchmarkToCompact(true, true)},
		{name: "range_entries_small_slice", rangeEntries: true, inputList: setupBenchmarkToCompact(false, true)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := base.TestCtx(b)
			for b.Loop() {
				bm.inputList.SkippedSequenceCompact(ctx, 100)
			}
		})
	}
}

// TestGetOldestSkippedSequence:
//   - Create list of no items, single items and range items
//   - Assert that getOldest() correctly returns the oldest sequence in the list, 0 if the list is empty
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
			name:     "empty_list",
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
			skippedList := NewSkippedSequenceSkiplist()
			var err error
			if !testCase.empty {
				if !testCase.rangeItems {
					for _, v := range testCase.inputList {
						err = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(v[0]))
						require.NoError(t, err)
					}
				} else {
					for _, v := range testCase.inputList {
						err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(v[0], v[1]))
						require.NoError(t, err)
					}
				}
			}
			assert.Equal(t, testCase.expected, skippedList.getOldest())
		})
	}
}

// TestRemoveSequenceRange:
//   - Setup skipped list
//   - Remove the range specified in the test case
//   - Assert on error returned from _removeSeqRange depending on whether testcase is a error case
//   - Assert on expected resulting skipped list
//   - Assert on number of skipped sequences in the list after removal
func TestRemoveSequenceRange(t *testing.T) {
	testCases := []struct {
		name                        string
		inputList                   [][]uint64
		expected                    [][]uint64
		expectedNumSequencesInSlice int64
		rangeToRemove               []uint64
		errorCase                   bool
	}{
		{
			name:                        "x_startSeq_on_range",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {29, 30}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{25, 28},
			expectedNumSequencesInSlice: 26,
		},
		{
			name:                        "y_endSeq_on_range",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 26}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{27, 30},
			expectedNumSequencesInSlice: 26,
		},
		{
			name:                        "x_y_startSeq_endSeq_on_range",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {45, 50}},
			rangeToRemove:               []uint64{35, 40},
			expectedNumSequencesInSlice: 24,
		},
		{
			name:                        "x_y_in_middle_of_range",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 36}, {39, 40}, {45, 50}},
			rangeToRemove:               []uint64{37, 38},
			expectedNumSequencesInSlice: 28,
		},
		{
			name:                        "x+1_from_startSeq_y_-1_from_startSeq",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 15}, {20, 20}, {25, 30}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{16, 19},
			expectedNumSequencesInSlice: 26,
		},
		{
			name:                        "single_sequence_removed",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}, {55}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{55, 55},
			expectedNumSequencesInSlice: 30,
		},
		{
			name:                        "single_sequence_removed_from_range",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 26}, {28, 30}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{27, 27},
			expectedNumSequencesInSlice: 29,
		},
		{
			name:                        "x_y_across_multiple_elements",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 25}, {38, 40}, {45, 50}},
			rangeToRemove:               []uint64{26, 37},
			expectedNumSequencesInSlice: 22,
		},
		{
			name:                        "x_does_not_exist_in_list",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {36, 40}, {45, 50}},
			rangeToRemove:               []uint64{32, 35},
			expectedNumSequencesInSlice: 29,
		},
		{
			name:                        "y_does_not_exist_in_list",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{25, 31},
			expectedNumSequencesInSlice: 24,
		},
		{
			name:                        "x_and_y_do_not_exist_in_list",
			inputList:                   [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			expected:                    [][]uint64{{5, 10}, {15, 20}, {25, 30}, {35, 40}, {45, 50}},
			rangeToRemove:               []uint64{31, 33},
			expectedNumSequencesInSlice: 30,
			errorCase:                   true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedList := NewSkippedSequenceSkiplist()
			var err error
			for _, input := range testCase.inputList {
				if len(input) == 1 {
					err = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input[0]))
					require.NoError(t, err)
				} else {
					err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(input[0], input[1]))
					require.NoError(t, err)
				}
			}

			elem, _, err := skippedList.list.Remove(NewSkippedSequenceRangeEntry(testCase.rangeToRemove[0], testCase.rangeToRemove[1]))
			if testCase.errorCase {
				require.Error(t, err)
				require.Nil(t, elem)
			} else {
				require.NoError(t, err)
				require.NotNil(t, elem)
			}

			i := 0
			for c := skippedList.list.Front(); c != nil; c = c.Next() {
				if testCase.expected[i][0] == testCase.expected[i][1] {
					assert.True(t, isSingleEntry(c.Key()))
				} else {
					assert.False(t, isSingleEntry(c.Key()))
				}
				// if we expect range at this index, assert on it
				assert.Equal(t, testCase.expected[i][0], c.Key().Start)
				assert.Equal(t, testCase.expected[i][1], c.Key().End)
				i++
			}
			// assert on current count of skipped sequences
			assert.Equal(t, testCase.expectedNumSequencesInSlice, skippedList.list.NumSequencesInList)
		})
	}

}

func TestProcessUnusedSequenceRangeAtSkipped(t *testing.T) {
	testCases := []struct {
		name        string
		expected    [][]uint64
		removalSeqs []uint64
		errorCase   bool
	}{
		{
			name:        "test_case_1",
			expected:    [][]uint64{{10, 30}, {71, 100}, {120, 150}},
			removalSeqs: []uint64{50, 70},
			errorCase:   true,
		},
		{
			name:        "test_case_2",
			expected:    [][]uint64{{71, 100}, {120, 150}},
			removalSeqs: []uint64{5, 70},
		},
		{
			name:        "test_case_3",
			expected:    [][]uint64{{10, 30}, {60, 64}, {120, 150}},
			removalSeqs: []uint64{65, 110},
		},
		{
			name:        "test_case_4",
			expected:    [][]uint64{{10, 30}, {120, 150}},
			removalSeqs: []uint64{50, 115},
		},
		{
			name:        "test_case_5",
			expected:    [][]uint64{{10, 30}, {126, 150}},
			removalSeqs: []uint64{50, 125},
		},
		{
			name:        "test_case_6",
			expected:    [][]uint64{{10, 30}, {60, 100}, {120, 129}},
			removalSeqs: []uint64{130, 200},
		},
		{
			name:        "test_case_7",
			expected:    [][]uint64{{10, 14}, {120, 150}},
			removalSeqs: []uint64{15, 100},
		},
		{
			name:        "test_case_8",
			expected:    [][]uint64{{10, 30}, {60, 64}},
			removalSeqs: []uint64{65, 200},
		},
		{
			name:        "test_case_9",
			expected:    [][]uint64{{10, 30}, {60, 100}, {120, 150}},
			removalSeqs: []uint64{160, 200},
		},
		{
			name:        "test_case_10",
			expected:    [][]uint64{{10, 30}, {60, 100}, {120, 150}},
			removalSeqs: []uint64{2, 5},
		},
		{
			name:        "test_case_11",
			expected:    [][]uint64{{10, 30}, {60, 100}, {120, 150}},
			removalSeqs: []uint64{35, 50},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedList := NewSkippedSequenceSkiplist()
			ctx := base.TestCtx(t)

			// fill list with some ranges
			err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(10, 30))
			require.NoError(t, err)
			err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(60, 100))
			require.NoError(t, err)
			err = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(120, 150))
			require.NoError(t, err)

			// run processUnusedSequenceRangeAtSkipped and assert that sequences are removed as expected
			skippedList.processUnusedSequenceRangeAtSkipped(ctx, testCase.removalSeqs[0], testCase.removalSeqs[1])

			i := 0
			for c := skippedList.list.Front(); c != nil; c = c.Next() {
				assert.Equal(t, testCase.expected[i][0], c.Key().Start)
				assert.Equal(t, testCase.expected[i][1], c.Key().End)
				i++
			}
		})
	}
}

// setupBenchmark sets up a skipped sequence list for benchmark tests
func setupBenchmark(largeSlice bool, rangeEntries bool) *SkippedSequenceSkiplist {
	skippedList := NewSkippedSequenceSkiplist()
	_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(1, 0))
	if largeSlice {
		size := uint64(30_000_000)
		if testing.Short() {
			size = 300
		}
		for i := uint64(1); i < size; i++ {
			if rangeEntries {
				_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(i*multiplier, (i*multiplier)+2))
			} else {
				_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(i * multiplier))
			}
		}
	} else {
		for i := uint64(1); i < 1000; i++ {
			if rangeEntries {
				_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(i*multiplier, (i*multiplier)+2))
			} else {
				_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(i * multiplier))
			}
		}
	}
	return skippedList
}

func setupBenchmarkForInsert(largeSlice bool, rangeEntries bool) *SkippedSequenceSkiplist {
	skippedList := NewSkippedSequenceSkiplist()
	// add low entry and very high entries then benchmark will insert in middle of these values high and low values
	_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(1, 0))
	_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(200000000, 0))
	if largeSlice {
		size := 30_000_000
		if testing.Short() {
			size = 300
		}
		for range size {
			startSeq := skippedList.list.GetLastElement().Key().End
			if rangeEntries {
				_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(startSeq+2, startSeq+5))
			} else {
				_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(startSeq + 2))
			}
		}
	} else {
		for i := 1000; i < 2000; i++ {
			startSeq := skippedList.list.GetLastElement().Key().End
			if rangeEntries {
				_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(startSeq+2, startSeq+5))
			} else {
				_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(startSeq + 2))
			}
		}
	}
	return skippedList
}

// setupBenchmarkToCompact sets up a skipped sequence list for compaction based benchmark tests
func setupBenchmarkToCompact(largeSlice bool, rangeEntries bool) *SkippedSequenceSkiplist {
	skippedList := NewSkippedSequenceSkiplist()
	inputTime := time.Now().Unix() - 1000
	_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(1, inputTime))
	if largeSlice {
		size := 30_000_000
		if testing.Short() {
			size = 100_000
		}
		for range size {
			startSeq := skippedList.list.GetLastElement().Key().End
			if rangeEntries {
				// add range entries with old timestamps for compaction
				_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(startSeq+2, startSeq+5, inputTime))
			} else {
				// add single entries with old timestamps for compaction
				_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(startSeq+2, inputTime))
			}
		}
	} else {
		for range 1000 {
			startSeq := skippedList.list.GetLastElement().Key().End
			if rangeEntries {
				// add range entries with old timestamps for compaction
				_ = skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntryAt(startSeq+2, startSeq+5, inputTime))
			} else {
				// add single entries with old timestamps for compaction
				_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(startSeq+2, inputTime))
			}
		}
	}
	// have one entry to not be compacted
	_ = skippedList.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(100000000))
	return skippedList
}

func TestContainsAcrossRange(t *testing.T) {
	skippedList := NewSkippedSequenceSkiplist()

	err := skippedList.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(10, 15))
	require.NoError(t, err)

	// run contains on each seq in above range and assert it returns true
	for i := 10; i < 16; i++ {
		assert.True(t, skippedList.Contains(uint64(i)))
	}

}

// isSingleEntry returns true if the entry is a single sequence entry, false if not. Used for testing purposes
func isSingleEntry(entry skiplist.SkippedSequenceEntry) bool {
	// if no star and end seq equal then it's a single entry
	return entry.Start == entry.End
}

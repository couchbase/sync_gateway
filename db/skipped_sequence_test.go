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
//   - Pending CBG-3853 add contiguous sequence to slice and assert that it replaces the last element with a range entry
func TestPushSingleSkippedSequence(t *testing.T) {
	skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

	for i := 0; i < 10; i++ {
		skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
	}

	var prevTime int64 = 0
	for j := 0; j < 10; j++ {
		assert.Equal(t, uint64(j*2), skippedSlice.list[j].getLastSeq())
		assert.GreaterOrEqual(t, skippedSlice.list[j].getTimestamp(), prevTime)
		prevTime = skippedSlice.list[j].getTimestamp()
	}
	// Pending CBG-3853, add a new single entry that is contiguous with end of the slice which should repalce last
	// entry with a range

}

func BenchmarkPushSingleSkippedSequence(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
	}{
		{name: "single_entries", rangeEntries: false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			for i := 0; i < b.N; i++ {
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
			}
		})
	}
}

// TestIsSequenceSkipped:
//   - Create a skipped slice
//   - Test each sequence added returns true for Contains
//   - Assert that Contains returns false for a sequence that doesn't exist in the slice
//
// Will add more test cases to this as development continues (for sequence ranges + mixed single and ranges) pending CBG-3853
func TestIsSequenceSkipped(t *testing.T) {
	testCases := []struct {
		name      string
		inputList []uint64
	}{
		{
			name:      "list_full_single_items",
			inputList: []uint64{2, 6, 100, 200, 500},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

			for _, input := range testCase.inputList {
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input))
			}
			for _, v := range testCase.inputList {
				assert.True(t, skippedSlice.Contains(v))
			}

			// try a non existent sequence
			assert.False(t, skippedSlice.Contains(150))

		})
	}
}

func BenchmarkIsSkippedFunction(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		largeSlice   bool
	}{
		{name: "single_entries_large_slice", rangeEntries: false, largeSlice: true},
		{name: "single_entries_small_slice", rangeEntries: false, largeSlice: false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			if bm.largeSlice {
				for i := 0; i < 10000; i++ {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				}
			} else {
				for i := 0; i < 100; i++ {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				}
			}
			for i := 0; i < b.N; i++ {
				skippedSlice.Contains(uint64(i * 2))
			}
		})
	}
}

// TestRemoveSeqFromSkipped:
//   - Create skipped sequence slice
//   - Remove a sequence from that slice and assert the resulting slice is as expected
//   - Attempt to remove a non-existent sequence and assert it returns an error
//
// Will add more test cases to this as development continues (for sequence ranges + mixed single and ranges) pending CBG-3853
func TestRemoveSeqFromSkipped(t *testing.T) {
	testCases := []struct {
		name         string
		inputList    []uint64
		expectedList []uint64
		remove       uint64
		errorRemove  uint64
	}{
		{
			name:         "list_full_single_items",
			inputList:    []uint64{2, 6, 100, 200, 500},
			expectedList: []uint64{2, 6, 200, 500},
			remove:       100,
			errorRemove:  150,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			for _, input := range testCase.inputList {
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input))
			}

			err := skippedSlice.removeSeq(testCase.remove)
			require.NoError(t, err)

			for i := 0; i < len(skippedSlice.list); i++ {
				assert.Equal(t, testCase.expectedList[i], skippedSlice.list[i].getLastSeq())
			}

			err = skippedSlice.removeSeq(testCase.errorRemove)
			require.Error(t, err)
		})
	}
}

func BenchmarkRemoveSeqFromSkippedList(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		largeSlice   bool
	}{
		{name: "single_entries_large_slice", rangeEntries: false, largeSlice: true},
		{name: "single_entries_small_slice", rangeEntries: false, largeSlice: false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			if bm.largeSlice {
				for i := 0; i < 10000; i++ {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				}
			} else {
				for i := 0; i < 100; i++ {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				}
			}
			for i := 0; i < b.N; i++ {
				_ = skippedSlice.removeSeq(uint64(i * 2))
			}
		})
	}
}

// TestInsertItemInSlice:
//   - Create skipped sequence slice
//   - Insert a new value in the slice at index 2 to maintain order
//   - Assert the resulting slice is correct
//
// Will add more test cases to this as development continues (for sequence ranges + mixed single and ranges) pending CBG-3853
func TestInsertItemInSlice(t *testing.T) {
	testCases := []struct {
		name         string
		inputList    []uint64
		expectedList []uint64
		insert       uint64
		index        int
	}{
		{
			name:         "single_items",
			inputList:    []uint64{2, 6, 100, 200, 500},
			expectedList: []uint64{2, 6, 70, 100, 200, 500},
			insert:       70,
			index:        2,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

			for _, input := range testCase.inputList {
				skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(input))
			}

			// attempt to insert at test case index to keep order
			skippedSlice.insert(testCase.index, NewSingleSkippedSequenceEntry(testCase.insert))

			for i := 0; i < len(skippedSlice.list); i++ {
				assert.Equal(t, testCase.expectedList[i], skippedSlice.list[i].getLastSeq())
			}
		})
	}
}

func BenchmarkInsertSkippedItem(b *testing.B) {
	benchmarks := []struct {
		name         string
		rangeEntries bool
		largeSlice   bool
	}{
		{name: "single_entries_large_slice", rangeEntries: false, largeSlice: true},
		{name: "single_entries_small_slice", rangeEntries: false, largeSlice: false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			if bm.largeSlice {
				for i := 0; i < 10000; i++ {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				}
			} else {
				for i := 0; i < 100; i++ {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(uint64(i * 2)))
				}
			}
			sequenceNum := 40000
			for i := 0; i < b.N; i++ {
				skippedSlice.insert(i, NewSingleSkippedSequenceEntry(uint64(sequenceNum*i)))
			}
		})
	}
}

// TestCompactSkippedList:
//   - Create skipped sequence slice with old timestamp
//   - Push new entry with future timestamp
//   - Run compact and assert that each item is compacted apart from the last added item
//
// Will add more test cases to this as development continues (for sequence ranges + mixed single and ranges) pending CBG-3853
func TestCompactSkippedList(t *testing.T) {
	testCases := []struct {
		name         string
		inputList    []uint64
		expectedList []uint64
		numRemoved   int
	}{
		{
			name:         "single_items",
			inputList:    []uint64{2, 6, 100, 200, 500},
			expectedList: []uint64{600},
			numRemoved:   5,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)

			for _, input := range testCase.inputList {
				skippedSlice.PushSkippedSequenceEntry(testSingleSkippedEntryOldTimestamp(input))
			}
			// alter timestamp so we don't compact this entry
			entry := NewSingleSkippedSequenceEntry(600)
			entry.timestamp = entry.timestamp + 10000
			skippedSlice.PushSkippedSequenceEntry(entry)

			numRemoved := skippedSlice.SkippedSequenceCompact(base.TestCtx(t), 1)

			require.Len(t, skippedSlice.list, 1)
			assert.Equal(t, uint64(600), skippedSlice.list[0].getLastSeq())
			assert.Equal(t, testCase.numRemoved, numRemoved)
		})
	}
}

// TestCompactSkippedListClipHandling:
//   - Create new skipped sequence slice with old timestamps + clip headroom defined at 100
//   - Push new future entry on the slice
//   - Run compact and assert that the capacity of the slice is correct after clip is run
//
// Will add more test cases to this as development continues (for sequence ranges + mixed single and ranges) pending CBG-3853
func TestCompactSkippedListClipHandling(t *testing.T) {
	testCases := []struct {
		name        string
		expectedCap int
	}{
		{
			name:        "single_items",
			expectedCap: 100,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// define clip headroom at 100 for test
			skippedSlice := NewSkippedSequenceSlice(100)

			for i := 0; i < 100; i++ {
				skippedSlice.PushSkippedSequenceEntry(testSingleSkippedEntryOldTimestamp(uint64(i * 2)))
			}
			// alter timestamp so we don't compact this entry
			entry := NewSingleSkippedSequenceEntry(600)
			entry.timestamp = entry.timestamp + 10000
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
		largeSlice   bool
	}{
		{name: "single_entries_large_slice", rangeEntries: false, largeSlice: true},
		{name: "single_entries_small_slice", rangeEntries: false, largeSlice: false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// define clip headroom at 100 for test
			skippedSlice := NewSkippedSequenceSlice(100)
			if bm.largeSlice {
				for i := 0; i < 10000; i++ {
					skippedSlice.PushSkippedSequenceEntry(testSingleSkippedEntryOldTimestamp(uint64(i * 2)))
				}
			} else {
				for i := 0; i < 100; i++ {
					skippedSlice.PushSkippedSequenceEntry(testSingleSkippedEntryOldTimestamp(uint64(i * 2)))
				}
			}
			// have one entry to not be compacted
			skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(60000))
			for i := 0; i < b.N; i++ {
				skippedSlice.SkippedSequenceCompact(base.TestCtx(b), 100)
			}
		})
	}
}

// testSingleSkippedEntryOldTimestamp is creating a new single skipped sequence entry with an old timestamp
func testSingleSkippedEntryOldTimestamp(seq uint64) *SingleSkippedSequence {
	now := time.Now().Unix()
	assignedTimestamp := now - 1000
	return &SingleSkippedSequence{
		seq:       seq,
		timestamp: assignedTimestamp,
	}
}

func TestGetOldestSkippedSequence(t *testing.T) {
	testCases := []struct {
		name      string
		inputList []uint64
		expected  uint64
		empty     bool
	}{
		{
			name:      "single_items",
			inputList: []uint64{2, 6, 100, 200, 500},
			expected:  2,
		},
		{
			name:     "empty_slice",
			empty:    true,
			expected: 0,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			skippedSlice := NewSkippedSequenceSlice(DefaultClipCapacityHeadroom)
			if !testCase.empty {
				for _, v := range testCase.inputList {
					skippedSlice.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntry(v))
				}
			}
			assert.Equal(t, testCase.expected, skippedSlice.getOldest())
		})
	}
}

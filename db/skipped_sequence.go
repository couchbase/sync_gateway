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
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	DefaultClipCapacityHeadroom = 1000
)

// SkippedSequenceSlice stores the set of skipped sequences as an ordered slice of single skipped sequences
// or skipped sequence ranges
type SkippedSequenceSlice struct {
	list                          []*SkippedSequenceListEntry
	ClipCapacityHeadroom          int
	NumCurrentSkippedSequences    int64
	NumCumulativeSkippedSequences int64
	lock                          sync.RWMutex
}

// SkippedSequenceListEntry contains start + end sequence for a range of skipped sequences +
// a timestamp at which the entry was created in unix format. If an entry is a singular skipped sequence the start and
// end sequence will be equal
type SkippedSequenceListEntry struct {
	start     uint64 // start sequence of a range
	end       uint64 // end sequence of the range (0 if a singular skipped sequence)
	timestamp int64  // timestamp this entry was created in unix format
}

func NewSkippedSequenceSlice(clipHeadroom int) *SkippedSequenceSlice {
	return &SkippedSequenceSlice{
		list:                 []*SkippedSequenceListEntry{},
		ClipCapacityHeadroom: clipHeadroom,
	}
}

// NewSkippedSequenceRangeEntry returns a SkippedSequenceListEntry with the specified sequence range and the current
// timestamp in unix time
func NewSkippedSequenceRangeEntry(start, end uint64) *SkippedSequenceListEntry {
	return &SkippedSequenceListEntry{
		start:     start,
		end:       end,
		timestamp: time.Now().Unix(),
	}
}

// NewSkippedSequenceRangeEntryAt returns a SkippedSequenceListEntry with the specified sequences and the supplied
// timestamp in unix time
func NewSkippedSequenceRangeEntryAt(start, end uint64, timeStamp int64) *SkippedSequenceListEntry {
	return &SkippedSequenceListEntry{
		start:     start,
		end:       end,
		timestamp: timeStamp,
	}
}

// NewSingleSkippedSequenceEntry returns a SkippedSequenceListEntry with start and end seq defined as equal
// with the current timestamp in unix time
func NewSingleSkippedSequenceEntry(seq uint64) *SkippedSequenceListEntry {
	return &SkippedSequenceListEntry{
		start:     seq,
		end:       seq,
		timestamp: time.Now().Unix(),
	}
}

// NewSingleSkippedSequenceEntryAt returns a SkippedSequenceListEntry with start and end seq defined as equal
// and the supplied timestamp in unix time
func NewSingleSkippedSequenceEntryAt(seq uint64, timeStamp int64) *SkippedSequenceListEntry {
	return &SkippedSequenceListEntry{
		start:     seq,
		end:       seq,
		timestamp: timeStamp,
	}
}

// getTimestamp returns the timestamp of the entry
func (s *SkippedSequenceListEntry) getTimestamp() int64 {
	return s.timestamp
}

// getStartSeq gets the start sequence in the range on the entry, for single items the sequence (startSeq) will be returned
func (s *SkippedSequenceListEntry) getStartSeq() uint64 {
	return s.start
}

// getLastSeq gets the last sequence in the range on the entry, for single items the sequence 0 will be returned
func (s *SkippedSequenceListEntry) getLastSeq() uint64 {
	return s.end
}

// setLastSeq sets the last sequence in the range on the entry
func (s *SkippedSequenceListEntry) setLastSeq(seq uint64) {
	s.end = seq
}

// setStartSeq sets the last sequence in the range on the entry
func (s *SkippedSequenceListEntry) setStartSeq(seq uint64) {
	s.start = seq
}

// getNumSequencesInEntry returns the number of sequences an entry in skipped slice holds,
// for single entries it will return just 1
func (s *SkippedSequenceListEntry) getNumSequencesInEntry() int64 {
	if s.end == s.start {
		return 1
	}
	numSequences := (s.end - s.start) + 1
	return int64(numSequences)
}

// extendRange will set the range last seq to the incoming contiguous entry's last seq
// + set the timestamp.
func (s *SkippedSequenceListEntry) extendRange(lastSeq uint64, timeStamp int64) {
	s.timestamp = timeStamp
	s.setLastSeq(lastSeq)
}

// Contains returns true if a given sequence exists in the skipped sequence slice
func (s *SkippedSequenceSlice) Contains(x uint64) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, found := s._findSequence(x)
	return found
}

// SkippedSequenceCompact will compact the entries with timestamp old enough. It will also clip
// the capacity of the slice to length + 100 if the current capacity is 2.5x the length
func (s *SkippedSequenceSlice) SkippedSequenceCompact(ctx context.Context, maxWait int64) (numSequencesCompacted int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	timeNow := time.Now().Unix()
	indexToDelete := -1
	for _, v := range s.list {
		timeStamp := v.getTimestamp()
		if (timeNow - timeStamp) >= maxWait {
			indexToDelete++
			// update count of sequences being compacted from the slice
			numSequencesCompacted = numSequencesCompacted + v.getNumSequencesInEntry()
		} else {
			// exit early to avoid iterating through whole slice
			break
		}
	}
	// if we have incremented the index counter, we need to the index range from the slice
	if indexToDelete > -1 {
		s.list = slices.Delete(s.list, 0, indexToDelete+1)
	}
	// resize slice to reclaim memory if we need to
	s._clip(ctx)
	return numSequencesCompacted
}

// _clip will clip the capacity of the slice to the current length plus the configured headroom in ClipCapacityHeadroom
// if the current capacity of the slice in 2.5x the length
func (s *SkippedSequenceSlice) _clip(ctx context.Context) {
	// threshold is 2.5x the current length of the slice
	threshold := 2.5 * float64(len(s.list))

	if cap(s.list) > int(threshold) {
		// check if we can safely clip without an out of bound errors
		if (len(s.list) + s.ClipCapacityHeadroom) < cap(s.list) {
			base.DebugfCtx(ctx, base.KeyCache, "clipping skipped list capacity")
			s.list = s.list[:len(s.list):s.ClipCapacityHeadroom]
		}
	}
}

// _findSequence will use binary search to search the elements in the slice for a given sequence
func (s *SkippedSequenceSlice) _findSequence(x uint64) (int, bool) {
	i, found := slices.BinarySearchFunc(s.list, x, binarySearchFunc)
	return i, found
}

// binarySearchFunc contains the custom search function for searching the skipped sequence slice for a particular sequence
func binarySearchFunc(a *SkippedSequenceListEntry, seq uint64) int {
	if a.getStartSeq() > seq {
		return 1
	}

	if a.getLastSeq() >= seq {
		return 0
	}
	return -1
}

// removeSeq will remove a given sequence from the slice if it exists
func (s *SkippedSequenceSlice) removeSeq(x uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	index, found := s._findSequence(x)
	if !found {
		return fmt.Errorf("sequence %d not found in the skipped list", x)
	}

	// take the element at the index and handle cases required to removal of a sequence
	rangeElem := s.list[index]
	numSequences := rangeElem.getNumSequencesInEntry()

	if numSequences == 1 {
		// if not a range, we can just remove the single entry from the slice
		s.list = slices.Delete(s.list, index, index+1)
		return nil
	}

	// range entry handling
	// if x == startSeq set startSeq+1, if x == lastSeq set lastSeq-1
	if rangeElem.getStartSeq() == x {
		rangeElem.setStartSeq(x + 1)
		return nil
	}
	if rangeElem.getLastSeq() == x {
		rangeElem.setLastSeq(x - 1)
		return nil
	}

	if numSequences == 3 {
		// if we get here then x is in middle of the 3 sequence range. x being == startSeq or
		// lastSeq is handled above
		// add new single entry at elem + 1 then modify range
		newElem := NewSingleSkippedSequenceEntryAt(rangeElem.getLastSeq(), rangeElem.getTimestamp())
		s._insert(index+1, newElem)
		// make rangeElem a single entry
		rangeElem.setLastSeq(rangeElem.getStartSeq())
		return nil
	}

	// if we get here we can assume that startSeq < x < lastSeq
	// split index range
	newElem := NewSkippedSequenceRangeEntryAt(x+1, rangeElem.getLastSeq(), rangeElem.getTimestamp())
	s._insert(index+1, newElem)
	rangeElem.setLastSeq(x - 1)

	return nil
}

// _insert will insert element in middle of slice maintaining order of rest of slice
func (s *SkippedSequenceSlice) _insert(index int, entry *SkippedSequenceListEntry) {
	s.list = append(s.list, nil)
	copy(s.list[index+1:], s.list[index:])
	s.list[index] = entry
}

// PushSkippedSequenceEntry will append a new skipped sequence entry to the end of the slice, if adding a contiguous
// sequence function will expand the last entry of the slice to reflect this
func (s *SkippedSequenceSlice) PushSkippedSequenceEntry(entry *SkippedSequenceListEntry) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.list) == 0 {
		s.list = append(s.list, entry)
		return
	}
	// get index of last entry + last seq of entry
	index := len(s.list) - 1
	lastEntryLastSeq := s.list[index].getLastSeq()
	if (lastEntryLastSeq + 1) == entry.getStartSeq() {
		// adding contiguous sequence
		// set last seq in the range to the new arriving sequence + alter timestamp to incoming entries timestamp
		s.list[index].extendRange(entry.getLastSeq(), entry.getTimestamp())
	} else {
		s.list = append(s.list, entry)
	}
	s.NumCurrentSkippedSequences += entry.getNumSequencesInEntry()

}

// getOldest returns the start sequence of the first element in the skipped sequence slice
func (s *SkippedSequenceSlice) getOldest() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.list) < 1 {
		return 0
	}
	// grab fist element in slice and take the start seq of that range/single sequence
	return s.list[0].getStartSeq()
}

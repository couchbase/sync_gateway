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

type SkippedSequenceListEntry interface {
	getTimestamp() int64
	getLastSeq() uint64
	setStartSeq(seq uint64)
	setLastSeq(seq uint64)
	getStartSeq() uint64
	isRange() bool
	getNumSequencesInEntry() int64
	extendRange(lastSeq uint64, timeStamp int64)
}

// SkippedSequenceSlice stores the set of skipped sequences as an ordered slice of single skipped sequences
// or skipped sequence ranges
type SkippedSequenceSlice struct {
	list                 []SkippedSequenceListEntry
	ClipCapacityHeadroom int
	lock                 sync.RWMutex
}

var _ SkippedSequenceListEntry = &SingleSkippedSequence{}
var _ SkippedSequenceListEntry = &SkippedSequenceRange{}

// SingleSkippedSequence contains a single skipped sequence value + unix timestamp of the time it's created
type SingleSkippedSequence struct {
	seq       uint64
	timestamp int64
}

// SkippedSequenceRange contains a skipped sequence range + unix timestamp of the time it's created
type SkippedSequenceRange struct {
	start     uint64
	end       uint64
	timestamp int64
}

func NewSkippedSequenceSlice(clipHeadroom int) *SkippedSequenceSlice {
	return &SkippedSequenceSlice{
		list:                 []SkippedSequenceListEntry{},
		ClipCapacityHeadroom: clipHeadroom,
	}
}

// NewSingleSkippedSequenceEntryAt returns a SingleSkippedSequence with the specified sequence and the supplied
// timestamp in unix time
func NewSingleSkippedSequenceEntryAt(sequence uint64, timeStamp int64) *SingleSkippedSequence {
	return &SingleSkippedSequence{
		seq:       sequence,
		timestamp: timeStamp,
	}
}

// NewSingleSkippedSequenceEntry returns a SingleSkippedSequence with the specified sequence and the current
// time in unix time
func NewSingleSkippedSequenceEntry(sequence uint64) *SingleSkippedSequence {
	return &SingleSkippedSequence{
		seq:       sequence,
		timestamp: time.Now().Unix(),
	}
}

// NewSkippedSequenceRangeEntryAt returns a SkippedSequenceRange with the specified sequence range and the supplied
// timestamp in unix time
func NewSkippedSequenceRangeEntryAt(start, end uint64, timeStamp int64) *SkippedSequenceRange {
	return &SkippedSequenceRange{
		start:     start,
		end:       end,
		timestamp: timeStamp,
	}
}

// NewSkippedSequenceRangeEntry returns a SkippedSequenceRange with the specified sequence range and the current
// timestamp in unix time
func NewSkippedSequenceRangeEntry(start, end uint64) *SkippedSequenceRange {
	return &SkippedSequenceRange{
		start:     start,
		end:       end,
		timestamp: time.Now().Unix(),
	}
}

// getTimestamp returns the timestamp of the entry
func (s *SingleSkippedSequence) getTimestamp() int64 {
	return s.timestamp
}

// getLastSeq gets the last sequence in the range on the entry, for single items the sequence will be returned
func (s *SingleSkippedSequence) getLastSeq() uint64 {
	return s.seq
}

// setLastSeq sets the last sequence in the range on the entry, for single items it's a no-op
func (s *SingleSkippedSequence) setLastSeq(seq uint64) {
	// no-op
}

// setStartSeq sets the last sequence in the range on the entry, for single items it's a no-op
func (s *SingleSkippedSequence) setStartSeq(seq uint64) {
	// no-op
}

// getStartSeq gets the start sequence in the range on the entry, for single items the sequence will be returned
func (s *SingleSkippedSequence) getStartSeq() uint64 {
	return s.seq
}

// isRange returns true if the entry is a sequence range entry, false if not
func (s *SingleSkippedSequence) isRange() bool {
	return false
}

// getNumSequencesInEntry returns the number of sequences an entry in skipped slice holds,
// for single entries it will return just 1
func (s *SingleSkippedSequence) getNumSequencesInEntry() int64 {
	return 1
}

// extendRange will set the range last seq to the incoming contiguous entry's last seq
// + set the timestamp.
func (s *SingleSkippedSequence) extendRange(lastSeq uint64, timeStamp int64) {
	// no-op
}

// getTimestamp returns the timestamp of the entry
func (s *SkippedSequenceRange) getTimestamp() int64 {
	return s.timestamp
}

// getStartSeq gets the start sequence in the range on the entry, for single items the sequence will be returned
func (s *SkippedSequenceRange) getStartSeq() uint64 {
	return s.start
}

// getLastSeq gets the last sequence in the range on the entry, for single items the sequence will be returned
func (s *SkippedSequenceRange) getLastSeq() uint64 {
	return s.end
}

// isRange returns true if the entry is a sequence range entry, false if not
func (s *SkippedSequenceRange) isRange() bool {
	return true
}

// setLastSeq sets the last sequence in the range on the entry, for single items it's a no-op
func (s *SkippedSequenceRange) setLastSeq(seq uint64) {
	s.end = seq
}

// setStartSeq sets the last sequence in the range on the entry, for single items it's a no-op
func (s *SkippedSequenceRange) setStartSeq(seq uint64) {
	s.start = seq
}

// getNumSequencesInEntry returns the number of sequences an entry in skipped slice holds,
// for single entries it will return just 1
func (s *SkippedSequenceRange) getNumSequencesInEntry() int64 {
	numSequences := (s.end - s.start) + 1
	return int64(numSequences)
}

// extendRange will set the range last seq to the incoming contiguous entry's last seq
// + set the timestamp.
func (s *SkippedSequenceRange) extendRange(lastSeq uint64, timeStamp int64) {
	s.timestamp = timeStamp
	s.setLastSeq(lastSeq)
}

// Contains returns true if a given sequence exists in the skipped sequence slice
func (s *SkippedSequenceSlice) Contains(x uint64) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, found := s.findSequence(x)
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
	s.clip(ctx)
	return numSequencesCompacted
}

// clip will clip the capacity of the slice to the current length plus the configured headroom in ClipCapacityHeadroom
// if the current capacity of the slice in 2.5x the length
func (s *SkippedSequenceSlice) clip(ctx context.Context) {
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

// findSequence will use binary search to search the elements in the slice for a given sequence
func (s *SkippedSequenceSlice) findSequence(x uint64) (int, bool) {
	i, found := slices.BinarySearchFunc(s.list, x, binarySearchFunc)
	return i, found
}

// binarySearchFunc contains the custom search function for searching the skipped sequence slice for a particular sequence
func binarySearchFunc(a SkippedSequenceListEntry, seq uint64) int {
	if !a.isRange() {
		if a.getStartSeq() > seq {
			return 1
		} else if a.getStartSeq() == seq {
			return 0
		}
		return -1
	} else {
		if a.getStartSeq() > seq {
			return 1
		} else if a.getLastSeq() >= seq {
			return 0
		}
		return -1
	}
}

// removeSeq will remove a given sequence from the slice if it exists
func (s *SkippedSequenceSlice) removeSeq(x uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	index, found := s.findSequence(x)
	if !found {
		return fmt.Errorf("sequence %d not found in the skipped list", x)
	}
	// handle border cases where x is equal to start or end sequence on range (or just sequence for single entries)
	if !s.list[index].isRange() {
		// if not a range, we can just remove the single entry from the slice
		s.list = slices.Delete(s.list, index, index+1)
		return nil
	} else {
		// range entry handling
		rangeElem := s.list[index]
		numSequences := rangeElem.getNumSequencesInEntry()
		if numSequences == 1 {
			// simple delete case
			s.list = slices.Delete(s.list, index, index+1)
			return nil
		}

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
			s.insert(index+1, newElem)
			rangeElem.setLastSeq(x - 1)
			return nil
		}

		// handling for if x is startSeq+1 or lastSeq-1
		if rangeElem.getStartSeq() == x-1 {
			// insert single skipped entry + alter start seq to x + 1
			newElem := NewSingleSkippedSequenceEntryAt(x-1, rangeElem.getTimestamp())
			s.insert(index, newElem)
			rangeElem.setStartSeq(x + 1)
			return nil
		}

		if rangeElem.getLastSeq() == x+1 {
			// insert single skipped entry at index+1 + alter last seq to x - 1
			newElem := NewSingleSkippedSequenceEntryAt(x+1, rangeElem.getTimestamp())
			s.insert(index+1, newElem)
			rangeElem.setLastSeq(x - 1)
			return nil
		}

		// if we get here we can assume that startSeq < x < lastSeq
		// split index range
		newElem := NewSkippedSequenceRangeEntryAt(x+1, rangeElem.getLastSeq(), rangeElem.getTimestamp())
		s.insert(index+1, newElem)
		rangeElem.setLastSeq(x - 1)
	}
	return nil
}

// insert will insert element in middle of slice maintaining order of rest of slice
func (s *SkippedSequenceSlice) insert(index int, entry SkippedSequenceListEntry) {
	s.list = append(s.list, nil)
	copy(s.list[index+1:], s.list[index:])
	s.list[index] = entry
}

// PushSkippedSequenceEntry will append a new skipped sequence entry to the end of the slice, if adding a contiguous
// sequence function will expand the last entry of the slice to reflect this
func (s *SkippedSequenceSlice) PushSkippedSequenceEntry(entry SkippedSequenceListEntry) {
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
		if s.list[index].isRange() {
			// set last seq in the range to the new arriving sequence + alter timestamp to incoming entries timestamp
			s.list[index].extendRange(entry.getLastSeq(), entry.getTimestamp())
		} else {
			// take the sequence from the single entry and create a new range entry in its place
			lastEntryStartSeq := s.list[index].getStartSeq()
			if entry.isRange() {
				// avoid extra allocation by altering its start seq and simply replacing last entry with the incoming entry
				entry.setStartSeq(lastEntryStartSeq)
				s.list[index] = entry
			} else {
				endSeq := entry.getLastSeq()
				s.list[index] = NewSkippedSequenceRangeEntry(lastEntryStartSeq, endSeq)
			}
		}
	} else {
		s.list = append(s.list, entry)
	}

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

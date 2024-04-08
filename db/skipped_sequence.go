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
	setLastSeq(seq uint64)
	getStartSeq() uint64
	isRange() bool
	getNumSequencesInEntry() int
}

// SkippedSequenceSlice stores the set of skipped sequences as an ordered slice of single skipped sequences
// or skipped sequence ranges
type SkippedSequenceSlice struct {
	list                 []SkippedSequenceListEntry
	ClipCapacityHeadroom int
	lock                 sync.RWMutex
}

var _ SkippedSequenceListEntry = &SingleSkippedSequence{}

// SingleSkippedSequence contains a single skipped sequence value + unix timestamp of the time it's created
type SingleSkippedSequence struct {
	seq       uint64
	timestamp int64
}

func NewSkippedSequenceSlice(clipHeadroom int) *SkippedSequenceSlice {
	return &SkippedSequenceSlice{
		list:                 []SkippedSequenceListEntry{},
		ClipCapacityHeadroom: clipHeadroom,
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

// getTimestamp returns the timestamp of the entry
func (s *SingleSkippedSequence) getTimestamp() int64 {
	return s.timestamp
}

// getLastSeq gets the last sequence in the range on the entry, for single items the sequence will be returned
func (s *SingleSkippedSequence) getLastSeq() uint64 {
	return s.seq
}

// setLastSeq sets the last sequence in the range on the entry, for single items its a no-op
func (s *SingleSkippedSequence) setLastSeq(seq uint64) {
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

// getNumSequencesInEntry returns the number of sequences a entry in skipped slice holds,
// for single entries it will return just 1
func (s *SingleSkippedSequence) getNumSequencesInEntry() int {
	return 1
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
func (s *SkippedSequenceSlice) SkippedSequenceCompact(ctx context.Context, maxWait int64) (numSequencesCompacted int) {
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

func binarySearchFunc(a SkippedSequenceListEntry, seq uint64) int {
	singleSeq, ok := a.(*SingleSkippedSequence)
	if ok {
		if singleSeq.seq > seq {
			return 1
		}
		if singleSeq.seq == seq {
			return 0
		}
		return -1
	}
	// should never get here as it stands, will have extra handling here pending CBG-3853
	return 1
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
	}
	// more range handling here CBG-3853, temporarily error as we shouldn't get to this point at this time
	return fmt.Errorf("entered range handling code")
}

// insert will insert element in middle of slice maintaining order of rest of slice
func (s *SkippedSequenceSlice) insert(index int, entry SkippedSequenceListEntry) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.list = append(s.list, nil)
	copy(s.list[index+1:], s.list[index:])
	s.list[index] = entry
}

// PushSkippedSequenceEntry will append a new skipped sequence entry to the end of the slice, if adding a contiguous
// sequence function will expand the last entry of the slice to reflect this
func (s *SkippedSequenceSlice) PushSkippedSequenceEntry(entry *SingleSkippedSequence) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.list) == 0 {
		s.list = append(s.list, entry)
		return
	}
	// adding contiguous sequence handling here, pending CBG-3853
	s.list = append(s.list, entry)
}

func (s *SkippedSequenceSlice) getOldest() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.list) < 1 {
		return 0
	}
	// grab fist element in slice and take the start seq of that range/single sequence
	return s.list[0].getStartSeq()
}

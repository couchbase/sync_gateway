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
	"sort"
	"strconv"
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

// String will return a string representation of the SkippedSequenceListEntry
// Formats: Singular: "#<seq>" or ranges: "#<start>-#<end>"
func (s *SkippedSequenceListEntry) String() string {
	seqStr := "#" + strconv.FormatUint(s.start, 10)
	if s.end != 0 && s.end != s.start {
		seqStr += "-#" + strconv.FormatUint(s.end, 10)
	}
	return seqStr
}

// SkippedSequenceStats will hold all stats associated with the skipped sequence slice, used for getStats()
type SkippedSequenceStats struct {
	NumCurrentSkippedSequencesStat    int64
	NumCumulativeSkippedSequencesStat int64
	ListCapacityStat                  int64
	ListLengthStat                    int64
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
			base.WarnfCtx(ctx, "Abandoning previously skipped sequence entry: %v after %s", v, time.Duration(timeNow-timeStamp)*time.Second)
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
	// decrement number of current skipped sequences by the number of sequences compacted
	s.NumCurrentSkippedSequences -= numSequencesCompacted

	return numSequencesCompacted
}

// _clip will clip the capacity of the slice to the current length plus the configured headroom in ClipCapacityHeadroom
// if the current capacity of the slice in 2.5x the length
func (s *SkippedSequenceSlice) _clip(ctx context.Context) {
	// threshold is 2.5x the current length of the slice
	threshold := 2.5 * float64(len(s.list))

	if cap(s.list) > int(threshold) {
		newCap := len(s.list) + s.ClipCapacityHeadroom
		// check if we can safely clip without an out of bound errors
		if newCap < cap(s.list) {
			base.DebugfCtx(ctx, base.KeyCache, "clipping skipped list capacity")
			s.list = s.list[:len(s.list):newCap]
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

// _removeSeqRange will remove sequence between x and y from the skipped sequence slice
func (s *SkippedSequenceSlice) _removeSeqRange(ctx context.Context, startSeq, endSeq uint64) (int64, error) {
	if len(s.list) == 0 {
		return 0, fmt.Errorf("skipped sequence list is empty, unable to remove sequence range %d to %d", startSeq, endSeq)
	}

	startIndex, found := s._findSequence(startSeq)
	// if both sequence x and y aren't in the same element, the range contains sequences that are not present in
	// skipped sequence list. This is due to any contiguous sequences in the list will be in the same element. So
	// if startSeq and endSeq are in different elements there is at least one sequence between these values not in the list
	if !found {
		base.DebugfCtx(ctx, base.KeyCache, "sequence range %d to %d specified has sequences in that are not present in skipped list", startSeq, endSeq)
		return s.NumCurrentSkippedSequences, base.ErrSkippedSequencesMissing
	}
	// put this below a check for !found to avoid out of bound error
	rangeElem := s.list[startIndex]
	if endSeq > rangeElem.getLastSeq() {
		base.DebugfCtx(ctx, base.KeyCache, "sequence range %d to %d specified has sequences in that are not present in skipped list", startSeq, endSeq)
		return s.NumCurrentSkippedSequences, base.ErrSkippedSequencesMissing
	}

	// handle sequence range removal
	// update number of sequences currently in slice stat
	numCurrSkippedSequences := (endSeq - startSeq) + 1
	s.NumCurrentSkippedSequences -= int64(numCurrSkippedSequences)

	// if single sequence element
	if rangeElem.getStartSeq() == startSeq && rangeElem.getLastSeq() == endSeq {
		// simply remove the element
		s.list = slices.Delete(s.list, startIndex, startIndex+1)
		return s.NumCurrentSkippedSequences, nil
	}
	// check if one of x or y is equal to start or end seq
	if rangeElem.getStartSeq() == startSeq {
		rangeElem.setStartSeq(endSeq + 1)
		return s.NumCurrentSkippedSequences, nil
	}
	if rangeElem.getLastSeq() == endSeq {
		rangeElem.setLastSeq(startSeq - 1)
		return s.NumCurrentSkippedSequences, nil
	}
	// assume we are removing from middle of the range
	newElem := NewSkippedSequenceRangeEntryAt(endSeq+1, rangeElem.getLastSeq(), rangeElem.getTimestamp())
	s._insert(startIndex+1, newElem)
	rangeElem.setLastSeq(startSeq - 1)
	return s.NumCurrentSkippedSequences, nil

}

// removeSeq will remove a given sequence from the slice if it exists
func (s *SkippedSequenceSlice) removeSeq(x uint64) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.list) == 0 {
		return 0, fmt.Errorf("skipped sequence list is empty, unable to remove sequence %d", x)
	}

	index, found := s._findSequence(x)
	if !found {
		return s.NumCurrentSkippedSequences, fmt.Errorf("sequence %d not found in the skipped list", x)
	}
	// if found we need to decrement the current num skipped sequences stat
	s.NumCurrentSkippedSequences -= 1

	// take the element at the index and handle cases required to removal of a sequence
	rangeElem := s.list[index]
	numSequences := rangeElem.getNumSequencesInEntry()

	if numSequences == 1 {
		// if not a range, we can just remove the single entry from the slice
		s.list = slices.Delete(s.list, index, index+1)
		return s.NumCurrentSkippedSequences, nil
	}

	// range entry handling
	// if x == startSeq set startSeq+1, if x == lastSeq set lastSeq-1
	if rangeElem.getStartSeq() == x {
		rangeElem.setStartSeq(x + 1)
		return s.NumCurrentSkippedSequences, nil
	}
	if rangeElem.getLastSeq() == x {
		rangeElem.setLastSeq(x - 1)
		return s.NumCurrentSkippedSequences, nil
	}

	if numSequences == 3 {
		// if we get here then x is in middle of the 3 sequence range. x being == startSeq or
		// lastSeq is handled above
		// add new single entry at elem + 1 then modify range
		newElem := NewSingleSkippedSequenceEntryAt(rangeElem.getLastSeq(), rangeElem.getTimestamp())
		s._insert(index+1, newElem)
		// make rangeElem a single entry
		rangeElem.setLastSeq(rangeElem.getStartSeq())
		return s.NumCurrentSkippedSequences, nil
	}

	// if we get here we can assume that startSeq < x < lastSeq
	// split index range
	newElem := NewSkippedSequenceRangeEntryAt(x+1, rangeElem.getLastSeq(), rangeElem.getTimestamp())
	s._insert(index+1, newElem)
	rangeElem.setLastSeq(x - 1)

	return s.NumCurrentSkippedSequences, nil
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

	// update num current skipped sequences count + the cumulative count of skipped sequences
	numSequencesIncoming := entry.getNumSequencesInEntry()
	s.NumCurrentSkippedSequences += numSequencesIncoming
	s.NumCumulativeSkippedSequences += numSequencesIncoming

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

// getStats will return all associated stats with the skipped sequence slice
func (s *SkippedSequenceSlice) getStats() SkippedSequenceStats {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return SkippedSequenceStats{
		NumCumulativeSkippedSequencesStat: s.NumCumulativeSkippedSequences,
		NumCurrentSkippedSequencesStat:    s.NumCurrentSkippedSequences,
		ListCapacityStat:                  int64(cap(s.list)),
		ListLengthStat:                    int64(len(s.list)),
	}
}

// processUnusedSequenceRangeAtSkipped will batch remove unused sequence range form skipped sequences, if duplicate
// sequences are present, we will iterate through skipped list removing the non-duplicate sequences
func (s *SkippedSequenceSlice) processUnusedSequenceRangeAtSkipped(ctx context.Context, fromSequence, toSequence uint64) int64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// batch remove from skipped
	numSkipped, err := s._removeSeqRange(ctx, fromSequence, toSequence)
	if err != nil && err == base.ErrSkippedSequencesMissing {
		base.DebugfCtx(ctx, base.KeyCache, err.Error())
		// we have sequences (possibly duplicate sequences) in the unused range that don't exist in skipped list
		// handle any potential duplicate sequences between the unused range and the skipped list
		numSkipped, err = s._removeSubsetOfRangeFromSkipped(ctx, fromSequence, toSequence)
		if err != nil {
			base.WarnfCtx(ctx, "error processing subset of unused sequences in skipped list: %v", err)
		}
	} else if err != nil {
		// if we get here then the skipped list must be empty
		base.InfofCtx(ctx, base.KeyCache, "error attempting to remove unused sequence range form skipped: %v", err)
	}
	return numSkipped
}

// _removeSubsetOfRangeFromSkipped will be called if we get error removing the unused sequence range indicating we have duplicate sequence in the unused sequence range
// in the unused sequence range. This function will iterate through each skipped list element from the minimum index that has overlap with unused range
// and remove any sequences within the unused sequence range
func (s *SkippedSequenceSlice) _removeSubsetOfRangeFromSkipped(ctx context.Context, startSeq, endSeq uint64) (int64, error) {
	if len(s.list) == 0 {
		base.DebugfCtx(ctx, base.KeyCache, "unused sequence range of #%d to %d is a duplicate sequence range", startSeq, endSeq)
		return 0, nil
	}
	var indexStart *int // the minimum index that has overlap from unused sequence range (thus index to start iterating from)

	// find first element that has common elements
	_, _ = sort.Find(len(s.list), func(i int) int {
		value := s.list[i]
		if value.getStartSeq() > endSeq {
			// range is less than current skipped entry
			return -1
		}
		if startSeq >= value.getStartSeq() && startSeq <= value.getLastSeq() {
			indexStart = base.Ptr(i)
			// return 0 as there can't be any elems lower in skipped list with overlap with the range
			// if the above is true
			return 0
		}
		if value.getStartSeq() > startSeq {
			if endSeq > value.getStartSeq() {
				indexStart = base.Ptr(i)
			}
			return -1
		}
		// range is larger then current element
		return 1
	})

	var numSkipped int64
	var err error
	skippedLen := len(s.list)
	if indexStart != nil {
		// We have found the lowest elem in skipped list with overlap with unused range
		// iterate through from this index removing any duplicate sequences.
		for j := *indexStart; j < skippedLen; j++ {
			skippedElem := s.list[j]
			var removeStartSequence, removeEndSequence uint64
			if endSeq < skippedElem.getStartSeq() {
				// exit loop, range and skipped have no common elements
				break
			}

			if skippedElem.getStartSeq() >= startSeq {
				removeStartSequence = skippedElem.getStartSeq()
			} else if skippedElem.getLastSeq() >= startSeq {
				removeStartSequence = startSeq
			}

			if skippedElem.getLastSeq() <= endSeq {
				removeEndSequence = skippedElem.getLastSeq()
			} else if skippedElem.getLastSeq() >= endSeq {
				removeEndSequence = endSeq
			}
			numSkipped, err = s._removeSeqRange(ctx, removeStartSequence, removeEndSequence)
			if err != nil {
				return numSkipped, err
			}
			// if above operation removes an entire element, list length wil need updating and index
			// needs to be reprocessed
			if len(s.list) == skippedLen-1 {
				j--
				skippedLen--
			}

			startSeq = removeEndSequence + 1
			// after potentially removing an element from the skipped list we need to re-process
			// the same index again on the loop
			if startSeq > endSeq {
				// exit if we have processed all our unused sequence range
				break
			}
		}
	}
	return numSkipped, nil
}

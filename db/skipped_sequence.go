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
	skiplist "github.com/gregns1/fast-skiplist"
)

// SkippedSequenceSkiplist is a skiplist implementation of the skipped sequence list, no mutex needed as the skiplist
// has this covered
type SkippedSequenceSkiplist struct {
	list                          *skiplist.SkipList
	NumCumulativeSkippedSequences int64
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
	NumCurrentSkippedSequencesStat    int64 // current count of skipped sequences
	NumCumulativeSkippedSequencesStat int64 // cumulative count of skipped sequences
	ListLengthStat                    int64 // number of nodes in skiplist
}

func NewSkippedSequenceSkiplist() *SkippedSequenceSkiplist {
	return &SkippedSequenceSkiplist{
		list: skiplist.New(),
	}
}

// NewSkippedSequenceRangeEntry returns a SkippedSequenceListEntry with the specified sequence range and the current
// timestamp in unix time
func NewSkippedSequenceRangeEntry(start, end uint64) skiplist.SkippedSequenceEntry {
	return skiplist.SkippedSequenceEntry{
		Start:     start,
		End:       end,
		Timestamp: time.Now().Unix(),
	}
}

// NewSkippedSequenceRangeEntryAt returns a SkippedSequenceListEntry with the specified sequences and the supplied
// timestamp in unix time
func NewSkippedSequenceRangeEntryAt(start, end uint64, timeStamp int64) skiplist.SkippedSequenceEntry {
	return skiplist.SkippedSequenceEntry{
		Start:     start,
		End:       end,
		Timestamp: timeStamp,
	}
}

// NewSingleSkippedSequenceEntry returns a SkippedSequenceListEntry with start and end seq defined as equal
// with the current timestamp in unix time
func NewSingleSkippedSequenceEntry(seq uint64) skiplist.SkippedSequenceEntry {
	return skiplist.SkippedSequenceEntry{
		Start:     seq,
		End:       seq,
		Timestamp: time.Now().Unix(),
	}
}

// NewSingleSkippedSequenceEntryAt returns a SkippedSequenceListEntry with start and end seq defined as equal
// and the supplied timestamp in unix time
func NewSingleSkippedSequenceEntryAt(seq uint64, timeStamp int64) skiplist.SkippedSequenceEntry {
	return skiplist.SkippedSequenceEntry{
		Start:     seq,
		End:       seq,
		Timestamp: timeStamp,
	}
}

func (s *SkippedSequenceSkiplist) Contains(x uint64) bool {
	elem := s.list.Get(skiplist.SkippedSequenceEntry{Start: x, End: x})
	return elem != nil
}

// SkippedSequenceCompact will compact the entries with timestamp old enough.
func (s *SkippedSequenceSkiplist) SkippedSequenceCompact(ctx context.Context, maxWait int64) (numSequencesCompacted int64) {
	numSequencesCompacted = s.list.CompactList(time.Now().Unix(), maxWait)
	return numSequencesCompacted
}

func (s *SkippedSequenceSkiplist) PushSkippedSequenceEntry(entry skiplist.SkippedSequenceEntry) {
	elem := s.list.Set(entry)
	if elem != nil {
		// update num current skipped sequences count + the cumulative count of skipped sequences
		numSequencesIncoming := entry.GetNumSequencesInEntry()
		s.NumCumulativeSkippedSequences += numSequencesIncoming
	}
}

// getOldest returns the start sequence of the first element in the skipped sequence list
func (s *SkippedSequenceSkiplist) getOldest() uint64 {
	elem := s.list.Front()
	if elem != nil {
		return elem.Key().Start
	}
	// list empty
	return 0
}

// getStats will return all associated stats with the skipped sequence list
func (s *SkippedSequenceSkiplist) getStats() SkippedSequenceStats {
	return SkippedSequenceStats{
		NumCumulativeSkippedSequencesStat: s.NumCumulativeSkippedSequences,
		NumCurrentSkippedSequencesStat:    s.list.GetNumSequencesInList(),
		ListLengthStat:                    int64(s.list.GetLength()),
	}
}

// processUnusedSequenceRangeAtSkipped will batch remove unused sequence range from skipped sequences, if duplicate
// sequences are present, we will iterate through skipped list removing the non-duplicate sequences
func (s *SkippedSequenceSkiplist) processUnusedSequenceRangeAtSkipped(ctx context.Context, fromSequence, toSequence uint64) {
	// batch remove from skipped
	elem := s.list.Remove(skiplist.SkippedSequenceEntry{Start: fromSequence, End: toSequence})
	if elem == nil {
		base.DebugfCtx(ctx, base.KeyCache, "Unused sequence range #%d to #%d not present in skipped list", fromSequence, toSequence)
		return
	}
}

/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// File renamed from `change_log.go` in CBG-1949 PR #5452 commit `e4454e4640fbbd593949ea9ef3b11e23279d0281`

package channels

import (
	"fmt"
	"time"
)

// Bits in LogEntry.Flags
const (
	Deleted  = 1 << iota // This rev is a deletion
	Removed              // Doc was removed from this channel
	Hidden               // This rev is not the default (hidden by a conflict)
	Conflict             // Document is in conflict at this time
	Branched             // Revision tree is branched
	Added                // Doc was added to this channel

)

// LogEntry stores information about a change to a document in a cache.
type LogEntry struct {
	Channels       ChannelMap    // Channels this entry is in or was removed from
	DocID          string        // Document ID
	RevID          string        // Revision ID
	Sequence       uint64        // Sequence number
	EndSequence    uint64        // End sequence on range of sequences that have been released by the sequence allocator (0 if entry is single sequence)
	TimeReceived   FeedTimestamp // Time received from tap feed
	CollectionID   uint32        // Collection ID
	Flags          uint8         // Deleted/Removed/Hidden flags
	Skipped        bool          // Late arriving entry
	IsPrincipal    bool          // Whether the log-entry is a tracking entry for a principal doc
	UnusedSequence bool          // Whether the log-entry is a tracking entry for a unused sequence(s)
}

func (l LogEntry) String() string {
	return fmt.Sprintf(
		"seq: %d docid: %s revid: %s collectionID: %d",
		l.Sequence,
		l.DocID,
		l.RevID,
		l.CollectionID,
	)
}

// IsRemoved returns true if the entry represents a channel removal.
func (l *LogEntry) IsRemoved() bool {
	return l.Flags&Removed != 0
}

// IsDeleted returns true if the entry represents a document deletion.
func (l *LogEntry) IsDeleted() bool {
	return l.Flags&Deleted != 0
}

// IsActive returns false if the entry is either a removal or a delete.
func (l *LogEntry) IsActive() bool {
	return !l.IsRemoved() && !l.IsDeleted()
}

// SetRemoved marks the entry as a channel removal.
func (l *LogEntry) SetRemoved() {
	l.Flags |= Removed
}

// SetDeleted marks the entry as a document deletion.
func (l *LogEntry) SetDeleted() {
	l.Flags |= Deleted
}

// IsUnusedRange returns true if the entry represents an unused sequence document with more than one sequence.
func (l *LogEntry) IsUnusedRange() bool {
	return l.UnusedSequence && l.EndSequence > 0
}

type ChannelMap map[string]*ChannelRemoval
type ChannelRemoval struct {
	Seq     uint64 `json:"seq,omitempty"`
	RevID   string `json:"rev"`
	Deleted bool   `json:"del,omitempty"`
}

func (channelMap ChannelMap) ChannelsRemovedAtSequence(seq uint64) (ChannelMap, string) {
	var channelsRemoved = make(ChannelMap)
	var revIdRemoved string
	for channel, removal := range channelMap {
		if removal != nil && removal.Seq == seq {
			channelsRemoved[channel] = removal
			revIdRemoved = removal.RevID // Will be the same RevID for each removal
		}
	}
	return channelsRemoved, revIdRemoved
}

func (channelMap ChannelMap) KeySet() []string {
	result := make([]string, len(channelMap))
	i := 0
	for key, _ := range channelMap {
		result[i] = key
		i++
	}
	return result
}

// FeedTimestamp is a timestamp struct used by DCP. This avoids a conversion from time.Time, while reducing the size from 24 bytes to 8 bytes while having type safety. The time is always assumed to be in local time.
type FeedTimestamp int64

// NewFeedTimestampFromNow creates a new FeedTimestamp from the current time.
func NewFeedTimestampFromNow() FeedTimestamp {
	return FeedTimestamp(time.Now().UnixNano())
}

// NewFeedTimestamp creates a new FeedTimestamp from a specific time.Time.
func NewFeedTimestamp(t *time.Time) FeedTimestamp {
	return FeedTimestamp(t.UnixNano())
}

// Since returns the nanoseconds that have passed since this timestamp. This function can overflow.
func (t FeedTimestamp) Since() int64 {
	return time.Now().UnixNano() - int64(t)
}

// OlderThan returns true if the timestamp is older than the given duration.
func (t FeedTimestamp) OlderThan(duration time.Duration) bool {
	return t.Since() > int64(duration)
}

// OlderOrEqual returns true if the timestamp is older or equal to the given duration.
func (t FeedTimestamp) OlderOrEqual(duration time.Duration) bool {
	return t.Since() >= int64(duration)
}

// After returns true if the timestamp is after the given time.
func (t FeedTimestamp) After(other time.Time) bool {
	return int64(t) > other.UnixNano()
}

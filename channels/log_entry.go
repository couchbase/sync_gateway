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

// LogEntry
type LogEntryType uint8

const (
	LogEntryDocument = LogEntryType(iota)
	LogEntryPrincipal
	LogEntryCheckpoint
	LogEntryRollback
	LogEntryPurge
)

// Bits in LogEntry.Flags
const (
	Deleted  = 1 << iota // This rev is a deletion
	Removed              // Doc was removed from this channel
	Hidden               // This rev is not the default (hidden by a conflict)
	Conflict             // Document is in conflict at this time
	Branched             // Revision tree is branched
	Added                // Doc was added to this channel

	kMaxFlag = (1 << iota) - 1
)

type LogEntry struct {
	Sequence     uint64       // Sequence number
	DocID        string       // Document ID
	RevID        string       // Revision ID
	Flags        uint8        // Deleted/Removed/Hidden flags
	VbNo         uint16       // vbucket number
	TimeSaved    time.Time    // Time doc revision was saved (just used for perf metrics)
	TimeReceived time.Time    // Time received from tap feed
	Channels     ChannelMap   // Channels this entry is in or was removed from
	Skipped      bool         // Late arriving entry
	Type         LogEntryType // Log entry type
	Value        []byte       // Snapshot metadata (when Type=LogEntryCheckpoint)
	PrevSequence uint64       // Sequence of previous active revision
	IsPrincipal  bool         // Whether the log-entry is a tracking entry for a principal doc
}

func (l LogEntry) String() string {
	return fmt.Sprintf(
		"seq: %d docid: %s revid: %s vbno: %d type: %v",
		l.Sequence,
		l.DocID,
		l.RevID,
		l.VbNo,
		l.Type,
	)
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
			revIdRemoved = removal.RevID //Will be the same RevID for each removal
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

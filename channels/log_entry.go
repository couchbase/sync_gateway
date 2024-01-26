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

	"github.com/couchbase/sync_gateway/base"
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
	CollectionID uint32       // Collection ID
	SourceID     string       // SourceID allocated to the doc's Current Version on the HLV
	Version      uint64       // Version allocated to the doc's Current Version on the HLV
}

func (l LogEntry) String() string {
	return fmt.Sprintf(
		"seq: %d docid: %s revid: %s vbno: %d type: %v collectionID: %d source: %s version: %d",
		l.Sequence,
		l.DocID,
		l.RevID,
		l.VbNo,
		l.Type,
		l.CollectionID,
		l.SourceID,
		l.Version,
	)
}

type ChannelMap map[string]*ChannelRemoval
type ChannelRemoval struct {
	Seq     uint64        `json:"seq,omitempty"`
	Rev     RevAndVersion `json:"rev"`
	Deleted bool          `json:"del,omitempty"`
}

func (channelMap ChannelMap) ChannelsRemovedAtSequence(seq uint64) (ChannelMap, RevAndVersion) {
	var channelsRemoved = make(ChannelMap)
	var revIdRemoved RevAndVersion
	for channel, removal := range channelMap {
		if removal != nil && removal.Seq == seq {
			channelsRemoved[channel] = removal
			revIdRemoved = removal.Rev // Will be the same Rev for each removal
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

// RevAndVersion is used to store both revTreeID and currentVersion in a single property, for backwards compatibility
// with existing indexes using rev.  When only RevTreeID is specified, is marshalled/unmarshalled as a string.  Otherwise
// marshalled normally.
type RevAndVersion struct {
	RevTreeID      string `json:"rev,omitempty"`
	CurrentSource  string `json:"src,omitempty"`
	CurrentVersion string `json:"vrs,omitempty"` // String representation of version
}

// RevAndVersionJSON aliases RevAndVersion to support conditional unmarshalling from either string (revTreeID) or
// map (RevAndVersion) representations
type RevAndVersionJSON RevAndVersion

// Marshals RevAndVersion as simple string when only RevTreeID is specified - otherwise performs standard
// marshalling
func (rv RevAndVersion) MarshalJSON() (data []byte, err error) {

	if rv.CurrentSource == "" {
		return base.JSONMarshal(rv.RevTreeID)
	}
	return base.JSONMarshal(RevAndVersionJSON(rv))
}

// Unmarshals either from string (legacy, revID only) or standard RevAndVersion unmarshalling.
func (rv *RevAndVersion) UnmarshalJSON(data []byte) error {

	if len(data) == 0 {
		return nil
	}
	switch data[0] {
	case '"':
		return base.JSONUnmarshal(data, &rv.RevTreeID)
	case '{':
		return base.JSONUnmarshal(data, (*RevAndVersionJSON)(rv))
	default:
		return fmt.Errorf("unrecognized JSON format for RevAndVersion: %s", data)
	}
}

package channels

import (
	"fmt"
	"sort"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// LogEntryType
type LogEntryType uint8

const (
	LogEntryDocument = LogEntryType(iota)
	LogEntryPrincipal
	LogEntryCheckpoint
	LogEntryRollback
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
}

type ChannelMap map[string]*ChannelRemoval
type ChannelRemoval struct {
	Seq     uint64 `json:"seq,omitempty"`
	RevID   string `json:"rev"`
	Deleted bool   `json:"del,omitempty"`
}

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

// A sequential log of document revisions added to a channel, used to generate _changes feeds.
// The log is sorted by increasing sequence number.
type ChangeLog struct {
	Since   uint64      // Sequence this log is valid _after_, i.e. max sequence not in the log
	Entries []*LogEntry // Ordered entries
}

func (entry *LogEntry) checkValid() bool {
	return entry.Sequence > 0 && entry.DocID != "" && entry.RevID != "" && entry.Flags <= kMaxFlag
}

func (entry *LogEntry) assertValid() {
	if !entry.checkValid() {
		panic(fmt.Sprintf("Invalid entry: %+v", entry))
	}
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

func (cp *ChangeLog) LastSequence() uint64 {
	if n := len(cp.Entries); n > 0 {
		return cp.Entries[n-1].Sequence
	}
	return cp.Since
}

// Adds a new entry, always at the end of the log.
func (cp *ChangeLog) Add(newEntry LogEntry) {
	newEntry.assertValid()
	if len(cp.Entries) == 0 || newEntry.Sequence == cp.Since {
		cp.Since = newEntry.Sequence - 1
	}
	cp.Entries = append(cp.Entries, &newEntry)
}

func (cp *ChangeLog) AddEntries(entries []*LogEntry) {
	if len(entries) == 0 {
		return
	}
	if len(cp.Entries) == 0 {
		cp.Since = entries[0].Sequence - 1
	}
	cp.Entries = append(cp.Entries, entries...)
}

// Removes the oldest entries to limit the log's length to `maxLength`.
func (cp *ChangeLog) TruncateTo(maxLength int) int {
	if remove := len(cp.Entries) - maxLength; remove > 0 {
		// Set Since to the max of the sequences being removed:
		for _, entry := range cp.Entries[0:remove] {
			if entry.Sequence > cp.Since {
				cp.Since = entry.Sequence
			}
		}
		// Copy entries into a new array to avoid leaving the entire old array in memory:
		newEntries := make([]*LogEntry, maxLength)
		copy(newEntries, cp.Entries[remove:])
		cp.Entries = newEntries
		return remove
	}
	return 0
}

// Returns a slice of all entries added after the one with sequence number 'after'.
// (They're not guaranteed to have higher sequence numbers; sequences may be added out of order.)
func (cp *ChangeLog) EntriesAfter(after uint64) []*LogEntry {
	entries := cp.Entries
	if after == cp.Since {
		return entries
	}
	for i, entry := range entries {
		if entry.Sequence == after {
			return entries[i+1:]
		}
	}
	return nil
}

// Filters the log to only the entries added after the one with sequence number 'after.
func (cp *ChangeLog) FilterAfter(after uint64) {
	cp.Entries = cp.EntriesAfter(after)
	if after > cp.Since {
		cp.Since = after
	}
}

func (cp *ChangeLog) HasEmptyEntries() bool {
	for _, entry := range cp.Entries {
		if entry.DocID == "" {
			return true
		}
	}
	return false
}

// Returns a copy without empty (no DocID) entries that resulted from revisions that have
// been replaced. Result does not share a slice with the original cp.
func (cp *ChangeLog) CopyRemovingEmptyEntries() *ChangeLog {
	dst := make([]*LogEntry, 0, len(cp.Entries))
	for _, entry := range cp.Entries {
		if entry.DocID != "" {
			dst = append(dst, entry)
		}
	}
	return &ChangeLog{cp.Since, dst}
}

func (cp *ChangeLog) Dump() {
	base.Logf("Since: %d\n", cp.Since)
	for _, e := range cp.Entries {
		base.Logf("    %5d %q %q %b\n", e.Sequence, e.DocID, e.RevID, e.Flags)
	}
}

// Sorts the entries by increasing sequence.
func (c *ChangeLog) Sort() {
	sort.Sort(c)
}

func (c *ChangeLog) Len() int { // part of sort.Interface
	return len(c.Entries)
}

func (c *ChangeLog) Less(i, j int) bool { // part of sort.Interface
	return c.Entries[i].Sequence < c.Entries[j].Sequence
}

func (c *ChangeLog) Swap(i, j int) { // part of sort.Interface
	c.Entries[i], c.Entries[j] = c.Entries[j], c.Entries[i]
}

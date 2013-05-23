package channels

import (
	"fmt"
	"sort"
)

type LogEntry struct {
	Sequence uint64 `json:"seq"`
	DocID    string `json:"doc"`
	RevID    string `json:"rev"`
	Deleted  bool   `json:"del,omitempty"`
	Removed  bool   `json:"rmv,omitempty"`
	Hidden   bool   `json:"hid,omitempty"`
}

// A sequential log of document revisions added to a channel, used to generate _changes feeds.
// The log is sorted by ascending sequence.
// Only a document's latest (by sequence) revision appears in the log.
// An empty RevID denotes a removal from the channel.
type ChangeLog struct {
	Since   uint64 // Sequence this is valid after
	Entries []LogEntry
}

// Adds a new entry, or returns false if it already exists in the log.
func (cp *ChangeLog) Add(newEntry LogEntry) bool {
	if newEntry.Sequence == 0 || newEntry.DocID == "" || newEntry.RevID == "" {
		panic(fmt.Sprintf("Invalid entry: %+v", newEntry))
	}
	entries := cp.Entries
	where := sort.Search(len(entries), func(i int) bool {
		return entries[i].Sequence >= newEntry.Sequence
	})
	if where < len(entries) && entries[where].Sequence == newEntry.Sequence {
		return false
	}
	if len(entries) == 0 || newEntry.Sequence <= cp.Since {
		cp.Since = newEntry.Sequence - 1
	}
	insertion := []LogEntry{newEntry}
	cp.Entries = append(entries[:where], append(insertion, entries[where:]...)...)
	return true
}

// Removes a specific doc/revision, if present
func (cp *ChangeLog) Remove(docID, revID string) bool {
	if revID != "" {
		entries := cp.Entries
		for i, entry := range entries {
			if entry.DocID == docID && entry.RevID == revID {
				copy(entries[i:], entries[i+1:])
				cp.Entries = entries[:len(entries)-1]
				return true
			}
		}
	}
	return false
}

// Inserts a new entry, removing the one for the parent revision (if any).
func (cp *ChangeLog) Update(newEntry LogEntry, parentRevID string) bool {
	if !cp.Add(newEntry) {
		return false
	}
	cp.Remove(newEntry.DocID, parentRevID)
	return true
}

// Removes the oldest entries to limit the log's length to `maxLength`.
func (cp *ChangeLog) TruncateTo(maxLength int) {
	if remove := len(cp.Entries) - maxLength; remove > 0 {
		cp.Since = cp.Entries[remove-1].Sequence + 1
		cp.Entries = cp.Entries[remove:]
	}
}

// Returns a slice of all entries with sequences greater than 'since'.
func (cp *ChangeLog) EntriesSince(since uint64) []LogEntry {
	entries := cp.Entries
	if entries == nil || since <= cp.Since {
		return entries
	}
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].Sequence > since
	})
	return entries[start:]
}

// Filters the log to only the entries with sequences greater than 'since'.
func (cp *ChangeLog) FilterSince(since uint64) {
	entries := cp.Entries
	if entries == nil || since <= cp.Since {
		return
	}
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].Sequence > since
	})
	if start > 0 {
		cp.Since = entries[start-1].Sequence + 1
		cp.Entries = entries[start:]
	}
}

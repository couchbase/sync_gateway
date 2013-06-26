package channels

import (
	"fmt"
)

type LogEntry struct {
	Sequence uint64 // Sequence number
	DocID    string // Empty if this entry has been replaced
	RevID    string // Empty if this entry has been replaced
	Flags    uint8  // Deleted/Removed/Hidden flags
}

const (
	Deleted = 1 << iota
	Removed
	Hidden
)

// A sequential log of document revisions added to a channel, used to generate _changes feeds.
// The log is sorted by order revisions were added; this is mostly but not always by sequence.
// Revisions replaced by children are not removed but their doc/rev IDs are changed to "".
type ChangeLog struct {
	Since   uint64      // Sequence this is valid after
	Entries []*LogEntry // Entries in order they were added (not sequence order!)
}

// Adds a new entry, always at the end of the log.
func (cp *ChangeLog) Add(newEntry LogEntry) {
	if newEntry.Sequence == 0 || newEntry.DocID == "" || newEntry.RevID == "" {
		panic(fmt.Sprintf("Invalid entry: %+v", newEntry))
	}
	if len(cp.Entries) == 0 || newEntry.Sequence <= cp.Since {
		cp.Since = newEntry.Sequence - 1
	}
	cp.Entries = append(cp.Entries, &newEntry)
}

// Removes a specific doc/revision, if present. (It's actually marked as empty, not removed.)
func (cp *ChangeLog) Remove(docID, revID string) bool {
	if revID != "" {
		entries := cp.Entries
		for i, entry := range entries {
			if entry.DocID == docID && entry.RevID == revID {
				entry.DocID = ""
				entry.RevID = ""
				entries[i] = entry // write it back
				return true
			}
		}
	}
	return false
}

// Inserts a new entry, removing the one for the parent revision (if any).
func (cp *ChangeLog) Update(newEntry LogEntry, parentRevID string) {
	cp.Add(newEntry)
	cp.Remove(newEntry.DocID, parentRevID)
}

// Removes the oldest entries to limit the log's length to `maxLength`.
func (cp *ChangeLog) TruncateTo(maxLength int) int {
	if remove := len(cp.Entries) - maxLength; remove > 0 {
		cp.Since = cp.Entries[remove-1].Sequence
		cp.Entries = cp.Entries[remove:]
		return remove
	}
	return 0
}

// Returns a slice of all entries added after the one with sequence number 'after'.
// (They're not guaranteed to have higher sequence numbers; sequences may be added out of order.)
func (cp *ChangeLog) EntriesAfter(after uint64) []*LogEntry {
	entries := cp.Entries
	for i, entry := range entries {
		if entry.Sequence == after {
			return entries[i+1:]
		}
	}
	return entries
}

// Filters the log to only the entries added after the one with sequence number 'after.
func (cp *ChangeLog) FilterAfter(after uint64) {
	cp.Entries = cp.EntriesAfter(after)
	if after > cp.Since {
		cp.Since = after
	}
}

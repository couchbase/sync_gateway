package channels

import (
	"sort"
)

// The maximum number of entries that will be kept in a log. If the length would overflow this
// limit, the earliest/oldest entries are removed to make room.
var MaxLogLength = 500

type LogEntry struct {
	Sequence uint64 `json:"seq"`
	DocID    string `json:"doc"`
	RevID    string `json:"rev"`
	Deleted  bool   `json:"del,omitempty"`
	Removed  bool   `json:"rmv,omitempty"`
}

// A sequential log of document revisions added to a channel, used to generate _changes feeds.
// The log is sorted by ascending sequence.
// Only a document's latest (by sequence) revision appears in the log.
// An empty RevID denotes a removal from the channel.
type ChannelLog []LogEntry

// Adds a new entry to a ChannelLog, in sorted sequence order.
// Any earlier entry with the same document ID will be deleted.
// Returns true if the entry was added, false if not (it already exists or is too old)
func (cp *ChannelLog) Add(newEntry LogEntry) bool {
	c := *cp
	// Figure out which entry if any to remove (earlier sequence for same docID)
	// and where to insert the new entry:
	remove := -1
	insert := -1
	for i, entry := range c {
		if entry.Sequence == newEntry.Sequence {
			return false // already have this entry
		} else if entry.Sequence > newEntry.Sequence {
			if entry.DocID == newEntry.DocID {
				return false // already have newer entry for this doc
			}
			if insert < 0 {
				insert = i // here's where to insert it
			}
		} else if entry.DocID == newEntry.DocID {
			remove = i // older entry for this doc can be removed
		}
	}
	if insert < 0 {
		insert = len(c)
	}
	if remove < 0 && len(c) >= MaxLogLength {
		// Log is full so remove the oldest item
		remove = 0
	}

	if remove >= 0 {
		// Remove the item at index 'remove' and insert a new one at index 'insert':
		copy(c[remove:insert], c[remove+1:insert+1])
		c[insert-1] = newEntry
	} else {
		// or just insert:
		insertion := []LogEntry{newEntry}
		*cp = append(c[:insert], append(insertion, c[insert:]...)...)
	}
	return true
}

// Returns a slice of all entries with sequences greater than 'since'.
func (c ChannelLog) Since(since uint64) []LogEntry {
	if c == nil || since == 0 {
		return c
	}
	start := sort.Search(len(c), func(i int) bool {
		return c[i].Sequence > since
	})
	return c[start:]
}

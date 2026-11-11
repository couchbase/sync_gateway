/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

import (
	"fmt"
	"slices"
	"strings"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

// The model records what the simulated bucket holds. It deliberately has no notion of the cache's pending or
// skipped bookkeeping: properties are stated against state the cache reports (nextSequence, the skipped list,
// validFrom), or compared exactly once every sequence has been delivered.

// fuzzRev is one written revision of a document.
type fuzzRev struct {
	docIndex   int
	docID      string
	generation int
	seq        uint64
	revID      string
	channels   channels.ChannelMap // active channels map to nil; channels left map to their removal, as in sync data
	recentSeqs []uint64
	unusedSeqs []uint64
}

// currentSequence is the sequence the cache treats as the document's latest when it walks recent_sequences. A
// write that wasted a sequence is counted from the first wasted one.
func (r *fuzzRev) currentSequence() uint64 {
	if len(r.unusedSeqs) > 0 {
		return r.unusedSeqs[0]
	}
	return r.seq
}

func (r *fuzzRev) String() string {
	return fmt.Sprintf("%s@%d(%s)", r.docID, r.seq, r.revID[:min(len(r.revID), 6)])
}

// fuzzSeqRecord says what an allocated sequence is. rev is set for document writes.
type fuzzSeqRecord struct {
	kind     fuzzEventKind
	rev      *fuzzRev // the write that allocated it, for eventDoc; nil otherwise
	burnedBy *fuzzRev // for an unused sequence a write wasted
	name     string   // eventPrincipal: the principal's key
	from, to uint64   // eventUnusedRange: the whole range the sequence belongs to
}

// fuzzChannelState is a document's state in one channel as the bucket's channel view would report it.
type fuzzChannelState struct {
	seq     uint64
	revID   string
	removed bool
}

func (s fuzzChannelState) String() string {
	if s.removed {
		return fmt.Sprintf("removal@%d(%s)", s.seq, s.revID[:min(len(s.revID), 6)])
	}
	return fmt.Sprintf("active@%d(%s)", s.seq, s.revID[:min(len(s.revID), 6)])
}

// matches reports whether a cached or received entry is this state of docID.
func (s fuzzChannelState) matches(docID string, e *db.LogEntry) bool {
	return e != nil && e.DocID == docID && e.Sequence == s.seq && e.RevID == s.revID && e.IsRemoved() == s.removed
}

func describeEntry(e *db.LogEntry) string {
	if e == nil {
		return "<none>"
	}
	return fuzzChannelState{seq: e.Sequence, revID: e.RevID, removed: e.IsRemoved()}.String()
}

// fuzzModel is the bucket's truth.
type fuzzModel struct {
	revs      [fuzzNumDocs][]*fuzzRev // every written revision, oldest first
	seqs      map[uint64]fuzzSeqRecord
	delivered map[uint64]bool // document-write sequences whose event has been delivered at least once
	abandoned map[uint64]bool // skipped sequences the cache gave up on without ever receiving them
	queries   []fuzzQuery     // every channel view query the cache made
}

// isLaterState reports whether e is the state in channel of a write of rev's document after rev.
func (m *fuzzModel) isLaterState(rev *fuzzRev, channel string, e *db.LogEntry) bool {
	for _, later := range m.revs[rev.docIndex] {
		if later.seq <= rev.seq {
			continue
		}
		if state, ok := later.channelState(channel); ok && state.matches(rev.docID, e) {
			return true
		}
	}
	return false
}

// fuzzQuery is one channel view query, with the sequences that had been written when it ran.
type fuzzQuery struct {
	channel          string
	startSeq, endSeq uint64 // inclusive; an endSeq of zero is unbounded
	allocated        uint64 // sequences below this had been written
	nextSequence     uint64 // the cache's nextSequence when the query ran
	skipped          []uint64
	cache            *fuzzCacheSnapshot // the channel's cache just before the read, nil if it did not exist
}

// viewOmittedState reports whether a channel view query was the only route by which a revision's state could have
// reached a channel cache, and the view left it out because the document had been rewritten. The view only shows a
// document's latest write, so a cache that backfilled across the revision's sequence does not hold the older state.
//
// The revision must already have been processed when the query ran (below nextSequence and not skipped), and the
// channel's cache must not have held its state then. Otherwise the revision reached, or was due to reach, the cache by
// normal processing after the query, and the cache has to hold it.
func (m *fuzzModel) viewOmittedState(channel string, rev *fuzzRev) bool {
	for _, q := range m.queries {
		if q.channel != channel || rev.seq < q.startSeq || (q.endSeq != 0 && rev.seq > q.endSeq) {
			continue
		}
		if rev.seq >= q.nextSequence || slices.Contains(q.skipped, rev.seq) {
			continue
		}
		if q.cache != nil && q.cache.validFrom <= rev.seq {
			if held := q.cache.entryFor(rev.docID); held != nil && held.Sequence == rev.seq {
				continue
			}
		}
		for _, later := range m.revs[rev.docIndex] {
			if later.seq > rev.seq && later.seq < q.allocated {
				return true
			}
		}
	}
	return false
}

func newFuzzModel() *fuzzModel {
	return &fuzzModel{
		seqs:      map[uint64]fuzzSeqRecord{},
		delivered: map[uint64]bool{},
		abandoned: map[uint64]bool{},
	}
}

func (m *fuzzModel) latestRev(docIndex int) *fuzzRev {
	revs := m.revs[docIndex]
	if len(revs) == 0 {
		return nil
	}
	return revs[len(revs)-1]
}

// highestDelivered returns the doc's highest-sequence revision whose event has been delivered, or nil.
func (m *fuzzModel) highestDelivered(docIndex int) *fuzzRev {
	revs := m.revs[docIndex]
	for i := len(revs) - 1; i >= 0; i-- {
		if m.delivered[revs[i].seq] {
			return revs[i]
		}
	}
	return nil
}

// channelState is a revision's state in a channel: the "*" channel holds every revision as active, a channel
// the revision lists as active is active at the revision's own sequence, and a channel it lists as left is a
// removal at the sequence it was left. The channel map keeps removals forever, so a document that was ever in a
// channel always has a state there.
func (r *fuzzRev) channelState(channel string) (fuzzChannelState, bool) {
	if channel == channels.UserStarChannel {
		return fuzzChannelState{seq: r.seq, revID: r.revID}, true
	}
	removal, ok := r.channels[channel]
	if !ok {
		return fuzzChannelState{}, false
	}
	if removal == nil {
		return fuzzChannelState{seq: r.seq, revID: r.revID}, true
	}
	return fuzzChannelState{seq: removal.Seq, revID: removal.Rev.RevTreeID, removed: true}, true
}

// entriesPutBy lists the channel entries a revision's own delivery adds to the caches, in channel name order:
// the channels it is active in, the channels it was removed from at its own sequence, and "*".
func (r *fuzzRev) entriesPutBy() []string {
	names := []string{channels.UserStarChannel}
	for name, removal := range r.channels {
		if removal == nil || removal.Seq == r.seq {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// removedAt lists the channels the revision removed the document from at seq, which are the channels the cache
// adds an entry to when it replays seq from a later revision's recent_sequences, plus "*".
func (r *fuzzRev) removedAt(seq uint64) []string {
	var names []string
	for name, removal := range r.channels {
		if removal != nil && removal.Seq == seq {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	names = append(names, channels.UserStarChannel)
	slices.Sort(names)
	return names
}

// channelView is what the bucket's channel view returns: for every document its latest written state in the
// channel, whether or not the cache has seen that write. startSeq and endSeq are inclusive and an endSeq of
// zero is unbounded, as in the real query. Entries are in sequence order.
func (m *fuzzModel) channelView(collectionID uint32, channel string, startSeq, endSeq uint64, limit int, activeOnly bool) db.LogEntries {
	view := make(db.LogEntries, 0)
	for docIndex := range m.revs {
		latest := m.latestRev(docIndex)
		if latest == nil {
			continue
		}
		state, ok := latest.channelState(channel)
		if !ok || state.seq < startSeq || (endSeq != 0 && state.seq > endSeq) || (activeOnly && state.removed) {
			continue
		}
		entry := &db.LogEntry{DocID: latest.docID, RevID: state.revID, Sequence: state.seq, CollectionID: collectionID}
		if state.removed {
			entry.Flags |= channels.Removed
		}
		view = append(view, entry)
	}
	slices.SortFunc(view, func(a, b *db.LogEntry) int {
		switch {
		case a.Sequence < b.Sequence:
			return -1
		case a.Sequence > b.Sequence:
			return 1
		}
		return 0
	})
	if limit > 0 && len(view) > limit {
		view = view[:limit]
	}
	return view
}

// fuzzReceived is one entry a simulated feed received.
type fuzzReceived struct {
	channel string
	seq     uint64
	docID   string
	revID   string
	removed bool
}

func (r fuzzReceived) String() string {
	return fmt.Sprintf("%s:%d:%s:%s:%t", r.channel, r.seq, r.docID, r.revID[:min(len(r.revID), 6)], r.removed)
}

// fuzzCacheSnapshot is one channel cache's contents, read under the cache's own lock.
type fuzzCacheSnapshot struct {
	name      string
	validFrom uint64
	entries   []*db.LogEntry
}

// entryFor returns the cache's entry for a document, or nil. A channel cache is short, so a scan is cheaper than
// building an index for every snapshot.
func (s *fuzzCacheSnapshot) entryFor(docID string) *db.LogEntry {
	for _, e := range s.entries {
		if e.DocID == docID {
			return e
		}
	}
	return nil
}

func (s *fuzzCacheSnapshot) String() string {
	parts := make([]string, 0, len(s.entries))
	for _, e := range s.entries {
		parts = append(parts, fmt.Sprintf("%d:%s:%t", e.Sequence, e.DocID, e.IsRemoved()))
	}
	return fmt.Sprintf("%s validFrom=%d [%s]", s.name, s.validFrom, strings.Join(parts, " "))
}

// allChannelNames is every channel a cache can exist for in a run.
var allChannelNames = append(slices.Clone(fuzzChannelNames[:]), channels.UserStarChannel)

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
	"slices"

	"github.com/couchbase/sync_gateway/db"
)

// Properties checked against the model. Every cache read goes through an accessor that takes the cache's own lock:
// the race detector does not treat synctest's fake sleeps as synchronisation with the cache's background tasks.

// existingCache returns a channel's cache without creating it.
func (h *fuzzHarness) existingCache(name string) (db.SingleChannelCache, bool) {
	value, ok := h.channelCache.ChannelCachesForTest(h.t).Get(h.channelID(name))
	if !ok {
		return nil, false
	}
	cache, ok := value.(db.SingleChannelCache)
	if !ok {
		h.failf("channel cache for %s has unexpected type %T", name, value)
	}
	return cache, true
}

// snapshot reads one channel's cache if it exists, or returns nil.
func (h *fuzzHarness) snapshot(name string) *fuzzCacheSnapshot {
	cache, ok := h.existingCache(name)
	if !ok {
		return nil
	}
	// A read since sequence 0 returns the whole log together with validFrom, both under one lock.
	validFrom, entries := cache.GetCachedChanges(db.ChangesOptions{})
	return &fuzzCacheSnapshot{name: name, validFrom: validFrom, entries: entries}
}

// snapshots reads every channel cache that exists, keyed by channel name.
func (h *fuzzHarness) snapshots() map[string]*fuzzCacheSnapshot {
	snaps := make(map[string]*fuzzCacheSnapshot, len(allChannelNames))
	for _, name := range allChannelNames {
		if snap := h.snapshot(name); snap != nil {
			snaps[name] = snap
		}
	}
	return snaps
}

// checkAfterOp checks the properties that hold after every op.
func (h *fuzzHarness) checkAfterOp() {
	h.t.Helper()
	h.pinLateLogs()
	next, stable, skipped := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	h.refreshStepEnd(next, skipped)
	h.recordTrace(next, stable, skipped)
	if v := h.takeViolation(); v != "" {
		h.failf("%s", v)
	}

	// I6: the cache only moves forward.
	if next < h.lastNext {
		h.failf("I6: nextSequence went backwards from %d to %d", h.lastNext, next)
	}
	if stable < h.lastStable {
		h.failf("I6: high_seq_stable went backwards from %d to %d", h.lastStable, stable)
	}
	h.lastNext, h.lastStable = next, stable

	snaps := h.snapshots()
	h.checkStructure(next, snaps)
	h.checkSequenceViews(next)

	// I3: a channel cache vouches for nothing below validFrom, so it must not hold anything there.
	for _, name := range allChannelNames {
		snap, ok := snaps[name]
		if !ok {
			continue
		}
		for _, e := range snap.entries {
			if e.Sequence < snap.validFrom {
				h.failf("I3: channel %s holds %s for %s below validFrom %d\ncache: %s", name, describeEntry(e), e.DocID, snap.validFrom, snap)
			}
		}
	}

	// I1: once a document's newest delivered revision has been processed, every channel cache that covers it holds
	// its entry. Processed means below nextSequence and not skipped, whatever the stable sequence: a real changes
	// feed reads up to the cache's high sequence and sends what is above the stable sequence with compound
	// sequences, so it relies on everything processed being in the cache, not only what is stable.
	for docIndex := range h.model.revs {
		rev := h.model.highestDelivered(docIndex)
		// A newest revision that is not cached yet (pending or skipped) says nothing about the cache, and neither
		// does one the cache gave up on. Older delivered revisions are not checked because the newest may already
		// have superseded them.
		if rev == nil || h.model.abandoned[rev.seq] || rev.seq >= next || slices.Contains(skipped, rev.seq) {
			continue
		}
		for _, name := range rev.entriesPutBy() {
			snap, ok := snaps[name]
			if !ok || snap.validFrom > rev.seq {
				continue
			}
			want, _ := rev.channelState(name)
			got := snap.entryFor(rev.docID)
			switch {
			case want.matches(rev.docID, got):
			case got == nil && h.model.viewOmittedState(name, rev):
			case got != nil && got.Sequence > rev.seq:
				// The cache backfilled this channel from the bucket after the document was rewritten, so it holds the
				// state of a later write, which the cache has not necessarily been sent yet.
				if !h.model.isLaterState(rev, name, got) {
					h.failf("I1: channel %s holds %s for %s, which is neither %s (delivered and processed) nor the state of any later write\ncache: %s",
						name, describeEntry(got), rev.docID, want, snap)
				}
			default:
				h.failf("I1: channel %s (validFrom %d) holds %s for %s, want %s (delivered and processed, nextSequence %d, stable %d)\ncache: %s",
					name, snap.validFrom, describeEntry(got), rev.docID, want, next, stable, snap)
			}
		}
	}
}

// fuzzLateEntry is one entry the harness expects in a channel's late log, with the number of late-feed clients it has
// registered on it.
type fuzzLateEntry struct {
	seq       uint64
	listeners int
}

// pinLateLogs registers a late-feed client on every channel cache that exists and has not been pinned yet, and
// seeds the harness's model of its late log. Caches only come into being inside harness ops (CreateCache, a feed's
// GetChanges, start), on the harness goroutine, and none is ever evicted or replaced here: there are only six
// channels against a limit of 50000, so compaction cannot run. The background tasks only prune existing caches. So
// pinning after every op, and again before every delivery, covers every cache before any late entry is added to it.
func (h *fuzzHarness) pinLateLogs() {
	for _, name := range allChannelNames {
		cache, ok := h.existingCache(name)
		if !ok {
			continue
		}
		if pinned, ok := h.pinned[name]; ok {
			if pinned != cache {
				h.failf("channel cache %s was replaced after it was pinned", name)
			}
			continue
		}
		log := h.lateLog(name)
		if len(log) == 0 {
			// Registering on an evicted cache returns 0 without pinning anything; a live cache always keeps an entry.
			h.failf("channel cache %s has no late log to pin", name)
		}
		model := make([]fuzzLateEntry, len(log))
		for i, seq := range log {
			model[i].seq = seq
		}
		h.pinned[name] = cache
		h.lateModel[name] = model
		h.registerLateClient(name)
	}
}

// registerLateClient registers one more late-feed client on a channel's newest late log entry and never releases
// it. Doing this after every delivery models stalled feeds parked at many positions, so late logs stay populated and
// the length cap has to force out entries that still have listeners.
func (h *fuzzHarness) registerLateClient(name string) {
	model := h.lateModel[name]
	newest := &model[len(model)-1]
	if got := h.pinned[name].RegisterLateSequenceClient(); got != newest.seq {
		h.failf("late log: registering on channel %s's newest entry returned %d, model expects %d (model %v)", name, got, newest.seq, model)
	}
	newest.listeners++
}

// resyncLateLogs brings the model back in line after the harness slept. The age-based prune runs on a timer, so it
// can only have fired during a sleep, and it only drops entries from the front, regardless of listeners. The cache's
// log must therefore be a suffix of the model's; the surviving entries keep their listener counts.
func (h *fuzzHarness) resyncLateLogs() {
	for name := range h.pinned {
		model, actual := h.lateModel[name], h.lateLog(name)
		if len(actual) > len(model) || len(actual) == 0 {
			h.failf("late log: after a sleep channel %s's late log is %v, which is not a suffix of the model's %v", name, actual, model)
		}
		model = model[len(model)-len(actual):]
		for i, seq := range actual {
			if model[i].seq != seq {
				h.failf("late log: after a sleep channel %s's late log is %v, which is not a suffix of the model's %v", name, actual, h.lateModel[name])
			}
		}
		h.lateModel[name] = model
	}
}

func (h *fuzzHarness) lateLog(name string) []uint64 {
	cache, ok := h.existingCache(name)
	if !ok {
		h.failf("channel cache %s disappeared", name)
	}
	impl, ok := db.AsSingleChannelCacheImplForTest(h.t, cache)
	if !ok {
		h.failf("channel cache %s is not a single channel cache", name)
	}
	return impl.LateLogSequencesForTest(h.t)
}

// lateAdds works out, from the sequences the cache reported leaving its skipped list during a delivery, which
// late sequences the delivery added to each channel cache's late log, in order. The cache reports every sequence that
// arrived late, but only some reach the channel caches, so each is classified by what the event carries:
//   - the document's own sequence is added to the channels the revision is active in or was removed from at that
//     sequence, and to "*";
//   - an older sequence replayed from recent_sequences is a removal entry if the document left a channel there,
//     which is added to those channels and "*", and otherwise an unused sequence, which adds nothing;
//   - the event's own unused sequences add nothing.
//
// The cache processes an event's unused sequences, then the replayed sequences in ascending order, then the own
// sequence, so the reported order must follow that.
func (h *fuzzHarness) lateAdds(ev *fuzzEvent, arrived []uint64) map[string][]uint64 {
	added := map[string][]uint64{}
	if ev.kind != eventDoc {
		// Only the event's own sequence can arrive: a range is released without going through the skipped removal.
		for _, seq := range arrived {
			if seq < ev.fromSeq || seq > ev.toSeq {
				h.failf("late log: delivering %s reported sequence %d leaving the skipped list, which it does not carry", ev.docID, seq)
			}
		}
		return added
	}
	rev := ev.rev
	lastRank, lastSeq := 0, uint64(0)
	for _, seq := range arrived {
		var names []string
		rank := 1
		switch {
		case seq == rev.seq:
			names, rank = rev.entriesPutBy(), 2
		case slices.Contains(rev.unusedSeqs, seq):
			rank = 0
		case slices.Contains(rev.recentSeqs, seq) && seq < rev.currentSequence():
			names = rev.removedAt(seq)
		default:
			h.failf("late log: delivering %s reported sequence %d leaving the skipped list, which the revision does not carry (recent %v, unused %v)",
				rev, seq, rev.recentSeqs, rev.unusedSeqs)
		}
		if rank < lastRank || (rank == lastRank && seq <= lastSeq) {
			h.failf("late log: delivering %s reported sequences %v leaving the skipped list out of processing order", rev, arrived)
		}
		lastRank, lastSeq = rank, seq
		for _, name := range names {
			if _, ok := h.pinned[name]; ok {
				added[name] = append(added[name], seq)
			}
		}
	}
	return added
}

// checkLateLog applies a delivery's late adds to the model of each late log, with the cache's own purge rule, and
// checks every channel's late log against it. On each add the cache drops entries from the front while more than one
// remains and either the front has no listener or the log is longer than the channel cache's maximum length (the
// leading sentinel counts). A listener does not protect an entry from the length cap. No time passes during a
// delivery, so the age-based prune cannot fire between the delivery and this check.
func (h *fuzzHarness) checkLateLog(ev *fuzzEvent) {
	h.t.Helper()
	arrived := h.takeSkippedRemoved()
	added := h.lateAdds(ev, arrived)
	for name, seqs := range added {
		log := h.lateModel[name]
		for _, seq := range seqs {
			log = append(log, fuzzLateEntry{seq: seq})
			for len(log) > 1 && (log[0].listeners == 0 || len(log) > h.cfg.ChannelMaxLen) {
				log = log[1:]
			}
		}
		h.lateModel[name] = log
	}
	for _, name := range allChannelNames {
		model, ok := h.lateModel[name]
		if !ok {
			continue
		}
		want := make([]uint64, len(model))
		for i, e := range model {
			want[i] = e.seq
		}
		if got := h.lateLog(name); !slices.Equal(got, want) {
			h.failf("late log: after delivering %s (late adds %v), channel %s's late log is %v, want %v (model %v)", ev.rev, added, name, got, want, model)
		}
	}
	for name := range h.pinned {
		h.registerLateClient(name)
	}
}

// checkEnd checks the end state, once everything has been delivered and the cache has timed out what it was waiting
// for. Every sequence the model allocated is accounted for, so the cache can be compared exactly.
func (h *fuzzHarness) checkEnd() {
	h.t.Helper()
	next, stable, skipped := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	if next != h.nextSeq {
		h.failf("end: nextSequence is %d, want %d", next, h.nextSeq)
	}
	if stable != h.nextSeq-1 {
		h.failf("end: high_seq_stable is %d, want %d", stable, h.nextSeq-1)
	}
	if len(skipped) != 0 {
		h.failf("end: skipped list is not empty: %v", skipped)
	}

	snaps := h.snapshots()
	for _, name := range allChannelNames {
		snap, ok := snaps[name]
		if !ok {
			continue
		}
		h.checkEndCache(snap)
		h.endCaches = append(h.endCaches, snap.String())
	}
	for i, feed := range h.feeds {
		h.checkEndFeed(i, feed)
	}
}

// checkEndCache compares one channel cache with the bucket's final state. A document whose final state sits at an
// abandoned sequence is skipped: the cache never saw that write, so it can legitimately hold an older state of it or
// none.
func (h *fuzzHarness) checkEndCache(snap *fuzzCacheSnapshot) {
	h.t.Helper()
	for _, e := range snap.entries {
		if e.Sequence < snap.validFrom {
			h.failf("I3: channel %s holds %s for %s below validFrom %d\ncache: %s", snap.name, describeEntry(e), e.DocID, snap.validFrom, snap)
		}
	}
	for docIndex := range h.model.revs {
		latest := h.model.latestRev(docIndex)
		if latest == nil {
			continue
		}
		got := snap.entryFor(latest.docID)
		want, inChannel := latest.channelState(snap.name)
		if inChannel && h.model.abandoned[want.seq] {
			continue
		}
		switch {
		case !inChannel && got != nil:
			h.failf("end: channel %s holds %s for %s, which was never in it\ncache: %s", snap.name, describeEntry(got), latest.docID, snap)
		case inChannel && want.seq >= snap.validFrom && !want.matches(latest.docID, got):
			h.failf("end: channel %s (validFrom %d) holds %s for %s, want %s\ncache: %s", snap.name, snap.validFrom, describeEntry(got), latest.docID, want, snap)
		case inChannel && want.seq < snap.validFrom && got != nil:
			h.failf("end: channel %s (validFrom %d) holds %s for %s, whose final state %s is below validFrom\ncache: %s", snap.name, snap.validFrom, describeEntry(got), latest.docID, want, snap)
		}
	}
}

// checkEndFeed checks what a feed was sent: no entry twice, and for every document its last entry in each channel is
// the final state, unless the final state is at an abandoned sequence.
func (h *fuzzHarness) checkEndFeed(feedIndex int, feed *fuzzFeed) {
	h.t.Helper()
	type key struct {
		channel string
		seq     uint64
	}
	seen := map[key]bool{}
	last := map[string]map[string]fuzzReceived{}
	for _, r := range feed.received {
		k := key{r.channel, r.seq}
		if seen[k] {
			h.failf("feed %d received channel %s sequence %d twice", feedIndex, r.channel, r.seq)
		}
		seen[k] = true
		if last[r.channel] == nil {
			last[r.channel] = map[string]fuzzReceived{}
		}
		last[r.channel][r.docID] = r
	}
	for _, name := range feed.channels {
		for docIndex := range h.model.revs {
			latest := h.model.latestRev(docIndex)
			if latest == nil {
				continue
			}
			want, inChannel := latest.channelState(name)
			if !inChannel || h.model.abandoned[want.seq] {
				continue
			}
			got, received := last[name][latest.docID]
			if !received || got.seq != want.seq || got.revID != want.revID || got.removed != want.removed {
				h.failf("feed %d (channels %v): last entry received in channel %s for %s is %v, want %s", feedIndex, feed.channels, name, latest.docID, got, want)
			}
		}
	}
}

// fuzzMaxNumChannels is the most channel caches the cache will create. The harness uses far fewer channels, so a
// channel cache is always created; a bypass cache would only be legitimate if a future script could reach this limit.
var fuzzMaxNumChannels = db.DefaultCacheOptions().MaxNumChannels

// checkCacheExists is P-exists: a channel cache must exist after the harness creates one or reads from it, as long as the
// harness cannot reach the cache's channel limit. Every other property only looks at caches that exist, so a cache that
// silently serves everything as a bypass cache would otherwise pass them all.
func (h *fuzzHarness) checkCacheExists(name, what string) {
	h.t.Helper()
	if len(allChannelNames) >= fuzzMaxNumChannels {
		return
	}
	if h.snapshot(name) == nil {
		h.failf("P-exists: no channel cache for %s after %s", name, what)
	}
}

// checkStructure checks the shape of every channel cache after every op:
//   - P-len: it holds at most ChannelCacheMaxLength entries;
//   - P-order: its entries are in strictly ascending sequence order with at most one per document, which the length
//     prune and the late insert rely on;
//   - P-mono: its validFrom only rises when entries were pruned, either by length (the cache is then at its maximum)
//     or by age (which only happens while time passes), never because an entry was added.
func (h *fuzzHarness) checkStructure(next uint64, snaps map[string]*fuzzCacheSnapshot) {
	h.t.Helper()
	slept := h.slept
	h.slept = false
	if h.lastValidFrom == nil {
		h.lastValidFrom = map[string]uint64{}
	}
	for name, snap := range snaps {
		if len(snap.entries) > h.cfg.ChannelMaxLen {
			h.failf("P-len: channel %s holds %d entries, more than the maximum %d\ncache: %s", name, len(snap.entries), h.cfg.ChannelMaxLen, snap)
		}
		docs := make(map[string]bool, len(snap.entries))
		for i, e := range snap.entries {
			if i > 0 && e.Sequence <= snap.entries[i-1].Sequence {
				h.failf("P-order: channel %s has %s after sequence %d\ncache: %s", name, describeEntry(e), snap.entries[i-1].Sequence, snap)
			}
			if docs[e.DocID] {
				h.failf("P-order: channel %s holds %s twice\ncache: %s", name, e.DocID, snap)
			}
			docs[e.DocID] = true
		}
		if prev, ok := h.lastValidFrom[name]; ok && snap.validFrom > prev && len(snap.entries) != h.cfg.ChannelMaxLen && !slept {
			h.failf("P-mono: channel %s validFrom rose from %d to %d without a prune (nextSequence %d)\ncache: %s", name, prev, snap.validFrom, next, snap)
		}
		h.lastValidFrom[name] = snap.validFrom
	}
}

// checkSequenceViews checks that the cache's views of how far it has got agree: the channel caches' high sequence and
// LastSequence, which reads the copy of nextSequence published without the cache lock, both equal nextSequence-1 once
// every op has finished. A sequence is only ever processed at nextSequence, or skipped by moving nextSequence past it
// when the entry behind it is processed, so the highest processed sequence is always nextSequence-1.
func (h *fuzzHarness) checkSequenceViews(next uint64) {
	h.t.Helper()
	high, last := h.channelCache.GetHighCacheSequence(), h.cache.LastSequence()
	if high != next-1 || last != next-1 {
		h.failf("sequence views disagree: nextSequence %d, channel cache high sequence %d, LastSequence %d (want %d for both)", next, high, last, next-1)
	}
}

// checkReadServedBy is P-hit and P-warm for one feed read of a channel: a read goes to the view exactly when the cache
// that existed before it was not valid from the read's first sequence, and an unlimited read leaves the cache valid from
// there unless it is full. P-warm relies on the harness never setting ActiveOnly, which makes a read skip the backfill.
func (h *fuzzHarness) checkReadServedBy(name string, since uint64, limit int, before *fuzzCacheSnapshot, queried bool) {
	h.t.Helper()
	if before != nil {
		if want := before.validFrom > since+1; queried != want {
			h.failf("P-hit: feed read of %s since %d with the cache valid from %d queried the view: %v, want %v", name, since, before.validFrom, queried, want)
		}
	}
	after := h.snapshot(name)
	if after != nil && limit == 0 && len(after.entries) < h.cfg.ChannelMaxLen && after.validFrom > since+1 {
		h.failf("P-warm: unlimited feed read of %s since %d left the cache (%d of %d entries) valid only from %d\ncache: %s",
			name, since, len(after.entries), h.cfg.ChannelMaxLen, after.validFrom, after)
	}
}

// checkNoNeedlessWait is called right after a delivery, before any time has passed. The cache releases contiguous
// sequences as they arrive, so a feed never waits for the pending time-out on a sequence the cache has already been
// given: once every sequence from the previous nextSequence up to N has been delivered or replayed from a
// recent_sequences list, nextSequence must have reached N+1. Only a sequence that really is missing may hold it back.
//
// A sequence counts as given if a delivery claimed it. Dropped and abandoned sequences are never claimed, so they hold
// nextSequence back, which is right: the cache has not been given them. A sequence at or above the previous nextSequence
// cannot be in the skipped list, so a claimed one is always either processed or waiting behind a gap. Principals and
// unused ranges are claimed like any other sequence, a Duplicate delivery claims nothing new, and the pending overflow
// can only move nextSequence further, so the lower bound holds in each case.
func (h *fuzzHarness) checkNoNeedlessWait(nextBefore uint64) {
	h.t.Helper()
	next, _, skipped := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	// failf must not run with notifyLock held, so read the claims first.
	lowestUnclaimed, waiting := func() (uint64, []uint64) {
		h.notifyLock.Lock()
		defer h.notifyLock.Unlock()
		claimed := func(seq uint64) bool {
			_, ok := h.notify.claims[seq]
			return ok
		}
		lowest := nextBefore
		for ; lowest < h.nextSeq && claimed(lowest); lowest++ {
		}
		var waiting []uint64
		for seq := max(nextBefore, next); seq < h.nextSeq; seq++ {
			if claimed(seq) {
				waiting = append(waiting, seq)
			}
		}
		return lowest, waiting
	}()
	if next < lowestUnclaimed {
		h.failf("no needless wait: nextSequence is %d but every sequence below %d has been given to the cache (claimed but unprocessed: %v, skipped %v)", next, lowestUnclaimed, waiting, skipped)
	}
}

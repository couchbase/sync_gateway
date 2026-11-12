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

// Notification properties. A feed waiting on a channel is woken by a notification naming that channel, so the cache
// must name a channel when something readable has appeared in it (N2: no lost wake) and only then (N1: every wake
// has a reason). They are stated against what the cache reports as readable, so they hold whether the cache notifies
// as it caches or only when notifications are flushed.
//
// A change is a (key, sequence) pair, where the key is a channel name, "*", or the key the cache uses for unused
// sequences. A sequence is readable once it has been processed: below nextSequence and not in the skipped list. A
// notification covers every readable change for the keys it names that no earlier notification covered.

// fuzzCheckUnusedKeyWakes says whether N1 and N2 apply to the unused-sequence key.
// TODO: CBG-6148 enable once the unused-sequence key is notified for the entry that becomes readable rather than for the arriving event
const fuzzCheckUnusedKeyWakes = false

// fuzzRoute is how a sequence reaches the channel caches once it is processed, which decides what it changes.
type fuzzRoute uint8

const (
	routeNone   fuzzRoute = iota // a principal: it only advances the sequence
	routeOwn                     // a document's own sequence: the channels it is in or left at that sequence, and "*"
	routeReplay                  // an older sequence replayed from recent_sequences: the channels left at it, and "*"
	routeUnused                  // an unused sequence or a member of an unused range
	routeBurned                  // a sequence a document write wasted, processed as an unused entry
)

// fuzzClaim records the first delivery that pushed a sequence into the cache. The cache ignores a sequence it has
// already been given, so the first route is the one that counts.
type fuzzClaim struct {
	route fuzzRoute
	rev   *fuzzRev // the event's revision, for routeOwn and routeReplay
}

// changeKeys lists the keys a processed sequence changes.
func (c fuzzClaim) changeKeys(seq uint64, unusedKey string) []string {
	switch c.route {
	case routeOwn:
		return c.rev.entriesPutBy()
	case routeReplay:
		if names := c.rev.removedAt(seq); len(names) > 0 {
			return names
		}
		// Not a removal, so the cache processes the replay as an unused entry.
		return []string{unusedKey}
	case routeUnused, routeBurned:
		return []string{unusedKey}
	}
	return nil
}

// fuzzNotification is one call of the cache's notify callback.
type fuzzNotification struct {
	keys    []string
	covered []int // per key, how many changes it newly covered
}

func (n fuzzNotification) String() string {
	parts := make([]string, len(n.keys))
	for i, k := range n.keys {
		parts[i] = fmt.Sprintf("%s:+%d", k, n.covered[i])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// fuzzNotifyState is the notification tracker, guarded by fuzzHarness.notifyLock. The notify callback runs on
// whichever goroutine caches entries, including the cache's background task, so everything it reads or writes is
// kept here.
type fuzzNotifyState struct {
	claims    map[uint64]fuzzClaim
	uncovered map[string][]uint64 // key -> readable changes no notification has covered
	nextSeen  uint64              // nextSequence at the last refresh
	skipped   []uint64            // the skipped list at the last refresh
	pending   []fuzzNotification  // notifications since the trace last drained them
	violation string              // the first N1 violation, reported at the end of the step
}

// claimSequences records, before ev is delivered, the sequences it pushes into the cache for the first time and how.
// The cache processes an event's unused sequences, then the sequences it replays from recent_sequences, then the own
// sequence. A replayed sequence is only pushed if it is not yet processed, and every processed sequence has been
// claimed by an earlier delivery, so an unclaimed one is either pending-to-be or skipped and is pushed now.
func (h *fuzzHarness) claimSequences(ev *fuzzEvent) {
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	st := &h.notify
	if st.claims == nil {
		st.claims = map[uint64]fuzzClaim{}
		st.uncovered = map[string][]uint64{}
		st.nextSeen = 1
	}
	claim := func(seq uint64, c fuzzClaim) {
		if _, ok := st.claims[seq]; ok || h.model.abandoned[seq] {
			return
		}
		st.claims[seq] = c
	}
	switch ev.kind {
	case eventDoc:
		rev := ev.rev
		for _, seq := range rev.unusedSeqs {
			claim(seq, fuzzClaim{route: routeBurned})
		}
		for _, seq := range rev.recentSeqs {
			if seq < rev.currentSequence() {
				claim(seq, fuzzClaim{route: routeReplay, rev: rev})
			}
		}
		claim(rev.seq, fuzzClaim{route: routeOwn, rev: rev})
	case eventUnused, eventUnusedRange:
		for seq := ev.fromSeq; seq <= ev.toSeq; seq++ {
			claim(seq, fuzzClaim{route: routeUnused})
		}
	case eventPrincipal:
		claim(ev.fromSeq, fuzzClaim{route: routeNone})
	}
}

// refreshLocked records the changes that became readable since the last refresh. Sequences newly below nextSequence
// and not skipped, and sequences that left the skipped list, have been processed. A sequence that left the skipped list
// without ever being claimed was abandoned, not processed.
func (h *fuzzHarness) refreshLocked(next uint64, skipped []uint64) {
	st := &h.notify
	if st.claims == nil {
		st.claims = map[uint64]fuzzClaim{}
		st.uncovered = map[string][]uint64{}
		st.nextSeen = 1
	}
	unusedKey := db.UnusedSeqChannelIDForTest().Name
	readable := func(seq uint64) {
		c, ok := st.claims[seq]
		if !ok {
			return
		}
		for _, key := range c.changeKeys(seq, unusedKey) {
			st.uncovered[key] = append(st.uncovered[key], seq)
		}
	}
	for seq := st.nextSeen; seq < next; seq++ {
		if !slices.Contains(skipped, seq) {
			readable(seq)
		}
	}
	for _, seq := range st.skipped {
		if !slices.Contains(skipped, seq) {
			readable(seq)
		}
	}
	st.nextSeen = max(st.nextSeen, next)
	st.skipped = slices.Clone(skipped)
}

// trackNotification is the cache's notify callback as far as the properties go. It runs without any cache lock held,
// so it can read the cache's state.
func (h *fuzzHarness) trackNotification(chans channels.Set) {
	next, _, skipped := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	unusedID := db.UnusedSeqChannelIDForTest()
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	h.refreshLocked(next, skipped)
	st := &h.notify
	n := fuzzNotification{}
	for id := range chans {
		n.keys = append(n.keys, id.Name)
	}
	slices.Sort(n.keys)
	wasted := ""
	for _, key := range n.keys {
		newly := len(st.uncovered[key])
		delete(st.uncovered, key)
		n.covered = append(n.covered, newly)
		isUnused := key == unusedID.Name
		if newly == 0 && wasted == "" && (!isUnused || fuzzCheckUnusedKeyWakes) {
			wasted = key
		}
	}
	if wasted != "" && st.violation == "" {
		st.violation = fmt.Sprintf("N1: a notification named %s, but nothing readable in it was uncovered (nextSequence %d, skipped %v); notification %s", wasted, next, skipped, n)
	}
	st.pending = append(st.pending, n)
}

// drainNotifications hands the notifications since the last drain to the trace.
func (h *fuzzHarness) drainNotifications() []fuzzNotification {
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	out := h.notify.pending
	h.notify.pending = nil
	return out
}

// takeViolation returns the first N1 violation seen since the run began.
func (h *fuzzHarness) takeViolation() string {
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	return h.notify.violation
}

// refreshFromCache updates the readable set from the cache's current state.
func (h *fuzzHarness) refreshFromCache() {
	next, _, skipped := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	h.refreshStepEnd(next, skipped)
}

// refreshStepEnd updates the readable set at the end of a step, so changes made readable without any notification
// are tracked too.
func (h *fuzzHarness) refreshStepEnd(next uint64, skipped []uint64) {
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	h.refreshLocked(next, skipped)
}

// checkNoLostWakes is N2: every readable change has been covered by a notification fired since it became readable.
func (h *fuzzHarness) checkNoLostWakes() {
	h.t.Helper()
	h.refreshFromCache()
	// failf must not run with notifyLock held, so collect the lost changes first.
	lost := func() []string {
		h.notifyLock.Lock()
		defer h.notifyLock.Unlock()
		var lost []string
		for key, seqs := range h.notify.uncovered {
			if len(seqs) > 0 && (fuzzCheckUnusedKeyWakes || key != db.UnusedSeqChannelIDForTest().Name) {
				lost = append(lost, fmt.Sprintf("%s%v", key, seqs))
			}
		}
		return lost
	}()
	if len(lost) > 0 {
		slices.Sort(lost)
		h.failf("N2: readable changes no notification has covered since they became readable: %s", strings.Join(lost, " "))
	}
}

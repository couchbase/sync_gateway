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
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

const (
	fuzzPendingSeqMaxWait = 5 * time.Second
	fuzzSkippedSeqMaxWait = 10 * time.Minute

	// fuzzMaxRecentSequences and fuzzMinRecentSequences mirror the pruning thresholds a write applies to a
	// document's recent_sequences.
	fuzzMaxRecentSequences = 20
	fuzzMinRecentSequences = 5
)

// fuzzCache is the part of the change cache the harness drives. The cache type itself is unexported in
// package db, so the harness holds it through this interface.
type fuzzCache interface {
	DocChanged(event sgbucket.FeedEvent, docType db.DocumentType)
	ProcessPrincipalDocForTest(t testing.TB, ctx context.Context, docID string, docJSON []byte, isUser bool, timeReceived channels.FeedTimestamp)
	SetManualNotifyChangeFuncForTest(t testing.TB, notify func(context.Context, channels.Set))
	FlushNotificationsForTest(t testing.TB)
	GetNextSequenceForTest(t testing.TB) uint64
	GetMaxStableCachedForTest(t testing.TB, ctx context.Context) uint64
	SkippedSeqsForTest(t testing.TB) *db.SkippedSequenceSkiplist
	SequenceStateForTest(t testing.TB, ctx context.Context) (nextSequence, maxStable uint64, skipped []uint64)
	Start(initialSequence uint64) error
	Stop(ctx context.Context)
}

// fuzzEnv is the state shared by every script run: one database context, built once because creating it
// is far too slow to repeat per input.
type fuzzEnv struct {
	dbContext    *db.DatabaseContext
	ctx          context.Context
	collectionID uint32
}

func newFuzzEnv(tb testing.TB) *fuzzEnv {
	ctx := base.TestCtx(tb)
	bucket := base.GetTestBucket(tb)
	dbContext, err := db.NewDatabaseContext(ctx, "db", bucket, false, db.DatabaseContextOptions{
		Scopes: db.GetScopesOptions(tb, bucket, 1),
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { dbContext.Close(ctx) })

	ctx = dbContext.AddDatabaseLogContext(ctx)
	require.NoError(tb, dbContext.StartOnlineProcesses(ctx))
	return &fuzzEnv{
		dbContext:    dbContext,
		ctx:          ctx,
		collectionID: db.GetSingleDatabaseCollection(tb, dbContext).GetCollectionID(),
	}
}

// fuzzEventKind says what a simulated feed event carries.
type fuzzEventKind uint8

const (
	eventDoc fuzzEventKind = iota
	eventUnused
	eventUnusedRange
	eventPrincipal
)

// fuzzEvent is one mutation the simulated DCP feed can deliver. It is built when its sequences are
// allocated and delivered, possibly more than once, later.
type fuzzEvent struct {
	kind     fuzzEventKind
	fromSeq  uint64 // first (or only) sequence the event carries
	toSeq    uint64 // last sequence; equal to fromSeq except for a range
	docID    string
	feed     sgbucket.FeedEvent // eventDoc, eventUnused and eventUnusedRange
	rev      *fuzzRev           // eventDoc: the write this event carries
	json     []byte             // eventPrincipal body
	isUser   bool               // eventPrincipal
	delivery int                // times delivered so far
}

// fuzzDoc tracks what a document looked like at its last write, to build the next write's sync data.
type fuzzDoc struct {
	generation int
	recentSeqs []uint64
	channels   channels.ChannelMap // active channels map to nil; channels it left map to their removal
}

// fuzzFeed is a simulated changes feed reading a fixed set of channels. It keeps a since per channel and only
// ever advances it to the stable sequence, so it relies on the cache never adding an entry at or below a
// stable sequence it has reported. This is a simplification of the harness: a real feed also reads above the stable
// sequence, and the cache's invariants do not depend on this feed's bound.
type fuzzFeed struct {
	channels []string
	since    map[string]uint64
	received []fuzzReceived // everything received, in receipt order
}

// fuzzHarness drives a real change cache from a script. It is built and used only inside a synctest
// bubble, so the cache's background tasks run on fake time.
type fuzzHarness struct {
	t            *testing.T
	env          *fuzzEnv
	cfg          fuzzConfig
	cache        fuzzCache
	channelCache *db.ChannelCacheImplForTest
	feeds        [fuzzNumFeeds]*fuzzFeed

	nextSeq     uint64 // next sequence the simulated server will allocate
	docs        [fuzzNumDocs]*fuzzDoc
	undelivered []*fuzzEvent // oldest first
	withheld    []*fuzzEvent // oldest first
	delivered   []*fuzzEvent // in first-delivery order, one entry per event
	principals  int

	model        *fuzzModel
	ops          []fuzzOp // the script being run, for failure messages
	opIndex      int
	curOp        fuzzOp
	phase        string // set for the harness's own steps after the ops, which have no op
	trace        []fuzzTraceStep
	stepEvent    *fuzzEvent // the event the current step allocated, withheld or delivered
	stepNote     string     // why the current step did nothing, if it did
	stepDelivery int        // how many times stepEvent has been delivered, counting this step
	stopped      bool
	armed        *fuzzQuery                       // the cache state a feed read started from, which the query it triggers records
	pinned       map[string]db.SingleChannelCache // channel caches with late clients registered; see pinLateLogs
	lateModel    map[string][]fuzzLateEntry       // the late log the harness expects each pinned cache to hold
	lastNext     uint64                           // nextSequence at the previous check
	lastStable   uint64                           // max stable sequence at the previous check

	endCaches []string // each channel cache's contents after the end-state checks

	skippedLock    sync.Mutex
	skippedRemoved []uint64 // sequences that left the skipped list on arrival since the last reset, in order

	notifyLock    sync.Mutex
	notifications []string        // each notification's channel names, sorted and comma-joined, in arrival order
	notify        fuzzNotifyState // guarded by notifyLock
}

// fuzzResult is the cache state at the end of a run. Comparing two runs of one script checks the harness
// is deterministic.
type fuzzResult struct {
	NextSequence  uint64
	MaxStable     uint64
	Skipped       [][2]uint64
	Notifications []string
	Abandoned     []uint64
	Caches        []string               // each existing channel cache's contents at the end of the run
	Feeds         [fuzzNumFeeds][]string // everything each feed received
}

func newFuzzHarness(t *testing.T, env *fuzzEnv, cfg fuzzConfig) *fuzzHarness {
	h := &fuzzHarness{t: t, env: env, cfg: cfg, nextSeq: 1, model: newFuzzModel(), lastNext: 1, pinned: map[string]db.SingleChannelCache{}, lateModel: map[string][]fuzzLateEntry{}}
	for i := range h.feeds {
		h.feeds[i] = &fuzzFeed{channels: cfg.feedChannels(i), since: map[string]uint64{}}
	}
	return h
}

// start builds the channel cache and change cache for this run.
func (h *fuzzHarness) start() {
	t, ctx := h.t, h.env.ctx
	options := db.DefaultCacheOptions()
	options.CachePendingSeqMaxNum = h.cfg.PendingMaxNum
	options.CachePendingSeqMaxWait = fuzzPendingSeqMaxWait
	options.CacheSkippedSeqMaxWait = fuzzSkippedSeqMaxWait
	options.ChannelCacheMaxLength = h.cfg.ChannelMaxLen

	// A channel cache created after sequences were cached backfills from the bucket's channel view, which
	// the model answers from every write, delivered or not.
	queryHandler := &db.QueryHandlerForTest{}
	queryHandler.SetQueryCallback(func(channel string, startSeq, endSeq uint64, limit int, activeOnly bool) (db.LogEntries, error) {
		if h.armed == nil {
			h.failf("channel view query for %s [%d,%d] outside a feed read", channel, startSeq, endSeq)
		}
		q := *h.armed
		q.channel, q.startSeq, q.endSeq, q.allocated = channel, startSeq, endSeq, h.nextSeq
		h.model.queries = append(h.model.queries, q)
		return h.model.channelView(h.env.collectionID, channel, startSeq, endSeq, limit, activeOnly), nil
	})
	channelCache, err := db.NewChannelCacheForTest(t, ctx, "db", options.ChannelCacheOptions, queryHandler.AsFactory,
		channels.NewActiveChannels(h.env.dbContext.DbStats.Cache().NumActiveChannels), h.env.dbContext.DbStats.Cache())
	require.NoError(t, err)
	h.channelCache = channelCache

	cache := db.NewChangeCacheForTest(t)
	require.NoError(t, cache.Init(ctx, h.env.dbContext, channelCache, nil, &options, h.env.dbContext.MetadataKeys))
	// Init leaves the cache locked until Start, and its background tasks take that lock before they read the
	// notify callback, so installing it here is ordered before every read. After Start it would race them.
	cache.SetManualNotifyChangeFuncForTest(t, h.recordNotification)
	// The hook runs under the same lock, so it is installed here for the same reason.
	cache.SetRemoveSkippedHookForTest(t, h.recordSkippedRemoved)
	require.NoError(t, cache.Start(0))
	h.cache = cache

	if h.cfg.starChannel() {
		h.createChannelCache(channels.UserStarChannel)
	}
}

// stop shuts down everything start created, so no goroutine outlives the bubble. It is safe to call twice, since a
// failed check unwinds through a deferred stop.
func (h *fuzzHarness) stop() {
	if h.stopped || h.cache == nil {
		return
	}
	h.stopped = true
	h.cache.Stop(h.env.ctx)
	h.channelCache.Stop(h.env.ctx)
}

func (h *fuzzHarness) recordNotification(_ context.Context, chans channels.Set) {
	names := make([]string, 0, len(chans))
	for id := range chans {
		names = append(names, id.Name)
	}
	slices.Sort(names)
	func() {
		h.notifyLock.Lock()
		defer h.notifyLock.Unlock()
		h.notifications = append(h.notifications, strings.Join(names, ","))
	}()
	// trackNotification takes notifyLock itself, after reading the cache's state without it held.
	h.trackNotification(chans)
}

// recordSkippedRemoved is the cache's hook for a sequence leaving the skipped list on arrival. It is called from
// cache code with the cache's lock held, so it only records.
func (h *fuzzHarness) recordSkippedRemoved(seq uint64) {
	h.skippedLock.Lock()
	defer h.skippedLock.Unlock()
	h.skippedRemoved = append(h.skippedRemoved, seq)
}

func (h *fuzzHarness) resetSkippedRemoved() {
	h.skippedLock.Lock()
	defer h.skippedLock.Unlock()
	h.skippedRemoved = nil
}

func (h *fuzzHarness) takeSkippedRemoved() []uint64 {
	h.skippedLock.Lock()
	defer h.skippedLock.Unlock()
	return slices.Clone(h.skippedRemoved)
}

func (h *fuzzHarness) channelID(name string) channels.ID {
	return channels.NewID(name, h.env.collectionID)
}

// result reads the end state. Call after stop, so the unlocked reads cannot race a background task.
func (h *fuzzHarness) result() fuzzResult {
	res := fuzzResult{
		NextSequence: h.cache.GetNextSequenceForTest(h.t),
		MaxStable:    h.cache.GetMaxStableCachedForTest(h.t, h.env.ctx),
	}
	for e := h.cache.SkippedSeqsForTest(h.t).ListForTest(h.t).Front(); e != nil; e = e.Next() {
		res.Skipped = append(res.Skipped, [2]uint64{e.Key().Start, e.Key().End})
	}
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	res.Notifications = slices.Clone(h.notifications)
	res.Abandoned = slices.Sorted(maps.Keys(h.model.abandoned))
	res.Caches = h.endCaches
	for i, feed := range h.feeds {
		for _, r := range feed.received {
			res.Feeds[i] = append(res.Feeds[i], r.String())
		}
	}
	return res
}

func (h *fuzzHarness) allocSeq() uint64 {
	seq := h.nextSeq
	h.nextSeq++
	return seq
}

// run executes ops in order, checking the cache's invariants after each, then settles the run and checks the
// end state.
func (h *fuzzHarness) run(ops []fuzzOp) {
	h.ops = ops
	for i, op := range ops {
		h.beginStep(i, op, "")
		h.step(op)
		h.checkAfterOp()
	}
	h.quiesce()
	h.checkEnd()
}

// sleep passes fake time so the cache's background tasks run, and records what they abandoned. Nothing is delivered
// while the harness sleeps, so a sequence that leaves the skipped list across a sleep was given up on by the cache's
// skipped-sequence cleanup, which ticks on its own timer whichever op is sleeping.
func (h *fuzzHarness) sleep(d time.Duration) {
	_, _, before := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	time.Sleep(d)
	synctest.Wait()
	_, _, after := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	h.resyncLateLogs()
	for _, seq := range before {
		if !slices.Contains(after, seq) {
			h.model.abandoned[seq] = true
		}
	}
}

// timeoutPending lets the cache's own InsertPendingEntries task run when fake time passes its deadline. The
// extra nanosecond keeps the harness's wake-up off the task's tick instant.
func (h *fuzzHarness) timeoutPending() {
	h.sleep(fuzzPendingSeqMaxWait + time.Nanosecond)
}

// cleanSkipped sleeps long enough for the cache to abandon skipped sequences that have waited the full max wait.
func (h *fuzzHarness) cleanSkipped() {
	h.sleep(fuzzSkippedSeqMaxWait + time.Nanosecond)
}

// quiesce delivers everything still outstanding, oldest first, then lets the cache time out whatever is left
// pending and has every feed read once more.
func (h *fuzzHarness) quiesce() {
	for len(h.undelivered) > 0 {
		ev := h.undelivered[0]
		h.undelivered = h.undelivered[1:]
		h.beginStep(len(h.ops), fuzzOp{}, "quiesce: deliver undelivered")
		h.deliver(ev)
		h.checkAfterOp()
	}
	for len(h.withheld) > 0 {
		ev := h.withheld[0]
		h.withheld = h.withheld[1:]
		h.beginStep(len(h.ops), fuzzOp{}, "quiesce: deliver withheld")
		h.deliver(ev)
		h.checkAfterOp()
	}
	h.beginStep(len(h.ops), fuzzOp{}, "quiesce: time out pending")
	h.timeoutPending()
	h.checkAfterOp()
	for i := range h.feeds {
		h.beginStep(len(h.ops), fuzzOp{}, fmt.Sprintf("quiesce: feed %d reads", i))
		h.feedRead(i)
		h.checkAfterOp()
	}
	h.beginStep(len(h.ops), fuzzOp{}, "quiesce: flush notifications")
	h.cache.FlushNotificationsForTest(h.t)
	h.checkAfterOp()
	h.checkNoLostWakes()
}

func (h *fuzzHarness) step(op fuzzOp) {
	switch op.Code {
	case opWrite:
		h.write(op.writeDoc(), op.writeMask(), op.writeExtraSeq())
		h.stepEvent = h.undelivered[len(h.undelivered)-1]
	case opDeliver:
		if len(h.undelivered) == 0 {
			h.stepNote = "nothing undelivered"
		}
		h.deliverOne(op.deliverWindow(), op.deliverPick())
	case opWithhold:
		if len(h.undelivered) == 0 {
			h.stepNote = "nothing undelivered"
			break
		}
		h.stepEvent = h.undelivered[0]
		h.withheld = append(h.withheld, h.undelivered[0])
		h.undelivered = h.undelivered[1:]
	case opDeliverLate:
		if len(h.withheld) == 0 {
			h.stepNote = "nothing withheld"
			break
		}
		ev := h.withheld[0]
		h.withheld = h.withheld[1:]
		h.deliver(ev)
	case opDuplicate:
		if len(h.delivered) == 0 {
			h.stepNote = "nothing delivered yet"
			break
		}
		h.deliver(h.delivered[int(op.Args[0])%len(h.delivered)])
	case opUnused:
		h.unused()
		h.stepEvent = h.undelivered[len(h.undelivered)-1]
	case opUnusedRange:
		h.unusedRange(op.unusedRangeLen())
		h.stepEvent = h.undelivered[len(h.undelivered)-1]
	case opPrincipal:
		h.principal(op.principalIsUser())
		h.stepEvent = h.undelivered[len(h.undelivered)-1]
	case opTimeoutPending:
		h.timeoutPending()
	case opCleanSkipped:
		h.cleanSkipped()
	case opCreateCache:
		h.createChannelCache(fuzzChannelNames[op.channelIndex()])
	case opFeedRead:
		h.feedRead(op.feedIndex())
	case opFlush:
		h.cache.FlushNotificationsForTest(h.t)
		h.checkNoLostWakes()
	case opStall, opNop:
		// opStall is RESERVED and opNop is what unknown opcodes decode to.
	}
}

// write allocates the next sequence for a document and queues the feed event for it.
func (h *fuzzHarness) write(docIndex int, mask uint8, extraSeq bool) {
	doc := h.docs[docIndex]
	if doc == nil {
		doc = &fuzzDoc{channels: channels.ChannelMap{}}
		h.docs[docIndex] = doc
	}

	// A write that wastes a sequence (a conflict) allocates it ahead of the one the document lands on.
	var unusedSeqs []uint64
	if extraSeq {
		unusedSeqs = append(unusedSeqs, h.allocSeq())
	}
	seq := h.allocSeq()

	doc.generation++
	revID := fmt.Sprintf("%d-%032x", doc.generation, seq)

	// Channels the document was in and no longer is get a removal entry at this sequence; channels it
	// left earlier keep their older removal, and channels it joins (or rejoins) become active again.
	wanted := channelsInMask(mask)
	for name, entry := range doc.channels {
		if entry == nil && !slices.Contains(wanted, name) {
			doc.channels[name] = &channels.ChannelRemoval{Seq: seq, Rev: channels.RevAndVersion{RevTreeID: revID}}
		}
	}
	for _, name := range wanted {
		doc.channels[name] = nil
	}

	// Mirrors how a write prunes and extends recent_sequences.
	if len(doc.recentSeqs) >= fuzzMaxRecentSequences {
		doc.recentSeqs = slices.Clone(doc.recentSeqs[len(doc.recentSeqs)-fuzzMinRecentSequences:])
	}
	doc.recentSeqs = append(doc.recentSeqs, unusedSeqs...)
	doc.recentSeqs = append(doc.recentSeqs, seq)
	slices.Sort(doc.recentSeqs)

	docID := fmt.Sprintf("doc%d", docIndex)
	rev := &fuzzRev{
		docIndex:   docIndex,
		docID:      docID,
		generation: doc.generation,
		seq:        seq,
		revID:      revID,
		channels:   cloneChannelMap(doc.channels),
		recentSeqs: slices.Clone(doc.recentSeqs),
		unusedSeqs: unusedSeqs,
	}
	h.model.revs[docIndex] = append(h.model.revs[docIndex], rev)
	h.model.seqs[seq] = fuzzSeqRecord{kind: eventDoc, rev: rev}
	for _, unusedSeq := range unusedSeqs {
		h.model.seqs[unusedSeq] = fuzzSeqRecord{kind: eventUnused, burnedBy: rev}
	}
	syncData := db.SyncData{
		RevAndVersion:   channels.RevAndVersion{RevTreeID: revID},
		Sequence:        seq,
		UnusedSequences: unusedSeqs,
		RecentSequences: slices.Clone(doc.recentSeqs),
		Channels:        cloneChannelMap(doc.channels),
		History:         db.RevTree{revID: &db.RevInfo{ID: revID, Channels: base.SetOf(wanted...)}},
		TimeSaved:       time.Now(),
	}
	h.undelivered = append(h.undelivered, &fuzzEvent{
		kind:    eventDoc,
		fromSeq: seq,
		toSeq:   seq,
		docID:   docID,
		rev:     rev,
		feed:    feedEventForTest(h.t, docID, h.env.collectionID, syncData),
	})
}

// cloneChannelMap copies a channel map so later writes can't change the sync data of an event that is
// still waiting to be delivered.
func cloneChannelMap(m channels.ChannelMap) channels.ChannelMap {
	out := make(channels.ChannelMap, len(m))
	for k, v := range m {
		if v != nil {
			removal := *v
			v = &removal
		}
		out[k] = v
	}
	return out
}

func (h *fuzzHarness) unused() {
	seq := h.allocSeq()
	h.model.seqs[seq] = fuzzSeqRecord{kind: eventUnused}
	key := h.env.dbContext.MetadataKeys.UnusedSeqKey(seq)
	h.undelivered = append(h.undelivered, &fuzzEvent{
		kind: eventUnused, fromSeq: seq, toSeq: seq, docID: key,
		feed: sgbucket.FeedEvent{Key: []byte(key)},
	})
}

func (h *fuzzHarness) unusedRange(n int) {
	from := h.nextSeq
	h.nextSeq += uint64(n)
	to := h.nextSeq - 1
	for seq := from; seq <= to; seq++ {
		h.model.seqs[seq] = fuzzSeqRecord{kind: eventUnusedRange, from: from, to: to}
	}
	key := h.env.dbContext.MetadataKeys.UnusedSeqRangeKey(from, to)
	h.undelivered = append(h.undelivered, &fuzzEvent{
		kind: eventUnusedRange, fromSeq: from, toSeq: to, docID: key,
		feed: sgbucket.FeedEvent{Key: []byte(key)},
	})
}

func (h *fuzzHarness) principal(isUser bool) {
	seq := h.allocSeq()
	h.principals++
	name := fmt.Sprintf("p%d", h.principals)
	prefix := "_user:"
	if !isUser {
		prefix = "_role:"
	}
	h.model.seqs[seq] = fuzzSeqRecord{kind: eventPrincipal, name: prefix + name}
	h.undelivered = append(h.undelivered, &fuzzEvent{
		kind: eventPrincipal, fromSeq: seq, toSeq: seq, docID: prefix + name, isUser: isUser,
		json: []byte(fmt.Sprintf(`{"name":%q,"sequence":%d}`, name, seq)),
	})
}

// deliverOne delivers the pick'th of the first window undelivered events, modelling vBucket interleave.
func (h *fuzzHarness) deliverOne(window, pick int) {
	if len(h.undelivered) == 0 {
		return
	}
	window = min(window, len(h.undelivered))
	idx := pick % window
	ev := h.undelivered[idx]
	h.undelivered = slices.Delete(h.undelivered, idx, idx+1)
	h.deliver(ev)
}

// deliver hands an event to the cache the way the caching feed does.
func (h *fuzzHarness) deliver(ev *fuzzEvent) {
	if ev.delivery == 0 {
		h.delivered = append(h.delivered, ev)
	}
	ev.delivery++
	h.stepEvent, h.stepDelivery = ev, ev.delivery
	h.pinLateLogs()
	h.resetSkippedRemoved()
	h.claimSequences(ev)
	if ev.kind == eventDoc {
		h.model.delivered[ev.rev.seq] = true
	}

	received := channels.NewFeedTimestampFromNow()
	feed := ev.feed
	feed.TimeReceived = time.Now()
	switch ev.kind {
	case eventDoc:
		h.cache.DocChanged(feed, db.DocTypeDocument)
	case eventUnused:
		h.cache.DocChanged(feed, db.DocTypeUnusedSeq)
	case eventUnusedRange:
		h.cache.DocChanged(feed, db.DocTypeUnusedSeqRange)
	case eventPrincipal:
		h.cache.ProcessPrincipalDocForTest(h.t, h.env.ctx, ev.docID, ev.json, ev.isUser, received)
	}
	h.checkLateLog(ev)
}

func (h *fuzzHarness) createChannelCache(name string) {
	_, err := db.GetSingleChannelCacheForTest(h.t, h.env.ctx, h.channelCache, h.channelID(name))
	require.NoError(h.t, err)
}

// feedRead has a feed read every channel it watches since where it last got to. This simulated feed is deliberately
// simple: it reads the stable sequence first, so a sequence the cache has not yet made stable is never reported to
// it, and entries beyond that are dropped to be read next time.
func (h *fuzzHarness) feedRead(feedIndex int) {
	feed := h.feeds[feedIndex]
	next, stable, skipped := h.cache.SequenceStateForTest(h.t, h.env.ctx)
	for _, name := range feed.channels {
		since := feed.since[name]
		// Arm the query callback with the state just before this read, which viewOmittedState uses to decide whether
		// the view was the cache's only route to a revision. It is captured here because the callback runs inside
		// GetChanges and must not read the caches itself. Disarming afterwards makes any other query fail loudly.
		h.armed = &fuzzQuery{nextSequence: next, skipped: skipped, cache: h.snapshot(name)}
		entries, err := h.channelCache.GetChanges(h.env.ctx, h.channelID(name), db.ChangesOptions{
			Since:      db.SequenceID{Seq: since},
			ChangesCtx: h.env.ctx,
		})
		h.armed = nil
		require.NoError(h.t, err)
		for _, e := range entries {
			if e.Sequence <= since {
				h.failf("feed %d read channel %s since %d and was given %s", feedIndex, name, since, describeEntry(e))
			}
			if e.Sequence > stable {
				continue
			}
			feed.received = append(feed.received, fuzzReceived{
				channel: name, seq: e.Sequence, docID: e.DocID, revID: e.RevID, removed: e.IsRemoved(),
			})
		}
		feed.since[name] = max(since, stable)
	}
}

// runFuzzScript runs data against a fresh cache inside its own synctest bubble and returns the end state.
func runFuzzScript(t *testing.T, env *fuzzEnv, data []byte) fuzzResult {
	cfg, ops := decodeScript(data)
	var res fuzzResult
	synctest.Test(t, func(t *testing.T) {
		h := newFuzzHarness(t, env, cfg)
		h.start()
		// A failed check ends this goroutine without returning, which would leave the cache's background tasks
		// running and turn the failure into a bubble deadlock panic.
		defer h.stop()
		h.run(ops)
		h.stop()
		res = h.result()
	})
	return res
}

func fuzzSeeds() map[string][]byte {
	cfg := func(c *fuzzConfig) {
		c.PendingMaxNum = 10
		c.ChannelMaxLen = 5
		c.FeedMasks = [fuzzNumFeeds]uint8{chanMask("A"), chanMask("A", "B"), chanMask("C", "D", "E"), chanMask("B", "E")}
	}
	return map[string][]byte{
		"abandonedDuringTimeouts": abandonedDuringTimeoutsSeed(180),
		"inOrderWrites": newScriptBuilder().config(cfg).
			Write(0, chanMask("A"), false).Deliver(1).
			Write(1, chanMask("A", "B"), false).Deliver(1).
			Write(0, chanMask("B"), true).Deliver(2).Deliver(1).
			FeedRead(0).FeedRead(1).Flush().Bytes(),
		"withheldThenLate": newScriptBuilder().config(cfg).
			Write(0, chanMask("A"), false).Write(1, chanMask("A"), false).Write(2, chanMask("B"), false).
			Withhold().Deliver(1).Deliver(1).TimeoutPending().
			DeliverLate().CreateCache(0).FeedRead(0).CleanSkipped().Bytes(),
		"unusedRangeOverWithheld": newScriptBuilder().config(cfg).
			Write(0, chanMask("A"), false).Write(1, chanMask("B"), false).Write(2, chanMask("A"), false).
			Withhold().Withhold().UnusedRange(2).Deliver(1).Deliver(1).Deliver(1).
			TimeoutPending().DeliverLate().DeliverLate().Flush().Bytes(),
		"principalAndDuplicates": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.Flags |= 1 }).
			Principal(true).Write(0, chanMask("A", "C"), false).Unused().Principal(false).
			DeliverAt(4, 2).Deliver(1).Deliver(1).Deliver(1).Duplicate(0).Duplicate(1).Duplicate(2).
			Write(0, chanMask("C"), true).Deliver(1).Duplicate(3).TimeoutPending().Bytes(),
		// A cache created after a write backfills from the bucket view, which shows only each document's latest write.
		"backfillAfterRewrite": newScriptBuilder().config(cfg).
			Write(0, chanMask("B"), false).Deliver(1).Write(0, chanMask("B", "C"), false).
			FeedRead(2).Write(0, chanMask("C"), false).Deliver(1).Deliver(1).CreateCache(1).FeedRead(1).FeedRead(3).Bytes(),
		// A sequence abandoned by the skipped-list cleanup, then a later revision of the same document.
		"abandonedThenRewritten": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.Flags |= 1 }).
			Write(0, chanMask("A", "B"), false).Write(1, chanMask("A"), false).Write(0, chanMask("A"), false).
			Withhold().Deliver(1).Deliver(1).TimeoutPending().CleanSkipped().DeliverLate().
			Write(0, chanMask("B"), false).Deliver(1).FeedRead(0).FeedRead(1).Bytes(),
		// A document leaves a channel at a sequence that is skipped, and the cache replays it from a later revision.
		"lateRemovalReplay": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.Flags |= 1 }).
			CreateCache(0).CreateCache(1).CreateCache(2).
			Write(0, chanMask("A"), false).Write(0, chanMask("B"), false).Write(1, chanMask("A"), false).Write(2, chanMask("B"), false).
			Withhold().Withhold().Deliver(1).Deliver(1).TimeoutPending().
			Write(0, chanMask("C"), false).Deliver(1).FeedRead(0).FeedRead(1).DeliverLate().DeliverLate().FeedRead(0).FeedRead(1).Bytes(),
		// The document's own skipped sequence arrives, into caches that already exist.
		"lateOwnSequence": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.Flags |= 1 }).
			CreateCache(0).CreateCache(1).Write(0, chanMask("A", "B"), false).Write(1, chanMask("A"), false).Write(2, chanMask("B"), false).
			Withhold().Deliver(1).Deliver(1).TimeoutPending().FeedRead(0).DeliverLate().FeedRead(0).FeedRead(1).Bytes(),
		// More late arrivals in one channel than the channel cache's maximum length, so the late log hits its cap.
		"lateLogCap": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.ChannelMaxLen = 3 }).
			CreateCache(0).
			Write(0, chanMask("A"), false).Write(1, chanMask("A"), false).Write(2, chanMask("A"), false).
			Write(3, chanMask("A"), false).Write(4, chanMask("A"), false).Write(5, chanMask("A"), false).
			Withhold().Withhold().Withhold().Withhold().Withhold().Deliver(1).TimeoutPending().
			DeliverLate().DeliverLate().DeliverLate().DeliverLate().DeliverLate().FeedRead(0).Bytes(),
		// A revision skipped when the channel cache is created is not in the backfill, since the view shows only the
		// document's rewrite (sequence 5, beyond the cache's validFrom), and must still reach the cache when it
		// arrives late.
		"lateIntoBackfilledCache": newScriptBuilder().config(cfg).
			Write(0, chanMask("A"), false).Write(1, chanMask("A"), false).Write(2, chanMask("A"), false).
			Write(3, chanMask("B"), false).Write(0, chanMask("A"), false).
			Withhold().Deliver(1).Deliver(1).TimeoutPending().CreateCache(0).FeedRead(0).DeliverLate().FeedRead(0).Bytes(),
		// The same, but the first revision was processed before the cache existed, so the backfill is its only route
		// and its absence from the cache is expected.
		"processedBeforeBackfill": newScriptBuilder().config(cfg).
			Write(0, chanMask("A"), false).Write(1, chanMask("A"), false).Write(2, chanMask("A"), false).
			Write(3, chanMask("B"), false).Write(0, chanMask("A"), false).
			Deliver(1).Deliver(1).Deliver(1).CreateCache(0).FeedRead(0).Deliver(1).Deliver(1).FeedRead(0).Bytes(),
		// Delivering the first event processes its unused sequence, which flushes an overdue pending entry and skips the
		// document's own sequence just before the delivery processes it as a late arrival.
		"skipDuringDelivery": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.Flags |= 1 }).
			Write(0, chanMask("E"), true).TimeoutPending().Write(1, chanMask("E"), false).
			Withhold().Deliver(1).TimeoutPending().DeliverLate().Bytes(),
		// The same flush also skips a sequence that no delivery brings in, which stays skipped.
		"skipDuringDeliveryPartial": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.Flags |= 1 }).
			Write(0, chanMask("E"), true).Write(2, chanMask("E"), false).Write(1, chanMask("E"), false).TimeoutPending().
			Withhold().Withhold().Deliver(1).TimeoutPending().DeliverLate().Bytes(),
		// The flush that skips the document's own sequence also skips older sequences that the same delivery then
		// replays from recent_sequences, as unused sequences.
		"skipReplayDuringDelivery": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.PendingMaxNum = 3 }).
			Unused().Write(0, chanMask("E"), false).Unused().Unused().Write(0, chanMask("E"), false).
			Unused().Unused().Unused().Unused().Write(0, chanMask("E"), true).
			Withhold().Withhold().Withhold().Withhold().Withhold().Withhold().Bytes(),
		// The same, but the document left channel E at one of the replayed sequences, so its replay is a removal that
		// reaches the late logs of E and "*".
		"skipRemovalReplayDuringDelivery": newScriptBuilder().config(cfg).config(func(c *fuzzConfig) { c.PendingMaxNum = 3; c.Flags |= 1 }).
			CreateCache(4).
			Unused().Write(0, chanMask("E"), false).Unused().Unused().Write(0, chanMask("D"), false).
			Unused().Unused().Unused().Unused().Write(0, chanMask("D"), true).
			Withhold().Withhold().Withhold().Withhold().Withhold().Withhold().Bytes(),
		// Each notify site releases a document that was waiting for an earlier sequence, so its channels must be
		// notified by whichever event closed the gap.
		"notifyPrincipalReleasesPending": newScriptBuilder().config(cfg).
			Principal(true).Write(0, chanMask("A", "B"), false).Withhold().Deliver(1).DeliverLate().Flush().Bytes(),
		"notifyUnusedReleasesPending": newScriptBuilder().config(cfg).
			Unused().Write(0, chanMask("A"), false).Withhold().Deliver(1).DeliverLate().Flush().Bytes(),
		"notifyRangeReleasesPending": newScriptBuilder().config(cfg).
			UnusedRange(2).Write(0, chanMask("B"), false).Withhold().Deliver(1).DeliverLate().Flush().Bytes(),
		"notifySingleRangeReleasesPending": newScriptBuilder().config(cfg).
			UnusedRange(1).Write(0, chanMask("C"), false).Withhold().Deliver(1).DeliverLate().Flush().Bytes(),
		// A pending document is released by the cache's own background task, which has no event to notify from.
		"notifyBackgroundRelease": newScriptBuilder().config(cfg).
			Write(0, chanMask("A"), false).Write(1, chanMask("B"), false).Withhold().Deliver(1).TimeoutPending().Flush().Bytes(),
	}
}

// abandonedDuringTimeoutsSeed skips a sequence and then lets the cache's own cleanup task abandon it while the
// harness only sleeps through pending timeouts: the task ticks every half of the skipped max wait, so it needs the
// skip to age past that wait, which takes timeouts*fuzzPendingSeqMaxWait of fake time.
func abandonedDuringTimeoutsSeed(timeouts int) []byte {
	b := newScriptBuilder().config(func(c *fuzzConfig) { c.PendingMaxNum = 10; c.Flags |= 1 }).
		Write(0, chanMask("E"), false).Unused().Withhold().Deliver(1)
	for range timeouts {
		b.TimeoutPending()
	}
	return b.DeliverLate().Bytes()
}

// FuzzChangeCacheDispatch feeds arbitrary scripts to a real change cache. It checks that the cache neither
// panics, hangs nor races, and that what it holds and reports agrees with the simulated bucket: see
// change_cache_fuzz_check_test.go for the properties.
func FuzzChangeCacheDispatch(f *testing.F) {
	base.SetUpTestLogging(f, base.LevelWarn, base.KeyAll)
	env := newFuzzEnv(f)
	for _, seed := range fuzzSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runFuzzScript(t, env, data)
	})
}

func TestFuzzScriptRoundTrip(t *testing.T) {
	b := newScriptBuilder().config(func(c *fuzzConfig) {
		c.Consumers = 5
		c.RunSize = 32
		c.TrackerCapacity = 8
		c.PendingMaxNum = 50
		c.ChannelMaxLen = 3
		c.Flags = 0xF1 // unused flag bits must survive
		c.FeedMasks = [fuzzNumFeeds]uint8{1, 31, 6, 0}
		c.Spare = [6]uint8{1, 2, 3, 4, 5, 255}
	}).Write(7, chanMask("A", "E"), true).Deliver(8).DeliverAt(3, 31).Withhold().DeliverLate().Duplicate(200).
		Unused().UnusedRange(8).Principal(false).TimeoutPending().CleanSkipped().CreateCache(4).
		FeedRead(3).Flush().Stall(6)

	cfg, ops := decodeScript(b.Bytes())
	assert.Equal(t, b.cfg, cfg)
	assert.Equal(t, b.ops, ops)
	assert.Equal(t, b.Bytes(), (&scriptBuilder{cfg: cfg, ops: ops}).Bytes())

	// Every seed round-trips too.
	for name, seed := range fuzzSeeds() {
		cfg, ops := decodeScript(seed)
		assert.Equal(t, seed, (&scriptBuilder{cfg: cfg, ops: ops}).Bytes(), name)
	}
}

func TestFuzzScriptDecodeTotal(t *testing.T) {
	for _, data := range [][]byte{nil, {}, {0}, make([]byte, fuzzHeaderLen-1), make([]byte, fuzzHeaderLen),
		append(make([]byte, fuzzHeaderLen), byte(opWrite)),                // operands cut short
		append(make([]byte, fuzzHeaderLen), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), // unknown opcodes
	} {
		assert.NotPanics(t, func() { decodeScript(data) })
	}

	r := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		data := randomScriptBytes(r, int(r.UintN(1024)))
		cfg, ops := decodeScript(data)
		assert.LessOrEqual(t, len(ops), fuzzMaxOps)
		assert.GreaterOrEqual(t, cfg.PendingMaxNum, 3)
		assert.LessOrEqual(t, cfg.PendingMaxNum, 50)
		assert.GreaterOrEqual(t, cfg.ChannelMaxLen, 3)
		assert.LessOrEqual(t, cfg.ChannelMaxLen, 20)
	}

	// A script is capped at fuzzMaxOps however long the input is.
	long := append(make([]byte, fuzzHeaderLen), slices.Repeat([]byte{byte(opWithhold)}, 3*fuzzMaxOps)...)
	_, ops := decodeScript(long)
	assert.Len(t, ops, fuzzMaxOps)
}

func TestFuzzHarnessDeterministic(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelWarn, base.KeyAll)
	env := newFuzzEnv(t)
	for name, seed := range fuzzSeeds() {
		t.Run(name, func(t *testing.T) {
			first := runFuzzScript(t, env, seed)
			second := runFuzzScript(t, env, seed)
			assert.Equal(t, first, second)
			assert.NotEmpty(t, first.Notifications, "a seed script should cache something")
		})
	}
}

func TestFuzzBuilderRender(t *testing.T) {
	script := newScriptBuilder().config(func(c *fuzzConfig) { c.PendingMaxNum = 10; c.Flags = 1 }).
		Write(0, chanMask("B", "C"), true).Write(1, 0, false).Deliver(2).DeliverAt(6, 3).Withhold().Unused().
		Principal(false).CreateCache(4).FeedRead(2).TimeoutPending().
		add(opWrite, 48, 48, 49). // operands the builder's decoded API cannot express
		add(opUnusedRange, 12).
		add(opNop)
	cfg, ops := decodeScript(script.Bytes())
	expected := `newScriptBuilder().
	config(func(c *fuzzConfig) { c.PendingMaxNum = 10; c.Flags = 1 }).
	Write(0, chanMask("B", "C"), true).
	Write(1, chanMask(), false).
	Deliver(2).
	DeliverAt(6, 3).
	Withhold().
	Unused().
	Principal(false).
	CreateCache(4).
	FeedRead(2).
	TimeoutPending().
	add(opWrite, 48, 48, 49).
	add(opUnusedRange, 12).
	add(opNop).
	Bytes()`
	assert.Equal(t, expected, renderBuilder(cfg, ops))
	assert.Equal(t, renderBuilder(cfg, ops), renderBuilder(decodeScript(script.Bytes())))
}

// TestFuzzBuilderRoundTrip checks that the calls renderBuilder prints rebuild the script they came from, byte for
// byte, including operands the decoded builder API cannot express. The text and the call it stands for are produced
// together, so applying the calls tests what the text says.
func TestFuzzBuilderRoundTrip(t *testing.T) {
	check := func(name string, data []byte) {
		cfg, ops := decodeScript(data)
		rebuilt := newScriptBuilder()
		for _, step := range builderSteps(cfg, ops) {
			step.apply(rebuilt)
		}
		assert.Equal(t, (&scriptBuilder{cfg: cfg, ops: ops}).Bytes(), rebuilt.Bytes(), name)
	}
	for name, seed := range fuzzSeeds() {
		check(name, seed)
	}
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 500 {
		check(fmt.Sprintf("random %d", i), randomScriptBytes(r, int(r.UintN(200))))
	}
}

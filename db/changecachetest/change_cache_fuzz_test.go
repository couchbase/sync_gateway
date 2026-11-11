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

// fuzzFeed is a simulated changes feed reading a fixed set of channels.
type fuzzFeed struct {
	channels []string
}

// fuzzHarness drives a real change cache from a script. It is built and used only inside a synctest
// bubble, so the cache's background tasks run on fake time.
type fuzzHarness struct {
	t            *testing.T
	env          *fuzzEnv
	cfg          fuzzConfig
	cache        fuzzCache
	channelCache db.ChannelCache
	feeds        [fuzzNumFeeds]*fuzzFeed

	nextSeq     uint64 // next sequence the simulated server will allocate
	docs        [fuzzNumDocs]*fuzzDoc
	undelivered []*fuzzEvent // oldest first
	withheld    []*fuzzEvent // oldest first
	delivered   []*fuzzEvent // in first-delivery order, one entry per event
	principals  int

	notifyLock    sync.Mutex
	notifications []string // each notification's channel names, sorted and comma-joined, in arrival order
}

// fuzzResult is the cache state at the end of a run. Comparing two runs of one script checks the harness
// is deterministic.
type fuzzResult struct {
	NextSequence  uint64
	MaxStable     uint64
	Skipped       [][2]uint64
	Notifications []string
}

func newFuzzHarness(t *testing.T, env *fuzzEnv, cfg fuzzConfig) *fuzzHarness {
	h := &fuzzHarness{t: t, env: env, cfg: cfg, nextSeq: 1}
	for i := range h.feeds {
		h.feeds[i] = &fuzzFeed{channels: cfg.feedChannels(i)}
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

	// The query handler is empty: these scripts never need a channel's history from the bucket.
	queryHandler := &db.QueryHandlerForTest{}
	channelCache, err := db.NewChannelCacheForTest(t, ctx, "db", options.ChannelCacheOptions, queryHandler.AsFactory,
		channels.NewActiveChannels(h.env.dbContext.DbStats.Cache().NumActiveChannels), h.env.dbContext.DbStats.Cache())
	require.NoError(t, err)
	h.channelCache = channelCache

	cache := db.NewChangeCacheForTest(t)
	require.NoError(t, cache.Init(ctx, h.env.dbContext, channelCache, nil, &options, h.env.dbContext.MetadataKeys))
	// Init leaves the cache locked until Start, and its background tasks take that lock before they read the
	// notify callback, so installing it here is ordered before every read. After Start it would race them.
	cache.SetManualNotifyChangeFuncForTest(t, h.recordNotification)
	require.NoError(t, cache.Start(0))
	h.cache = cache

	if h.cfg.starChannel() {
		h.createChannelCache(channels.UserStarChannel)
	}
}

// stop shuts down everything start created, so no goroutine outlives the bubble.
func (h *fuzzHarness) stop() {
	h.cache.Stop(h.env.ctx)
	h.channelCache.Stop(h.env.ctx)
}

func (h *fuzzHarness) recordNotification(_ context.Context, chans channels.Set) {
	names := make([]string, 0, len(chans))
	for id := range chans {
		names = append(names, id.Name)
	}
	slices.Sort(names)
	h.notifyLock.Lock()
	defer h.notifyLock.Unlock()
	h.notifications = append(h.notifications, strings.Join(names, ","))
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
	return res
}

func (h *fuzzHarness) allocSeq() uint64 {
	seq := h.nextSeq
	h.nextSeq++
	return seq
}

// run executes ops in order.
func (h *fuzzHarness) run(ops []fuzzOp) {
	for _, op := range ops {
		h.step(op)
	}
}

func (h *fuzzHarness) step(op fuzzOp) {
	switch op.Code {
	case opWrite:
		h.write(op.writeDoc(), op.writeMask(), op.writeExtraSeq())
	case opDeliver:
		h.deliverOne(op.deliverWindow(), op.deliverPick())
	case opWithhold:
		if len(h.undelivered) > 0 {
			h.withheld = append(h.withheld, h.undelivered[0])
			h.undelivered = h.undelivered[1:]
		}
	case opDeliverLate:
		if len(h.withheld) > 0 {
			ev := h.withheld[0]
			h.withheld = h.withheld[1:]
			h.deliver(ev)
		}
	case opDuplicate:
		if len(h.delivered) > 0 {
			h.deliver(h.delivered[int(op.Args[0])%len(h.delivered)])
		}
	case opUnused:
		h.unused()
	case opUnusedRange:
		h.unusedRange(op.unusedRangeLen())
	case opPrincipal:
		h.principal(op.principalIsUser())
	case opTimeoutPending:
		// The cache's own InsertPendingEntries task does the work when fake time passes its deadline. The
		// extra nanosecond keeps the harness's wake-up off the task's tick instant.
		time.Sleep(fuzzPendingSeqMaxWait + time.Nanosecond)
		synctest.Wait()
	case opCleanSkipped:
		time.Sleep(fuzzSkippedSeqMaxWait + time.Nanosecond)
		synctest.Wait()
	case opCreateCache:
		h.createChannelCache(fuzzChannelNames[op.channelIndex()])
	case opFeedRead:
		h.feedRead(op.feedIndex())
	case opFlush:
		h.cache.FlushNotificationsForTest(h.t)
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
}

func (h *fuzzHarness) createChannelCache(name string) {
	_, err := db.GetSingleChannelCacheForTest(h.t, h.env.ctx, h.channelCache, h.channelID(name))
	require.NoError(h.t, err)
}

// feedRead reads every channel the feed watches from sequence zero.
func (h *fuzzHarness) feedRead(feedIndex int) {
	for _, name := range h.feeds[feedIndex].channels {
		_, err := h.channelCache.GetChanges(h.env.ctx, h.channelID(name), db.ChangesOptions{ChangesCtx: h.env.ctx})
		require.NoError(h.t, err)
	}
}

// runFuzzScript runs data against a fresh cache inside its own synctest bubble and returns the end state.
func runFuzzScript(t *testing.T, env *fuzzEnv, data []byte) fuzzResult {
	cfg, ops := decodeScript(data)
	var res fuzzResult
	synctest.Test(t, func(t *testing.T) {
		h := newFuzzHarness(t, env, cfg)
		h.start()
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
	}
}

// FuzzChangeCacheDispatch feeds arbitrary scripts to a real change cache. It checks only that the cache
// neither panics, hangs nor races.
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

/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db/dispatchtracker"
	"github.com/couchbase/sync_gateway/db/notifyaccumulator"
)

const (
	DefaultCachePendingSeqMaxNum  = 10000            // Max number of waiting sequences
	DefaultCachePendingSeqMaxWait = 5 * time.Second  // Max time we'll wait for a pending sequence before sending to missed queue
	DefaultSkippedSeqMaxWait      = 60 * time.Minute // Max time we'll wait for an entry in the missing before purging
	QueryTombstoneBatch           = 250              // Max number of tombstones checked per query during Compact
	unusedSeqKey                  = "_unusedSeqKey"  // Key used by ChangeWaiter to mark unused sequences
	unusedSeqCollectionID         = 0                // Collection ID used by ChangeWaiter to mark unused sequences

	defaultWaitForSequence = 30 * time.Second // wait time for sequence to arrive over changes feed after allocation
)

// DocumentType indicates the type of document being processed in the change cache (e.g. User doc etc).
type DocumentType uint8

const (
	DocTypeUnknown        DocumentType = iota // Unknown document type
	DocTypeDocument                           // Customer data document type
	DocTypeUser                               // User document
	DocTypeRole                               // Role document
	DocTypeUnusedSeq                          // Unused sequence notification
	DocTypeUnusedSeqRange                     // Unused sequence range notification
	DocTypeSGCfg                              // Cfg docs for import feed management
)

// Enable keeping a channel-log for the "*" channel (channel.UserStarChannel). The only time this channel is needed is if
// someone has access to "*" (e.g. admin-party) and tracks its changes feed.
const EnableStarChannelLog = true

// Manages a cache of the recent change history of all channels.
//
// Core responsibilities:
//
// - Receive DCP changes via callbacks
//   - Perform sequence buffering to ensure documents are received in sequence order
//   - Propagating DCP changes down to appropriate channel caches
type changeCache struct {
	db                    *DatabaseContext
	logCtx                context.Context               // fix in sg-bucket to ProcessEvent
	logsDisabled          bool                          // If true, ignore incoming tap changes
	nextSequence          uint64                        // Next consecutive sequence number to add; the sequence buffering state. Read and advanced only by the ordering code under the lock, which sees it change mid-section. Code outside the lock should use getPublishedNextSequence().
	publishedNextSequence atomic.Uint64                 // nextSequence as of the last lock release, for lock-free readers. Stored under the lock just before every release, so it never moves backwards. Read via getPublishedNextSequence().
	initialSequence       atomic.Uint64                 // DB's current sequence at startup time. Atomic because DocChanged reads it without the lock, and the feed starts before Start sets it.
	receivedSeqs          map[uint64]struct{}           // Set of all sequences received
	pendingLogs           LogPriorityQueue              // Out-of-sequence entries waiting to be cached
	notifyAcc             notifyaccumulator.Accumulator // Channels whose caches gained entries since the last drain. Merged after the entries are cached; drained by the change listener's broadcast tick.
	started               base.AtomicBool               // Set by the Start method
	stopped               base.AtomicBool               // Set by the Stop method
	skippedSeqs           *SkippedSequenceSkiplist      // Skipped sequences still pending on the DCP caching feed
	removeSkippedHook     func(seq uint64)              // Called with each sequence that leaves skippedSeqs on arrival, so tests can observe late arrivals. Read and called under the lock.
	released              []*LogEntry                   // Entries released by ordering during the current lock hold, waiting for _dispatchReleased. Guarded by the lock; empty between holds.
	tracker               *dispatchtracker.Tracker      // Retires runs in ticket order and gives the watermark. Created by Init.
	runSize               int                           // Maximum number of entries in one run: dispatchtracker.RunSize, smaller in tests.
	nextTicket            uint64                        // Ticket for the next run. Guarded by the lock; starts at 1.
	releaseBudget         int                           // Entries the current lock hold may still release: one run's worth for each free tracker slot. Guarded by the lock; set at the start of each hold.
	numPartialDrains      uint64                        // Lock holds that left releasable entries in pending because their budget ran out. Guarded by the lock. TODO: CBG-6120 - publish as the dispatch_partial_drains stat.
	retiredScratch        []*[]channels.ID              // Reused to collect the buffers of runs the tracker retires. Guarded by the lock.
	lock                  sync.RWMutex                  // Coordinates access to struct fields
	options               CacheOptions                  // Cache config
	terminator            chan bool                     // Signal termination of background goroutines
	backgroundTasks       []BackgroundTask              // List of background tasks.
	initTime              time.Time                     // Cache init time - used for latency calculations
	channelCache          ChannelCache                  // Underlying channel cache
	lastAddPendingTime    int64                         // The most recent time _releasePending was run, as epoch time
	internalStats         changeCacheStats              // Running stats for the change cache.  Only applied to expvars on a call to changeCache.updateStats
	cfgEventCallback      base.CfgEventNotifyFunc       // Callback for Cfg updates received over the caching feed
	sgCfgPrefix           string                        // Prefix for SG Cfg doc keys
	metaKeys              *base.MetadataKeys            // Metadata key formatter
}

type changeCacheStats struct {
	highSeqFeed   uint64
	pendingSeqLen int
	maxPending    int
}

func (c *changeCache) updateStats(ctx context.Context) {

	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.db == nil {
		return
	}
	// grab skipped sequence stats
	skippedSequenceListStats := c.skippedSeqs.getStats()

	c.db.DbStats.Database().HighSeqFeed.SetIfMax(c.internalStats.highSeqFeed)
	c.db.DbStats.Cache().PendingSeqLen.Set(int64(c.internalStats.pendingSeqLen))
	c.db.DbStats.CBLReplicationPull().MaxPending.SetIfMax(int64(c.internalStats.maxPending))
	c.db.DbStats.Cache().HighSeqStable.Set(c._getMaxStableCached(ctx))
	c.db.DbStats.Cache().NumCurrentSeqsSkipped.Set(skippedSequenceListStats.NumCurrentSkippedSequencesStat)
	c.db.DbStats.Cache().NumSkippedSeqs.Set(skippedSequenceListStats.NumCumulativeSkippedSequencesStat)
	c.db.DbStats.Cache().SkippedSequenceSkiplistNodes.Set(skippedSequenceListStats.ListLengthStat)
}

type LogEntry = channels.LogEntry

type LogEntries []*LogEntry

// A priority-queue of LogEntries, kept ordered by increasing sequence #.
type LogPriorityQueue []*LogEntry

type CacheOptions struct {
	ChannelCacheOptions
	CachePendingSeqMaxWait time.Duration // Max wait for pending sequence before skipping
	CachePendingSeqMaxNum  int           // Max number of pending sequences before skipping
	CacheSkippedSeqMaxWait time.Duration // Max wait for skipped sequence before abandoning
	MaxRunsInFlight        uint64        // Completion tracker capacity, in runs; must pass dispatchtracker.ValidateCapacity. Default and minimum dispatchtracker.MinCapacity (1024)
	// Broadcast ticker intervals for waking continuous _changes feeds. Zero means use the package default
	// (DefaultBroadcastChangesTime / SkippedSequenceBroadcastChangesTime). Internal tuning knob - primarily
	// so tests can avoid the 500ms skipped-sequence slow mode dominating runtime. Not exposed via REST config.
	BroadcastChangesInterval         time.Duration // Normal-mode feed broadcast interval
	SkippedSequenceBroadcastInterval time.Duration // Slow-mode feed broadcast interval (skipped sequences present)
}

func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CachePendingSeqMaxWait:           DefaultCachePendingSeqMaxWait,
		CachePendingSeqMaxNum:            DefaultCachePendingSeqMaxNum,
		CacheSkippedSeqMaxWait:           DefaultSkippedSeqMaxWait,
		MaxRunsInFlight:                  dispatchtracker.MinCapacity,
		BroadcastChangesInterval:         DefaultBroadcastChangesTime,
		SkippedSequenceBroadcastInterval: SkippedSequenceBroadcastChangesTime,
		ChannelCacheOptions: ChannelCacheOptions{
			ChannelCacheAge:             DefaultChannelCacheAge,
			ChannelCacheMinLength:       DefaultChannelCacheMinLength,
			ChannelCacheMaxLength:       DefaultChannelCacheMaxLength,
			MaxNumChannels:              DefaultChannelCacheMaxNumber,
			CompactHighWatermarkPercent: DefaultCompactHighWatermarkPercent,
			CompactLowWatermarkPercent:  DefaultCompactLowWatermarkPercent,
			ChannelQueryLimit:           DefaultQueryPaginationLimit,
			LateLogAge:                  DefaultLateLogAge,
		},
	}
}

// ////// HOUSEKEEPING:

// Initializes a new changeCache.
// lastSequence is the last known database sequence assigned.
// Channels that gain entries are not notified from here: they collect in notifyAcc, which the change listener's
// broadcast tick drains (see drainNotifications).
// After calling Init(), you must call .Start() to start using the cache, otherwise it will be in a locked state
// and callers will block on trying to obtain the lock.

func (c *changeCache) Init(ctx context.Context, dbContext *DatabaseContext, channelCache ChannelCache, options *CacheOptions, metaKeys *base.MetadataKeys) error {
	c.db = dbContext
	c.logCtx = ctx

	c.receivedSeqs = make(map[uint64]struct{})
	c.terminator = make(chan bool)
	c.initTime = time.Now()
	c.skippedSeqs = NewSkippedSequenceSkiplist()
	c.lastAddPendingTime = time.Now().UnixNano()
	c.sgCfgPrefix = dbContext.MetadataKeys.SGCfgPrefix(c.db.Options.GroupID)
	c.metaKeys = metaKeys

	// init cache options
	if options != nil {
		c.options = *options
	} else {
		c.options = DefaultCacheOptions()
	}

	c.channelCache = channelCache

	// The database config always sets MaxRunsInFlight, sized from max_num_pending when not configured. Options built
	// without the config, as many tests build them, leave it zero, so size it the same way here.
	trackerCapacity := c.options.MaxRunsInFlight
	if trackerCapacity == 0 {
		var err error
		if trackerCapacity, err = dispatchtracker.CapacityFor(c.options.CachePendingSeqMaxNum); err != nil {
			return fmt.Errorf("change cache max pending sequences: %w", err)
		}
	}
	tracker, err := dispatchtracker.NewTracker(trackerCapacity)
	if err != nil {
		return fmt.Errorf("change cache max runs in flight: %w", err)
	}
	c.tracker = tracker
	c.runSize = dispatchtracker.RunSize
	c.nextTicket = 1

	base.InfofCtx(ctx, base.KeyCache, "Initializing changes cache for %s with options %+v", base.UD(c.db.Name), c.options)

	heap.Init(&c.pendingLogs)

	// background tasks that perform housekeeping duties on the cache
	bgt, err := NewBackgroundTask(ctx, "InsertPendingEntries", c.InsertPendingEntries, c.options.CachePendingSeqMaxWait/2, c.terminator)
	if err != nil {
		return err
	}
	c.backgroundTasks = append(c.backgroundTasks, bgt)

	bgt, err = NewBackgroundTask(ctx, "CleanSkippedSequenceQueue", c.CleanSkippedSequenceQueue, c.options.CacheSkippedSeqMaxWait/2, c.terminator)
	if err != nil {
		return err
	}
	c.backgroundTasks = append(c.backgroundTasks, bgt)

	// Lock the cache -- not usable until .Start() called.  This fixes the DCP startup race condition documented in SG #3558.
	c.lock.Lock()
	return nil
}

func (c *changeCache) Start(initialSequence uint64) error {

	// Unlock the cache after this function returns.
	defer c.lock.Unlock()

	// Set initial sequence for sequence buffering
	c._setInitialSequence(initialSequence)

	// Set initial sequence for cache (validFrom)
	c.channelCache.Init(initialSequence)

	if !c.started.CompareAndSwap(false, true) {
		return errors.New("changeCache already started")
	}

	return nil
}

// Stops the cache. Clears its state and tells the housekeeping task to stop.
func (c *changeCache) Stop(ctx context.Context) {

	if !c.started.IsTrue() {
		// changeCache never started - nothing to stop
		return
	}

	if !c.stopped.CompareAndSwap(false, true) {
		base.WarnfCtx(ctx, "changeCache was already stopped")
		return
	}

	// Signal to background goroutines that the changeCache has been stopped, so they can exit
	// their loop
	close(c.terminator)

	// Wait for changeCache background tasks to finish.
	waitForBGTCompletion(ctx, BGTCompletionMaxWait, c.backgroundTasks, c.db.Name)

	c.lock.Lock()
	c.logsDisabled = true
	c.lock.Unlock()
}

// drainNotifications passes every channel merged since the last drain to apply, which must not keep the set. The
// change listener's broadcast tick calls it while holding tapNotifier.L, so the stamp and broadcast share one lock hold.
// The tick is the only drainer of a database's cache; tests drain only standalone caches (DrainNotificationsForTest).
func (c *changeCache) drainNotifications(apply func(channels.Set)) {
	c.notifyAcc.Drain(apply)
}

// Empty out all channel caches.
func (c *changeCache) Clear(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Reset initialSequence so that any new channel caches have their validFrom set to the current last sequence
	// the point at which the change cache was initialized / re-initialized.
	// No need to touch c.nextSequence here, because we don't want to touch the sequence buffering state.
	initialSequence, err := c.db.LastSequence(ctx)
	if err != nil {
		return err
	}
	c.initialSequence.Store(initialSequence)

	c.pendingLogs = nil
	heap.Init(&c.pendingLogs)

	c.initTime = time.Now()

	c.channelCache.Clear()
	return nil
}

// If set to false, DocChanged() becomes a no-op.
func (c *changeCache) EnableChannelIndexing(enable bool) {
	c.lock.Lock()
	c.logsDisabled = !enable
	c.lock.Unlock()
}

// Triggers _releasePending if it hasn't been run in CachePendingSeqMaxWait.  Error returned to fulfil BackgroundTaskFunc signature.
func (c *changeCache) InsertPendingEntries(ctx context.Context) error {

	lastAddPendingLogsTime := atomic.LoadInt64(&c.lastAddPendingTime)
	if time.Since(time.Unix(0, lastAddPendingLogsTime)) < c.options.CachePendingSeqMaxWait {
		return nil
	}

	// Release any entries that have been pending too long:
	c.orderAndDispatch(ctx, func() {
		c._releasePending(ctx)
	})
	return nil
}

// Cleanup function, invoked periodically.
// Removes skipped entries from skippedSeqs that have been waiting longer
// than CacheSkippedSeqMaxWait from the slice.
func (c *changeCache) CleanSkippedSequenceQueue(ctx context.Context) error {

	base.InfofCtx(ctx, base.KeyCache, "Starting CleanSkippedSequenceQueue for database %s", base.MD(c.db.Name))

	compactedSequences, numSequencesLeftInList := c.skippedSeqs.SkippedSequenceCompact(ctx, int64(c.options.CacheSkippedSeqMaxWait.Seconds()))
	if compactedSequences == 0 {
		base.InfofCtx(ctx, base.KeyCache, "CleanSkippedSequenceQueue complete.  No sequences to be compacted from skipped sequence list for database %s.", base.MD(c.db.Name))
		return nil
	}

	c.db.DbStats.Cache().AbandonedSeqs.Add(compactedSequences)

	// update the notify mode
	if numSequencesLeftInList == 0 {
		c.db.BroadcastSlowMode.CompareAndSwap(true, false)
	}

	base.InfofCtx(ctx, base.KeyCache, "CleanSkippedSequenceQueue complete.  Cleaned %d sequences from skipped list for database %s.", compactedSequences, base.MD(c.db.Name))
	return nil
}

// ////// ADDING CHANGES:

// Note that DocChanged may be executed concurrently for multiple events (in the DCP case, DCP events
// originating from multiple vbuckets).  Only processEntry is locking - all other functionality needs to support
// concurrent processing.
func (c *changeCache) DocChanged(event sgbucket.FeedEvent, docType DocumentType) {
	ctx := c.logCtx
	docID := string(event.Key)
	dcpValue := event.Value

	timeReceived := channels.NewFeedTimestamp(&event.TimeReceived)

	// ** This method does not directly access any state of c, so it doesn't lock.
	switch docType {
	case DocTypeUnknown:
		return // no-op unknown doc type
	case DocTypeUser, DocTypeRole:
		if docBody, err := c.fetchMetadataDocBody(ctx, docID, event.Cas); err != nil {
			if base.IsDocNotFoundError(err) || base.IsCasMismatch(err) {
				base.DebugfCtx(ctx, base.KeyCache, "Skipping now changed principal doc %q feed event - a newer mutation will be processed", base.UD(docID))
			} else {
				base.WarnfCtx(ctx, "Error fetching body for principal doc %q: %v - will not be processed", base.UD(docID), err)
			}
		} else {
			c.processPrincipalDoc(ctx, docID, docBody, docType == DocTypeUser, timeReceived)
		}
		return
	case DocTypeUnusedSeq:
		c.processUnusedSequence(ctx, docID, timeReceived)
		return
	case DocTypeUnusedSeqRange:
		c.processUnusedSequenceRange(ctx, docID)
		return
	case DocTypeSGCfg:
		if c.cfgEventCallback != nil {
			c.cfgEventCallback(docID, event.Cas, nil)
		}
		return
	case DocTypeDocument: // processed below
	}

	collection, exists := c.db.CollectionByID[event.CollectionID]
	if !exists {
		cID := event.CollectionID
		if cID == base.DefaultCollectionID && base.MetadataCollectionID == base.DefaultCollectionID {
			// It's possible for the `_default` collection to be associated with other databases writing non-principal documents,
			// but we still need this collection's feed for the sgCfgPrefix docs.
		} else if cID == base.MetadataCollectionID {
			// When Metadata moves to a different collection, we should start to warn again - we don't expect non-metadata mutations here!
			base.WarnfCtx(ctx, "DocChanged(): Non-metadata mutation for doc %q in MetadataStore - kv ID: %d", base.UD(docID), cID)
		} else {
			// Unrecognised collection
			// we shouldn't be receiving mutations for a collection we're not running a database for (except the metadata store)
			base.WarnfCtx(ctx, "DocChanged(): Could not find collection for doc %q - kv ID: %d", base.UD(docID), cID)
		}
		return
	}

	ctx = collection.AddCollectionContext(ctx)

	// If the document has no xattrs, it can not have a _sync xattr
	if event.DataType&base.MemcachedDataTypeXattr == 0 {
		return
	}

	// If this is a binary document, we can ignore.
	if event.DataType == base.MemcachedDataTypeRaw {
		return
	}

	// Only the sync metadata the cache needs is parsed. The body and the rest of _sync are skipped.
	feedData, err := unmarshalCachingFeedData(dcpValue, event.DataType, collection.UserXattrKey())
	if err != nil {
		if errors.Is(err, sgbucket.ErrEmptyMetadata) {
			base.WarnfCtx(ctx, "Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		} else {
			base.DebugfCtx(ctx, base.KeyCache, "Unable to unmarshal sync metadata for feed document %q.  Will not be included in channel cache.  Error: %v", base.UD(docID), err)
		}
		return
	}

	if feedData == nil {
		return
	}

	syncData := &feedData.syncData
	rawUserXattr := feedData.rawUserXattr
	isDelete := event.Opcode == sgbucket.FeedOpDeletion

	if isSGWrite, isSGWriteAmbiguous := syncData.IsSGWriteXattrOnly(ctx, event.Cas, isDelete, rawUserXattr, &feedData.rawVV); isSGWriteAmbiguous {
		// CRC is the only remaining check, but we need to fetch the doc body now
		c.db.DbStats.Cache().IsSGWriteKVFetchCount.Add(1)
		docBody, cas, err := collection.GetCollectionDatastore().GetRaw(ctx, docID)
		if err != nil {
			// Can't fetch body - drop this event. A subsequent mutation
			// will arrive on the feed if the doc is updated again.
			base.DebugfCtx(ctx, base.KeyCache, "Unable to fetch doc body for IsSGWrite on %q, dropping: %v", base.UD(docID), err)
			return
		} else if cas != event.Cas {
			// Doc mutated since DCP event - drop this stale event,
			// the newer mutation will arrive on the feed.
			base.DebugfCtx(ctx, base.KeyCache, "CAS mismatch on body fetch for IsSGWrite on %q (event=%d, current=%d), dropping stale event", base.UD(docID), event.Cas, cas)
			return
		}
		if base.Crc32cHashString(docBody) != syncData.Crc32c {
			return
		}
	} else if !isSGWrite {
		return
	}

	if syncData.Sequence <= c.initialSequence.Load() {
		return // DCP is sending us an old value from before I started up; ignore it
	}

	latency := FeedLatency(syncData.TimeSaved, c.initTime, time.Now())
	if latency > 0 {
		c.db.DbStats.Database().DCPReceivedTime.Add(latency.Nanoseconds())
	}
	c.db.DbStats.Database().DCPReceivedCount.Add(1)

	// If the doc update wasted any sequences due to conflicts, add empty entries for them:
	if len(syncData.UnusedSequences) > 0 {
		for _, seq := range syncData.UnusedSequences {
			change := &LogEntry{
				Sequence:       seq,
				TimeReceived:   timeReceived,
				CollectionID:   event.CollectionID,
				UnusedSequence: true,
			}
			c.processEntry(ctx, change)
		}
		base.DebugfCtx(ctx, base.KeyCache, "Received unused sequences in unused_sequences property for (%q / %q): %v", base.UD(docID), syncData.GetRevTreeID(), syncData.UnusedSequences)
	}

	// If the recent sequence history includes any sequences earlier than the current sequence, and
	// not already seen by the gateway (more recent than c.nextSequence), add them as empty entries
	// so that they are included in sequence buffering.
	// If one of these sequences represents a removal from a channel then set the LogEntry removed flag
	// and the set of channels it was removed from
	currentSequence := syncData.Sequence
	if len(syncData.UnusedSequences) > 0 {
		currentSequence = syncData.UnusedSequences[0]
	}

	if len(syncData.RecentSequences) > 0 {
		nextSequence := c.getPublishedNextSequence()
		seqsCached := make([]uint64, 0, len(syncData.RecentSequences))

		for _, seq := range syncData.RecentSequences {
			// seq < currentSequence means the sequence is not the latest allocated to this document
			// seq >= nextSequence means this sequence is a pending sequence to be expected in the cache
			// the two conditions above together means that the cache expects us to run processEntry on this sequence as its pending
			// If seq < current sequence allocated to the doc and seq is in skipped list this means that this sequence
			// never arrived over the caching feed due to deduplication and was pushed to a skipped sequence list
			isSkipped := (seq < currentSequence && seq < nextSequence) && c.WasSkipped(seq)
			if (seq >= nextSequence && seq < currentSequence) || isSkipped {
				seqsCached = append(seqsCached, seq)
				change := &LogEntry{
					Sequence:     seq,
					TimeReceived: timeReceived,
					CollectionID: event.CollectionID,
					Skipped:      isSkipped,
				}

				// if the doc was removed from one or more channels at this sequence
				// Set the removed flag and removed channel set on the LogEntry
				if channelRemovals, atRev := syncData.Channels.ChannelsRemovedAtSequence(seq); len(channelRemovals) > 0 {
					change.DocID = docID
					change.RevID = atRev.RevTreeID
					change.SourceID = atRev.CurrentSource
					change.Version = base.HexCasToUint64(atRev.CurrentVersion)
					change.Channels = channelRemovals
				} else {
					change.UnusedSequence = true // treat as unused sequence when sequence is not channel removal
				}

				c.processEntry(ctx, change)
			}
		}
		if len(seqsCached) > 0 {
			base.DebugfCtx(ctx, base.KeyCache, "Received deduplicated seqs in recent_sequences property for (%q / %q): %v", base.UD(docID), syncData.GetRevTreeID(), seqsCached)
		}
	}

	// Now add the entry for the new doc revision:
	if len(rawUserXattr) > 0 {
		collection.revisionCache.Remove(ctx, docID, syncData.GetRevTreeID())
	}
	// remove the local doc from the revision cache if the change is a result of a conflict resolution that resulted
	// in local wins, given the HLV will have been updated but CV not changed
	if syncData.Flags&channels.UnchangedCV != 0 {
		vrs := Version{
			SourceID: syncData.RevAndVersion.CurrentSource,
			Value:    base.HexCasToUint64(syncData.RevAndVersion.CurrentVersion),
		}
		collection.revisionCache.Remove(ctx, docID, vrs.String())
	}

	change := &LogEntry{
		Sequence:     syncData.Sequence,
		DocID:        docID,
		RevID:        syncData.GetRevTreeID(),
		Flags:        syncData.Flags,
		TimeReceived: timeReceived,
		Channels:     syncData.Channels,
		CollectionID: event.CollectionID,
	}
	if len(feedData.rawVV) > 0 {
		change.SourceID = syncData.RevAndVersion.CurrentSource
		change.Version = base.HexCasToUint64(syncData.RevAndVersion.CurrentVersion)
	}

	millisecondLatency := int(latency / time.Millisecond)

	// If latency is larger than 1 minute or is negative there is likely an issue and this should be clear to the user
	if millisecondLatency >= 60*1000 {
		base.InfofCtx(ctx, base.KeyChanges, "Received #%d after %3dms (%q / %q)", change.Sequence, millisecondLatency, base.UD(change.DocID), change.RevID)
	} else {
		base.DebugfCtx(ctx, base.KeyChanges, "Received #%d after %3dms (%q / %q)", change.Sequence, millisecondLatency, base.UD(change.DocID), change.RevID)
	}

	// Each processEntry call is ordered separately, and the channels of what it caches are merged for notification
	// once the watermark covers those entries. So one event's unused sequences, recent-sequence removals and revision
	// can wake a feed in separate broadcasts. That costs at most an extra wake, since a feed is only woken for entries
	// it can already read.
	c.processEntry(ctx, change)
}

// FeedLatency measures from timeSaved or the time the cache started working the feed, whichever is
// later, so a document written before startup is not charged for the time the cache was down.
// Returns zero when timeSaved is unset.
func FeedLatency(timeSaved, initTime, now time.Time) time.Duration {
	if timeSaved.IsZero() {
		return 0
	}
	if timeSaved.After(initTime) {
		return now.Sub(timeSaved)
	}
	return now.Sub(initTime)
}

// Simplified principal limited to properties needed by caching
type cachePrincipal struct {
	Name     string `json:"name"`
	Sequence uint64 `json:"sequence"`
}

func (c *changeCache) Remove(ctx context.Context, collectionID uint32, docIDs []string, startTime time.Time) (count int) {
	return c.channelCache.Remove(ctx, collectionID, docIDs, startTime)
}

// Principals unmarshalled during caching don't need to instantiate a real principal - we're just using name and seq from the document
func (c *changeCache) unmarshalCachePrincipal(docJSON []byte) (cachePrincipal, error) {
	var principal cachePrincipal
	err := base.JSONUnmarshal(docJSON, &principal)
	return principal, err
}

// Process unused sequence notification.  Extracts sequence from docID and sends to cache for buffering
func (c *changeCache) processUnusedSequence(ctx context.Context, docID string, timeReceived channels.FeedTimestamp) {
	sequenceStr := strings.TrimPrefix(docID, c.metaKeys.UnusedSeqPrefix())
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to identify sequence number for unused sequence notification with key: %s, error: %v", base.UD(docID), err)
		return
	}
	c.releaseUnusedSequence(ctx, sequence, timeReceived)

}

func (c *changeCache) releaseUnusedSequence(ctx context.Context, sequence uint64, timeReceived channels.FeedTimestamp) {
	change := &LogEntry{
		Sequence:       sequence,
		TimeReceived:   timeReceived,
		UnusedSequence: true,
	}
	base.InfofCtx(ctx, base.KeyCache, "Received #%d (unused sequence)", sequence)

	// Waiters are woken only if the unused sequence is released; a buffered or duplicate one wakes nobody.
	c.processEntry(ctx, change)
}

// releaseUnusedSequenceRange will handle unused sequence range arriving over DCP. It will batch remove from skipped or
// push a range to pending sequences, or both.
func (c *changeCache) releaseUnusedSequenceRange(ctx context.Context, fromSequence uint64, toSequence uint64, timeReceived channels.FeedTimestamp) {

	base.InfofCtx(ctx, base.KeyCache, "Received #%d-#%d (unused sequence range)", fromSequence, toSequence)

	// if range is single value, just run sequence through process entry and return early
	if fromSequence == toSequence {
		change := &LogEntry{
			Sequence:       toSequence,
			TimeReceived:   timeReceived,
			UnusedSequence: true,
		}
		c.processEntry(ctx, change)
		return
	}

	// push unused range to either pending or skipped lists based on current state of the change cache
	c.processUnusedRange(ctx, fromSequence, toSequence, timeReceived)
}

// processUnusedRange handles pushing unused range to pending or skipped lists
func (c *changeCache) processUnusedRange(ctx context.Context, fromSequence, toSequence uint64, timeReceived channels.FeedTimestamp) {
	c.orderAndDispatch(ctx, func() {
		c._orderUnusedRange(ctx, fromSequence, toSequence, timeReceived)
	})
}

// _orderUnusedRange is the ordering half of processUnusedRange. Callers must hold c.lock.
func (c *changeCache) _orderUnusedRange(ctx context.Context, fromSequence, toSequence uint64, timeReceived channels.FeedTimestamp) {
	var numSkipped int64
	if toSequence < c.nextSequence {
		// batch remove from skipped
		var removed bool
		numSkipped, removed = c.skippedSeqs.processUnusedSequenceRangeAtSkipped(ctx, fromSequence, toSequence)
		// The range leaves skippedSeqs here, during ordering, rather than once a run is cached as a late sequence
		// does: it has no entries to cache, so there is nothing a reader could reach before it is readable. The
		// stable sequence may rise at once, but the watermark still bounds it below anything not yet cached.
		//
		// Waiters on the unused key are woken by releasing the range as an unused entry, so the wake is sent once the
		// watermark has passed the range, as every other wake is. It skips _releaseEntry: nextSequence is already past
		// the range, and it was never in receivedSeqs. While dispatch is synchronous the watermark always has passed
		// the range by the time it arrives: the hold that skipped it released past it, and that hold's runs retired
		// before it ended. Once runs are cached asynchronously (CBG-6120) they may still be in flight. With runs of 2:
		//
		//	hold H1  skips 101-104, releases 105-108 as t1 [105 106] and t2 [107 108]   watermark 100
		//	hold H2  101-104 arrives, leaves skippedSeqs, is released as t3 [101-104]   watermark 100
		//	         t1 and t2 are cached                                               watermark 108
		//	         t3 retires, which runs in ticket order put after t2, and wakes     watermark 108
		//
		// Waking as the range arrives would send the wake with the watermark at 100.
		//
		// This is the only ordering call in its hold, so the entry is first in the release and only a resumed drain,
		// of higher sequences, can follow it. It ends an inner run only at a run size of 1, and that run's
		// releasedThrough, the range's end, is then below the watermark, which only ever rises.
		if removed {
			c.released = append(c.released, &LogEntry{Sequence: fromSequence, EndSequence: toSequence, UnusedSequence: true})
			c.releaseBudget--
		}
	} else if fromSequence >= c.nextSequence {
		// whole range to pending
		c._pushRangeToPending(fromSequence, toSequence, timeReceived)
		// unblock any pending sequences we can after new range(s) have been pushed to pending
		c._releasePending(ctx)
		c.internalStats.pendingSeqLen = len(c.pendingLogs)
	} else {
		// An unused sequence range than includes c.nextSequence in the middle of the range
		// isn't possible under normal processing - unused sequence ranges will normally be moved
		// from pending to skipped in their entirety, as it's the processing of the pending sequence
		// *after* the range that triggers the range to be skipped.  A partial range in skipped means
		// a duplicate entry with a sequence within the bounds of the range was previously present
		// in pending.
		base.WarnfCtx(ctx, "unused sequence range of #%d to %d contains duplicate sequences, will be ignored", fromSequence, toSequence)
	}
	if numSkipped == 0 {
		c.db.BroadcastSlowMode.CompareAndSwap(true, false)
	}
}

// _pushRangeToPending will push an unused sequence range to pendingLogs
func (c *changeCache) _pushRangeToPending(startSeq, endSeq uint64, timeReceived channels.FeedTimestamp) {

	entry := &LogEntry{
		TimeReceived:   timeReceived,
		Sequence:       startSeq,
		EndSequence:    endSeq,
		UnusedSequence: true,
	}
	heap.Push(&c.pendingLogs, entry)

}

// Process unused sequence notification.  Extracts sequence from docID and sends to cache for buffering
func (c *changeCache) processUnusedSequenceRange(ctx context.Context, docID string) {
	// _sync:unusedSequences:fromSeq:toSeq
	sequencesStr := strings.TrimPrefix(docID, c.metaKeys.UnusedSeqRangePrefix())
	sequences := strings.Split(sequencesStr, ":")
	if len(sequences) != 2 {
		return
	}

	fromSequence, err := strconv.ParseUint(sequences[0], 10, 64)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to identify from sequence number for unused sequences notification with key: %s, error: %s", base.UD(docID), err)
		return
	}
	toSequence, err := strconv.ParseUint(sequences[1], 10, 64)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to identify to sequence number for unused sequence notification with key: %s, error: %s", base.UD(docID), err)
		return
	}

	c.releaseUnusedSequenceRange(ctx, fromSequence, toSequence, channels.NewFeedTimestampFromNow())
}

// fetchMetadataDocBody fetches the full document body via a KV get from the metadata store.
// This works around the xattr-only caching feed not including document bodies,
// which are required for metadata doc types (e.g. user/role principal docs).
func (c *changeCache) fetchMetadataDocBody(ctx context.Context, docID string, expectedCas uint64) ([]byte, error) {
	docBody, cas, err := c.db.MetadataStore.GetRaw(ctx, docID)
	if err != nil {
		return nil, err
	}
	if cas != expectedCas {
		return nil, sgbucket.CasMismatchErr{Expected: expectedCas, Actual: cas}
	}
	return docBody, nil
}

func (c *changeCache) processPrincipalDoc(ctx context.Context, docID string, docJSON []byte, isUser bool, timeReceived channels.FeedTimestamp) {

	// Currently the cache isn't really doing much with user docs; mostly it needs to know about
	// them because they have sequence numbers, so without them the sequence of sequences would
	// have gaps in it, causing later sequences to get stuck in the queue.
	princ, err := c.unmarshalCachePrincipal(docJSON)
	if err != nil {
		base.WarnfCtx(ctx, "changeCache: Error unmarshaling doc %q: %v", base.UD(docID), err)
		return
	}
	sequence := princ.Sequence

	if sequence <= c.initialSequence.Load() {
		return // Tap is sending us an old value from before I started up; ignore it
	}

	// Now add the (somewhat fictitious) entry:
	change := &LogEntry{
		Sequence:     sequence,
		TimeReceived: timeReceived,
		IsPrincipal:  true,
	}
	if isUser {
		change.DocID = "_user/" + princ.Name
	} else {
		change.DocID = "_role/" + princ.Name
	}

	base.InfofCtx(ctx, base.KeyChanges, "Received #%d (%q)", change.Sequence, base.UD(change.DocID))

	c.processEntry(ctx, change)
}

// processEntry handles a newly-arrived LogEntry: it caches it, with any pending entries it releases, or holds it in
// pending until the sequences before it arrive. The channels of everything cached are merged for notification.
func (c *changeCache) processEntry(ctx context.Context, change *LogEntry) {
	c.orderAndDispatch(ctx, func() {
		c._orderEntry(ctx, change)
	})
}

// _orderEntry is the ordering half of processEntry: it decides whether change is released now, held in pending, or
// ignored as a duplicate. Callers must hold c.lock.
func (c *changeCache) _orderEntry(ctx context.Context, change *LogEntry) {
	if c.logsDisabled {
		return
	}

	sequence := change.Sequence
	if change.Sequence > c.internalStats.highSeqFeed {
		c.internalStats.highSeqFeed = change.Sequence
	}

	// Duplicate handling - there are a few cases where processEntry can be called multiple times for a sequence:
	//   - recentSequences for rapidly updated documents
	//   - principal mutations that don't increment sequence
	// We can cancel processing early in these scenarios.
	// Check if this is a duplicate of an already processed sequence
	if sequence < c.nextSequence && !change.Skipped {
		// check for presence in skippedSeqs, it's possible that change.skipped can be marked false in recent sequence handling
		// but this change is subsequently pushed to skipped before acquiring cache mutex in this function
		if !c.WasSkipped(sequence) {
			base.DebugfCtx(ctx, base.KeyCache, "  Ignoring duplicate of #%d", sequence)
			return
		} else {
			change.Skipped = true
		}
	}

	// Check if this is a duplicate of a pending sequence
	if _, found := c.receivedSeqs[sequence]; found {
		base.DebugfCtx(ctx, base.KeyCache, "  Ignoring duplicate of #%d", sequence)
		return
	}
	c.receivedSeqs[sequence] = struct{}{}

	if sequence == c.nextSequence || c.nextSequence == 0 {
		// This is the expected next sequence so we can release it now:
		c._releaseEntry(change)
		// Also release any pending sequences that are now contiguous:
		c._releasePending(ctx)
	} else if sequence > c.nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		heap.Push(&c.pendingLogs, change)
		numPending := len(c.pendingLogs)
		c.internalStats.pendingSeqLen = numPending
		if base.LogDebugEnabled(ctx, base.KeyCache) {
			base.DebugfCtx(ctx, base.KeyCache, "  Deferring #%d (%d now waiting for #%d...#%d) doc %q / %q",
				sequence, numPending, c.nextSequence, c.pendingLogs[0].Sequence-1, base.UD(change.DocID), change.RevID)
		}
		// Update max pending high watermark stat
		if numPending > c.internalStats.maxPending {
			c.internalStats.maxPending = numPending
		}

		if numPending > c.options.CachePendingSeqMaxNum {
			// Too many pending; release the oldest one:
			c._releasePending(ctx)
		}
	} else if sequence > c.initialSequence.Load() {
		// Out-of-order sequence received!
		// Remove from skipped sequence queue
		if !change.Skipped {
			// Error removing from skipped sequences
			base.DebugfCtx(ctx, base.KeyCache, "  Received unexpected out-of-order change - not in skippedSeqs (seq %d, expecting %d) doc %q / %q", sequence, c.nextSequence, base.UD(change.DocID), change.RevID)
		} else {
			base.DebugfCtx(ctx, base.KeyCache, "  Received previously skipped out-of-order change (seq %d, expecting %d) doc %q / %q ", sequence, c.nextSequence, base.UD(change.DocID), change.RevID)
		}

		c._releaseEntry(change)
		// The run holding it removes it from skipped once it is cached (see dispatchRun), so that lowSequence doesn't
		// get incremented until results are available in cache.
		//
		// TODO: CBG-6120 - once runs are cached without c.lock, the sequence has left receivedSeqs but is still in
		// skippedSeqs until its run removes it, so a duplicate arriving in that window passes WasSkipped and is released
		// again. It must stay visible to the duplicate check until its run has removed it from skippedSeqs.
	}
}

// orderAndDispatch is the single lock hold every ordering entry point goes through. order decides what is
// released, within a budget of one run for each free tracker slot, then _dispatchReleased caches it before the lock
// is released, so no other ordering call can observe a released entry that isn't cached yet.
//
// A hold whose budget ran out leaves releasable entries at the front of pending. No arrival will match them, since
// they have already arrived, so every hold resumes that drain once order returns, whatever order did.
func (c *changeCache) orderAndDispatch(ctx context.Context, order func()) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.releaseBudget = int(c.tracker.Capacity()-c.tracker.InFlight(c.nextTicket)) * c.runSize
	order()
	// Several of order's paths never drain pending: a document landing in pending behind a gap, a duplicate, a late
	// sequence, an unused range removed from skippedSeqs. Any of them may follow a hold that ran out of budget, so the
	// drain is resumed here. When order did drain, this is a no-op: a drain cut short by the budget leaves none to
	// spend, and one stopped at a gap leaves nothing releasable.
	if !c.logsDisabled && c.releaseBudget > 0 && c._pendingReleasable() {
		c._releasePending(ctx)
	}
	if c.releaseBudget <= 0 && c._pendingReleasable() {
		c.numPartialDrains++
	}
	c._dispatchReleased(ctx)
	c._setPublishedNextSequence()
}

// _pendingReleasable reports whether the oldest pending entry can be released now, without waiting for anything to
// arrive. Outside a drain cut short by its budget, a drain always leaves the oldest pending entry above
// nextSequence. Callers must hold c.lock.
func (c *changeCache) _pendingReleasable() bool {
	return len(c.pendingLogs) > 0 && c.pendingLogs[0].Sequence <= c.nextSequence
}

// _releaseEntry moves the ordering state past change and queues it for dispatch at the end of the lock hold, using
// one unit of the hold's budget. It doesn't touch the channel caches. Callers must hold c.lock.
func (c *changeCache) _releaseEntry(change *LogEntry) {
	if change.Sequence >= c.nextSequence {
		c.nextSequence = change.Sequence + 1
	}
	// check if change is unused sequence range
	if change.EndSequence != 0 {
		c.nextSequence = change.EndSequence + 1
	}
	delete(c.receivedSeqs, change.Sequence)
	c.released = append(c.released, change)
	c.releaseBudget--
}

// _releasePending releases the first change(s) from pendingLogs if they're the next sequence.  If not, and we've been
// waiting too long for nextSequence, move nextSequence to skipped queue. Released entries are cached by
// _dispatchReleased at the end of the lock hold.
//
// It stops when the hold's budget runs out, leaving the rest in pending for a later hold to resume. The check comes
// before any other, so a sequence the budget holds back is never pushed to skipped: the skip only applies when the
// oldest pending entry is not the next sequence. Callers must hold c.lock.
func (c *changeCache) _releasePending(ctx context.Context) {
	var isNext bool

	for len(c.pendingLogs) > 0 && c.releaseBudget > 0 {
		oldestPending := c.pendingLogs[0]
		isNext = oldestPending.Sequence == c.nextSequence

		if isNext {
			oldestPending = c._popPendingLog(ctx)
			c._releaseEntry(oldestPending)
		} else if oldestPending.Sequence < c.nextSequence {
			// oldest pending is lower than next sequence, should be ignored
			base.InfofCtx(ctx, base.KeyCache, "Oldest entry in pending logs %v (%d, %d) is earlier than cache next sequence (%d), ignoring as sequence has already been cached", base.UD(oldestPending.DocID), oldestPending.Sequence, oldestPending.EndSequence, c.nextSequence)
			oldestPending = c._popPendingLog(ctx)

			// If the oldestPending was a range that extended past nextSequence, the part past nextSequence is
			// still unreleased. Release it like any other entry, so nextSequence and the channel cache's high
			// sequence move together.
			if oldestPending.IsUnusedRange() && oldestPending.EndSequence >= c.nextSequence {
				c._releaseEntry(oldestPending)
			}
		} else if len(c.pendingLogs) > c.options.CachePendingSeqMaxNum || c.pendingLogs[0].TimeReceived.OlderOrEqual(c.options.CachePendingSeqMaxWait) {
			//  Skip all sequences up to the oldest Pending
			c.PushSkipped(ctx, c.nextSequence, oldestPending.Sequence-1)
			c.nextSequence = oldestPending.Sequence
		} else {
			// nextSequence is not in pending logs, and pending logs size/age doesn't trigger skipped sequences
			break
		}
	}

	c.internalStats.pendingSeqLen = len(c.pendingLogs)

	atomic.StoreInt64(&c.lastAddPendingTime, time.Now().UnixNano())
}

// _popPendingLog pops the next pending LogEntry from the c.pendingLogs heap.  When the popped entry is an unused range,
// performs a defensive check for duplicates with the next entry in pending.  If unused range overlaps with next entry,
// reduces the unused range to stop at the next pending entry.
func (c *changeCache) _popPendingLog(ctx context.Context) *LogEntry {
	poppedEntry := heap.Pop(&c.pendingLogs).(*LogEntry)
	// If it's not a range, no additional handling needed
	if !poppedEntry.IsUnusedRange() {
		return poppedEntry
	}
	// If there are no more pending logs, no additional handling needed
	if len(c.pendingLogs) == 0 {
		return poppedEntry
	}

	nextPendingEntry := c.pendingLogs[0]
	// If popped entry range does not overlap with next pending entry, no additional handling needed
	//  e.g. popped [15-20], nextPendingEntry is [25]
	if poppedEntry.EndSequence < nextPendingEntry.Sequence {
		return poppedEntry
	}

	// If nextPendingEntry's sequence duplicates the start of the unused range, ignored popped entry and return next entry instead
	//   e.g. popped [15-20], nextPendingEntry is [15]
	if poppedEntry.Sequence == nextPendingEntry.Sequence {
		base.InfofCtx(ctx, base.KeyCache, "Unused sequence range in pendingLogs (%d, %d) has start equal to next pending sequence (%s, %d) - unused range will be ignored", poppedEntry.Sequence, poppedEntry.EndSequence, nextPendingEntry.DocID, nextPendingEntry.Sequence)
		return c._popPendingLog(ctx)
	}

	// Otherwise, reduce the popped unused range to end before the next pending sequence
	//  e.g. popped [15-20], nextPendingEntry is [18]
	base.InfofCtx(ctx, base.KeyCache, "Unused sequence range in pendingLogs (%d, %d) overlaps with next pending sequence (%s, %d) - unused range will be truncated", poppedEntry.Sequence, poppedEntry.EndSequence, nextPendingEntry.DocID, nextPendingEntry.Sequence)
	poppedEntry.EndSequence = nextPendingEntry.Sequence - 1
	return poppedEntry
}

func (c *changeCache) GetStableSequence(docID string) SequenceID {
	// Stable sequence is independent of docID in changeCache
	return SequenceID{Seq: c.LastSequence()}
}

func (c *changeCache) getChannelCache() ChannelCache {
	return c.channelCache
}

// ////// CHANGE ACCESS:

func (c *changeCache) GetChanges(ctx context.Context, channel channels.ID, options ChangesOptions) ([]*LogEntry, error) {

	if c.stopped.IsTrue() {
		return nil, base.HTTPErrorf(503, "Database closed")
	}
	return c.channelCache.GetChanges(ctx, channel, options)
}

// Returns the sequence number the cache is up-to-date with.
func (c *changeCache) LastSequence() uint64 {

	lastSequence := c.getPublishedNextSequence() - 1
	return lastSequence
}

func (c *changeCache) getOldestSkippedSequence(ctx context.Context) uint64 {
	oldestSkippedSeq := c.skippedSeqs.getOldest()
	if oldestSkippedSeq > 0 {
		base.DebugfCtx(ctx, base.KeyChanges, "Get oldest skipped, returning: %d", oldestSkippedSeq)
	}
	return oldestSkippedSeq
}

// Set the initial sequence.  Presumes that change cache is already locked.
func (c *changeCache) _setInitialSequence(initialSequence uint64) {
	c.initialSequence.Store(initialSequence)
	c.nextSequence = initialSequence + 1
	c._setPublishedNextSequence()
}

// getPublishedNextSequence returns nextSequence as of the last release of c.lock, without taking the lock.
// Unlike a read-lock read, it doesn't wait for a critical section in progress, it returns the value from
// before that section, where a read-lock reader would block and then see the value after it. Nothing from
// that section is visible until the lock is released, so callers can't have observed it yet. Ordering code
// must read c.nextSequence directly.
func (c *changeCache) getPublishedNextSequence() uint64 {
	if publishSeq := c.publishedNextSequence.Load(); publishSeq != 0 {
		return publishSeq
	}
	// Nothing can be published until Start sets initialSequence, Init will hold change cache lock till then so this
	// blocks until this is released
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.nextSequence
}

// _setPublishedNextSequence sets the atomic copy of nextSequence that is the visible publish version of it. Callers must hold c.lock.
func (c *changeCache) _setPublishedNextSequence() {
	c.publishedNextSequence.Store(c.nextSequence)
}

// ////// LOG PRIORITY QUEUE -- container/heap callbacks that should not be called directly.   Use heap.Init/Push/etc()

func (h LogPriorityQueue) Len() int           { return len(h) }
func (h LogPriorityQueue) Less(i, j int) bool { return h[i].Sequence < h[j].Sequence }
func (h LogPriorityQueue) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *LogPriorityQueue) Push(x any) {
	*h = append(*h, x.(*LogEntry))
}

func (h *LogPriorityQueue) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// ////// SKIPPED SEQUENCE QUEUE

func (c *changeCache) RemoveSkipped(x uint64) error {
	_, numSkipped, err := c.skippedSeqs.list.Remove(NewSingleSkippedSequenceEntryAt(x, 0))
	if err != nil {
		return fmt.Errorf("sequence %d not found in the skipped list, err: %v", x, err)
	}
	if numSkipped == 0 {
		c.db.BroadcastSlowMode.CompareAndSwap(true, false)
	}
	if c.removeSkippedHook != nil {
		c.removeSkippedHook(x)
	}
	return nil
}

func (c *changeCache) WasSkipped(x uint64) bool {
	return c.skippedSeqs.Contains(x)
}

func (c *changeCache) PushSkipped(ctx context.Context, startSeq uint64, endSeq uint64) {
	if startSeq > endSeq {
		base.InfofCtx(ctx, base.KeyCache, "cannot push negative skipped sequence range to skipped list: %d %d", startSeq, endSeq)
		return
	}
	err := c.skippedSeqs.PushSkippedSequenceEntry(NewSkippedSequenceRangeEntry(startSeq, endSeq))
	if err != nil {
		base.InfofCtx(ctx, base.KeyCache, "Error pushing skipped sequence range to skipped list: %v", err)
		return
	}
	c.db.BroadcastSlowMode.CompareAndSwap(false, true)
}

// waitForSequenceNotSkipped will wait until the specified sequence is no longer in the skipped list.
func (c *changeCache) waitForSequenceNotSkipped(ctx context.Context, sequence uint64, maxWaitTime time.Duration) error {
	startTime := time.Now()

	worker := func() (bool, error, any) {
		if c.channelCache.GetHighCacheSequence() >= sequence {
			foundInMissing := c.skippedSeqs.Contains(sequence)
			if !foundInMissing {
				base.DebugfCtx(ctx, base.KeyCache, "waitForSequenceNotSkipped(%d) took %v", sequence, time.Since(startTime))
				return false, nil, nil
			}
		}
		// retry
		return true, nil, nil
	}

	ctx, cancel := context.WithDeadline(ctx, startTime.Add(maxWaitTime))
	sleeper := base.SleeperFuncCtx(base.CreateMaxDoublingSleeperFunc(math.MaxInt64, 1, 100), ctx)
	err, _ := base.RetryLoop(ctx, fmt.Sprintf("waitForSequenceNotSkipped(%d)", sequence), worker, sleeper)
	cancel()
	return err
}

// _getMaxStableCached returns the highest sequence at and below which everything is cached and nothing is still to
// arrive: the watermark, held below the oldest skipped sequence.
func (c *changeCache) _getMaxStableCached(ctx context.Context) uint64 {
	watermark := c.channelCache.GetHighCacheSequence()
	if oldestSkipped := c.getOldestSkippedSequence(ctx); oldestSkipped > 0 {
		return min(oldestSkipped-1, watermark)
	}
	return watermark
}

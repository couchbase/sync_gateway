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
)

const (
	DefaultCachePendingSeqMaxNum  = 10000            // Max number of waiting sequences
	DefaultCachePendingSeqMaxWait = 5 * time.Second  // Max time we'll wait for a pending sequence before sending to missed queue
	DefaultSkippedSeqMaxWait      = 60 * time.Minute // Max time we'll wait for an entry in the missing before purging
	QueryTombstoneBatch           = 250              // Max number of tombstones checked per query during Compact
	unusedSeqKey                  = "_unusedSeqKey"  // Key used by ChangeWaiter to mark unused sequences
	unusedSeqCollectionID         = 0                // Collection ID used by ChangeWaiter to mark unused sequences
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
	db                 *DatabaseContext
	logCtx             context.Context                     // fix in sg-bucket to ProcessEvent
	logsDisabled       bool                                // If true, ignore incoming tap changes
	nextSequence       uint64                              // Next consecutive sequence number to add.  State variable for sequence buffering tracking.  Should use getNextSequence() rather than accessing directly.
	initialSequence    uint64                              // DB's current sequence at startup time.
	receivedSeqs       map[uint64]struct{}                 // Set of all sequences received
	pendingLogs        LogPriorityQueue                    // Out-of-sequence entries waiting to be cached
	notifyChangeFunc   func(context.Context, channels.Set) // Client callback that notifies of channel changes
	started            base.AtomicBool                     // Set by the Start method
	stopped            base.AtomicBool                     // Set by the Stop method
	skippedSeqs        *SkippedSequenceSkiplist            // Skipped sequences still pending on the DCP caching feed
	lock               sync.Mutex                          // Coordinates access to struct fields
	options            CacheOptions                        // Cache config
	terminator         chan bool                           // Signal termination of background goroutines
	backgroundTasks    []BackgroundTask                    // List of background tasks.
	initTime           time.Time                           // Cache init time - used for latency calculations
	channelCache       ChannelCache                        // Underlying channel cache
	lastAddPendingTime int64                               // The most recent time _addPendingLogs was run, as epoch time
	internalStats      changeCacheStats                    // Running stats for the change cache.  Only applied to expvars on a call to changeCache.updateStats
	cfgEventCallback   base.CfgEventNotifyFunc             // Callback for Cfg updates received over the caching feed
	sgCfgPrefix        string                              // Prefix for SG Cfg doc keys
	metaKeys           *base.MetadataKeys                  // Metadata key formatter
}

type changeCacheStats struct {
	highSeqFeed   uint64
	pendingSeqLen int
	maxPending    int
}

func (c *changeCache) updateStats(ctx context.Context) {

	c.lock.Lock()
	defer c.lock.Unlock()
	if c.db == nil {
		return
	}
	// grab skipped sequence stats
	skippedSequenceListStats := c.skippedSeqs.getStats()

	c.db.DbStats.Database().HighSeqFeed.SetIfMax(int64(c.internalStats.highSeqFeed))
	c.db.DbStats.Cache().PendingSeqLen.Set(int64(c.internalStats.pendingSeqLen))
	c.db.DbStats.CBLReplicationPull().MaxPending.SetIfMax(int64(c.internalStats.maxPending))
	c.db.DbStats.Cache().HighSeqStable.Set(int64(c._getMaxStableCached(ctx)))
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
}

func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CachePendingSeqMaxWait: DefaultCachePendingSeqMaxWait,
		CachePendingSeqMaxNum:  DefaultCachePendingSeqMaxNum,
		CacheSkippedSeqMaxWait: DefaultSkippedSeqMaxWait,
		ChannelCacheOptions: ChannelCacheOptions{
			ChannelCacheAge:             DefaultChannelCacheAge,
			ChannelCacheMinLength:       DefaultChannelCacheMinLength,
			ChannelCacheMaxLength:       DefaultChannelCacheMaxLength,
			MaxNumChannels:              DefaultChannelCacheMaxNumber,
			CompactHighWatermarkPercent: DefaultCompactHighWatermarkPercent,
			CompactLowWatermarkPercent:  DefaultCompactLowWatermarkPercent,
			ChannelQueryLimit:           DefaultQueryPaginationLimit,
		},
	}
}

// ////// HOUSEKEEPING:

// Initializes a new changeCache.
// lastSequence is the last known database sequence assigned.
// notifyChangeFunc is an optional function that will be called to notify of channel changes.
// After calling Init(), you must call .Start() to start using the cache, otherwise it will be in a locked state
// and callers will block on trying to obtain the lock.

func (c *changeCache) Init(ctx context.Context, dbContext *DatabaseContext, channelCache ChannelCache, notifyChange func(context.Context, channels.Set), options *CacheOptions, metaKeys *base.MetadataKeys) error {
	c.db = dbContext
	c.logCtx = ctx

	c.notifyChangeFunc = notifyChange
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

// Empty out all channel caches.
func (c *changeCache) Clear(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Reset initialSequence so that any new channel caches have their validFrom set to the current last sequence
	// the point at which the change cache was initialized / re-initialized.
	// No need to touch c.nextSequence here, because we don't want to touch the sequence buffering state.
	var err error
	c.initialSequence, err = c.db.LastSequence(ctx)
	if err != nil {
		return err
	}

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

// Triggers addPendingLogs if it hasn't been run in CachePendingSeqMaxWait.  Error returned to fulfil BackgroundTaskFunc signature.
func (c *changeCache) InsertPendingEntries(ctx context.Context) error {

	lastAddPendingLogsTime := atomic.LoadInt64(&c.lastAddPendingTime)
	if time.Since(time.Unix(0, lastAddPendingLogsTime)) < c.options.CachePendingSeqMaxWait {
		return nil
	}

	// Trigger _addPendingLogs to process any entries that have been pending too long:
	c.lock.Lock()
	changedChannels := c._addPendingLogs(ctx)
	c.lock.Unlock()

	c.notifyChange(ctx, changedChannels)

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
	docJSON := event.Value
	changedChannelsCombined := channels.Set{}

	timeReceived := channels.NewFeedTimestamp(&event.TimeReceived)
	// ** This method does not directly access any state of c, so it doesn't lock.
	// Is this a user/role doc for this database?
	switch docType {
	case DocTypeUnknown:
		return // no-op unknown doc type
	case DocTypeUser:
		c.processPrincipalDoc(ctx, docID, docJSON, true, timeReceived)
		return
	case DocTypeRole:
		c.processPrincipalDoc(ctx, docID, docJSON, false, timeReceived)
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

	// If this is a delete and there are no xattrs (no existing SG revision), we can ignore
	if event.Opcode == sgbucket.FeedOpDeletion && len(docJSON) == 0 {
		base.DebugfCtx(ctx, base.KeyCache, "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(docID))
		return
	}

	// If this is a binary document (and not one of the above types), we can ignore.  Currently only performing this check when xattrs
	// are enabled, because walrus doesn't support DataType on feed.
	if collection.UseXattrs() && event.DataType == base.MemcachedDataTypeRaw {
		return
	}

	// First unmarshal the doc (just its metadata, to save time/memory):
	doc, syncData, err := UnmarshalDocumentSyncDataFromFeed(docJSON, event.DataType, collection.UserXattrKey(), false)
	if err != nil {
		// Avoid log noise related to failed unmarshaling of binary documents.
		if event.DataType != base.MemcachedDataTypeRaw {
			base.DebugfCtx(ctx, base.KeyCache, "Unable to unmarshal sync metadata for feed document %q.  Will not be included in channel cache.  Error: %v", base.UD(docID), err)
		}
		if errors.Is(err, sgbucket.ErrEmptyMetadata) {
			base.WarnfCtx(ctx, "Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		}
		return
	}

	// If using xattrs and this isn't an SG write, we shouldn't attempt to cache.
	rawUserXattr := doc.Xattrs[collection.UserXattrKey()]
	if collection.UseXattrs() {
		if syncData == nil {
			return
		}
		var rawVV *rawHLV
		vv := doc.Xattrs[base.VvXattrName]
		if len(vv) > 0 {
			rawVV = base.Ptr(rawHLV(vv))
		}
		isSGWrite, _, _ := syncData.IsSGWrite(ctx, event.Cas, doc.Body, rawUserXattr, rawVV)
		if !isSGWrite {
			return
		}
	}

	// If not using xattrs and no sync metadata found, check whether we're mid-upgrade and attempting to read a doc w/ metadata stored in xattr
	// before ignoring the mutation.
	if !collection.UseXattrs() && !syncData.HasValidSyncData() {
		migratedDoc, _ := collection.checkForUpgrade(ctx, docID, DocUnmarshalNoHistory)
		if migratedDoc != nil && migratedDoc.Cas == event.Cas {
			base.InfofCtx(ctx, base.KeyCache, "Found mobile xattr on doc %q without %s property - caching, assuming upgrade in progress.", base.UD(docID), base.SyncPropertyName)
			syncData = &migratedDoc.SyncData
		} else {
			base.InfofCtx(ctx, base.KeyCache, "changeCache: Doc %q does not have valid sync data.", base.UD(docID))
			collection.dbStats().Cache().NonMobileIgnoredCount.Add(1)
			return
		}
	}

	if syncData.Sequence <= c.initialSequence {
		return // DCP is sending us an old value from before I started up; ignore it
	}

	// Measure feed latency from timeSaved or the time we started working the feed, whichever is later
	var feedLatency time.Duration
	if !syncData.TimeSaved.IsZero() {
		if syncData.TimeSaved.After(c.initTime) {
			feedLatency = time.Since(syncData.TimeSaved)
		} else {
			feedLatency = time.Since(c.initTime)
		}
		// Record latency when greater than zero
		feedNano := feedLatency.Nanoseconds()
		if feedNano > 0 {
			c.db.DbStats.Database().DCPReceivedTime.Add(feedNano)
		}
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
			changedChannels := c.processEntry(ctx, change)
			channelSet := channels.SetFromArrayNoValidate(changedChannels)
			changedChannelsCombined = changedChannelsCombined.Update(channelSet)
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
		nextSequence := c.getNextSequence()
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

				changedChannels := c.processEntry(ctx, change)
				channelSet := channels.SetFromArrayNoValidate(changedChannels)
				changedChannelsCombined = changedChannelsCombined.Update(channelSet)
			}
		}
		if len(seqsCached) > 0 {
			base.DebugfCtx(ctx, base.KeyCache, "Received deduplicated seqs in recent_sequences property for (%q / %q): %v", base.UD(docID), syncData.GetRevTreeID(), seqsCached)
		}
	}

	// Now add the entry for the new doc revision:
	if len(rawUserXattr) > 0 {
		collection.revisionCache.RemoveRevOnly(ctx, docID, syncData.GetRevTreeID())
	}
	// remove the local doc from the revision cache if the change is a result of a conflict resolution that resulted
	// in local wins, given the HLV will have been updated but CV not changed
	if syncData.Flags&channels.UnchangedCV != 0 {
		vrs := Version{
			SourceID: syncData.RevAndVersion.CurrentSource,
			Value:    base.HexCasToUint64(syncData.RevAndVersion.CurrentVersion),
		}
		collection.revisionCache.RemoveCVOnly(ctx, docID, &vrs)
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
	if len(doc.Xattrs[base.VvXattrName]) > 0 {
		change.SourceID = syncData.RevAndVersion.CurrentSource
		change.Version = base.HexCasToUint64(syncData.RevAndVersion.CurrentVersion)
	}

	millisecondLatency := int(feedLatency / time.Millisecond)

	// If latency is larger than 1 minute or is negative there is likely an issue and this should be clear to the user
	if millisecondLatency >= 60*1000 {
		base.InfofCtx(ctx, base.KeyChanges, "Received #%d after %3dms (%q / %q)", change.Sequence, millisecondLatency, base.UD(change.DocID), change.RevID)
	} else {
		base.DebugfCtx(ctx, base.KeyChanges, "Received #%d after %3dms (%q / %q)", change.Sequence, millisecondLatency, base.UD(change.DocID), change.RevID)
	}

	changedChannels := c.processEntry(ctx, change)
	channelSet := channels.SetFromArrayNoValidate(changedChannels)
	changedChannelsCombined = changedChannelsCombined.Update(channelSet)

	// Notify change listeners for all of the changed channels
	if c.notifyChangeFunc != nil && len(changedChannelsCombined) > 0 {
		c.notifyChangeFunc(ctx, changedChannelsCombined)
	}

}

// Simplified principal limited to properties needed by caching
type cachePrincipal struct {
	Name     string `json:"name"`
	Sequence uint64 `json:"sequence"`
}

func (c *changeCache) notifyChange(ctx context.Context, chs []channels.ID) {
	if c.notifyChangeFunc == nil || len(chs) == 0 {
		return
	}
	c.notifyChangeFunc(ctx, channels.SetFromArrayNoValidate(chs))
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

	// Since processEntry may unblock pending sequences, if there were any changed channels we need
	// to notify any change listeners that are working changes feeds for these channels
	var channelSet channels.Set
	changedChannels := c.processEntry(ctx, change)
	if changedChannels == nil {
		channelSet = channels.SetOfNoValidate(unusedSeqChannelID)
	} else {
		channelSet = channels.SetFromArrayNoValidate(changedChannels)
		channelSet.Add(unusedSeqChannelID)
	}
	if c.notifyChangeFunc != nil && len(channelSet) > 0 {
		c.notifyChangeFunc(ctx, channelSet)
	}
}

// releaseUnusedSequenceRange will handle unused sequence range arriving over DCP. It will batch remove from skipped or
// push a range to pending sequences, or both.
func (c *changeCache) releaseUnusedSequenceRange(ctx context.Context, fromSequence uint64, toSequence uint64, timeReceived channels.FeedTimestamp) {

	base.InfofCtx(ctx, base.KeyCache, "Received #%d-#%d (unused sequence range)", fromSequence, toSequence)

	allChangedChannels := channels.SetOfNoValidate(unusedSeqChannelID)

	// if range is single value, just run sequence through process entry and return early
	if fromSequence == toSequence {
		change := &LogEntry{
			Sequence:       toSequence,
			TimeReceived:   timeReceived,
			UnusedSequence: true,
		}
		changedChannels := c.processEntry(ctx, change)
		channelSet := channels.SetFromArrayNoValidate(changedChannels)
		allChangedChannels = allChangedChannels.Update(channelSet)
		if c.notifyChangeFunc != nil {
			c.notifyChangeFunc(ctx, allChangedChannels)
		}
		return
	}

	// push unused range to either pending or skipped lists based on current state of the change cache
	changedChannels := c.processUnusedRange(ctx, fromSequence, toSequence, timeReceived)
	allChangedChannels = allChangedChannels.Update(channels.SetFromArrayNoValidate(changedChannels))

	if c.notifyChangeFunc != nil {
		c.notifyChangeFunc(ctx, allChangedChannels)
	}
}

// processUnusedRange handles pushing unused range to pending or skipped lists
func (c *changeCache) processUnusedRange(ctx context.Context, fromSequence, toSequence uint64, timeReceived channels.FeedTimestamp) []channels.ID {
	c.lock.Lock()
	defer c.lock.Unlock()

	var numSkipped int64
	var changedChannels []channels.ID
	if toSequence < c.nextSequence {
		// batch remove from skipped
		numSkipped = c.skippedSeqs.processUnusedSequenceRangeAtSkipped(ctx, fromSequence, toSequence)
	} else if fromSequence >= c.nextSequence {
		// whole range to pending
		c._pushRangeToPending(fromSequence, toSequence, timeReceived)
		// unblock any pending sequences we can after new range(s) have been pushed to pending
		changedChannels = append(changedChannels, c._addPendingLogs(ctx)...)
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
	return changedChannels
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
		base.WarnfCtx(ctx, "Unable to identify from sequence number for unused sequences notification with key: %s, error:", base.UD(docID), err)
		return
	}
	toSequence, err := strconv.ParseUint(sequences[1], 10, 64)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to identify to sequence number for unused sequence notification with key: %s, error:", base.UD(docID), err)
		return
	}

	c.releaseUnusedSequenceRange(ctx, fromSequence, toSequence, channels.NewFeedTimestampFromNow())
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

	if sequence <= c.initialSequence {
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

	changedChannels := c.processEntry(ctx, change)

	c.notifyChange(ctx, changedChannels)
}

// processEntry handles a newly-arrived LogEntry and returns the changes channels from this revision.
// This can be any existing, removed or newly added channels. Its possible for channels slice returned to have duplicates
// in it. It is the callers responsibility to de-duplicate before notifying any changes.
func (c *changeCache) processEntry(ctx context.Context, change *LogEntry) []channels.ID {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.logsDisabled {
		return nil
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
			return nil
		} else {
			change.Skipped = true
		}
	}

	// Check if this is a duplicate of a pending sequence
	if _, found := c.receivedSeqs[sequence]; found {
		base.DebugfCtx(ctx, base.KeyCache, "  Ignoring duplicate of #%d", sequence)
		return nil
	}
	c.receivedSeqs[sequence] = struct{}{}

	changedChannels := make([]channels.ID, 0, len(change.Channels))
	if sequence == c.nextSequence || c.nextSequence == 0 {
		// This is the expected next sequence so we can add it now:
		changedChannels = c._addToCache(ctx, change)
		// Also add any pending sequences that are now contiguous:
		changedChannels = append(changedChannels, c._addPendingLogs(ctx)...)
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
			// Too many pending; add the oldest one:
			changedChannels = append(changedChannels, c._addPendingLogs(ctx)...)
		}
	} else if sequence > c.initialSequence {
		// Out-of-order sequence received!
		// Remove from skipped sequence queue
		if !change.Skipped {
			// Error removing from skipped sequences
			base.DebugfCtx(ctx, base.KeyCache, "  Received unexpected out-of-order change - not in skippedSeqs (seq %d, expecting %d) doc %q / %q", sequence, c.nextSequence, base.UD(change.DocID), change.RevID)
		} else {
			base.DebugfCtx(ctx, base.KeyCache, "  Received previously skipped out-of-order change (seq %d, expecting %d) doc %q / %q ", sequence, c.nextSequence, base.UD(change.DocID), change.RevID)
		}

		changedChannels = append(changedChannels, c._addToCache(ctx, change)...)
		// Add to cache before removing from skipped, to ensure lowSequence doesn't get incremented until results are available
		// in cache
		err := c.RemoveSkipped(sequence)
		if err != nil {
			base.DebugfCtx(ctx, base.KeyCache, "Error removing skipped sequence: #%d from cache: %v", sequence, err)
		}
	}
	return changedChannels
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (c *changeCache) _addToCache(ctx context.Context, change *LogEntry) []channels.ID {

	if change.Sequence >= c.nextSequence {
		c.nextSequence = change.Sequence + 1
	}
	// check if change is unused sequence range
	if change.EndSequence != 0 {
		c.nextSequence = change.EndSequence + 1
	}
	delete(c.receivedSeqs, change.Sequence)

	// If unused sequence, notify the cache and return
	if change.UnusedSequence {
		c.channelCache.AddUnusedSequence(change)
		return nil
	}

	if change.IsPrincipal {
		c.channelCache.AddPrincipal(change)
		return nil
	}

	// updatedChannels tracks the set of channels that should be notified of the change.  This includes
	// the change's active channels, as well as any channel removals for the active revision.
	updatedChannels := c.channelCache.AddToCache(ctx, change)
	if base.LogDebugEnabled(ctx, base.KeyChanges) {
		base.DebugfCtx(ctx, base.KeyChanges, " #%d ==> channels %v", change.Sequence, base.UD(updatedChannels))
	}

	if change.TimeReceived != 0 {
		c.db.DbStats.Database().DCPCachingCount.Add(1)
		c.db.DbStats.Database().DCPCachingTime.Add(change.TimeReceived.Since())
	}

	return updatedChannels
}

// _addPendingLogs Add the first change(s) from pendingLogs if they're the next sequence.  If not, and we've been
// waiting too long for nextSequence, move nextSequence to skipped queue.
// Returns the channels that changed. This may return the same channel more than once, channels should be deduplicated
// before notifying the changes.
func (c *changeCache) _addPendingLogs(ctx context.Context) []channels.ID {
	// pre allocate slice for changed channels, give size 5 to allow for some headroom so we
	// aren't constantly growing the slice
	changedChannels := make([]channels.ID, 0, 5)
	var isNext bool

	for len(c.pendingLogs) > 0 {
		oldestPending := c.pendingLogs[0]
		isNext = oldestPending.Sequence == c.nextSequence

		if isNext {
			oldestPending = c._popPendingLog(ctx)
			changedChannels = append(changedChannels, c._addToCache(ctx, oldestPending)...)
		} else if oldestPending.Sequence < c.nextSequence {
			// oldest pending is lower than next sequence, should be ignored
			base.InfofCtx(ctx, base.KeyCache, "Oldest entry in pending logs %v (%d, %d) is earlier than cache next sequence (%d), ignoring as sequence has already been cached", base.UD(oldestPending.DocID), oldestPending.Sequence, oldestPending.EndSequence, c.nextSequence)
			oldestPending = c._popPendingLog(ctx)

			// If the oldestPending was a range that extended past nextSequence, update nextSequence
			if oldestPending.IsUnusedRange() && oldestPending.EndSequence >= c.nextSequence {
				c.nextSequence = oldestPending.EndSequence + 1
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
	return changedChannels
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

	lastSequence := c.getNextSequence() - 1
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
	c.initialSequence = initialSequence
	c.nextSequence = initialSequence + 1
}

// Concurrent-safe get value of nextSequence
func (c *changeCache) getNextSequence() (nextSequence uint64) {
	c.lock.Lock()
	nextSequence = c.nextSequence
	c.lock.Unlock()
	return nextSequence
}

// ////// LOG PRIORITY QUEUE -- container/heap callbacks that should not be called directly.   Use heap.Init/Push/etc()

func (h LogPriorityQueue) Len() int           { return len(h) }
func (h LogPriorityQueue) Less(i, j int) bool { return h[i].Sequence < h[j].Sequence }
func (h LogPriorityQueue) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *LogPriorityQueue) Push(x interface{}) {
	*h = append(*h, x.(*LogEntry))
}

func (h *LogPriorityQueue) Pop() interface{} {
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

// waitForSequence blocks up to maxWaitTime until the given sequence has been received.
func (c *changeCache) waitForSequence(ctx context.Context, sequence uint64, maxWaitTime time.Duration) error {
	startTime := time.Now()

	worker := func() (bool, error, interface{}) {
		if c.getNextSequence() >= sequence+1 {
			base.DebugfCtx(ctx, base.KeyCache, "waitForSequence(%d) took %v", sequence, time.Since(startTime))
			return false, nil, nil
		}
		// retry
		return true, nil, nil
	}

	ctx, cancel := context.WithDeadline(ctx, startTime.Add(maxWaitTime))
	sleeper := base.SleeperFuncCtx(base.CreateMaxDoublingSleeperFunc(math.MaxInt64, 1, 100), ctx)
	err, _ := base.RetryLoop(ctx, fmt.Sprintf("waitForSequence(%d)", sequence), worker, sleeper)
	cancel()
	return err
}

// waitForSequenceNotSkipped blocks up to maxWaitTime until the given sequence has been received or skipped.
func (c *changeCache) waitForSequenceNotSkipped(ctx context.Context, sequence uint64, maxWaitTime time.Duration) error {
	startTime := time.Now()

	worker := func() (bool, error, interface{}) {
		if c.getNextSequence() >= sequence+1 {
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

func (c *changeCache) _getMaxStableCached(ctx context.Context) uint64 {
	oldestSkipped := c.getOldestSkippedSequence(ctx)
	if oldestSkipped > 0 {
		return oldestSkipped - 1
	}
	return c.nextSequence - 1
}

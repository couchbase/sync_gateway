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
	"container/list"
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

// Enable keeping a channel-log for the "*" channel (channel.UserStarChannel). The only time this channel is needed is if
// someone has access to "*" (e.g. admin-party) and tracks its changes feed.
var EnableStarChannelLog = true

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
	initialSequence    uint64                              // DB's current sequence at startup time. Should use getInitialSequence() rather than accessing directly.
	receivedSeqs       map[uint64]struct{}                 // Set of all sequences received
	pendingLogs        LogPriorityQueue                    // Out-of-sequence entries waiting to be cached
	notifyChange       func(context.Context, channels.Set) // Client callback that notifies of channel changes
	started            base.AtomicBool                     // Set by the Start method
	stopped            base.AtomicBool                     // Set by the Stop method
	skippedSeqs        *SkippedSequenceList                // Skipped sequences still pending on the TAP feed
	lock               sync.RWMutex                        // Coordinates access to struct fields
	options            CacheOptions                        // Cache config
	terminator         chan bool                           // Signal termination of background goroutines
	backgroundTasks    []BackgroundTask                    // List of background tasks.
	initTime           time.Time                           // Cache init time - used for latency calculations
	channelCache       ChannelCache                        // Underlying channel cache
	lastAddPendingTime int64                               // The most recent time _addPendingLogs was run, as epoch time
	internalStats      changeCacheStats                    // Running stats for the change cache.  Only applied to expvars on a call to changeCache.updateStats
	cfgEventCallback   base.CfgEventNotifyFunc             // Callback for Cfg updates recieved over the caching feed
	sgCfgPrefix        string                              // Prefix for SG Cfg doc keys
	metaKeys           *base.MetadataKeys                  // Metadata key formatter
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
	c.db.DbStats.Database().HighSeqFeed.SetIfMax(int64(c.internalStats.highSeqFeed))
	c.db.DbStats.Cache().PendingSeqLen.Set(int64(c.internalStats.pendingSeqLen))
	c.db.DbStats.CBLReplicationPull().MaxPending.SetIfMax(int64(c.internalStats.maxPending))
	c.db.DbStats.Cache().HighSeqStable.Set(int64(c._getMaxStableCached(ctx)))

}

type LogEntry channels.LogEntry

func (l LogEntry) String() string {
	return channels.LogEntry(l).String()
}

func (entry *LogEntry) IsRemoved() bool {
	return entry.Flags&channels.Removed != 0
}

func (entry *LogEntry) IsDeleted() bool {
	return entry.Flags&channels.Deleted != 0
}

// Returns false if the entry is either a removal or a delete
func (entry *LogEntry) IsActive() bool {
	return !entry.IsRemoved() && !entry.IsDeleted()
}

func (entry *LogEntry) SetRemoved() {
	entry.Flags |= channels.Removed
}

func (entry *LogEntry) SetDeleted() {
	entry.Flags |= channels.Deleted
}

type LogEntries []*LogEntry

// A priority-queue of LogEntries, kept ordered by increasing sequence #.
type LogPriorityQueue []*LogEntry

type SkippedSequence struct {
	seq       uint64
	timeAdded time.Time
}

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
// notifyChange is an optional function that will be called to notify of channel changes.
// After calling Init(), you must call .Start() to start useing the cache, otherwise it will be in a locked state
// and callers will block on trying to obtain the lock.

func (c *changeCache) Init(ctx context.Context, dbContext *DatabaseContext, channelCache ChannelCache, notifyChange func(context.Context, channels.Set), options *CacheOptions, metaKeys *base.MetadataKeys) error {
	c.db = dbContext
	c.logCtx = ctx

	c.notifyChange = notifyChange
	c.receivedSeqs = make(map[uint64]struct{})
	c.terminator = make(chan bool)
	c.initTime = time.Now()
	c.skippedSeqs = NewSkippedSequenceList()
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
	if c.notifyChange != nil && len(changedChannels) > 0 {
		c.notifyChange(ctx, changedChannels)
	}
	c.lock.Unlock()

	return nil
}

// Cleanup function, invoked periodically.
// Removes skipped entries from skippedSeqs that have been waiting longer
// than MaxChannelLogMissingWaitTime from the queue.  Attempts view retrieval
// prior to removal.  Only locks skipped sequence queue to build the initial set (GetSkippedSequencesOlderThanMaxWait)
// and subsequent removal (RemoveSkipped).
func (c *changeCache) CleanSkippedSequenceQueue(ctx context.Context) error {

	oldSkippedSequences := c.GetSkippedSequencesOlderThanMaxWait()
	if len(oldSkippedSequences) == 0 {
		return nil
	}

	base.InfofCtx(ctx, base.KeyCache, "Starting CleanSkippedSequenceQueue, found %d skipped sequences older than max wait for database %s", len(oldSkippedSequences), base.MD(c.db.Name))

	// Purge sequences not found from the skipped sequence queue
	numRemoved := c.RemoveSkippedSequences(ctx, oldSkippedSequences)
	c.db.DbStats.Cache().AbandonedSeqs.Add(numRemoved)

	base.InfofCtx(ctx, base.KeyCache, "CleanSkippedSequenceQueue complete.  Not Found:%d for database %s.", len(oldSkippedSequences), base.MD(c.db.Name))
	return nil
}

// ////// ADDING CHANGES:

// Note that DocChanged may be executed concurrently for multiple events (in the DCP case, DCP events
// originating from multiple vbuckets).  Only processEntry is locking - all other functionality needs to support
// concurrent processing.
func (c *changeCache) DocChanged(event sgbucket.FeedEvent) {
	ctx := c.logCtx
	docID := string(event.Key)
	docJSON := event.Value
	changedChannelsCombined := channels.Set{}

	// ** This method does not directly access any state of c, so it doesn't lock.
	// Is this a user/role doc for this database?
	if strings.HasPrefix(docID, c.metaKeys.UserKeyPrefix()) {
		c.processPrincipalDoc(ctx, docID, docJSON, true, event.TimeReceived)
		return
	} else if strings.HasPrefix(docID, c.metaKeys.RoleKeyPrefix()) {
		c.processPrincipalDoc(ctx, docID, docJSON, false, event.TimeReceived)
		return
	}

	// Is this an unused sequence notification?
	if strings.HasPrefix(docID, c.metaKeys.UnusedSeqPrefix()) {
		c.processUnusedSequence(ctx, docID, event.TimeReceived)
		return
	}
	if strings.HasPrefix(docID, c.metaKeys.UnusedSeqRangePrefix()) {
		c.processUnusedSequenceRange(ctx, docID)
		return
	}

	if strings.HasPrefix(docID, c.sgCfgPrefix) {
		if c.cfgEventCallback != nil {
			c.cfgEventCallback(docID, event.Cas, nil)
		}
		return
	}

	collection, exists := c.db.CollectionByID[event.CollectionID]
	if !exists {
		cID := event.CollectionID
		if cID == base.DefaultCollectionID {
			// It's possible for the `_default` collection to be associated with other databases writing non-principal documents,
			// but we still need this collection's feed for the MetadataStore docs. Conditionally drop log level to avoid spurious warnings.
			base.DebugfCtx(ctx, base.KeyCache, "DocChanged(): Ignoring non-metadata mutation for doc %q in the default collection - kv ID: %d", base.UD(docID), cID)
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

	ctx = base.CollectionLogCtx(ctx, collection.Name)

	// If this is a delete and there are no xattrs (no existing SG revision), we can ignore
	if event.Opcode == sgbucket.FeedOpDeletion && len(docJSON) == 0 {
		base.DebugfCtx(ctx, base.KeyImport, "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(docID))
		return
	}

	// If this is a binary document (and not one of the above types), we can ignore.  Currently only performing this check when xattrs
	// are enabled, because walrus doesn't support DataType on feed.
	if collection.UseXattrs() && event.DataType == base.MemcachedDataTypeRaw {
		return
	}

	// First unmarshal the doc (just its metadata, to save time/memory):
	syncData, rawBody, _, rawUserXattr, err := UnmarshalDocumentSyncDataFromFeed(docJSON, event.DataType, collection.userXattrKey(), false)
	if err != nil {
		// Avoid log noise related to failed unmarshaling of binary documents.
		if event.DataType != base.MemcachedDataTypeRaw {
			base.DebugfCtx(ctx, base.KeyCache, "Unable to unmarshal sync metadata for feed document %q.  Will not be included in channel cache.  Error: %v", base.UD(docID), err)
		}
		if err == base.ErrEmptyMetadata {
			base.WarnfCtx(ctx, "Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		}
		return
	}

	// If using xattrs and this isn't an SG write, we shouldn't attempt to cache.
	if collection.UseXattrs() {
		if syncData == nil {
			return
		}
		isSGWrite, _, _ := syncData.IsSGWrite(event.Cas, rawBody, rawUserXattr)
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

	if syncData.Sequence <= c.getInitialSequence() {
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
	for _, seq := range syncData.UnusedSequences {
		base.InfofCtx(ctx, base.KeyCache, "Received unused #%d in unused_sequences property for (%q / %q)", seq, base.UD(docID), syncData.CurrentRev)
		change := &LogEntry{
			Sequence:     seq,
			TimeReceived: event.TimeReceived,
			CollectionID: event.CollectionID,
		}
		changedChannels := c.processEntry(ctx, change)
		changedChannelsCombined = changedChannelsCombined.Update(changedChannels)
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

		for _, seq := range syncData.RecentSequences {
			if seq >= c.getNextSequence() && seq < currentSequence {
				base.InfofCtx(ctx, base.KeyCache, "Received deduplicated #%d in recent_sequences property for (%q / %q)", seq, base.UD(docID), syncData.CurrentRev)
				change := &LogEntry{
					Sequence:     seq,
					TimeReceived: event.TimeReceived,
					CollectionID: event.CollectionID,
				}

				// if the doc was removed from one or more channels at this sequence
				// Set the removed flag and removed channel set on the LogEntry
				if channelRemovals, atRevId := syncData.Channels.ChannelsRemovedAtSequence(seq); len(channelRemovals) > 0 {
					change.DocID = docID
					change.RevID = atRevId
					change.Channels = channelRemovals
				}

				changedChannels := c.processEntry(ctx, change)
				changedChannelsCombined = changedChannelsCombined.Update(changedChannels)
			}
		}
	}

	// Now add the entry for the new doc revision:
	if len(rawUserXattr) > 0 {
		collection.revisionCache.Remove(docID, syncData.CurrentRev)
	}
	change := &LogEntry{
		Sequence:     syncData.Sequence,
		DocID:        docID,
		RevID:        syncData.CurrentRev,
		Flags:        syncData.Flags,
		TimeReceived: event.TimeReceived,
		TimeSaved:    syncData.TimeSaved,
		Channels:     syncData.Channels,
		CollectionID: event.CollectionID,
	}

	millisecondLatency := int(feedLatency / time.Millisecond)

	// If latency is larger than 1 minute or is negative there is likely an issue and this should be clear to the user
	if millisecondLatency >= 60*1000 {
		base.InfofCtx(ctx, base.KeyChanges, "Received #%d after %3dms (%q / %q)", change.Sequence, millisecondLatency, base.UD(change.DocID), change.RevID)
	} else {
		base.DebugfCtx(ctx, base.KeyChanges, "Received #%d after %3dms (%q / %q)", change.Sequence, millisecondLatency, base.UD(change.DocID), change.RevID)
	}

	changedChannels := c.processEntry(ctx, change)
	changedChannelsCombined = changedChannelsCombined.Update(changedChannels)

	// Notify change listeners for all of the changed channels
	if c.notifyChange != nil && len(changedChannelsCombined) > 0 {
		c.notifyChange(ctx, changedChannelsCombined)
	}

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
func (c *changeCache) processUnusedSequence(ctx context.Context, docID string, timeReceived time.Time) {
	sequenceStr := strings.TrimPrefix(docID, c.metaKeys.UnusedSeqPrefix())
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to identify sequence number for unused sequence notification with key: %s, error: %v", base.UD(docID), err)
		return
	}
	c.releaseUnusedSequence(ctx, sequence, timeReceived)

}

func (c *changeCache) releaseUnusedSequence(ctx context.Context, sequence uint64, timeReceived time.Time) {
	change := &LogEntry{
		Sequence:     sequence,
		TimeReceived: timeReceived,
	}
	base.InfofCtx(ctx, base.KeyCache, "Received #%d (unused sequence)", sequence)

	// Since processEntry may unblock pending sequences, if there were any changed channels we need
	// to notify any change listeners that are working changes feeds for these channels
	changedChannels := c.processEntry(ctx, change)
	unusedSeq := channels.NewID(unusedSeqKey, unusedSeqCollectionID)
	if changedChannels == nil {
		changedChannels = channels.SetOfNoValidate(unusedSeq)
	} else {
		changedChannels.Add(unusedSeq)
	}
	c.channelCache.AddSkippedSequence(change)
	if c.notifyChange != nil && len(changedChannels) > 0 {
		c.notifyChange(ctx, changedChannels)
	}
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

	// TODO: There should be a more efficient way to do this
	for seq := fromSequence; seq <= toSequence; seq++ {
		c.releaseUnusedSequence(ctx, seq, time.Now())
	}
}

func (c *changeCache) processPrincipalDoc(ctx context.Context, docID string, docJSON []byte, isUser bool, timeReceived time.Time) {

	// Currently the cache isn't really doing much with user docs; mostly it needs to know about
	// them because they have sequence numbers, so without them the sequence of sequences would
	// have gaps in it, causing later sequences to get stuck in the queue.
	princ, err := c.unmarshalCachePrincipal(docJSON)
	if err != nil {
		base.WarnfCtx(ctx, "changeCache: Error unmarshaling doc %q: %v", base.UD(docID), err)
		return
	}
	sequence := princ.Sequence

	if sequence <= c.getInitialSequence() {
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
	if c.notifyChange != nil && len(changedChannels) > 0 {
		c.notifyChange(ctx, changedChannels)
	}
}

// Handles a newly-arrived LogEntry.
func (c *changeCache) processEntry(ctx context.Context, change *LogEntry) channels.Set {
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
	if sequence < c.nextSequence && !c.WasSkipped(sequence) {
		base.DebugfCtx(ctx, base.KeyCache, "  Ignoring duplicate of #%d", sequence)
		return nil
	}

	// Check if this is a duplicate of a pending sequence
	if _, found := c.receivedSeqs[sequence]; found {
		base.DebugfCtx(ctx, base.KeyCache, "  Ignoring duplicate of #%d", sequence)
		return nil
	}
	c.receivedSeqs[sequence] = struct{}{}

	var changedChannels channels.Set
	if sequence == c.nextSequence || c.nextSequence == 0 {
		// This is the expected next sequence so we can add it now:
		changedChannels = channels.SetFromArrayNoValidate(c._addToCache(ctx, change))
		// Also add any pending sequences that are now contiguous:
		changedChannels = changedChannels.Update(c._addPendingLogs(ctx))
	} else if sequence > c.nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		heap.Push(&c.pendingLogs, change)
		numPending := len(c.pendingLogs)
		c.internalStats.pendingSeqLen = numPending
		if base.LogDebugEnabled(base.KeyCache) {
			base.DebugfCtx(ctx, base.KeyCache, "  Deferring #%d (%d now waiting for #%d...#%d) doc %q / %q",
				sequence, numPending, c.nextSequence, c.pendingLogs[0].Sequence-1, base.UD(change.DocID), change.RevID)
		}
		// Update max pending high watermark stat
		if numPending > c.internalStats.maxPending {
			c.internalStats.maxPending = numPending
		}

		if numPending > c.options.CachePendingSeqMaxNum {
			// Too many pending; add the oldest one:
			changedChannels = c._addPendingLogs(ctx)
		}
	} else if sequence > c.initialSequence {
		// Out-of-order sequence received!
		// Remove from skipped sequence queue
		if !c.WasSkipped(sequence) {
			// Error removing from skipped sequences
			base.InfofCtx(ctx, base.KeyCache, "  Received unexpected out-of-order change - not in skippedSeqs (seq %d, expecting %d) doc %q / %q", sequence, c.nextSequence, base.UD(change.DocID), change.RevID)
		} else {
			base.InfofCtx(ctx, base.KeyCache, "  Received previously skipped out-of-order change (seq %d, expecting %d) doc %q / %q ", sequence, c.nextSequence, base.UD(change.DocID), change.RevID)
			change.Skipped = true
		}

		changedChannels = changedChannels.UpdateWithSlice(c._addToCache(ctx, change))
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
	delete(c.receivedSeqs, change.Sequence)

	// If unused sequence or principal, we're done after updating sequence
	if change.DocID == "" {
		return nil
	}

	if change.IsPrincipal {
		c.channelCache.AddPrincipal(change)
		return nil
	}

	// updatedChannels tracks the set of channels that should be notified of the change.  This includes
	// the change's active channels, as well as any channel removals for the active revision.
	updatedChannels := c.channelCache.AddToCache(ctx, change)
	if base.LogDebugEnabled(base.KeyChanges) {
		base.DebugfCtx(ctx, base.KeyChanges, " #%d ==> channels %v", change.Sequence, base.UD(updatedChannels))
	}

	if !change.TimeReceived.IsZero() {
		c.db.DbStats.Database().DCPCachingCount.Add(1)
		c.db.DbStats.Database().DCPCachingTime.Add(time.Since(change.TimeReceived).Nanoseconds())
	}

	return updatedChannels
}

// Add the first change(s) from pendingLogs if they're the next sequence.  If not, and we've been
// waiting too long for nextSequence, move nextSequence to skipped queue.
// Returns the channels that changed.
func (c *changeCache) _addPendingLogs(ctx context.Context) channels.Set {
	var changedChannels channels.Set

	for len(c.pendingLogs) > 0 {
		change := c.pendingLogs[0]
		isNext := change.Sequence == c.nextSequence
		if isNext {
			heap.Pop(&c.pendingLogs)
			changedChannels = changedChannels.UpdateWithSlice(c._addToCache(ctx, change))
		} else if len(c.pendingLogs) > c.options.CachePendingSeqMaxNum || time.Since(c.pendingLogs[0].TimeReceived) >= c.options.CachePendingSeqMaxWait {
			c.db.DbStats.Cache().NumSkippedSeqs.Add(1)
			c.PushSkipped(ctx, c.nextSequence)
			c.nextSequence++
		} else {
			break
		}
	}

	c.internalStats.pendingSeqLen = len(c.pendingLogs)

	atomic.StoreInt64(&c.lastAddPendingTime, time.Now().UnixNano())
	return changedChannels
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
	c.lock.RLock()
	nextSequence = c.nextSequence
	c.lock.RUnlock()
	return nextSequence
}

// Concurrent-safe get value of initialSequence
func (c *changeCache) getInitialSequence() (initialSequence uint64) {
	c.lock.RLock()
	initialSequence = c.initialSequence
	c.lock.RUnlock()
	return initialSequence
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
	err := c.skippedSeqs.Remove(x)
	c.db.DbStats.Cache().SkippedSeqLen.Set(int64(c.skippedSeqs.skippedList.Len()))
	return err
}

// Removes a set of sequences.  Logs warning on removal error, returns count of successfully removed.
func (c *changeCache) RemoveSkippedSequences(ctx context.Context, sequences []uint64) (removedCount int64) {
	numRemoved := c.skippedSeqs.RemoveSequences(ctx, sequences)
	c.db.DbStats.Cache().SkippedSeqLen.Set(int64(c.skippedSeqs.skippedList.Len()))
	return numRemoved
}

func (c *changeCache) WasSkipped(x uint64) bool {
	return c.skippedSeqs.Contains(x)
}

func (c *changeCache) PushSkipped(ctx context.Context, sequence uint64) {
	err := c.skippedSeqs.Push(&SkippedSequence{seq: sequence, timeAdded: time.Now()})
	if err != nil {
		base.InfofCtx(ctx, base.KeyCache, "Error pushing skipped sequence: %d, %v", sequence, err)
		return
	}
	c.db.DbStats.Cache().SkippedSeqLen.Set(int64(c.skippedSeqs.skippedList.Len()))
}

func (c *changeCache) GetSkippedSequencesOlderThanMaxWait() (oldSequences []uint64) {
	return c.skippedSeqs.getOlderThan(c.options.CacheSkippedSeqMaxWait)
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

func (c *changeCache) getMaxStableCached(ctx context.Context) uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c._getMaxStableCached(ctx)
}

func (c *changeCache) _getMaxStableCached(ctx context.Context) uint64 {
	oldestSkipped := c.getOldestSkippedSequence(ctx)
	if oldestSkipped > 0 {
		return oldestSkipped - 1
	}
	return c.nextSequence - 1
}

// SkippedSequenceList stores the set of skipped sequences as an ordered list of *SkippedSequence with an associated map
// for sequence-based lookup.
type SkippedSequenceList struct {
	skippedList *list.List               // Ordered list of skipped sequences
	skippedMap  map[uint64]*list.Element // Map from sequence to list elements
	lock        sync.RWMutex             // Coordinates access to skippedSequenceList
}

func NewSkippedSequenceList() *SkippedSequenceList {
	return &SkippedSequenceList{
		skippedMap:  map[uint64]*list.Element{},
		skippedList: list.New(),
	}
}

// getOldest returns the sequence of the first element in the skippedSequenceList
func (l *SkippedSequenceList) getOldest() (oldestSkippedSeq uint64) {
	l.lock.RLock()
	if firstElement := l.skippedList.Front(); firstElement != nil {
		value := firstElement.Value.(*SkippedSequence)
		oldestSkippedSeq = value.seq
	}
	l.lock.RUnlock()
	return oldestSkippedSeq
}

// Removes a single entry from the list.
func (l *SkippedSequenceList) Remove(x uint64) error {
	l.lock.Lock()
	err := l._remove(x)
	l.lock.Unlock()
	return err
}

func (l *SkippedSequenceList) RemoveSequences(ctx context.Context, sequences []uint64) (removedCount int64) {
	l.lock.Lock()
	for _, seq := range sequences {
		err := l._remove(seq)
		if err != nil {
			base.WarnfCtx(ctx, "Error purging skipped sequence %d from skipped sequence queue: %v", seq, err)
		} else {
			removedCount++
		}
	}
	l.lock.Unlock()
	return removedCount
}

// Removes an entry from the list.  Expects callers to hold l.lock.Lock
func (l *SkippedSequenceList) _remove(x uint64) error {
	if listElement, ok := l.skippedMap[x]; ok {
		l.skippedList.Remove(listElement)
		delete(l.skippedMap, x)
		return nil
	} else {
		return errors.New("Value not found")
	}
}

// Contains does a simple search to detect presence
func (l *SkippedSequenceList) Contains(x uint64) bool {
	l.lock.RLock()
	_, ok := l.skippedMap[x]
	l.lock.RUnlock()
	return ok
}

// Push sequence to the end of SkippedSequenceList.  Validates sequence ordering in list.
func (l *SkippedSequenceList) Push(x *SkippedSequence) (err error) {

	l.lock.Lock()
	validPush := false
	lastElement := l.skippedList.Back()
	if lastElement == nil {
		validPush = true
	} else {
		lastSkipped, _ := lastElement.Value.(*SkippedSequence)
		if lastSkipped.seq < x.seq {
			validPush = true
		}
	}

	if validPush {
		newListElement := l.skippedList.PushBack(x)
		l.skippedMap[x.seq] = newListElement
	} else {
		err = errors.New("Can't push sequence lower than existing maximum")
	}

	l.lock.Unlock()
	return err

}

// getOldest returns a slice of sequences older than the specified duration of the first element in the skippedSequenceList
func (l *SkippedSequenceList) getOlderThan(skippedExpiry time.Duration) []uint64 {

	l.lock.RLock()
	oldSequences := make([]uint64, 0)
	for e := l.skippedList.Front(); e != nil; e = e.Next() {
		skippedSeq := e.Value.(*SkippedSequence)
		if time.Since(skippedSeq.timeAdded) > skippedExpiry {
			oldSequences = append(oldSequences, skippedSeq.seq)
		} else {
			// skippedSeqs are ordered by arrival time, so can stop iterating once we find one
			// still inside the time window
			break
		}
	}
	l.lock.RUnlock()
	return oldSequences
}

package db

import (
	"container/heap"
	"errors"
	"expvar"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	DefaultCachePendingSeqMaxNum  = 10000            // Max number of waiting sequences
	DefaultCachePendingSeqMaxWait = 5 * time.Second  // Max time we'll wait for a pending sequence before sending to missed queue
	DefaultSkippedSeqMaxWait      = 60 * time.Minute // Max time we'll wait for an entry in the missing before purging
)

// Enable keeping a channel-log for the "*" channel (channel.UserStarChannel). The only time this channel is needed is if
// someone has access to "*" (e.g. admin-party) and tracks its changes feed.
var EnableStarChannelLog = true

var changeCacheExpvars *expvar.Map

func init() {
	changeCacheExpvars = expvar.NewMap("syncGateway_changeCache")
	changeCacheExpvars.Set("maxPending", new(base.IntMax))
}

// Manages a cache of the recent change history of all channels.
type changeCache struct {
	context         *DatabaseContext
	logsDisabled    bool                     // If true, ignore incoming tap changes
	nextSequence    uint64                   // Next consecutive sequence number to add
	initialSequence uint64                   // DB's current sequence at startup time
	receivedSeqs    map[uint64]struct{}      // Set of all sequences received
	pendingLogs     LogPriorityQueue         // Out-of-sequence entries waiting to be cached
	channelCaches   map[string]*channelCache // A cache of changes for each channel
	onChange        func(base.Set)           // Client callback that notifies of channel changes
	stopped         bool                     // Set by the Stop method
	skippedSeqs     SkippedSequenceQueue     // Skipped sequences still pending on the TAP feed
	skippedSeqLock  sync.RWMutex             // Coordinates access to skippedSeqs queue
	lock            sync.RWMutex             // Coordinates access to struct fields
	lateSeqLock     sync.RWMutex             // Coordinates access to late sequence caches
	options         CacheOptions             // Cache config
	terminator      chan bool                // Signal termination of background goroutines
}

type LogEntry channels.LogEntry

func (l LogEntry) String() string {
	return channels.LogEntry(l).String()
}

type LogEntries []*LogEntry

// A priority-queue of LogEntries, kept ordered by increasing sequence #.
type LogPriorityQueue []*LogEntry

// An ordered queue that supports the Remove operation
type SkippedSequenceQueue []*SkippedSequence

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

//////// HOUSEKEEPING:

// Initializes a new changeCache.
// lastSequence is the last known database sequence assigned.
// onChange is an optional function that will be called to notify of channel changes.
func (c *changeCache) Init(context *DatabaseContext, lastSequence SequenceID, onChange func(base.Set), options *CacheOptions, indexOptions *ChannelIndexOptions) error {
	c.context = context
	c.initialSequence = lastSequence.Seq
	c.nextSequence = lastSequence.Seq + 1
	c.onChange = onChange
	c.channelCaches = make(map[string]*channelCache, 10)
	c.receivedSeqs = make(map[uint64]struct{})
	c.terminator = make(chan bool)

	// init cache options
	c.options = CacheOptions{
		CachePendingSeqMaxWait: DefaultCachePendingSeqMaxWait,
		CachePendingSeqMaxNum:  DefaultCachePendingSeqMaxNum,
		CacheSkippedSeqMaxWait: DefaultSkippedSeqMaxWait,
	}

	if options != nil {
		if options.CachePendingSeqMaxNum > 0 {
			c.options.CachePendingSeqMaxNum = options.CachePendingSeqMaxNum
		}

		if options.CachePendingSeqMaxWait > 0 {
			c.options.CachePendingSeqMaxWait = options.CachePendingSeqMaxWait
		}

		if options.CacheSkippedSeqMaxWait > 0 {
			c.options.CacheSkippedSeqMaxWait = options.CacheSkippedSeqMaxWait
		}
		c.options.ChannelCacheOptions = options.ChannelCacheOptions
	}

	base.LogToR("Cache", "Initializing changes cache with options %+v", c.options)

	if context.UseGlobalSequence() {
		base.LogfR("Initializing changes cache for database %s with sequence: %d", base.UD(context.Name), c.initialSequence)
	}

	heap.Init(&c.pendingLogs)

	// Start a background task for periodic housekeeping:
	go func() {
		for {
			select {
			case <-time.After(c.options.CachePendingSeqMaxWait / 2):
				c.CleanUp()
			case <-c.terminator:
				return
			}
		}
	}()

	// Start a background task for SkippedSequenceQueue housekeeping:
	go func() {
		for {
			select {
			case <-time.After(c.options.CacheSkippedSeqMaxWait / 2):
				c.CleanSkippedSequenceQueue()
			case <-c.terminator:
				return
			}
		}
	}()

	return nil
}

// Stops the cache. Clears its state and tells the housekeeping task to stop.
func (c *changeCache) Stop() {

	// Signal to background goroutines that the changeCache has been stopped, so they can exit
	// their loop
	close(c.terminator)

	c.lock.Lock()
	c.stopped = true
	c.logsDisabled = true
	c.lock.Unlock()
}

func (c *changeCache) IsStopped() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.stopped
}

// Forgets all cached changes for all channels.
func (c *changeCache) Clear() {
	c.lock.Lock()
	c.initialSequence, _ = c.context.LastSequence()
	c.channelCaches = make(map[string]*channelCache, 10)
	c.pendingLogs = nil
	heap.Init(&c.pendingLogs)
	c.lock.Unlock()
}

// If set to false, DocChanged() becomes a no-op.
func (c *changeCache) EnableChannelIndexing(enable bool) {
	c.lock.Lock()
	c.logsDisabled = !enable
	c.lock.Unlock()
}

// Cleanup function, invoked periodically.
// Inserts pending entries that have been waiting too long.
// Removes entries older than MaxChannelLogCacheAge from the cache.
// Returns false if the changeCache has been closed.
func (c *changeCache) CleanUp() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.channelCaches == nil {
		return false
	}

	// If entries have been pending too long, add them to the cache:
	changedChannels := c._addPendingLogs()
	if c.onChange != nil && len(changedChannels) > 0 {
		c.onChange(changedChannels)
	}

	// Remove old cache entries:
	for channelName := range c.channelCaches {
		c._getChannelCache(channelName).pruneCache()
	}
	return true
}

// Cleanup function, invoked periodically.
// Removes skipped entries from skippedSeqs that have been waiting longer
// than MaxChannelLogMissingWaitTime from the queue.  Attempts view retrieval
// prior to removal
func (c *changeCache) CleanSkippedSequenceQueue() bool {

	foundEntries, pendingDeletes := func() ([]*LogEntry, []uint64) {
		c.skippedSeqLock.Lock()
		defer c.skippedSeqLock.Unlock()

		var found []*LogEntry
		var deletes []uint64
		for _, skippedSeq := range c.skippedSeqs {
			if time.Since(skippedSeq.timeAdded) > c.options.CacheSkippedSeqMaxWait {
				// Attempt to retrieve the sequence from the view before we remove.  View call
				// expects 'since' value, and returns results greater than the since value, so
				// set 'since' to our target sequence - 1
				sinceSequence := skippedSeq.seq - 1
				endSequence := skippedSeq.seq
				options := ChangesOptions{Since: SequenceID{Seq: sinceSequence}}
				// Note: The view query is only going to hit for active revisions - sequences associated with inactive revisions
				//       aren't indexed by the channel view.  This means we can potentially miss channel removals:
				//       when an older revision is missed by the TAP feed, and a channel is removed in that revision,
				//       the doc won't be flagged as removed from that channel in the in-memory channel cache.
				entries, err := c.context.getChangesInChannelFromView("*", endSequence, options)
				if err == nil && len(entries) > 0 {
					// Found it - store to send to the caches.
					found = append(found, entries[0])
				} else {
					if err != nil {
						base.WarnR("Error retrieving changes from view during skipped sequence check: %v", err)
					}
					base.WarnR("Skipped Sequence %d didn't show up in MaxChannelLogMissingWaitTime, and isn't available from the * channel view.  If it's a valid sequence, it won't be replicated until Sync Gateway is restarted.", skippedSeq.seq)
				}
				// Remove from skipped queue
				deletes = append(deletes, skippedSeq.seq)
			} else {
				// skippedSeqs are ordered by arrival time, so can stop iterating once we find one
				// still inside the time window
				break
			}
		}
		return found, deletes
	}()

	changedChannelsCombined := base.Set{}

	// Add found entries
	for _, entry := range foundEntries {
		entry.Skipped = true
		// Need to populate the actual channels for this entry - the entry returned from the * channel
		// view will only have the * channel
		doc, err := c.context.GetDocument(entry.DocID, DocUnmarshalNoHistory)
		if err != nil {
			base.WarnR("Unable to retrieve doc when processing skipped document %q: abandoning sequence %d", base.UD(entry.DocID), entry.Sequence)
			continue
		}
		entry.Channels = doc.Channels

		changedChannels := c.processEntry(entry)
		changedChannelsCombined = changedChannelsCombined.Union(changedChannels)
	}

	// Since the calls to processEntry() above may unblock pending sequences, if there were any changed channels we need
	// to notify any change listeners that are working changes feeds for these channels
	if c.onChange != nil && len(changedChannelsCombined) > 0 {
		c.onChange(changedChannelsCombined)
	}

	// Purge pending deletes
	for _, sequence := range pendingDeletes {
		err := c.RemoveSkipped(sequence)
		if err != nil {
			base.WarnR("Error purging skipped sequence %d from skipped sequence queue", sequence)
		} else {
			dbExpvars.Add("abandoned_seqs", 1)
		}
	}

	return true
}

// FOR TESTS ONLY: Blocks until the given sequence has been received.
func (c *changeCache) waitForSequenceID(sequence SequenceID) {
	c.waitForSequence(sequence.Seq)
}

func (c *changeCache) waitForSequence(sequence uint64) {
	var i int
	for i = 0; i < 20; i++ {
		c.lock.RLock()
		nextSequence := c.nextSequence
		c.lock.RUnlock()
		if nextSequence >= sequence+1 {
			base.LogfR("waitForSequence(%d) took %d ms", sequence, i*100)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic(fmt.Sprintf("changeCache: Sequence %d never showed up!", sequence))
}

// FOR TESTS ONLY: Blocks until the given sequence has been received.
func (c *changeCache) waitForSequenceWithMissing(sequence uint64) {
	var i int
	for i = 0; i < 20; i++ {
		c.lock.RLock()
		nextSequence := c.nextSequence
		c.lock.RUnlock()
		if nextSequence >= sequence+1 {
			foundInMissing := false
			c.skippedSeqLock.RLock()
			for _, skippedSeq := range c.skippedSeqs {
				if skippedSeq.seq == sequence {
					foundInMissing = true
					break
				}
			}
			c.skippedSeqLock.RUnlock()
			if !foundInMissing {
				base.LogfR("waitForSequence(%d) took %d ms", sequence, i*100)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic(fmt.Sprintf("changeCache: Sequence %d never showed up!", sequence))
}

//////// ADDING CHANGES:

// Given a newly changed document (received from the tap feed), adds change entries to channels.
// The JSON must be the raw document from the bucket, with the metadata and all.
func (c *changeCache) DocChanged(event sgbucket.FeedEvent) {
	if event.Synchronous {
		c.DocChangedSynchronous(event)
	} else {
		go c.DocChangedSynchronous(event)
	}
}

// Note that DocChangedSynchronous may be executed concurrently for multiple events (in the DCP case, DCP events
// originating from multiple vbuckets).  Only processEntry is locking - all other functionality needs to support
// concurrent processing.
func (c *changeCache) DocChangedSynchronous(event sgbucket.FeedEvent) {
	entryTime := time.Now()
	docID := string(event.Key)
	docJSON := event.Value
	changedChannelsCombined := base.Set{}

	// ** This method does not directly access any state of c, so it doesn't lock.
	// Is this a user/role doc?
	if strings.HasPrefix(docID, auth.UserKeyPrefix) {
		c.processPrincipalDoc(docID, docJSON, true, event.Expiry)
		return
	} else if strings.HasPrefix(docID, auth.RoleKeyPrefix) {
		c.processPrincipalDoc(docID, docJSON, false, event.Expiry)
		return
	}

	// Is this an unused sequence notification?
	if strings.HasPrefix(docID, UnusedSequenceKeyPrefix) {
		c.processUnusedSequence(docID)
		return
	}

	// If this is a delete and there are no xattrs (no existing SG revision), we can ignore
	if event.Opcode == sgbucket.FeedOpDeletion && len(docJSON) == 0 {
		base.LogToR("Import+", "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(docID))
		return
	}

	// If this is a binary document (and not one of the above types), we can ignore.  Currently only performing this check when xattrs
	// are enabled, because walrus doesn't support DataType on feed.
	if c.context.UseXattrs() && event.DataType == base.MemcachedDataTypeRaw {
		return
	}

	// First unmarshal the doc (just its metadata, to save time/memory):
	syncData, rawBody, rawXattr, err := UnmarshalDocumentSyncDataFromFeed(docJSON, event.DataType, false)
	if err != nil {
		// Avoid log noise related to failed unmarshaling of binary documents.
		if event.DataType != base.MemcachedDataTypeRaw {
			base.LogToR("Cache+", "Unable to unmarshal sync metadata for feed document %q.  Will not be included in channel cache.  Error: %v", base.UD(docID), err)
		}
		if err == base.ErrEmptyMetadata {
			base.WarnR("Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		}
		return
	}

	// Import handling.
	if c.context.UseXattrs() {
		// If this isn't an SG write, we shouldn't attempt to cache.  Import if this node is configured for import, otherwise ignore.
		if syncData == nil || !syncData.IsSGWrite(event.Cas) {
			if c.context.autoImport {
				// If syncData is nil, or if this was not an SG write, attempt to import
				isDelete := event.Opcode == sgbucket.FeedOpDeletion
				if isDelete {
					rawBody = nil
				}

				db := Database{DatabaseContext: c.context, user: nil}
				_, err := db.ImportDocRaw(docID, rawBody, rawXattr, isDelete, event.Cas, &event.Expiry, ImportFromFeed)
				if err != nil {
					if err == base.ErrImportCasFailure {
						base.LogToR("Import+", "Not importing mutation - document %s has been subsequently updated and will be imported based on that mutation.", base.UD(docID))
					} else if err == base.ErrImportCancelledFilter {
						// No logging required - filter info already logged during importDoc
					} else {
						base.LogToR("Import+", "Did not import doc %q - external update will not be accessible via Sync Gateway.  Reason: %v", base.UD(docID), err)
					}
				}
			}
			return
		}
	}

	if !syncData.HasValidSyncData(c.context.writeSequences()) {
		// No sync metadata found - check whether we're mid-upgrade and attempting to read a doc w/ metadata stored in xattr
		migratedDoc, _ := c.context.checkForUpgrade(docID)
		if migratedDoc != nil && migratedDoc.Cas == event.Cas {
			base.LogToR("Cache", "Found mobile xattr on document without _sync property - caching, assuming upgrade in progress.")
			syncData = &migratedDoc.syncData
		} else {
			base.WarnR("changeCache: Doc %q does not have valid sync data.", base.UD(docID))
			return
		}
	}

	if syncData.Sequence <= c.initialSequence {
		return // Tap is sending us an old value from before I started up; ignore it
	}

	// Record a histogram of the Tap feed's lag:
	tapLag := time.Since(syncData.TimeSaved) - time.Since(entryTime)
	lagMs := int(tapLag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-tap-%04dms", lagMs), 1)

	// If the doc update wasted any sequences due to conflicts, add empty entries for them:
	for _, seq := range syncData.UnusedSequences {
		base.LogToR("Cache", "Received unused #%d for (%q / %q)", seq, base.UD(docID), syncData.CurrentRev)
		change := &LogEntry{
			Sequence:     seq,
			TimeReceived: time.Now(),
		}
		changedChannels := c.processEntry(change)
		changedChannelsCombined = changedChannelsCombined.Union(changedChannels)
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
		nextSeq := c.getNextSequence()
		for _, seq := range syncData.RecentSequences {
			if seq >= nextSeq && seq < currentSequence {
				base.LogToR("Cache", "Received deduplicated #%d for (%q / %q)", seq, base.UD(docID), syncData.CurrentRev)
				change := &LogEntry{
					Sequence:     seq,
					TimeReceived: time.Now(),
				}

				//if the doc was removed from one or more channels at this sequence
				// Set the removed flag and removed channel set on the LogEntry
				if channelRemovals, atRevId := syncData.Channels.ChannelsRemovedAtSequence(seq); len(channelRemovals) > 0 {
					change.DocID = docID
					change.RevID = atRevId
					change.Channels = channelRemovals
				}

				changedChannels := c.processEntry(change)
				changedChannelsCombined = changedChannelsCombined.Union(changedChannels)
			}
		}
	}

	// Now add the entry for the new doc revision:
	change := &LogEntry{
		Sequence:     syncData.Sequence,
		DocID:        docID,
		RevID:        syncData.CurrentRev,
		Flags:        syncData.Flags,
		TimeReceived: time.Now(),
		TimeSaved:    syncData.TimeSaved,
		Channels:     syncData.Channels,
	}
	base.LogToR("Cache", "Received #%d after %3dms (%q / %q)", change.Sequence, int(tapLag/time.Millisecond), base.UD(change.DocID), change.RevID)

	changedChannels := c.processEntry(change)
	changedChannelsCombined = changedChannelsCombined.Union(changedChannels)

	// Notify change listeners for all of the changed channels
	if c.onChange != nil && len(changedChannelsCombined) > 0 {
		c.onChange(changedChannelsCombined)
	}

}

func (c *changeCache) unmarshalPrincipal(docJSON []byte, isUser bool) (auth.Principal, error) {

	c.context.BucketLock.RLock()
	defer c.context.BucketLock.RUnlock()
	if c.context.Bucket != nil {
		return c.context.Authenticator().UnmarshalPrincipal(docJSON, "", 0, isUser)
	} else {
		return nil, fmt.Errorf("Attempt to unmarshal principal using closed bucket")
	}
}

// Process unused sequence notification.  Extracts sequence from docID and sends to cache for buffering
func (c *changeCache) processUnusedSequence(docID string) {
	sequenceStr := strings.TrimPrefix(docID, UnusedSequenceKeyPrefix)
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		base.WarnR("Unable to identify sequence number for unused sequence notification with key: %s, error:", base.UD(docID), err)
		return
	}
	change := &LogEntry{
		Sequence:     sequence,
		TimeReceived: time.Now(),
	}
	base.LogToR("Cache", "Received #%d (unused sequence)", sequence)

	// Since processEntry may unblock pending sequences, if there were any changed channels we need
	// to notify any change listeners that are working changes feeds for these channels
	changedChannels := c.processEntry(change)
	if c.onChange != nil && len(changedChannels) > 0 {
		c.onChange(changedChannels)
	}

}

// TODO: maybe just pass whole DCP Event + isUser (or calc'd from event)
func (c *changeCache) processPrincipalDoc(docID string, docJSON []byte, isUser bool, expiry uint32) {
	// Currently the cache isn't really doing much with user docs; mostly it needs to know about
	// them because they have sequence numbers, so without them the sequence of sequences would
	// have gaps in it, causing later sequences to get stuck in the queue.
	princ, err := c.unmarshalPrincipal(docJSON, isUser)
	if princ == nil {
		base.WarnR("changeCache: Error unmarshaling doc %q: %v", base.UD(docID), err)
		return
	}

	if princ.GetUpdateExpiry() != expiry {
		// Looks like this is a GetAndTouch change that only changed the expiry, since otherwise
		// the user's UpdateExpiry field would have been updated to match the doc expiry.
		// Ignore this DCP event completely.
		return
	}

	sequence := princ.Sequence()
	c.lock.RLock()
	initialSequence := c.initialSequence
	c.lock.RUnlock()
	if sequence <= initialSequence {
		return // Tap is sending us an old value from before I started up; ignore it
	}

	// Now add the (somewhat fictitious) entry:
	change := &LogEntry{
		Sequence:     sequence,
		TimeReceived: time.Now(),
	}
	if isUser {
		change.DocID = "_user/" + princ.Name()
	} else {
		change.DocID = "_role/" + princ.Name()
	}

	base.LogToR("Cache", "Received #%d (%q)", change.Sequence, base.UD(change.DocID))


	// TODO: coalesce this onChange() callback for changedChannels and docId (append to end of changedChannels)
	changedChannels := c.processEntry(change)
	if c.onChange != nil && len(changedChannels) > 0 {
		c.onChange(changedChannels)
	}

	// Notify the listener that the principal doc changed
	if c.onChange != nil {
		c.onChange(base.SetOf(docID))
	}
}

// Handles a newly-arrived LogEntry.
func (c *changeCache) processEntry(change *LogEntry) base.Set {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.logsDisabled {
		return nil
	}

	sequence := change.Sequence
	nextSequence := c.nextSequence
	if _, found := c.receivedSeqs[sequence]; found {
		base.LogToR("Cache+", "  Ignoring duplicate of #%d", sequence)
		return nil
	}
	c.receivedSeqs[sequence] = struct{}{}
	// FIX: c.receivedSeqs grows monotonically. Need a way to remove old sequences.

	var changedChannels base.Set
	if sequence == nextSequence || nextSequence == 0 {
		// This is the expected next sequence so we can add it now:
		changedChannels = c._addToCache(change)
		// Also add any pending sequences that are now contiguous:
		changedChannels = changedChannels.Union(c._addPendingLogs())
	} else if sequence > nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		heap.Push(&c.pendingLogs, change)
		numPending := len(c.pendingLogs)
		base.LogToR("Cache", "  Deferring #%d (%d now waiting for #%d...#%d)",
			sequence, numPending, nextSequence, c.pendingLogs[0].Sequence-1)
		changeCacheExpvars.Get("maxPending").(*base.IntMax).SetIfMax(int64(numPending))
		if numPending > c.options.CachePendingSeqMaxNum {
			// Too many pending; add the oldest one:
			changedChannels = c._addPendingLogs()
		}
	} else if sequence > c.initialSequence {
		// Out-of-order sequence received!
		// Remove from skipped sequence queue
		if !c.WasSkipped(sequence) {
			// Error removing from skipped sequences
			base.LogToR("Cache", "  Received unexpected out-of-order change - not in skippedSeqs (seq %d, expecting %d) doc %q / %q", sequence, nextSequence, base.UD(change.DocID), change.RevID)
		} else {
			base.LogToR("Cache", "  Received previously skipped out-of-order change (seq %d, expecting %d) doc %q / %q ", sequence, nextSequence, base.UD(change.DocID), change.RevID)
			change.Skipped = true
		}

		changedChannels = c._addToCache(change)
		// Add to cache before removing from skipped, to ensure lowSequence doesn't get incremented until results are available
		// in cache
		c.RemoveSkipped(sequence)
	}
	return changedChannels
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (c *changeCache) _addToCache(change *LogEntry) base.Set {
	if change.Sequence >= c.nextSequence {
		c.nextSequence = change.Sequence + 1
	}
	if change.DocID == "" {
		return nil // this was a placeholder for an unused sequence
	}
	addedTo := make([]string, 0, 4)
	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory

	// If it's a late sequence, we want to add to all channel late queues within a single write lock,
	// to avoid a changes feed seeing the same late sequence in different iteration loops (and sending
	// twice)
	func() {
		if change.Skipped {
			c.lateSeqLock.Lock()
			base.LogToR("Sequences", "Acquired late sequence lock for %d", change.Sequence)
			defer c.lateSeqLock.Unlock()
		}

		for channelName, removal := range ch {
			if removal == nil || removal.Seq == change.Sequence {
				channelCache := c._getChannelCache(channelName)
				channelCache.addToCache(change, removal != nil)
				addedTo = append(addedTo, channelName)
				if change.Skipped {
					channelCache.AddLateSequence(change)
				}
			}
		}

		if EnableStarChannelLog {
			channelCache := c._getChannelCache(channels.UserStarChannel)
			channelCache.addToCache(change, false)
			addedTo = append(addedTo, channels.UserStarChannel)
			if change.Skipped {
				channelCache.AddLateSequence(change)
			}
		}
	}()

	// Record a histogram of the overall lag from the time the doc was saved:
	lag := time.Since(change.TimeSaved)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-total-%04dms", lagMs), 1)
	// ...and from the time the doc was received from Tap:
	lag = time.Since(change.TimeReceived)
	lagMs = int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-queue-%04dms", lagMs), 1)

	return base.SetFromArray(addedTo)
}

// Add the first change(s) from pendingLogs if they're the next sequence.  If not, and we've been
// waiting too long for nextSequence, move nextSequence to skipped queue.
// Returns the channels that changed.
func (c *changeCache) _addPendingLogs() base.Set {
	var changedChannels base.Set

	for len(c.pendingLogs) > 0 {
		change := c.pendingLogs[0]
		isNext := change.Sequence == c.nextSequence
		if isNext {
			heap.Pop(&c.pendingLogs)
			changedChannels = changedChannels.Union(c._addToCache(change))
		} else if len(c.pendingLogs) > c.options.CachePendingSeqMaxNum || time.Since(c.pendingLogs[0].TimeReceived) >= c.options.CachePendingSeqMaxWait {
			changeCacheExpvars.Add("outOfOrder", 1)
			c.PushSkipped(c.nextSequence)
			c.nextSequence++
		} else {
			break
		}
	}
	return changedChannels
}

func (c *changeCache) getChannelCache(channelName string) *channelCache {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c._getChannelCache(channelName)
}

func (c *changeCache) getNextSequence() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.nextSequence
}

func (c *changeCache) GetStableSequence(docID string) SequenceID {
	c.lock.RLock()
	defer c.lock.RUnlock()
	// Stable sequence is independent of docID in changeCache
	return SequenceID{Seq: c.nextSequence - 1}
}

func (c *changeCache) GetStableClock(stale bool) (clock base.SequenceClock, err error) {
	return nil, errors.New("Change cache doesn't use vector clocks")
}

func (c *changeCache) _getChannelCache(channelName string) *channelCache {
	cache := c.channelCaches[channelName]
	if cache == nil {
		cache = newChannelCacheWithOptions(c.context, channelName, c.initialSequence+1, c.options)
		c.channelCaches[channelName] = cache
	}
	return cache
}

//////// CHANGE ACCESS:

func (c *changeCache) GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error) {

	if c.IsStopped() {
		return nil, base.HTTPErrorf(503, "Database closed")
	}
	return c.getChannelCache(channelName).GetChanges(options)
}

func (c *changeCache) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {
	return c.getChannelCache(channelName).getCachedChanges(options)
}

// Returns the sequence number the cache is up-to-date with.
func (c *changeCache) LastSequence() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.nextSequence - 1
}

func (c *changeCache) _allChannels() base.Set {
	array := make([]string, len(c.channelCaches))
	i := 0
	for name := range c.channelCaches {
		array[i] = name
		i++
	}
	return base.SetFromArray(array)
}

func (c *changeCache) getOldestSkippedSequence() uint64 {
	c.skippedSeqLock.RLock()
	defer c.skippedSeqLock.RUnlock()
	if len(c.skippedSeqs) > 0 {
		base.LogToR("Sequences", "get oldest, returning: %d", c.skippedSeqs[0].seq)
		return c.skippedSeqs[0].seq
	} else {
		return uint64(0)
	}
}

//////// LOG PRIORITY QUEUE -- container/heap callbacks that should not be called directly.   Use heap.Init/Push/etc()

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

//////// SKIPPED SEQUENCE QUEUE

func (c *changeCache) RemoveSkipped(x uint64) error {
	c.skippedSeqLock.Lock()
	defer c.skippedSeqLock.Unlock()
	return c.skippedSeqs.Remove(x)
}

func (c *changeCache) WasSkipped(x uint64) bool {
	c.skippedSeqLock.RLock()
	defer c.skippedSeqLock.RUnlock()

	return c.skippedSeqs.Contains(x)
}

func (c *changeCache) PushSkipped(sequence uint64) {

	c.skippedSeqLock.Lock()
	defer c.skippedSeqLock.Unlock()
	c.skippedSeqs.Push(&SkippedSequence{seq: sequence, timeAdded: time.Now()})
}

// Remove does a simple binary search to find and remove.
func (h *SkippedSequenceQueue) Remove(x uint64) error {

	i := SearchSequenceQueue(*h, x)
	if i < len(*h) && (*h)[i].seq == x {
		// GC-friendly removal of skipped sequence entries from the queue.
		// (https://github.com/golang/go/wiki/SliceTricks)
		copy((*h)[i:], (*h)[i+1:])
		(*h)[len(*h)-1] = nil
		*h = (*h)[:len(*h)-1]
		return nil
	} else {
		return errors.New("Value not found")
	}

}

// Contains does a simple search to detect presence
func (h SkippedSequenceQueue) Contains(x uint64) bool {

	i := SearchSequenceQueue(h, x)
	if i < len(h) && h[i].seq == x {
		return true
	} else {
		return false
	}

}

// We always know that incoming missed sequence numbers will be larger than any previously
// added, so we don't need to do any sorting - just append to the slice
func (h *SkippedSequenceQueue) Push(x *SkippedSequence) error {
	// ensure valid sequence
	if len(*h) > 0 && x.seq <= (*h)[len(*h)-1].seq {
		return errors.New("Can't push sequence lower than existing maximum")
	}
	*h = append(*h, x)
	return nil

}

// Skipped Sequence version of sort.SearchInts - based on http://golang.org/src/sort/search.go?s=2959:2994#L73
func SearchSequenceQueue(a SkippedSequenceQueue, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i].seq >= x })
}

package db

import (
	"container/heap"
	"errors"
	"expvar"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

var DefaultMaxChannelLogPendingCount = 10000              // Max number of waiting sequences
var DefaultMaxChannelLogPendingWaitTime = 5 * time.Second // Max time we'll wait for a pending sequence before sending to missed queue

var DefaultMaxChannelLogSkippedWaitTime = 60 * time.Minute // Max time we'll wait for an entry in the missing before purging

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
	options         CacheOptions             // Cache config
}

type LogEntry channels.LogEntry

type LogEntries []*LogEntry

// A priority-queue of LogEntries, kept ordered by increasing sequence #.
type LogPriorityQueue []*LogEntry

// An ordered queue of supporting Remove
type SkippedSequenceQueue []SkippedSequence

type SkippedSequence struct {
	seq       uint64
	timeAdded time.Time
}

type CacheOptions struct {
	CachePendingSeqMaxWait time.Duration // Max wait for pending sequence before skipping
	CachePendingSeqMaxNum  int           // Max number of pending sequences before skipping
	CacheSkippedSeqMaxWait time.Duration // Max wait for skipped sequence before abandoning
}

//////// HOUSEKEEPING:

// Initializes a new changeCache.
// lastSequence is the last known database sequence assigned.
// onChange is an optional function that will be called to notify of channel changes.
func (c *changeCache) Init(context *DatabaseContext, lastSequence uint64, onChange func(base.Set), options CacheOptions) {
	c.context = context
	c.initialSequence = lastSequence
	c.nextSequence = lastSequence + 1
	c.onChange = onChange
	c.channelCaches = make(map[string]*channelCache, 10)
	c.receivedSeqs = make(map[uint64]struct{})

	// init cache options
	c.options = CacheOptions{
		CachePendingSeqMaxWait: DefaultMaxChannelLogPendingWaitTime,
		CachePendingSeqMaxNum:  DefaultMaxChannelLogPendingCount,
		CacheSkippedSeqMaxWait: DefaultMaxChannelLogSkippedWaitTime,
	}

	if options.CachePendingSeqMaxNum > 0 {
		c.options.CachePendingSeqMaxNum = options.CachePendingSeqMaxNum
	}

	if options.CachePendingSeqMaxWait > 0 {
		c.options.CachePendingSeqMaxWait = options.CachePendingSeqMaxWait
	}

	if options.CacheSkippedSeqMaxWait > 0 {
		c.options.CacheSkippedSeqMaxWait = options.CacheSkippedSeqMaxWait
	}

	base.LogTo("Cache", "Initializing changes cache with options %v", c.options)

	heap.Init(&c.pendingLogs)

	// Start a background task for periodic housekeeping:
	go func() {
		for c.CleanUp() {
			time.Sleep(c.options.CachePendingSeqMaxWait / 2)
		}
	}()

	// Start a background task for SkippedSequenceQueue housekeeping:
	go func() {
		for c.CleanSkippedSequenceQueue() {
			time.Sleep(c.options.CacheSkippedSeqMaxWait / 2)
		}
	}()
}

// Stops the cache. Clears its state and tells the housekeeping task to stop.
func (c *changeCache) Stop() {
	c.lock.Lock()
	c.stopped = true
	c.logsDisabled = true
	c.lock.Unlock()
}

// Forgets all cached changes for all channels.
func (c *changeCache) ClearLogs() {
	c.lock.Lock()
	c.initialSequence, _ = c.context.LastSequence()
	c.channelCaches = make(map[string]*channelCache, 10)
	c.pendingLogs = nil
	heap.Init(&c.pendingLogs)
	c.lock.Unlock()
}

// If set to false, DocChanged() becomes a no-op.
func (c *changeCache) EnableChannelLogs(enable bool) {
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
	for channelName, _ := range c.channelCaches {
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

		var foundEntries []*LogEntry
		var pendingDeletes []uint64
		for _, skippedSeq := range c.skippedSeqs {
			if time.Since(skippedSeq.timeAdded) > c.options.CacheSkippedSeqMaxWait {
				options := ChangesOptions{Since: SequenceID{Seq: skippedSeq.seq}}
				entries, err := c.context.getChangesInChannelFromView("*", skippedSeq.seq, options)
				if err != nil && len(entries) > 0 {
					// Found it - store to send to the caches.
					foundEntries = append(foundEntries, entries[0])
					dbExpvars.Add("skip_purge_view_hit", 1)
				} else {
					base.Warn("Skipped Sequence %d didn't show up in MaxChannelLogMissingWaitTime, and isn't available from the * channel view - will be abandoned.", skippedSeq.seq)
					pendingDeletes = append(pendingDeletes, skippedSeq.seq)
					dbExpvars.Add("skip_purge_view_miss", 1)
				}
			} else {
				// skippedSeqs are ordered by arrival time, so can stop iterating once we find one
				// still inside the time window
				break
			}
		}
		return foundEntries, pendingDeletes
	}()

	// Add found entries
	for _, entry := range foundEntries {
		changeCacheExpvars.Add("processEntry-count-CleanSkipped", 1)
		c.processEntry(entry)
	}

	// Purge pending deletes
	for _, sequence := range pendingDeletes {
		err := c.RemoveSkipped(sequence)
		if err != nil {
			base.Warn("Error purging skipped sequence %d from skipped sequence queue, %v", sequence, err)
		} else {
			dbExpvars.Add("abandoned_seqs", 1)
		}
	}

	return true
}

// FOR TESTS ONLY: Blocks until the given sequence has been received.
func (c *changeCache) waitForSequence(sequence uint64) {
	var i int
	for i = 0; i < 20; i++ {
		c.lock.Lock()
		nextSequence := c.nextSequence
		c.lock.Unlock()
		if nextSequence >= sequence+1 {
			base.Logf("waitForSequence(%d) took %d ms", sequence, i*100)
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
		c.lock.Lock()
		nextSequence := c.nextSequence
		c.lock.Unlock()
		if nextSequence >= sequence+1 {
			foundInMissing := false
			for _, skippedSeq := range c.skippedSeqs {
				if skippedSeq.seq == sequence {
					foundInMissing = true
					break
				}
			}
			if !foundInMissing {
				base.Logf("waitForSequence(%d) took %d ms", sequence, i*100)
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
func (c *changeCache) DocChanged(docID string, docJSON []byte) {
	entryTime := time.Now()
	// ** This method does not directly access any state of c, so it doesn't lock.
	go func() {
		// Is this a user/role doc?
		if strings.HasPrefix(docID, auth.UserKeyPrefix) {
			c.processPrincipalDoc(docID, docJSON, true)
			return
		} else if strings.HasPrefix(docID, auth.RoleKeyPrefix) {
			c.processPrincipalDoc(docID, docJSON, false)
			return
		}

		// First unmarshal the doc (just its metadata, to save time/memory):
		doc, err := unmarshalDocumentSyncData(docJSON, false)
		if err != nil || !doc.hasValidSyncData() {
			base.Warn("changeCache: Error unmarshaling doc %q: %v", docID, err)
			return
		}

		if doc.Sequence <= c.initialSequence {
			return // Tap is sending us an old value from before I started up; ignore it
		}

		// Record a histogram of the Tap feed's lag:
		tapLag := time.Since(doc.TimeSaved) - time.Since(entryTime)
		lagMs := int(tapLag/(100*time.Millisecond)) * 100
		changeCacheExpvars.Add(fmt.Sprintf("lag-tap-%04dms", lagMs), 1)

		// If the doc update wasted any sequences due to conflicts, add empty entries for them:
		for _, seq := range doc.UnusedSequences {
			base.LogTo("Cache", "Received unused #%d for (%q / %q)", seq, docID, doc.CurrentRev)
			change := &LogEntry{
				Sequence:     seq,
				TimeReceived: time.Now(),
				TimeSaved:    time.Now(),
			}
			changeCacheExpvars.Add("processEntry-count-UnusedSequences", 1)
			c.processEntry(change)
		}

		if doc.TimeSaved.IsZero() {
			base.Warn("Found doc with zero timeSaved, docID=%s", docID)
		}

		// Now add the entry for the new doc revision:
		change := &LogEntry{
			Sequence:     doc.Sequence,
			DocID:        docID,
			RevID:        doc.CurrentRev,
			Flags:        doc.Flags,
			TimeReceived: time.Now(),
			TimeSaved:    doc.TimeSaved,
			Channels:     doc.Channels,
		}
		base.LogTo("Cache", "Received #%d after %3dms (%q / %q)", change.Sequence, int(tapLag/time.Millisecond), change.DocID, change.RevID)

		changeCacheExpvars.Add("processEntry-count-DocChanged", 1)
		changedChannels := c.processEntry(change)

		if c.onChange != nil && len(changedChannels) > 0 {
			c.onChange(changedChannels)
		}
	}()
}

func (c *changeCache) processPrincipalDoc(docID string, docJSON []byte, isUser bool) {
	// Currently the cache isn't really doing much with user docs; mostly it needs to know about
	// them because they have sequence numbers, so without them the sequence of sequences would
	// have gaps in it, causing later sequences to get stuck in the queue.
	princ, err := c.context.Authenticator().UnmarshalPrincipal(docJSON, "", 0, isUser)
	if princ == nil {
		base.Warn("changeCache: Error unmarshaling doc %q: %v", docID, err)
		return
	}
	sequence := princ.Sequence()
	if sequence <= c.initialSequence {
		return // Tap is sending us an old value from before I started up; ignore it
	}

	// Now add the (somewhat fictitious) entry:
	change := &LogEntry{
		Sequence:     sequence,
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	if isUser {
		change.DocID = "_user/" + princ.Name()
	} else {
		change.DocID = "_role/" + princ.Name()
	}

	base.LogTo("Cache", "Received #%d (%q)", change.Sequence, change.DocID)

	changeCacheExpvars.Add("processEntry-count-processPrincipalDoc", 1)
	c.processEntry(change)
}

// Handles a newly-arrived LogEntry.
func (c *changeCache) processEntry(change *LogEntry) base.Set {
	changeCacheLockTime := time.Now()
	c.lock.Lock()
	defer c.lock.Unlock()

	lag := time.Since(changeCacheLockTime)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-cache-lock-processEntry-%05dms", lagMs), 1)

	if c.logsDisabled {
		return nil
	}

	sequence := change.Sequence
	nextSequence := c.nextSequence
	if _, found := c.receivedSeqs[sequence]; found {
		base.LogTo("Cache+", "  Ignoring duplicate of #%d", sequence)
		return nil
	}
	c.receivedSeqs[sequence] = struct{}{}
	// FIX: c.receivedSeqs grows monotonically. Need a way to remove old sequences.

	var changedChannels base.Set
	if sequence == nextSequence || nextSequence == 0 {
		// This is the expected next sequence so we can add it now:
		changedChannels = c._addToCache(change, false)
		// Also add any pending sequences that are now contiguous:
		changedChannels = changedChannels.Union(c._addPendingLogs())
	} else if sequence > nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		heap.Push(&c.pendingLogs, change)
		numPending := len(c.pendingLogs)
		base.LogTo("Cache", "  Deferring #%d (%d now waiting for #%d...#%d)",
			sequence, numPending, nextSequence, c.pendingLogs[0].Sequence-1)
		changeCacheExpvars.Get("maxPending").(*base.IntMax).SetIfMax(int64(numPending))
		if numPending > c.options.CachePendingSeqMaxNum {
			// Too many pending; add the oldest one:
			dbExpvars.Add("pending_cache_full", 1)
			changedChannels = c._addPendingLogs()
		}
	} else if sequence > c.initialSequence {
		// Out-of-order sequence received!
		// Remove from skipped sequence queue
		wasSkipped := false
		if c.skippedSeqs.Remove(sequence) != nil {
			// Error removing from skipped sequences
			dbExpvars.Add("late_find_fail", 1)
			base.LogTo("Cache", "  Received unexpected out-of-order change - not in skippedSeqs (seq %d, expecting %d) doc %q / %q", sequence, nextSequence, change.DocID, change.RevID)
		} else {
			dbExpvars.Add("late_find_success", 1)
			base.LogTo("Cache", "  Received previously skipped out-of-order change (seq %d, expecting %d) doc %q / %q ", sequence, nextSequence, change.DocID, change.RevID)
			wasSkipped = true
		}

		changedChannels = c._addToCache(change, wasSkipped)

	}
	return changedChannels
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (c *changeCache) _addToCache(change *LogEntry, isLateSequence bool) base.Set {

	sentToCache := time.Now()
	if change.Sequence >= c.nextSequence {
		c.nextSequence = change.Sequence + 1
	}
	if change.DocID == "" {
		return nil // this was a placeholder for an unused sequence
	}
	addedTo := make([]string, 0, 4)
	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory

	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			channelCache := c._getChannelCache(channelName)
			channelCache.addToCache(change, removal != nil)
			addedTo = append(addedTo, channelName)
			if isLateSequence {
				channelCache.AddLateSequence(change)
			}
		}
	}

	if EnableStarChannelLog {
		c._getChannelCache(channels.UserStarChannel).addToCache(change, false)
		addedTo = append(addedTo, channels.UserStarChannel)
	}

	// Record a histogram of the overall lag from the time the doc was saved:
	lag := time.Since(change.TimeSaved)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-total-%05dms", lagMs), 1)
	if lagMs > 10000000 {
		base.Warn("Warning - extremely long total lag - time saved was: %v", change.TimeSaved)
	}
	// ...and from the time the doc was received from Tap:
	lag = time.Since(change.TimeReceived)
	lagMs = int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-queue-%05dms", lagMs), 1)

	// ...and from the time the doc was sent for caching:
	lag = time.Since(sentToCache)
	lagMs = int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-caching-%05dms", lagMs), 1)

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
			changedChannels = changedChannels.Union(c._addToCache(change, false))
		} else if len(c.pendingLogs) > c.options.CachePendingSeqMaxNum || time.Since(c.pendingLogs[0].TimeReceived) >= c.options.CachePendingSeqMaxWait {
			base.LogTo("Cache", "Adding #%d to skipped", c.nextSequence)
			changeCacheExpvars.Add("outOfOrder", 1)
			c.addToSkipped(c.nextSequence)
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

func (c *changeCache) _getChannelCache(channelName string) *channelCache {
	cache := c.channelCaches[channelName]
	if cache == nil {
		cache = newChannelCache(c.context, channelName, c.initialSequence+1)
		c.channelCaches[channelName] = cache
	}
	return cache
}

//////// CHANGE ACCESS:

func (c *changeCache) GetChangesInChannel(channelName string, options ChangesOptions) ([]*LogEntry, error) {
	if c.stopped {
		return nil, base.HTTPErrorf(503, "Database closed")
	}
	return c._getChannelCache(channelName).GetChanges(options)
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
	for name, _ := range c.channelCaches {
		array[i] = name
		i++
	}
	return base.SetFromArray(array)
}

func (c *changeCache) addToSkipped(sequence uint64) {

	dbExpvars.Add("skipped_sequences", 1)
	c.PushSkipped(SkippedSequence{seq: sequence, timeAdded: time.Now()})
}

func (c *changeCache) getOldestSkippedSequence() uint64 {
	c.skippedSeqLock.RLock()
	defer c.skippedSeqLock.RUnlock()
	if len(c.skippedSeqs) > 0 {
		return c.skippedSeqs[0].seq
	} else {
		return uint64(0)
	}
}

//////// LOG PRIORITY QUEUE

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

func (c *changeCache) PushSkipped(x SkippedSequence) {
	c.skippedSeqLock.Lock()
	defer c.skippedSeqLock.Unlock()
	c.skippedSeqs.Push(x)
}

// Remove does a simple binary search to find and remove.
func (h *SkippedSequenceQueue) Remove(x uint64) error {

	// TODO: use sort.SearchInts instead?
	old := *h
	low := 0
	high := len(old) - 1

	for low <= high {
		mid := (low + high) >> 1
		midVal := old[mid].seq
		if midVal < x {
			low = mid + 1
		} else if midVal > x {
			high = mid - 1
		} else {
			//remove from list and return
			*h = append(old[:mid], old[mid+1:]...)
			return nil
		}
	}
	return errors.New("Value not found")
}

// We always know that incoming missed sequence numbers will be larger than any previously
// added, so we don't need to do any sorting - just append to the slice
func (h *SkippedSequenceQueue) Push(x SkippedSequence) {
	*h = append(*h, x)
}

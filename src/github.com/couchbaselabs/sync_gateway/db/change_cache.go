package db

import (
	"container/heap"
	"expvar"
	"fmt"
	"github.com/couchbaselabs/sync_gateway/channels"
	"sort"
	"sync"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
)

var ChannelLogCacheLength = 50                     // Keep at least this many entries in cache
var ChannelLogCacheAge = 60 * time.Second          // Keep entries at least this long
var MaxChannelLogPendingCount = 1000               // Max number of waiting sequences
var MaxChannelLogPendingWaitTime = 5 * time.Second // Max time we'll wait for a missing sequence

// Enable keeping a channel-log for the "*" channel. The only time this channel is needed is if
// someone has access to "*" (e.g. admin-party) and tracks its changes feed.
var EnableStarChannelLog = true

var changeCacheExpvars *expvar.Map

func init() {
	changeCacheExpvars = expvar.NewMap("syncGateway_changeCache")
	changeCacheExpvars.Set("maxPending", new(base.IntMax))
}

// Manages a cache of the recent change history of all channels.
type changeCache struct {
	logsDisabled bool                  // If true, ignore incoming tap changes
	nextSequence uint64                // Next consecutive sequence number to add
	pendingLogs  LogEntries            // Out-of-sequence entries waiting to be cached
	channelLogs  map[string]LogEntries // The cache itself!
	onChange     func(base.Set)        // Client callback that notifies of channel changes
	lock         sync.RWMutex          // Coordinates access to channelLogs
}

// A basic LogEntry annotated with its channels and arrival time
type LogEntry struct {
	channels.LogEntry            // The sequence, doc/rev ID
	received          time.Time  // Time received from tap feed
	timeSaved         time.Time  // Time doc revision was saved
	channels          ChannelMap // Channels this entry is in
}

// A priority-queue of LogEntries, kept ordered by increasing sequence #.
type LogEntries []*LogEntry

//////// HOUSEKEEPING:

// Initializes a new changeCache.
// lastSequence is the last known database sequence assigned.
// onChange is an optional function that will be called to notify of channel changes.
func (c *changeCache) Init(lastSequence uint64, onChange func(base.Set)) {
	c.nextSequence = lastSequence + 1
	c.onChange = onChange
	c.channelLogs = make(map[string]LogEntries, 10)
	heap.Init(&c.pendingLogs)
	base.LogTo("Cache", "Initialized changeCache with nextSequence=#%d", c.nextSequence)

	// Start a background task for periodic housekeeping:
	go func() {
		for c.CleanUp() {
			time.Sleep(MaxChannelLogPendingWaitTime / 2)
		}
	}()
}

// Stops the cache. Clears its state and tells the housekeeping task to stop.
func (c *changeCache) Stop() {
	c.lock.Lock()
	c.channelLogs = nil
	c.pendingLogs = nil
	c.logsDisabled = true
	c.lock.Unlock()
}

// Forgets all cached changes for all channels.
func (c *changeCache) ClearLogs() {
	c.lock.Lock()
	c.nextSequence = 0
	c.channelLogs = make(map[string]LogEntries, 10)
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

// FOR TESTS ONLY: Blocks until the given sequence has been received.
func (c *changeCache) waitForSequence(sequence uint64) {
	var i int
	for i = 0; i < 20; i++ {
		c.lock.Lock()
		nextSequence := c.nextSequence
		c.lock.Unlock()
		if nextSequence >= sequence+1 {
			base.Log("waitForSequence(%d) took %d ms", sequence, i*100)
			return
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
		// First unmarshal the doc (just its metadata, to save time/memory):
		doc, err := unmarshalDocumentSyncData(docJSON, false)
		if err != nil || !doc.hasValidSyncData() {
			base.Warn("changeCache: Error unmarshaling doc %q: %v", docID, err)
			return
		}

		// Record a histogram of the Tap feed's lag:
		tapLag := time.Since(doc.TimeSaved) - time.Since(entryTime)
		lagMs := int(tapLag/(100*time.Millisecond)) * 100
		changeCacheExpvars.Add(fmt.Sprintf("tapLag-%04dms", lagMs), 1)

		// If the doc update wasted any sequences due to conflicts, add empty entries for them:
		for _, seq := range doc.UnusedSequences {
			base.LogTo("Cache", "Received unused #%d for (%q / %q)", seq, docID, doc.CurrentRev)
			change := &LogEntry{
				LogEntry: channels.LogEntry{
					Sequence: seq,
				},
				received: time.Now(),
			}
			c.processEntry(change)
		}

		// Now add the entry for the new doc revision:
		change := &LogEntry{
			LogEntry: channels.LogEntry{
				Sequence: doc.Sequence,
				DocID:    docID,
				RevID:    doc.CurrentRev,
				Flags:    doc.Flags,
			},
			received:  time.Now(),
			timeSaved: doc.TimeSaved,
			channels:  doc.Channels,
		}
		base.LogTo("Cache", "Received #%d (%q / %q)", change.Sequence, change.DocID, change.RevID)

		changedChannels := c.processEntry(change)
		if c.onChange != nil && len(changedChannels) > 0 {
			c.onChange(changedChannels)
		}
	}()
}

// Handles a newly-arrived LogEntry.
func (c *changeCache) processEntry(change *LogEntry) base.Set {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.logsDisabled {
		return nil
	}

	var changedChannels base.Set
	if change.Sequence == c.nextSequence || c.nextSequence == 0 {
		// This is the expected next sequence so we can add it now:
		changedChannels = c._addToCache(change)
		// Also add any pending sequences that are now contiguous:
		changedChannels = changedChannels.Union(c._addPendingLogs())
	} else if change.Sequence > c.nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		heap.Push(&c.pendingLogs, change)
		numPending := len(c.pendingLogs)
		base.LogTo("Cache", "  Deferring #%d (gap of %d sequences; %d now deferred)",
			change.Sequence, c.pendingLogs[0].Sequence-c.nextSequence, numPending)
		changeCacheExpvars.Get("maxPending").(*base.IntMax).SetIfMax(int64(numPending))
		if numPending > MaxChannelLogPendingCount {
			// Too many pending; add the oldest one:
			changedChannels = c._addPendingLogs()
		}
	} else {
		// Out-of-order sequence received!
		base.Warn("  Received out-of-order change (seq %d, expecting %d) doc %q / %q", change.Sequence, c.nextSequence, change.DocID, change.RevID)
		changedChannels = c._addToCache(change)
	}
	return changedChannels
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.
func (c *changeCache) _addToCache(change *LogEntry) base.Set {
	if change.Sequence >= c.nextSequence {
		c.nextSequence = change.Sequence + 1
	}
	if change.DocID == "" {
		return nil // this was a placeholder for an unused sequence
	}
	addedTo := make([]string, 0, 4)
	ch := change.channels
	change.channels = nil // not needed anymore, so free some memory
	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			c._addToChannelCache(change, removal != nil, channelName)
			addedTo = append(addedTo, channelName)
		}
	}

	if EnableStarChannelLog {
		if len(addedTo) == 0 {
			// If change isn't in any channel, add to "*" to ensure it gets recorded
			c._addToChannelCache(change, false, "*")
		}
		addedTo = append(addedTo, "*")
	}

	// Record a histogram of the overall lag from the time the doc was saved:
	lag := time.Since(change.timeSaved)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-%04dms", lagMs), 1)

	return base.SetFromArray(addedTo)
}

// Low-level method to add a LogEntry to a single channel's cache.
func (c *changeCache) _addToChannelCache(change *LogEntry, isRemoval bool, channelName string) {
	log := c.channelLogs[channelName]
	if !isRemoval {
		log.addChange(change)
	} else {
		removalChange := *change
		removalChange.Flags |= channels.Removed
		log.addChange(&removalChange)
	}
	c.channelLogs[channelName] = log
	c._pruneCache(channelName)
	base.LogTo("Cache", "    #%d ==> channel %q", change.Sequence, channelName)
}

// Add the first change(s) from pendingLogs if they're the next sequence or have been waiting too
// long, or if there are simply too many pending entries.
// Returns the channels that changed.
func (c *changeCache) _addPendingLogs() base.Set {
	var changedChannels base.Set
	for len(c.pendingLogs) > 0 {
		change := c.pendingLogs[0]
		isNext := change.Sequence == c.nextSequence
		if isNext || len(c.pendingLogs) > MaxChannelLogPendingCount || time.Since(c.pendingLogs[0].received) >= MaxChannelLogPendingWaitTime {
			if !isNext {
				base.Warn("changeCache: Giving up, accepting #%d even though #%d is missing",
					change.Sequence, c.nextSequence)
				changeCacheExpvars.Add("outOfOrder", 1)
			}
			heap.Pop(&c.pendingLogs)
			changedChannels = changedChannels.Union(c._addToCache(change))
		} else {
			break
		}
	}
	return changedChannels
}

//////// CLEANUP:

// Cleanup function, invoked periodically.
// Inserts pending entries that have been waiting too long.
// Removes entries older than MaxChannelLogCacheAge from the cache.
// Returns false if the changeCache has been closed.
func (c *changeCache) CleanUp() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.channelLogs == nil {
		return false
	}

	// If entries have been pending too long, add them to the cache:
	changedChannels := c._addPendingLogs()
	if c.onChange != nil && len(changedChannels) > 0 {
		c.onChange(changedChannels)
	}

	// Remove old cache entries:
	for channelName, _ := range c.channelLogs {
		c._pruneCache(channelName)
	}
	return true
}

// Internal helper that prunes a single channel's cache. Caller MUST be holding the lock.
func (c *changeCache) _pruneCache(channelName string) {
	pruned := 0
	log := c.channelLogs[channelName]
	for len(log) > ChannelLogCacheLength && time.Since(log[0].received) > ChannelLogCacheAge {
		log = log[1:]
		pruned++
	}
	if pruned > 0 {
		c.channelLogs[channelName] = log
		base.LogTo("Cache", "Pruned %d old entries from channel %q", pruned, channelName)
	}
}

//////// CHANGE ACCESS:

// Returns all of the cached entries for sequences greater than 'since' in the given channels.
// Entries are returned in increasing-sequence order.
func (c *changeCache) GetChangesSince(channels base.Set, since uint64) []*LogEntry {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if channels.Contains("*") {
		// To get global changes, need to union together all channels
		channels = c._allChannels()
	}

	haveSequence := make(map[uint64]bool, 50)
	entries := make(LogEntries, 0, 50)
	for channelName, _ := range channels {
		log := c.channelLogs[channelName]
		for i := len(log) - 1; i >= 0 && log[i].Sequence > since; i-- {
			if entry := log[i]; !haveSequence[entry.Sequence] {
				haveSequence[entry.Sequence] = true
				entries = append(entries, entry)
			}
		}
	}
	sort.Sort(entries)
	var first uint64
	if len(entries) > 0 {
		first = entries[0].Sequence
	}
	base.LogTo("Cache", "getChangesSince(%q, %d) --> %d changes from #%d",
		channels.ToArray(), since, len(entries), first)
	return entries
}

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *changeCache) GetChangesInChannelSince(channelName string, since uint64) []*LogEntry {
	if channelName == "*" {
		// To get global changes, need to union together all channels
		return c.GetChangesSince(base.SetOf("*"), since)
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	// Find the first entry in the log to return:
	log := c.channelLogs[channelName]
	if log == nil {
		return nil
	}
	var start int
	for start = len(log) - 1; start >= 0 && log[start].Sequence > since; start-- {
	}
	start++

	// Now copy the entries:
	entries := make([]*LogEntry, len(log)-start)
	copy(entries[:], log[start:])

	var first uint64
	if len(entries) > 0 {
		first = entries[0].Sequence
	}
	base.LogTo("Cache", "getChangesInChannelSince(%q, %d) --> %d changes from #%d",
		channelName, since, len(entries), first)
	return entries
}

func (c *changeCache) _allChannels() base.Set {
	array := make([]string, len(c.channelLogs))
	i := 0
	for name, _ := range c.channelLogs {
		array[i] = name
		i++
	}
	return base.SetFromArray(array)
}

//////// LOGENTRIES (SORTABLE / HEAP INTERFACES)

func (h LogEntries) Len() int           { return len(h) }
func (h LogEntries) Less(i, j int) bool { return h[i].Sequence < h[j].Sequence }
func (h LogEntries) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *LogEntries) Push(x interface{}) {
	*h = append(*h, x.(*LogEntry))
}

func (h *LogEntries) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Adds an entry to an array of LogEntries. Any existing entry with the same DocID is removed.
func (logp *LogEntries) addChange(change *LogEntry) {
	log := *logp
	end := len(log) - 1
	for i := end; i >= 0; i-- {
		if log[i].DocID == change.DocID {
			copy(log[i:], log[i+1:])
			log[end] = change
			return
		}
	}
	*logp = append(log, change)
}

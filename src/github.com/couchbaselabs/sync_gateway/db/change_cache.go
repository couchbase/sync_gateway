package db

import (
	"container/heap"
	"expvar"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

var MaxChannelLogPendingCount = 10000              // Max number of waiting sequences
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
	context         *DatabaseContext
	logsDisabled    bool                     // If true, ignore incoming tap changes
	nextSequence    uint64                   // Next consecutive sequence number to add
	initialSequence uint64                   // DB's current sequence at startup time
	receivedSeqs    map[uint64]struct{}      // Set of all sequences received
	pendingLogs     LogPriorityQueue         // Out-of-sequence entries waiting to be cached
	channelCaches   map[string]*channelCache // A cache of changes for each channel
	onChange        func(base.Set)           // Client callback that notifies of channel changes
	lock            sync.RWMutex             // Coordinates access to struct fields
}

type LogEntry channels.LogEntry

type LogEntries []*LogEntry

// A priority-queue of LogEntries, kept ordered by increasing sequence #.
type LogPriorityQueue []*LogEntry

//////// HOUSEKEEPING:

// Initializes a new changeCache.
// lastSequence is the last known database sequence assigned.
// onChange is an optional function that will be called to notify of channel changes.
func (c *changeCache) Init(context *DatabaseContext, lastSequence uint64, onChange func(base.Set)) {
	c.context = context
	c.initialSequence = lastSequence
	c.nextSequence = lastSequence + 1
	c.onChange = onChange
	c.channelCaches = make(map[string]*channelCache, 10)
	c.receivedSeqs = make(map[uint64]struct{})
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
	c.channelCaches = nil
	c.pendingLogs = nil
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
			}
			c.processEntry(change)
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
	}
	if isUser {
		change.DocID = "_user/" + princ.Name()
	} else {
		change.DocID = "_role/" + princ.Name()
	}

	base.LogTo("Cache", "Received #%d (%q)", change.Sequence, change.DocID)

	c.processEntry(change)
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
		base.LogTo("Cache+", "  Ignoring duplicate of #%d", sequence)
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
		base.LogTo("Cache", "  Deferring #%d (%d now waiting for #%d...#%d)",
			sequence, numPending, nextSequence, c.pendingLogs[0].Sequence-1)
		changeCacheExpvars.Get("maxPending").(*base.IntMax).SetIfMax(int64(numPending))
		if numPending > MaxChannelLogPendingCount {
			// Too many pending; add the oldest one:
			changedChannels = c._addPendingLogs()
		}
	} else if sequence > c.initialSequence {
		// Out-of-order sequence received!
		base.Warn("  Received out-of-order change (seq %d, expecting %d) doc %q / %q", sequence, nextSequence, change.DocID, change.RevID)
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
	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory
	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			c._getChannelCache(channelName).addToCache(change, removal != nil)
			addedTo = append(addedTo, channelName)
		}
	}

	if EnableStarChannelLog {
		c._getChannelCache("*").addToCache(change, false)
		addedTo = append(addedTo, "*")
	}

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

// Add the first change(s) from pendingLogs if they're the next sequence or have been waiting too
// long, or if there are simply too many pending entries.
// Returns the channels that changed.
func (c *changeCache) _addPendingLogs() base.Set {
	var changedChannels base.Set
	for len(c.pendingLogs) > 0 {
		change := c.pendingLogs[0]
		isNext := change.Sequence == c.nextSequence
		if isNext || len(c.pendingLogs) > MaxChannelLogPendingCount || time.Since(c.pendingLogs[0].TimeReceived) >= MaxChannelLogPendingWaitTime {
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
	return c.getChannelCache(channelName).GetChanges(options)
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

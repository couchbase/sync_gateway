package db

import (
	"container/heap"
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

var ChannelLogCacheLength = 50                     // Keep at least this many entries in cache
var ChannelLogCacheAge = 60 * time.Second          // Keep entries at least this long
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
	logsDisabled    bool                  // If true, ignore incoming tap changes
	nextSequence    uint64                // Next consecutive sequence number to add
	initialSequence uint64                // DB's current sequence at startup time
	pendingLogs     LogEntries            // Out-of-sequence entries waiting to be cached
	receivedSeqs    map[uint64]struct{}   // Set of all sequences received
	channelLogs     map[string]LogEntries // The cache itself!
	onChange        func(base.Set)        // Client callback that notifies of channel changes
	lock            sync.RWMutex          // Coordinates access to channelLogs
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
func (c *changeCache) Init(context *DatabaseContext, lastSequence uint64, onChange func(base.Set)) {
	c.context = context
	c.initialSequence = lastSequence
	c.nextSequence = lastSequence + 1
	c.onChange = onChange
	c.channelLogs = make(map[string]LogEntries, 10)
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
		base.LogTo("Cache", "Received #%d after %3dms (%q / %q)", change.Sequence, int(tapLag/time.Millisecond), change.DocID, change.RevID)

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
	ch := change.channels
	change.channels = nil // not needed anymore, so free some memory
	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			c._addToChannelCache(change, removal != nil, channelName)
			addedTo = append(addedTo, channelName)
		}
	}

	if EnableStarChannelLog {
		c._addToChannelCache(change, false, "*")
		addedTo = append(addedTo, "*")
	}

	// Record a histogram of the overall lag from the time the doc was saved:
	lag := time.Since(change.timeSaved)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-total-%04dms", lagMs), 1)
	// ...and from the time the doc was received from Tap:
	lag = time.Since(change.received)
	lagMs = int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-queue-%04dms", lagMs), 1)

	return base.SetFromArray(addedTo)
}

// Low-level method to add a LogEntry to a single channel's cache.
func (c *changeCache) _addToChannelCache(change *LogEntry, isRemoval bool, channelName string) {
	log := c.channelLogs[channelName]
	if !isRemoval {
		log.appendChange(change)
	} else {
		removalChange := *change
		removalChange.Flags |= channels.Removed
		log.appendChange(&removalChange)
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

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *changeCache) GetCachedChangesInChannel(channelName string, options ChangesOptions) (validSince uint64, result []*LogEntry) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// Find the first entry in the log to return:
	log := c.channelLogs[channelName]
	if len(log) == 0 {
		base.LogTo("Cache", "getChangesInChannelSince(%q, %d) --> unknown channel",
			channelName, options.Since)
		return
	}
	var start int
	for start = len(log) - 1; start >= 0 && log[start].Sequence > options.Since; start-- {
	}
	start++

	if start > 0 {
		validSince = log[start-1].Sequence
	} else {
		validSince = log[0].Sequence - 1
	}

	n := len(log) - start
	if options.Limit > 0 && n < options.Limit {
		n = options.Limit
	}
	result = make([]*LogEntry, n)
	copy(result[0:], log[start:])
	base.LogTo("Cache", "getChangesInChannelSince(%q, %d) --> %d changes valid after #%d",
		channelName, options.Since, n, validSince)
	return
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

// Top-level method to get all the changes in a channel since the sequence 'since'.
// If the cache doesn't go back far enough, the view will be queried.
// View query results may be fed back into the cache if there's room.
func (c *changeCache) GetChangesInChannel(channelName string, options ChangesOptions) ([]*LogEntry, error) {
	cacheValidSince, resultFromCache := c.GetCachedChangesInChannel(channelName, options)

	// Did the cache fulfill the entire request?
	if resultFromCache != nil && cacheValidSince <= options.Since {
		return resultFromCache, nil
	}

	// No, need to backfill from view:
	resultFromView, err := c.context.getChangesInChannelFromView(channelName, cacheValidSince, options)
	if err != nil {
		return nil, err
	}

	//** Now acquire the lock
	c.lock.RLock()
	defer c.lock.RUnlock()

	// It's unlikely-but-possible that the view query will include sequences that haven't been
	// received over Tap yet; if so, trim those, because they'll confuse the ordering.
	numFromView := len(resultFromView)
	for numFromView > 0 && resultFromView[numFromView-1].Sequence >= c.nextSequence {
		numFromView--
		resultFromView = resultFromView[0:numFromView]
	}

	// Cache some of the view results, if there's room in the cache:
	roomInCache := ChannelLogCacheLength - len(resultFromCache)
	if roomInCache > 0 {
		log := c.channelLogs[channelName]
		if numFromView > 0 {
			if numFromView < roomInCache {
				roomInCache = numFromView
			}
			toCache := resultFromView[numFromView-roomInCache:]
			numPrepended := log.prependChanges(toCache)
			if numPrepended > 0 {
				base.LogTo("Cache", "  Added %d entries from view to cache of %q", numPrepended, channelName)
			}
		}
		if options.Since == 0 && len(log) < ChannelLogCacheLength {
			// If the view query goes back to the dawn of time, record a fake zero sequence in
			// the cache so any future requests won't need to hit the view again:
			fake := LogEntry{
				LogEntry: channels.LogEntry{
					Sequence: 0,
				},
				received: time.Now(),
			}
			log.prependChanges([]*LogEntry{&fake})
		}
		c.channelLogs[channelName] = log
	}

	// Concatenate the view & cache results:
	result := resultFromView
	if options.Limit == 0 || len(result) < options.Limit {
		n := len(resultFromCache)
		if options.Limit > 0 {
			n = options.Limit - len(result)
		}
		result = append(result, resultFromCache[0:n]...)
	}
	return result, nil
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

// Adds an entry to the end of an array of LogEntries.
// Any existing entry with the same DocID is removed.
func (logp *LogEntries) appendChange(change *LogEntry) {
	log := *logp
	end := len(log) - 1
	if end >= 0 {
		if change.Sequence <= log[end].Sequence {
			base.Warn("LogEntries.appendChange: out-of-order sequence #%d (last is #%d)",
				change.Sequence, log[end].Sequence)
		}
		for i := end; i >= 0; i-- {
			if log[i].DocID == change.DocID {
				copy(log[i:], log[i+1:])
				log[end] = change
				return
			}
		}
	}
	*logp = append(log, change)
}

// Prepends an array of entries to this one, skipping ones that I already have
func (logp *LogEntries) prependChanges(changes LogEntries) int {
	log := *logp
	if len(log) == 0 {
		*logp = make(LogEntries, len(changes))
		copy(*logp, changes)
		return len(changes)
	}
	firstSeq := log[0].Sequence
	for i := len(changes) - 1; i >= 0; i-- {
		if changes[i].Sequence < firstSeq {
			newLog := make(LogEntries, 0, i+len(log))
			newLog = append(newLog, changes[0:i+1]...)
			newLog = append(newLog, log...)
			*logp = newLog
			return i + 1
		}
	}
	return 0
}

package db

import (
	"container/heap"
	"fmt"
	"github.com/couchbaselabs/sync_gateway/channels"
	"sort"
	"sync"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
)

var MinChannelLogCacheLength = 50            // Keep at least this many in cache
var MaxChannelLogCacheAge = 60 * time.Second // Expire cache entries received longer ago than this

// Enable keeping a channel-log for the "*" channel. *ALL* revisions are written to this channel,
// which could be expensive in a busy environment. The only time this channel is needed is if
// someone has access to "*" (e.g. admin-party) and tracks its changes feed.
var EnableStarChannelLog = true

// Manages a cache of the recent change history of all channels.
type changeCache struct {
	logsDisabled bool
	nextSequence uint64
	pendingLogs  LogEntries
	channelLogs  map[string]LogEntries
	onChange     func(base.Set)
	lock         sync.RWMutex // Coordinates access to channelLogs
}

// A basic LogEntry annotated with its channels and arrival time
type LogEntry struct {
	channels.LogEntry
	received time.Time
	channels ChannelMap
}

// A heap of LogEntry structs, ordered by priority.
type LogEntries []*LogEntry

//////// HOUSEKEEPING:

// Initializes a new changeCache.
func (c *changeCache) Init(onChange func(base.Set)) {
	c.onChange = onChange
	c.channelLogs = make(map[string]LogEntries, 10)
	heap.Init(&c.pendingLogs)

	// Start a background task to periodically prune the cache:
	go func() {
		for {
			time.Sleep(MaxChannelLogCacheAge / 4)
			if !c.PruneCache() {
				break
			}
		}
	}()
}

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

// TESTING ONLY: Blocks until the given sequence has been received.
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
	// ** This method does not directly access any state of c, so it doesn't lock.
	doc, err := unmarshalDocument(docID, docJSON)
	if err != nil || !doc.hasValidSyncData() {
		return
	}

	change := &LogEntry{
		LogEntry: channels.LogEntry{
			Sequence: doc.Sequence,
			DocID:    docID,
			RevID:    doc.CurrentRev,
			Flags:    doc.Flags,
		},
		received: time.Now(),
		channels: doc.Channels,
	}
	base.LogTo("Cache", "Received #%d (%q / %q)", change.Sequence, change.DocID, change.RevID)

	changedChannels := c.processEntry(change)
	if c.onChange != nil && len(changedChannels) > 0 {
		c.onChange(changedChannels)
	}
}

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
		for len(c.pendingLogs) > 0 && c.pendingLogs[0].Sequence == c.nextSequence {
			oldEntry := heap.Pop(&c.pendingLogs).(*LogEntry)
			moreChannels := c._addToCache(oldEntry)
			changedChannels = changedChannels.Union(moreChannels)
		}
	} else if change.Sequence > c.nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		base.Log("changeCache: Gap in sequences! Putting #%d on ice", change.Sequence)
		heap.Push(&c.pendingLogs, change)
	} else {
		// Out-of-order sequence received!
		base.Warn("Received out-of-order change (seq %d, expecting %d) doc %q / %q", change.Sequence, c.nextSequence, change.DocID, change.RevID)
		changedChannels = c._addToCache(change)
	}
	return changedChannels
}

// Adds an entry to the cache.
func (c *changeCache) _addToCache(change *LogEntry) base.Set {
	addedTo := make([]string, 2)
	ch := change.channels
	change.channels = nil // not needed anymore, so free some memory
	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			log := c.channelLogs[channelName]
			if removal == nil {
				log.addChange(change)
			} else {
				removalChange := *change
				removalChange.Flags |= channels.Removed
				log.addChange(&removalChange)
			}
			c.channelLogs[channelName] = log
			c._pruneCache(channelName)
			addedTo = append(addedTo, channelName)
			base.LogTo("Cache", "Cached #%d (%q / %q) to channel %q", change.Sequence, change.DocID, change.RevID, channelName)
		}
	}
	c.nextSequence = change.Sequence + 1
	return base.SetFromArray(addedTo)
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

// Removes entries older than MaxChannelLogCacheAge from the cache.
// Returns false if the changeCache has been closed.
func (c *changeCache) PruneCache() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.channelLogs == nil {
		return false
	}
	for channelName, _ := range c.channelLogs {
		c._pruneCache(channelName)
	}
	return true
}

// Internal helper that prunes a single channel's cache. Caller MUST be holding the lock.
func (c *changeCache) _pruneCache(channelName string) {
	pruned := 0
	log := c.channelLogs[channelName]
	for len(log) > MinChannelLogCacheLength && time.Since(log[0].received) > MaxChannelLogCacheAge {
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
	entries := make(LogEntries, 50)
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
	if start == len(log) {
		return nil
	}
	entries := make([]*LogEntry, len(log)-start)
	copy(entries[:], log[start:])
	base.LogTo("Cache", "getCachedChangesInChannelSince(%q, %d) --> %d changes from #%d",
		channelName, since, len(entries), entries[0])
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

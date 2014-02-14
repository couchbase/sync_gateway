package db

import (
	"container/heap"
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
	nextSequence uint64
	channelLogs  map[string]LogEntries
	pendingLogs  LogEntries
	logsDisabled bool
	logsLock     sync.RWMutex
}

// A basic LogEntry annotated with its channels and arrival time
type LogEntry struct {
	channels.LogEntry
	Received     time.Time
	ChannelNames base.Set
}

// A heap of LogEntry structs, ordered by priority.
type LogEntries []*LogEntry

// Initializes a new changeCache.
func (c *changeCache) Init() {
	c.channelLogs = make(map[string]LogEntries, 10)
	heap.Init(&c.pendingLogs)

	// Start a timer to periodically prune the cache:
	go func() {
		for _ = range time.NewTicker(15 * time.Second).C {
			c.PruneCache()
		}
	}()
}

// Forgets all cached changes for all channels.
func (c *changeCache) ClearLogs() {
	c.logsLock.Lock()
	c.nextSequence = 0
	c.channelLogs = make(map[string]LogEntries, 10)
	c.pendingLogs = nil
	heap.Init(&c.pendingLogs)
	c.logsLock.Unlock()
}

// If set to false, DocChanged() becomes a no-op.
func (c *changeCache) EnableChannelLogs(enable bool) {
	c.logsDisabled = !enable
}

func (c *changeCache) DocChanged(docID string, docJSON []byte) base.Set {
	if c.logsDisabled {
		return nil
	}
	doc, err := unmarshalDocument(docID, docJSON)
	if err != nil || !doc.hasValidSyncData() {
		return nil
	}
	var flags uint8 = 0
	if doc.Deleted {
		flags |= channels.Deleted
	}
	if doc.NewestRev != "" {
		flags |= channels.Hidden
	}
	if _, inConflict := doc.History.winningRevision(); inConflict { //TEMP: Save flag in doc instead
		flags |= channels.Conflict
	}

	inChannels := make([]string, 0, len(doc.Channels))
	for channelName, removal := range doc.Channels {
		if removal == nil || removal.Seq == doc.Sequence {
			inChannels = append(inChannels, channelName)
			if removal != nil {
				flags |= channels.Removed
			}
		}
	}
	if len(inChannels) == 0 && EnableStarChannelLog {
		inChannels = append(inChannels, "*")
	}

	change := &LogEntry{
		LogEntry: channels.LogEntry{
			Sequence: doc.Sequence,
			DocID:    docID,
			RevID:    doc.CurrentRev,
			Flags:    flags,
		},
		Received:     time.Now(),
		ChannelNames: base.SetFromArray(inChannels),
	}
	base.LogTo("Cache", "Received #%d (%q / %q)", change.Sequence, change.DocID, change.RevID)

	var changedChannels base.Set

	if change.Sequence == c.nextSequence || c.nextSequence == 0 {
		// This is the expected next sequence so we can add it now:
		c.addToCache(change)
		changedChannels = change.ChannelNames
		// Also add any pending sequences that are now contiguous:
		for len(c.pendingLogs) > 0 && c.pendingLogs[0].Sequence == c.nextSequence {
			oldEntry := heap.Pop(&c.pendingLogs).(*LogEntry)
			c.addToCache(oldEntry)
			changedChannels = changedChannels.Union(oldEntry.ChannelNames)
		}
	} else if change.Sequence > c.nextSequence {
		// There's a missing sequence (or several), so put this one on ice until it arrives:
		base.TEMP("Gap in sequences! Putting #%d on ice", change.Sequence)
		heap.Push(&c.pendingLogs, change)
	} else {
		// Out-of-order sequence received!
		base.Warn("Received out-of-order change (seq %d, expecting %d) doc %q / %q", doc.Sequence, c.nextSequence, docID, doc.CurrentRev)
		panic("OUT OF ORDER") //TEMP
		c.addToCache(change)
		changedChannels = change.ChannelNames
	}

	return changedChannels
}

// Adds an entry to the cache.
func (c *changeCache) addToCache(change *LogEntry) {
	c.logsLock.Lock()
	defer c.logsLock.Unlock()
	for channelName, _ := range change.ChannelNames {
		c._pruneCache(channelName)
		log := c.channelLogs[channelName]
		log.addChange(change)
		c.channelLogs[channelName] = log
		base.LogTo("Cache", "Cached #%d (%q / %q) to channel %q", change.Sequence, change.DocID, change.RevID, channelName)
	}
	c.nextSequence = change.Sequence + 1
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
func (c *changeCache) PruneCache() {
	c.logsLock.Lock()
	defer c.logsLock.Unlock()
	for channelName, _ := range c.channelLogs {
		c._pruneCache(channelName)
	}
}

// Internal helper that prunes a single channel's cache. Caller MUST be holding the lock.
func (c *changeCache) _pruneCache(channelName string) {
	pruned := 0
	log := c.channelLogs[channelName]
	for len(log) > MinChannelLogCacheLength && time.Since(log[0].Received) > MaxChannelLogCacheAge {
		log = log[1:] //FIX: The backing array will keep growing
		pruned++
	}
	if pruned > 0 {
		c.channelLogs[channelName] = log
		base.LogTo("Cache", "Pruned %d old entries from channel %q", pruned, channelName)
	}
}

// Returns all of the cached entries for sequences greater than 'since' in the given channels.
// Entries are returned in increasing-sequence order.
func (c *changeCache) GetChangesSince(channels base.Set, since uint64) []*LogEntry {
	c.logsLock.RLock()
	defer c.logsLock.RUnlock()

	haveSequence := make(map[uint64]bool, 50)
	result := make(LogEntries, 50)
	for channelName, _ := range channels {
		log := c.channelLogs[channelName]
		for i := len(log) - 1; i >= 0 && log[i].Sequence > since; i-- {
			if entry := log[i]; !haveSequence[entry.Sequence] {
				haveSequence[entry.Sequence] = true
				result = append(result, entry)
			}
		}
	}
	sort.Sort(result)
	return result
}

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *changeCache) GetChangesInChannelSince(channelName string, since uint64) *channels.ChangeLog {
	c.logsLock.RLock()
	defer c.logsLock.RUnlock()

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
	entries := make([]*channels.LogEntry, 0, len(log)-start)
	for j := start; j < len(log); j++ {
		entries = append(entries, &log[j].LogEntry)
	}
	result := channels.ChangeLog{Entries: entries}
	if len(entries) > 0 {
		result.Since = entries[0].Sequence - 1
	}
	base.LogTo("Cache", "getCachedChangesInChannelSince(%q, %d) --> %d changes from #%d",
		channelName, since, len(entries), result.Since+1)
	return &result
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

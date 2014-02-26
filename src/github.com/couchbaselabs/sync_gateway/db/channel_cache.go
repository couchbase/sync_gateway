package db

import (
	"sync"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

var ChannelCacheMinLength = 50         // Keep at least this many entries in cache
var ChannelCacheMaxLength = 500        // Don't put more than this many entries in cache
var ChannelCacheAge = 60 * time.Second // Keep entries at least this long

const NoSeq = uint64(0x7FFFFFFFFFFFFFFF)

type channelCache struct {
	channelName string           // The channel name, duh
	context     *DatabaseContext // Database connection (used for view queries)
	logs        LogEntries       // Log entries in sequence order
	validFrom   uint64           // First sequence that logs is valid for
	lock        sync.RWMutex     // Controls access to logs, validFrom
	viewLock    sync.Mutex       // Ensures only one view query is made at a time
}

func newChannelCache(context *DatabaseContext, channelName string, validFrom uint64) *channelCache {
	return &channelCache{context: context, channelName: channelName, validFrom: validFrom}
}

// Low-level method to add a LogEntry to a single channel's cache.
func (c *channelCache) addToCache(change *LogEntry, isRemoval bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !isRemoval {
		c._appendChange(change)
	} else {
		removalChange := *change
		removalChange.Flags |= channels.Removed
		c._appendChange(&removalChange)
	}
	c._pruneCache()
	base.LogTo("Cache", "    #%d ==> channel %q", change.Sequence, c.channelName)
}

// Internal helper that prunes a single channel's cache. Caller MUST be holding the lock.
func (c *channelCache) _pruneCache() {
	pruned := 0
	for len(c.logs) > ChannelCacheMinLength && time.Since(c.logs[0].TimeReceived) > ChannelCacheAge {
		c.validFrom = c.logs[0].Sequence + 1
		c.logs = c.logs[1:]
		pruned++
	}
	if pruned > 0 {
		base.LogTo("Cache", "Pruned %d old entries from channel %q", pruned, c.channelName)
	}
}

func (c *channelCache) pruneCache() {
	c.lock.Lock()
	c._pruneCache()
	c.lock.Unlock()
}

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *channelCache) getCachedChanges(options ChangesOptions) (validFrom uint64, result []*LogEntry) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c._getCachedChanges(options)
}

func (c *channelCache) _getCachedChanges(options ChangesOptions) (validFrom uint64, result []*LogEntry) {
	// Find the first entry in the log to return:
	log := c.logs
	if len(log) == 0 {
		validFrom = c.validFrom
		return // Return nil if nothing is cached
	}
	var start int
	for start = len(log) - 1; start >= 0 && log[start].Sequence > options.Since; start-- {
	}
	start++

	if start > 0 {
		validFrom = log[start-1].Sequence + 1
	} else {
		validFrom = c.validFrom
	}

	n := len(log) - start
	if options.Limit > 0 && n < options.Limit {
		n = options.Limit
	}
	result = make([]*LogEntry, n)
	copy(result[0:], log[start:])
	return
}

// Top-level method to get all the changes in a channel since the sequence 'since'.
// If the cache doesn't go back far enough, the view will be queried.
// View query results may be fed back into the cache if there's room.
// initialSequence is used only if the cache is empty: it gives the max sequence to which the
// view should be queried, because we don't want the view query to outrun the chanceCache's
// nextSequence.
func (c *channelCache) GetChanges(options ChangesOptions) ([]*LogEntry, error) {
	// Use the cache, and return if it fulfilled the entire request:
	cacheValidFrom, resultFromCache := c.getCachedChanges(options)
	numFromCache := len(resultFromCache)
	if numFromCache > 0 || resultFromCache == nil {
		base.LogTo("Cache", "getCachedChanges(%q, %d) --> %d changes valid from #%d",
			c.channelName, options.Since, numFromCache, cacheValidFrom)
	} else if resultFromCache == nil {
		base.LogTo("Cache", "getCachedChanges(%q, %d) --> nothing cached",
			c.channelName, options.Since)
	}
	if cacheValidFrom <= options.Since+1 {
		return resultFromCache, nil
	}

	// Nope, we're going to have to backfill from the view.
	//** First acquire the _view_ lock (not the regular lock!)
	c.viewLock.Lock()
	defer c.viewLock.Unlock()

	// Another goroutine might have gotten the lock first and already queried the view and updated
	// the cache, so repeat the above:
	cacheValidFrom, resultFromCache = c._getCachedChanges(options)
	if len(resultFromCache) > numFromCache {
		base.LogTo("Cache", "2nd getCachedChanges(%q, %d) got %d more, valid from #%d!",
			c.channelName, options.Since, len(resultFromCache)-numFromCache, cacheValidFrom)
	}
	if cacheValidFrom <= options.Since+1 {
		return resultFromCache, nil
	}

	// Now query the view:
	resultFromView, err := c.context.getChangesInChannelFromView(c.channelName, cacheValidFrom-1,
		options)
	if err != nil {
		return nil, err
	}

	// Cache some of the view results, if there's room in the cache:
	roomInCache := ChannelCacheMaxLength - len(resultFromCache)
	if roomInCache > 0 {
		// ** Acquire the regular lock since we're about to change c.logs:
		c.lock.Lock()
		defer c.lock.Unlock()

		numFromView := len(resultFromView)
		if numFromView > 0 {
			if numFromView < roomInCache {
				roomInCache = numFromView
			}
			toCache := resultFromView[numFromView-roomInCache:]
			numPrepended := c._prependChanges(toCache)
			if numPrepended > 0 {
				base.LogTo("Cache", "  Added %d entries from view to cache of %q", numPrepended, c.channelName)
			}
		}
	}
	if options.Since < c.validFrom {
		c.validFrom = options.Since
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
	base.LogTo("Cache", "GetChangesInChannel(%q) --> %d rows", c.channelName, len(result))
	return result, nil
}

//////// LOGENTRIES:

func (c *channelCache) _adjustFirstSeq(change *LogEntry) {
	if change.Sequence < c.validFrom {
		c.validFrom = change.Sequence
	}
}

// Adds an entry to the end of an array of LogEntries.
// Any existing entry with the same DocID is removed.
func (c *channelCache) _appendChange(change *LogEntry) {
	log := c.logs
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
	} else {
		c._adjustFirstSeq(change)
	}
	c.logs = append(log, change)
}

// Prepends an array of entries to this one, skipping ones that I already have.
// Returns the number of entries actually prepended.
func (c *channelCache) _prependChanges(changes LogEntries) int {
	if len(changes) == 0 {
		return 0
	}
	log := c.logs
	if len(log) == 0 {
		c.logs = make(LogEntries, len(changes))
		copy(c.logs, changes)
		c._adjustFirstSeq(changes[0])
		return len(changes)
	}
	firstSequence := log[0].Sequence
	for i := len(changes) - 1; i >= 0; i-- {
		if changes[i].Sequence < firstSequence {
			newLog := make(LogEntries, 0, i+len(log))
			newLog = append(newLog, changes[0:i+1]...)
			newLog = append(newLog, log...)
			c.logs = newLog
			c._adjustFirstSeq(newLog[0])
			return i + 1
		}
	}
	return 0
}

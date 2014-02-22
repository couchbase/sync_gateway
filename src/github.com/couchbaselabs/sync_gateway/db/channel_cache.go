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

type channelCache struct {
	channelName string
	context     *DatabaseContext
	logs        LogEntries
	lock        sync.RWMutex
	viewLock    sync.Mutex
}

// Low-level method to add a LogEntry to a single channel's cache.
func (c *channelCache) addToCache(change *LogEntry, isRemoval bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !isRemoval {
		c.logs.appendChange(change)
	} else {
		removalChange := *change
		removalChange.Flags |= channels.Removed
		c.logs.appendChange(&removalChange)
	}
	c._pruneCache()
	base.LogTo("Cache", "    #%d ==> channel %q", change.Sequence, c.channelName)
}

// Internal helper that prunes a single channel's cache. Caller MUST be holding the lock.
func (c *channelCache) _pruneCache() {
	pruned := 0
	for len(c.logs) > ChannelCacheMinLength && time.Since(c.logs[0].TimeReceived) > ChannelCacheAge {
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

func (c *channelCache) _firstSequence() uint64 {
	if len(c.logs) == 0 {
		return 0
	}
	return c.logs[0].Sequence
}

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *channelCache) getCachedChanges(options ChangesOptions) (validSince uint64, result []*LogEntry) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c._getCachedChanges(options)
}

func (c *channelCache) _getCachedChanges(options ChangesOptions) (validSince uint64, result []*LogEntry) {
	// Find the first entry in the log to return:
	log := c.logs
	if len(log) == 0 {
		return // Return nil if nothing is cached
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
	return
}

// Top-level method to get all the changes in a channel since the sequence 'since'.
// If the cache doesn't go back far enough, the view will be queried.
// View query results may be fed back into the cache if there's room.
func (c *channelCache) GetChanges(options ChangesOptions, maxSequence uint64) ([]*LogEntry, error) {
	// Use the cache, and return if it fulfilled the entire request:
	cacheValidSince, resultFromCache := c.getCachedChanges(options)
	numFromCache := len(resultFromCache)
	if numFromCache > 0 || resultFromCache == nil {
		base.LogTo("Cache", "getCachedChanges(%q, %d) --> %d changes valid after #%d",
			c.channelName, options.Since, numFromCache, cacheValidSince)
	} else if resultFromCache == nil {
		base.LogTo("Cache", "getCachedChanges(%q, %d) --> nothing cached",
			c.channelName, options.Since)
	}
	if resultFromCache != nil && cacheValidSince <= options.Since {
		return resultFromCache, nil
	}

	// Nope, we're going to have to backfill from the view.
	//** First acquire the _view_ lock (not the regular lock!)
	c.viewLock.Lock()
	defer c.viewLock.Unlock()

	// Another goroutine might have gotten the lock first and already queried the view and updated
	// the cache, so repeat the above:
	cacheValidSince, resultFromCache = c._getCachedChanges(options)
	if len(resultFromCache) > numFromCache {
		base.LogTo("Cache", "2nd getCachedChanges(%q, %d) got %d more, valid since #%d!",
			c.channelName, options.Since, len(resultFromCache)-numFromCache, cacheValidSince)
	}
	if resultFromCache != nil && cacheValidSince <= options.Since {
		return resultFromCache, nil
	}

	// Now query the view:
	endSeq := cacheValidSince
	if endSeq == 0 || endSeq > maxSequence {
		endSeq = maxSequence
	}
	resultFromView, err := c.context.getChangesInChannelFromView(c.channelName, endSeq, options)
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
			numPrepended := c.logs.prependChanges(toCache)
			if numPrepended > 0 {
				base.LogTo("Cache", "  Added %d entries from view to cache of %q", numPrepended, c.channelName)
			}
		}
		if options.Since == 0 && len(c.logs) < ChannelCacheMaxLength {
			// If the view query goes back to the dawn of time, record a fake zero sequence in
			// the cache so any future requests won't need to hit the view again:
			fake := LogEntry{
				Sequence:     0,
				TimeReceived: time.Now(),
			}
			c.logs.prependChanges([]*LogEntry{&fake})
		}
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

package db

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/tleyden/isync"
)

var ChannelCacheMinLength = 50         // Keep at least this many entries in cache
var ChannelCacheMaxLength = 500        // Don't put more than this many entries in cache
var ChannelCacheAge = 60 * time.Second // Keep entries at least this long

const NoSeq = uint64(0x7FFFFFFFFFFFFFFF)

type channelCache struct {
	channelName      string                   // The channel name, duh
	context          *DatabaseContext         // Database connection (used for view queries)
	logs             LogEntries               // Log entries in sequence order
	validFrom        uint64                   // First sequence that logs is valid for
	lock             isync.InstrumentedLocker // Controls access to logs, validFrom
	viewLock         sync.Mutex               // Ensures only one view query is made at a time
	lateLogs         []*lateLogEntry          // Late arriving LogEntries, stored in the order they were received
	lastLateSequence uint64                   // Used for fast check of whether listener has the latest
	lateLogLock      isync.InstrumentedLocker // Controls access to lateLogs
}

func newChannelCache(context *DatabaseContext, channelName string, validFrom uint64) *channelCache {
	cache := &channelCache{context: context, channelName: channelName, validFrom: validFrom}
	cache.lock = isync.NewRWMutex()
	cache.lateLogLock = isync.NewRWMutex()
	cache.initializeLateLogs()
	return cache
}

// Low-level method to add a LogEntry to a single channel's cache.
func (c *channelCache) addToCache(change *LogEntry, isRemoval bool) {
	channelLockTime := time.Now()
	sessionId := c.lock.LockWithUserData("channel-cache-add-to-cache")
	defer c.lock.Unlock(sessionId)
	lag := time.Since(channelLockTime)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-channel-lock-addToCache-%05dms", lagMs), 1)

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
	sessionId := c.lock.LockWithUserData("channel-cache-prune-cache")
	c._pruneCache()
	c.lock.Unlock(sessionId)
}

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *channelCache) getCachedChanges(options ChangesOptions) (validFrom uint64, result []*LogEntry) {

	channelCacheLockTime := time.Now()
	sessionId := c.lock.RLockWithUserData("channel-cache-get-cached-changes")
	defer c.lock.RUnlock(sessionId)

	lag := time.Since(channelCacheLockTime)
	lagMs := int(lag/(100*time.Millisecond)) * 100
	changeCacheExpvars.Add(fmt.Sprintf("lag-channelcache-rlock-getCachedChanges-%05dms", lagMs), 1)
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
	for start = len(log) - 1; start >= 0 && log[start].Sequence > options.Since.SafeSequence(); start-- {
	}
	start++

	if start > 0 {
		validFrom = log[start-1].Sequence + 1
	} else {
		validFrom = c.validFrom
	}

	n := len(log) - start
	if options.Limit > 0 && n > options.Limit {
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
	startSeq := options.Since.SafeSequence() + 1
	if cacheValidFrom <= startSeq {
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
	if cacheValidFrom <= startSeq {
		return resultFromCache, nil
	}

	// Now query the view. We set the max sequence equal to cacheValidFrom, so we'll get one
	// overlap, which helps confirm that we've got everything.
	resultFromView, err := c.context.getChangesInChannelFromView(c.channelName, cacheValidFrom,
		options)
	if err != nil {
		return nil, err
	}

	// Cache some of the view results, if there's room in the cache:
	if len(resultFromCache) < ChannelCacheMaxLength {
		c.prependChanges(resultFromView, startSeq, options.Limit == 0)
	}

	result := resultFromView
	room := options.Limit - len(result)
	if (options.Limit == 0 || room > 0) && len(resultFromCache) > 0 {
		// Concatenate the view & cache results:
		if len(result) > 0 && resultFromCache[0].Sequence == result[len(result)-1].Sequence {
			resultFromCache = resultFromCache[1:]
		}
		n := len(resultFromCache)
		if options.Limit > 0 && room > 0 && room < n {
			n = room
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
			base.LogTo("Cache", "LogEntries.appendChange: out-of-order sequence #%d (last is #%d) - handling as insert",
				change.Sequence, log[end].Sequence)
			// insert the change in the array, ensuring the docID isn't already present
			insertChange(&c.logs, change)
			return
		}
		// If entry with DocID already exists, remove it.
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

// Insert out-of-sequence entry into the cache.  If the docId is already present in a later
// sequence, we skip the insert.  If the docId is already present in an earlier sequence,
// we remove the earlier sequence.
func insertChange(log *LogEntries, change *LogEntry) {

	end := len(*log) - 1
	insertAfterIndex := -1
	for i := end; i >= 0; i-- {
		currLog := (*log)[i]
		if insertAfterIndex == -1 && currLog.Sequence < change.Sequence {
			insertAfterIndex = i
		}
		if currLog.DocID == change.DocID {
			if currLog.Sequence >= change.Sequence {
				// we've already cached a later revision of this document, can ignore update
				return
			} else {
				// found existing prior to insert position
				if i == insertAfterIndex {
					// The sequence is adjacent to another with the same docId - replace it
					// instead of inserting
					(*log)[i] = change
					return
				} else {
					// Shift and insert to remove the old entry and add the new one
					copy((*log)[i:insertAfterIndex], (*log)[i+1:insertAfterIndex+1])
					(*log)[insertAfterIndex] = change
					return
				}
			}
		}
	}
	// We didn't find a match for DocID, so standard insert.
	*log = append(*log, nil)
	copy((*log)[insertAfterIndex+2:], (*log)[insertAfterIndex+1:])
	(*log)[insertAfterIndex+1] = change
	return
}

// Prepends an array of entries to this one, skipping ones that I already have.
// The new array needs to overlap with my current log, i.e. must contain the same sequence as
// c.logs[0], otherwise nothing will be added because the method can't confirm that there are no
// missing sequences in between.
// Returns the number of entries actually prepended.
func (c *channelCache) prependChanges(changes LogEntries, changesValidFrom uint64, openEnded bool) int {
	sessionId := c.lock.LockWithUserData("prependChanges")
	defer c.lock.Unlock(sessionId)

	log := c.logs
	if len(log) == 0 {
		// If my cache is empty, just copy the new changes:
		if len(changes) > 0 {
			if !openEnded && changes[len(changes)-1].Sequence < c.validFrom {
				return 0 // changes might not go all the way to the current time
			}
			if excess := len(changes) - ChannelCacheMaxLength; excess > 0 {
				changes = changes[excess:]
				changesValidFrom = changes[0].Sequence
			}
			c.logs = make(LogEntries, len(changes))
			copy(c.logs, changes)
			base.LogTo("Cache", "  Initialized cache of %q with %d entries from view (#%d--#%d)",
				c.channelName, len(changes), changes[0].Sequence, changes[len(changes)-1].Sequence)
		}
		c.validFrom = changesValidFrom
		return len(changes)

	} else if len(changes) == 0 {
		if openEnded && changesValidFrom < c.validFrom {
			c.validFrom = changesValidFrom
		}
		return 0

	} else {
		// Look for an overlap, and prepend everything up to that point:
		firstSequence := log[0].Sequence
		if changes[0].Sequence <= firstSequence {
			for i := len(changes) - 1; i >= 0; i-- {
				if changes[i].Sequence == firstSequence {
					if excess := i + len(log) - ChannelCacheMaxLength; excess > 0 {
						changes = changes[excess:]
						changesValidFrom = changes[0].Sequence
						i -= excess
					}
					if i > 0 {
						newLog := make(LogEntries, 0, i+len(log))
						newLog = append(newLog, changes[0:i]...)
						newLog = append(newLog, log...)
						c.logs = newLog
						base.LogTo("Cache", "  Added %d entries from view (#%d--#%d) to cache of %q",
							i, changes[0].Sequence, changes[i-1].Sequence, c.channelName)
					}
					c.validFrom = changesValidFrom
					return i
				}
			}
		}
		return 0
	}
}

type lateLogEntry struct {
	logEntry      *LogEntry
	arrived       time.Time
	listenerCount uint64
	listenerLock  sync.RWMutex
}

func (l *lateLogEntry) addListener() {
	l.listenerLock.Lock()
	l.listenerCount++
	l.listenerLock.Unlock()
}

func (l *lateLogEntry) removeListener() {
	l.listenerLock.Lock()
	l.listenerCount--
	l.listenerLock.Unlock()
}

func (l *lateLogEntry) getListenerCount() uint64 {
	l.listenerLock.RLock()
	defer l.listenerLock.RUnlock()
	return l.listenerCount
}

// Initialize the late-arriving log queue with a zero entry, used to track listeners.  This is needed
// to support purging later entries once everyone has seen them.
func (c *channelCache) initializeLateLogs() {
	log := &LogEntry{Sequence: 0}
	lateEntry := &lateLogEntry{
		logEntry:      log,
		listenerCount: 0,
	}
	c.lateLogs = append(c.lateLogs, lateEntry)
}

// Retrieve late-arriving sequences that have arrived since the previous sequence.  Retrieves set of sequences, and the last
// sequence number in the list.  Note that lateLogs is sorted by arrival on Tap, not sequence number.
func (c *channelCache) GetLateSequencesSince(sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {

	sessionId := c.lateLogLock.RLockWithUserData("channel-cache-get-late-sequences-since")
	defer c.lateLogLock.RUnlock(sessionId)

	// If we don't have any new late sequences, return empty
	if sinceSequence == c.lastLateSequence {
		return nil, sinceSequence, nil
	}

	// Return everything in lateLogs after sinceSequence (the caller's last seen sequence).
	var previousEntry *lateLogEntry
	previousFound := false

	for index, lateLog := range c.lateLogs {
		if previousFound {
			entries = append(entries, lateLog.logEntry)
		} else {
			if lateLog.logEntry.Sequence == sinceSequence {
				// Found the previous sequence.
				entries = make([]*LogEntry, 0, len(c.lateLogs)-index)
				previousEntry = lateLog
				previousFound = true
			}
		}
	}

	// If we didn't find the previous sequence, the consumer may be missing sequences and must rollback
	// to their last safe sequence
	if !previousFound {
		err = errors.New("Missing previous sequence - rollback required")
		return
	}

	// Remove listener from the sinceSequence entry, and add a listener to the latest
	previousEntry.removeListener()
	c.lateLogs[len(c.lateLogs)-1].addListener()
	lastSequence = c.lateLogs[len(c.lateLogs)-1].logEntry.Sequence

	return entries, lastSequence, nil

}

// Called on first call to the channel during changes processing, to get starting point for
// subsequent checks for late arriving sequences.
func (c *channelCache) InitLateSequenceClient() uint64 {
	log := c.lateLogs[len(c.lateLogs)-1]
	log.addListener()
	return log.logEntry.Sequence
}

func (c *channelCache) ReleaseLateSequenceClient(sequence uint64) error {
	for _, log := range c.lateLogs {
		if log.logEntry.Sequence == sequence {
			log.removeListener()
			return nil
		}
	}
	return errors.New("Sequence not found")
}

// Receive new late sequence
func (c *channelCache) AddLateSequence(change *LogEntry) {
	// Add to lateLogs.
	lateEntry := &lateLogEntry{
		logEntry:      change,
		arrived:       time.Now(),
		listenerCount: 0,
	}
	sessionId := c.lateLogLock.LockWithUserData("channel-cache-add-late-sequence")
	defer c.lateLogLock.Unlock(sessionId)
	c.lateLogs = append(c.lateLogs, lateEntry)
	c.lastLateSequence = change.Sequence
	// Currently we're only purging on add.  Could also consider a timed purge to handle the case
	// where all the listeners get caught up, but there aren't any subsequent late entries.  Not
	// a high priority, as the memory overhead for the late entries should be trivial, and probably
	// doesn't merit
	c._purgeLateLogEntries()
}

// Purge entries from the beginning of the list having no active listeners.  Any newly connecting clients
// will get these entries directly from the cache.  Always maintain
// at least one entry in the list, to track new listeners.  Expects to have a lock on lateLogLock.
func (c *channelCache) _purgeLateLogEntries() {
	for len(c.lateLogs) > 1 && c.lateLogs[0].getListenerCount() == 0 {
		c.lateLogs = c.lateLogs[1:]
	}
}

// Purge entries from the beginning of the list having no active listeners.  Any newly connecting clients
// will get these entries directly from the cache.  Always maintain
// at least one entry in the list, to track new listeners.
func (c *channelCache) purgeLateLogEntries() {
	sessionId := c.lateLogLock.LockWithUserData("channel-cache-purge-late-log-entries")
	defer c.lateLogLock.Unlock(sessionId)
	c._purgeLateLogEntries()

}

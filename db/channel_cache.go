package db

import (
	"errors"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

var (
	DefaultChannelCacheMinLength = 50               // Keep at least this many entries in cache
	DefaultChannelCacheMaxLength = 500              // Don't put more than this many entries in cache
	DefaultChannelCacheAge       = 60 * time.Second // Keep entries at least this long
)

const NoSeq = uint64(0x7FFFFFFFFFFFFFFF)

type channelCache struct {
	channelName      string               // The channel name, duh
	context          *DatabaseContext     // Database connection (used for view queries)
	logs             LogEntries           // Log entries in sequence order
	validFrom        uint64               // First sequence that logs is valid for
	lock             sync.RWMutex         // Controls access to logs, validFrom
	viewLock         sync.Mutex           // Ensures only one view query is made at a time
	lateLogs         []*lateLogEntry      // Late arriving LogEntries, stored in the order they were received
	lastLateSequence uint64               // Used for fast check of whether listener has the latest
	lateLogLock      sync.RWMutex         // Controls access to lateLogs
	options          *ChannelCacheOptions // Cache size/expiry settings
	cachedDocIDs     map[string]struct{}
}

func newChannelCache(context *DatabaseContext, channelName string, validFrom uint64) *channelCache {
	cache := &channelCache{context: context, channelName: channelName, validFrom: validFrom}
	cache.initializeLateLogs()
	cache.cachedDocIDs = make(map[string]struct{})
	cache.options = &ChannelCacheOptions{
		ChannelCacheMinLength: DefaultChannelCacheMinLength,
		ChannelCacheMaxLength: DefaultChannelCacheMaxLength,
		ChannelCacheAge:       DefaultChannelCacheAge,
	}
	cache.logs = make(LogEntries, 0, cache.options.ChannelCacheMaxLength)

	return cache
}

func newChannelCacheWithOptions(context *DatabaseContext, channelName string, validFrom uint64, options CacheOptions) *channelCache {
	cache := newChannelCache(context, channelName, validFrom)

	// Update cache options when present
	if options.ChannelCacheMinLength > 0 {
		cache.options.ChannelCacheMinLength = options.ChannelCacheMinLength
	}

	if options.ChannelCacheMaxLength > 0 {
		cache.options.ChannelCacheMaxLength = options.ChannelCacheMaxLength
	}

	if options.ChannelCacheAge > 0 {
		cache.options.ChannelCacheAge = options.ChannelCacheAge
	}

	base.LogTo("Cache", "Initialized cache for channel %q with options: %+v", cache.channelName, cache.options)

	return cache
}

type ChannelCacheOptions struct {
	ChannelCacheMinLength int           // Keep at least this many entries in cache
	ChannelCacheMaxLength int           // Don't put more than this many entries in cache
	ChannelCacheAge       time.Duration // Keep entries at least this long
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

	// If we are over max length, prune it down to max length
	if len(c.logs) > c.options.ChannelCacheMaxLength {
		pruned = len(c.logs) - c.options.ChannelCacheMaxLength
		for i := 0; i < pruned; i++ {
			delete(c.cachedDocIDs, c.logs[i].DocID)
		}
		c.validFrom = c.logs[pruned-1].Sequence + 1
		c.logs = c.logs[pruned:]
	}

	// Remove all entries who've been in the cache longer than channelCacheAge, except
	// those that fit within channelCacheMinLength and therefore not subject to cache age restrictions
	for len(c.logs) > c.options.ChannelCacheMinLength && time.Since(c.logs[0].TimeReceived) > c.options.ChannelCacheAge {
		c.validFrom = c.logs[0].Sequence + 1
		delete(c.cachedDocIDs, c.logs[0].DocID)
		c.logs = c.logs[1:]
		pruned++
	}
	if pruned > 0 {
		base.LogTo("Cache+", "Pruned %d old entries from channel %q", pruned, c.channelName)
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
		base.LogTo("Cache", "getCachedChanges(%q, %s) --> %d changes valid from #%d",
			c.channelName, options.Since.String(), numFromCache, cacheValidFrom)
	} else if resultFromCache == nil {
		base.LogTo("Cache", "getCachedChanges(%q, %s) --> nothing cached",
			c.channelName, options.Since.String())
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
	if len(resultFromCache) < c.options.ChannelCacheMaxLength {
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
			base.LogTo("Cache+", "LogEntries.appendChange: out-of-order sequence #%d (last is #%d) - handling as insert",
				change.Sequence, log[end].Sequence)
			// insert the change in the array, ensuring the docID isn't already present
			c.insertChange(&c.logs, change)
			return
		}
		// If entry with DocID already exists, remove it.
		if _, found := c.cachedDocIDs[change.DocID]; found {
			for i := end; i >= 0; i-- {
				if log[i].DocID == change.DocID {
					copy(log[i:], log[i+1:])
					log[end] = change
					return
				}
			}
		}

	} else {
		c._adjustFirstSeq(change)
	}
	c.logs = append(log, change)
	c.cachedDocIDs[change.DocID] = struct{}{}
}

// Insert out-of-sequence entry into the cache.  If the docId is already present in a later
// sequence, we skip the insert.  If the docId is already present in an earlier sequence,
// we remove the earlier sequence.
func (c *channelCache) insertChange(log *LogEntries, change *LogEntry) {

	defer func() {
		c.cachedDocIDs[change.DocID] = struct{}{}
	}()

	end := len(*log) - 1

	insertAtIndex := 0

	_, docIDExists := c.cachedDocIDs[change.DocID]

	// Walk log backwards until we find the point where we should insert this change.
	// (recall that logentries is sorted in ascending sequence order)
	for i := end; i >= 0; i-- {
		currLog := (*log)[i]
		if insertAtIndex == 0 && change.Sequence > currLog.Sequence {
			insertAtIndex = i + 1
		}
		if docIDExists {
			if currLog.DocID == change.DocID {
				if currLog.Sequence >= change.Sequence {
					// we've already cached a later revision of this document, can ignore update
					return
				} else {
					// found existing prior to insert position
					if i == insertAtIndex-1 {
						// The sequence is adjacent to another with the same docId - replace it
						// instead of inserting
						(*log)[i] = change
						return
					} else {
						// Shift and insert to remove the old entry and add the new one
						copy((*log)[i:insertAtIndex-1], (*log)[i+1:insertAtIndex])
						(*log)[insertAtIndex-1] = change
						return
					}
				}
			}
		}
	}

	// We didn't find a match for DocID, so standard insert.  Append an nil entry, shift existing, and insert.
	*log = append(*log, nil)
	copy((*log)[insertAtIndex+1:], (*log)[insertAtIndex:])
	(*log)[insertAtIndex] = change

	return
}

// Prepends an array of entries to this one, skipping ones that I already have.
// The new array needs to overlap with my current log, i.e. must contain the same sequence as
// c.logs[0], otherwise nothing will be added because the method can't confirm that there are no
// missing sequences in between.
// Returns the number of entries actually prepended.
func (c *channelCache) prependChanges(changes LogEntries, changesValidFrom uint64, openEnded bool) int {
	c.lock.Lock()
	defer c.lock.Unlock()

	log := c.logs
	if len(log) == 0 {
		// If my cache is empty, just copy the new changes:
		if len(changes) > 0 {
			if !openEnded && changes[len(changes)-1].Sequence < c.validFrom {
				return 0 // changes might not go all the way to the current time
			}
			if excess := len(changes) - c.options.ChannelCacheMaxLength; excess > 0 {
				changes = changes[excess:]
				changesValidFrom = changes[0].Sequence
			}
			c.logs = make(LogEntries, len(changes))
			copy(c.logs, changes)
			base.LogTo("Cache", "  Initialized cache of %q with %d entries from view (#%d--#%d)",
				c.channelName, len(changes), changes[0].Sequence, changes[len(changes)-1].Sequence)
		}
		c.validFrom = changesValidFrom
		c.addDocIDs(changes)
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
					if excess := i + len(log) - c.options.ChannelCacheMaxLength; excess > 0 {
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
	arrived       time.Time    // Time arrived in late log - for diagnostics tracking
	listenerCount uint64       // Tracks the number of active changes feeds referencing this entry.  Used to identify when it's safe to prune
	listenerLock  sync.RWMutex // lock for updating listenerCount
}

func (l *lateLogEntry) addListener() {
	l.listenerLock.Lock()
	defer l.listenerLock.Unlock()
	l.listenerCount++
}

func (l *lateLogEntry) removeListener() {
	l.listenerLock.Lock()
	defer l.listenerLock.Unlock()
	l.listenerCount--
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

	c.lateLogLock.RLock()
	defer c.lateLogLock.RUnlock()

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
		return nil, 0, err
	}

	// Remove listener from the sinceSequence entry, and add a listener to the latest
	previousEntry.removeListener()
	lastEntry := c.mostRecentLateLog()
	lastEntry.addListener()
	lastSequence = lastEntry.logEntry.Sequence

	return entries, lastSequence, nil

}

// Called on first call to the channel during changes processing, to get starting point for
// subsequent checks for late arriving sequences.
func (c *channelCache) InitLateSequenceClient() uint64 {
	latestLog := c.mostRecentLateLog()
	latestLog.addListener()
	return latestLog.logEntry.Sequence
}

// Called when a client (a continuous _changes feed) is no longer referencing the sequence number.
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
	c.lateLogLock.Lock()
	defer c.lateLogLock.Unlock()
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
	c.lateLogLock.Lock()
	defer c.lateLogLock.Unlock()
	c._purgeLateLogEntries()

}

func (c *channelCache) mostRecentLateLog() *lateLogEntry {
	if len(c.lateLogs) > 0 {
		return c.lateLogs[len(c.lateLogs)-1]
	}
	return nil
}

func (c *channelCache) addDocIDs(changes LogEntries) {
	for _, change := range changes {
		c.cachedDocIDs[change.DocID] = struct{}{}
	}
}

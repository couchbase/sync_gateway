package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

var (
	DefaultChannelCacheMinLength = 50               // Keep at least this many entries in cache
	DefaultChannelCacheMaxLength = 500              // Don't put more than this many entries in cache
	DefaultChannelCacheAge       = 60 * time.Second // Keep entries at least this long
	DefaultChannelCacheMaxNumber = 50000            // Default of 50k channel caches
)

const NoSeq = uint64(0x7FFFFFFFFFFFFFFF)

type ChannelCache interface {

	// Initializes the cache high sequence value
	Init(initialSequence uint64)

	// Adds an entry to the cache, returns set of channels it was added to
	AddToCache(change *LogEntry) base.Set

	// Notifies the cache of a principal update.  Updates the cache's high sequence
	AddPrincipal(change *LogEntry)

	// Remove purges the given doc IDs from all channel caches and returns the number of items removed.
	Remove(docIDs []string, startTime time.Time) (count int)

	// Returns set of changes for a given channel, within the bounds specified in options
	GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error)

	// Returns the set of all cached data for a given channel (intended for diagnostic usage)
	GetCachedChanges(channelName string) []*LogEntry

	// Clear reinitializes the cache to an empty state
	Clear()

	// Size of the the largest individual channel cache, invoked for stats reporting
	//// TODO: let the cache manage its own stats internally (maybe take an updateStats call)
	MaxCacheSize() int

	// Returns the highest cached sequence, used for changes synchronization
	GetHighCacheSequence() uint64

	// Late sequence handling
	GetLateSequencesSince(channelName string, sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error)
	RegisterLateSequenceClient(channelName string) uint64
	ReleaseLateSequenceClient(channelName string, currentSeq uint64) bool

	// Access to individual channel cache, intended for testing
	getSingleChannelCache(channelName string) *singleChannelCache
}

type StableSequenceCallbackFunc func() uint64

type channelCacheImpl struct {
	context           *DatabaseContext               // Passed to singleChannelCache for view queries  TODO: Could replace with view query callback?
	channelCaches     map[string]*singleChannelCache // A cache of changes for each channel
	cacheLock         sync.RWMutex                   // Mutex for access to channelCaches map
	terminator        chan bool                      // Signal terminator of background goroutines
	options           ChannelCacheOptions            // Channel cache options
	lateSeqLock       sync.RWMutex                   // Coordinates access to late sequence caches
	highCacheSequence uint64                         // The highest sequence that has been cached.  Used to initialize validFrom for new singleChannelCaches
}

func NewChannelCache(terminator chan bool, options ChannelCacheOptions, context *DatabaseContext) *channelCacheImpl {

	channelCache := &channelCacheImpl{
		context:       context,
		channelCaches: make(map[string]*singleChannelCache),
		terminator:    terminator,
		options:       options,
	}
	NewBackgroundTask("CleanAgedItems", context.Name, channelCache.cleanAgedItems, options.ChannelCacheAge, terminator)
	return channelCache
}

func (c *channelCacheImpl) Clear() {
	c.cacheLock.Lock()
	c.channelCaches = make(map[string]*singleChannelCache)
	c.cacheLock.Unlock()
}

func (c *channelCacheImpl) Init(initialSequence uint64) {
	c.cacheLock.Lock()
	c.highCacheSequence = initialSequence
	c.cacheLock.Unlock()
}

func (c *channelCacheImpl) GetHighCacheSequence() uint64 {
	c.cacheLock.RLock()
	highSeq := c.highCacheSequence
	c.cacheLock.RUnlock()
	return highSeq
}

// Returns high cache sequence.  Callers should hold read lock on cacheLock
func (c *channelCacheImpl) _getHighCacheSequence() uint64 {
	return c.highCacheSequence
}

// Updates the high cache sequence if the incoming sequence is higher than current
// Note: doesn't require compareAndSet on update, as cache expects AddToCache to only be called from
// a single goroutine
func (c *channelCacheImpl) updateHighCacheSequence(sequence uint64) {
	c.cacheLock.Lock()
	if sequence > c.highCacheSequence {
		c.highCacheSequence = sequence
		c.context.DbStats.StatsCache().Set(base.StatKeyHighSeqCached, base.ExpvarUInt64Val(sequence))
	}
	c.cacheLock.Unlock()
}

func (c *channelCacheImpl) getSingleChannelCache(channelName string) *singleChannelCache {
	return c.getChannelCache(channelName)
}

func (c *channelCacheImpl) AddPrincipal(change *LogEntry) {
	c.updateHighCacheSequence(change.Sequence)
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (c *channelCacheImpl) AddToCache(change *LogEntry) base.Set {

	// updatedChannels tracks the set of channels that should be notified of the change.  This includes
	// the change's active channels, as well as any channel removals for the active revision.
	updatedChannels := make(base.Set)

	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory

	// If it's a late sequence, we want to add to all channel late queues within a single write lock,
	// to avoid a changes feed seeing the same late sequence in different iteration loops (and sending
	// twice)
	if change.Skipped {
		c.lateSeqLock.Lock()
		base.Infof(base.KeyChanges, "Acquired late sequence lock in order to cache %d - doc %q / %q", change.Sequence, base.UD(change.DocID), change.RevID)
		defer c.lateSeqLock.Unlock()
	}

	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			channelCache, ok := c.getActiveChannelCache(channelName)
			if ok {
				channelCache.addToCache(change, removal != nil)
				if change.Skipped {
					channelCache.AddLateSequence(change)
				}
			}
			updatedChannels = updatedChannels.Add(channelName)
		}
	}

	if EnableStarChannelLog {
		channelCache, ok := c.getActiveChannelCache(channels.UserStarChannel)
		if ok {
			channelCache.addToCache(change, false)
			if change.Skipped {
				channelCache.AddLateSequence(change)
			}
		}
		updatedChannels = updatedChannels.Add(channels.UserStarChannel)
	}

	c.updateHighCacheSequence(change.Sequence)
	return updatedChannels
}

// Remove purges the given doc IDs from all channel caches and returns the number of items removed.
// count will be larger than the input slice if the same document is removed from multiple channel caches.
func (c *channelCacheImpl) Remove(docIDs []string, startTime time.Time) (count int) {
	// Exit early if there's no work to do
	if len(docIDs) == 0 {
		return 0
	}

	c.cacheLock.Lock()
	for _, channelCache := range c.channelCaches {
		count += channelCache.Remove(docIDs, startTime)
	}
	c.cacheLock.Unlock()
	return count
}

func (c *channelCacheImpl) GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error) {
	return c.getChannelCache(channelName).GetChanges(options)
}

func (c *channelCacheImpl) GetCachedChanges(channelName string) []*LogEntry {
	options := ChangesOptions{Since: SequenceID{Seq: 0}}
	_, changes := c.getChannelCache(channelName).getCachedChanges(options)
	return changes
}

func (c *channelCacheImpl) GetLateSequencesSince(channelName string, sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {
	return c.getChannelCache(channelName).GetLateSequencesSince(sinceSequence)
}

func (c *channelCacheImpl) RegisterLateSequenceClient(channelName string) uint64 {
	return c.getChannelCache(channelName).RegisterLateSequenceClient()
}

func (c *channelCacheImpl) ReleaseLateSequenceClient(channelName string, currentSequence uint64) bool {
	return c.getChannelCache(channelName).ReleaseLateSequenceClient(currentSequence)
}

// CleanAgedItems prunes the caches based on age of items. Error returned to fulfill BackgroundTaskFunc signature.
func (c *channelCacheImpl) cleanAgedItems(ctx context.Context) error {
	c.cacheLock.Lock()
	for _, channelCache := range c.channelCaches {
		channelCache.pruneCacheAge(ctx)
	}
	c.cacheLock.Unlock()
	return nil
}

func (c *channelCacheImpl) getChannelCache(channelName string) *singleChannelCache {

	c.cacheLock.RLock()
	singleChannelCache, ok := c.channelCaches[channelName]
	c.cacheLock.RUnlock()

	if ok {
		return singleChannelCache
	}

	// Cache doesn't exist, lock and create.  Note that locking here also prevents missed data in the following scenario:
	//     1. getChannelCache issued for channel A
	//     2. getChannelCache obtains stable sequence, seq=10
	//     3. addToCache (from another goroutine) receives sequence 11 in channel A, but detects that it's inactive (not in c.channelCaches)
	//     4. getChannelCache initializes cache with validFrom=10 and adds to c.channelCaches
	//  This scenario would result in sequence 11 missing from the cache.  Locking cacheLock here ensures that
	//  step 3 blocks until step 4 is complete (and so sees the channel as active)
	c.cacheLock.Lock()
	// Check if it was created while we waited for the lock
	if singleChannelCache, ok = c.channelCaches[channelName]; ok {
		c.cacheLock.Unlock()
		return singleChannelCache
	}

	// Everything after the current high sequence will be added to the cache via the feed
	validFrom := c._getHighCacheSequence() + 1

	singleChannelCache = newChannelCacheWithOptions(c.context, channelName, validFrom, c.options)
	c.channelCaches[channelName] = singleChannelCache

	c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheNumChannels, 1)
	c.cacheLock.Unlock()

	return singleChannelCache
}

func (c *channelCacheImpl) getActiveChannelCache(channelName string) (*singleChannelCache, bool) {
	c.cacheLock.RLock()
	cache, ok := c.channelCaches[channelName]
	c.cacheLock.RUnlock()
	return cache, ok
}

// TODO: let the cache manage its own stats internally (maybe take an updateStats call)
func (c *channelCacheImpl) MaxCacheSize() int {
	c.cacheLock.RLock()
	maxCacheSize := 0
	for _, channelCache := range c.channelCaches {
		channelSize := channelCache.GetSize()
		if channelSize > maxCacheSize {
			maxCacheSize = channelSize
		}
	}
	c.cacheLock.RUnlock()
	return maxCacheSize
}

// Minimizes need to perform GSI/View queries to respond to the changes feed by keeping a per-channel
// cache of the most recent changes in the channel.  Changes might be received over DCP won't necessarily
// be in sequence order, but the changes are buffered and re-ordered before inserting into the cache, and
// so the cache is always kept in ascending order by sequence.  (this is the global sync sequence, not the
// per-vbucket sequence).
//
// Uniqueness guarantee: a given document should only have _one_ entry in the cache, which represents the
// most recent revision (the revision with the highest sequence number).
//
// Completeness guarantee: the cache is guaranteed to have _every_ change in a channel from the validFrom sequence
// up to the current known latest change in that channel.  Eg, there are no gaps/holes.
//
// The validFrom state variable tracks the oldest sequence for which the cache is complete.  This may be earlier
// than the oldest entry in the cache.  Used to determine whether a GSI/View query is required for a given
// getChanges request.
//
// Shortly after startup and receiving a few changes from DCP, the cache might look something like this:
//
// ┌───────────────────────────────────────────────────────────────────────────────────────┐
// │                       Changes for Channel X (CBServer + Cache)                        │
// │   ┌─────────────────────────────────────────────────┐                                 │
// │   │            Cache Subset (MaxSize=3)             │                                 │
// │   │                                                 │                                 │
// │   │          ┌─────────┐  ┌─────────┐               │                                 │
// │   │          │ Seq: 8  │  │ Seq: 25 │               │                                 │
// │   │    ▲     │ Doc: A  │  │ Doc: B  │               │                                 │
// │   │    │     │ Rev: 1  │  │ Rev: 4  │               │                                 │
// │   │    │     └─────────┘  └─────────┘               │                                 │
// │   │                                                 │                                 │
// │   │       ValidFrom = Seq 5                         │                                 │
// │   │     (_sync:Seq at Startup)                      │                                 │
// │   │                                                 │                                 │
// │   └─────────────────────────────────────────────────┘                                 │
// │                                                                                       │
// └───────────────────────────────────────────────────────────────────────────────────────┘
//
// All changes for the channel currently fit in the cache.  The validFrom is set to the value of the _sync:Seq that
// was read on startup.
//
// Later after more data has been added that will fit in the cache, it will look more like this:
//
// ┌───────────────────────────────────────────────────────────────────────────────────────┐
// │                       Changes for Channel X (CBServer + Cache)                        │
// │                                     ┌─────────────────────────────────────────────┐   │
// │                                     │          Cache Subset (MaxSize=3)           │   │
// │                                     │                                             │   │
// │ ┌─────────┐ ┌─────────┐ ┌─────────┐ │         ┌─────────┐┌─────────┐ ┌─────────┐  │   │
// │ │ Seq: 8  │ │ Seq: 25 │ │Seq: ... │ │         │Seq: 6002││Seq: 7022│ │Seq: 7027│  │   │
// │ │ Doc: A  │ │ Doc: B  │ │Doc: ... │ │   ▲     │ Doc: A  ││ Doc: H  │ │ Doc: M  │  │   │
// │ │ Rev: 1  │ │ Rev: 4  │ │Rev: ... │ │   │     │Rev: 345 ││ Rev: 4  │ │ Rev: 47 │  │   │
// │ └─────────┘ └─────────┘ └─────────┘ │   │     └─────────┘└─────────┘ └─────────┘  │   │
// │                                     │                                             │   │
// │                                     │  ValidFrom = Seq 5989 (cache known          │   │
// │                                     │  to be complete + valid from seq)           │   │
// │                                     │                                             │   │
// │                                     └─────────────────────────────────────────────┘   │
// │                                                                                       │
// └───────────────────────────────────────────────────────────────────────────────────────┘
//
// If a calling function wanted to get all of the changes for the channel since Seq 3000, they would
// be forced to issue a GSI/View backfill query for changes is in the channel between 3000 and 5989,
// since the cache is only validFrom the sequence 5989.  In this case, none of those backfilled values
// will fit in the cache, which is already full, however if it did have capacity available, then those
// values would be _prepended_ to the front of the cache and the validFrom sequence would be lowered
// to account for the fact that it's now valid from a lower sequence value.  Entries that violated
// the guarantees described above would possibly be discarded.
type singleChannelCache struct {
	channelName      string               // The channel name, duh
	context          *DatabaseContext     // Database connection (used for view queries)
	logs             LogEntries           // Log entries in sequence order
	validFrom        uint64               // First sequence that logs is valid for, not necessarily the seq number of a change entry.
	lock             sync.RWMutex         // Controls access to logs, validFrom
	viewLock         sync.Mutex           // Ensures only one view query is made at a time
	lateLogs         []*lateLogEntry      // Late arriving LogEntries, stored in the order they were received
	lastLateSequence uint64               // Used for fast check of whether listener has the latest
	lateLogLock      sync.RWMutex         // Controls access to lateLogs
	options          *ChannelCacheOptions // Cache size/expiry settings
	cachedDocIDs     map[string]struct{}
}

func newChannelCache(context *DatabaseContext, channelName string, validFrom uint64) *singleChannelCache {
	cache := &singleChannelCache{context: context, channelName: channelName, validFrom: validFrom}
	cache.initializeLateLogs()
	cache.cachedDocIDs = make(map[string]struct{})
	cache.options = &ChannelCacheOptions{
		ChannelCacheMinLength: DefaultChannelCacheMinLength,
		ChannelCacheMaxLength: DefaultChannelCacheMaxLength,
		ChannelCacheAge:       DefaultChannelCacheAge,
		ChannelCacheMaxNumber: DefaultChannelCacheMaxNumber,
	}
	cache.logs = make(LogEntries, 0)

	return cache
}

func newChannelCacheWithOptions(context *DatabaseContext, channelName string, validFrom uint64, options ChannelCacheOptions) *singleChannelCache {
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

	if options.ChannelCacheMaxNumber > 0 {
		cache.options.ChannelCacheMaxNumber = options.ChannelCacheMaxNumber
	}

	base.Infof(base.KeyCache, "Initialized cache for channel %q with options: %+v, validFrom: %d", base.UD(cache.channelName), cache.options, validFrom)

	return cache
}

type ChannelCacheOptions struct {
	ChannelCacheMinLength int           // Keep at least this many entries in cache
	ChannelCacheMaxLength int           // Don't put more than this many entries in cache
	ChannelCacheAge       time.Duration // Keep entries at least this long
	ChannelCacheMaxNumber int           // Maximum number of channel caches which will exist at any one point
}

// Low-level method to add a LogEntry to a single channel's cache.
func (c *singleChannelCache) addToCache(change *LogEntry, isRemoval bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.wouldBeImmediatelyPruned(change) {
		base.Infof(base.KeyCache, "Not adding change #%d doc %q / %q ==> channel %q, since it will be immediately pruned",
			change.Sequence, base.UD(change.DocID), change.RevID, base.UD(c.channelName))
		return
	}

	if !isRemoval {
		c._appendChange(change)
	} else {
		removalChange := *change
		removalChange.Flags |= channels.Removed
		c._appendChange(&removalChange)
	}
	c._pruneCacheLength()
}

// If certain conditions are met, it's possible that this change will be added and then
// immediately pruned, which causes the issues described in https://github.com/couchbase/sync_gateway/issues/2662
func (c *singleChannelCache) wouldBeImmediatelyPruned(change *LogEntry) bool {

	// This might not be the one that is going to be immediately pruned
	if change.Sequence >= c.validFrom {
		return false
	}

	// If older than validFrom, never try to cache it
	return true

}

// Remove purges the given doc IDs from the channel cache and returns the number of items removed.
func (c *singleChannelCache) Remove(docIDs []string, startTime time.Time) (count int) {
	// Exit early if there's no work to do
	if len(docIDs) == 0 {
		return 0
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Build subset of docIDs that we know are present in the cache
	foundDocs := make(map[string]struct{}, 0)
	for _, docID := range docIDs {
		if _, found := c.cachedDocIDs[docID]; found {
			foundDocs[docID] = struct{}{}
		}
	}

	// Do the removals in one sweep of the channel cache
	end := len(c.logs) - 1
	for i := end; i >= 0; i-- {
		if _, ok := foundDocs[c.logs[i].DocID]; ok {
			docID := c.logs[i].DocID

			// Make sure the document we're about to remove is older than the start time of the purge
			// This is to ensure that resurrected documents do not accidentally get removed.
			if c.logs[i].TimeReceived.After(startTime) {
				base.Debugf(base.KeyCache, "Skipping removal of doc %q from cache %q - received after purge",
					base.UD(docID), base.UD(c.channelName))
				continue
			}

			// Decrement utilization stats for removed entry
			c.UpdateCacheUtilization(c.logs[i], -1)

			// Memory-leak safe delete from SliceTricks:
			copy(c.logs[i:], c.logs[i+1:])
			c.logs[len(c.logs)-1] = nil
			c.logs = c.logs[:len(c.logs)-1]
			delete(c.cachedDocIDs, docID)
			count++

			base.Tracef(base.KeyCache, "Removed doc %q from cache %q", base.UD(docID), base.UD(c.channelName))
		}
	}

	return count
}

// Internal helper that prunes a single channel's cache. Caller MUST be holding the lock.
func (c *singleChannelCache) _pruneCacheLength() (pruned int) {
	// If we are over max length, prune it down to max length
	if len(c.logs) > c.options.ChannelCacheMaxLength {
		pruned = len(c.logs) - c.options.ChannelCacheMaxLength
		for i := 0; i < pruned; i++ {
			c.UpdateCacheUtilization(c.logs[i], -1)
			delete(c.cachedDocIDs, c.logs[i].DocID)
		}
		c.validFrom = c.logs[pruned-1].Sequence + 1
		c.logs = c.logs[pruned:]
	}

	base.Debugf(base.KeyCache, "Pruned %d entries from channel %q", pruned, base.UD(c.channelName))

	return pruned
}

func (c *singleChannelCache) pruneCacheAge(ctx context.Context) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// time-based cache pruning doesn't make sense when MinLength >= MaxLength
	if c.options.ChannelCacheMinLength >= c.options.ChannelCacheMaxLength {
		return
	}

	pruned := 0
	// Remove all entries who've been in the cache longer than channelCacheAge, except
	// those that fit within channelCacheMinLength and therefore not subject to cache age restrictions
	for len(c.logs) > c.options.ChannelCacheMinLength && time.Since(c.logs[0].TimeReceived) > c.options.ChannelCacheAge {
		c.validFrom = c.logs[0].Sequence + 1
		c.UpdateCacheUtilization(c.logs[0], -1)
		delete(c.cachedDocIDs, c.logs[0].DocID)
		c.logs = c.logs[1:]
		pruned++
	}
	base.DebugfCtx(ctx, base.KeyCache, "Pruned %d old entries from channel %q", pruned, base.UD(c.channelName))

}

// Returns all of the cached entries for sequences greater than 'since' in the given channel.
// Entries are returned in increasing-sequence order.
func (c *singleChannelCache) getCachedChanges(options ChangesOptions) (validFrom uint64, result []*LogEntry) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	sinceSeq := options.Since.SafeSequence()
	limit := options.Limit

	//If the activeOnly option is set, then do not limit the number of entries returned
	//we don't know how many non active entries will be discarded from the entry set
	//by the caller, so the additional entries may be needed to return up to the limit requested
	if options.ActiveOnly {
		limit = 0
	}

	return c._getCachedChanges(sinceSeq, limit)
}

func (c *singleChannelCache) _getCachedChanges(sinceSeq uint64, limit int) (validFrom uint64, result []*LogEntry) {
	// Find the first entry in the log to return:
	log := c.logs
	if len(log) == 0 {
		validFrom = c.validFrom
		return // Return nil if nothing is cached
	}
	var start int
	for start = len(log) - 1; start >= 0 && log[start].Sequence > sinceSeq; start-- {
	}
	start++

	if start > 0 {
		validFrom = log[start-1].Sequence + 1
	} else {
		validFrom = c.validFrom
	}

	n := len(log) - start

	if limit > 0 && n > limit {
		n = limit
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
func (c *singleChannelCache) GetChanges(options ChangesOptions) ([]*LogEntry, error) {
	// Use the cache, and return if it fulfilled the entire request:
	cacheValidFrom, resultFromCache := c.getCachedChanges(options)
	numFromCache := len(resultFromCache)
	if numFromCache > 0 || resultFromCache == nil {
		base.InfofCtx(options.Ctx, base.KeyCache, "getCachedChanges(%q, %s) --> %d changes valid from #%d",
			base.UD(c.channelName), options.Since.String(), numFromCache, cacheValidFrom)
	} else if resultFromCache == nil {
		base.InfofCtx(options.Ctx, base.KeyCache, "getCachedChanges(%q, %s) --> nothing cached",
			base.UD(c.channelName), options.Since.String())
	}
	startSeq := options.Since.SafeSequence() + 1
	if cacheValidFrom <= startSeq {
		c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheHits, 1)
		return resultFromCache, nil
	}

	// Nope, we're going to have to backfill from the view.
	//** First acquire the _view_ lock (not the regular lock!)
	// Track pending queries via StatKeyChannelCachePendingQueries expvar
	c.context.DbStats.StatsCache().Add(base.StatKeyChannelCachePendingQueries, 1)
	c.viewLock.Lock()
	defer c.viewLock.Unlock()
	c.context.DbStats.StatsCache().Add(base.StatKeyChannelCachePendingQueries, -1)

	// Another goroutine might have gotten the lock first and already queried the view and updated
	// the cache, so repeat the above:
	cacheValidFrom, resultFromCache = c.getCachedChanges(options)
	if len(resultFromCache) > numFromCache {
		base.InfofCtx(options.Ctx, base.KeyCache, "2nd getCachedChanges(%q, %s) got %d more, valid from #%d!",
			base.UD(c.channelName), options.Since.String(), len(resultFromCache)-numFromCache, cacheValidFrom)
	}
	if cacheValidFrom <= startSeq {
		c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheHits, 1)
		return resultFromCache, nil
	}

	// Check whether the changes process has been terminated while we waited for the view lock, to avoid the view
	// overhead in that case (and prevent feedback loop on query backlog)
	select {
	case <-options.Terminator:
		return nil, fmt.Errorf("Changes feed terminated while waiting for view lock")
	default:
		// continue
	}

	// Now query the view. We set the max sequence equal to cacheValidFrom, so we'll get one
	// overlap, which helps confirm that we've got everything.
	c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheMisses, 1)
	endSeq := cacheValidFrom
	resultFromView, err := c.context.getChangesInChannelFromQuery(c.channelName, startSeq, endSeq, options.Limit, options.ActiveOnly)
	if err != nil {
		return nil, err
	}

	// Cache some of the query results, if there's room in the cache.  If query hit the limit,
	// the query results are only valid for the range of sequences in the result set.
	resultValidTo := endSeq
	numResults := len(resultFromView)
	if options.Limit != 0 && numResults >= options.Limit {
		resultValidTo = resultFromView[numResults-1].Sequence
	}
	if len(resultFromCache) < c.options.ChannelCacheMaxLength {
		c.prependChanges(resultFromView, startSeq, resultValidTo)
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
	base.InfofCtx(options.Ctx, base.KeyCache, "GetChangesInChannel(%q) --> %d rows", base.UD(c.channelName), len(result))

	return result, nil
}

//////// LOGENTRIES:

func (c *singleChannelCache) _adjustFirstSeq(change *LogEntry) {
	if change.Sequence < c.validFrom {
		c.validFrom = change.Sequence
	}
}

// Adds an entry to the end of an array of LogEntries.
// Any existing entry with the same DocID is removed.
func (c *singleChannelCache) _appendChange(change *LogEntry) {

	log := c.logs
	end := len(log) - 1
	if end >= 0 {
		if change.Sequence <= log[end].Sequence {
			base.Debugf(base.KeyCache, "LogEntries.appendChange: out-of-order sequence #%d (last is #%d) - handling as insert",
				change.Sequence, log[end].Sequence)
			// insert the change in the array, ensuring the docID isn't already present
			c.insertChange(&c.logs, change)
			return
		}
		// If entry with DocID already exists, remove it.
		if _, found := c.cachedDocIDs[change.DocID]; found {
			for i := end; i >= 0; i-- {
				if log[i].DocID == change.DocID {
					c.UpdateCacheUtilization(log[i], -1)
					copy(log[i:], log[i+1:])
					c.UpdateCacheUtilization(change, 1)
					log[end] = change
					return
				}
			}
		}

	} else {
		c._adjustFirstSeq(change)
	}
	c.logs = append(log, change)

	c.UpdateCacheUtilization(change, 1)
	c.cachedDocIDs[change.DocID] = struct{}{}
}

// Updates cache utilization.  Note that cache entries that are both removals and tombstones are counted as removals
func (c *singleChannelCache) UpdateCacheUtilization(entry *LogEntry, delta int64) {
	if entry.IsRemoved() {
		c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheRevsRemoval, delta)
	} else if entry.IsDeleted() {
		c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheRevsTombstone, delta)
	} else {
		c.context.DbStats.StatsCache().Add(base.StatKeyChannelCacheRevsActive, delta)
	}
}

// Insert out-of-sequence entry into the cache.  If the docId is already present in a later
// sequence, we skip the insert.  If the docId is already present in an earlier sequence,
// we remove the earlier sequence.
func (c *singleChannelCache) insertChange(log *LogEntries, change *LogEntry) {

	defer func() {
		c.cachedDocIDs[change.DocID] = struct{}{}
		c.UpdateCacheUtilization(change, 1)
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
					// found existing prior to insert position.  Decrement utilization for replaced version
					c.UpdateCacheUtilization((*log)[i], -1)
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
// the oldest entry in the cache (c.logs[0]), otherwise nothing will be added because the
// method can't confirm that there are no missing sequences in between.
//
// changesValidFrom represents the validfrom value of the query, and should always be LTE the
// oldest value (lowest sequence number) in the set of changes being prepended, although this is not strictly enforced.
//
// If all of the changes don't fit in the cache, then only the most recent (highest sequence number)
// changes will be added, and the oldest changes (lowest sequence numbers) will be discarded.  In that case, the validFrom
// state variable will be updated to reflect the oldest (lowest sequence number) of the change that actually made it into
// the cache.
//
// Returns the number of entries actually prepended.
func (c *singleChannelCache) prependChanges(changes LogEntries, changesValidFrom uint64, changesValidTo uint64) int {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If set of changes to prepend is empty, check whether validFrom should be updated
	if len(changes) == 0 {
		if changesValidFrom < c.validFrom && changesValidTo >= c.validFrom {
			base.Debugf(base.KeyCache, " changesValidFrom (%d) < c.validFrom < changesValidTo (%d), setting c.validFrom from %v -> %v",
				changesValidFrom, changesValidTo, c.validFrom, changesValidFrom)
			c.validFrom = changesValidFrom
		}
		return 0
	}

	// Ensure changes are valid to the cache's validFrom, otherwise unsafe to prepend
	if changesValidTo < c.validFrom {
		return 0
	}

	// If my cache is empty, just copy the new changes
	if len(c.logs) == 0 {
		if excess := len(changes) - c.options.ChannelCacheMaxLength; excess > 0 {
			changes = changes[excess:]
			changesValidFrom = changes[0].Sequence
		}
		c.logs = make(LogEntries, len(changes))
		copy(c.logs, changes)
		base.Infof(base.KeyCache, "  Initialized cache of %q with %d entries from query (#%d--#%d)",
			base.UD(c.channelName), len(changes), changes[0].Sequence, changes[len(changes)-1].Sequence)

		c.validFrom = changesValidFrom
		c.addDocIDs(changes)
		return len(changes)

	}

	// Prepending changes to a non-empty cache
	// Check whether there's capacity to prepend
	cacheCapacity := c.options.ChannelCacheMaxLength - len(c.logs)
	if cacheCapacity <= 0 {
		return 0
	}

	// Check whether the results to prepend are contiguous with the cache
	if changesValidFrom >= c.validFrom {
		return 0
	}

	// Iterate backward over changes set, building set to prepend.
	//   - Don't prepend any sequence values already in the cache (later than c.validFrom)
	//   - Ignore docIDs already in the cache
	//   - Stop when we have enough to fill to ChannelCacheMaxLength (or run out of query results)
	entriesToPrepend := make(LogEntries, 0, cacheCapacity)
	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]
		if change != nil && change.Sequence < c.validFrom {
			// If docid is already in cache, existing revision must be for a later sequence; can ignore this revision.
			if _, docIdExists := c.cachedDocIDs[change.DocID]; docIdExists {
				continue
			}
			entriesToPrepend = append(entriesToPrepend, nil)
			copy(entriesToPrepend[1:], entriesToPrepend)
			entriesToPrepend[0] = change
			c.cachedDocIDs[change.DocID] = struct{}{}
			c.UpdateCacheUtilization(change, 1)

			if len(entriesToPrepend) >= cacheCapacity {
				// If we reach capacity before prepending the entire set of changes, set changesValidFrom to the oldest sequence
				// that's been prepended to the cache
				changesValidFrom = change.Sequence
				break
			}
		}
	}

	numToPrepend := len(entriesToPrepend)
	if numToPrepend > 0 {
		c.logs = append(entriesToPrepend, c.logs...)
		base.Infof(base.KeyCache, "  Added %d entries from query (#%d--#%d) to cache of %q",
			numToPrepend, entriesToPrepend[0].Sequence, entriesToPrepend[numToPrepend-1].Sequence, base.UD(c.channelName))
	}
	base.Debugf(base.KeyCache, " Backfill cache from query c.validFrom from %v -> %v",
		c.validFrom, changesValidFrom)
	c.validFrom = changesValidFrom
	return numToPrepend

}

func (c *singleChannelCache) GetSize() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.logs)
}

type lateLogEntry struct {
	logEntry      *LogEntry
	arrived       time.Time    // Time arrived in late log - for diagnostics tracking
	listenerCount uint64       // Tracks the number of active changes feeds referencing this entry.  Used to identify when it's safe to prune
	listenerLock  sync.RWMutex // lock for updating listenerCount
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

func (l *lateLogEntry) getListenerCount() (count uint64) {
	l.listenerLock.RLock()
	count = l.listenerCount
	l.listenerLock.RUnlock()
	return count
}

// Initialize the late-arriving log queue with a zero entry, used to track listeners.  This is needed
// to support purging later entries once everyone has seen them.
func (c *singleChannelCache) initializeLateLogs() {
	log := &LogEntry{Sequence: 0}
	lateEntry := &lateLogEntry{
		logEntry:      log,
		listenerCount: 0,
	}
	c.lateLogs = append(c.lateLogs, lateEntry)
}

// Retrieve late-arriving sequences that have arrived since the previous sequence.  Retrieves set of sequences, and the last
// sequence number in the list.  Note that lateLogs is sorted by arrival on feed, not sequence number. Error indicates
// that sinceSequence isn't found in history, and caller should reset to low sequence.
func (c *singleChannelCache) GetLateSequencesSince(sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {

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
	lastEntry := c._mostRecentLateLog()
	lastEntry.addListener()
	lastSequence = lastEntry.logEntry.Sequence

	return entries, lastSequence, nil

}

// Called on first call to the channel during changes processing, to get starting point for
// subsequent checks for late arriving sequences.
func (c *singleChannelCache) RegisterLateSequenceClient() (latestLateSeq uint64) {

	c.lateLogLock.RLock()
	latestLog := c._mostRecentLateLog()
	latestLog.addListener()
	latestLateSeq = latestLog.logEntry.Sequence
	c.lateLogLock.RUnlock()
	return latestLateSeq
}

// Called when a client (a continuous _changes feed) is no longer referencing the sequence number.
func (c *singleChannelCache) ReleaseLateSequenceClient(sequence uint64) (success bool) {
	for _, log := range c.lateLogs {
		if log.logEntry.Sequence == sequence {
			log.removeListener()
			return true
		}
	}
	return false
}

// Receive new late sequence
func (c *singleChannelCache) AddLateSequence(change *LogEntry) {
	// Add to lateLogs.
	lateEntry := &lateLogEntry{
		logEntry:      change,
		arrived:       time.Now(),
		listenerCount: 0,
	}
	c.lateLogLock.Lock()
	c.lateLogs = append(c.lateLogs, lateEntry)
	c.lastLateSequence = change.Sequence
	// Currently we're only purging on add.  Could also consider a timed purge to handle the case
	// where all the listeners get caught up, but there aren't any subsequent late entries.  Not
	// a high priority, as the memory overhead for the late entries should be trivial, and probably
	// doesn't merit
	c._purgeLateLogEntries()
	c.lateLogLock.Unlock()
}

// Purge entries from the beginning of the list having no active listeners.  Any newly connecting clients
// will get these entries directly from the cache.  Always maintain
// at least one entry in the list, to track new listeners.  Expects to have a lock on lateLogLock.
func (c *singleChannelCache) _purgeLateLogEntries() {
	for len(c.lateLogs) > 1 && c.lateLogs[0].getListenerCount() == 0 {
		c.lateLogs = c.lateLogs[1:]
	}
}

// Purge entries from the beginning of the list having no active listeners.  Any newly connecting clients
// will get these entries directly from the cache.  Always maintain
// at least one entry in the list, to track new listeners.
func (c *singleChannelCache) purgeLateLogEntries() {
	c.lateLogLock.Lock()
	c._purgeLateLogEntries()
	c.lateLogLock.Unlock()
}

// mostRecentLateLog assumes caller has at least read lock on c.lateLogLock
func (c *singleChannelCache) _mostRecentLateLog() *lateLogEntry {
	if len(c.lateLogs) > 0 {
		return c.lateLogs[len(c.lateLogs)-1]
	}
	return nil
}

func (c *singleChannelCache) addDocIDs(changes LogEntries) {
	for _, change := range changes {
		c.cachedDocIDs[change.DocID] = struct{}{}
	}
}

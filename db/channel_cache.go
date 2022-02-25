/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	MinimumChannelCacheMaxNumber = 100 // Minimum size for channel cache capacity
)

var (
	DefaultChannelCacheMinLength       = 50               // Keep at least this many entries in cache
	DefaultChannelCacheMaxLength       = 500              // Don't put more than this many entries in cache
	DefaultChannelCacheAge             = 60 * time.Second // Keep entries at least this long
	DefaultChannelCacheMaxNumber       = 50000            // Default of 50k channel caches
	DefaultCompactHighWatermarkPercent = 80               // Default compaction high watermark (percent of MaxNumber)
	DefaultCompactLowWatermarkPercent  = 60               // Default compaction low watermark (percent of MaxNumber)
)

type ChannelCache interface {

	// Initializes the cache high sequence value
	Init(initialSequence uint64)

	// Adds an entry to the cache, returns set of channels it was added to
	AddToCache(change *LogEntry) []string

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
	// // TODO: let the cache manage its own stats internally (maybe take an updateStats call)
	MaxCacheSize() int

	// Returns the highest cached sequence, used for changes synchronization
	GetHighCacheSequence() uint64

	// Access to individual channel cache
	getSingleChannelCache(channelName string) SingleChannelCache

	// Access to individual bypass channel cache
	getBypassChannelCache(channelName string) SingleChannelCache

	// Stop stops the channel cache and it's background tasks.
	Stop()
}

// ChannelQueryHandler interface is implemented by databaseContext.
type ChannelQueryHandler interface {
	getChangesInChannelFromQuery(ctx context.Context, channelName string, startSeq, endSeq uint64, limit int, activeOnly bool) (LogEntries, error)
}

type StableSequenceCallbackFunc func() uint64

type channelCacheImpl struct {
	queryHandler         ChannelQueryHandler       // Passed to singleChannelCacheImpl for view queries.
	channelCaches        *base.RangeSafeCollection // A collection of singleChannelCaches
	backgroundTasks      []BackgroundTask          // List of background tasks specific to channel cache.
	dbName               string                    // Name of the database associated with the channel cache.
	terminator           chan bool                 // Signal terminator of background goroutines
	options              ChannelCacheOptions       // Channel cache options
	lateSeqLock          sync.RWMutex              // Coordinates access to late sequence caches
	highCacheSequence    uint64                    // The highest sequence that has been cached.  Used to initialize validFrom for new singleChannelCaches
	seqLock              sync.RWMutex              // Mutex for highCacheSequence
	maxChannels          int                       // Maximum number of channels in the cache
	compactHighWatermark int                       // High Watermark for cache compaction
	compactLowWatermark  int                       // Low Watermark for cache compaction
	compactRunning       base.AtomicBool           // Whether compact is currently running
	activeChannels       *channels.ActiveChannels  // Active channel handler
	cacheStats           *base.CacheStats          // Map used for cache stats
	validFromLock        sync.RWMutex              // Mutex used to avoid race between AddToCache and addChannelCache.  See CBG-520 for more details
}

func NewChannelCacheForContext(options ChannelCacheOptions, context *DatabaseContext) (*channelCacheImpl, error) {
	return newChannelCache(context.Name, options, context, context.activeChannels, context.DbStats.Cache())
}

func newChannelCache(dbName string, options ChannelCacheOptions, queryHandler ChannelQueryHandler,
	activeChannels *channels.ActiveChannels, cacheStats *base.CacheStats) (*channelCacheImpl, error) {

	channelCache := &channelCacheImpl{
		queryHandler:         queryHandler,
		channelCaches:        base.NewRangeSafeCollection(),
		dbName:               dbName,
		terminator:           make(chan bool),
		options:              options,
		maxChannels:          options.MaxNumChannels,
		compactHighWatermark: int(math.Round(float64(options.CompactHighWatermarkPercent) / 100 * float64(options.MaxNumChannels))),
		compactLowWatermark:  int(math.Round(float64(options.CompactLowWatermarkPercent) / 100 * float64(options.MaxNumChannels))),
		activeChannels:       activeChannels,
		cacheStats:           cacheStats,
	}
	bgt, err := NewBackgroundTask("CleanAgedItems", dbName, channelCache.cleanAgedItems, options.ChannelCacheAge, channelCache.terminator)
	if err != nil {
		return nil, err
	}
	channelCache.backgroundTasks = append(channelCache.backgroundTasks, bgt)
	base.DebugfCtx(context.Background(), base.KeyCache, "Initialized channel cache with maxChannels:%d, HWM: %d, LWM: %d",
		channelCache.maxChannels, channelCache.compactHighWatermark, channelCache.compactLowWatermark)
	return channelCache, nil
}

func (c *channelCacheImpl) Clear() {
	c.seqLock.Lock()
	c.channelCaches.Init()
	c.seqLock.Unlock()
}

// Stop stops the channel cache and it's background tasks.
func (c *channelCacheImpl) Stop() {
	// Signal to terminate channel cache background tasks.
	close(c.terminator)

	// Wait for channel cache background tasks to finish.
	waitForBGTCompletion(BGTCompletionMaxWait, c.backgroundTasks, c.dbName)
}

func (c *channelCacheImpl) Init(initialSequence uint64) {
	c.seqLock.Lock()
	c.highCacheSequence = initialSequence
	c.seqLock.Unlock()
}

func (c *channelCacheImpl) GetHighCacheSequence() uint64 {
	c.seqLock.RLock()
	highSeq := c.highCacheSequence
	c.seqLock.RUnlock()
	return highSeq
}

// Returns high cache sequence.  Callers should hold read lock on seqLock
func (c *channelCacheImpl) _getHighCacheSequence() uint64 {
	return c.highCacheSequence
}

// Updates the high cache sequence if the incoming sequence is higher than current
// Note: doesn't require compareAndSet on update, as cache expects AddToCache to only be called from
// a single goroutine
func (c *channelCacheImpl) updateHighCacheSequence(sequence uint64) {
	c.seqLock.Lock()
	if sequence > c.highCacheSequence {
		c.highCacheSequence = sequence
	}
	c.seqLock.Unlock()
}

// GetSingleChannelCache will create the cache for the channel if it doesn't exist.  If the cache is at
// capacity, will return a bypass channel cache.
func (c *channelCacheImpl) getSingleChannelCache(channelName string) SingleChannelCache {

	return c.getChannelCache(channelName)
}

func (c *channelCacheImpl) AddPrincipal(change *LogEntry) {
	c.updateHighCacheSequence(change.Sequence)
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (c *channelCacheImpl) AddToCache(change *LogEntry) (updatedChannels []string) {

	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory

	// updatedChannels tracks the set of channels that should be notified of the change.  This includes
	// the change's active channels, as well as any channel removals for the active revision.
	updatedChannels = make([]string, 0, len(ch))

	// If it's a late sequence, we want to add to all channel late queues within a single write lock,
	// to avoid a changes feed seeing the same late sequence in different iteration loops (and sending
	// twice)
	if change.Skipped {
		c.lateSeqLock.Lock()
		base.InfofCtx(context.TODO(), base.KeyChanges, "Acquired late sequence lock in order to cache %d - doc %q / %q", change.Sequence, base.UD(change.DocID), change.RevID)
		defer c.lateSeqLock.Unlock()
	}

	// Need to acquire the validFromLock prior to checking for active channel caches, to ensure that
	// any new caches that are added between the check for c.GetActiveChannelCache and the update of
	// c.highCacheSequence are initialized with the correct validFrom.
	var explicitStarChannel bool
	c.validFromLock.Lock()
	for channelName, removal := range ch {
		if removal == nil || removal.Seq == change.Sequence {
			// If the document has been explicitly added to the star channel by the sync function, don't need to recheck below
			if channelName == channels.UserStarChannel {
				explicitStarChannel = true
			}
			channelCache, ok := c.getActiveChannelCache(channelName)
			if ok {
				channelCache.addToCache(change, removal != nil)
				if change.Skipped {
					channelCache.AddLateSequence(change)
				}
			}
			// Need to notify even if channel isn't active, for case where number of connected changes channels exceeds cache capacity
			updatedChannels = append(updatedChannels, channelName)
		}
	}

	if EnableStarChannelLog && !explicitStarChannel {
		channelCache, ok := c.getActiveChannelCache(channels.UserStarChannel)
		if ok {
			channelCache.addToCache(change, false)
			if change.Skipped {
				channelCache.AddLateSequence(change)
			}
		}
		updatedChannels = append(updatedChannels, channels.UserStarChannel)
	}

	c.updateHighCacheSequence(change.Sequence)
	c.validFromLock.Unlock()
	return updatedChannels
}

// Remove purges the given doc IDs from all channel caches and returns the number of items removed.
// count will be larger than the input slice if the same document is removed from multiple channel caches.
func (c *channelCacheImpl) Remove(docIDs []string, startTime time.Time) (count int) {
	// Exit early if there's no work to do
	if len(docIDs) == 0 {
		return 0
	}

	removeCallback := func(v interface{}) bool {
		channelCache := AsSingleChannelCache(v)
		if channelCache == nil {
			return false
		}

		count += channelCache.Remove(docIDs, startTime)
		return true
	}

	c.channelCaches.Range(removeCallback)

	return count
}

func (c *channelCacheImpl) GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error) {

	return c.getChannelCache(channelName).GetChanges(options)
}

func (c *channelCacheImpl) GetCachedChanges(channelName string) []*LogEntry {
	options := ChangesOptions{Since: SequenceID{Seq: 0}}
	_, changes := c.getChannelCache(channelName).GetCachedChanges(options)
	return changes
}

// CleanAgedItems prunes the caches based on age of items. Error returned to fulfill BackgroundTaskFunc signature.
func (c *channelCacheImpl) cleanAgedItems(ctx context.Context) error {

	callback := func(v interface{}) bool {
		channelCache := AsSingleChannelCache(v)
		if channelCache == nil {
			return false
		}
		channelCache.pruneCacheAge(ctx)
		return true
	}
	c.channelCaches.Range(callback)

	return nil
}

func (c *channelCacheImpl) getChannelCache(channelName string) SingleChannelCache {

	cacheValue, found := c.channelCaches.Get(channelName)
	if found {
		return AsSingleChannelCache(cacheValue)
	}

	// Attempt to add a singleChannelCache for the channel name.  If unsuccessful, return a bypass channel cache
	singleChannelCache, ok := c.addChannelCache(channelName)
	if ok {
		return singleChannelCache
	}

	bypassChannelCache := &bypassChannelCache{
		channelName:  channelName,
		queryHandler: c.queryHandler,
	}
	c.cacheStats.ChannelCacheBypassCount.Add(1)
	return bypassChannelCache

}

func (c *channelCacheImpl) getBypassChannelCache(channelName string) SingleChannelCache {
	bypassChannelCache := &bypassChannelCache{
		channelName:  channelName,
		queryHandler: c.queryHandler,
	}
	return bypassChannelCache
}

// Converts an RangeSafeCollection value to a singleChannelCacheImpl.  On type
// conversion error, logs a warning and returns nil.
func AsSingleChannelCache(cacheValue interface{}) *singleChannelCacheImpl {
	singleChannelCache, ok := cacheValue.(*singleChannelCacheImpl)
	if !ok {
		base.WarnfCtx(context.Background(), "Unexpected channel cache value type: %T", cacheValue)
		return nil
	}
	return singleChannelCache
}

// Adds a new channel to the channel cache.  Locking seqLock is required here to prevent missed data in the following scenario:
//	//     1. addChannelCache issued for channel A
//	//     2. addChannelCache obtains stable sequence, seq=10
//	//     3. addToCache (from another goroutine) receives sequence 11 in channel A, but detects that it's inactive (not in c.channelCaches)
//	//     4. addChannelCache initializes cache with validFrom=10 and adds to c.channelCaches
//	//  This scenario would result in sequence 11 missing from the cache.  Locking seqLock ensures that
//	//  step 3 blocks until step 4 is complete (and so sees the channel as active)
func (c *channelCacheImpl) addChannelCache(channelName string) (*singleChannelCacheImpl, bool) {

	// Return nil if the cache at capacity.
	if c.channelCaches.Length() >= c.maxChannels {
		return nil, false
	}

	c.validFromLock.Lock()

	// Everything after the current high sequence will be added to the cache via the feed
	validFrom := c.GetHighCacheSequence() + 1

	singleChannelCache := newChannelCacheWithOptions(c.queryHandler, channelName, validFrom, c.options, c.cacheStats)
	cacheValue, created, cacheSize := c.channelCaches.GetOrInsert(channelName, singleChannelCache)
	c.validFromLock.Unlock()

	singleChannelCache = AsSingleChannelCache(cacheValue)

	if cacheSize > c.compactHighWatermark {
		c.startCacheCompaction()
	}

	if created {
		c.cacheStats.ChannelCacheNumChannels.Add(1)
		c.cacheStats.ChannelCacheChannelsAdded.Add(1)
	}

	return singleChannelCache, true
}

func (c *channelCacheImpl) getActiveChannelCache(channelName string) (*singleChannelCacheImpl, bool) {

	cacheValue, found := c.channelCaches.Get(channelName)
	if !found {
		return nil, false
	}
	cache := AsSingleChannelCache(cacheValue)
	return cache, cache != nil
}

func (c *channelCacheImpl) MaxCacheSize() int {

	maxCacheSize := 0
	callback := func(v interface{}) bool {
		channelCache := AsSingleChannelCache(v)
		if channelCache == nil {
			return false
		}
		channelSize := channelCache.GetSize()
		if channelSize > maxCacheSize {
			maxCacheSize = channelSize
		}
		return true
	}
	c.channelCaches.Range(callback)

	return maxCacheSize
}

func (c *channelCacheImpl) isCompactActive() bool {
	return c.compactRunning.IsTrue()
}

// startCacheCompaction starts a goroutine for cache compaction if it's not already running.
func (c *channelCacheImpl) startCacheCompaction() {
	compactNotStarted := c.compactRunning.CompareAndSwap(false, true)
	if compactNotStarted {
		go c.compactChannelCache()
	}
}

// Compact runs until the number of channels in the cache is lower than compactLowWatermark
func (c *channelCacheImpl) compactChannelCache() {
	defer c.compactRunning.Set(false)

	// Increment compact count on start, as timing is updated per loop iteration
	c.cacheStats.ChannelCacheCompactCount.Add(1)

	logCtx := context.TODO()
	cacheSize := c.channelCaches.Length()
	base.InfofCtx(logCtx, base.KeyCache, "Starting channel cache compaction, size %d", cacheSize)
	for {
		// channelCache close handling
		compactIterationStart := time.Now()

		select {
		case <-c.terminator:
			base.DebugfCtx(logCtx, base.KeyCache, "Channel cache compaction stopped due to cache close.")
			return
		default:
			// continue
		}

		// Maintain a target number of items to compact per iteration.  Break the list iteration when the target is reached
		targetEvictCount := cacheSize - c.compactLowWatermark
		if targetEvictCount <= 0 {
			base.InfofCtx(logCtx, base.KeyCache, "Stopping channel cache compaction, size %d", cacheSize)
			return
		}
		base.TracefCtx(logCtx, base.KeyCache, "Target eviction count: %d (lwm:%d)", targetEvictCount, c.compactLowWatermark)

		// Iterates through cache entries based on cache size at start of compaction iteration loop.  Intentionally
		// ignores channels added during compaction iteration
		inactiveEvictionCandidates := make([]*base.AppendOnlyListElement, 0)
		nruEvictionCandidates := make([]*base.AppendOnlyListElement, 0)

		// channelCacheList is an append only list.  Iterator iterates over the current list at the time the iterator was created.
		// Ensures that there are no data races with goroutines appending to the list.
		var elementCount int
		compactCallback := func(elem *base.AppendOnlyListElement) bool {
			elementCount++
			singleChannelCache, ok := elem.Value.(*singleChannelCacheImpl)
			if !ok {
				base.WarnfCtx(logCtx, "Non-cache entry (%T) found in channel cache during compaction - ignoring", elem.Value)
				return true
			}

			// If channel marked as recently used, update recently used flag and continue
			if singleChannelCache.recentlyUsed.IsTrue() {
				singleChannelCache.recentlyUsed.Set(false)
				return true
			}

			// Determine whether NRU channel is active, to establish eviction priority
			isActive := c.activeChannels.IsActive(singleChannelCache.channelName)
			if !isActive {
				base.TracefCtx(logCtx, base.KeyCache, "Marking inactive cache entry %q for eviction ", base.UD(singleChannelCache.channelName))
				inactiveEvictionCandidates = append(inactiveEvictionCandidates, elem)
			} else {
				base.TracefCtx(logCtx, base.KeyCache, "Marking NRU cache entry %q for eviction", base.UD(singleChannelCache.channelName))
				nruEvictionCandidates = append(nruEvictionCandidates, elem)
			}

			// If we have enough inactive channels to reach targetCount, terminate range
			if len(inactiveEvictionCandidates) >= targetEvictCount {
				base.TracefCtx(logCtx, base.KeyCache, "Eviction count target (%d) reached with inactive channels, proceeding to removal", targetEvictCount)
				return false
			}
			return true
		}

		c.channelCaches.RangeElements(compactCallback)

		remainingToEvict := targetEvictCount

		inactiveEvictCount := 0
		// We only want to evict up to targetEvictCount, with priority for inactive channels
		var evictionElements []*base.AppendOnlyListElement
		if len(inactiveEvictionCandidates) > targetEvictCount {
			evictionElements = inactiveEvictionCandidates[0:targetEvictCount]
			inactiveEvictCount = targetEvictCount
			remainingToEvict = 0
		} else {
			evictionElements = inactiveEvictionCandidates
			inactiveEvictCount = len(inactiveEvictionCandidates)
			remainingToEvict = remainingToEvict - len(inactiveEvictionCandidates)
		}

		if remainingToEvict > 0 {
			if len(nruEvictionCandidates) > remainingToEvict {
				evictionElements = append(evictionElements, nruEvictionCandidates[0:remainingToEvict]...)
				remainingToEvict = 0
			} else {
				evictionElements = append(evictionElements, nruEvictionCandidates...)
				remainingToEvict = remainingToEvict - len(nruEvictionCandidates)
			}
		}

		cacheSize = c.channelCaches.RemoveElements(evictionElements)

		// Update eviction stats
		c.updateEvictionStats(inactiveEvictCount, len(evictionElements), compactIterationStart)

		base.TracefCtx(logCtx, base.KeyCache, "Compact iteration complete - eviction count: %d (lwm:%d)", len(evictionElements), c.compactLowWatermark)
	}
}

// Updates cache stats
func (c *channelCacheImpl) updateEvictionStats(inactiveEvicted int, totalEvicted int, startTime time.Time) {
	// Eviction stats
	if inactiveEvicted > 0 {
		c.cacheStats.ChannelCacheChannelsEvictedInactive.Add(int64(inactiveEvicted))
	}
	nruEvicted := totalEvicted - inactiveEvicted
	if nruEvicted > 0 {
		c.cacheStats.ChannelCacheChannelsEvictedNRU.Add(int64(nruEvicted))
	}

	// Compaction time is updated on each iteration (to ensure stat is updated during long-running compact)
	c.cacheStats.ChannelCacheCompactTime.Add(time.Since(startTime).Nanoseconds())

	// Update channel count
	c.cacheStats.ChannelCacheNumChannels.Add(-1 * int64(totalEvicted))
}

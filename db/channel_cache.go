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
	AddToCache(ctx context.Context, change *LogEntry) []channels.ID

	// Notifies the cache of a principal update.  Updates the cache's high sequence
	AddPrincipal(change *LogEntry)

	// Notifies the cache of an unused sequence update. Updates the cache's high sequence
	AddUnusedSequence(change *LogEntry)

	// Remove purges the given doc IDs from all channel caches and returns the number of items removed.
	Remove(ctx context.Context, collectionID uint32, docIDs []string, startTime time.Time) (count int)

	// Returns set of changes for a given channel, within the bounds specified in options
	GetChanges(ctx context.Context, ch channels.ID, options ChangesOptions) ([]*LogEntry, error)

	// Returns the set of all cached data for a given channel (intended for diagnostic usage)
	GetCachedChanges(ctx context.Context, ch channels.ID) ([]*LogEntry, error)

	// Clear reinitializes the cache to an empty state
	Clear()

	// Size of the the largest individual channel cache, invoked for stats reporting
	// // TODO: let the cache manage its own stats internally (maybe take an updateStats call)
	MaxCacheSize(context.Context) int

	// Returns the highest cached sequence, used for changes synchronization
	GetHighCacheSequence() uint64

	// Access to individual channel cache
	getSingleChannelCache(ctx context.Context, ch channels.ID) (SingleChannelCache, error)

	// Access to individual bypass channel cache
	getBypassChannelCache(ch channels.ID) (SingleChannelCache, error)

	// Stop stops the channel cache and it's background tasks.
	Stop(context.Context)
}

var _ ChannelCache = &channelCacheImpl{}

// ChannelQueryHandler interface is implemented by databaseContext and databaseCollection.
type ChannelQueryHandler interface {
	getChangesInChannelFromQuery(ctx context.Context, channelName string, startSeq, endSeq uint64, limit int, activeOnly bool) (LogEntries, error)
}

// Function that returns a ChannelQueryHandlerFunc for the specified collectionID
type ChannelQueryHandlerFactory func(collectionID uint32) (ChannelQueryHandler, error)

type StableSequenceCallbackFunc func() uint64

type channelCacheImpl struct {
	queryHandlerFactory  ChannelQueryHandlerFactory    // Factory to look up ChannelQueryHandler for a collectionID
	channelCaches        *channels.RangeSafeCollection // A collection of singleChannelCaches
	backgroundTasks      []BackgroundTask              // List of background tasks specific to channel cache.
	dbName               string                        // Name of the database associated with the channel cache.
	terminator           chan bool                     // Signal terminator of background goroutines
	options              ChannelCacheOptions           // Channel cache options
	lateSeqLock          sync.RWMutex                  // Coordinates access to late sequence caches
	highCacheSequence    uint64                        // The highest sequence that has been cached.  Used to initialize validFrom for new singleChannelCaches
	seqLock              sync.RWMutex                  // Mutex for highCacheSequence
	maxChannels          int                           // Maximum number of channels in the cache
	compactHighWatermark int                           // High Watermark for cache compaction
	compactLowWatermark  int                           // Low Watermark for cache compaction
	compactRunning       base.AtomicBool               // Whether compact is currently running
	activeChannels       *channels.ActiveChannels      // Active channel handler
	cacheStats           *base.CacheStats              // Map used for cache stats
	validFromLock        sync.RWMutex                  // Mutex used to avoid race between AddToCache and addChannelCache.  See CBG-520 for more details
}

func NewChannelCacheForContext(ctx context.Context, options ChannelCacheOptions, context *DatabaseContext) (*channelCacheImpl, error) {
	return newChannelCache(ctx, context.Name, options, context.getQueryHandlerForCollection, context.activeChannels, context.DbStats.Cache())
}

func newChannelCache(ctx context.Context, dbName string, options ChannelCacheOptions, queryHandlerFactory ChannelQueryHandlerFactory,
	activeChannels *channels.ActiveChannels, cacheStats *base.CacheStats) (*channelCacheImpl, error) {

	channelCache := &channelCacheImpl{
		queryHandlerFactory:  queryHandlerFactory,
		channelCaches:        channels.NewRangeSafeCollection(),
		dbName:               dbName,
		terminator:           make(chan bool),
		options:              options,
		maxChannels:          options.MaxNumChannels,
		compactHighWatermark: int(math.Round(float64(options.CompactHighWatermarkPercent) / 100 * float64(options.MaxNumChannels))),
		compactLowWatermark:  int(math.Round(float64(options.CompactLowWatermarkPercent) / 100 * float64(options.MaxNumChannels))),
		activeChannels:       activeChannels,
		cacheStats:           cacheStats,
	}
	bgt, err := NewBackgroundTask(ctx, "CleanAgedItems", channelCache.cleanAgedItems, options.ChannelCacheAge, channelCache.terminator)
	if err != nil {
		return nil, err
	}
	channelCache.backgroundTasks = append(channelCache.backgroundTasks, bgt)
	base.DebugfCtx(ctx, base.KeyCache, "Initialized channel cache with maxChannels:%d, HWM: %d, LWM: %d",
		channelCache.maxChannels, channelCache.compactHighWatermark, channelCache.compactLowWatermark)
	return channelCache, nil
}

func (c *channelCacheImpl) Clear() {
	c.seqLock.Lock()
	c.channelCaches.Init()
	c.seqLock.Unlock()
}

// Stop stops the channel cache and it's background tasks.
func (c *channelCacheImpl) Stop(ctx context.Context) {
	// Signal to terminate channel cache background tasks.
	close(c.terminator)

	// Wait for channel cache background tasks to finish.
	waitForBGTCompletion(ctx, BGTCompletionMaxWait, c.backgroundTasks, c.dbName)
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
func (c *channelCacheImpl) getSingleChannelCache(ctx context.Context, ch channels.ID) (SingleChannelCache, error) {

	return c.getChannelCache(ctx, ch)
}

func (c *channelCacheImpl) AddPrincipal(change *LogEntry) {
	c.updateHighCacheSequence(change.Sequence)
}

// Add unused Sequence notifies the cache of an unused sequence update. Updates the cache's high sequence
func (c *channelCacheImpl) AddUnusedSequence(change *LogEntry) {
	c.updateHighCacheSequence(change.Sequence)
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (c *channelCacheImpl) AddToCache(ctx context.Context, change *LogEntry) (updatedChannels []channels.ID) {

	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory

	// updatedChannels tracks the set of channels that should be notified of the change.  This includes
	// the change's active channels, as well as any channel removals for the active revision.
	updatedChannels = make([]channels.ID, 0, len(ch))

	// If it's a late sequence, we want to add to all channel late queues within a single write lock,
	// to avoid a changes feed seeing the same late sequence in different iteration loops (and sending
	// twice)
	if change.Skipped {
		c.lateSeqLock.Lock()
		base.InfofCtx(ctx, base.KeyChanges, "Acquired late sequence lock in order to cache %d - doc %q / %q", change.Sequence, base.UD(change.DocID), change.RevID)
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
			channelCache, ok := c.getActiveChannelCache(ctx, channels.NewID(channelName, change.CollectionID))
			if ok {
				channelCache.addToCache(ctx, change, removal != nil)
				if change.Skipped {
					channelCache.AddLateSequence(change)
				}
			}
			// Need to notify even if channel isn't active, for case where number of connected changes channels exceeds cache capacity
			updatedChannels = append(updatedChannels, channels.NewID(channelName, change.CollectionID))
		}
	}

	if EnableStarChannelLog && !explicitStarChannel {
		channelCache, ok := c.getActiveChannelCache(ctx, channels.NewID(channels.UserStarChannel, change.CollectionID))
		if ok {
			channelCache.addToCache(ctx, change, false)
			if change.Skipped {
				channelCache.AddLateSequence(change)
			}
		}
		updatedChannels = append(updatedChannels, channels.NewID(channels.UserStarChannel, change.CollectionID))
	}

	c.updateHighCacheSequence(change.Sequence)
	c.validFromLock.Unlock()
	return updatedChannels
}

// Remove purges the given doc IDs from all channel caches and returns the number of items removed.
// count will be larger than the input slice if the same document is removed from multiple channel caches.
func (c *channelCacheImpl) Remove(ctx context.Context, collectionID uint32, docIDs []string, startTime time.Time) (count int) {
	// Exit early if there's no work to do
	if len(docIDs) == 0 {
		return 0
	}

	removeCallback := func(v interface{}) bool {
		channelCache := AsSingleChannelCache(ctx, v)
		if channelCache == nil {
			return false
		}
		if channelCache.ChannelID().CollectionID != collectionID {
			return true
		}
		count += channelCache.Remove(ctx, collectionID, docIDs, startTime)
		return true
	}

	c.channelCaches.Range(removeCallback)

	return count
}

func (c *channelCacheImpl) GetChanges(ctx context.Context, ch channels.ID, options ChangesOptions) ([]*LogEntry, error) {

	cache, err := c.getChannelCache(ctx, ch)
	if err != nil {
		return nil, err
	}
	return cache.GetChanges(ctx, options)
}

func (c *channelCacheImpl) GetCachedChanges(ctx context.Context, channel channels.ID) ([]*LogEntry, error) {
	options := ChangesOptions{Since: SequenceID{Seq: 0}}
	cache, err := c.getChannelCache(ctx, channel)
	if err != nil {
		return nil, err
	}
	_, changes := cache.GetCachedChanges(options)
	return changes, nil
}

// CleanAgedItems prunes the caches based on age of items. Error returned to fulfill BackgroundTaskFunc signature.
func (c *channelCacheImpl) cleanAgedItems(ctx context.Context) error {

	callback := func(v interface{}) bool {
		channelCache := AsSingleChannelCache(ctx, v)
		if channelCache == nil {
			return false
		}
		channelCache.pruneCacheAge(ctx)
		return true
	}
	c.channelCaches.Range(callback)

	return nil
}

func (c *channelCacheImpl) getChannelCache(ctx context.Context, channel channels.ID) (SingleChannelCache, error) {

	cacheValue, found := c.channelCaches.Get(channel)
	if found {
		return AsSingleChannelCache(ctx, cacheValue), nil
	}

	// Attempt to add a singleChannelCache for the channel name.  If unsuccessful, return a bypass channel cache
	singleChannelCache, ok := c.addChannelCache(ctx, channel)
	if ok {
		return singleChannelCache, nil
	}

	queryHandler, err := c.queryHandlerFactory(channel.CollectionID)
	if err != nil {
		return nil, err
	}

	bypassChannelCache := &bypassChannelCache{
		channel:      channel,
		queryHandler: queryHandler,
	}
	c.cacheStats.ChannelCacheBypassCount.Add(1)
	return bypassChannelCache, nil

}

func (c *channelCacheImpl) getBypassChannelCache(ch channels.ID) (SingleChannelCache, error) {
	queryHandler, err := c.queryHandlerFactory(ch.CollectionID)
	if err != nil {
		return nil, err
	}
	bypassChannelCache := &bypassChannelCache{
		channel:      ch,
		queryHandler: queryHandler,
	}
	return bypassChannelCache, nil
}

// Converts an RangeSafeCollection value to a singleChannelCacheImpl.  On type
// conversion error, logs a warning and returns nil.
func AsSingleChannelCache(ctx context.Context, cacheValue interface{}) *singleChannelCacheImpl {
	singleChannelCache, ok := cacheValue.(*singleChannelCacheImpl)
	if !ok {
		base.WarnfCtx(ctx, "Unexpected channel cache value type: %T", cacheValue)
		return nil
	}
	return singleChannelCache
}

// Adds a new channel to the channel cache.  Locking seqLock is required here to prevent missed data in the following scenario:
//
//	//     1. addChannelCache issued for channel A
//	//     2. addChannelCache obtains stable sequence, seq=10
//	//     3. addToCache (from another goroutine) receives sequence 11 in channel A, but detects that it's inactive (not in c.channelCaches)
//	//     4. addChannelCache initializes cache with validFrom=10 and adds to c.channelCaches
//	//  This scenario would result in sequence 11 missing from the cache.  Locking seqLock ensures that
//	//  step 3 blocks until step 4 is complete (and so sees the channel as active)
func (c *channelCacheImpl) addChannelCache(ctx context.Context, channel channels.ID) (*singleChannelCacheImpl, bool) {

	// Return nil if the cache at capacity.
	if c.channelCaches.Length() >= c.maxChannels {
		return nil, false
	}

	// Return nil if a queryHandler can't be obtained for the collectionID
	queryHandler, err := c.queryHandlerFactory(channel.CollectionID)
	if err != nil {
		return nil, false
	}

	c.validFromLock.Lock()

	// Everything after the current high sequence will be added to the cache via the feed
	validFrom := c.GetHighCacheSequence() + 1

	singleChannelCache :=
		newChannelCacheWithOptions(ctx, queryHandler, channel, validFrom, c.options, c.cacheStats)
	cacheValue, created, cacheSize := c.channelCaches.GetOrInsert(channel, singleChannelCache)
	c.validFromLock.Unlock()

	singleChannelCache = AsSingleChannelCache(ctx, cacheValue)

	if cacheSize > c.compactHighWatermark {
		c.startCacheCompaction(ctx)
	}

	if created {
		c.cacheStats.ChannelCacheNumChannels.Add(1)
		c.cacheStats.ChannelCacheChannelsAdded.Add(1)
	}

	return singleChannelCache, true
}

func (c *channelCacheImpl) getActiveChannelCache(ctx context.Context, channel channels.ID) (*singleChannelCacheImpl, bool) {

	cacheValue, found := c.channelCaches.Get(channel)
	if !found {
		return nil, false
	}
	cache := AsSingleChannelCache(ctx, cacheValue)
	return cache, cache != nil
}

func (c *channelCacheImpl) MaxCacheSize(ctx context.Context) int {

	maxCacheSize := 0
	callback := func(v interface{}) bool {
		channelCache := AsSingleChannelCache(ctx, v)
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
func (c *channelCacheImpl) startCacheCompaction(ctx context.Context) {
	compactNotStarted := c.compactRunning.CompareAndSwap(false, true)
	if compactNotStarted {
		go c.compactChannelCache(ctx)
	}
}

// Compact runs until the number of channels in the cache is lower than compactLowWatermark
func (c *channelCacheImpl) compactChannelCache(ctx context.Context) {
	defer c.compactRunning.Set(false)

	// Increment compact count on start, as timing is updated per loop iteration
	c.cacheStats.ChannelCacheCompactCount.Add(1)

	cacheSize := c.channelCaches.Length()
	base.InfofCtx(ctx, base.KeyCache, "Starting channel cache compaction, size %d", cacheSize)
	for {
		// channelCache close handling
		compactIterationStart := time.Now()

		select {
		case <-c.terminator:
			base.DebugfCtx(ctx, base.KeyCache, "Channel cache compaction stopped due to cache close.")
			return
		default:
			// continue
		}

		// Maintain a target number of items to compact per iteration.  Break the list iteration when the target is reached
		targetEvictCount := cacheSize - c.compactLowWatermark
		if targetEvictCount <= 0 {
			base.InfofCtx(ctx, base.KeyCache, "Stopping channel cache compaction, size %d", cacheSize)
			return
		}
		base.TracefCtx(ctx, base.KeyCache, "Target eviction count: %d (lwm:%d)", targetEvictCount, c.compactLowWatermark)

		// Iterates through cache entries based on cache size at start of compaction iteration loop.  Intentionally
		// ignores channels added during compaction iteration
		inactiveEvictionCandidates := make([]*channels.AppendOnlyListElement, 0)
		nruEvictionCandidates := make([]*channels.AppendOnlyListElement, 0)

		// channelCacheList is an append only list.  Iterator iterates over the current list at the time the iterator was created.
		// Ensures that there are no data races with goroutines appending to the list.
		var elementCount int
		compactCallback := func(elem *channels.AppendOnlyListElement) bool {
			elementCount++
			singleChannelCache, ok := elem.Value.(*singleChannelCacheImpl)
			if !ok {
				base.WarnfCtx(ctx, "Non-cache entry (%T) found in channel cache during compaction - ignoring", elem.Value)
				return true
			}

			// If channel marked as recently used, update recently used flag and continue
			if singleChannelCache.recentlyUsed.IsTrue() {
				singleChannelCache.recentlyUsed.Set(false)
				return true
			}

			// Determine whether NRU channel is active, to establish eviction priority
			isActive := c.activeChannels.IsActive(singleChannelCache.channelID)
			if !isActive {
				base.TracefCtx(ctx, base.KeyCache, "Marking inactive cache entry %q for eviction ", base.UD(singleChannelCache.channelID))
				inactiveEvictionCandidates = append(inactiveEvictionCandidates, elem)
			} else {
				base.TracefCtx(ctx, base.KeyCache, "Marking NRU cache entry %q for eviction", base.UD(singleChannelCache.channelID))
				nruEvictionCandidates = append(nruEvictionCandidates, elem)
			}

			// If we have enough inactive channels to reach targetCount, terminate range
			if len(inactiveEvictionCandidates) >= targetEvictCount {
				base.TracefCtx(ctx, base.KeyCache, "Eviction count target (%d) reached with inactive channels, proceeding to removal", targetEvictCount)
				return false
			}
			return true
		}

		c.channelCaches.RangeElements(compactCallback)

		remainingToEvict := targetEvictCount

		inactiveEvictCount := 0
		// We only want to evict up to targetEvictCount, with priority for inactive channels
		var evictionElements []*channels.AppendOnlyListElement
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
			} else {
				evictionElements = append(evictionElements, nruEvictionCandidates...)
			}
		}

		cacheSize = c.channelCaches.RemoveElements(evictionElements)

		// Update eviction stats
		c.updateEvictionStats(inactiveEvictCount, len(evictionElements), compactIterationStart)

		base.TracefCtx(ctx, base.KeyCache, "Compact iteration complete - eviction count: %d (lwm:%d)", len(evictionElements), c.compactLowWatermark)
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

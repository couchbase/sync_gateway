package db

import (
	"context"
	"expvar"
	"math"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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

// ChannelQueryHandler interface is implemented by databaseContext.
type ChannelQueryHandler interface {
	getChangesInChannelFromQuery(channelName string, startSeq, endSeq uint64, limit int, activeOnly bool) (LogEntries, error)
}

type StableSequenceCallbackFunc func() uint64

type channelCacheImpl struct {
	queryHandler         ChannelQueryHandler            // Passed to singleChannelCache for view queries.
	channelCaches        map[string]*singleChannelCache // A cache of changes for each channel
	channelCacheList     *base.AppendOnlyList           // List of pointers to channelCache entries, for non-locking iteration during compaction
	cacheLock            sync.RWMutex                   // Mutex for access to channelCaches map
	terminator           chan bool                      // Signal terminator of background goroutines
	options              ChannelCacheOptions            // Channel cache options
	lateSeqLock          sync.RWMutex                   // Coordinates access to late sequence caches
	highCacheSequence    uint64                         // The highest sequence that has been cached.  Used to initialize validFrom for new singleChannelCaches
	maxChannels          int                            // Maximum number of channels in the cache
	compactHighWatermark int                            // High Watermark for cache compaction
	compactLowWatermark  int                            // Low Watermark for cache compaction
	compactRunning       base.AtomicBool                // Whether compact is currently running
	activeChannels       channels.ActiveChannels        // Active channel handler
	statsMap             *expvar.Map                    // Map used for cache stats

}

func NewChannelCacheForContext(terminator chan bool, options ChannelCacheOptions, context *DatabaseContext) *channelCacheImpl {
	return newChannelCache(context.Name, terminator, options, context, context.activeChannels, context.DbStats.StatsCache())
}

func newChannelCache(dbName string, terminator chan bool, options ChannelCacheOptions, queryHandler ChannelQueryHandler, activeChannels channels.ActiveChannels, statsMap *expvar.Map) *channelCacheImpl {

	channelCache := &channelCacheImpl{
		queryHandler:         queryHandler,
		channelCaches:        make(map[string]*singleChannelCache),
		channelCacheList:     base.NewAppendOnlyList(),
		terminator:           terminator,
		options:              options,
		maxChannels:          options.MaxNumChannels,
		compactHighWatermark: int(math.Round(float64(options.CompactHighWatermarkPercent) / 100 * float64(options.MaxNumChannels))),
		compactLowWatermark:  int(math.Round(float64(options.CompactLowWatermarkPercent) / 100 * float64(options.MaxNumChannels))),
		activeChannels:       activeChannels,
		statsMap:             statsMap,
	}
	NewBackgroundTask("CleanAgedItems", dbName, channelCache.cleanAgedItems, options.ChannelCacheAge, terminator)
	base.Infof(base.KeyCache, "Initialized channel cache with maxChannels:%d, HWM: %d, LWM: %d", channelCache.maxChannels, channelCache.compactHighWatermark, channelCache.compactLowWatermark)
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
	}
	c.cacheLock.Unlock()
}

// GetSingleChannelCache will create the cache for the channel if it doesn't exist.  Intended for test
// usage only - otherwise management of per-channel caches should be opaque to ChannelCache consumers.
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

	return c.addChannelCache(channelName)
}

// Adds a new channel to the channel cache.  Note that locking here also prevents missed data in the following scenario:
//	//     1. getChannelCache issued for channel A
//	//     2. getChannelCache obtains stable sequence, seq=10
//	//     3. addToCache (from another goroutine) receives sequence 11 in channel A, but detects that it's inactive (not in c.channelCaches)
//	//     4. getChannelCache initializes cache with validFrom=10 and adds to c.channelCaches
//	//  This scenario would result in sequence 11 missing from the cache.  Locking cacheLock here ensures that
//	//  step 3 blocks until step 4 is complete (and so sees the channel as active)
func (c *channelCacheImpl) addChannelCache(channelName string) *singleChannelCache {

	c.cacheLock.Lock()

	// Check if it was created by another goroutine while we waited for the lock
	singleChannelCache, ok := c.channelCaches[channelName]
	if ok {
		c.cacheLock.Unlock()
		return singleChannelCache
	}

	// Everything after the current high sequence will be added to the cache via the feed
	validFrom := c._getHighCacheSequence() + 1

	singleChannelCache = newChannelCacheWithOptions(c.queryHandler, channelName, validFrom, c.options, c.statsMap)
	c.channelCaches[channelName] = singleChannelCache
	c.channelCacheList.PushBack(singleChannelCache)

	if !c.isCompactActive() && len(c.channelCaches) > c.compactHighWatermark {
		c.startCacheCompaction()
	}

	c.statsMap.Add(base.StatKeyChannelCacheNumChannels, 1)
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

func (c *channelCacheImpl) isCompactActive() bool {
	return c.compactRunning.IsTrue()
}

func (c *channelCacheImpl) startCacheCompaction() {
	c.compactRunning.Set(true)
	go c.compactChannelCache()
}

// Compact runs until the number of channels in the cache is lower than compactLowWatermark
func (c *channelCacheImpl) compactChannelCache() {
	c.cacheLock.RLock()
	cacheSize := len(c.channelCaches)
	c.cacheLock.RUnlock()

	base.Infof(base.KeyCache, "Starting channel cache compaction, size %d", cacheSize)
	defer c.compactRunning.Set(false)

	for {
		// channelCache close handling
		select {
		case <-c.terminator:
			base.Debugf(base.KeyCache, "Channel cache compaction stopped due to cache close.")
			return
		default:
			// continue
		}

		// Maintain a target number of items to compact per iteration.  Break the list iteration when the target is reached
		targetEvictCount := cacheSize - c.compactLowWatermark
		if targetEvictCount <= 0 {
			base.Infof(base.KeyCache, "Stopping channel cache compaction, size %d", cacheSize)
			return
		}
		base.Tracef(base.KeyCache, "Target eviction count: %d (lwm:%d)", targetEvictCount, c.compactLowWatermark)

		// Iterates through cache entries based on cache size at start of compaction iteration loop.  Intentionally
		// ignores channels added during compaction iteration
		inactiveEvictionCandidates := make([]*base.AppendOnlyElement, 0)
		nruEvictionCandidates := make([]*base.AppendOnlyElement, 0)
		elem := c.channelCacheList.Front()

		// channelCacheList is an append only list.  Iterating up to (cacheSize - 1) during compaction ensures that there are no data races
		// with goroutines appending to the list.
		for i := 0; i < cacheSize-1; i++ {
			singleChannelCache, ok := elem.Value.(*singleChannelCache)
			if !ok {
				base.Warnf(base.KeyCache, "Non-cache entry (%T) found in channel cache during compaction - ignoring", elem.Value)
				continue
			}

			// If channel marked as recently used, update recently used flag and continue
			if singleChannelCache.recentlyUsed.IsTrue() {
				singleChannelCache.recentlyUsed.Set(false)
				elem = elem.Next()
				continue
			}

			// Determine whether NRU channel is active, to establish eviction priority
			isActive := c.activeChannels.IsActive(singleChannelCache.channelName)
			if !isActive {
				base.Tracef(base.KeyCache, "Marking inactive cache entry %q for eviction ", base.UD(singleChannelCache.channelName))
				inactiveEvictionCandidates = append(inactiveEvictionCandidates, elem)
			} else {
				base.Tracef(base.KeyCache, "Marking NRU cache entry %q for eviction", base.UD(singleChannelCache.channelName))
				nruEvictionCandidates = append(nruEvictionCandidates, elem)
			}

			// If we have enough inactive channels to reach
			if len(inactiveEvictionCandidates) >= targetEvictCount {
				base.Tracef(base.KeyCache, "Eviction count target (%d) reached with inactive channels, proceeding to removal", targetEvictCount)
				break
			}
			elem = elem.Next()
		}

		evictedCount := 0
		c.cacheLock.Lock()
		// Evict inactive
		for _, elem := range inactiveEvictionCandidates {
			c._evictChannel(elem)
			evictedCount++
			if evictedCount >= targetEvictCount {
				break
			}
		}

		// Evict NRU
		for _, elem := range nruEvictionCandidates {
			if evictedCount >= targetEvictCount {
				break
			}
			c._evictChannel(elem)
			evictedCount++
		}

		base.Tracef(base.KeyCache, "Compact iteration complete - eviction count: %d (lwm:%d)", evictedCount, c.compactLowWatermark)
		cacheSize = len(c.channelCaches)
		c.cacheLock.Unlock()
	}
}

// Evicts a channel from the cache based on a channelCacheList element.  Requires caller to hold c.cacheLock.Lock
func (c *channelCacheImpl) _evictChannel(elem *base.AppendOnlyElement) {
	value := c.channelCacheList.Remove(elem)
	channelCache, ok := value.(*singleChannelCache)
	if !ok {
		base.Warnf(base.KeyCache, "Unexpected channelCacheList element type: %T", value)
	}
	delete(c.channelCaches, channelCache.channelName)

	base.Tracef(base.KeyCache, "Evicted channel %s from cache", channelCache.channelName)
}

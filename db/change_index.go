package db

import (
	"context"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// A ChangeIndex is responsible for indexing incoming events from change_listener, and
// and servicing requests for indexed changes from changes.go.  In addition to basic feed processing
// ChangeIndex is the component responsible for index consistency and sequence management.  The ChangeIndex
// doesn't define storage - see ChannelIndex for storage details.
//
// Currently there are two ChangeIndex implementations:
//   1. change_cache.go.
//      Stores recent index entries in memory, retrieves older entries
//      using view query.
//      Assumes a single SG node (itself) as index writer, processing entire mutation stream.
//      Uses _sync:seq for sequence management, and buffers
//      incoming sequences from the feed to provide consistency.
//   2. kv_change_index.go
//      Supports multiple SG nodes as index writers.  Uses vbucket sequence numbers for sequence management,
//      and maintains stable sequence vector clocks to provide consistency.

type ChangeIndex interface {

	// Initialize the index
	Init(context *DatabaseContext, notifyChange func(base.Set), cacheOptions *CacheOptions, indexOptions *ChannelIndexOptions) error

	// Stop the index
	Stop()

	// Start the index
	Start() error

	// Clear the index
	Clear() error

	// Enable/Disable indexing
	EnableChannelIndexing(enable bool)

	// Retrieve changes in a channel
	GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error)

	// Called to add a document to the index
	DocChanged(event sgbucket.FeedEvent)

	// Retrieves stable sequence for index
	GetStableSequence(docID string) SequenceID

	// Retrieves stable sequence for index.  Stale=false forces a reload of the clock from the bucket,
	// stable=true returns cached value (if available)
	GetStableClock(stale bool) (clock base.SequenceClock, err error)

	// Remove purges the given doc IDs and returns the number of items removed
	Remove(docIDs []string, startTime time.Time) (count int)

	// Handling specific to change_cache.go's sequence handling.  Ideally should refactor usage in changes.go to push
	// down into internal change_cache.go handling, but it's non-trivial refactoring
	getOldestSkippedSequence() uint64
	getChannelCache() ChannelCache

	// These methods should block up until maxWaitTime, or until the given sequence has been received by the change cache.
	waitForSequence(ctx context.Context, sequence uint64, maxWaitTime time.Duration) error
	waitForSequenceNotSkipped(ctx context.Context, sequence uint64, maxWaitTime time.Duration) error
}

// Index type
type IndexType uint8

const (
	KVIndex IndexType = iota
	MemoryCache
)

type ChannelIndexOptions struct {
	Type                      IndexType       // Index type
	Spec                      base.BucketSpec // Indexing bucket spec
	Bucket                    base.Bucket     // Indexing bucket
	Writer                    bool            // Cache Writer
	Options                   CacheOptions    // Caching options
	NumShards                 uint16          // The number of CBGT shards
	HashFrequency             uint16          // Hash frequency for changes feeds (in changes entries)
	TombstoneCompactFrequency *int            // Tombstone Compaction frequency (in hours)
}

type SequenceHashOptions struct {
	Bucket        base.Bucket // Hash lookup bucket
	Size          uint8       // Hash keyset size log 2
	Expiry        *uint32     // Expiry for untouched hash bucket docs
	HashFrequency *int        // Hash frequency for changes feed
}

// ChannelIndex defines the API used by the ChangeIndex to interact with the underlying index storage
type ChannelIndex interface {
	Add(entry *LogEntry) error
	AddSet(entries []*LogEntry) error
	GetClock() (uint64, error)
	SetClock() (uint64, error)
	GetCachedChanges(options ChangesOptions, stableSequence uint64)
	Compact()
}

func (entry *LogEntry) IsRemoved() bool {
	return entry.Flags&channels.Removed != 0
}

func (entry *LogEntry) IsDeleted() bool {
	return entry.Flags&channels.Deleted != 0
}

// Returns false if the entry is either a removal or a delete
func (entry *LogEntry) IsActive() bool {
	return !entry.IsRemoved() && !entry.IsDeleted()
}

func (entry *LogEntry) SetRemoved() {
	entry.Flags |= channels.Removed
}

func (entry *LogEntry) SetDeleted() {
	entry.Flags |= channels.Deleted
}

func (c ChannelIndexOptions) ValidateOrPanic() {
	if c.NumShards == 0 {
		base.Panicf(base.KeyAll, "The number of shards must be greater than 0")
	}

	// make sure num_shards is a power of two, or panic
	isPowerOfTwo := base.IsPowerOfTwo(c.NumShards)
	if !isPowerOfTwo {
		errMsg := "Invalid value for num_shards in feed_params: %v Must be a power of 2 so that all shards have the same number of vbuckets"
		base.Panicf(base.KeyAll, errMsg, c.NumShards)
	}

}

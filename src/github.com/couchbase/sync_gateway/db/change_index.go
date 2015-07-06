package db

import (
	"github.com/couchbase/sync_gateway/base"
)

// A ChangeIndex is responsible for indexing incoming events from change_listener, and
// and servicing requests for indexed changes from changes.go.  In addition to basic feed processiw2BV23                                                                                                                                                                                                                                                                                                                                                                                                                                                                             ng, the
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
	Init(context *DatabaseContext, lastSequence SequenceID, onChange func(base.Set), options CacheOptions)

	// Stop the index
	Stop()

	// Clear the index
	Clear()

	// Enable/Disable indexing
	EnableChannelIndexing(enable bool)

	// Retrieve changes in a channel
	GetChangesInChannel(channelName string, options ChangesOptions) ([]*LogEntry, error)

	// Called to add a document to the index
	DocChanged(docID string, docJSON []byte, vbucket uint64)

	// Retrieves stable sequence for index
	GetStableSequence() SequenceID

	// Utility functions for unit testing
	waitForSequenceID(sequence SequenceID)

	// Specific to change_cache.go's sequence handling.  Needs refactored
	getOldestSkippedSequence() uint64
	getChannelCache(channelName string) *channelCache //TODO replace with channel cache
	waitForSequence(sequence uint64)
	waitForSequenceWithMissing(sequence uint64)
}

// Index type
type IndexType uint8

const (
	KVIndex IndexType = iota
	MemoryCache
)

type ChangeIndexOptions struct {
	Type    IndexType    // Index type
	Bucket  base.Bucket  // Indexing bucket
	Options CacheOptions // Caching options
}

// ChannelIndex defines the API used by the ChangeIndex to interact with the underlying index storage
type ChannelIndex interface {
	Add(entry kvIndexEntry) error
	AddSet(entries []kvIndexEntry) error
	GetClock() (uint64, error)
	SetClock() (uint64, error)
	GetCachedChanges(options ChangesOptions, stableSequence uint64)
}

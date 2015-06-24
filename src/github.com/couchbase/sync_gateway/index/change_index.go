package index

import (
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// A ChangeIndex is responsible for indexing incoming events from change_listener, and
// and servicing requests for indexed changes from changes.go.  In addition to basic feed processiw2BV23                                                                                                                                                                                                                                                                                                                                                                                                                                                                             ng, the
// ChangeIndex is the component responsible for index consistency and sequence management.  The ChangeIndex
// doesn't define storage - see ChannelIndex for storage details.
//
// Currently there are two ChangeIndex implementations:
//   1. change_cache.go.
//      Expects a single SG node as index writer.  Uses _sync:seq for sequence management, and buffers
//      incoming sequences from the feed to provide consistency.
//   2. kv_index.go
//      Supports multiple SG nodes as index writers.  Uses vbucket sequence numbers for sequence management,
//      and maintains stable sequence vector clocks to provide consistency.

type ChangeIndex interface {

	// Initialize the index
	Init(context *db.DatabaseContext, lastSequence db.SequenceID, onChange func(base.Set), options db.CacheOptions)

	// Retrieve changes in a channel
	GetChangesInChannel(channelName string, options db.ChangesOptions) ([]*db.LogEntry, error)

	// Utility function for unit testing
	WaitForSequence(sequence db.SequenceID)
}

// Index type
type IndexType uint8

const (
	KVIndex IndexType = iota
	MemoryCache
)

type IndexOptions struct {
	Type    IndexType    // Index type
	Bucket  base.Bucket  // Indexing bucket
	Options CacheOptions // Caching options
}

// Support for in-memory cache
type CacheOptions struct {
	CachePendingSeqMaxWait time.Duration // Max wait for pending sequence before skipping
	CachePendingSeqMaxNum  int           // Max number of pending sequences before skipping
	CacheSkippedSeqMaxWait time.Duration // Max wait for skipped sequence before abandoning
}

type ChannelIndex interface {
}

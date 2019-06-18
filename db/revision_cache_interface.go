package db

import (
	"expvar"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// DefaultRevisionCacheSize is the number of recently-accessed doc revisions to cache in RAM
	DefaultRevisionCacheSize = 5000

	// DefaultRevisionCacheShardCount is the default number of shards to use for the revision cache
	DefaultRevisionCacheShardCount = 8
)

// RevisionCache is an interface that can be used to fetch a DocumentRevision for a Doc ID and Rev ID pair.
type RevisionCache interface {
	// Get returns the given revision, and stores if not already cached
	Get(docID, revID string, copyType BodyCopyType) (DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached
	GetActive(docID string, copyType BodyCopyType) (docRev DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(docID, revID string, copyType BodyCopyType) (docRev DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(docID string, docRev DocumentRevision)

	// UpdateDelta stores the given toDelta value in the given rev if cached
	UpdateDelta(docID, revID string, toDelta *RevisionDelta)
}

// Force compile-time check of all RevisionCache types for interface
var _ RevisionCache = &LRURevisionCache{}
var _ RevisionCache = &ShardedLRURevisionCache{}
var _ RevisionCache = &BypassRevisionCache{}

// NewRevisionCache returns a RevisionCache implementation for the given config options.
func NewRevisionCache(cacheOptions *RevisionCacheOptions, backingStore RevisionCacheBackingStore, statsCache *expvar.Map) RevisionCache {

	// If cacheOptions is not passed in, use defaults
	if cacheOptions == nil {
		cacheOptions = DefaultRevisionCacheOptions()
	}

	if cacheOptions.Size == 0 {
		bypassStat := statsCache.Get(base.StatKeyRevisionCacheBypass).(*expvar.Int)
		return NewBypassRevisionCache(backingStore, bypassStat)
	}

	cacheHitStat := statsCache.Get(base.StatKeyRevisionCacheHits).(*expvar.Int)
	cacheMissStat := statsCache.Get(base.StatKeyRevisionCacheMisses).(*expvar.Int)
	if cacheOptions.ShardCount > 1 {
		return NewShardedLRURevisionCache(cacheOptions.ShardCount, cacheOptions.Size, backingStore, cacheHitStat, cacheMissStat)
	}

	return NewLRURevisionCache(cacheOptions.Size, backingStore, cacheHitStat, cacheMissStat)
}

type RevisionCacheOptions struct {
	Size       uint32
	ShardCount uint16
}

func DefaultRevisionCacheOptions() *RevisionCacheOptions {
	return &RevisionCacheOptions{
		Size:       DefaultRevisionCacheSize,
		ShardCount: DefaultRevisionCacheShardCount,
	}
}

// RevisionCacheBackingStore is the inteface required to be passed into a RevisionCache constructor to provide a backing store for loading documents.
type RevisionCacheBackingStore interface {
	GetDocument(docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *document, err error)
	getRevision(doc *document, revid string) (Body, error)
}

// Revision information as returned by the rev cache
type DocumentRevision struct {
	RevID       string
	Body        Body
	History     Revisions
	Channels    base.Set
	Expiry      *time.Time
	Attachments AttachmentsMeta
	Delta       *RevisionDelta
}

type IDAndRev struct {
	DocID string
	RevID string
}

// RevisionDelta stores data about a delta between a revision and ToRevID.
type RevisionDelta struct {
	ToRevID           string   // Target revID for the delta
	DeltaBytes        []byte   // The actual delta
	AttachmentDigests []string // Digests for all attachments present on ToRevID
	RevisionHistory   []string // Revision history from parent of ToRevID to source revID, in descending order
}

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision) *RevisionDelta {
	return &RevisionDelta{
		ToRevID:           toRevision.RevID,
		DeltaBytes:        deltaBytes,
		AttachmentDigests: AttachmentDigests(toRevision.Attachments), // Flatten the AttachmentsMeta into a list of digests
		RevisionHistory:   toRevision.History.parseAncestorRevisions(fromRevID),
	}
}

// This is the RevisionCacheLoaderFunc callback for the context's RevisionCache.
// Its job is to load a revision from the bucket when there's a cache miss.
func revCacheLoader(backingStore RevisionCacheBackingStore, id IDAndRev) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error) {
	var doc *document
	if doc, err = backingStore.GetDocument(id.DocID, DocUnmarshalAll); doc == nil {
		return body, history, channels, attachments, expiry, err
	}

	return revCacheLoaderForDocument(backingStore, doc, id.RevID)
}

// Common revCacheLoader functionality used either during a cache miss (from revCacheLoader), or directly when retrieving current rev from cache
func revCacheLoaderForDocument(backingStore RevisionCacheBackingStore, doc *document, revid string) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error) {

	if body, err = backingStore.getRevision(doc, revid); err != nil {
		// If we can't find the revision (either as active or conflicted body from the document, or as old revision body backup), check whether
		// the revision was a channel removal.  If so, we want to store as removal in the revision cache
		removalBody, removalHistory, removalChannels, isRemoval, isRemovalErr := doc.IsChannelRemoval(revid)
		if isRemovalErr != nil {
			return body, history, channels, nil, nil, isRemovalErr
		}
		if isRemoval {
			return removalBody, removalHistory, removalChannels, nil, nil, nil
		} else {
			// If this wasn't a removal, return the original error from getRevision
			return body, history, channels, nil, nil, err
		}
	}
	if doc.History[revid].Deleted {
		body[BodyDeleted] = true
	}

	validatedHistory, getHistoryErr := doc.History.getHistory(revid)
	if getHistoryErr != nil {
		return body, history, channels, nil, nil, getHistoryErr
	}
	history = encodeRevisions(validatedHistory)
	channels = doc.History[revid].Channels

	return body, history, channels, doc.Attachments, doc.Expiry, err
}

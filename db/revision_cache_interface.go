package db

import (
	"encoding/json"
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
	Get(docID, revID string) (*DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached
	GetActive(docID string) (docRev *DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(docID, revID string) (docRev *DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(docRev DocumentRevision)

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

// RevisionCacheBackingStore is the interface required to be passed into a RevisionCache constructor to provide a backing store for loading documents.
type RevisionCacheBackingStore interface {
	GetDocument(docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error)
	getRevision(doc *Document, revid string) ([]byte, error)
}

// DocumentRevision stored and returned by the rev cache
type DocumentRevision struct {
	DocID       string
	RevID       string
	BodyBytes   []byte
	History     Revisions
	Channels    base.Set
	Expiry      *time.Time
	Attachments AttachmentsMeta
	Delta       *RevisionDelta
	Deleted     bool
}

// MutableBody returns a copy of the given document revision as a plain body (without any special properties)
// Callers are free to modify this body without affecting the document revision.
func (rev *DocumentRevision) MutableBody() (b Body, err error) {
	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}
	return b, nil
}

// Mutable1xBody returns a copy of the given document revision as a 1.x style body (with special properties)
// Callers are free to modify this body without affecting the document revision.
func (rev *DocumentRevision) Mutable1xBody(db *Database, requestedHistory Revisions, attachmentsSince []string, showExp bool) (b Body, err error) {
	b, err = rev.MutableBody()
	if err != nil {
		return nil, err
	}

	b[BodyId] = rev.DocID
	b[BodyRev] = rev.RevID

	// Add revision metadata:
	if requestedHistory != nil {
		b[BodyRevisions] = requestedHistory
	}

	if showExp && rev.Expiry != nil && !rev.Expiry.IsZero() {
		b[BodyExpiry] = rev.Expiry.Format(time.RFC3339)
	}

	if rev.Deleted {
		b[BodyDeleted] = true
	}

	// Add attachment data if requested:
	if attachmentsSince != nil {
		if len(rev.Attachments) > 0 {
			minRevpos := 1
			if len(attachmentsSince) > 0 {
				ancestor := rev.History.findAncestor(attachmentsSince)
				if ancestor != "" {
					minRevpos, _ = ParseRevID(ancestor)
					minRevpos++
				}
			}
			bodyAtts, err := db.loadAttachmentsData(rev.Attachments, minRevpos, rev.DocID)
			if err != nil {
				return nil, err
			}
			b[BodyAttachments] = bodyAtts
		}
	} else if rev.Attachments != nil {
		// Stamp attachment metadata back into the body
		b[BodyAttachments] = rev.Attachments
	}

	return b, nil
}

// As1xBytes returns a byte slice representing the 1.x style body, containing special properties (i.e. _id, _rev, _attachments, etc.)
func (rev *DocumentRevision) As1xBytes(db *Database, requestedHistory Revisions, attachmentsSince []string, showExp bool) (b []byte, err error) {
	// unmarshal
	body1x, err := rev.Mutable1xBody(db, requestedHistory, attachmentsSince, showExp)
	if err != nil {
		return nil, err
	}

	// TODO: We could avoid the unmarshal -> marshal work here by injecting properties into the original body bytes directly.
	return json.Marshal(body1x)
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
	ToChannels        base.Set // Full list of channels for the to revision
	RevisionHistory   []string // Revision history from parent of ToRevID to source revID, in descending order
	ToDeleted         bool     // Flag if ToRevID is a tombstone
}

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision *DocumentRevision, deleted bool) *RevisionDelta {
	return &RevisionDelta{
		ToRevID:           toRevision.RevID,
		DeltaBytes:        deltaBytes,
		AttachmentDigests: AttachmentDigests(toRevision.Attachments), // Flatten the AttachmentsMeta into a list of digests
		ToChannels:        toRevision.Channels,
		RevisionHistory:   toRevision.History.parseAncestorRevisions(fromRevID),
		ToDeleted:         deleted,
	}
}

// This is the RevisionCacheLoaderFunc callback for the context's RevisionCache.
// Its job is to load a revision from the bucket when there's a cache miss.
func revCacheLoader(backingStore RevisionCacheBackingStore, id IDAndRev) (bodyBytes []byte, history Revisions, channels base.Set, attachments AttachmentsMeta, deleted bool, expiry *time.Time, err error) {
	var doc *Document
	if doc, err = backingStore.GetDocument(id.DocID, DocUnmarshalAll); doc == nil {
		return bodyBytes, history, channels, attachments, deleted, expiry, err
	}

	return revCacheLoaderForDocument(backingStore, doc, id.RevID)
}

// Common revCacheLoader functionality used either during a cache miss (from revCacheLoader), or directly when retrieving current rev from cache
func revCacheLoaderForDocument(backingStore RevisionCacheBackingStore, doc *Document, revid string) (bodyBytes []byte, history Revisions, channels base.Set, attachments AttachmentsMeta, deleted bool, expiry *time.Time, err error) {

	if bodyBytes, err = backingStore.getRevision(doc, revid); err != nil {
		// If we can't find the revision (either as active or conflicted body from the document, or as old revision body backup), check whether
		// the revision was a channel removal.  If so, we want to store as removal in the revision cache
		removalBodyBytes, removalHistory, removalChannels, isRemoval, _, isRemovalErr := doc.IsChannelRemoval(revid)
		if isRemovalErr != nil {
			return bodyBytes, history, channels, nil, false, nil, isRemovalErr
		}
		if isRemoval {
			return removalBodyBytes, removalHistory, removalChannels, nil, false, nil, nil
		} else {
			// If this wasn't a removal, return the original error from getRevision
			return bodyBytes, history, channels, nil, false, nil, err
		}
	}
	deleted = doc.History[revid].Deleted

	validatedHistory, getHistoryErr := doc.History.getHistory(revid)
	if getHistoryErr != nil {
		return bodyBytes, history, channels, nil, deleted, nil, getHistoryErr
	}
	history = encodeRevisions(validatedHistory)
	channels = doc.History[revid].Channels

	return bodyBytes, history, channels, doc.Attachments, deleted, doc.Expiry, err
}

package db

import (
	"encoding/json"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// DefaultRevisionCacheSize is the number of recently-accessed doc revisions to cache in RAM
	DefaultRevisionCacheSize = 5000

	// DefaultRevisionCacheShardCount is the default number of shards to use for the revision cache
	DefaultRevisionCacheShardCount = 16
)

// RevisionCache is an interface that can be used to fetch a DocumentRevision for a Doc ID and Rev ID pair.
type RevisionCache interface {
	// Get returns the given revision, and stores if not already cached.
	// When includeBody=true, the returned DocumentRevision will include a mutable shallow copy of the marshaled body.
	// When includeDelta=true, the returned DocumentRevision will include delta - requires additional locking during retrieval.
	Get(docID, revID string, includeBody bool, includeDelta bool) (DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached.
	// When includeBody=true, the returned DocumentRevision will include a mutable shallow copy of the marshaled body.
	GetActive(docID string, includeBody bool) (docRev DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(docID, revID string) (docRev DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(docRev DocumentRevision)

	// UpdateDelta stores the given toDelta value in the given rev if cached
	UpdateDelta(docID, revID string, toDelta RevisionDelta)
}

const (
	RevCacheIncludeBody  = true
	RevCacheOmitBody     = false
	RevCacheIncludeDelta = true
	RevCacheOmitDelta    = false
)

// Force compile-time check of all RevisionCache types for interface
var _ RevisionCache = &LRURevisionCache{}
var _ RevisionCache = &ShardedLRURevisionCache{}
var _ RevisionCache = &BypassRevisionCache{}

// NewRevisionCache returns a RevisionCache implementation for the given config options.
func NewRevisionCache(cacheOptions *RevisionCacheOptions, backingStore RevisionCacheBackingStore, cacheStats *base.CacheStats) RevisionCache {

	// If cacheOptions is not passed in, use defaults
	if cacheOptions == nil {
		cacheOptions = DefaultRevisionCacheOptions()
	}

	if cacheOptions.Size == 0 {
		bypassStat := cacheStats.RevisionCacheBypass
		return NewBypassRevisionCache(backingStore, bypassStat)
	}

	cacheHitStat := cacheStats.RevisionCacheHits
	cacheMissStat := cacheStats.RevisionCacheMisses

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
	getRevision(doc *Document, revid string) ([]byte, Body, AttachmentsMeta, error)
}

// DocumentRevision stored and returned by the rev cache
type DocumentRevision struct {
	DocID string
	RevID string
	// BodyBytes contains the raw document, with no special properties.
	BodyBytes   []byte
	History     Revisions
	Channels    base.Set
	Expiry      *time.Time
	Attachments AttachmentsMeta
	Delta       *RevisionDelta
	Deleted     bool

	_shallowCopyBody Body // an unmarshalled body that can produce shallow copies
}

// DeepMutableBody returns a deep copy of the given document revision as a plain body (without any special properties)
// Callers are free to modify any of this body without affecting the document revision.
func (rev *DocumentRevision) DeepMutableBody() (b Body, err error) {
	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}

	return b, nil
}

// MutableBody returns a shallow copy of the given document revision as a plain body (without any special properties)
// Callers are only free to modify top-level properties of this body without affecting the document revision.
func (rev *DocumentRevision) MutableBody() (b Body, err error) {
	// if we already have an unmarshalled body, take a copy and return it
	if rev._shallowCopyBody != nil {
		return rev._shallowCopyBody, nil
	}

	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}

	// store a copy of the unmarshalled body for next time we need it
	rev._shallowCopyBody = b

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

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision, deleted bool) RevisionDelta {
	return RevisionDelta{
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
func revCacheLoader(backingStore RevisionCacheBackingStore, id IDAndRev, unmarshalBody bool) (bodyBytes []byte, body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, deleted bool, expiry *time.Time, err error) {
	var doc *Document
	unmarshalLevel := DocUnmarshalSync
	if unmarshalBody {
		unmarshalLevel = DocUnmarshalAll
	}
	if doc, err = backingStore.GetDocument(id.DocID, unmarshalLevel); doc == nil {
		return bodyBytes, body, history, channels, attachments, deleted, expiry, err
	}

	return revCacheLoaderForDocument(backingStore, doc, id.RevID)
}

// Common revCacheLoader functionality used either during a cache miss (from revCacheLoader), or directly when retrieving current rev from cache
func revCacheLoaderForDocument(backingStore RevisionCacheBackingStore, doc *Document, revid string) (bodyBytes []byte, body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, deleted bool, expiry *time.Time, err error) {
	if bodyBytes, body, attachments, err = backingStore.getRevision(doc, revid); err != nil {
		// If we can't find the revision (either as active or conflicted body from the document, or as old revision body backup), check whether
		// the revision was a channel removal.  If so, we want to store as removal in the revision cache
		removalBodyBytes, removalHistory, removalChannels, isRemoval, isDelete, isRemovalErr := doc.IsChannelRemoval(revid)
		if isRemovalErr != nil {
			return bodyBytes, body, history, channels, nil, isDelete, nil, isRemovalErr
		}

		if isRemoval {
			return removalBodyBytes, body, removalHistory, removalChannels, nil, isDelete, nil, nil
		} else {
			// If this wasn't a removal, return the original error from getRevision
			return bodyBytes, body, history, channels, nil, isDelete, nil, err
		}
	}

	deleted = doc.History[revid].Deleted

	validatedHistory, getHistoryErr := doc.History.getHistory(revid)
	if getHistoryErr != nil {
		return bodyBytes, body, history, channels, nil, deleted, nil, getHistoryErr
	}
	history = encodeRevisions(validatedHistory)
	channels = doc.History[revid].Channels

	return bodyBytes, body, history, channels, attachments, deleted, doc.Expiry, err
}

/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
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
	// GetWithRev returns the given revision, and stores if not already cached.
	// When includeBody=true, the returned DocumentRevision will include a mutable shallow copy of the marshaled body.
	// When includeDelta=true, the returned DocumentRevision will include delta - requires additional locking during retrieval.
	GetWithRev(ctx context.Context, docID, revID string, includeBody, includeDelta bool) (DocumentRevision, error)

	// GetWithCV returns the given revision by the CV, and stores if not already cached.
	// When includeBody=true, the returned DocumentRevision will include a mutable shallow copy of the marshaled body.
	// When includeDelta=true, the returned DocumentRevision will include delta - requires additional locking during retrieval.
	GetWithCV(ctx context.Context, docID string, cv *CurrentVersionVector, includeBody, includeDelta bool) (DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached.
	// When includeBody=true, the returned DocumentRevision will include a mutable shallow copy of the marshaled body.
	GetActive(ctx context.Context, docID string, includeBody bool) (docRev DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(ctx context.Context, docID, revID string) (docRev DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(ctx context.Context, docRev DocumentRevision)

	// Update will remove existing value and re-create new one
	Upsert(ctx context.Context, docRev DocumentRevision)

	// RemoveWithRev eliminates a revision in the cache using its revID.
	RemoveWithRev(docID, revID string)

	// RemoveWithCV eliminates a revision in the cache using its current version.
	RemoveWithCV(docID string, cv *CurrentVersionVector)

	// UpdateDelta stores the given toDelta value in the given rev if cached
	UpdateDelta(ctx context.Context, docID, revID string, toDelta RevisionDelta)
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
	GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error)
	getRevision(ctx context.Context, doc *Document, revid string) ([]byte, Body, AttachmentsMeta, error)
	getCurrentVersion(ctx context.Context, doc *Document) ([]byte, Body, AttachmentsMeta, error)
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
	Removed     bool // True if the revision is a removal.
	CV          *CurrentVersionVector

	_shallowCopyBody Body // an unmarshalled body that can produce shallow copies
}

// MutableBody returns a deep copy of the given document revision as a plain body (without any special properties)
// Callers are free to modify any of this body without affecting the document revision.
func (rev *DocumentRevision) MutableBody() (b Body, err error) {
	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}

	return b, nil
}

// Body returns an unmarshalled body that is kept in the document revision to produce shallow copies.
// If an unmarshalled copy is not available in the document revision, it makes a copy from the raw body
// bytes and stores it in document revision itself before returning the body.
func (rev *DocumentRevision) Body() (b Body, err error) {
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
func (rev *DocumentRevision) Mutable1xBody(ctx context.Context, db *DatabaseCollectionWithUser, requestedHistory Revisions, attachmentsSince []string, showExp bool) (b Body, err error) {
	b, err = rev.Body()
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, base.RedactErrorf("null doc body for docID: %s revID: %s", base.UD(rev.DocID), base.UD(rev.RevID))
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
					minRevpos, _ = ParseRevID(ctx, ancestor)
					minRevpos++
				}
			}
			bodyAtts, err := db.loadAttachmentsData(rev.Attachments, minRevpos, rev.DocID)
			if err != nil {
				return nil, err
			}
			DeleteAttachmentVersion(bodyAtts)
			b[BodyAttachments] = bodyAtts
		}
	} else if rev.Attachments != nil {
		// Stamp attachment metadata back into the body
		DeleteAttachmentVersion(rev.Attachments)
		b[BodyAttachments] = rev.Attachments
	}

	return b, nil
}

// As1xBytes returns a byte slice representing the 1.x style body, containing special properties (i.e. _id, _rev, _attachments, etc.)
func (rev *DocumentRevision) As1xBytes(ctx context.Context, db *DatabaseCollectionWithUser, requestedHistory Revisions, attachmentsSince []string, showExp bool) (b []byte, err error) {
	// unmarshal
	body1x, err := rev.Mutable1xBody(ctx, db, requestedHistory, attachmentsSince, showExp)
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

type IDandCV struct {
	DocID   string
	Version uint64
	Source  string
}

// RevisionDelta stores data about a delta between a revision and ToRevID.
type RevisionDelta struct {
	ToRevID               string                  // Target revID for the delta
	DeltaBytes            []byte                  // The actual delta
	AttachmentStorageMeta []AttachmentStorageMeta // Storage metadata of all attachments present on ToRevID
	ToChannels            base.Set                // Full list of channels for the to revision
	RevisionHistory       []string                // Revision history from parent of ToRevID to source revID, in descending order
	ToDeleted             bool                    // Flag if ToRevID is a tombstone
}

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision, deleted bool, toRevAttStorageMeta []AttachmentStorageMeta) RevisionDelta {
	return RevisionDelta{
		ToRevID:               toRevision.RevID,
		DeltaBytes:            deltaBytes,
		AttachmentStorageMeta: toRevAttStorageMeta,
		ToChannels:            toRevision.Channels,
		RevisionHistory:       toRevision.History.parseAncestorRevisions(fromRevID),
		ToDeleted:             deleted,
	}
}

// This is the RevisionCacheLoaderFunc callback for the context's RevisionCache.
// Its job is to load a revision from the bucket when there's a cache miss.
func revCacheLoader(ctx context.Context, backingStore RevisionCacheBackingStore, id IDAndRev, unmarshalBody bool) (bodyBytes []byte, body Body, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, fetchedCV CurrentVersionVector, err error) {
	var doc *Document
	unmarshalLevel := DocUnmarshalSync
	if unmarshalBody {
		unmarshalLevel = DocUnmarshalAll
	}
	if doc, err = backingStore.GetDocument(ctx, id.DocID, unmarshalLevel); doc == nil {
		return bodyBytes, body, history, channels, removed, attachments, deleted, expiry, fetchedCV, err
	}

	return revCacheLoaderForDocument(ctx, backingStore, doc, id.RevID)
}

// revCacheLoaderForCv will load a document from the bucket using the CV, comapre the fetched doc and the CV specified in the function,
// and will still return revid for purpose of populating the Rev ID lookup map on the cache
func revCacheLoaderForCv(ctx context.Context, backingStore RevisionCacheBackingStore, id IDandCV, unmarshalBody bool) (bodyBytes []byte, body Body, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, revid string, err error) {
	cv := CurrentVersionVector{
		VersionCAS: id.Version,
		SourceID:   id.Source,
	}
	var doc *Document
	unmarshalLevel := DocUnmarshalSync
	if unmarshalBody {
		unmarshalLevel = DocUnmarshalAll
	}
	if doc, err = backingStore.GetDocument(ctx, id.DocID, unmarshalLevel); doc == nil {
		return bodyBytes, body, history, channels, removed, attachments, deleted, expiry, revid, err
	}
	// nil check to be panic safe
	if doc.HLV != nil {
		// fetch the current version for the loaded doc and compare against the CV specified in the IDandCV key
		fetchedDocSource, fetchedDocVersion := doc.HLV.GetCurrentVersion()
		if fetchedDocSource != cv.SourceID || fetchedDocVersion != cv.VersionCAS {
			return bodyBytes, body, history, channels, removed, attachments, deleted, expiry, revid, base.RedactErrorf("mismatch between specified current version and fetched document current version for doc %s", base.UD(id.DocID))
		}
	} else {
		return bodyBytes, body, history, channels, removed, attachments, deleted, expiry, revid, base.RedactErrorf("mismatch between specified current version and fetched document current version for doc %s", base.UD(id.DocID))
	}

	return revCacheLoaderForDocumentCV(ctx, backingStore, doc, cv)
}

// Common revCacheLoader functionality used either during a cache miss (from revCacheLoader), or directly when retrieving current rev from cache
func revCacheLoaderForDocument(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document, revid string) (bodyBytes []byte, body Body, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, fetchedCV CurrentVersionVector, err error) {
	if bodyBytes, body, attachments, err = backingStore.getRevision(ctx, doc, revid); err != nil {
		// If we can't find the revision (either as active or conflicted body from the document, or as old revision body backup), check whether
		// the revision was a channel removal. If so, we want to store as removal in the revision cache
		removalBodyBytes, removalHistory, activeChannels, isRemoval, isDelete, isRemovalErr := doc.IsChannelRemoval(ctx, revid)
		if isRemovalErr != nil {
			return bodyBytes, body, history, channels, isRemoval, nil, isDelete, nil, fetchedCV, isRemovalErr
		}

		if isRemoval {
			return removalBodyBytes, body, removalHistory, activeChannels, isRemoval, nil, isDelete, nil, fetchedCV, nil
		} else {
			// If this wasn't a removal, return the original error from getRevision
			return bodyBytes, body, history, channels, removed, nil, isDelete, nil, fetchedCV, err
		}
	}
	deleted = doc.History[revid].Deleted

	validatedHistory, getHistoryErr := doc.History.getHistory(revid)
	if getHistoryErr != nil {
		return bodyBytes, body, history, channels, removed, nil, deleted, nil, fetchedCV, getHistoryErr
	}
	history = encodeRevisions(ctx, doc.ID, validatedHistory)
	channels = doc.History[revid].Channels
	if doc.HLV != nil {
		fetchedCV = CurrentVersionVector{SourceID: doc.HLV.SourceID, VersionCAS: doc.HLV.Version}
	}

	return bodyBytes, body, history, channels, removed, attachments, deleted, doc.Expiry, fetchedCV, err
}

// revCacheLoaderForDocumentCV used either during cache miss (from revCacheLoaderForCv), or used directly when getting current active CV from cache
func revCacheLoaderForDocumentCV(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document, cv CurrentVersionVector) (bodyBytes []byte, body Body, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, revid string, err error) {
	if bodyBytes, body, attachments, err = backingStore.getCurrentVersion(ctx, doc); err != nil {
		// we need implementation of IsChannelRemoval for CV here.
		// pending CBG-3213 support of channel removal for CV
	}
	channels = doc.currentChannels()
	revid = doc.CurrentRev

	return bodyBytes, body, history, channels, removed, attachments, deleted, doc.Expiry, revid, err
}

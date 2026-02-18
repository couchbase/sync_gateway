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
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// DefaultRevisionCacheSize is the number of recently-accessed doc revisions to cache in RAM
	DefaultRevisionCacheSize uint32 = 5000

	// DefaultRevisionCacheShardCount is the default number of shards to use for the revision cache
	DefaultRevisionCacheShardCount uint16 = 16
)

// RevisionCache is an interface that can be used to fetch a DocumentRevision for a Doc ID and Rev ID pair.
type RevisionCache interface {

	// GetWithRev returns the given revision, and stores if not already cached.
	// When includeDelta=true, the returned DocumentRevision will include delta - requires additional locking during retrieval.
	GetWithRev(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (DocumentRevision, error)

	// GetWithCV returns the given revision by CV, and stores if not already cached.
	// When includeDelta=true, the returned DocumentRevision will include delta - requires additional locking during retrieval.
	// When loadBackup=true, will load from backup revisions if requested version is not active document
	GetWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32, includeDelta bool, loadBackup bool) (DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached.
	GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(ctx context.Context, docRev DocumentRevision, collectionID uint32)

	// Upsert will remove existing value and re-create new one
	Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32)

	// RemoveWithRev evicts a revision from the cache using its revID.
	RemoveWithRev(ctx context.Context, docID, revID string, collectionID uint32)

	// RemoveWithCV evicts a revision from the cache using its current version.
	RemoveWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32)

	// RemoveRevOnly removes the specified key from the revID lookup map in the cache
	RemoveRevOnly(ctx context.Context, docID, revID string, collectionID uint32)

	// RemoveCVOnly removes the specified key from the HLV lookup map in the cache
	RemoveCVOnly(ctx context.Context, docID string, cv *Version, collectionID uint32)

	// UpdateDelta stores the given toDelta value in the given rev if cached
	UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta)

	// UpdateDeltaCV stores the given toDelta value in the given rev if cached but will look up in cache by cv
	UpdateDeltaCV(ctx context.Context, docID string, cv *Version, collectionID uint32, toDelta RevisionDelta)
}

const (
	RevCacheIncludeDelta = true
	RevCacheOmitDelta    = false
)

// Force compile-time check of all RevisionCache types for interface
var _ RevisionCache = &LRURevisionCache{}
var _ RevisionCache = &ShardedLRURevisionCache{}
var _ RevisionCache = &BypassRevisionCache{}

// NewRevisionCache returns a RevisionCache implementation for the given config options.
func NewRevisionCache(cacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, cacheStats *base.CacheStats) RevisionCache {

	// If cacheOptions is not passed in, use defaults
	if cacheOptions == nil {
		cacheOptions = DefaultRevisionCacheOptions()
	}

	if cacheOptions.MaxItemCount == 0 {
		bypassStat := cacheStats.RevisionCacheBypass
		return NewBypassRevisionCache(backingStores, bypassStat)
	}

	cacheHitStat := cacheStats.RevisionCacheHits
	cacheMissStat := cacheStats.RevisionCacheMisses
	cacheNumItemsStat := cacheStats.RevisionCacheNumItems
	cacheMemoryStat := cacheStats.RevisionCacheTotalMemory
	if cacheNumItemsStat.Value() != 0 {
		cacheNumItemsStat.Set(0)
	}
	if cacheMemoryStat.Value() != 0 {
		cacheMemoryStat.Set(0)
	}

	if cacheOptions.ShardCount > 1 {
		return NewShardedLRURevisionCache(cacheOptions, backingStores, cacheHitStat, cacheMissStat, cacheNumItemsStat, cacheMemoryStat)
	}

	return NewLRURevisionCache(cacheOptions, backingStores, cacheHitStat, cacheMissStat, cacheNumItemsStat, cacheMemoryStat)
}

type RevisionCacheOptions struct {
	MaxItemCount  uint32
	MaxBytes      int64
	ShardCount    uint16
	InsertOnWrite bool
}

func DefaultRevisionCacheOptions() *RevisionCacheOptions {
	return &RevisionCacheOptions{
		MaxItemCount: DefaultRevisionCacheSize,
		ShardCount:   DefaultRevisionCacheShardCount,
	}
}

// RevisionCacheBackingStore is the interface required to be passed into a RevisionCache constructor to provide a backing store for loading documents.
type RevisionCacheBackingStore interface {
	GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error)
	getRevision(ctx context.Context, doc *Document, revid string) ([]byte, AttachmentsMeta, base.Set, error)
	getCurrentVersion(ctx context.Context, doc *Document, cv Version, loadBackup bool) ([]byte, AttachmentsMeta, base.Set, bool, error)
}

// collectionRevisionCache is a view of a revision cache for a collection.
type collectionRevisionCache struct {
	revCache     *RevisionCache
	collectionID uint32
}

// NewCollectionRevisionCache returns a view of a revision cache for a collection.
func newCollectionRevisionCache(revCache *RevisionCache, collectionID uint32) collectionRevisionCache {
	return collectionRevisionCache{
		revCache:     revCache,
		collectionID: collectionID,
	}
}

// Get is for per collection access to Get method
func (c *collectionRevisionCache) GetWithRev(ctx context.Context, docID, revID string, includeDelta bool) (DocumentRevision, error) {
	return (*c.revCache).GetWithRev(ctx, docID, revID, c.collectionID, includeDelta)
}

// Get is for per collection access to Get method
func (c *collectionRevisionCache) GetWithCV(ctx context.Context, docID string, cv *Version, includeDelta bool, loadBackup bool) (DocumentRevision, error) {
	return (*c.revCache).GetWithCV(ctx, docID, cv, c.collectionID, includeDelta, loadBackup)
}

// GetActive is for per collection access to GetActive method
func (c *collectionRevisionCache) GetActive(ctx context.Context, docID string) (DocumentRevision, error) {
	return (*c.revCache).GetActive(ctx, docID, c.collectionID)
}

// Peek is for per collection access to Peek method
func (c *collectionRevisionCache) Peek(ctx context.Context, docID, revID string) (DocumentRevision, bool) {
	return (*c.revCache).Peek(ctx, docID, revID, c.collectionID)
}

// Put is for per collection access to Put method
func (c *collectionRevisionCache) Put(ctx context.Context, docRev DocumentRevision) {
	(*c.revCache).Put(ctx, docRev, c.collectionID)
}

// Upsert is for per collection access to Upsert method
func (c *collectionRevisionCache) Upsert(ctx context.Context, docRev DocumentRevision) {
	(*c.revCache).Upsert(ctx, docRev, c.collectionID)
}

// RemoveWithRev is for per collection access to Remove method
func (c *collectionRevisionCache) RemoveWithRev(ctx context.Context, docID, revID string) {
	(*c.revCache).RemoveWithRev(ctx, docID, revID, c.collectionID)
}

func (c *collectionRevisionCache) RemoveRevOnly(ctx context.Context, docID, revID string) {
	(*c.revCache).RemoveRevOnly(ctx, docID, revID, c.collectionID)
}

func (c *collectionRevisionCache) RemoveCVOnly(ctx context.Context, docID string, cv *Version) {
	(*c.revCache).RemoveCVOnly(ctx, docID, cv, c.collectionID)
}

// RemoveWithCV is for per collection access to Remove method
func (c *collectionRevisionCache) RemoveWithCV(ctx context.Context, docID string, cv *Version) {
	(*c.revCache).RemoveWithCV(ctx, docID, cv, c.collectionID)
}

// UpdateDelta is for per collection access to UpdateDelta method
func (c *collectionRevisionCache) UpdateDelta(ctx context.Context, docID, revID string, toDelta RevisionDelta) {
	(*c.revCache).UpdateDelta(ctx, docID, revID, c.collectionID, toDelta)
}

// UpdateDeltaCV is for per collection access to UpdateDeltaCV method
func (c *collectionRevisionCache) UpdateDeltaCV(ctx context.Context, docID string, cv *Version, toDelta RevisionDelta) {
	(*c.revCache).UpdateDeltaCV(ctx, docID, cv, c.collectionID, toDelta)
}

// DocumentRevision stored and returned by the rev cache
type DocumentRevision struct {
	DocID string
	RevID string
	// BodyBytes contains the raw document, with no special properties.
	BodyBytes              []byte
	History                Revisions
	Channels               base.Set
	Expiry                 *time.Time
	Attachments            AttachmentsMeta
	Delta                  *RevisionDelta
	RevCacheValueDeltaLock *sync.Mutex // shared mutex for the revcache value to avoid concurrent delta generation
	Deleted                bool
	Removed                bool  // True if the revision is a removal.
	MemoryBytes            int64 // storage of the doc rev bytes measurement, includes size of delta when present too
	CV                     *Version
	HlvHistory             string
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

	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}

	return b, nil
}

// Inject1xBodyProperties will inject special properties (_rev etc) into document body avoiding unnecessary marshal work
func (rev *DocumentRevision) Inject1xBodyProperties(ctx context.Context, db *DatabaseCollectionWithUser, requestedHistory Revisions, attachmentsSince []string, showExp bool) ([]byte, error) {

	kvPairs := []base.KVPair{
		{Key: BodyId, Val: rev.DocID},
		{Key: BodyRev, Val: rev.RevID},
	}

	if rev.CV != nil {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyCV, Val: rev.CV.String()})
	}

	if requestedHistory != nil {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyRevisions, Val: requestedHistory})
	}

	if showExp && rev.Expiry != nil && !rev.Expiry.IsZero() {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyExpiry, Val: rev.Expiry.Format(time.RFC3339)})
	}

	if rev.Deleted {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: rev.Deleted})
	}

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
			kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: bodyAtts})
		}
	} else if rev.Attachments != nil {
		// Stamp attachment metadata back into the body
		DeleteAttachmentVersion(rev.Attachments)
		kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: rev.Attachments})
	}

	newBytes, err := base.InjectJSONProperties(rev.BodyBytes, kvPairs...)
	if err != nil {
		return nil, err
	}
	return newBytes, nil
}

// Mutable1xBody returns a copy of the given document revision as a 1.x style body (with special properties)
// Callers are free to modify this body without affecting the document revision.
func (rev *DocumentRevision) Mutable1xBody(ctx context.Context, db *DatabaseCollectionWithUser, requestedHistory Revisions, attachmentsSince []string, showExp bool, showCV bool) (b Body, err error) {
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

	if showCV && rev.CV != nil && !rev.CV.IsEmpty() {
		b[BodyCV] = rev.CV.String()
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
	// inject the special properties
	body1x, err := rev.Inject1xBodyProperties(ctx, db, requestedHistory, attachmentsSince, showExp)
	if err != nil {
		return nil, err
	}

	return body1x, nil
}

type IDAndRev struct {
	DocID        string
	RevID        string
	CollectionID uint32
}

type IDandCV struct {
	DocID        string
	Version      uint64
	Source       string
	CollectionID uint32
}

// RevisionDelta stores data about a delta between a revision and ToRevID.
type RevisionDelta struct {
	ToRevID               string                  // Target revID for the delta
	ToCV                  string                  // Target CV for the delta
	DeltaBytes            []byte                  // The actual delta
	AttachmentStorageMeta []AttachmentStorageMeta // Storage metadata of all attachments present on ToRevID
	ToChannels            base.Set                // Full list of channels for the to revision
	RevisionHistory       []string                // Revision history from parent of ToRevID to source revID, in descending order
	HlvHistory            string                  // HLV History in CBL format
	ToDeleted             bool                    // Flag if ToRevID is a tombstone
	totalDeltaBytes       int64                   // totalDeltaBytes is the total bytes for channels, revisions and body on the delta itself
}

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision, deleted bool, toRevAttStorageMeta []AttachmentStorageMeta) RevisionDelta {
	revDelta := RevisionDelta{
		ToRevID:               toRevision.RevID,
		DeltaBytes:            deltaBytes,
		AttachmentStorageMeta: toRevAttStorageMeta,
		ToChannels:            toRevision.Channels,
		RevisionHistory:       toRevision.History.parseAncestorRevisions(fromRevID),
		HlvHistory:            toRevision.HlvHistory,
		ToDeleted:             deleted,
	}
	if toRevision.CV != nil {
		revDelta.ToCV = toRevision.CV.String()
	}
	revDelta.CalculateDeltaBytes()
	return revDelta
}

// This is the RevisionCacheLoaderFunc callback for the context's RevisionCache.
// Its job is to load a revision from the bucket when there's a cache miss.
func revCacheLoader(ctx context.Context, backingStore RevisionCacheBackingStore, id IDAndRev) (bodyBytes []byte, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, hlv *HybridLogicalVector, err error) {
	var doc *Document
	if doc, err = backingStore.GetDocument(ctx, id.DocID, DocUnmarshalSync); doc == nil {
		return bodyBytes, history, channels, removed, attachments, deleted, expiry, hlv, err
	}
	return revCacheLoaderForDocument(ctx, backingStore, doc, id.RevID)
}

// revCacheLoaderForCv will load a document from the bucket using the CV, compare the fetched doc and the CV specified in the function,
// and will still return revid for purpose of populating the Rev ID lookup map on the cache
func revCacheLoaderForCv(ctx context.Context, backingStore RevisionCacheBackingStore, id IDandCV, loadBackup bool) (bodyBytes []byte, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, revid string, hlv *HybridLogicalVector, err error) {
	cv := Version{
		Value:    id.Version,
		SourceID: id.Source,
	}
	var doc *Document
	if doc, err = backingStore.GetDocument(ctx, id.DocID, DocUnmarshalSync); doc == nil {
		return bodyBytes, history, channels, removed, attachments, deleted, expiry, revid, hlv, err
	}

	return revCacheLoaderForDocumentCV(ctx, backingStore, doc, cv, loadBackup)
}

// Common revCacheLoader functionality used either during a cache miss (from revCacheLoader), or directly when retrieving current rev from cache

func revCacheLoaderForDocument(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document, revid string) (bodyBytes []byte, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, hlv *HybridLogicalVector, err error) {
	if bodyBytes, attachments, channels, err = backingStore.getRevision(ctx, doc, revid); err != nil {
		// If we can't find the revision (either as active or conflicted body from the document, or as old revision body backup), check whether
		// the revision was a channel removal. If so, we want to store as removal in the revision cache
		removalBodyBytes, removalHistory, activeChannels, isRemoval, isDelete, isRemovalErr := doc.IsChannelRemoval(ctx, revid)
		if isRemovalErr != nil {
			return bodyBytes, history, channels, isRemoval, nil, isDelete, nil, hlv, isRemovalErr
		}

		if isRemoval {
			return removalBodyBytes, removalHistory, activeChannels, isRemoval, nil, isDelete, nil, hlv, nil
		}

		// If this wasn't a removal, return the original error from getRevision
		return bodyBytes, history, channels, removed, nil, isDelete, nil, hlv, err
	}
	deleted = doc.History[revid].Deleted

	validatedHistory, getHistoryErr := doc.History.getHistory(revid)
	if getHistoryErr != nil {
		return bodyBytes, history, channels, removed, nil, deleted, nil, hlv, getHistoryErr
	}
	history = encodeRevisions(ctx, doc.ID, validatedHistory)
	// only add doc hlv if the revision we have fetched is current revision, otherwise we don't know whether hlv applies to that revision
	if doc.GetRevTreeID() == revid {
		if doc.HLV != nil {
			hlv = doc.HLV
		}
	}

	return bodyBytes, history, channels, removed, attachments, deleted, doc.Expiry, hlv, err
}

// revCacheLoaderForDocumentCV used either during cache miss (from revCacheLoaderForCv), or used directly when getting current active CV from cache
// nolint:staticcheck
func revCacheLoaderForDocumentCV(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document, cv Version, loadBackup bool) (bodyBytes []byte, history Revisions, channels base.Set, removed bool, attachments AttachmentsMeta, deleted bool, expiry *time.Time, revid string, hlv *HybridLogicalVector, err error) {
	if bodyBytes, attachments, channels, deleted, err = backingStore.getCurrentVersion(ctx, doc, cv, loadBackup); err != nil {
		return nil, nil, nil, false, nil, false, nil, "", nil, err
	}

	// if we have request current version on the doc we can add revision ID too. If not we cannot know what the
	// corresponding revID is to pair with the request CV
	if doc.HLV.ExtractCurrentVersionFromHLV().Equal(cv) {
		revid = doc.GetRevTreeID()
		deleted = doc.Deleted
		hlv = doc.HLV
	}

	validatedHistory, getHistoryErr := doc.History.getHistory(revid)
	if getHistoryErr != nil {
		return bodyBytes, history, channels, removed, attachments, deleted, doc.Expiry, revid, hlv, err
	}
	history = encodeRevisions(ctx, doc.ID, validatedHistory)

	return bodyBytes, history, channels, removed, attachments, deleted, doc.Expiry, revid, hlv, err
}

func (c *DatabaseCollection) getCurrentVersion(ctx context.Context, doc *Document, cv Version, loadBackup bool) (bodyBytes []byte, attachments AttachmentsMeta, channels base.Set, deleted bool, err error) {
	if err = doc.HasCurrentVersion(ctx, cv); err != nil {
		if !loadBackup {
			// do not attempt to fetch backup revision by CV unless specified
			return nil, nil, nil, false, ErrMissing
		}
		bodyBytes, channels, deleted, err = c.getOldRevisionJSON(ctx, doc.ID, base.Crc32cHashString([]byte(cv.String())))
		if err != nil || bodyBytes == nil {
			return nil, nil, nil, false, err
		}
	} else {
		bodyBytes, err = doc.BodyBytes(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Marshal error when retrieving active current version body: %v", err)
			return nil, nil, nil, false, err
		}
		channels = doc.SyncData.getCurrentChannels()
		attachments = doc.Attachments()
	}

	// handle backup revision inline attachments, or pre-2.5 meta
	if inlineAtts, cleanBodyBytes, _, err := extractInlineAttachments(bodyBytes); err != nil {
		return nil, nil, nil, false, err
	} else if len(inlineAtts) > 0 {
		// we found some inline attachments, so merge them with attachments, and update the bodies
		attachments = mergeAttachments(inlineAtts, attachments)
		bodyBytes = cleanBodyBytes
	}
	return bodyBytes, attachments, channels, deleted, err
}

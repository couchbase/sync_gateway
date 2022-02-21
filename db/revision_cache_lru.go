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
	"container/list"
	"context"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

type ShardedLRURevisionCache struct {
	caches    []*LRURevisionCache
	numShards uint16
}

// Creates a sharded revision cache with the given capacity and an optional loader function.
func NewShardedLRURevisionCache(shardCount uint16, capacity uint32, backingStore RevisionCacheBackingStore, cacheHitStat, cacheMissStat *base.SgwIntStat) *ShardedLRURevisionCache {

	caches := make([]*LRURevisionCache, shardCount)
	// Add 10% to per-shared cache capacity to ensure overall capacity is reached under non-ideal shard hashing
	perCacheCapacity := 1.1 * float32(capacity) / float32(shardCount)
	for i := 0; i < int(shardCount); i++ {
		caches[i] = NewLRURevisionCache(uint32(perCacheCapacity+0.5), backingStore, cacheHitStat, cacheMissStat)
	}

	return &ShardedLRURevisionCache{
		caches:    caches,
		numShards: shardCount,
	}
}

func (sc *ShardedLRURevisionCache) getShard(docID string) *LRURevisionCache {
	return sc.caches[sgbucket.VBHash(docID, sc.numShards)]
}

func (sc *ShardedLRURevisionCache) Get(ctx context.Context, docID, revID string, includeBody bool, includeDelta bool) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).Get(ctx, docID, revID, includeBody, includeDelta)
}

func (sc *ShardedLRURevisionCache) Peek(ctx context.Context, docID, revID string) (docRev DocumentRevision, found bool) {
	return sc.getShard(docID).Peek(ctx, docID, revID)
}

func (sc *ShardedLRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, toDelta RevisionDelta) {
	sc.getShard(docID).UpdateDelta(ctx, docID, revID, toDelta)
}

func (sc *ShardedLRURevisionCache) GetActive(ctx context.Context, docID string, includeBody bool) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).GetActive(ctx, docID, includeBody)
}

func (sc *ShardedLRURevisionCache) Put(ctx context.Context, docRev DocumentRevision) {
	sc.getShard(docRev.DocID).Put(ctx, docRev)
}

func (sc *ShardedLRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision) {
	sc.getShard(docRev.DocID).Upsert(ctx, docRev)
}

func (sc *ShardedLRURevisionCache) Invalidate(ctx context.Context, docID, revID string) {
	sc.getShard(docID).Invalidate(ctx, docID, revID)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	cache        map[IDAndRev]*list.Element // Fast lookup of list element by doc/rev ID
	lruList      *list.List                 // List ordered by most recent access (Front is newest)
	capacity     uint32                     // Max number of revisions to cache
	backingStore RevisionCacheBackingStore  // provides the methods used by the RevisionCacheLoaderFunc
	lock         sync.Mutex                 // For thread-safety
	cacheHits    *base.SgwIntStat
	cacheMisses  *base.SgwIntStat
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	key         IDAndRev        // doc/rev IDs
	bodyBytes   []byte          // Revision body (with no special properties)
	history     Revisions       // Rev history encoded like a "_revisions" property
	channels    base.Set        // Set of channels that have access
	expiry      *time.Time      // Document expiry
	attachments AttachmentsMeta // Document _attachments property
	delta       *RevisionDelta  // Available delta *from* this revision
	deleted     bool            // True if revision is a tombstone
	err         error           // Error from loaderFunc if it failed
	lock        sync.RWMutex    // Synchronizes access to this struct
	body        Body            // unmarshalled body (if available)
	removed     bool            // True if revision is a removal
	invalid     bool            // Marks a revision as invalid meaning it won't be used
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(capacity uint32, backingStore RevisionCacheBackingStore, cacheHitStat, cacheMissStat *base.SgwIntStat) *LRURevisionCache {

	return &LRURevisionCache{
		cache:        map[IDAndRev]*list.Element{},
		lruList:      list.New(),
		capacity:     capacity,
		backingStore: backingStore,
		cacheHits:    cacheHitStat,
		cacheMisses:  cacheMissStat,
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get.
func (rc *LRURevisionCache) Get(ctx context.Context, docID, revID string, includeBody bool, includeDelta bool) (DocumentRevision, error) {
	return rc.getFromCache(ctx, docID, revID, true, includeBody, includeDelta)
}

// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) Peek(ctx context.Context, docID, revID string) (docRev DocumentRevision, found bool) {
	docRev, err := rc.getFromCache(ctx, docID, revID, false, RevCacheOmitBody, RevCacheOmitDelta)
	if err != nil {
		return DocumentRevision{}, false
	}
	return docRev, docRev.BodyBytes != nil
}

// Attempt to update the delta on a revision cache entry.  If the entry is no longer resident in the cache,
// fails silently
func (rc *LRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, toDelta RevisionDelta) {
	value := rc.getValue(docID, revID, false)
	if value != nil {
		value.updateDelta(toDelta)
	}
}

func (rc *LRURevisionCache) getFromCache(ctx context.Context, docID, revID string, loadOnCacheMiss bool, includeBody bool, includeDelta bool) (DocumentRevision, error) {
	value := rc.getValue(docID, revID, loadOnCacheMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}

	if value.invalid {
		return rc.LoadInvalidRevFromBackingStore(ctx, value.key, nil, includeBody, includeDelta)
	}

	docRev, statEvent, err := value.load(ctx, rc.backingStore, includeBody, includeDelta)
	rc.statsRecorderFunc(statEvent)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return docRev, err
}

// In the event that a revision in invalid it needs to be replaced later and the revision cache value should not be
// used. This function grabs the value directly from the bucket.
func (rc *LRURevisionCache) LoadInvalidRevFromBackingStore(ctx context.Context, key IDAndRev, doc *Document, includeBody bool, includeDelta bool) (DocumentRevision, error) {
	var delta *RevisionDelta
	var docRevBody Body

	value := revCacheValue{
		key:     key,
		invalid: true,
	}

	// If doc has been passed in use this to grab values. Otherwise run revCacheLoader which will grab the Document
	// first
	if doc != nil {
		value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoaderForDocument(ctx, rc.backingStore, doc, key.RevID)
	} else {
		value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoader(ctx, rc.backingStore, key, includeBody)
	}

	if includeDelta {
		delta = value.delta
	}

	if includeBody {
		docRevBody = value.body
	}

	docRev, err := value.asDocumentRevision(docRevBody, delta)

	// Classify operation as a cache miss
	rc.statsRecorderFunc(false)

	return docRev, err

}

// Attempts to retrieve the active revision for a document from the cache.  Requires retrieval
// of the document from the bucket to guarantee the current active revision, but does minimal unmarshalling
// of the retrieved document to get the current rev from _sync metadata.  If active rev is already in the
// rev cache, will use it.  Otherwise will add to the rev cache using the raw document obtained in the
// initial retrieval.
func (rc *LRURevisionCache) GetActive(ctx context.Context, docID string, includeBody bool) (DocumentRevision, error) {

	// Look up active rev for doc.  Note - can't rely on DocUnmarshalAll here when includeBody=true, because for a
	// cache hit we don't want to do that work (yet).
	bucketDoc, getErr := rc.backingStore.GetDocument(ctx, docID, DocUnmarshalSync)
	if getErr != nil {
		return DocumentRevision{}, getErr
	}
	if bucketDoc == nil {
		return DocumentRevision{}, nil
	}

	// Retrieve from or add to rev cache
	value := rc.getValue(docID, bucketDoc.CurrentRev, true)

	if value.invalid {
		return rc.LoadInvalidRevFromBackingStore(ctx, value.key, bucketDoc, includeBody, false)
	}

	docRev, statEvent, err := value.loadForDoc(ctx, rc.backingStore, bucketDoc, includeBody)
	rc.statsRecorderFunc(statEvent)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return docRev, err
}

func (rc *LRURevisionCache) statsRecorderFunc(cacheHit bool) {

	if cacheHit {
		rc.cacheHits.Add(1)
	} else {
		rc.cacheMisses.Add(1)
	}
}

// Adds a revision to the cache.
func (rc *LRURevisionCache) Put(ctx context.Context, docRev DocumentRevision) {
	if docRev.History == nil {
		// TODO: CBG-1948
		panic("Missing history for RevisionCache.Put")
	}
	value := rc.getValue(docRev.DocID, docRev.RevID, true)
	value.store(docRev)
}

// Upsert a revision in the cache.
func (rc *LRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision) {
	key := IDAndRev{DocID: docRev.DocID, RevID: docRev.RevID}

	rc.lock.Lock()
	// If element exists remove from lrulist
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.Remove(elem)
	}

	// Add new value and overwrite existing cache key, pushing to front to maintain order
	value := &revCacheValue{key: key}
	rc.cache[key] = rc.lruList.PushFront(value)

	// Purge oldest item if required
	for len(rc.cache) > int(rc.capacity) {
		rc.purgeOldest_()
	}
	rc.lock.Unlock()

	value.store(docRev)
}

func (rc *LRURevisionCache) Invalidate(ctx context.Context, docID, revID string) {
	value := rc.getValue(docID, revID, false)
	if value != nil {
		value.setInvalidFlag()
	}
}

func (rc *LRURevisionCache) getValue(docID, revID string, create bool) (value *revCacheValue) {
	if docID == "" || revID == "" {
		// TODO: CBG-1948
		panic("RevisionCache: invalid empty doc/rev id")
	}
	key := IDAndRev{DocID: docID, RevID: revID}
	rc.lock.Lock()
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{key: key}
		rc.cache[key] = rc.lruList.PushFront(value)
		for len(rc.cache) > int(rc.capacity) {
			rc.purgeOldest_()
		}
	}
	rc.lock.Unlock()
	return
}

func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	if element := rc.cache[value.key]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.key)
	}
	rc.lock.Unlock()
}

func (rc *LRURevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	delete(rc.cache, value.key)
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(ctx context.Context, backingStore RevisionCacheBackingStore, includeBody bool, includeDelta bool) (docRev DocumentRevision, cacheHit bool, err error) {

	// Reading the delta from the revCacheValue requires holding the read lock, so it's managed outside asDocumentRevision,
	// to reduce locking when includeDelta=false
	var delta *RevisionDelta
	var docRevBody Body

	// Attempt to read cached value.
	value.lock.RLock()
	if value.bodyBytes != nil || value.err != nil {
		if includeDelta {
			delta = value.delta
		}
		if includeBody {
			docRevBody = value.body
		}
		value.lock.RUnlock()

		docRev, err = value.asDocumentRevision(docRevBody, delta)

		// On cache hit, if body is requested and not present in revCacheValue, generate from bytes and update revCacheValue
		if includeBody && docRev._shallowCopyBody == nil && err == nil {
			err = value.updateBody(ctx)
			docRev._shallowCopyBody = value.body.ShallowCopy()
		}
		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.bodyBytes != nil || value.err != nil {
		cacheHit = true
		// If body is requested and not already present in cache, populate value.body from value.BodyBytes
		if includeBody && value.body == nil && value.err == nil {
			if err := value.body.Unmarshal(value.bodyBytes); err != nil {
				base.WarnfCtx(ctx, "Unable to marshal BodyBytes in revcache for %s %s", base.UD(value.key.DocID), value.key.RevID)
			}
		}
	} else {
		cacheHit = false
		value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoader(ctx, backingStore, value.key, includeBody)
	}

	if includeDelta {
		delta = value.delta
	}
	if includeBody {
		docRevBody = value.body
	}
	value.lock.Unlock()

	docRev, err = value.asDocumentRevision(docRevBody, delta)
	return docRev, cacheHit, err
}

// Populate value.Body by unmarshalling value.bodyBytes
func (value *revCacheValue) updateBody(ctx context.Context) (err error) {
	var body Body
	if err := body.Unmarshal(value.bodyBytes); err != nil {
		// On unmarshal error, warn return docRev without body
		base.WarnfCtx(ctx, "Unable to marshal BodyBytes in revcache for %s %s", base.UD(value.key.DocID), value.key.RevID)
		return err
	}

	value.lock.Lock()
	if value.body == nil {
		value.body = body
	}
	value.lock.Unlock()
	return nil
}

// asDocumentRevision copies the rev cache value into a DocumentRevision.  Should only be called for non-empty
// revCacheValues - copies all immutable revCacheValue properties, and adds the provided body/delta.
func (value *revCacheValue) asDocumentRevision(body Body, delta *RevisionDelta) (DocumentRevision, error) {

	docRev := DocumentRevision{
		DocID:       value.key.DocID,
		RevID:       value.key.RevID,
		BodyBytes:   value.bodyBytes,
		History:     value.history,
		Channels:    value.channels,
		Expiry:      value.expiry,
		Attachments: value.attachments.ShallowCopy(), // Avoid caller mutating the stored attachments
		Deleted:     value.deleted,
		Removed:     value.removed,
		Invalid:     value.invalid,
	}
	if body != nil {
		docRev._shallowCopyBody = body.ShallowCopy()
	}
	docRev.Delta = delta

	return docRev, value.err
}

// Retrieves the body etc. out of a revCacheValue.  If they aren't already present, loads into the cache value using
// the provided document.
func (value *revCacheValue) loadForDoc(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document, includeBody bool) (docRev DocumentRevision, cacheHit bool, err error) {

	var docRevBody Body
	value.lock.RLock()
	if value.bodyBytes != nil || value.err != nil {
		if includeBody {
			docRevBody = value.body
		}
		value.lock.RUnlock()
		docRev, err = value.asDocumentRevision(docRevBody, nil)
		// If the body is requested and not yet populated on revCacheValue, populate it from the doc
		if includeBody && docRev._shallowCopyBody == nil {
			body := doc.Body()
			value.lock.Lock()
			if value.body == nil {
				value.body = body
			}
			value.lock.Unlock()
			docRev._shallowCopyBody = body.ShallowCopy()
		}

		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.bodyBytes != nil || value.err != nil {
		cacheHit = true
		// If body is requested and not already present in cache, attempt to generate from bytes and insert into cache
		if includeBody && value.body == nil {
			if err := value.body.Unmarshal(value.bodyBytes); err != nil {
				base.WarnfCtx(ctx, "Unable to marshal BodyBytes in revcache for %s %s", base.UD(value.key.DocID), value.key.RevID)
			}
		}
	} else {
		cacheHit = false
		value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoaderForDocument(ctx, backingStore, doc, value.key.RevID)
	}
	if includeBody {
		docRevBody = value.body
	}
	value.lock.Unlock()
	docRev, err = value.asDocumentRevision(docRevBody, nil)
	return docRev, cacheHit, err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(docRev DocumentRevision) {
	value.lock.Lock()
	if value.bodyBytes == nil {
		// value already has doc id/rev id in key
		value.bodyBytes = docRev.BodyBytes
		value.history = docRev.History
		value.channels = docRev.Channels
		value.expiry = docRev.Expiry
		value.attachments = docRev.Attachments.ShallowCopy() // Don't store attachments the caller might later mutate
		value.deleted = docRev.Deleted
		value.err = nil
		value.body = docRev._shallowCopyBody.ShallowCopy()
	}
	value.lock.Unlock()
}

func (value *revCacheValue) updateDelta(toDelta RevisionDelta) {
	value.lock.Lock()
	value.delta = &toDelta
	value.lock.Unlock()
}

func (value *revCacheValue) setInvalidFlag() {
	value.lock.Lock()
	value.invalid = true
	value.lock.Unlock()
}

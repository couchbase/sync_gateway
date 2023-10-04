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

func (sc *ShardedLRURevisionCache) Get(ctx context.Context, docID, revID string, cv *CurrentVersionVector, includeBody, includeDelta bool) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).Get(ctx, docID, revID, cv, includeBody, includeDelta)
}

func (sc *ShardedLRURevisionCache) Peek(ctx context.Context, docID, revID string, cv *CurrentVersionVector) (docRev DocumentRevision, found bool) {
	return sc.getShard(docID).Peek(ctx, docID, revID, cv)
}

func (sc *ShardedLRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, cv *CurrentVersionVector, toDelta RevisionDelta) {
	sc.getShard(docID).UpdateDelta(ctx, docID, revID, nil, toDelta)
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

func (sc *ShardedLRURevisionCache) Remove(docID, revID string, cv *CurrentVersionVector) {
	sc.getShard(docID).Remove(docID, revID, cv)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	backingStore RevisionCacheBackingStore
	cache        map[IDAndRev]*list.Element
	hlvCache     map[IDandCV]*list.Element
	lruList      *list.List
	cacheHits    *base.SgwIntStat
	cacheMisses  *base.SgwIntStat
	lock         sync.Mutex
	capacity     uint32
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	err         error
	history     Revisions
	channels    base.Set
	expiry      *time.Time
	attachments AttachmentsMeta
	delta       *RevisionDelta
	body        Body
	key         IDAndRev
	hlvKey      IDandCV
	cvLoad      bool
	bodyBytes   []byte
	lock        sync.RWMutex
	deleted     bool
	removed     bool
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(capacity uint32, backingStore RevisionCacheBackingStore, cacheHitStat, cacheMissStat *base.SgwIntStat) *LRURevisionCache {

	return &LRURevisionCache{
		cache:        map[IDAndRev]*list.Element{},
		hlvCache:     map[IDandCV]*list.Element{},
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
func (rc *LRURevisionCache) Get(ctx context.Context, docID, revID string, cv *CurrentVersionVector, includeBody, includeDelta bool) (DocumentRevision, error) {
	return rc.getFromCache(ctx, docID, revID, cv, true, includeBody, includeDelta)
}

// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) Peek(ctx context.Context, docID, revID string, cv *CurrentVersionVector) (docRev DocumentRevision, found bool) {
	docRev, err := rc.getFromCache(ctx, docID, revID, cv, false, RevCacheOmitBody, RevCacheOmitDelta)
	if err != nil {
		return DocumentRevision{}, false
	}
	return docRev, docRev.BodyBytes != nil
}

// Attempt to update the delta on a revision cache entry.  If the entry is no longer resident in the cache,
// fails silently
func (rc *LRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, cv *CurrentVersionVector, toDelta RevisionDelta) {
	var value *revCacheValue
	if cv == nil {
		value = rc.getValue(docID, revID, false)
		if value != nil {
			value.updateDelta(toDelta)
		}
	} else {
		value = rc.getValueByCV(docID, "", cv, false)
		if value != nil {
			value.updateDelta(toDelta)
		}
	}
}

func (rc *LRURevisionCache) getFromCache(ctx context.Context, docID, revID string, cv *CurrentVersionVector, loadOnCacheMiss, includeBody, includeDelta bool) (DocumentRevision, error) {
	var value *revCacheValue
	if cv != nil {
		value = rc.getValueByCV(docID, revID, cv, loadOnCacheMiss)
		if value == nil {
			return DocumentRevision{}, nil
		}
		value.cvLoad = true
	} else {
		value = rc.getValue(docID, revID, loadOnCacheMiss)
		if value == nil {
			return DocumentRevision{}, nil
		}
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
		key: key,
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
	var value *revCacheValue
	if docRev.CV == nil {
		value = rc.getValue(docRev.DocID, docRev.RevID, true)
	} else {
		value = rc.getValueByCV(docRev.DocID, docRev.RevID, docRev.CV, true)
	}

	value.store(docRev)
}

// Upsert a revision in the cache.
func (rc *LRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision) {
	// update the cv cache, then the revid cache too for backwards compatability
	if docRev.CV != nil {
		rc.upsertHLVCache(ctx, docRev)
		// we have also updated the revID lookup cache too so return early here
		return
	}
	rc.upsertRevIDCache(ctx, docRev)
}

func (rc *LRURevisionCache) upsertRevIDCache(ctx context.Context, docRev DocumentRevision) {
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

// upsertHLVCache will upsert a value to the cache updating both the CV lookup map and the revID lookup map
func (rc *LRURevisionCache) upsertHLVCache(ctx context.Context, docRev DocumentRevision) {
	key := IDandCV{DocID: docRev.DocID, Source: docRev.CV.SourceID, Version: docRev.CV.VersionCAS}
	legacyKey := IDAndRev{DocID: docRev.DocID, RevID: docRev.RevID}

	rc.lock.Lock()
	if elem := rc.hlvCache[key]; elem != nil {
		rc.lruList.Remove(elem)
	}

	value := &revCacheValue{hlvKey: key, key: legacyKey}
	newElem := rc.lruList.PushFront(value)
	rc.hlvCache[key] = newElem
	rc.cache[legacyKey] = newElem

	for rc.lruList.Len() > int(rc.capacity) {
		rc.purgeOldest_()
	}
	rc.lock.Unlock()

	value.store(docRev)
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
		for rc.lruList.Len() > int(rc.capacity) {
			rc.purgeOldest_()
		}
	}
	rc.lock.Unlock()
	return
}

// getValueByCV gets a value from rev cache by CV, if not found and create is true, will add the value to cache and both lookup maps
func (rc *LRURevisionCache) getValueByCV(docID string, revID string, cv *CurrentVersionVector, create bool) (value *revCacheValue) {
	if docID == "" || cv == nil {
		return nil
	}

	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.VersionCAS}
	rc.lock.Lock()
	if elem := rc.hlvCache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		// we need to keep this new element in the CV lookup cache and the revid cache for backwards compatability reasons
		legacyKey := IDAndRev{DocID: docID, RevID: revID}
		value = &revCacheValue{hlvKey: key, key: legacyKey}
		newElem := rc.lruList.PushFront(value)
		rc.cache[legacyKey] = newElem
		rc.hlvCache[key] = newElem
		for rc.lruList.Len() > int(rc.capacity) {
			rc.purgeOldest_()
		}
	}
	rc.lock.Unlock()
	return
}

// Remove removes a value from the revision cache, if present.
func (rc *LRURevisionCache) Remove(docID, revID string, cv *CurrentVersionVector) {

	// attempt to remove from both cv lookup maps and revid lookup map, it is possible a revision will be present in both
	if cv != nil {
		rc.removeFromCacheCV(docID, cv)
	}
	rc.removeFromCacheIDRev(docID, revID)
}

// removeFromCacheCV removes an entry from rev cache by CV
func (rc *LRURevisionCache) removeFromCacheCV(docID string, cv *CurrentVersionVector) {
	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.VersionCAS}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.hlvCache[key]
	if !ok {
		return
	}
	rc.lruList.Remove(element)
	delete(rc.hlvCache, key)
}

// removeFromCacheIDRev removes an entry from rev cache by revID
func (rc *LRURevisionCache) removeFromCacheIDRev(docID, revID string) {
	key := IDAndRev{DocID: docID, RevID: revID}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.cache[key]
	if !ok {
		return
	}
	rc.lruList.Remove(element)
	delete(rc.cache, key)
}

// removeValue removes a value from the revision cache, if present and the value matches the the value. If there's an item in the revision cache with a matching docID and revID but the document is different, this item will not be removed from the rev cache.
func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if element := rc.cache[value.key]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.key)
	}
	// need to also check hlv lookup cache map
	if element := rc.hlvCache[value.hlvKey]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.hlvCache, value.hlvKey)
	}
}

func (rc *LRURevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	delete(rc.cache, value.key)
	delete(rc.hlvCache, value.hlvKey)
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
		if value.cvLoad {
			value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoaderForCv(ctx, backingStore, value.hlvKey, includeBody)
		} else {
			value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoader(ctx, backingStore, value.key, includeBody)
		}
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
	var docID string
	if value.cvLoad {
		docID = value.hlvKey.DocID
	} else {
		docID = value.key.DocID
	}

	docRev := DocumentRevision{
		DocID:       docID,
		RevID:       value.key.RevID,
		BodyBytes:   value.bodyBytes,
		History:     value.history,
		Channels:    value.channels,
		Expiry:      value.expiry,
		Attachments: value.attachments.ShallowCopy(), // Avoid caller mutating the stored attachments
		Deleted:     value.deleted,
		Removed:     value.removed,
		CV:          &CurrentVersionVector{VersionCAS: value.hlvKey.Version, SourceID: value.hlvKey.Source},
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
			body := doc.Body(ctx)
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
		if value.cvLoad {
			cv := CurrentVersionVector{
				SourceID:   value.hlvKey.Source,
				VersionCAS: value.hlvKey.Version,
			}
			value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoaderForDocumentCV(ctx, backingStore, doc, cv)
		} else {
			value.bodyBytes, value.body, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoaderForDocument(ctx, backingStore, doc, value.key.RevID)
		}
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

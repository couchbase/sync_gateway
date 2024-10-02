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
func NewShardedLRURevisionCache(shardCount uint16, capacity uint32, backingStores map[uint32]RevisionCacheBackingStore, cacheHitStat, cacheMissStat *base.SgwIntStat) *ShardedLRURevisionCache {

	caches := make([]*LRURevisionCache, shardCount)
	// Add 10% to per-shared cache capacity to ensure overall capacity is reached under non-ideal shard hashing
	perCacheCapacity := 1.1 * float32(capacity) / float32(shardCount)
	for i := 0; i < int(shardCount); i++ {
		caches[i] = NewLRURevisionCache(uint32(perCacheCapacity+0.5), backingStores, cacheHitStat, cacheMissStat)
	}

	return &ShardedLRURevisionCache{
		caches:    caches,
		numShards: shardCount,
	}
}

func (sc *ShardedLRURevisionCache) getShard(docID string) *LRURevisionCache {
	return sc.caches[sgbucket.VBHash(docID, sc.numShards)]
}

func (sc *ShardedLRURevisionCache) GetWithRev(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).GetWithRev(ctx, docID, revID, collectionID, includeDelta)
}

func (sc *ShardedLRURevisionCache) GetWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32, includeDelta bool) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).GetWithCV(ctx, docID, cv, collectionID, includeDelta)
}

func (sc *ShardedLRURevisionCache) Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return sc.getShard(docID).Peek(ctx, docID, revID, collectionID)
}

func (sc *ShardedLRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	sc.getShard(docID).UpdateDelta(ctx, docID, revID, collectionID, toDelta)
}

func (sc *ShardedLRURevisionCache) UpdateDeltaCV(ctx context.Context, docID string, cv *Version, collectionID uint32, toDelta RevisionDelta) {
	sc.getShard(docID).UpdateDeltaCV(ctx, docID, cv, collectionID, toDelta)
}

func (sc *ShardedLRURevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).GetActive(ctx, docID, collectionID)
}

func (sc *ShardedLRURevisionCache) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	sc.getShard(docRev.DocID).Put(ctx, docRev, collectionID)
}

func (sc *ShardedLRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	sc.getShard(docRev.DocID).Upsert(ctx, docRev, collectionID)
}

func (sc *ShardedLRURevisionCache) RemoveWithRev(docID, revID string, collectionID uint32) {
	sc.getShard(docID).RemoveWithRev(docID, revID, collectionID)
}

func (sc *ShardedLRURevisionCache) RemoveWithCV(docID string, cv *Version, collectionID uint32) {
	sc.getShard(docID).RemoveWithCV(docID, cv, collectionID)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	backingStores map[uint32]RevisionCacheBackingStore
	cache         map[IDAndRev]*list.Element
	hlvCache      map[IDandCV]*list.Element
	lruList       *list.List
	cacheHits     *base.SgwIntStat
	cacheMisses   *base.SgwIntStat
	lock          sync.Mutex
	capacity      uint32
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	err         error
	history     Revisions
	channels    base.Set
	expiry      *time.Time
	attachments AttachmentsMeta
	delta       *RevisionDelta
	id          string
	cv          Version
	hlvHistory  string
	revID       string
	bodyBytes   []byte
	lock        sync.RWMutex
	deleted     bool
	removed     bool
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(capacity uint32, backingStores map[uint32]RevisionCacheBackingStore, cacheHitStat, cacheMissStat *base.SgwIntStat) *LRURevisionCache {

	return &LRURevisionCache{
		cache:         map[IDAndRev]*list.Element{},
		hlvCache:      map[IDandCV]*list.Element{},
		lruList:       list.New(),
		capacity:      capacity,
		backingStores: backingStores,
		cacheHits:     cacheHitStat,
		cacheMisses:   cacheMissStat,
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get.
func (rc *LRURevisionCache) GetWithRev(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (DocumentRevision, error) {
	return rc.getFromCacheByRev(ctx, docID, revID, collectionID, true, includeDelta)
}

func (rc *LRURevisionCache) GetWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32, includeDelta bool) (DocumentRevision, error) {
	return rc.getFromCacheByCV(ctx, docID, cv, collectionID, true, includeDelta)
}

// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool) {
	docRev, err := rc.getFromCacheByRev(ctx, docID, revID, collectionID, false, RevCacheOmitDelta)
	if err != nil {
		return DocumentRevision{}, false
	}
	return docRev, docRev.BodyBytes != nil
}

// Attempt to update the delta on a revision cache entry.  If the entry is no longer resident in the cache,
// fails silently
func (rc *LRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	value := rc.getValue(docID, revID, collectionID, false)
	if value != nil {
		value.updateDelta(toDelta)
	}
}

func (rc *LRURevisionCache) UpdateDeltaCV(ctx context.Context, docID string, cv *Version, collectionID uint32, toDelta RevisionDelta) {
	value := rc.getValueByCV(docID, cv, collectionID, false)
	if value != nil {
		value.updateDelta(toDelta)
	}
}

func (rc *LRURevisionCache) getFromCacheByRev(ctx context.Context, docID, revID string, collectionID uint32, loadOnCacheMiss, includeDelta bool) (DocumentRevision, error) {
	value := rc.getValue(docID, revID, collectionID, loadOnCacheMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}

	docRev, cacheHit, err := value.load(ctx, rc.backingStores[collectionID], includeDelta)
	rc.statsRecorderFunc(cacheHit)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	if !cacheHit && err == nil {
		rc.addToHLVMapPostLoad(docID, docRev.RevID, docRev.CV)
	}

	return docRev, err
}

func (rc *LRURevisionCache) getFromCacheByCV(ctx context.Context, docID string, cv *Version, collectionID uint32, loadCacheOnMiss bool, includeDelta bool) (DocumentRevision, error) {
	value := rc.getValueByCV(docID, cv, collectionID, loadCacheOnMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}

	docRev, cacheHit, err := value.load(ctx, rc.backingStores[collectionID], includeDelta)
	rc.statsRecorderFunc(cacheHit)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}

	if !cacheHit && err == nil {
		rc.addToRevMapPostLoad(docID, docRev.RevID, docRev.CV, collectionID)
	}

	return docRev, err
}

// In the event that a revision in invalid it needs to be replaced later and the revision cache value should not be
// used. This function grabs the value directly from the bucket.
func (rc *LRURevisionCache) LoadInvalidRevFromBackingStore(ctx context.Context, key IDAndRev, doc *Document, collectionID uint32, includeDelta bool) (DocumentRevision, error) {
	var delta *RevisionDelta

	value := revCacheValue{
		id:    key.DocID,
		revID: key.RevID,
	}

	// If doc has been passed in use this to grab values. Otherwise run revCacheLoader which will grab the Document
	// first
	if doc != nil {
		value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, _, value.err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, key.RevID)
	} else {
		value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, _, value.err = revCacheLoader(ctx, rc.backingStores[collectionID], key)
	}

	if includeDelta {
		delta = value.delta
	}

	docRev, err := value.asDocumentRevision(delta)

	// Classify operation as a cache miss
	rc.statsRecorderFunc(false)

	return docRev, err

}

// Attempts to retrieve the active revision for a document from the cache.  Requires retrieval
// of the document from the bucket to guarantee the current active revision, but does minimal unmarshalling
// of the retrieved document to get the current rev from _sync metadata.  If active rev is already in the
// rev cache, will use it.  Otherwise will add to the rev cache using the raw document obtained in the
// initial retrieval.
func (rc *LRURevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (DocumentRevision, error) {

	// Look up active rev for doc.  Note - can't rely on DocUnmarshalAll here when includeBody=true, because for a
	// cache hit we don't want to do that work (yet).
	bucketDoc, getErr := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if getErr != nil {
		return DocumentRevision{}, getErr
	}
	if bucketDoc == nil {
		return DocumentRevision{}, nil
	}

	// Retrieve from or add to rev cache
	value := rc.getValue(docID, bucketDoc.CurrentRev, collectionID, true)

	docRev, cacheHit, err := value.loadForDoc(ctx, rc.backingStores[collectionID], bucketDoc)
	rc.statsRecorderFunc(cacheHit)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	} else {
		// add successfully fetched value to CV lookup map too
		rc.addToHLVMapPostLoad(docID, docRev.RevID, docRev.CV)
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
func (rc *LRURevisionCache) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	if docRev.History == nil {
		// TODO: CBG-1948
		panic("Missing history for RevisionCache.Put")
	}

	// doc should always have a cv present in a PUT operation on the cache (update HLV is called before hand in doc update process)
	// thus we can call getValueByCV directly the update the rev lookup post this
	value := rc.getValueByCV(docRev.DocID, docRev.CV, collectionID, true)
	value.store(docRev)

	// add new doc version to the rev id lookup map
	rc.addToRevMapPostLoad(docRev.DocID, docRev.RevID, docRev.CV, collectionID)
}

// Upsert a revision in the cache.
func (rc *LRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	var value *revCacheValue
	// similar to PUT operation we should have the CV defined by this point (updateHLV is called before calling this)
	key := IDandCV{DocID: docRev.DocID, Source: docRev.CV.SourceID, Version: docRev.CV.Value, CollectionID: collectionID}
	legacyKey := IDAndRev{DocID: docRev.DocID, RevID: docRev.RevID, CollectionID: collectionID}

	rc.lock.Lock()
	// lookup for element in hlv lookup map, if not found for some reason try rev lookup map
	if elem := rc.hlvCache[key]; elem != nil {
		rc.lruList.Remove(elem)
	} else if elem = rc.cache[legacyKey]; elem != nil {
		rc.lruList.Remove(elem)
	}

	// Add new value and overwrite existing cache key, pushing to front to maintain order
	// also ensure we add to rev id lookup map too
	value = &revCacheValue{id: docRev.DocID, cv: *docRev.CV}
	elem := rc.lruList.PushFront(value)
	rc.hlvCache[key] = elem
	rc.cache[legacyKey] = elem

	for rc.lruList.Len() > int(rc.capacity) {
		rc.purgeOldest_()
	}
	rc.lock.Unlock()
	// store upsert value
	value.store(docRev)
}

func (rc *LRURevisionCache) getValue(docID, revID string, collectionID uint32, create bool) (value *revCacheValue) {
	if docID == "" || revID == "" {
		// TODO: CBG-1948
		panic("RevisionCache: invalid empty doc/rev id")
	}
	key := IDAndRev{DocID: docID, RevID: revID, CollectionID: collectionID}
	rc.lock.Lock()
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{id: docID, revID: revID}
		rc.cache[key] = rc.lruList.PushFront(value)
		for rc.lruList.Len() > int(rc.capacity) {
			rc.purgeOldest_()
		}
	}
	rc.lock.Unlock()
	return
}

// getValueByCV gets a value from rev cache by CV, if not found and create is true, will add the value to cache and both lookup maps

func (rc *LRURevisionCache) getValueByCV(docID string, cv *Version, collectionID uint32, create bool) (value *revCacheValue) {
	if docID == "" || cv == nil {
		return nil
	}

	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value, CollectionID: collectionID}
	rc.lock.Lock()
	if elem := rc.hlvCache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{id: docID, cv: *cv}
		newElem := rc.lruList.PushFront(value)
		rc.hlvCache[key] = newElem
		for rc.lruList.Len() > int(rc.capacity) {
			rc.purgeOldest_()
		}
	}
	rc.lock.Unlock()
	return
}

// addToRevMapPostLoad will generate and entry in the Rev lookup map for a new document entering the cache
func (rc *LRURevisionCache) addToRevMapPostLoad(docID, revID string, cv *Version, collectionID uint32) {
	legacyKey := IDAndRev{DocID: docID, RevID: revID, CollectionID: collectionID}
	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value, CollectionID: collectionID}

	rc.lock.Lock()
	defer rc.lock.Unlock()
	// check for existing value in rev cache map (due to concurrent fetch by rev ID)
	cvElem, cvFound := rc.hlvCache[key]
	revElem, revFound := rc.cache[legacyKey]
	if !cvFound {
		// its possible the element has been evicted if we don't find the element above (high churn on rev cache)
		// need to return doc revision to caller still but no need repopulate the cache
		return
	}
	// Check if another goroutine has already updated the rev map
	if revFound {
		if cvElem == revElem {
			// already match, return
			return
		}
		// if CV map and rev map are targeting different list elements, update to have both use the cv map element
		rc.cache[legacyKey] = cvElem
		rc.lruList.Remove(revElem)
	} else {
		// if not found we need to add the element to the rev lookup (for PUT code path)
		rc.cache[legacyKey] = cvElem
	}
}

// addToHLVMapPostLoad will generate and entry in the CV lookup map for a new document entering the cache
func (rc *LRURevisionCache) addToHLVMapPostLoad(docID, revID string, cv *Version) {
	legacyKey := IDAndRev{DocID: docID, RevID: revID}
	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value}

	rc.lock.Lock()
	defer rc.lock.Unlock()
	// check for existing value in rev cache map (due to concurrent fetch by rev ID)
	cvElem, cvFound := rc.hlvCache[key]
	revElem, revFound := rc.cache[legacyKey]
	if !revFound {
		// its possible the element has been evicted if we don't find the element above (high churn on rev cache)
		// need to return doc revision to caller still but no need repopulate the cache
		return
	}
	// Check if another goroutine has already updated the cv map
	if cvFound {
		if cvElem == revElem {
			// already match, return
			return
		}
		// if CV map and rev map are targeting different list elements, update to have both use the cv map element
		rc.cache[legacyKey] = cvElem
		rc.lruList.Remove(revElem)
	}
}

// Remove removes a value from the revision cache, if present.
func (rc *LRURevisionCache) RemoveWithRev(docID, revID string, collectionID uint32) {
	rc.removeFromCacheByRev(docID, revID, collectionID)
}

// RemoveWithCV removes a value from rev cache by CV reference if present
func (rc *LRURevisionCache) RemoveWithCV(docID string, cv *Version, collectionID uint32) {
	rc.removeFromCacheByCV(docID, cv, collectionID)
}

// removeFromCacheByCV removes an entry from rev cache by CV
func (rc *LRURevisionCache) removeFromCacheByCV(docID string, cv *Version, collectionID uint32) {
	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value, CollectionID: collectionID}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.hlvCache[key]
	if !ok {
		return
	}
	// grab the revid key from the value to enable us to remove the reference from the rev lookup map too
	elem := element.Value.(*revCacheValue)
	legacyKey := IDAndRev{DocID: docID, RevID: elem.revID}
	rc.lruList.Remove(element)
	delete(rc.hlvCache, key)
	// remove from rev lookup map too
	delete(rc.cache, legacyKey)
}

// removeFromCacheByRev removes an entry from rev cache by revID
func (rc *LRURevisionCache) removeFromCacheByRev(docID, revID string, collectionID uint32) {
	key := IDAndRev{DocID: docID, RevID: revID, CollectionID: collectionID}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.cache[key]
	if !ok {
		return
	}
	// grab the cv key from the value to enable us to remove the reference from the rev lookup map too
	elem := element.Value.(*revCacheValue)
	hlvKey := IDandCV{DocID: docID, Source: elem.cv.SourceID, Version: elem.cv.Value}
	rc.lruList.Remove(element)
	delete(rc.cache, key)
	// remove from CV lookup map too
	delete(rc.hlvCache, hlvKey)
}

// removeValue removes a value from the revision cache, if present and the value matches the the value. If there's an item in the revision cache with a matching docID and revID but the document is different, this item will not be removed from the rev cache.
func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	revKey := IDAndRev{DocID: value.id, RevID: value.revID}
	if element := rc.cache[revKey]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, revKey)
	}
	// need to also check hlv lookup cache map
	hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value}
	if element := rc.hlvCache[hlvKey]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.hlvCache, hlvKey)
	}
}

func (rc *LRURevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	revKey := IDAndRev{DocID: value.id, RevID: value.revID}
	hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value}
	delete(rc.cache, revKey)
	delete(rc.hlvCache, hlvKey)
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(ctx context.Context, backingStore RevisionCacheBackingStore, includeDelta bool) (docRev DocumentRevision, cacheHit bool, err error) {

	// Reading the delta from the revCacheValue requires holding the read lock, so it's managed outside asDocumentRevision,
	// to reduce locking when includeDelta=false
	var delta *RevisionDelta
	var revid string

	// Attempt to read cached value.
	value.lock.RLock()
	if value.bodyBytes != nil || value.err != nil {
		if includeDelta {
			delta = value.delta
		}
		value.lock.RUnlock()

		docRev, err = value.asDocumentRevision(delta)

		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.bodyBytes != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		hlv := &HybridLogicalVector{}
		if value.revID == "" {
			hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value}
			value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, revid, hlv, value.err = revCacheLoaderForCv(ctx, backingStore, hlvKey)
			// based off the current value load we need to populate the revid key with what has been fetched from the bucket (for use of populating the opposite lookup map)
			value.revID = revid
			if hlv != nil {
				value.hlvHistory = hlv.ToHistoryForHLV()
			}
		} else {
			revKey := IDAndRev{DocID: value.id, RevID: value.revID}
			value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, hlv, value.err = revCacheLoader(ctx, backingStore, revKey)
			// based off the revision load we need to populate the hlv key with what has been fetched from the bucket (for use of populating the opposite lookup map)
			if hlv != nil {
				value.cv = *hlv.ExtractCurrentVersionFromHLV()
				value.hlvHistory = hlv.ToHistoryForHLV()
			}
		}
	}

	if includeDelta {
		delta = value.delta
	}
	value.lock.Unlock()

	docRev, err = value.asDocumentRevision(delta)
	return docRev, cacheHit, err
}

// asDocumentRevision copies the rev cache value into a DocumentRevision.  Should only be called for non-empty
// revCacheValues - copies all immutable revCacheValue properties, and adds the provided body/delta.
func (value *revCacheValue) asDocumentRevision(delta *RevisionDelta) (DocumentRevision, error) {

	docRev := DocumentRevision{
		DocID:       value.id,
		RevID:       value.revID,
		BodyBytes:   value.bodyBytes,
		History:     value.history,
		Channels:    value.channels,
		Expiry:      value.expiry,
		Attachments: value.attachments.ShallowCopy(), // Avoid caller mutating the stored attachments
		Deleted:     value.deleted,
		Removed:     value.removed,
		hlvHistory:  value.hlvHistory,
		CV:          &value.cv,
	}
	docRev.Delta = delta

	return docRev, value.err
}

// Retrieves the body etc. out of a revCacheValue.  If they aren't already present, loads into the cache value using
// the provided document.
func (value *revCacheValue) loadForDoc(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document) (docRev DocumentRevision, cacheHit bool, err error) {

	var revid string
	value.lock.RLock()
	if value.bodyBytes != nil || value.err != nil {
		value.lock.RUnlock()
		docRev, err = value.asDocumentRevision(nil)

		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.bodyBytes != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		hlv := &HybridLogicalVector{}
		if value.revID == "" {
			value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, revid, hlv, value.err = revCacheLoaderForDocumentCV(ctx, backingStore, doc, value.cv)
			value.revID = revid
		} else {
			value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, hlv, value.err = revCacheLoaderForDocument(ctx, backingStore, doc, value.revID)
		}
		if hlv != nil {
			value.cv = *hlv.ExtractCurrentVersionFromHLV()
			value.hlvHistory = hlv.ToHistoryForHLV()
		}
	}
	value.lock.Unlock()
	docRev, err = value.asDocumentRevision(nil)
	return docRev, cacheHit, err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(docRev DocumentRevision) {
	value.lock.Lock()
	if value.bodyBytes == nil {
		value.revID = docRev.RevID
		value.bodyBytes = docRev.BodyBytes
		value.history = docRev.History
		value.channels = docRev.Channels
		value.expiry = docRev.Expiry
		value.attachments = docRev.Attachments.ShallowCopy() // Don't store attachments the caller might later mutate
		value.deleted = docRev.Deleted
		value.err = nil
		value.hlvHistory = docRev.hlvHistory
	}
	value.lock.Unlock()
}

func (value *revCacheValue) updateDelta(toDelta RevisionDelta) {
	value.lock.Lock()
	value.delta = &toDelta
	value.lock.Unlock()
}

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
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

type ShardedLRURevisionCache struct {
	caches    []*LRURevisionCache
	numShards uint16
}

// Creates a sharded revision cache with the given capacity and an optional loader function.
func NewShardedLRURevisionCache(revCacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, cacheHitStat, cacheMissStat, cacheNumItemsStat, cacheMemoryStat *base.SgwIntStat) *ShardedLRURevisionCache {

	caches := make([]*LRURevisionCache, revCacheOptions.ShardCount)
	// Add 10% to per-shared cache capacity to ensure overall capacity is reached under non-ideal shard hashing
	perCacheCapacity := 1.1 * float32(revCacheOptions.MaxItemCount) / float32(revCacheOptions.ShardCount)
	revCacheOptions.MaxItemCount = uint32(perCacheCapacity)
	var perCacheMemoryCapacity float32
	if revCacheOptions.MaxBytes > 0 {
		perCacheMemoryCapacity = float32(revCacheOptions.MaxBytes) / float32(revCacheOptions.ShardCount)
		revCacheOptions.MaxBytes = int64(perCacheMemoryCapacity)
	}

	for i := 0; i < int(revCacheOptions.ShardCount); i++ {
		caches[i] = NewLRURevisionCache(revCacheOptions, backingStores, cacheHitStat, cacheMissStat, cacheNumItemsStat, cacheMemoryStat)
	}

	return &ShardedLRURevisionCache{
		caches:    caches,
		numShards: revCacheOptions.ShardCount,
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

func (sc *ShardedLRURevisionCache) RemoveWithRev(ctx context.Context, docID, revID string, collectionID uint32) {
	sc.getShard(docID).RemoveWithRev(ctx, docID, revID, collectionID)
}

func (sc *ShardedLRURevisionCache) RemoveWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	sc.getShard(docID).RemoveWithCV(ctx, docID, cv, collectionID)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	backingStores        map[uint32]RevisionCacheBackingStore
	cache                map[IDAndRev]*list.Element
	hlvCache             map[IDandCV]*list.Element
	lruList              *list.List
	cacheHits            *base.SgwIntStat
	cacheMisses          *base.SgwIntStat
	cacheNumItems        *base.SgwIntStat
	lock                 sync.Mutex
	capacity             uint32           // Max number of items capacity of LRURevisionCache
	memoryCapacity       int64            // Max memory capacity of LRURevisionCache
	currMemoryUsage      base.AtomicInt   // count of number of bytes used currently in the LRURevisionCache
	cacheMemoryBytesStat *base.SgwIntStat // stat for overall revision cache memory usage in bytes.  When using sharded cache, will be shared by all shards.
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	err          error
	history      Revisions
	channels     base.Set
	expiry       *time.Time
	attachments  AttachmentsMeta
	delta        *RevisionDelta
	id           string
	cv           Version
	hlvHistory   string
	revID        string
	bodyBytes    []byte
	lock         sync.RWMutex
	deleted      bool
	removed      bool
	itemBytes    atomic.Int64
	collectionID uint32
	canEvict     atomic.Bool
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(revCacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, cacheHitStat *base.SgwIntStat, cacheMissStat *base.SgwIntStat, cacheNumItemsStat *base.SgwIntStat, revCacheMemoryStat *base.SgwIntStat) *LRURevisionCache {

	return &LRURevisionCache{
		cache:                map[IDAndRev]*list.Element{},
		hlvCache:             map[IDandCV]*list.Element{},
		lruList:              list.New(),
		capacity:             revCacheOptions.MaxItemCount,
		backingStores:        backingStores,
		cacheHits:            cacheHitStat,
		cacheMisses:          cacheMissStat,
		cacheNumItems:        cacheNumItemsStat,
		cacheMemoryBytesStat: revCacheMemoryStat,
		memoryCapacity:       revCacheOptions.MaxBytes,
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
	value := rc.getValue(ctx, docID, revID, collectionID, false)
	if value != nil {
		outGoingBytes := value.updateDelta(toDelta)
		if outGoingBytes != 0 {
			rc.currMemoryUsage.Add(outGoingBytes)
			rc.cacheMemoryBytesStat.Add(outGoingBytes)
		}
		// check for memory based eviction
		rc.revCacheMemoryBasedEviction(ctx)
	}
}

func (rc *LRURevisionCache) UpdateDeltaCV(ctx context.Context, docID string, cv *Version, collectionID uint32, toDelta RevisionDelta) {
	value := rc.getValueByCV(ctx, docID, cv, collectionID, false)
	if value != nil {
		outGoingBytes := value.updateDelta(toDelta)
		if outGoingBytes != 0 {
			rc.currMemoryUsage.Add(outGoingBytes)
			rc.cacheMemoryBytesStat.Add(outGoingBytes)
		}
		// check for memory based eviction
		rc.revCacheMemoryBasedEviction(ctx)
	}
}

func (rc *LRURevisionCache) getFromCacheByRev(ctx context.Context, docID, revID string, collectionID uint32, loadOnCacheMiss, includeDelta bool) (DocumentRevision, error) {
	value := rc.getValue(ctx, docID, revID, collectionID, loadOnCacheMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}

	docRev, statEvent, err := value.load(ctx, rc.backingStores[collectionID], includeDelta)
	rc.statsRecorderFunc(statEvent)

	if !statEvent && err == nil {
		// cache miss so we had to load doc, increment memory count
		rc.incrRevCacheMemoryUsage(ctx, value.getItemBytes())
		// check for memory based eviction
		rc.revCacheMemoryBasedEviction(ctx)
		rc.addToHLVMapPostLoad(docID, docRev.RevID, docRev.CV, collectionID)
	}

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}

	return docRev, err
}

func (rc *LRURevisionCache) getFromCacheByCV(ctx context.Context, docID string, cv *Version, collectionID uint32, loadCacheOnMiss bool, includeDelta bool) (DocumentRevision, error) {
	value := rc.getValueByCV(ctx, docID, cv, collectionID, loadCacheOnMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}

	docRev, cacheHit, err := value.load(ctx, rc.backingStores[collectionID], includeDelta)
	rc.statsRecorderFunc(cacheHit)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}

	if !cacheHit && err == nil {
		// cache miss so we had to load doc, increment memory count
		rc.incrRevCacheMemoryUsage(ctx, value.getItemBytes())
		// check for memory based eviction
		rc.revCacheMemoryBasedEviction(ctx)
		rc.addToRevMapPostLoad(docID, docRev.RevID, docRev.CV, collectionID)
	}

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
	value := rc.getValue(ctx, docID, bucketDoc.CurrentRev, collectionID, true)

	docRev, statEvent, err := value.loadForDoc(ctx, rc.backingStores[collectionID], bucketDoc)
	rc.statsRecorderFunc(statEvent)

	if !statEvent && err == nil {
		// cache miss so we had to load doc, increment memory count
		rc.incrRevCacheMemoryUsage(ctx, value.getItemBytes())
		// check for rev cache memory based eviction
		rc.revCacheMemoryBasedEviction(ctx)
	}

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	} else {
		// add successfully fetched value to CV lookup map too
		rc.addToHLVMapPostLoad(docID, docRev.RevID, docRev.CV, collectionID)
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
	value := rc.getValueByCV(ctx, docRev.DocID, docRev.CV, collectionID, true)
	// increment incoming bytes
	docRev.CalculateBytes()
	rc.incrRevCacheMemoryUsage(ctx, docRev.MemoryBytes)
	value.store(docRev)

	// add new doc version to the rev id lookup map
	rc.addToRevMapPostLoad(docRev.DocID, docRev.RevID, docRev.CV, collectionID)

	// check for rev cache memory based eviction
	rc.revCacheMemoryBasedEviction(ctx)
}

// Upsert a revision in the cache.
func (rc *LRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	var value *revCacheValue
	// similar to PUT operation we should have the CV defined by this point (updateHLV is called before calling this)
	key := IDandCV{DocID: docRev.DocID, Source: docRev.CV.SourceID, Version: docRev.CV.Value, CollectionID: collectionID}
	legacyKey := IDAndRev{DocID: docRev.DocID, RevID: docRev.RevID, CollectionID: collectionID}

	rc.lock.Lock()

	newItem := true
	// lookup for element in hlv lookup map, if not found for some reason try rev lookup map
	var existingElem *list.Element
	var found bool
	existingElem, found = rc.hlvCache[key]
	if !found {
		existingElem, found = rc.cache[legacyKey]
	}
	if found {
		revItem := existingElem.Value.(*revCacheValue)
		// decrement item bytes by the removed item
		rc._decrRevCacheMemoryUsage(ctx, -revItem.getItemBytes())
		rc.lruList.Remove(existingElem)
		newItem = false
	}

	// Add new value and overwrite existing cache key, pushing to front to maintain order
	// also ensure we add to rev id lookup map too
	value = &revCacheValue{id: docRev.DocID, cv: *docRev.CV, collectionID: collectionID}
	elem := rc.lruList.PushFront(value)
	rc.hlvCache[key] = elem
	rc.cache[legacyKey] = elem

	// only increment if we are inserting new item to cache
	if newItem {
		rc.cacheNumItems.Add(1)
	}

	// Purge oldest item if over number capacity
	numItemsRemoved, numBytesEvicted := rc._numberCapacityEviction()
	if numBytesEvicted > 0 {
		rc._decrRevCacheMemoryUsage(ctx, -numBytesEvicted)
	}
	rc.lock.Unlock() // release lock after eviction finished
	if numItemsRemoved > 0 {
		rc.cacheNumItems.Add(-numItemsRemoved)
	}

	docRev.CalculateBytes()
	// add new item bytes to overall count
	rc.incrRevCacheMemoryUsage(ctx, docRev.MemoryBytes)
	value.store(docRev)

	// check we aren't over memory capacity, if so perform eviction
	rc.revCacheMemoryBasedEviction(ctx)
}

func (rc *LRURevisionCache) getValue(ctx context.Context, docID, revID string, collectionID uint32, create bool) (value *revCacheValue) {
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
		value = &revCacheValue{id: docID, revID: revID, collectionID: collectionID}
		value.canEvict.Store(false)
		rc.cache[key] = rc.lruList.PushFront(value)
		rc.cacheNumItems.Add(1)

		numItemsRemoved, numBytesEvicted := rc._numberCapacityEviction()
		if numBytesEvicted > 0 {
			rc._decrRevCacheMemoryUsage(ctx, -numBytesEvicted)
		}
		rc.lock.Unlock() // release lock as eviction is finished
		if numItemsRemoved > 0 {
			rc.cacheNumItems.Add(-numItemsRemoved)
		}
		// return early as rev cache mutex has been released at this point
		return
	}
	rc.lock.Unlock()
	return
}

func (rc *LRURevisionCache) getValueByCV(ctx context.Context, docID string, cv *Version, collectionID uint32, create bool) (value *revCacheValue) {
	if docID == "" || cv == nil {
		return nil
	}

	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value, CollectionID: collectionID}
	rc.lock.Lock()
	if elem := rc.hlvCache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{id: docID, cv: *cv, collectionID: collectionID}
		value.canEvict.Store(false)
		newElem := rc.lruList.PushFront(value)
		rc.hlvCache[key] = newElem
		rc.cacheNumItems.Add(1)

		// evict if over number capacity
		numItemsRemoved, numBytesEvicted := rc._numberCapacityEviction()
		if numBytesEvicted > 0 {
			rc._decrRevCacheMemoryUsage(ctx, -numBytesEvicted)
		}
		rc.lock.Unlock() // release lock as eviction is finished

		if numItemsRemoved > 0 {
			rc.cacheNumItems.Add(-numItemsRemoved)
		}
		// return early as rev cache mutex has been released at this point
		return
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
func (rc *LRURevisionCache) addToHLVMapPostLoad(docID, revID string, cv *Version, collectionID uint32) {
	legacyKey := IDAndRev{DocID: docID, RevID: revID, CollectionID: collectionID}
	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value, CollectionID: collectionID}

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
	} else {
		// if not found we need to add the element to the hlv lookup
		rc.hlvCache[key] = revElem
	}
}

// Remove removes a value from the revision cache, if present.
func (rc *LRURevisionCache) RemoveWithRev(ctx context.Context, docID, revID string, collectionID uint32) {
	rc.removeFromCacheByRev(ctx, docID, revID, collectionID)
}

// RemoveWithCV removes a value from rev cache by CV reference if present
func (rc *LRURevisionCache) RemoveWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	rc.removeFromCacheByCV(ctx, docID, cv, collectionID)
}

// removeFromCacheByCV removes an entry from rev cache by CV
func (rc *LRURevisionCache) removeFromCacheByCV(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	key := IDandCV{DocID: docID, Source: cv.SourceID, Version: cv.Value, CollectionID: collectionID}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.hlvCache[key]
	if !ok {
		return
	}
	// grab the revid key from the value to enable us to remove the reference from the rev lookup map too
	elem := element.Value.(*revCacheValue)

	// we can only remove this value if the value has finished loading from the bucket
	_, _ = base.RetryLoop(ctx, "remove from revision cache by cv", func() (shouldRetry bool, err error, _ any) {
		if !elem.canEvict.Load() {
			// value is still in process of being loaded from bucket, we need to wait for this to finish
			return true, nil, nil
		}
		return false, nil, nil
	}, base.CreateMaxDoublingSleeperFunc(30, 10, 500))

	legacyKey := IDAndRev{DocID: docID, RevID: elem.revID, CollectionID: collectionID}
	rc.lruList.Remove(element)
	delete(rc.hlvCache, key)
	// remove from rev lookup map too
	delete(rc.cache, legacyKey)
	rc._decrRevCacheMemoryUsage(ctx, -elem.getItemBytes())
	rc.cacheNumItems.Add(-1)
}

// removeFromCacheByRev removes an entry from rev cache by revID
func (rc *LRURevisionCache) removeFromCacheByRev(ctx context.Context, docID, revID string, collectionID uint32) {
	key := IDAndRev{DocID: docID, RevID: revID, CollectionID: collectionID}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.cache[key]
	if !ok {
		return
	}
	// grab the cv key from the value to enable us to remove the reference from the rev lookup map too
	elem := element.Value.(*revCacheValue)

	// we can only remove this value if the value has finished loading from the bucket
	_, _ = base.RetryLoop(ctx, "remove from revision cache by rev", func() (shouldRetry bool, err error, _ any) {
		if !elem.canEvict.Load() {
			// value is still in process of being loaded from bucket, we need to wait for this to finish
			return true, nil, nil
		}
		return false, nil, nil
	}, base.CreateMaxDoublingSleeperFunc(30, 10, 500))

	hlvKey := IDandCV{DocID: docID, Source: elem.cv.SourceID, Version: elem.cv.Value, CollectionID: collectionID}
	rc.lruList.Remove(element)
	// decrement the overall memory bytes count
	rc._decrRevCacheMemoryUsage(ctx, -elem.getItemBytes())
	delete(rc.cache, key)
	// remove from CV lookup map too
	delete(rc.hlvCache, hlvKey)
	rc.cacheNumItems.Add(-1)
}

// removeValue removes a value from the revision cache, if present and the value matches the the value. If there's an item in the revision cache with a matching docID and revID but the document is different, this item will not be removed from the rev cache.
func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	revKey := IDAndRev{DocID: value.id, RevID: value.revID}
	var itemRemoved bool
	if element := rc.cache[revKey]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, revKey)
		itemRemoved = true
	}
	// need to also check hlv lookup cache map
	hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value}
	if element := rc.hlvCache[hlvKey]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.hlvCache, hlvKey)
		itemRemoved = true
	}

	if itemRemoved {
		rc.cacheNumItems.Add(-1)
	}
}

// _numberCapacityEviction will iterate removing the last element in cache til we fall below the maximum number of items
// threshold for this shard, retuning the bytes evicted and number of items evicted
func (rc *LRURevisionCache) _numberCapacityEviction() (numItemsEvicted int64, numBytesEvicted int64) {
	for rc.lruList.Len() > int(rc.capacity) {
		value := rc._findEvictionValue()
		if value == nil {
			// no more ready for eviction
			break
		}
		hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value, CollectionID: value.collectionID}
		revKey := IDAndRev{DocID: value.id, RevID: value.revID, CollectionID: value.collectionID}
		delete(rc.cache, revKey)
		delete(rc.hlvCache, hlvKey)
		numItemsEvicted++
		numBytesEvicted += value.getItemBytes()
	}
	return numItemsEvicted, numBytesEvicted
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
		// we only want to set can evict if we are having to load the doc from the bucket into value,
		// avoiding setting this value multiple times in the case other goroutines are loading the same value
		defer func() {
			value.canEvict.Store(true) // once done loading doc we can set the value to be ready for eviction
		}()

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

	docRev, err = value.asDocumentRevision(delta)
	// if not cache hit, we loaded from bucket. Calculate doc rev size and assign to rev cache value
	if !cacheHit {
		docRev.CalculateBytes()
		value.itemBytes.Store(docRev.MemoryBytes)
	}
	value.lock.Unlock()

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
		// we only want to set can evict if we are having to load the doc from the bucket into value,
		// avoiding setting this value multiple times in the case other goroutines are loading the same value
		defer func() {
			value.canEvict.Store(true) // once done loading doc we can set the value to be ready for eviction
		}()

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
	docRev, err = value.asDocumentRevision(nil)
	// if not cache hit, we loaded from bucket. Calculate doc rev size and assign to rev cache value
	if !cacheHit {
		docRev.CalculateBytes()
		value.itemBytes.Store(docRev.MemoryBytes)
	}
	value.lock.Unlock()
	return docRev, cacheHit, err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(docRev DocumentRevision) {
	value.lock.Lock()
	if value.bodyBytes == nil {
		value.revID = docRev.RevID
		value.id = docRev.DocID
		value.bodyBytes = docRev.BodyBytes
		value.history = docRev.History
		value.channels = docRev.Channels
		value.expiry = docRev.Expiry
		value.attachments = docRev.Attachments.ShallowCopy() // Don't store attachments the caller might later mutate
		value.deleted = docRev.Deleted
		value.err = nil
		value.itemBytes.Store(docRev.MemoryBytes)
		value.hlvHistory = docRev.hlvHistory
	}
	value.lock.Unlock()
	value.canEvict.Store(true) // now we have stored the doc revision in the cache, we can allow eviction
}

func (value *revCacheValue) updateDelta(toDelta RevisionDelta) (diffInBytes int64) {
	value.lock.Lock()
	var previousDeltaBytes int64
	if value.delta != nil {
		// delta exists, need to pull this to update overall memory size correctly
		previousDeltaBytes = value.delta.totalDeltaBytes
	}
	diffInBytes = toDelta.totalDeltaBytes - previousDeltaBytes
	value.delta = &toDelta
	if diffInBytes != 0 {
		value.itemBytes.Add(diffInBytes)
	}
	value.lock.Unlock()
	return diffInBytes
}

// getItemBytes atomically retrieves the rev cache items overall memory footprint
func (value *revCacheValue) getItemBytes() int64 {
	return value.itemBytes.Load()
}

// CalculateBytes will calculate the bytes from revisions in the document, body and channels on the document
func (rev *DocumentRevision) CalculateBytes() {
	var totalBytes int
	for v := range rev.Channels {
		bytes := len([]byte(v))
		totalBytes += bytes
	}
	// calculate history
	digests, _ := GetStringArrayProperty(rev.History, RevisionsIds)
	historyBytes := 32 * len(digests)
	totalBytes += historyBytes
	// add body bytes into calculation
	totalBytes += len(rev.BodyBytes)

	// convert the int to int64 type and assign to document revision
	rev.MemoryBytes = int64(totalBytes)
}

// CalculateDeltaBytes will calculate bytes from delta channels, delta revisions and delta body
func (delta *RevisionDelta) CalculateDeltaBytes() {
	var totalBytes int
	for v := range delta.ToChannels {
		bytes := len([]byte(v))
		totalBytes += bytes
	}
	// history calculation
	historyBytes := 32 * len(delta.RevisionHistory)
	totalBytes += historyBytes

	// account for delta body
	totalBytes += len(delta.DeltaBytes)

	delta.totalDeltaBytes = int64(totalBytes)
}

// revCacheMemoryBasedEviction checks for rev cache eviction, if required calls performEviction which will acquire lock to evict
func (rc *LRURevisionCache) revCacheMemoryBasedEviction(ctx context.Context) {
	// if memory capacity is not set, don't check for eviction this way
	if rc.memoryCapacity > 0 && rc.currMemoryUsage.Value() > rc.memoryCapacity {
		rc.performEviction(ctx)
	}
}

// performEviction will evict the oldest items in the cache till we are below the memory threshold
func (rc *LRURevisionCache) performEviction(ctx context.Context) {
	var numItemsRemoved, numBytesRemoved int64
	rc.lock.Lock() // hold rev cache lock to remove items from cache until we're below memory threshold for the shard
	// check if we are over memory capacity after holding rev cache mutex (protect against another goroutine evicting whilst waiting for mutex above)
	if currMemoryUsage := rc.currMemoryUsage.Value(); currMemoryUsage > rc.memoryCapacity {
		// find amount of bytes needed to evict till below threshold
		bytesNeededToRemove := currMemoryUsage - rc.memoryCapacity
		for bytesNeededToRemove > numBytesRemoved {
			value := rc._findEvictionValue()
			if value == nil {
				// no more values ready for eviction
				break
			}
			revKey := IDAndRev{DocID: value.id, RevID: value.revID, CollectionID: value.collectionID}
			hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value, CollectionID: value.collectionID}
			delete(rc.cache, revKey)
			delete(rc.hlvCache, hlvKey)
			numItemsRemoved++
			valueBytes := value.getItemBytes()
			numBytesRemoved += valueBytes
		}
	}
	rc._decrRevCacheMemoryUsage(ctx, -numBytesRemoved) // need update rev cache memory stats before release lock to stop other goroutines evicting based on outdated stats
	rc.lock.Unlock()                                   // release lock after removing items from cache
	rc.cacheNumItems.Add(-numItemsRemoved)
}

// _decrRevCacheMemoryUsage atomically decreases overall memory usage for cache and the actual rev cache objects usage.
// You should be holding rev cache lock in using this function to avoid eviction processes over evicting items
func (rc *LRURevisionCache) _decrRevCacheMemoryUsage(ctx context.Context, bytesCount int64) {
	// We need to keep track of the current LRURevisionCache memory usage AND the overall usage of the cache. We need
	// overall memory usage for the stat added to show rev cache usage plus we need the current rev cache capacity of the
	// LRURevisionCache object for sharding the rev cache. This way we can perform eviction on per shard basis much like
	// we do with the number of items capacity eviction
	if bytesCount > 0 {
		// function is for decrementing memory usage, so return if incrementing
		base.AssertfCtx(ctx, "Attempting to increment memory stats with inside decrement function, should use incrRevCacheMemoryUsage instead")
		return
	}
	rc.currMemoryUsage.Add(bytesCount)
	rc.cacheMemoryBytesStat.Add(bytesCount)
}

// incrRevCacheMemoryUsage atomically increases overall memory usage for cache and the actual rev cache objects usage.
// You do not need to hold rev cache lock when using this function
func (rc *LRURevisionCache) incrRevCacheMemoryUsage(ctx context.Context, bytesCount int64) {
	// We need to keep track of the current LRURevisionCache memory usage AND the overall usage of the cache. We need
	// overall memory usage for the stat added to show rev cache usage plus we need the current rev cache capacity of the
	// LRURevisionCache object for sharding the rev cache. This way we can perform eviction on per shard basis much like
	// we do with the number of items capacity eviction
	if bytesCount < 0 {
		// function is for incrementing memory usage, so return if decrementing
		base.AssertfCtx(ctx, "Attempting to decrement memory stats with inside increment function, should use _decrRevCacheMemoryUsage instead whilst holding rev cache lock")
		return
	}
	rc.currMemoryUsage.Add(bytesCount)
	rc.cacheMemoryBytesStat.Add(bytesCount)
}

func (rc *LRURevisionCache) _findEvictionValue() *revCacheValue {
	evictionCandidate := rc.lruList.Back()
	revItem := evictionCandidate.Value.(*revCacheValue)

	if revItem.canEvict.Load() {
		rc.lruList.Remove(evictionCandidate)
		return revItem
	}

	// iterate through list backwards to find value ready for eviction
	evictionCandidate = evictionCandidate.Prev()
	for evictionCandidate != nil {
		revItem = evictionCandidate.Value.(*revCacheValue)
		if revItem.canEvict.Load() {
			rc.lruList.Remove(evictionCandidate)
			return revItem
		}
		// check prev value
		evictionCandidate = evictionCandidate.Prev()
	}
	return nil
}

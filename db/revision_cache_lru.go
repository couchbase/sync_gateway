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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

type ShardedLRURevisionCache struct {
	caches    []*RevisionCacheOrchestrator
	numShards uint16
}

// Creates a sharded revision cache with the given capacity and an optional loader function.
func NewShardedLRURevisionCache(revCacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, revCacheStats revisionCacheStats, deltaSyncStats *base.DeltaSyncStats, initDeltaCache bool) *ShardedLRURevisionCache {

	caches := make([]*RevisionCacheOrchestrator, revCacheOptions.ShardCount)
	// Add 10% to per-shared cache capacity to ensure overall capacity is reached under non-ideal shard hashing
	perCacheCapacity := 1.1 * float32(revCacheOptions.MaxItemCount) / float32(revCacheOptions.ShardCount)
	revCacheOptions.MaxItemCount = uint32(perCacheCapacity)
	var perCacheMemoryCapacity float32
	if revCacheOptions.MaxBytes > 0 {
		perCacheMemoryCapacity = float32(revCacheOptions.MaxBytes) / float32(revCacheOptions.ShardCount)
		revCacheOptions.MaxBytes = int64(perCacheMemoryCapacity)
	}
	for i := 0; i < int(revCacheOptions.ShardCount); i++ {
		caches[i] = NewRevisionCacheOrchestrator(revCacheOptions, backingStores, revCacheStats, deltaSyncStats, initDeltaCache)
	}

	return &ShardedLRURevisionCache{
		caches:    caches,
		numShards: revCacheOptions.ShardCount,
	}
}

func (sc *ShardedLRURevisionCache) getShard(docID string) *RevisionCacheOrchestrator {
	return sc.caches[sgbucket.VBHash(docID, sc.numShards)]
}

func (sc *ShardedLRURevisionCache) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (docRev DocumentRevision, b bool, err error) {
	return sc.getShard(docID).Get(ctx, docID, versionString, collectionID, loadBackup)
}

func (sc *ShardedLRURevisionCache) Peek(ctx context.Context, docID string, versionString string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return sc.getShard(docID).Peek(ctx, docID, versionString, collectionID)
}

func (sc *ShardedLRURevisionCache) UpdateDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32, toDelta RevisionDelta) {
	sc.getShard(docID).UpdateDelta(ctx, docID, fromVersionString, toVersionString, collectionID, toDelta)
}

func (sc *ShardedLRURevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, checkForMemoryEviction bool, err error) {
	return sc.getShard(docID).GetActive(ctx, docID, collectionID)
}

func (sc *ShardedLRURevisionCache) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	sc.getShard(docRev.DocID).Put(ctx, docRev, collectionID)
}

func (sc *ShardedLRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	sc.getShard(docRev.DocID).Upsert(ctx, docRev, collectionID)
}

func (sc *ShardedLRURevisionCache) Remove(ctx context.Context, docID, versionString string, collectionID uint32) {
	sc.getShard(docID).Remove(ctx, docID, versionString, collectionID)
}

func (sc *ShardedLRURevisionCache) GetWithDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) (DocumentRevision, error) {
	return sc.getShard(docID).GetWithDelta(ctx, docID, fromVersionString, toVersionString, collectionID)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	backingStores    map[uint32]RevisionCacheBackingStore
	cache            map[revCacheKey]*list.Element
	memoryController *CacheMemoryController // used to control memory usage of revision cache and delta cache combined
	lruList          *list.List
	cacheHits        *base.SgwIntStat
	cacheMisses      *base.SgwIntStat
	cacheNumItems    *base.SgwIntStat
	lock             sync.Mutex
	capacity         uint32 // Max number of items capacity of LRURevisionCache
}

// Memory accounting lifecycle states for revCacheValue.
// The transitions are:
//
//	memStateLoading → memStateSized  (CAS in Get/Put after bytes are known and stat incremented)
//	memStateLoading → memStateRemoved (Swap in Remove/eviction before increment happened)
//	memStateSized   → memStateRemoved (Swap in Remove/eviction after stat was incremented)
const (
	memStateLoading = int32(0) // item is in the cache but bytes have not yet been accounted in the memory stat
	memStateSized   = int32(1) // bytes have been accounted; a future Remove/eviction must decrement
	memStateRemoved = int32(2) // item has been removed; any in-flight CAS to memStateSized will fail
)

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	err          error
	history      Revisions
	channels     base.Set
	expiry       *time.Time
	attachments  AttachmentsMeta
	id           string
	itemKey      revCacheKey
	cv           Version
	hlvHistory   string
	revID        string
	bodyBytes    []byte
	lock         sync.RWMutex
	deleted      bool
	removed      bool
	itemBytes    atomic.Int64
	collectionID uint32
	memState     atomic.Int32  // memory accounting lifecycle; see memStateXxx constants
	deltaLock    sync.Mutex    // synchronizes GetDelta across multiple clients for each fromRevision
	accessOrder  atomic.Uint64 // stamped on insert; used for cross-cache eviction ordering
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(revCacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, revCacheStats revisionCacheStats, memoryController *CacheMemoryController) *LRURevisionCache {

	return &LRURevisionCache{
		cache:            map[revCacheKey]*list.Element{},
		lruList:          list.New(),
		capacity:         revCacheOptions.MaxItemCount,
		backingStores:    backingStores,
		cacheHits:        revCacheStats.cacheHitStat,
		cacheMisses:      revCacheStats.cacheMissStat,
		cacheNumItems:    revCacheStats.cacheNumItemsStat,
		memoryController: memoryController,
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get. Any successful load will return true for
// flag to check for memory based eviction.
func (rc *LRURevisionCache) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (DocumentRevision, bool, error) {
	value := rc.getValue(ctx, docID, versionString, collectionID, true)
	if value == nil {
		return DocumentRevision{}, false, nil
	}

	docRev, cacheHit, err := value.load(ctx, rc.backingStores[collectionID], loadBackup)
	rc.statsRecorderFunc(cacheHit)

	incrementStatEvent := !cacheHit && err == nil
	if incrementStatEvent {
		// Transition to memStateSized under a CAS so that a concurrent Remove that ran before
		// the load completed (reading itemBytes=0) cannot result in a permanently inflated stat.
		// If Remove already set memStateRemoved the CAS fails and we skip the increment — the
		// item is gone and its bytes were never decremented, so no increment is needed either.
		if !value.memState.CompareAndSwap(memStateLoading, memStateSized) {
			incrementStatEvent = false
		} else {
			rc.memoryController.incrementBytesCount(value.getItemBytes())
		}
	}

	if err != nil {
		rc.removeValueForFailedLoad(value) // don't keep failed loads in the cache
	}

	return docRev, incrementStatEvent, err
}

func (rc *LRURevisionCache) GetWithDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) (DocumentRevision, error) {
	// GetWithDelta is not supported directly by LRURevisionCache;
	// use RevisionCacheOrchestrator for delta support.
	return DocumentRevision{}, errors.New("delta retrieval not supported via LRURevisionCache, use RevisionCacheOrchestrator")
}

// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) Peek(ctx context.Context, docID string, versionString string, collectionID uint32) (docRev DocumentRevision, found bool) {
	value := rc.peekCacheForKey(ctx, CreateRevisionCacheKey(docID, versionString, collectionID))
	if value == nil {
		return DocumentRevision{}, false
	}
	docRev, err := value.asDocumentRevision(nil)
	if err != nil {
		return DocumentRevision{}, false
	}
	return docRev, docRev.BodyBytes != nil
}

func (rc *LRURevisionCache) UpdateDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32, toDelta RevisionDelta) {
	// no-op
}

// Attempts to retrieve the active revision for a document from the cache.  Requires retrieval
// of the document from the bucket to guarantee the current active revision, but does minimal unmarshalling
// of the retrieved document to get the current rev from _sync metadata.  If active rev is already in the
// rev cache, will use it.  Otherwise will add to the rev cache using the raw document obtained in the
// initial retrieval. Returns document revision, bool to indicate to caller whether we should check for memory based
// eviction or not, and any error.
func (rc *LRURevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (DocumentRevision, bool, error) {

	// Look up active rev for doc.  Note - can't rely on DocUnmarshalAll here when includeBody=true, because for a
	// cache hit we don't want to do that work (yet).
	bucketDoc, getErr := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if getErr != nil {
		return DocumentRevision{}, false, getErr
	}
	if bucketDoc == nil {
		return DocumentRevision{}, false, nil
	}

	// Retrieve from or add to rev cache
	// We need to use revisionID here as not every document in the bucket is yet guaranteed to
	// have a HLV assigned to it.
	value := rc.getValue(ctx, docID, bucketDoc.GetRevTreeID(), collectionID, true)

	docRev, cacheHit, err := value.loadForDoc(ctx, rc.backingStores[collectionID], bucketDoc)
	rc.statsRecorderFunc(cacheHit)

	incrementStatEvent := !cacheHit && err == nil
	if incrementStatEvent {
		// Transition to memStateSized under a CAS so that a concurrent Remove that ran before
		// the load completed (reading itemBytes=0) cannot result in a permanently inflated stat.
		// If Remove already set memStateRemoved the CAS fails and we skip the increment — the
		// item is gone and its bytes were never decremented, so no increment is needed either.
		if !value.memState.CompareAndSwap(memStateLoading, memStateSized) {
			incrementStatEvent = false
		} else {
			rc.memoryController.incrementBytesCount(value.getItemBytes())
		}
	}

	if err != nil {
		rc.removeValueForFailedLoad(value) // don't keep failed loads in the cache
	}

	return docRev, incrementStatEvent, err
}

func (rc *LRURevisionCache) statsRecorderFunc(cacheHit bool) {

	if cacheHit {
		rc.cacheHits.Add(1)
	} else {
		rc.cacheMisses.Add(1)
	}
}

// Put adds a revision to the cache. NOTE: this function only adds an entry keyed by CV
func (rc *LRURevisionCache) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	if docRev.History == nil {
		// TODO: CBG-1948
		panic("Missing history for RevisionCache.Put")
	}

	value := rc.getValue(ctx, docRev.DocID, docRev.CV.String(), collectionID, true)
	// increment incoming bytes
	docRev.CalculateBytes()
	// Store itemBytes on the value before the CAS so that a concurrent Remove or eviction
	// that wins the Swap to memStateRemoved will read the correct size.
	value.itemBytes.Store(docRev.MemoryBytes)
	// CAS guards against double-counting if this key already exists and is memStateSized,
	// and against incrementing for an item that was concurrently removed (memStateRemoved).
	if value.memState.CompareAndSwap(memStateLoading, memStateSized) {
		rc.memoryController.incrementBytesCount(docRev.MemoryBytes)
	}
	value.store(docRev)
}

// Upsert a revision in the cache. This function only upserts for CV key
func (rc *LRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	cvKey := CreateRevisionCacheKey(docRev.DocID, docRev.CV.String(), collectionID)

	numItemsRemoved, cvValue := rc.upsertDocToCache(ctx, cvKey, docRev, collectionID)
	if numItemsRemoved > 0 {
		rc.cacheNumItems.Add(-numItemsRemoved)
	}

	docRev.CalculateBytes()
	// Store itemBytes before the CAS for the same reason as in Put.
	cvValue.itemBytes.Store(docRev.MemoryBytes)
	// CAS guards against incrementing for a value that was concurrently removed.
	if cvValue.memState.CompareAndSwap(memStateLoading, memStateSized) {
		rc.memoryController.incrementBytesCount(docRev.MemoryBytes)
	}
	cvValue.store(docRev)
}

func (rc *LRURevisionCache) upsertDocToCache(ctx context.Context, cvKey revCacheKey, docRev DocumentRevision, collectionID uint32) (int64, *revCacheValue) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	newItem := true
	// lookup for element in hlv lookup map, if not found for some reason try rev lookup map
	var existingElem *list.Element
	var found bool
	existingElem, found = rc.cache[cvKey]
	if found {
		revItem := existingElem.Value.(*revCacheValue)
		// Swap to removed and only decrement if bytes were already accounted (memStateSized).
		// If the old item was still loading its increment CAS will now fail, so no decrement needed.
		if revItem.memState.Swap(memStateRemoved) == memStateSized {
			rc.memoryController.decrementBytesCount(revItem.getItemBytes())
		}
		rc.lruList.Remove(existingElem)
		newItem = false
	}

	// Add new value and overwrite existing cache key, pushing to front to maintain order
	// also ensure we add to rev id lookup map too
	cvValue := &revCacheValue{id: docRev.DocID, cv: *docRev.CV, collectionID: collectionID, itemKey: cvKey}
	elem := rc.lruList.PushFront(cvValue)
	cvValue.accessOrder.Store(nextAccessOrder()) // stamp on insert
	rc.cache[cvKey] = elem

	// only increment if we are inserting new item to cache
	if newItem {
		rc.cacheNumItems.Add(1)
	}

	// Purge oldest item if over number capacity
	numItemsRemoved, numBytesEvicted := rc._numberCapacityEviction()
	if numBytesEvicted > 0 {
		rc.memoryController.decrementBytesCount(numBytesEvicted)
	}
	return numItemsRemoved, cvValue
}

func (rc *LRURevisionCache) peekCacheForKey(ctx context.Context, key revCacheKey) (value *revCacheValue) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	}
	return
}

func (rc *LRURevisionCache) getValue(ctx context.Context, docID, docVersionString string, collectionID uint32, create bool) (value *revCacheValue) {
	if docID == "" || docVersionString == "" {
		return nil
	}
	key := CreateRevisionCacheKey(docID, docVersionString, collectionID)
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{id: docID, collectionID: collectionID, itemKey: key}
		if currentVersion, parseErr := ParseVersion(docVersionString); parseErr != nil {
			value.revID = docVersionString
		} else {
			value.cv = currentVersion
		}
		value.accessOrder.Store(nextAccessOrder()) // stamp on insert
		rc.cache[key] = rc.lruList.PushFront(value)
		rc.cacheNumItems.Add(1)

		numItemsRemoved, numBytesEvicted := rc._numberCapacityEviction()
		if numBytesEvicted > 0 {
			rc.memoryController.decrementBytesCount(numBytesEvicted)
		}
		if numItemsRemoved > 0 {
			rc.cacheNumItems.Add(-numItemsRemoved)
		}
	}
	return
}

// Remove removes a rev from revision cache lookup map, if present.
func (rc *LRURevisionCache) Remove(ctx context.Context, docID, versionString string, collectionID uint32) {
	key := CreateRevisionCacheKey(docID, versionString, collectionID)
	rc.lock.Lock()
	defer rc.lock.Unlock()
	elem, ok := rc.cache[key]
	if !ok {
		return
	}
	revValue, ok := elem.Value.(*revCacheValue)
	if !ok {
		return
	}
	rc.lruList.Remove(elem)
	rc.cacheNumItems.Add(-1)
	delete(rc.cache, key)
	// Swap to removed after the map/list are already updated.
	// Only decrement if bytes were already accounted (memStateSized); if the item was still
	// loading (memStateLoading) the in-flight CAS in Get/Put will fail and skip the increment.
	if revValue.memState.Swap(memStateRemoved) == memStateSized {
		rc.memoryController.decrementBytesCount(revValue.getItemBytes())
	}
}

// removeValueForFailedLoad removes a value after a failed load. Must only be called for failed loads;
// to remove a cached item use Remove. No memory decrement is needed because failed loads never reach
// the CAS that transitions to memStateSized, but we store memStateRemoved as a safety guard.
func (rc *LRURevisionCache) removeValueForFailedLoad(value *revCacheValue) {
	// Mark removed before acquiring the lock so any concurrent CAS-based increment sees the
	// terminal state and skips, even if it races with this function.
	value.memState.Store(memStateRemoved)
	rc.lock.Lock()
	defer rc.lock.Unlock()
	var itemRemoved bool
	if element := rc.cache[value.itemKey]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.itemKey)
		itemRemoved = true
	}
	if itemRemoved {
		rc.cacheNumItems.Add(-1)
	}
}

// _numberCapacityEviction will iterate removing the last element in cache til we fall below the maximum number of items
// threshold for this shard, returning the bytes evicted and number of items evicted.
func (rc *LRURevisionCache) _numberCapacityEviction() (numItemsEvicted int64, numBytesEvicted int64) {
	for rc.lruList.Len() > int(rc.capacity) {
		value := rc._findEvictionValue()
		if value == nil {
			// no more ready for eviction
			break
		}
		delete(rc.cache, value.itemKey)
		numItemsEvicted++
		// Only count bytes if they were already accounted in the stat (memStateSized).
		// If the item was still in memStateLoading the in-flight CAS will fail and skip
		// the increment, so there is nothing to decrement.
		if value.memState.Swap(memStateRemoved) == memStateSized {
			numBytesEvicted += value.getItemBytes()
		}
	}
	return numItemsEvicted, numBytesEvicted
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(ctx context.Context, backingStore RevisionCacheBackingStore, loadBackup bool) (docRev DocumentRevision, cacheHit bool, err error) {

	// Reading the delta from the revCacheValue requires holding the read lock, so it's managed outside asDocumentRevision,
	// to reduce locking when includeDelta=false
	var delta *RevisionDelta
	var revid string

	// Attempt to read cached value.
	value.lock.RLock()
	if value.bodyBytes != nil || value.err != nil {
		value.lock.RUnlock()

		docRev, err = value.asDocumentRevision(delta)

		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	defer value.lock.Unlock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.bodyBytes != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		hlv := &HybridLogicalVector{}
		if value.revID == "" {
			hlvKey := IDandCV{DocID: value.id, Source: value.cv.SourceID, Version: value.cv.Value}
			value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, revid, hlv, value.err = revCacheLoaderForCv(ctx, backingStore, hlvKey, loadBackup)
			// based off the current value load we need to populate the revid key with what has been fetched from the bucket (for use of populating the opposite lookup map)
			if revid != "" {
				value.revID = revid
			}
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

	docRev, err = value.asDocumentRevision(delta)
	// if not cache hit, we loaded from bucket. Calculate doc rev size and assign to rev cache value
	if !cacheHit && err == nil {
		docRev.CalculateBytes()
		value.itemBytes.Store(docRev.MemoryBytes)
	}

	return docRev, cacheHit, err
}

// asDocumentRevision copies the rev cache value into a DocumentRevision.  Should only be called for non-empty
// revCacheValues - copies all immutable revCacheValue properties, and adds the provided body/delta.
func (value *revCacheValue) asDocumentRevision(delta *RevisionDelta) (DocumentRevision, error) {

	docRev := DocumentRevision{
		DocID:                  value.id,
		RevID:                  value.revID,
		BodyBytes:              value.bodyBytes,
		History:                value.history,
		Channels:               value.channels,
		Expiry:                 value.expiry,
		Attachments:            value.attachments.ShallowCopy(), // Avoid caller mutating the stored attachments
		Delta:                  delta,
		Deleted:                value.deleted,
		Removed:                value.removed,
		HlvHistory:             value.hlvHistory,
		RevCacheValueDeltaLock: &value.deltaLock,
	}
	// only populate CV if we have a value
	if !value.cv.IsEmpty() {
		docRev.CV = &value.cv
	}

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
	defer value.lock.Unlock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.bodyBytes != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		hlv := &HybridLogicalVector{}
		if value.revID == "" {
			value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, revid, hlv, value.err = revCacheLoaderForDocumentCV(ctx, backingStore, doc, value.cv, false)
			if revid != "" {
				value.revID = revid
			}
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
	if !cacheHit && err == nil {
		docRev.CalculateBytes()
		value.itemBytes.Store(docRev.MemoryBytes)
	}
	return docRev, cacheHit, err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(docRev DocumentRevision) {
	value.lock.Lock()
	defer value.lock.Unlock()
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
		value.hlvHistory = docRev.HlvHistory
	}
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

func (rc *LRURevisionCache) _findEvictionValue() *revCacheValue {
	evictionCandidate := rc.lruList.Back()
	if evictionCandidate == nil {
		return nil
	}
	revItem, ok := evictionCandidate.Value.(*revCacheValue)
	if !ok {
		return nil
	}
	rc.lruList.Remove(evictionCandidate)
	return revItem
}

// peekLRUTailAccessOrder returns the accessOrder of the least-recently-used item,
// or 0 if the cache is empty.
// Called by the orchestrator to compare against the delta cache tail before deciding which to evict.
func (rc *LRURevisionCache) peekLRUTailAccessOrder() uint64 {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if elem := rc.lruList.Back(); elem != nil {
		if v, ok := elem.Value.(*revCacheValue); ok {
			return v.accessOrder.Load()
		}
	}
	return 0
}

// evictLRUTail removes the LRU loaded item and returns the bytes to decrement from the memory
// controller, or 0 if there was nothing eligible to evict or the item's bytes were not yet
// accounted (in which case the in-flight CAS in Get will fail, so no decrement is needed).
func (rc *LRURevisionCache) evictLRUTail() int64 {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	value := rc._findEvictionValue()
	if value == nil {
		return 0
	}
	delete(rc.cache, value.itemKey)
	rc.cacheNumItems.Add(-1)
	if value.memState.Swap(memStateRemoved) == memStateSized {
		return value.getItemBytes()
	}
	return 0
}

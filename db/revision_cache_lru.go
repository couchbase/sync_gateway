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

func (sc *ShardedLRURevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, cacheHit bool, err error) {
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
	itemLoaded   atomic.Bool
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
// any error returned by the loaderFunction will be returned from Get.
func (rc *LRURevisionCache) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (DocumentRevision, bool, error) {
	value := rc.getValue(ctx, docID, versionString, collectionID, true)
	if value == nil {
		return DocumentRevision{}, false, nil
	}

	docRev, cacheHit, err := value.load(ctx, rc.backingStores[collectionID], loadBackup)
	rc.statsRecorderFunc(cacheHit)

	incrementStatEvent := !cacheHit && err == nil
	if incrementStatEvent {
		// cache miss so we had to load doc, increment memory count
		rc.memoryController.incrementBytesCount(ctx, value.getItemBytes())
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
// initial retrieval.
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

	docRev, statEvent, err := value.loadForDoc(ctx, rc.backingStores[collectionID], bucketDoc)
	rc.statsRecorderFunc(statEvent)

	incrementStatEvent := !statEvent && err == nil
	if incrementStatEvent {
		// cache miss so we had to load doc, increment memory count
		rc.memoryController.incrementBytesCount(ctx, value.getItemBytes())
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
	rc.memoryController.incrementBytesCount(ctx, docRev.MemoryBytes)
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
	// add new item bytes to overall count
	rc.memoryController.incrementBytesCount(ctx, docRev.MemoryBytes)
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
		// decrement item bytes by the removed item
		rc.memoryController.decrementBytesCount(ctx, revItem.getItemBytes())
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
		rc.memoryController.decrementBytesCount(ctx, numBytesEvicted)
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
		value.itemLoaded.Store(false)
		value.accessOrder.Store(nextAccessOrder()) // stamp on insert
		rc.cache[key] = rc.lruList.PushFront(value)
		rc.cacheNumItems.Add(1)

		numItemsRemoved, numBytesEvicted := rc._numberCapacityEviction()
		if numBytesEvicted > 0 {
			rc.memoryController.decrementBytesCount(ctx, numBytesEvicted)
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
	rc.memoryController.decrementBytesCount(ctx, revValue.getItemBytes())
	rc.lruList.Remove(elem)
	rc.cacheNumItems.Add(-1)
	delete(rc.cache, key)
}

// removeValueForFailedLoad removes a value from after a failed load. NOTE this mist only be called for failed load, rto remove an item from the rev cache you must call Remove.
// No decrement of memory stats needed as failed loads don't increment memory stats.
func (rc *LRURevisionCache) removeValueForFailedLoad(value *revCacheValue) {
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
// threshold for this shard, retuning the bytes evicted and number of items evicted
func (rc *LRURevisionCache) _numberCapacityEviction() (numItemsEvicted int64, numBytesEvicted int64) {
	for rc.lruList.Len() > int(rc.capacity) {
		value := rc._findEvictionValue()
		if value == nil {
			// no more ready for eviction
			break
		}
		// delete item from lookup map
		delete(rc.cache, value.itemKey)
		numItemsEvicted++
		numBytesEvicted += value.getItemBytes()
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
		// we only want to set can evict if we are having to load the doc from the bucket into value,
		// avoiding setting this value multiple times in the case other goroutines are loading the same value
		defer func() {
			value.itemLoaded.Store(true) // once done loading doc we can set the value to be ready for eviction
		}()

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
	if !cacheHit {
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
		// we only want to set can evict if we are having to load the doc from the bucket into value,
		// avoiding setting this value multiple times in the case other goroutines are loading the same value
		defer func() {
			value.itemLoaded.Store(true) // once done loading doc we can set the value to be ready for eviction
		}()

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
	if !cacheHit {
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
	value.itemLoaded.Store(true) // now we have stored the doc revision in the cache, we can allow eviction
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

	if revItem.itemLoaded.Load() {
		rc.lruList.Remove(evictionCandidate)
		return revItem
	}

	// iterate through list backwards to find value ready for eviction
	evictionCandidate = evictionCandidate.Prev()
	for evictionCandidate != nil {
		revItem = evictionCandidate.Value.(*revCacheValue)
		if revItem.itemLoaded.Load() {
			rc.lruList.Remove(evictionCandidate)
			return revItem
		}
		// check prev value
		evictionCandidate = evictionCandidate.Prev()
	}
	return nil
}

// peekLRUTailAccessOrder returns the accessOrder of the least-recently-used loaded item,
// or 0 if the cache is empty or has no loaded items ready for eviction.
// Called by the orchestrator to compare against the delta cache tail before deciding which to evict.
func (rc *LRURevisionCache) peekLRUTailAccessOrder() uint64 {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	// Walk backwards to find a loaded item — mirrors _findEvictionValue but without removing.
	for elem := rc.lruList.Back(); elem != nil; elem = elem.Prev() {
		if v, ok := elem.Value.(*revCacheValue); ok && v.itemLoaded.Load() {
			return v.accessOrder.Load()
		}
	}
	return 0
}

// evictLRUTail removes the LRU loaded item and notifies the memory controller.
// Returns false if there was nothing eligible to evict.
func (rc *LRURevisionCache) evictLRUTail(ctx context.Context) int64 {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	value := rc._findEvictionValue() // already walks back to find a loaded item
	if value == nil {
		return 0
	}
	delete(rc.cache, value.itemKey)
	rc.cacheNumItems.Add(-1)
	return value.getItemBytes()
}

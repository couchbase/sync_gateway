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
func NewShardedLRURevisionCache(revCacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, cacheHitStat, cacheMissStat, cacheNumItemsStat, cacheMemoryStat *base.SgwIntStat) *ShardedLRURevisionCache {

	caches := make([]*LRURevisionCache, revCacheOptions.ShardCount)
	// Add 10% to per-shared cache capacity to ensure overall capacity is reached under non-ideal shard hashing
	perCacheCapacity := 1.1 * float32(revCacheOptions.MaxItemCount) / float32(revCacheOptions.ShardCount)
	revCacheOptions.MaxItemCount = uint32(perCacheCapacity)
	var perCacheMemoryCapacity float32
	if revCacheOptions.MaxBytes > 0 {
		perCacheMemoryCapacity = 1.1 * float32(revCacheOptions.MaxBytes) / float32(revCacheOptions.ShardCount)
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

func (sc *ShardedLRURevisionCache) Get(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).Get(ctx, docID, revID, collectionID, includeDelta)
}

func (sc *ShardedLRURevisionCache) Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return sc.getShard(docID).Peek(ctx, docID, revID, collectionID)
}

func (sc *ShardedLRURevisionCache) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	sc.getShard(docID).UpdateDelta(ctx, docID, revID, collectionID, toDelta)
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

func (sc *ShardedLRURevisionCache) Remove(docID, revID string, collectionID uint32) {
	sc.getShard(docID).Remove(docID, revID, collectionID)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	backingStores    map[uint32]RevisionCacheBackingStore
	cache            map[IDAndRev]*list.Element
	lruList          *list.List
	cacheHits        *base.SgwIntStat
	cacheMisses      *base.SgwIntStat
	cacheNumItems    *base.SgwIntStat
	lock             sync.Mutex
	capacity         uint32
	memoryCapacity   int64
	cacheMemoryBytes *base.SgwIntStat
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	err         error
	history     Revisions
	channels    base.Set
	expiry      *time.Time
	attachments AttachmentsMeta
	delta       *RevisionDelta
	key         IDAndRev
	bodyBytes   []byte
	lock        sync.RWMutex
	deleted     bool
	removed     bool
	itemBytes   int64
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(revCacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, cacheHitStat *base.SgwIntStat, cacheMissStat *base.SgwIntStat, cacheNumItemsStat *base.SgwIntStat, revCacheMemoryStat *base.SgwIntStat) *LRURevisionCache {

	return &LRURevisionCache{
		cache:            map[IDAndRev]*list.Element{},
		lruList:          list.New(),
		capacity:         revCacheOptions.MaxItemCount,
		backingStores:    backingStores,
		cacheHits:        cacheHitStat,
		cacheMisses:      cacheMissStat,
		cacheNumItems:    cacheNumItemsStat,
		cacheMemoryBytes: revCacheMemoryStat,
		memoryCapacity:   revCacheOptions.MaxBytes,
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get.
func (rc *LRURevisionCache) Get(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (DocumentRevision, error) {
	return rc.getFromCache(ctx, docID, revID, collectionID, true, includeDelta)
}

// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool) {
	docRev, err := rc.getFromCache(ctx, docID, revID, collectionID, false, RevCacheOmitDelta)
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
		outGoingBytes := value.updateDelta(toDelta)
		if outGoingBytes != 0 {
			rc.cacheMemoryBytes.Add(outGoingBytes)
		}
		// check for memory based eviction
		rc.revCacheMemoryBasedEviction()
	}
}

func (rc *LRURevisionCache) getFromCache(ctx context.Context, docID, revID string, collectionID uint32, loadOnCacheMiss, includeDelta bool) (DocumentRevision, error) {
	value := rc.getValue(docID, revID, collectionID, loadOnCacheMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}

	docRev, statEvent, err := value.load(ctx, rc.backingStores[collectionID], includeDelta)
	rc.statsRecorderFunc(statEvent)

	if !statEvent && err == nil {
		// cache miss so we had to load doc, increment memory count
		rc.cacheMemoryBytes.Add(value.getItemBytes())
		// check for memory based eviction
		rc.revCacheMemoryBasedEviction()
	}

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
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
	value := rc.getValue(docID, bucketDoc.CurrentRev, collectionID, true)

	docRev, statEvent, err := value.loadForDoc(ctx, rc.backingStores[collectionID], bucketDoc)
	rc.statsRecorderFunc(statEvent)

	if !statEvent && err == nil {
		// cache miss so we had to load doc, increment memory count
		rc.cacheMemoryBytes.Add(value.getItemBytes())
		// check for rev cache memory based eviction
		rc.revCacheMemoryBasedEviction()
	}

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
func (rc *LRURevisionCache) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	if docRev.History == nil {
		// TODO: CBG-1948
		panic("Missing history for RevisionCache.Put")
	}
	value := rc.getValue(docRev.DocID, docRev.RevID, collectionID, true)
	// increment incoming bytes
	docRev.CalculateBytes()
	rc.cacheMemoryBytes.Add(docRev.MemoryBytes)
	value.store(docRev)
	// check for rev cache memory based eviction
	rc.revCacheMemoryBasedEviction()
}

// Upsert a revision in the cache.
func (rc *LRURevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	key := IDAndRev{DocID: docRev.DocID, RevID: docRev.RevID, CollectionID: collectionID}

	rc.lock.Lock()
	newItem := true
	// If element exists remove from lrulist
	if elem := rc.cache[key]; elem != nil {
		revItem := elem.Value.(*revCacheValue)
		// decrement item bytes by the removed item
		rc.cacheMemoryBytes.Add(-revItem.getItemBytes())
		rc.lruList.Remove(elem)
		newItem = false
	}

	// Add new value and overwrite existing cache key, pushing to front to maintain order
	value := &revCacheValue{key: key}
	rc.cache[key] = rc.lruList.PushFront(value)
	// only increment if we are inserting new item to cache
	if newItem {
		rc.cacheNumItems.Add(1)
	}

	// Purge oldest item if over number capacity
	var numItemsRemoved int
	for len(rc.cache) > int(rc.capacity) {
		rc.purgeOldest_()
		numItemsRemoved++
	}
	if numItemsRemoved > 0 {
		rc.cacheNumItems.Add(int64(-numItemsRemoved))
	}

	docRev.CalculateBytes()
	// add new item bytes to overall count
	rc.cacheMemoryBytes.Add(docRev.MemoryBytes)
	value.store(docRev)

	// check we aren't over memory capacity, if so perform eviction
	numItemsRemoved = 0
	if rc.memoryCapacity > 0 {
		for rc.cacheMemoryBytes.Value() > rc.memoryCapacity {
			rc.purgeOldest_()
			numItemsRemoved++
		}
		rc.cacheNumItems.Add(int64(-numItemsRemoved))
	}

	rc.lock.Unlock()
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
		value = &revCacheValue{key: key}
		rc.cache[key] = rc.lruList.PushFront(value)
		rc.cacheNumItems.Add(1)

		// evict if over number capacity
		var numItemsRemoved int
		for len(rc.cache) > int(rc.capacity) {
			rc.purgeOldest_()
			numItemsRemoved++
		}
		if numItemsRemoved > 0 {
			rc.cacheNumItems.Add(int64(-numItemsRemoved))
		}
	}
	rc.lock.Unlock()
	return
}

// Remove removes a value from the revision cache, if present.
func (rc *LRURevisionCache) Remove(docID, revID string, collectionID uint32) {
	key := IDAndRev{DocID: docID, RevID: revID, CollectionID: collectionID}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	element, ok := rc.cache[key]
	if !ok {
		return
	}
	rc.lruList.Remove(element)
	// decrement the overall memory bytes count
	revItem := element.Value.(*revCacheValue)
	rc.cacheMemoryBytes.Add(-revItem.getItemBytes())
	delete(rc.cache, key)
	rc.cacheNumItems.Add(-1)
}

// removeValue removes a value from the revision cache, if present and the value matches the the value. If there's an item in the revision cache with a matching docID and revID but the document is different, this item will not be removed from the rev cache.
func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	if element := rc.cache[value.key]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.key)
		rc.cacheNumItems.Add(-1)
	}
	rc.lock.Unlock()
}

func (rc *LRURevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	delete(rc.cache, value.key)
	// decrement memory overall size
	rc.cacheMemoryBytes.Add(-value.getItemBytes())
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(ctx context.Context, backingStore RevisionCacheBackingStore, includeDelta bool) (docRev DocumentRevision, cacheHit bool, err error) {

	// Reading the delta from the revCacheValue requires holding the read lock, so it's managed outside asDocumentRevision,
	// to reduce locking when includeDelta=false
	var delta *RevisionDelta

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
		value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoader(ctx, backingStore, value.key)
	}

	if includeDelta {
		delta = value.delta
	}

	docRev, err = value.asDocumentRevision(delta)
	// if not cache hit, we loaded from bucket. Calculate doc rev size and assign to rev cache value
	if !cacheHit {
		docRev.CalculateBytes()
		value.itemBytes = docRev.MemoryBytes
	}
	value.lock.Unlock()

	return docRev, cacheHit, err
}

// asDocumentRevision copies the rev cache value into a DocumentRevision.  Should only be called for non-empty
// revCacheValues - copies all immutable revCacheValue properties, and adds the provided body/delta.
func (value *revCacheValue) asDocumentRevision(delta *RevisionDelta) (DocumentRevision, error) {

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
	}
	docRev.Delta = delta

	return docRev, value.err
}

// Retrieves the body etc. out of a revCacheValue.  If they aren't already present, loads into the cache value using
// the provided document.
func (value *revCacheValue) loadForDoc(ctx context.Context, backingStore RevisionCacheBackingStore, doc *Document) (docRev DocumentRevision, cacheHit bool, err error) {

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
		value.bodyBytes, value.history, value.channels, value.removed, value.attachments, value.deleted, value.expiry, value.err = revCacheLoaderForDocument(ctx, backingStore, doc, value.key.RevID)
	}
	docRev, err = value.asDocumentRevision(nil)
	// if not cache hit, we loaded from bucket. Calculate doc rev size and assign to rev cache value
	if !cacheHit {
		docRev.CalculateBytes()
		value.itemBytes = docRev.MemoryBytes
	}
	value.lock.Unlock()
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
		value.itemBytes = docRev.MemoryBytes
	}
	value.lock.Unlock()
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
		value.itemBytes += diffInBytes
	}
	value.lock.Unlock()
	return diffInBytes
}

// getItemBytes acquires read lock and retrieves the rev cache items overall memory footprint
func (value *revCacheValue) getItemBytes() int64 {
	value.lock.RLock()
	defer value.lock.RUnlock()
	return value.itemBytes
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
func (rc *LRURevisionCache) revCacheMemoryBasedEviction() {
	// if memory capacity is not set, don't check for eviction this way
	if rc.memoryCapacity > 0 && rc.cacheMemoryBytes.Value() > rc.memoryCapacity {
		rc.performEviction()
	}
}

// performEviction will evict the oldest items in the cache till we are below the memory threshold
func (rc *LRURevisionCache) performEviction() {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	var numItemsRemoved int
	for rc.cacheMemoryBytes.Value() > rc.memoryCapacity {
		rc.purgeOldest_()
		numItemsRemoved++
	}
	rc.cacheNumItems.Add(int64(-numItemsRemoved))
}

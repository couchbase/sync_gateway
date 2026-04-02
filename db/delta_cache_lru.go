/*
Copyright 2026-Present Couchbase, Inc.

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

	"github.com/couchbase/sync_gateway/base"
)

type LRUDeltaCache struct {
	cache            map[deltaCacheKey]*list.Element
	memoryController *CacheMemoryController // used to control memory usage of revision cache and delta cache combined
	lruList          *list.List
	cacheNumDeltas   *base.SgwIntStat
	lock             sync.Mutex
	capacity         uint32 // Max number of items capacity of LRUDeltaCache
}

// deltaCacheValue is the delta cache payload data. Stored as the Value of a list Element.
type deltaCacheValue struct {
	delta       *RevisionDelta
	itemKey     deltaCacheKey
	accessOrder atomic.Uint64 // stamped on insert; used for cross-cache eviction ordering
}

// deltaCacheKey is used to key the lookup map to delta cache items
type deltaCacheKey struct {
	docID          string
	toDocVersion   string
	fromDocVersion string
	collectionID   uint32
}

func createDeltaCacheKey(docID string, fromVersionString, toVersionString string, collectionID uint32) deltaCacheKey {
	return deltaCacheKey{docID: docID, fromDocVersion: fromVersionString, toDocVersion: toVersionString, collectionID: collectionID}
}

func NewLRUDeltaCache(revCacheOptions *RevisionCacheOptions, deltaSyncStats *base.DeltaSyncStats, memoryController *CacheMemoryController) *LRUDeltaCache {
	return &LRUDeltaCache{
		cache:            map[deltaCacheKey]*list.Element{},
		lruList:          list.New(),
		capacity:         revCacheOptions.MaxItemCount,
		cacheNumDeltas:   deltaSyncStats.DeltaCacheNumItems,
		memoryController: memoryController,
	}
}

// addDelta will cache a newly generated delta
func (dc *LRUDeltaCache) addDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32, toDelta RevisionDelta) {
	if docID == "" || fromVersionString == "" || toVersionString == "" {
		return
	}
	key := createDeltaCacheKey(docID, fromVersionString, toVersionString, collectionID)
	dc.lock.Lock()
	defer dc.lock.Unlock()
	if elem := dc.cache[key]; elem != nil {
		dc.lruList.MoveToFront(elem)
		return
	}
	value := &deltaCacheValue{delta: &toDelta, itemKey: key}
	elem := dc.lruList.PushFront(value)
	dc.cache[key] = elem
	dc.cacheNumDeltas.Add(1)
	value.accessOrder.Store(nextAccessOrder()) // stamp on insert
	numItemsRemoved, bytesEvicted := dc._numberCapacityEviction()
	if numItemsRemoved > 0 {
		dc.cacheNumDeltas.Add(-numItemsRemoved)
	}
	if dc.memoryController != nil {
		dc.memoryController.incrementBytesCount(value.delta.totalDeltaBytes)
		if bytesEvicted > 0 {
			dc.memoryController.decrementBytesCount(bytesEvicted)
		}
	}
}

// _numberCapacityEviction will iterate removing the last element in cache til we fall below the maximum number of items
// threshold for this shard, returning the number of items evicted
func (dc *LRUDeltaCache) _numberCapacityEviction() (numItemsEvicted int64, bytesEvicted int64) {
	for dc.lruList.Len() > int(dc.capacity) {
		value := dc.lruList.Back()
		if value == nil {
			// no items to remove
			break
		}
		deltaValue := value.Value.(*deltaCacheValue)
		dc.lruList.Remove(value)
		// delete item from lookup map
		delete(dc.cache, deltaValue.itemKey)
		numItemsEvicted++
		bytesEvicted += deltaValue.delta.totalDeltaBytes
	}
	return numItemsEvicted, bytesEvicted
}

// getCachedDelta will retrieve a delta if cached
func (dc *LRUDeltaCache) getCachedDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) *RevisionDelta {
	if docID == "" || fromVersionString == "" || toVersionString == "" {
		return nil
	}
	key := createDeltaCacheKey(docID, fromVersionString, toVersionString, collectionID)
	dc.lock.Lock()
	defer dc.lock.Unlock()
	var deltaValue *RevisionDelta
	if elem := dc.cache[key]; elem != nil {
		dc.lruList.MoveToFront(elem)
		cachedDelta := elem.Value.(*deltaCacheValue)
		deltaValue = cachedDelta.delta
	}
	return deltaValue
}

// CalculateDeltaBytes will calculate bytes from delta revisions and delta body
func (delta *RevisionDelta) CalculateDeltaBytes() {
	var totalBytes int
	// history calculation
	historyBytes := 32 * len(delta.RevisionHistory)
	totalBytes += historyBytes

	// account for delta body
	totalBytes += len(delta.DeltaBytes)

	delta.totalDeltaBytes = int64(totalBytes)
}

// peekLRUTailAccessOrder returns the accessOrder of the LRU item, or 0 if the cache is empty.
func (c *LRUDeltaCache) peekLRUTailAccessOrder() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	if elem := c.lruList.Back(); elem != nil {
		return elem.Value.(*deltaCacheValue).accessOrder.Load()
	}
	return 0
}

// evictLRUTail removes the LRU item and notifies the memory controller.
// Returns false if the cache was empty.
func (dc *LRUDeltaCache) evictLRUTail() int64 {
	dc.lock.Lock()
	defer dc.lock.Unlock()
	elem := dc.lruList.Back()
	if elem == nil {
		return 0
	}
	val := elem.Value.(*deltaCacheValue)
	dc.lruList.Remove(elem)
	delete(dc.cache, val.itemKey)
	dc.cacheNumDeltas.Add(-1)
	return val.delta.totalDeltaBytes
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

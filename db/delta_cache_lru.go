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

	"github.com/couchbase/sync_gateway/base"
)

type LRUDeltaCache struct {
	backingStores  map[uint32]RevisionCacheBackingStore
	cache          map[deltaCacheKey]*list.Element
	lruList        *list.List
	cacheHits      *base.SgwIntStat
	cacheMisses    *base.SgwIntStat
	cacheNumDeltas *base.SgwIntStat
	lock           sync.RWMutex
	capacity       uint32 // Max number of items capacity of LRUDeltaCache
}

// deltaCacheValue is the delta cache payload data. Stored as the Value of a list Element.
type deltaCacheValue struct {
	delta   *RevisionDelta
	itemKey deltaCacheKey
}

type deltaCacheKey struct {
	docID          string
	toDocVersion   string
	fromDocVersion string
	collectionID   uint32
}

func createDeltaCacheKey(docID string, fromVersionString, toVersionString string, collectionID uint32) deltaCacheKey {
	return deltaCacheKey{docID: docID, fromDocVersion: fromVersionString, toDocVersion: toVersionString, collectionID: collectionID}
}

func NewLRUDeltaCache(revCacheOptions *RevisionCacheOptions, deltaSyncStats *base.DeltaSyncStats, backingStores map[uint32]RevisionCacheBackingStore) *LRUDeltaCache {
	return &LRUDeltaCache{
		cache:          map[deltaCacheKey]*list.Element{},
		lruList:        list.New(),
		capacity:       revCacheOptions.MaxItemCount,
		backingStores:  backingStores,
		cacheHits:      deltaSyncStats.DeltaCacheHit,
		cacheMisses:    deltaSyncStats.DeltaCacheMiss,
		cacheNumDeltas: deltaSyncStats.DeltaCacheNumItems,
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
	numItemsRemoved := dc._numberCapacityEviction()
	if numItemsRemoved > 0 {
		dc.cacheNumDeltas.Add(-numItemsRemoved)
	}
}

// _numberCapacityEviction will iterate removing the last element in cache til we fall below the maximum number of items
// threshold for this shard, returning the number of items evicted
func (dc *LRUDeltaCache) _numberCapacityEviction() (numItemsEvicted int64) {
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
	}
	return numItemsEvicted
}

// getCachedDelta will retrieve a delta if cached
func (dc *LRUDeltaCache) getCachedDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) *RevisionDelta {
	if docID == "" || fromVersionString == "" || toVersionString == "" {
		return nil
	}
	key := createDeltaCacheKey(docID, fromVersionString, toVersionString, collectionID)
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	var deltaValue *RevisionDelta
	if elem := dc.cache[key]; elem != nil {
		dc.lruList.MoveToFront(elem)
		cachedDelta := elem.Value.(*deltaCacheValue)
		deltaValue = cachedDelta.delta
	}
	return deltaValue
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

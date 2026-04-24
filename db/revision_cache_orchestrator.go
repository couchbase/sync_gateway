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
	"context"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// RevisionCacheOrchestrator orchestrates between the revisionCache and a deltaCache.
type RevisionCacheOrchestrator struct {
	revisionCache    *LRURevisionCache      // holds document revisions
	deltaCache       *LRUDeltaCache         // holds computed deltas, only initialized when delta sync is enabled
	memoryController *CacheMemoryController // used to control memory usage of revision cache and delta cache combined
	evictionLock     sync.Mutex             // This is to synchronise the eviction process so we don't have multiple goroutines fighting to evict
	evictNextFromRev bool                   // round-robin flag: alternates which cache is tried first on each eviction
}

// NewRevisionCacheOrchestrator creates a new RevisionCacheOrchestrator.
func NewRevisionCacheOrchestrator(cacheOptions *RevisionCacheOptions, backingStores map[uint32]RevisionCacheBackingStore, revCacheStats revisionCacheStats, deltaSyncStats *base.DeltaSyncStats, initDeltaCache bool) *RevisionCacheOrchestrator {
	mc := newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat)
	revOrchestrator := &RevisionCacheOrchestrator{
		revisionCache:    NewLRURevisionCache(cacheOptions, backingStores, revCacheStats, mc),
		memoryController: mc,
	}
	if initDeltaCache {
		revOrchestrator.deltaCache = NewLRUDeltaCache(cacheOptions, deltaSyncStats, mc)
	}
	return revOrchestrator
}

func (c *RevisionCacheOrchestrator) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (DocumentRevision, bool, error) {
	docRev, checkForMemoryEviction, err := c.revisionCache.Get(ctx, docID, versionString, collectionID, loadBackup)
	if checkForMemoryEviction {
		c.triggerMemoryEviction()
	}
	return docRev, checkForMemoryEviction, err
}

func (c *RevisionCacheOrchestrator) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, checkForMemoryEviction bool, err error) {
	docRev, checkForMemoryEviction, err = c.revisionCache.GetActive(ctx, docID, collectionID)
	if checkForMemoryEviction {
		c.triggerMemoryEviction()
	}
	return docRev, checkForMemoryEviction, err
}

func (c *RevisionCacheOrchestrator) Peek(ctx context.Context, docID, versionString string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return c.revisionCache.Peek(ctx, docID, versionString, collectionID)
}

func (c *RevisionCacheOrchestrator) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Put(ctx, docRev, collectionID)
	c.triggerMemoryEviction()
}

func (c *RevisionCacheOrchestrator) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Upsert(ctx, docRev, collectionID)
	c.triggerMemoryEviction()
}

func (c *RevisionCacheOrchestrator) Remove(ctx context.Context, docID, versionString string, collectionID uint32) {
	c.revisionCache.Remove(ctx, docID, versionString, collectionID)
}

func (c *RevisionCacheOrchestrator) UpdateDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32, toDelta RevisionDelta) {
	if c.deltaCache == nil {
		return
	}
	c.deltaCache.addDelta(ctx, docID, fromVersionString, toVersionString, collectionID, toDelta)
	// check for memory based eviction
	c.triggerMemoryEviction()
}

func (c *RevisionCacheOrchestrator) GetWithDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) (DocumentRevision, error) {
	docRev, checkForMemoryEviction, err := c.revisionCache.Get(ctx, docID, fromVersionString, collectionID, RevCacheLoadBackupRev)
	if err != nil {
		return docRev, err
	}
	if c.deltaCache != nil {
		cachedDelta := c.deltaCache.getCachedDelta(ctx, docID, fromVersionString, toVersionString, collectionID)
		docRev.Delta = cachedDelta
	}
	// check for memory based eviction
	if checkForMemoryEviction {
		c.triggerMemoryEviction()
	}
	return docRev, nil
}

// triggerMemoryEviction is called after any write to either sub-cache.
func (c *RevisionCacheOrchestrator) triggerMemoryEviction() {
	if c.memoryController == nil || !c.memoryController.IsOverCapacity() {
		// no eviction to take place
		return
	}
	// Acquire lock till eviction is done. This is to protect against multiple goroutines attempting to
	// perform this at the same time on the shard.
	c.evictionLock.Lock()
	defer c.evictionLock.Unlock()

	var numBytesRemoved int64
	bytesNeededToEvict := c.memoryController.bytesToEvict()
	if bytesNeededToEvict == 0 {
		// a different goroutine has evicted enough already
		return
	}
	for numBytesRemoved < bytesNeededToEvict {
		bytes, evicted := c._evictOneItem()
		if !evicted {
			// both caches exhausted
			break
		}
		numBytesRemoved += bytes
	}
	c.memoryController.decrementBytesCount(numBytesRemoved)
}

// _evictOneItem removes one item from either the revision or delta cache using round-robin
// selection. If the primary cache is empty, it immediately falls back to the other cache.
// Returns (bytes freed, true) when an item was removed, or (0, false) when both are empty.
// Must only be called while holding evictionLock.
func (c *RevisionCacheOrchestrator) _evictOneItem() (int64, bool) {
	// When the delta cache is not enabled, always evict from the revision cache.
	if c.deltaCache == nil {
		return c.revisionCache.evictLRUTail()
	}

	// flip eviction toggle
	c.evictNextFromRev = !c.evictNextFromRev
	first, second := c.revisionCache.evictLRUTail, c.deltaCache.evictLRUTail
	if !c.evictNextFromRev {
		first, second = second, first
	}
	if bytes, evicted := first(); evicted {
		return bytes, true
	}
	return second()
}

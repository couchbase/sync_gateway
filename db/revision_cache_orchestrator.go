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
)

// RevisionCacheOrchestrator orchestrates between the revisionCache and a deltaCache.
type RevisionCacheOrchestrator struct {
	revisionCache    *LRURevisionCache      // holds document revisions
	deltaCache       *LRUDeltaCache         // holds computed deltas, only initialed when delta sync is enabled
	memoryController *CacheMemoryController // used to control memory usage of revision cache and delta cache combined
}

// NewRevisionCacheOrchestrator creates a new RevisionCacheOrchestrator.
func NewRevisionCacheOrchestrator(revisionCache *LRURevisionCache, deltaCache *LRUDeltaCache, memoryController *CacheMemoryController) *RevisionCacheOrchestrator {
	return &RevisionCacheOrchestrator{
		revisionCache:    revisionCache,
		deltaCache:       deltaCache,
		memoryController: memoryController,
	}
}

func (c *RevisionCacheOrchestrator) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (DocumentRevision, bool, error) {
	docRev, cacheHit, err := c.revisionCache.Get(ctx, docID, versionString, collectionID, loadBackup)
	if cacheHit {
		c.triggerMemoryEviction(ctx)
	}
	return docRev, cacheHit, err
}

func (c *RevisionCacheOrchestrator) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, cacheHit bool, err error) {
	docRev, cacheHit, err = c.revisionCache.GetActive(ctx, docID, collectionID)
	if cacheHit {
		c.triggerMemoryEviction(ctx)
	}
	return docRev, cacheHit, err
}

func (c *RevisionCacheOrchestrator) Peek(ctx context.Context, docID, versionString string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return c.revisionCache.Peek(ctx, docID, versionString, collectionID)
}

func (c *RevisionCacheOrchestrator) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Put(ctx, docRev, collectionID)
	c.triggerMemoryEviction(ctx)
}

func (c *RevisionCacheOrchestrator) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Upsert(ctx, docRev, collectionID)
	c.triggerMemoryEviction(ctx)
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
	c.triggerMemoryEviction(ctx)
}

func (c *RevisionCacheOrchestrator) GetWithDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) (DocumentRevision, error) {
	docRev, _, err := c.revisionCache.Get(ctx, docID, fromVersionString, collectionID, RevCacheLoadBackupRev)
	if err != nil {
		return docRev, err
	}
	if c.deltaCache != nil {
		cachedDelta := c.deltaCache.getCachedDelta(ctx, docID, fromVersionString, toVersionString, collectionID)
		docRev.Delta = cachedDelta
	}
	return docRev, nil
}

// triggerMemoryEviction is called after any write to either sub-cache.
func (c *RevisionCacheOrchestrator) triggerMemoryEviction(ctx context.Context) {
	if c.memoryController == nil || !c.memoryController.IsOverCapacity() {
		// no eviction to take place
		return
	}
	// do we need to syncdhirnise this????

	var numBytesRemoved int64
	var deltaCandidateOrder uint64
	bytesNeededToEvict := c.memoryController.bytesToEvict()
	for bytesNeededToEvict > numBytesRemoved {
		if c.deltaCache != nil {
			deltaCandidateOrder = c.deltaCache.peekLRUTailAccessOrder()
		}
		revCandidateOrder := c.revisionCache.peekLRUTailAccessOrder()
		if revCandidateOrder == 0 && deltaCandidateOrder == 0 {
			// Both caches are empty — nothing more to evict.
			//base.AssertfCtx(ctx, "memory")
			break
		}
		var bytesRemoved int64
		evictFromRev := deltaCandidateOrder == 0 || (revCandidateOrder != 0 && revCandidateOrder < deltaCandidateOrder)
		if evictFromRev {
			bytesRemoved = c.revisionCache.evictLRUTail(ctx)
		} else {
			bytesRemoved = c.deltaCache.evictLRUTail(ctx)
		}
		numBytesRemoved += bytesRemoved
		if bytesRemoved == 0 {
			// Nothing evictable in the chosen cache (e.g. all items still loading).
			// Try the other cache before giving up.
			if evictFromRev && c.deltaCache != nil {
				c.deltaCache.evictLRUTail(ctx)
			} else {
				c.revisionCache.evictLRUTail(ctx)
			}
			break
		}
	}
	c.memoryController.decrementBytesCount(ctx, numBytesRemoved)
}

// evictLRUTail removes the LRU item and notifies the memory controller.
// Returns false if the cache was empty.
func (dc *LRUDeltaCache) evictLRUTail(ctx context.Context) int64 {
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

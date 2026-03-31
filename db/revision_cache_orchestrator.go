/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import "context"

// RevisionCacheOrchestrator orchestrates between the revisionCache and a deltaCache.
type RevisionCacheOrchestrator struct {
	revisionCache *LRURevisionCache
	deltaCache    *LRUDeltaCache
}

// NewRevisionCacheOrchestrator creates a new RevisionCacheOrchestrator.
func NewRevisionCacheOrchestrator(revisionCache *LRURevisionCache, deltaCache *LRUDeltaCache) *RevisionCacheOrchestrator {
	return &RevisionCacheOrchestrator{
		revisionCache: revisionCache,
		deltaCache:    deltaCache,
	}
}

func (c *RevisionCacheOrchestrator) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (DocumentRevision, error) {
	return c.revisionCache.Get(ctx, docID, versionString, collectionID, loadBackup)
}

func (c *RevisionCacheOrchestrator) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, err error) {
	return c.revisionCache.GetActive(ctx, docID, collectionID)
}

func (c *RevisionCacheOrchestrator) Peek(ctx context.Context, docID, versionString string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return c.revisionCache.Peek(ctx, docID, versionString, collectionID)
}

func (c *RevisionCacheOrchestrator) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Put(ctx, docRev, collectionID)
}

func (c *RevisionCacheOrchestrator) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Upsert(ctx, docRev, collectionID)
}

func (c *RevisionCacheOrchestrator) Remove(ctx context.Context, docID, versionString string, collectionID uint32) {
	c.revisionCache.Remove(ctx, docID, versionString, collectionID)
}

func (c *RevisionCacheOrchestrator) UpdateDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32, toDelta RevisionDelta) {
	if c.deltaCache == nil {
		return
	}
	c.deltaCache.addDelta(ctx, docID, fromVersionString, toVersionString, collectionID, toDelta)
}

func (c *RevisionCacheOrchestrator) GetWithDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) (DocumentRevision, error) {
	docRev, err := c.revisionCache.Get(ctx, docID, fromVersionString, collectionID, RevCacheLoadBackupRev)
	if err != nil {
		return docRev, err
	}
	if c.deltaCache != nil {
		cachedDelta := c.deltaCache.getCachedDelta(ctx, docID, fromVersionString, toVersionString, collectionID)
		docRev.Delta = cachedDelta
	}
	return docRev, nil
}

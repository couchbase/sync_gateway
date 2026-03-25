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
	// delta cache to be implemented: CBG-5234
}

// NewCompositeRevisionCache creates a new RevisionCacheOrchestrator.
func NewCompositeRevisionCache(bodyCache *LRURevisionCache) *RevisionCacheOrchestrator {
	return &RevisionCacheOrchestrator{
		revisionCache: bodyCache,
	}
}

func (c *RevisionCacheOrchestrator) GetWithRev(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (DocumentRevision, error) {
	return c.revisionCache.GetWithRev(ctx, docID, revID, collectionID, includeDelta)
}

func (c *RevisionCacheOrchestrator) GetWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32, includeDelta bool, loadBackup bool) (DocumentRevision, error) {
	return c.revisionCache.GetWithCV(ctx, docID, cv, collectionID, includeDelta, loadBackup)
}

func (c *RevisionCacheOrchestrator) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, err error) {
	return c.revisionCache.GetActive(ctx, docID, collectionID)
}

func (c *RevisionCacheOrchestrator) Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return c.revisionCache.Peek(ctx, docID, revID, collectionID)
}

func (c *RevisionCacheOrchestrator) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Put(ctx, docRev, collectionID)
}

func (c *RevisionCacheOrchestrator) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	c.revisionCache.Upsert(ctx, docRev, collectionID)
}

func (c *RevisionCacheOrchestrator) RemoveWithRev(ctx context.Context, docID, revID string, collectionID uint32) {
	c.revisionCache.RemoveWithRev(ctx, docID, revID, collectionID)
}

func (c *RevisionCacheOrchestrator) RemoveWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	c.revisionCache.RemoveWithCV(ctx, docID, cv, collectionID)
}

func (c *RevisionCacheOrchestrator) RemoveRevOnly(ctx context.Context, docID, revID string, collectionID uint32) {
	c.revisionCache.RemoveRevOnly(ctx, docID, revID, collectionID)
}

func (c *RevisionCacheOrchestrator) RemoveCVOnly(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	c.revisionCache.RemoveCVOnly(ctx, docID, cv, collectionID)
}

func (c *RevisionCacheOrchestrator) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	c.revisionCache.UpdateDelta(ctx, docID, revID, collectionID, toDelta)
}

func (c *RevisionCacheOrchestrator) UpdateDeltaCV(ctx context.Context, docID string, cv *Version, collectionID uint32, toDelta RevisionDelta) {
	c.revisionCache.UpdateDeltaCV(ctx, docID, cv, collectionID, toDelta)
}

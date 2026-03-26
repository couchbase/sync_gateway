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

// NewRevisionCacheOrchestrator creates a new RevisionCacheOrchestrator.
func NewRevisionCacheOrchestrator(revisionCache *LRURevisionCache) *RevisionCacheOrchestrator {
	return &RevisionCacheOrchestrator{
		revisionCache: revisionCache,
	}
}

func (c *RevisionCacheOrchestrator) Get(ctx context.Context, docID, versionString string, collectionID uint32, includeDelta, loadBackup bool) (DocumentRevision, error) {
	return c.revisionCache.Get(ctx, docID, versionString, collectionID, includeDelta, loadBackup)
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

func (c *RevisionCacheOrchestrator) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	c.revisionCache.UpdateDelta(ctx, docID, revID, collectionID, toDelta)
}

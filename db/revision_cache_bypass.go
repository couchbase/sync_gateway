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
	"context"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// BypassRevisionCache is an implementation of the RevisionCache interface that does not perform any caching.
// For any Get operation, it will always immediately fetch the requested revision from the backing store.
type BypassRevisionCache struct {
	backingStores map[uint32]RevisionCacheBackingStore
	bypassStat    *base.SgwIntStat
}

func NewBypassRevisionCache(backingStores map[uint32]RevisionCacheBackingStore, bypassStat *base.SgwIntStat) *BypassRevisionCache {
	return &BypassRevisionCache{
		backingStores: backingStores,
		bypassStat:    bypassStat,
	}
}

// Get fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) Get(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (docRev DocumentRevision, err error) {

	doc, err := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID:                  revID,
		RevCacheValueDeltaLock: &sync.Mutex{}, // initialize the mutex for delta updates
	}
	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, revID)
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (docRev DocumentRevision, err error) {

	doc, err := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: doc.CurrentRev,
		RevCacheValueDeltaLock: &sync.Mutex{}, // initialize the mutex for delta updates
	}

	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, doc.SyncData.CurrentRev)
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// Peek is a no-op for a BypassRevisionCache, and always returns a false 'found' value.
func (rc *BypassRevisionCache) Peek(ctx context.Context, docID, revID string, collectionID uint32) (docRev DocumentRevision, found bool) {
	return DocumentRevision{}, false
}

// Put is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Put(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	// no-op
}

// Update is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Upsert(ctx context.Context, docRev DocumentRevision, collectionID uint32) {
	// no-op
}

func (rc *BypassRevisionCache) Remove(ctx context.Context, docID, revID string, collectionID uint32) {
	// nop
}

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	// no-op
}

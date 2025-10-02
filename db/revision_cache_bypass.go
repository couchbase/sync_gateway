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

// GetWithRev fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) GetWithRev(ctx context.Context, docID, revID string, collectionID uint32, includeDelta bool) (docRev DocumentRevision, err error) {
	doc, err := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: revID,
	}
	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, hlv, err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, revID)
	if err != nil {
		return DocumentRevision{}, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.HlvHistory = hlv.ToHistoryForHLV()
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetWithCV fetches the Current Version for the given docID and CV immediately from the bucket.
func (rc *BypassRevisionCache) GetWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32, includeDelta bool) (docRev DocumentRevision, err error) {

	docRev = DocumentRevision{
		CV: cv,
	}

	doc, err := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if err != nil {
		return DocumentRevision{}, err
	}

	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, docRev.RevID, hlv, err = revCacheLoaderForDocumentCV(ctx, rc.backingStores[collectionID], doc, *cv)
	if err != nil {
		return DocumentRevision{}, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.HlvHistory = hlv.ToHistoryForHLV()
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
		RevID: doc.GetRevTreeID(),
	}

	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, hlv, err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, doc.SyncData.GetRevTreeID())
	if err != nil {
		return DocumentRevision{}, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.HlvHistory = hlv.ToHistoryForHLV()
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

func (rc *BypassRevisionCache) RemoveWithRev(ctx context.Context, docID, revID string, collectionID uint32) {
	// no-op
}

func (rc *BypassRevisionCache) RemoveRevOnly(ctx context.Context, docID, revID string, collectionID uint32) {
	// no-op
}

func (rc *BypassRevisionCache) RemoveCVOnly(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	// no-op
}

func (rc *BypassRevisionCache) RemoveWithCV(ctx context.Context, docID string, cv *Version, collectionID uint32) {
	// no-op
}

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(ctx context.Context, docID, revID string, collectionID uint32, toDelta RevisionDelta) {
	// no-op
}

// UpdateDeltaCV is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDeltaCV(ctx context.Context, docID string, cv *Version, collectionID uint32, toDelta RevisionDelta) {
	// no-op
}

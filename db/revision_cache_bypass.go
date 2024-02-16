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
	backingStore RevisionCacheBackingStore
	bypassStat   *base.SgwIntStat
}

func NewBypassRevisionCache(backingStore RevisionCacheBackingStore, bypassStat *base.SgwIntStat) *BypassRevisionCache {
	return &BypassRevisionCache{
		backingStore: backingStore,
		bypassStat:   bypassStat,
	}
}

// GetWithRev fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) GetWithRev(ctx context.Context, docID, revID string, includeBody, includeDelta bool) (docRev DocumentRevision, err error) {

	unmarshalLevel := DocUnmarshalSync
	if includeBody {
		unmarshalLevel = DocUnmarshalAll
	}
	doc, err := rc.backingStore.GetDocument(ctx, docID, unmarshalLevel)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: revID,
	}
	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev._shallowCopyBody, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, hlv, err = revCacheLoaderForDocument(ctx, rc.backingStore, doc, revID)
	if err != nil {
		return DocumentRevision{}, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.hlvHistory = hlv.ToHistoryForHLV()
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetWithCV fetches the Current Version for the given docID and CV immediately from the bucket.
func (rc *BypassRevisionCache) GetWithCV(ctx context.Context, docID string, cv *Version, includeBody, includeDelta bool) (docRev DocumentRevision, err error) {

	unmarshalLevel := DocUnmarshalSync
	if includeBody {
		unmarshalLevel = DocUnmarshalAll
	}

	doc, err := rc.backingStore.GetDocument(ctx, docID, unmarshalLevel)
	if err != nil {
		return DocumentRevision{}, err
	}

	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev._shallowCopyBody, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, docRev.RevID, hlv, err = revCacheLoaderForDocumentCV(ctx, rc.backingStore, doc, *cv)
	if err != nil {
		return DocumentRevision{}, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.hlvHistory = hlv.ToHistoryForHLV()
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(ctx context.Context, docID string, includeBody bool) (docRev DocumentRevision, err error) {

	unmarshalLevel := DocUnmarshalSync
	if includeBody {
		unmarshalLevel = DocUnmarshalAll
	}
	doc, err := rc.backingStore.GetDocument(ctx, docID, unmarshalLevel)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: doc.CurrentRev,
	}

	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev._shallowCopyBody, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, hlv, err = revCacheLoaderForDocument(ctx, rc.backingStore, doc, doc.SyncData.CurrentRev)
	if err != nil {
		return DocumentRevision{}, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.hlvHistory = hlv.ToHistoryForHLV()
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// Peek is a no-op for a BypassRevisionCache, and always returns a false 'found' value.
func (rc *BypassRevisionCache) Peek(ctx context.Context, docID, revID string) (docRev DocumentRevision, found bool) {
	return DocumentRevision{}, false
}

// Put is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Put(ctx context.Context, docRev DocumentRevision) {
	// no-op
}

// Update is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Upsert(ctx context.Context, docRev DocumentRevision) {
	// no-op
}

func (rc *BypassRevisionCache) RemoveWithRev(docID, revID string) {
	// nop
}

func (rc *BypassRevisionCache) RemoveWithCV(docID string, cv *Version) {
	// nop
}

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(ctx context.Context, docID, revID string, toDelta RevisionDelta) {
	// no-op
}

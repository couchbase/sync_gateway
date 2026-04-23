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

// Get fetches the revision for the given docID and revID immediately from the bucket. Always returns false for checking
// for memory based eviction as there is nothing to evict from when using bypass revision cache.
func (rc *BypassRevisionCache) Get(ctx context.Context, docID, versionString string, collectionID uint32, loadBackup bool) (docRev DocumentRevision, b bool, err error) {
	doc, err := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if err != nil {
		return DocumentRevision{}, false, err
	}

	docRev = DocumentRevision{
		DocID:                  docID,
		RevCacheValueDeltaLock: &sync.Mutex{}, // initialize the mutex for delta updates
	}
	if version, err := ParseVersion(versionString); err != nil {
		docRev.RevID = versionString
	} else {
		docRev.CV = &version
	}

	var hlv *HybridLogicalVector
	if docRev.RevID != "" {
		docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, hlv, err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, versionString)
	} else {
		docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, docRev.RevID, hlv, err = revCacheLoaderForDocumentCV(ctx, rc.backingStores[collectionID], doc, *docRev.CV, loadBackup)
	}
	if err != nil {
		return DocumentRevision{}, false, err
	}
	if hlv != nil {
		if docRev.CV == nil {
			docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		}
		docRev.HlvHistory = hlv.ToHistoryForHLV()
	}

	rc.bypassStat.Add(1)

	return docRev, false, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(ctx context.Context, docID string, collectionID uint32) (DocumentRevision, bool, error) {

	doc, err := rc.backingStores[collectionID].GetDocument(ctx, docID, DocUnmarshalSync)
	if err != nil {
		return DocumentRevision{}, false, err
	}

	docRev := DocumentRevision{
		RevID:                  doc.GetRevTreeID(),
		DocID:                  docID,
		RevCacheValueDeltaLock: &sync.Mutex{}, // initialize the mutex for delta updates
	}

	// We need to use revisionID loader pathway as not every document in the bucket is yet guaranteed to
	// have a HLV assigned to it.
	var hlv *HybridLogicalVector
	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, hlv, err = revCacheLoaderForDocument(ctx, rc.backingStores[collectionID], doc, doc.SyncData.GetRevTreeID())
	if err != nil {
		return DocumentRevision{}, false, err
	}
	if hlv != nil {
		docRev.CV = hlv.ExtractCurrentVersionFromHLV()
		docRev.HlvHistory = hlv.ToHistoryForHLV()
	}

	rc.bypassStat.Add(1)

	return docRev, false, nil
}

// Peek is a no-op for a BypassRevisionCache, and always returns a false 'found' value.
func (rc *BypassRevisionCache) Peek(ctx context.Context, docID string, versionString string, collectionID uint32) (docRev DocumentRevision, found bool) {
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

func (rc *BypassRevisionCache) Remove(ctx context.Context, docID, versionString string, collectionID uint32) {
	// no-op
}

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32, toDelta RevisionDelta) {
	// no-op
}

func (rc *BypassRevisionCache) GetWithDelta(ctx context.Context, docID, fromVersionString, toVersionString string, collectionID uint32) (DocumentRevision, error) {
	docRev, _, err := rc.Get(ctx, docID, fromVersionString, collectionID, true)
	if err != nil {
		return DocumentRevision{}, err
	}
	// no-op for delta cache fetch
	return docRev, nil
}

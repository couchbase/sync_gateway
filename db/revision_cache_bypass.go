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

// Get fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) Get(docID, revID string, includeBody bool, includeDelta bool) (docRev DocumentRevision, err error) {

	unmarshalLevel := DocUnmarshalSync
	if includeBody {
		unmarshalLevel = DocUnmarshalAll
	}
	doc, err := rc.backingStore.GetDocument(docID, unmarshalLevel)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: revID,
	}
	docRev.BodyBytes, docRev._shallowCopyBody, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, err = revCacheLoaderForDocument(rc.backingStore, doc, revID)
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(docID string, includeBody bool) (docRev DocumentRevision, err error) {

	unmarshalLevel := DocUnmarshalSync
	if includeBody {
		unmarshalLevel = DocUnmarshalAll
	}
	doc, err := rc.backingStore.GetDocument(docID, unmarshalLevel)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: doc.CurrentRev,
	}

	docRev.BodyBytes, docRev._shallowCopyBody, docRev.History, docRev.Channels, docRev.Removed, docRev.Attachments, docRev.Deleted, docRev.Expiry, err = revCacheLoaderForDocument(rc.backingStore, doc, doc.SyncData.CurrentRev)
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// Peek is a no-op for a BypassRevisionCache, and always returns a false 'found' value.
func (rc *BypassRevisionCache) Peek(docID, revID string) (docRev DocumentRevision, found bool) {
	return DocumentRevision{}, false
}

// Put is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Put(docRev DocumentRevision) {
	// no-op
}

// Update is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Upsert(docRev DocumentRevision) {
	// no-op
}

func (rc *BypassRevisionCache) Remove(docID, revID string) {
	// nop
}

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(docID, revID string, toDelta RevisionDelta) {
	// no-op
}

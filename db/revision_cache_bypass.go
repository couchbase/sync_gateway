package db

import (
	"expvar"
)

// BypassRevisionCache is an implementation of the RevisionCache interface that does not perform any caching.
// For any Get operation, it will always immediately fetch the requested revision from the backing store.
type BypassRevisionCache struct {
	backingStore RevisionCacheBackingStore
	bypassStat   *expvar.Int
}

func NewBypassRevisionCache(backingStore RevisionCacheBackingStore, bypassStat *expvar.Int) *BypassRevisionCache {
	return &BypassRevisionCache{
		backingStore: backingStore,
		bypassStat:   bypassStat,
	}
}

// Get fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) Get(docID, revID string) (docRev DocumentRevision, err error) {

	docRev = DocumentRevision{
		RevID: revID,
	}

	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Attachments, docRev.Deleted, docRev.Expiry, err = revCacheLoader(rc.backingStore, IDAndRev{DocID: docID, RevID: revID})
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(docID string) (docRev DocumentRevision, err error) {
	doc, err := rc.backingStore.GetDocument(docID, DocUnmarshalAll)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev = DocumentRevision{
		RevID: doc.CurrentRev,
	}

	docRev.BodyBytes, docRev.History, docRev.Channels, docRev.Attachments, docRev.Deleted, docRev.Expiry, err = revCacheLoaderForDocument(rc.backingStore, doc, doc.SyncData.CurrentRev)
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

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	// no-op
}

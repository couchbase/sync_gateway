package db

import (
	"expvar"

	"github.com/pkg/errors"
)

// BypassRevisionCache is an implementation of the RevisionCache interface that does not perform any caching.
// For any Get operation, it will always immediately fetch the requested revision from the backing store.
type BypassRevisionCache struct {
	backingStore RevisionCacheBackingStore
	bypassStat   *expvar.Int
}

func NewBypassRevisionCache(backingStore RevisionCacheBackingStore, bypassStat *expvar.Int) (*BypassRevisionCache, error) {
	if backingStore == nil {
		return nil, errors.New("nil backingStore")
	}

	return &BypassRevisionCache{
		backingStore: backingStore,
		bypassStat:   bypassStat,
	}, nil
}

// Get fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) Get(docID, revID string, copyType BodyCopyType) (docRev DocumentRevision, err error) {
	docRev.RevID = revID
	docRev.Body, docRev.History, docRev.Channels, docRev.Attachments, docRev.Expiry, err = revCacheLoader(rc.backingStore, IDAndRev{DocID: docID, RevID: revID})
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(docID string, copyType BodyCopyType) (docRev DocumentRevision, err error) {
	doc, err := rc.backingStore.GetDocument(docID, DocUnmarshalAll)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev.RevID = doc.CurrentRev
	docRev.Body, docRev.History, docRev.Channels, docRev.Attachments, docRev.Expiry, err = revCacheLoaderForDocument(rc.backingStore, doc, doc.syncData.CurrentRev)
	if err != nil {
		return DocumentRevision{}, err
	}

	rc.bypassStat.Add(1)

	return docRev, nil
}

// Peek is a no-op for a BypassRevisionCache, and always returns an empty DocumentRevision with a false 'found' value.
func (rc *BypassRevisionCache) Peek(docID, revID string, copyType BodyCopyType) (docRev DocumentRevision, found bool) {
	return DocumentRevision{}, false
}

// Put is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) Put(docID string, docRev DocumentRevision) {
	// no-op
}

// UpdateDelta is a no-op for a BypassRevisionCache
func (rc *BypassRevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	// no-op
}

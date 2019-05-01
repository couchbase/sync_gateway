package db

import "github.com/pkg/errors"

// BypassRevisionCache is an implementation of the RevisionCache interface that does perform any caching.
// For any Get operation, it will always immediately fetch the requested revision from the bucket.
type BypassRevisionCache struct {
	loaderFunc RevisionCacheLoaderFunc
}

// Get fetches the revision for the given docID and revID immediately from the bucket.
func (rc *BypassRevisionCache) Get(docID, revID string, copyType BodyCopyType) (docRev DocumentRevision, err error) {
	if rc.loaderFunc == nil {
		return DocumentRevision{}, errors.New("nil loaderFunc")
	}

	docRev.RevID = revID
	docRev.Body, docRev.History, docRev.Channels, docRev.Attachments, docRev.Expiry, err = rc.loaderFunc(IDAndRev{DocID: docID, RevID: revID})
	if err != nil {
		return DocumentRevision{}, err
	}

	return docRev, nil
}

// GetActive fetches the active revision for the given docID immediately from the bucket.
func (rc *BypassRevisionCache) GetActive(docID string, context *DatabaseContext, copyType BodyCopyType) (docRev DocumentRevision, err error) {
	doc, err := context.GetDocument(docID, DocUnmarshalAll)
	if err != nil {
		return DocumentRevision{}, err
	}

	docRev.RevID = doc.CurrentRev
	docRev.Body, docRev.History, docRev.Channels, docRev.Attachments, docRev.Expiry, err = context.revCacheLoaderForDocument(doc, doc.syncData.CurrentRev)
	if err != nil {
		return DocumentRevision{}, err
	}

	return docRev, nil
}

// Peek is a no-op for a BypassRevisionCache, and always returns an empty DocumentRevision with a false 'found' value.
func (rc *BypassRevisionCache) Peek(docID, revID string) (docRev DocumentRevision, found bool) {
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

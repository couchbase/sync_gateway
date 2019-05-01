package db

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// RevisionCache is an interface that can be used to fetch a DocumentRevision for a Doc ID and Rev ID pair.
type RevisionCache interface {
	// Get returns the given revision, and stores if not already cached
	Get(docID, revID string, copyType BodyCopyType) (DocumentRevision, error)

	// GetActive returns the current revision for the given doc ID, and stores if not already cached
	GetActive(docID string, context *DatabaseContext, copyType BodyCopyType) (docRev DocumentRevision, err error)

	// Peek returns the given revision if present in the cache
	Peek(docID, revID string) (docRev DocumentRevision, found bool)

	// Put will store the given docRev in the cache
	Put(docID string, docRev DocumentRevision)

	// UpdateDelta stores the given toDelta value in the given rev if cached
	UpdateDelta(docID, revID string, toDelta *RevisionDelta)
}

// Force compile-time check of RevisionCache types for interface
var _ RevisionCache = &LRURevisionCache{}
var _ RevisionCache = &ShardedLRURevisionCache{}

// Revision information as returned by the rev cache
type DocumentRevision struct {
	RevID       string
	Body        Body
	History     Revisions
	Channels    base.Set
	Expiry      *time.Time
	Attachments AttachmentsMeta
	Delta       *RevisionDelta
}

type IDAndRev struct {
	DocID string
	RevID string
}

type RevisionDelta struct {
	ToRevID           string   // Target revID for the delta
	DeltaBytes        []byte   // The actual delta
	AttachmentDigests []string // Digests for all attachments present on ToRevID
	RevisionHistory   []string // Revision history from parent of ToRevID to source revID, in descending order
}

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision) *RevisionDelta {
	return &RevisionDelta{
		ToRevID:           toRevision.RevID,
		DeltaBytes:        deltaBytes,
		AttachmentDigests: AttachmentDigests(toRevision.Attachments), // Flatten the AttachmentsMeta into a list of digests
		RevisionHistory:   toRevision.History.parseAncestorRevisions(fromRevID),
	}
}

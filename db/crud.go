//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math"
	"net/http"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/pkg/errors"
)

const (
	kMaxRecentSequences            = 20    // Maximum number of sequences stored in RecentSequences before pruning is triggered
	kMinRecentSequences            = 5     // Minimum number of sequences that should be left stored in RecentSequences during compaction
	unusedSequenceWarningThreshold = 10000 // Warn when releasing more than this many sequences due to existing sequence on the document
)

// ErrForbidden is returned when the user requests a document without a revision that they do not have access to.
// this is different from a client specifically requesting a revision they know about, which are treated as a _removal.
var ErrForbidden = base.HTTPErrorf(403, "forbidden")

var ErrMissing = base.HTTPErrorf(404, "missing")
var ErrDeleted = base.HTTPErrorf(404, "deleted")

// ////// READING DOCUMENTS:

func realDocID(docid string) string {
	if len(docid) > 250 {
		return "" // Invalid doc IDs
	}
	if strings.HasPrefix(docid, "_") {
		return "" // Disallow "_" prefix, which is for special docs
	}
	return docid
}

// getRevSeqNo fetches the revSeqNo for a document, using the virtual xattr if available. Returns the cas from this fetch.
func (c *DatabaseCollection) getRevSeqNo(ctx context.Context, docID string) (revSeqNo, cas uint64, err error) {
	xattrs, cas, err := c.dataStore.GetXattrs(ctx, docID, []string{base.VirtualXattrRevSeqNo})
	if err != nil {
		return 0, 0, err
	}
	revSeqNo, err = unmarshalRevSeqNo(xattrs[base.VirtualXattrRevSeqNo])
	return revSeqNo, cas, err
}

// GetDocument with raw returns the document from the bucket. This may perform an on-demand import.
func (c *DatabaseCollection) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	doc, _, err = c.GetDocumentWithRaw(ctx, docid, unmarshalLevel)
	return doc, err
}

// GetDocumentWithRaw returns the document from the bucket. This may perform an on-demand import.
func (c *DatabaseCollection) GetDocumentWithRaw(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, rawBucketDoc *sgbucket.BucketDocument, err error) {
	key := realDocID(docid)
	if key == "" {
		return nil, nil, base.HTTPErrorf(400, "Invalid doc ID")
	}
	if c.UseXattrs() {
		doc, rawBucketDoc, err = c.GetDocWithXattrs(ctx, key, unmarshalLevel)
		if err != nil {
			return nil, nil, err
		}
		isSgWrite, crc32Match, _ := doc.IsSGWrite(ctx, rawBucketDoc.Body)
		if crc32Match {
			c.dbStats().Database().Crc32MatchCount.Add(1)
		}

		// If existing doc wasn't an SG Write, import the doc.
		if !isSgWrite {
			// reload to get revseqno for on-demand import
			doc, rawBucketDoc, err = c.getDocWithXattrs(ctx, key, append(c.syncGlobalSyncAndUserXattrKeys(), base.VirtualXattrRevSeqNo), unmarshalLevel)
			if err != nil {
				return nil, nil, err
			}
			isSgWrite, _, _ := doc.IsSGWrite(ctx, rawBucketDoc.Body)
			if !isSgWrite {
				var importErr error
				doc, importErr = c.OnDemandImportForGet(ctx, docid, doc, rawBucketDoc.Body, rawBucketDoc.Xattrs, rawBucketDoc.Cas)
				if importErr != nil {
					return nil, nil, importErr
				}
				// nil, nil returned when ErrImportCancelled is swallowed by importDoc switch
				if doc == nil {
					return nil, nil, base.ErrNotFound
				}
			}
		}
		if !doc.HasValidSyncData() {
			return nil, nil, base.HTTPErrorf(404, "Not imported")
		}
	} else {
		rawDoc, cas, getErr := c.dataStore.GetRaw(key)
		if getErr != nil {
			return nil, nil, getErr
		}

		doc, err = unmarshalDocument(key, rawDoc)
		if err != nil {
			return nil, nil, err
		}

		if !doc.HasValidSyncData() {
			// Check whether doc has been upgraded to use xattrs
			upgradeDoc, _ := c.checkForUpgrade(ctx, docid, unmarshalLevel)
			if upgradeDoc == nil {
				return nil, nil, base.HTTPErrorf(404, "Not imported")
			}
			doc = upgradeDoc
		}

		rawBucketDoc = &sgbucket.BucketDocument{
			Body: rawDoc,
			Cas:  cas,
		}
	}

	return doc, rawBucketDoc, nil
}

// GetDocWithXattrs retrieves a document from the bucket, including sync gateway metadta xattrs, and the user xattr, if specified.
func (c *DatabaseCollection) GetDocWithXattrs(ctx context.Context, key string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, rawBucketDoc *sgbucket.BucketDocument, err error) {
	return c.getDocWithXattrs(ctx, key, c.syncGlobalSyncAndUserXattrKeys(), unmarshalLevel)
}

// GetDocWithXattrs retrieves a document from the bucket, including sync gateway metadta xattrs, and the user xattr, if specified. Arbitrary xattrs can be passed into this function to allow VirtualXattrRevSeqNo to be returned and set on Document.
func (c *DatabaseCollection) getDocWithXattrs(ctx context.Context, key string, xattrKeys []string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, rawBucketDoc *sgbucket.BucketDocument, err error) {
	rawBucketDoc = &sgbucket.BucketDocument{}
	var getErr error
	rawBucketDoc.Body, rawBucketDoc.Xattrs, rawBucketDoc.Cas, getErr = c.dataStore.GetWithXattrs(ctx, key, xattrKeys)
	if getErr != nil {
		return nil, nil, getErr
	}

	var unmarshalErr error
	doc, unmarshalErr = c.unmarshalDocumentWithXattrs(ctx, key, rawBucketDoc.Body, rawBucketDoc.Xattrs, rawBucketDoc.Cas, unmarshalLevel)
	if unmarshalErr != nil {
		return nil, nil, unmarshalErr
	}

	return doc, rawBucketDoc, nil
}

// This gets *just* the Sync Metadata (_sync field) rather than the entire doc, for efficiency reasons.
func (c *DatabaseCollection) GetDocSyncData(ctx context.Context, docid string) (SyncData, error) {

	emptySyncData := SyncData{}
	key := realDocID(docid)
	if key == "" {
		return emptySyncData, base.HTTPErrorf(400, "Invalid doc ID")
	}

	if c.UseXattrs() {
		// Retrieve doc and xattr from bucket, unmarshal only xattr.
		// Triggers on-demand import when document xattr doesn't match cas.
		rawDoc, xattrs, cas, getErr := c.dataStore.GetWithXattrs(ctx, key, c.syncGlobalSyncAndUserXattrKeys())
		if getErr != nil {
			return emptySyncData, getErr
		}

		// Unmarshal xattr only
		doc, unmarshalErr := c.unmarshalDocumentWithXattrs(ctx, docid, nil, xattrs, cas, DocUnmarshalSync)
		if unmarshalErr != nil {
			return emptySyncData, unmarshalErr
		}

		isSgWrite, crc32Match, _ := doc.IsSGWrite(ctx, rawDoc)
		if crc32Match {
			c.dbStats().Database().Crc32MatchCount.Add(1)
		}

		// If existing doc wasn't an SG Write, import the doc.
		if !isSgWrite {
			var importErr error

			doc, importErr = c.OnDemandImportForGet(ctx, docid, doc, rawDoc, xattrs, cas)
			if importErr != nil {
				return emptySyncData, importErr
			}
		}

		return doc.SyncData, nil

	} else {
		// Non-xattr.  Retrieve doc from bucket, unmarshal metadata only.
		rawDocBytes, _, err := c.dataStore.GetRaw(key)
		if err != nil {
			return emptySyncData, err
		}

		docRoot := documentRoot{
			SyncData: &SyncData{History: make(RevTree)},
		}
		if err := base.JSONUnmarshal(rawDocBytes, &docRoot); err != nil {
			return emptySyncData, err
		}

		return *docRoot.SyncData, nil
	}

}

// unmarshalDocumentWithXattrs populates individual xattrs on unmarshalDocumentWithXattrs from a provided xattrs map
func (db *DatabaseCollection) unmarshalDocumentWithXattrs(ctx context.Context, docid string, data []byte, xattrs map[string][]byte, cas uint64, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	return unmarshalDocumentWithXattrs(ctx, docid, data, xattrs[base.SyncXattrName], xattrs[base.VvXattrName], xattrs[base.MouXattrName], xattrs[db.userXattrKey()], xattrs[base.VirtualXattrRevSeqNo], xattrs[base.GlobalXattrName], cas, unmarshalLevel)

}

// GetDocSyncDataNoImport returns unmarshalled value of the _sync xattr.
// This gets *just* the Sync Metadata (_sync field) rather than the entire doc, for efficiency
// reasons. Unlike GetDocSyncData it does not check for on-demand import; this means it does not
// need to read the doc body from the bucket.
func (db *DatabaseCollection) GetDocSyncDataNoImport(ctx context.Context, docid string, level DocumentUnmarshalLevel) (*SyncData, *HybridLogicalVector, error) {
	xattrs, cas, err := db.dataStore.GetXattrs(ctx, docid, []string{base.SyncXattrName, base.VvXattrName})
	if err != nil {
		return nil, nil, err
	}
	if len(xattrs[base.SyncXattrName]) == 0 {
		return nil, nil, base.ErrXattrNotFound
	}
	doc, err := db.unmarshalDocumentWithXattrs(ctx, docid, nil, xattrs, cas, level)
	if err != nil {
		return nil, nil, err
	}
	return &doc.SyncData, doc.HLV, nil
}

// OnDemandImportForGet. Attempts to import the doc based on the provided id, contents and cas. ImportDocRaw does cas retry handling
// if the document gets updated after the initial retrieval attempt that triggered this.
func (c *DatabaseCollection) OnDemandImportForGet(ctx context.Context, docid string, doc *Document, rawDoc []byte, xattrs map[string][]byte, cas uint64) (docOut *Document, err error) {
	isDelete := rawDoc == nil
	importDb := DatabaseCollectionWithUser{DatabaseCollection: c, user: nil}
	var importErr error

	if syncDataErr := doc.validateSyncDataForImport(ctx, c.dbCtx, docid); syncDataErr != nil {
		return nil, syncDataErr
	}

	importOpts := importDocOptions{
		isDelete: isDelete,
		mode:     ImportOnDemand,
		revSeqNo: doc.RevSeqNo,
		expiry:   nil,
	}

	docOut, importErr = importDb.ImportDocRaw(ctx, docid, rawDoc, xattrs, importOpts, cas)

	if importErr == base.ErrImportCancelledFilter {
		// If the import was cancelled due to filter, treat as 404 not imported
		return nil, base.HTTPErrorf(http.StatusNotFound, "Not imported")
	} else if importErr != nil {
		// Treat any other failure to perform an on-demand import as not found
		base.DebugfCtx(ctx, base.KeyImport, "Unable to import doc %q during on demand import for get - will be treated as not found.  Reason: %v", base.UD(docid), importErr)
		return nil, base.HTTPErrorf(http.StatusNotFound, "Not found")
	}
	return docOut, nil
}

// GetRev returns the revision for the given docID and revOrCV, or the current active revision if revOrCV is empty.
func (db *DatabaseCollectionWithUser) GetRev(ctx context.Context, docID, revOrCV string, history bool, attachmentsSince []string) (DocumentRevision, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}
	return db.getRev(ctx, docID, revOrCV, maxHistory, nil)
}

// Returns the body of the current revision of a document
func (db *DatabaseCollectionWithUser) Get1xBody(ctx context.Context, docid string) (Body, error) {
	return db.Get1xRevBody(ctx, docid, "", false, nil)
}

// Get Rev with all-or-none history based on specified 'history' flag
func (db *DatabaseCollectionWithUser) Get1xRevBody(ctx context.Context, docid, revOrCV string, history bool, attachmentsSince []string) (Body, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}

	return db.Get1xRevBodyWithHistory(ctx, docid, revOrCV, Get1xRevBodyOptions{
		MaxHistory:       maxHistory,
		HistoryFrom:      nil,
		AttachmentsSince: attachmentsSince,
		ShowExp:          false,
	})
}

type Get1xRevBodyOptions struct {
	MaxHistory       int
	HistoryFrom      []string
	AttachmentsSince []string
	ShowExp          bool
	ShowCV           bool
}

// Retrieves rev with request history specified as collection of revids (historyFrom)
func (db *DatabaseCollectionWithUser) Get1xRevBodyWithHistory(ctx context.Context, docid, revOrCV string, opts Get1xRevBodyOptions) (Body, error) {
	rev, err := db.getRev(ctx, docid, revOrCV, opts.MaxHistory, opts.HistoryFrom)
	if err != nil {
		return nil, err
	}

	// RequestedHistory is the _revisions returned in the body.  Avoids mutating revision.History, in case it's needed
	// during attachment processing below
	requestedHistory := rev.History
	if opts.MaxHistory == 0 {
		requestedHistory = nil
	}
	if requestedHistory != nil {
		_, requestedHistory = trimEncodedRevisionsToAncestor(ctx, requestedHistory, opts.HistoryFrom, opts.MaxHistory)
	}

	return rev.Mutable1xBody(ctx, db, requestedHistory, opts.AttachmentsSince, opts.ShowExp, opts.ShowCV)
}

// Underlying revision retrieval used by Get1xRevBody, Get1xRevBodyWithHistory, GetRevCopy.
// Returns the revision of a document using the revision cache.
//   - revOrCV may be "", meaning the current revision. It can be a RevTree ID or a HLV CV.
//   - maxHistory is >0 if the caller wants a revision history; it's the max length of the history.
//   - historyFrom is an optional list of revIDs the client already has. If any of these are found
//     in the revision's history, it will be trimmed after that revID.
//   - attachmentsSince is nil to return no attachment bodies, otherwise a (possibly empty) list of
//     revisions for which the client already has attachments and doesn't need bodies. Any attachment
//     that hasn't changed since one of those revisions will be returned as a stub.
func (db *DatabaseCollectionWithUser) getRev(ctx context.Context, docid, revOrCV string, maxHistory int, historyFrom []string) (DocumentRevision, error) {
	var (
		revID *string
		cv    *Version
	)

	var (
		revision DocumentRevision
		getErr   error
	)
	if revOrCV != "" {
		// Get a specific revision body and history from the revision cache
		// (which will load them if necessary, by calling revCacheLoader, above)
		if currentVersion, parseErr := ParseVersion(revOrCV); parseErr != nil {
			// try as a rev ID
			revID = &revOrCV
			revision, getErr = db.revisionCache.GetWithRev(ctx, docid, *revID, RevCacheOmitDelta)
		} else {
			cv = &currentVersion
			revision, getErr = db.revisionCache.GetWithCV(ctx, docid, cv, RevCacheOmitDelta)
		}
	} else {
		// No rev given, so load active revision
		revision, getErr = db.revisionCache.GetActive(ctx, docid)
	}
	if getErr != nil {
		return DocumentRevision{}, getErr
	}

	return db.documentRevisionForRequest(ctx, docid, revision, revID, cv, maxHistory, historyFrom)
}

// documentRevisionForRequest processes the given DocumentRevision and returns a version of it for a given client request, depending on access, deleted, etc.
func (db *DatabaseCollectionWithUser) documentRevisionForRequest(ctx context.Context, docID string, revision DocumentRevision, revID *string, cv *Version, maxHistory int, historyFrom []string) (DocumentRevision, error) {
	// ensure only one of cv or revID is specified
	if cv != nil && revID != nil {
		return DocumentRevision{}, fmt.Errorf("must have one of cv or revID in documentRevisionForRequest (had cv=%v revID=%v)", cv, revID)
	}
	var requestedVersion string
	if revID != nil {
		requestedVersion = *revID
	} else if cv != nil {
		requestedVersion = cv.String()
	}

	if revision.BodyBytes == nil {
		if db.ForceAPIForbiddenErrors() {
			base.InfofCtx(ctx, base.KeyCRUD, "Doc: %s %s is missing", base.UD(docID), base.MD(requestedVersion))
			return DocumentRevision{}, ErrForbidden
		}
		return DocumentRevision{}, ErrMissing
	}

	db.collectionStats.NumDocReads.Add(1)
	db.collectionStats.DocReadsBytes.Add(int64(len(revision.BodyBytes)))

	// RequestedHistory is the _revisions returned in the body.  Avoids mutating revision.History, in case it's needed
	// during attachment processing below
	requestedHistory := revision.History
	if maxHistory == 0 {
		requestedHistory = nil
	}
	if requestedHistory != nil {
		_, requestedHistory = trimEncodedRevisionsToAncestor(ctx, requestedHistory, historyFrom, maxHistory)
	}

	isAuthorized, redactedRevision := db.authorizeUserForChannels(docID, revision.RevID, cv, revision.Channels, revision.Deleted, requestedHistory)
	if !isAuthorized {
		// client just wanted active revision, not a specific one
		if requestedVersion == "" {
			return DocumentRevision{}, ErrForbidden
		}
		if db.ForceAPIForbiddenErrors() {
			base.InfofCtx(ctx, base.KeyCRUD, "Not authorized to view doc: %s %s", base.UD(docID), base.MD(requestedVersion))
			return DocumentRevision{}, ErrForbidden
		}
		return redactedRevision, nil
	}

	// If the revision is a removal cache entry (no body), but the user has access to that removal, then just
	// return 404 missing to indicate that the body of the revision is no longer available.
	if revision.Removed {
		return DocumentRevision{}, ErrMissing
	}

	if revision.Deleted && requestedVersion == "" {
		return DocumentRevision{}, ErrDeleted
	}

	return revision, nil
}

func (db *DatabaseCollectionWithUser) GetCV(ctx context.Context, docid string, cv *Version, revTreeHistory bool) (revision DocumentRevision, err error) {
	if cv != nil {
		revision, err = db.revisionCache.GetWithCV(ctx, docid, cv, RevCacheOmitDelta)
	} else {
		revision, err = db.revisionCache.GetActive(ctx, docid)
	}
	if err != nil {
		return DocumentRevision{}, err
	}
	maxHistory := 0
	if revTreeHistory {
		maxHistory = math.MaxInt32
	}

	return db.documentRevisionForRequest(ctx, docid, revision, nil, cv, maxHistory, nil)
}

// GetDelta attempts to return the delta between fromRevId and toRevId.  If the delta can't be generated,
// returns nil.
func (db *DatabaseCollectionWithUser) GetDelta(ctx context.Context, docID, fromRev, toRev string, useCVRevCache bool) (delta *RevisionDelta, redactedRev *DocumentRevision, err error) {

	if docID == "" || fromRev == "" || toRev == "" {
		return nil, nil, nil
	}
	var fromRevision DocumentRevision
	var fromRevVrs Version
	if useCVRevCache {
		fromRevVrs, err = ParseVersion(fromRev)
		if err != nil {
			return nil, nil, err
		}
		fromRevision, err = db.revisionCache.GetWithCV(ctx, docID, &fromRevVrs, RevCacheIncludeDelta)
		if err != nil {
			return nil, nil, err
		}
	} else {
		fromRevision, err = db.revisionCache.GetWithRev(ctx, docID, fromRev, RevCacheIncludeDelta)
		if err != nil {
			return nil, nil, err
		}
	}

	// If the fromRevision is a removal cache entry (no body), but the user has access to that removal, then just
	// return 404 missing to indicate that the body of the revision is no longer available.
	// Delta can't be generated if we don't have the fromRevision body.
	if fromRevision.Removed {
		return nil, nil, ErrMissing
	}

	// If the fromRevision was a tombstone, then return error to tell delta sync to send full body replication
	if fromRevision.Deleted {
		return nil, nil, base.ErrDeltaSourceIsTombstone
	}

	// If both body and delta are not available for fromRevId, the delta can't be generated
	if fromRevision.BodyBytes == nil && fromRevision.Delta == nil {
		return nil, nil, err
	}

	// If delta is found, check whether it is a delta for the toRevID we want
	if fromRevision.Delta != nil {
		if fromRevision.Delta.ToCV == toRev || fromRevision.Delta.ToRevID == toRev {

			isAuthorized, redactedBody := db.authorizeUserForChannels(docID, toRev, fromRevision.CV, fromRevision.Delta.ToChannels, fromRevision.Delta.ToDeleted, encodeRevisions(ctx, docID, fromRevision.Delta.RevisionHistory))
			if !isAuthorized {
				return nil, &redactedBody, nil
			}

			// Case 2a. 'some rev' is the rev we're interested in - return the delta
			// db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaCacheHits, 1)
			db.dbStats().DeltaSync().DeltaCacheHit.Add(1)
			return fromRevision.Delta, nil, nil
		}
	}

	// Delta is unavailable, but the body is available.
	if fromRevision.BodyBytes != nil {

		db.dbStats().DeltaSync().DeltaCacheMiss.Add(1)
		var toRevision DocumentRevision
		if useCVRevCache {
			cv, err := ParseVersion(toRev)
			if err != nil {
				return nil, nil, err
			}
			toRevision, err = db.revisionCache.GetWithCV(ctx, docID, &cv, RevCacheIncludeDelta)
			if err != nil {
				return nil, nil, err
			}
		} else {
			toRevision, err = db.revisionCache.GetWithRev(ctx, docID, toRev, RevCacheIncludeDelta)
			if err != nil {
				return nil, nil, err
			}
		}

		deleted := toRevision.Deleted
		isAuthorized, redactedBody := db.authorizeUserForChannels(docID, toRev, toRevision.CV, toRevision.Channels, deleted, toRevision.History)
		if !isAuthorized {
			return nil, &redactedBody, nil
		}

		if toRevision.Removed {
			return nil, nil, ErrMissing
		}

		// If the revision we're generating a delta to is a tombstone, mark it as such and don't bother generating a delta
		if deleted {
			revCacheDelta := newRevCacheDelta([]byte(base.EmptyDocument), fromRev, toRevision, deleted, nil)
			if useCVRevCache {
				db.revisionCache.UpdateDeltaCV(ctx, docID, &fromRevVrs, revCacheDelta)
			} else {
				db.revisionCache.UpdateDelta(ctx, docID, fromRev, revCacheDelta)
			}
			return &revCacheDelta, nil, nil
		}

		// We didn't unmarshal fromBody earlier (in case we could get by with just the delta), so need do it now
		var fromBodyCopy Body
		if err := fromBodyCopy.Unmarshal(fromRevision.BodyBytes); err != nil {
			return nil, nil, err
		}

		// We didn't unmarshal toBody earlier (in case we could get by with just the delta), so need do it now
		var toBodyCopy Body
		if err := toBodyCopy.Unmarshal(toRevision.BodyBytes); err != nil {
			return nil, nil, err
		}

		// If attachments have changed between these revisions, we'll stamp the metadata into the bodies before diffing
		// so that the resulting delta also contains attachment metadata changes
		if fromRevision.Attachments != nil {
			// the delta library does not handle deltas in non builtin types,
			// so we need the map[string]interface{} type conversion here
			DeleteAttachmentVersion(fromRevision.Attachments)
			fromBodyCopy[BodyAttachments] = map[string]interface{}(fromRevision.Attachments)
		}

		var toRevAttStorageMeta []AttachmentStorageMeta
		if toRevision.Attachments != nil {
			// Flatten the AttachmentsMeta into a list of digest version pairs.
			toRevAttStorageMeta = ToAttachmentStorageMeta(toRevision.Attachments)
			DeleteAttachmentVersion(toRevision.Attachments)
			toBodyCopy[BodyAttachments] = map[string]interface{}(toRevision.Attachments)
		}

		deltaBytes, err := base.Diff(fromBodyCopy, toBodyCopy)
		if err != nil {
			return nil, nil, err
		}
		revCacheDelta := newRevCacheDelta(deltaBytes, fromRev, toRevision, deleted, toRevAttStorageMeta)

		// Write the newly calculated delta back into the cache before returning
		if useCVRevCache {
			db.revisionCache.UpdateDeltaCV(ctx, docID, &fromRevVrs, revCacheDelta)
		} else {
			db.revisionCache.UpdateDelta(ctx, docID, fromRev, revCacheDelta)
		}
		return &revCacheDelta, nil, nil
	}

	return nil, nil, nil
}

func (col *DatabaseCollectionWithUser) authorizeUserForChannels(docID, revID string, cv *Version, channels base.Set, isDeleted bool, history Revisions) (isAuthorized bool, redactedRev DocumentRevision) {

	if col.user != nil {
		if err := col.user.AuthorizeAnyCollectionChannel(col.ScopeName, col.Name, channels); err != nil {
			// On access failure, return (only) the doc history and deletion/removal
			// status instead of returning an error. For justification see the comment in
			// the getRevFromDoc method, below
			redactedRev = DocumentRevision{
				DocID:   docID,
				RevID:   revID,
				History: history,
				Deleted: isDeleted,
				CV:      cv,
			}
			if isDeleted {
				// Deletions are denoted by the deleted message property during 2.x replication
				redactedRev.BodyBytes = []byte(base.EmptyDocument)
			} else {
				// ... but removals are still denoted by the _removed property in the body, even for 2.x replication
				redactedRev.BodyBytes = []byte(RemovedRedactedDocument)
			}
			return false, redactedRev
		}
	}

	return true, DocumentRevision{}
}

// Returns the body of a revision of a document, as well as the document's current channels
// and the user/roles it grants channel access to.
func (db *DatabaseCollectionWithUser) Get1xRevAndChannels(ctx context.Context, docID string, revID string, listRevisions bool) (bodyBytes []byte, channels channels.ChannelMap, access UserAccessMap, roleAccess UserAccessMap, flags uint8, sequence uint64, gotRevID string, removed bool, err error) {
	doc, err := db.GetDocument(ctx, docID, DocUnmarshalAll)
	if doc == nil {
		return
	}
	bodyBytes, removed, err = db.get1xRevFromDoc(ctx, doc, revID, listRevisions)
	if err != nil {
		return
	}
	channels = doc.Channels
	access = doc.Access
	roleAccess = doc.RoleAccess
	sequence = doc.Sequence
	flags = doc.Flags
	if revID == "" {
		gotRevID = doc.GetRevTreeID()
	} else {
		gotRevID = revID
	}
	return
}

// Returns an HTTP 403 error if the User is not allowed to access any of this revision's channels.
func (col *DatabaseCollectionWithUser) authorizeDoc(doc *Document, revid string) error {
	user := col.user
	if doc == nil || user == nil {
		return nil // A nil User means access control is disabled
	}
	if revid == "" {
		revid = doc.GetRevTreeID()
	}
	if rev := doc.History[revid]; rev != nil {
		// Authenticate against specific revision:
		return col.user.AuthorizeAnyCollectionChannel(col.ScopeName, col.Name, rev.Channels)
	} else {
		// No such revision; let the caller proceed and return a 404
		return nil
	}
}

// Gets a revision of a document. If it's obsolete it will be loaded from the database if possible.
// inline "_attachments" properties in the body will be extracted and returned separately if present (pre-2.5 metadata, or backup revisions)
func (c *DatabaseCollection) getRevision(ctx context.Context, doc *Document, revid string) (bodyBytes []byte, attachments AttachmentsMeta, err error) {
	bodyBytes = doc.getRevisionBodyJSON(ctx, revid, c.RevisionBodyLoader)

	// No inline body, so look for separate doc:
	if bodyBytes == nil {
		if !doc.History.contains(revid) {
			return nil, nil, ErrMissing
		}

		bodyBytes, err = c.getOldRevisionJSON(ctx, doc.ID, revid)
		if err != nil || bodyBytes == nil {
			return nil, nil, err
		}
	}

	// optimistically grab the doc body and to store as a pre-unmarshalled version, as well as anticipating no inline attachments.
	if doc.GetRevTreeID() == revid {
		attachments = doc.Attachments()
	}

	// handle backup revision inline attachments, or pre-2.5 meta
	if inlineAtts, cleanBodyBytes, _, err := extractInlineAttachments(bodyBytes); err != nil {
		return nil, nil, err
	} else if len(inlineAtts) > 0 {
		// we found some inline attachments, so merge them with attachments, and update the bodies
		attachments = mergeAttachments(inlineAtts, attachments)
		bodyBytes = cleanBodyBytes
	}

	return bodyBytes, attachments, nil
}

// mergeAttachments copies the attachmentsB map, and merges attachmentsA into it. If both maps are nil, return nil.
// Conflicting attachment names fall back to a revpos comparison - highest wins. If equivalent, attachment from attachmentsB wins.
func mergeAttachments(attachmentsA, attachmentsB AttachmentsMeta) AttachmentsMeta {
	if len(attachmentsA)+len(attachmentsB) == 0 {
		return nil // noop
	} else if len(attachmentsA) == 0 {
		return copyMap(attachmentsB)
	} else if len(attachmentsB) == 0 {
		return copyMap(attachmentsA)
	}

	merged := maps.Clone(attachmentsB)

	// Iterate over source attachments, and merge with attachmentsB
	for attName, attA := range attachmentsA {
		if attB, exists := attachmentsB[attName]; !exists {
			// we didn't have an attachment matching this name already in syncData, so we'll use the attachment from attachmentsA.
			merged[attName] = attA
		} else {
			// we had the same attachment name in attachmentsB and in pre25Attachments.
			// Use whichever has the highest revpos.
			var attARevpos, attBRevpos int64
			if attAMeta, ok := attA.(map[string]interface{}); ok {
				attARevpos, ok = base.ToInt64(attAMeta["revpos"])
				if !ok {
					// There was no revpos in attachmentsA, so attachmentsB attachment will win.
					continue
				}
			}
			if attBMeta, ok := attB.(map[string]interface{}); ok {
				// if attBRevpos can't be converted into an int64, pre25 revpos wins, so fall through with attBRevpos=0
				attBRevpos, _ = base.ToInt64(attBMeta["revpos"])
			}

			// attachmentsA has larger revpos
			if attARevpos > attBRevpos {
				merged[attName] = attA
			}
		}
	}

	return merged
}

// extractInlineAttachments moves any inline attachments, from backup revision bodies, or pre-2.5 "_attachments", along with a "cleaned" version of bodyBytes and body.
func extractInlineAttachments(bodyBytes []byte) (attachments AttachmentsMeta, cleanBodyBytes []byte, cleanBody Body, err error) {
	if !bytes.Contains(bodyBytes, []byte(`"`+BodyAttachments+`"`)) {
		// we can safely say this doesn't contain any inline attachments.
		return nil, bodyBytes, nil, nil
	}

	var body Body
	if err = body.Unmarshal(bodyBytes); err != nil {
		return nil, nil, nil, err
	}

	bodyAtts, ok := body[BodyAttachments]
	if !ok {
		// no _attachments found (in a top-level property)
		// probably a false-positive on the byte scan above
		return nil, bodyBytes, body, nil
	}

	attsMap, ok := bodyAtts.(map[string]interface{})
	if !ok {
		// "_attachments" in body was not valid attachment metadata
		return nil, bodyBytes, body, nil
	}

	// remove _attachments from body and marshal for clean bodyBytes.
	delete(body, BodyAttachments)
	bodyBytes, err = base.JSONMarshal(body)
	if err != nil {
		return nil, nil, nil, err
	}

	return attsMap, bodyBytes, body, nil
}

// Gets the body of a revision's nearest ancestor, as raw JSON (without _id or _rev.)
// If no ancestor has any JSON, returns nil but no error.
func (db *DatabaseCollectionWithUser) getAncestorJSON(ctx context.Context, doc *Document, revid string) ([]byte, error) {
	for {
		if revid = doc.History.getParent(revid); revid == "" {
			return nil, nil
		} else if body := doc.getRevisionBodyJSON(ctx, revid, db.RevisionBodyLoader); body != nil {
			return body, nil
		}
	}
}

// Returns the body of a revision given a document struct. Checks user access.
// If the user is not authorized to see the specific revision they asked for,
// instead returns a minimal deletion or removal revision to let them know it's gone.
func (db *DatabaseCollectionWithUser) get1xRevFromDoc(ctx context.Context, doc *Document, revid string, listRevisions bool) (bodyBytes []byte, removed bool, err error) {
	var attachments AttachmentsMeta
	if err := db.authorizeDoc(doc, revid); err != nil {
		// As a special case, you don't need channel access to see a deletion revision,
		// otherwise the client's replicator can't process the deletion (since deletions
		// usually aren't on any channels at all!) But don't show the full body. (See #59)
		// Update: this applies to non-deletions too, since the client may have lost access to
		// the channel and gotten a "removed" entry in the _changes feed. It then needs to
		// incorporate that tombstone and for that it needs to see the _revisions property.
		if revid == "" || doc.History[revid] == nil {
			return nil, false, err
		}
		if doc.History[revid].Deleted {
			bodyBytes = []byte(base.EmptyDocument)
		} else {
			bodyBytes = []byte(RemovedRedactedDocument)
			removed = true
		}
	} else {
		if revid == "" {
			revid = doc.GetRevTreeID()
			if doc.History[revid].Deleted == true {
				return nil, false, ErrDeleted
			}
		}
		if bodyBytes, attachments, err = db.getRevision(ctx, doc, revid); err != nil {
			return nil, false, err
		}
	}

	kvPairs := []base.KVPair{
		{Key: BodyId, Val: doc.ID},
		{Key: BodyRev, Val: revid},
	}

	if len(attachments) > 0 {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: attachments})
	}

	if doc.History[revid].Deleted {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
	}

	if listRevisions {
		validatedHistory, getHistoryErr := doc.History.getHistory(revid)
		if getHistoryErr != nil {
			return nil, removed, getHistoryErr
		}
		kvPairs = append(kvPairs, base.KVPair{Key: BodyRevisions, Val: encodeRevisions(ctx, doc.ID, validatedHistory)})
	}

	bodyBytes, err = base.InjectJSONProperties(bodyBytes, kvPairs...)
	if err != nil {
		return nil, removed, err
	}

	return bodyBytes, removed, nil
}

// Returns the body and rev ID of the asked-for revision or the most recent available ancestor.
func (db *DatabaseCollectionWithUser) getAvailableRev(ctx context.Context, doc *Document, revid string) ([]byte, string, AttachmentsMeta, error) {
	for ; revid != ""; revid = doc.History[revid].Parent {
		if bodyBytes, attachments, _ := db.getRevision(ctx, doc, revid); bodyBytes != nil {
			return bodyBytes, revid, attachments, nil
		}
	}
	return nil, "", nil, ErrMissing
}

// Returns the 1x-style body of the asked-for revision or the most recent available ancestor.
func (db *DatabaseCollectionWithUser) getAvailable1xRev(ctx context.Context, doc *Document, revid string) ([]byte, error) {
	bodyBytes, ancestorRevID, attachments, err := db.getAvailableRev(ctx, doc, revid)
	if err != nil {
		return nil, err
	}

	kvPairs := []base.KVPair{
		{Key: BodyId, Val: doc.ID},
		{Key: BodyRev, Val: ancestorRevID},
	}

	if ancestorRev, ok := doc.History[ancestorRevID]; ok && ancestorRev != nil && ancestorRev.Deleted {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
	}

	if len(attachments) > 0 {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: attachments})
	}

	bodyBytes, err = base.InjectJSONProperties(bodyBytes, kvPairs...)
	if err != nil {
		return nil, err
	}

	return bodyBytes, nil
}

// Returns the attachments of the asked-for revision or the most recent available ancestor.
// Returns nil if no attachments or ancestors are found.
func (db *DatabaseCollectionWithUser) getAvailableRevAttachments(ctx context.Context, doc *Document, revid string) (ancestorAttachments AttachmentsMeta, foundAncestor bool) {
	_, _, attachments, err := db.getAvailableRev(ctx, doc, revid)
	if err != nil {
		return nil, false
	}

	return attachments, true
}

// Moves a revision's ancestor's body out of the document object and into a separate db doc.
func (db *DatabaseCollectionWithUser) backupAncestorRevs(ctx context.Context, doc *Document, newDoc *Document) {

	// Find an ancestor that still has JSON in the document:
	var json []byte
	ancestorRevId := newDoc.RevID
	for {
		if ancestorRevId = doc.History.getParent(ancestorRevId); ancestorRevId == "" {
			// No ancestors with JSON found. Return early
			return
		} else if json = doc.getRevisionBodyJSON(ctx, ancestorRevId, db.RevisionBodyLoader); json != nil {
			break
		}
	}

	// Back up the revision JSON as a separate doc in the bucket:
	db.backupRevisionJSON(ctx, doc.ID, doc.HLV.GetCurrentVersionString(), json)

	// Nil out the ancestor rev's body in the document struct:
	if ancestorRevId == doc.GetRevTreeID() {
		doc.RemoveBody()
	} else {
		doc.removeRevisionBody(ctx, ancestorRevId)
	}
}

// ////// UPDATING DOCUMENTS:

// OnDemandImportForWrite imports a document before a subsequent document is written to Sync Gateway on top of the import document. Returns base.ErrCasFailureShouldRetry in the case that this import nees to be retried. This function is expected to be called within a callback to WriteUpdateWithXattrs.
func (db *DatabaseCollectionWithUser) OnDemandImportForWrite(ctx context.Context, docid string, doc *Document, deleted bool) error {
	revSeqNo, cas, err := db.getRevSeqNo(ctx, docid)
	if err != nil {
		return err
	}
	if cas != doc.Cas {
		return base.ErrCasFailureShouldRetry
	}

	if syncDataErr := doc.validateSyncDataForImport(ctx, db.dbCtx, docid); syncDataErr != nil {
		return syncDataErr
	}
	// Check whether the doc requiring import is an SDK delete
	isDelete := false
	if doc.Body(ctx) == nil {
		isDelete = true
	} else {
		isDelete = deleted
	}
	// Use an admin-scoped database for import
	importDb := DatabaseCollectionWithUser{DatabaseCollection: db.DatabaseCollection, user: nil}

	importOpts := importDocOptions{
		expiry:   nil,
		mode:     ImportOnDemand,
		isDelete: isDelete,
		revSeqNo: revSeqNo,
	}
	importedDoc, importErr := importDb.ImportDoc(ctx, docid, doc, importOpts) // nolint:staticcheck

	if importErr == base.ErrImportCancelledFilter {
		// Document exists, but existing doc wasn't imported based on import filter.  Treat write as insert
		doc.SyncData = SyncData{History: make(RevTree)}
	} else if importErr != nil {
		return importErr
	} else {
		doc = importedDoc // nolint:staticcheck
	}
	return nil
}

// updateHLV updates the HLV in the sync data appropriately based on what type of document update event we are encountering. mouMatch represents if the _mou.cas == doc.cas
func (db *DatabaseCollectionWithUser) updateHLV(ctx context.Context, d *Document, docUpdateEvent DocUpdateType, mouMatch bool) (*Document, error) {

	hasHLV := d.HLV != nil
	if d.HLV == nil {
		d.HLV = &HybridLogicalVector{}
		base.DebugfCtx(ctx, base.KeyVV, "No existing HLV for doc %s", base.UD(d.ID))
	} else {
		base.DebugfCtx(ctx, base.KeyVV, "Existing HLV for doc %s before modification %+v", base.UD(d.ID), d.HLV)
	}
	switch docUpdateEvent {
	case ExistingVersion:
		// preserve any other logic on the HLV that has been done by the client, only update to cvCAS will be needed
		d.HLV.CurrentVersionCAS = expandMacroCASValueUint64
	case Import:
		// Do not update HLV if the current document version (cas) is already included in the existing HLV, as either:
		//    1. _vv.cvCAS == document.cas (current mutation is already present as cv), or
		//    2. _mou.cas == document.cas (current mutation is already present as cv, and was imported on a different cluster)

		cvCASMatch := hasHLV && d.HLV.CurrentVersionCAS == d.Cas
		if !hasHLV || (!cvCASMatch && !mouMatch) {
			// Otherwise this is an SDK mutation made by the local cluster that should be added to HLV.
			newVVEntry := Version{}
			newVVEntry.SourceID = db.dbCtx.EncodedSourceID
			newVVEntry.Value = d.Cas
			err := d.HLV.AddVersion(newVVEntry)
			if err != nil {
				return nil, err
			}
			d.HLV.CurrentVersionCAS = d.Cas
			base.DebugfCtx(ctx, base.KeyVV, "Adding new version to HLV due to import for doc %s, updated HLV %+v", base.UD(d.ID), d.HLV)
		} else {
			base.DebugfCtx(ctx, base.KeyVV, "Not updating HLV due to _mou.cas == doc.cas for doc %s, extant HLV %+v", base.UD(d.ID), d.HLV)
		}
	case NewVersion, ExistingVersionWithUpdateToHLV:
		// add a new entry to the version vector
		newVVEntry := Version{}
		newVVEntry.SourceID = db.dbCtx.EncodedSourceID
		newVVEntry.Value = expandMacroCASValueUint64
		err := d.HLV.AddVersion(newVVEntry)
		if err != nil {
			return nil, err
		}
		// update the cvCAS on the SGWrite event too
		d.HLV.CurrentVersionCAS = expandMacroCASValueUint64
	}
	d.SyncData.SetCV(d.HLV)
	return d, nil
}

// MigrateAttachmentMetadata will move any attachment metadata defined in sync data to global sync xattr
func (c *DatabaseCollectionWithUser) MigrateAttachmentMetadata(ctx context.Context, docID string, cas uint64, syncData *SyncData) error {
	xattrs, _, err := c.dataStore.GetXattrs(ctx, docID, []string{base.GlobalXattrName})
	if err != nil && !base.IsXattrNotFoundError(err) {
		return err
	}
	var globalData GlobalSyncData
	if xattrs[base.GlobalXattrName] != nil {
		// we have a global xattr to preserve
		err := base.JSONUnmarshal(xattrs[base.GlobalXattrName], &globalData)
		if err != nil {
			return base.RedactErrorf("Failed to Unmarshal global sync data when attempting to migrate sync data attachments to global xattr with id: %s. Error: %v", base.UD(docID), err)
		}
	}
	globalData.Attachments = mergeAttachments(syncData.AttachmentsPre4dot0, globalData.Attachments)
	syncData.AttachmentsPre4dot0 = nil // clear out the pre-4.0 attachments so we don't try to write them to the doc
	globalXattr, err := base.JSONMarshal(globalData)
	if err != nil {
		return base.RedactErrorf("Failed to Marshal global sync data when attempting to migrate sync data attachments to global xattr with id: %s. Error: %v", base.UD(docID), err)
	}
	rawSyncXattr, err := base.JSONMarshal(*syncData)
	if err != nil {
		return base.RedactErrorf("Failed to Marshal sync data when attempting to migrate sync data attachments to global xattr with id: %s. Error: %v", base.UD(docID), err)
	}

	// build macro expansion for sync data. This will avoid the update to xattrs causing an extra import event (i.e. sync cas will be == to doc cas)
	opts := &sgbucket.MutateInOptions{}
	spec := macroExpandSpec(base.SyncXattrName)
	opts.MacroExpansion = spec
	opts.PreserveExpiry = true // if doc has expiry, we should preserve this

	updatedXattr := map[string][]byte{base.SyncXattrName: rawSyncXattr, base.GlobalXattrName: globalXattr}
	_, err = c.dataStore.UpdateXattrs(ctx, docID, 0, cas, updatedXattr, opts)
	return err
}

// Updates or creates a document.
// The new body's BodyRev property must match the current revision's, if any.
func (db *DatabaseCollectionWithUser) Put(ctx context.Context, docid string, body Body) (newRevID string, doc *Document, err error) {

	delete(body, BodyId)

	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body[BodyRev].(string)
	generation, _ := ParseRevID(ctx, matchRev)
	if generation < 0 {
		return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++
	delete(body, BodyRev)

	// remove CV before RevTreeID generation
	matchCV, _ := body[BodyCV].(string)
	delete(body, BodyCV)

	// Not extracting it yet because we need this property around to generate a RevTreeID
	deleted, _ := body[BodyDeleted].(bool)

	expiry, err := body.ExtractExpiry()
	if err != nil {
		return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}

	// Create newDoc which will be used to pass around Body
	newDoc := &Document{
		ID: docid,
	}

	// Pull out attachments
	newDoc.SetAttachments(GetBodyAttachments(body))
	delete(body, BodyAttachments)

	delete(body, BodyRevisions)

	err = validateAPIDocUpdate(body)
	if err != nil {
		return "", nil, err
	}

	docUpdateEvent := NewVersion
	allowImport := db.UseXattrs()
	updateRevCache := true
	doc, newRevID, err = db.updateAndReturnDoc(ctx, newDoc.ID, allowImport, &expiry, nil, docUpdateEvent, nil, false, updateRevCache, func(doc *Document) (resultDoc *Document, resultAttachmentData updatedAttachments, createNewRevIDSkipped bool, updatedExpiry *uint32, resultErr error) {
		var isSgWrite bool
		var crc32Match bool

		// Is this doc an sgWrite?
		if doc != nil {
			isSgWrite, crc32Match, _ = doc.IsSGWrite(ctx, nil)
			if crc32Match {
				db.dbStats().Database().Crc32MatchCount.Add(1)
			}
		}

		// (Be careful: this block can be invoked multiple times if there are races!)
		// If the existing doc isn't an SG write, import prior to updating
		if doc != nil && !isSgWrite && db.UseXattrs() {
			err := db.OnDemandImportForWrite(ctx, newDoc.ID, doc, deleted)
			if err != nil {
				if db.ForceAPIForbiddenErrors() {
					base.InfofCtx(ctx, base.KeyCRUD, "Importing doc %q prior to write caused error", base.UD(newDoc.ID))
					return nil, nil, false, nil, ErrForbidden
				}
				return nil, nil, false, nil, err
			}
		}

		var conflictErr error

		// OCC check of matchCV against CV on the doc
		if matchCV != "" {
			if matchCV == doc.HLV.GetCurrentVersionString() {
				// set matchRev to the current revision ID and allow existing codepaths to perform RevTree-based update.
				matchRev = doc.GetRevTreeID()
				// bump generation based on retrieved RevTree ID
				generation, _ = ParseRevID(ctx, matchRev)
				generation++
			} else if doc.hasFlag(channels.Conflict | channels.Hidden) {
				// Can't use CV as an OCC Value when a document is in conflict, or we're updating the non-winning leaf
				// There's no way to get from a given old CV to a RevTreeID to perform the update correctly, since we don't maintain linear history for a given SourceID.
				// Reject the request and force the user to resolve the conflict using RevTree IDs which does have linear history available.
				conflictErr = base.HTTPErrorf(http.StatusBadRequest, "Cannot use CV to modify a document in conflict - resolve first with RevTree ID")
			} else {
				conflictErr = base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
			}
		}

		if conflictErr == nil {
			// Make sure matchRev matches an existing leaf revision:
			if matchRev == "" {
				matchRev = doc.GetRevTreeID()
				if matchRev != "" {
					// PUT with no parent rev given, but there is an existing current revision.
					// This is OK as long as the current one is deleted.
					if !doc.History[matchRev].Deleted {
						conflictErr = base.HTTPErrorf(http.StatusConflict, "Document exists")
					} else {
						generation, _ = ParseRevID(ctx, matchRev)
						generation++
					}
				}
			} else if !doc.History.isLeaf(matchRev) || db.IsIllegalConflict(ctx, doc, matchRev, deleted, false, nil) {
				conflictErr = base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
			}
		}

		// Make up a new _rev, and add it to the history:
		bodyWithoutInternalProps, wasStripped := stripInternalProperties(body)
		canonicalBytesForRevID, err := base.JSONMarshalCanonical(bodyWithoutInternalProps)
		if err != nil {
			return nil, nil, false, nil, err
		}

		// We needed to keep _deleted around in the body until we generated a rev ID, but now we can ditch it.
		_, isDeleted := body[BodyDeleted]
		if isDeleted {
			delete(body, BodyDeleted)
		}

		// and now we can finally update the newDoc body to be without any special properties
		newDoc.UpdateBody(body)

		// If no special properties were stripped and document wasn't deleted, the canonical bytes represent the current
		// body.  In this scenario, store canonical bytes as newDoc._rawBody
		if !wasStripped && !isDeleted {
			newDoc._rawBody = canonicalBytesForRevID
		}

		// Handle telling the user if there is a conflict
		if conflictErr != nil {
			if db.ForceAPIForbiddenErrors() {
				// Make sure the user has permission to modify the document before confirming doc existence
				mutableBody, metaMap, newRevID, err := db.prepareSyncFn(doc, newDoc)
				if err != nil {
					base.InfofCtx(ctx, base.KeyCRUD, "Failed to prepare to run sync function: %v", err)
					return nil, nil, false, nil, ErrForbidden
				}

				_, _, _, _, _, err = db.runSyncFn(ctx, doc, mutableBody, metaMap, newRevID)
				if err != nil {
					base.DebugfCtx(ctx, base.KeyCRUD, "Could not modify doc %q due to %s and sync func rejection: %v", base.UD(doc.ID), conflictErr, err)
					return nil, nil, false, nil, ErrForbidden
				}
			}
			return nil, nil, false, nil, conflictErr
		}

		// Process the attachments, and populate _sync with metadata. This alters 'body' so it has to
		// be done before calling CreateRevID (the ID is based on the digest of the body.)
		newAttachments, err := db.storeAttachments(ctx, doc, newDoc.Attachments(), generation, matchRev, nil)
		if err != nil {
			return nil, nil, false, nil, err
		}

		newRev := CreateRevIDWithBytes(generation, matchRev, canonicalBytesForRevID)

		if err := doc.History.addRevision(newDoc.ID, RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted}); err != nil {
			base.InfofCtx(ctx, base.KeyCRUD, "Failed to add revision ID: %s, for doc: %s, error: %v", newRev, base.UD(docid), err)
			return nil, nil, false, nil, base.ErrRevTreeAddRevFailure
		}

		newDoc.RevID = newRev
		newDoc.Deleted = deleted

		return newDoc, newAttachments, false, nil, nil
	})

	return newRevID, doc, err
}

// PutExistingCurrentVersion:
//   - newDoc: new incoming doc
//   - newDocHLV: new incoming doc's HLV
//   - existsingDoc: existing doc in bucket (if present)
//   - revTreeHistory: list of revID's from the incoming docs history (including docs current rev).
//   - alignRevTrees: if this is true then we will align the new write with the incoming docs rev tree. If this is
//     false and len(revTreeHistory) > 0 then this means the local version of this doc does not have an HLV so this parameter
//     will be used to check for conflicts.
func (db *DatabaseCollectionWithUser) PutExistingCurrentVersion(ctx context.Context, newDoc *Document, newDocHLV *HybridLogicalVector, existingDoc *sgbucket.BucketDocument, revTreeHistory []string, alignRevTrees bool, conflictResolver ConflictResolvers) (doc *Document, cv *Version, newRevID string, err error) {
	var matchRev string
	if existingDoc != nil {
		doc, unmarshalErr := db.unmarshalDocumentWithXattrs(ctx, newDoc.ID, existingDoc.Body, existingDoc.Xattrs, existingDoc.Cas, DocUnmarshalRev)
		if unmarshalErr != nil {
			return nil, nil, "", base.HTTPErrorf(http.StatusBadRequest, "Error unmarshaling existing doc")
		}
		matchRev = doc.GetRevTreeID()
	}
	generation, _ := ParseRevID(ctx, matchRev)
	if generation < 0 {
		return nil, nil, "", base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++ //nolint

	docUpdateEvent := ExistingVersion
	allowImport := db.UseXattrs()
	updateRevCache := true
	doc, newRevID, err = db.updateAndReturnDoc(ctx, newDoc.ID, allowImport, &newDoc.DocExpiry, nil, docUpdateEvent, existingDoc, false, updateRevCache, func(doc *Document) (resultDoc *Document, resultAttachmentData updatedAttachments, createNewRevIDSkipped bool, updatedExpiry *uint32, resultErr error) {
		// (Be careful: this block can be invoked multiple times if there are races!)

		var isSgWrite bool
		var crc32Match bool

		// Is this doc an sgWrite?
		if doc != nil {
			isSgWrite, crc32Match, _ = doc.IsSGWrite(ctx, nil)
			if crc32Match {
				db.dbStats().Database().Crc32MatchCount.Add(1)
			}
		}

		// If the existing doc isn't an SG write, import prior to updating
		if doc != nil && !isSgWrite && db.UseXattrs() {
			err := db.OnDemandImportForWrite(ctx, newDoc.ID, doc, newDoc.Deleted)
			if err != nil {
				return nil, nil, false, nil, err
			}
		}

		// set up revTreeID for backward compatibility
		var previousRevTreeID string
		var prevGeneration int
		var newGeneration int
		if len(revTreeHistory) == 0 {
			previousRevTreeID = doc.GetRevTreeID()
			prevGeneration, _ = ParseRevID(ctx, previousRevTreeID)
			newGeneration = prevGeneration + 1
		} else {
			previousRevTreeID = revTreeHistory[0]
			prevGeneration, _ = ParseRevID(ctx, previousRevTreeID)
			// if incoming rev tree list is from a legacy pre upgraded doc, we should have new revID generation based
			// off the previous current rev +1. If we have rev tree list filled from ISGR's rev tree property then we
			// should use the current rev of inc
			if !alignRevTrees {
				newGeneration = prevGeneration + 1
			} else {
				newGeneration = prevGeneration
			}
		}
		revTreeConflictChecked := false
		var parent string
		var currentRevIndex int

		// Conflict check here
		// if doc has no HLV defined this is a new doc we haven't seen before, skip conflict check
		if doc.HLV == nil {
			doc.HLV = NewHybridLogicalVector()
			doc.HLV.UpdateWithIncomingHLV(newDocHLV)
			if err != nil {
				return nil, nil, false, nil, err
			}
			if alignRevTrees {
				err := doc.alignRevTreeHistory(ctx, newDoc, revTreeHistory)
				if err != nil {
					return nil, nil, false, nil, err
				}
			}
		} else {
			conflictStatus := doc.IsInConflict(ctx, newDocHLV)
			switch conflictStatus {
			case HLVNoConflictRevAlreadyPresent:
				base.DebugfCtx(ctx, base.KeyCRUD, "PutExistingCurrentVersion(%q): No new versions to add.  existing: %#v  new:%#v", base.UD(newDoc.ID), doc.HLV, newDocHLV)
				return nil, nil, false, nil, base.ErrUpdateCancel // No new revisions to add
			case HLVNoConflict:
				if doc.HLV.EqualCV(newDocHLV) {
					base.DebugfCtx(ctx, base.KeyCRUD, "PutExistingCurrentVersion(%q): No new versions to add.  existing: %#v  new:%#v", base.UD(newDoc.ID), doc.HLV, newDocHLV)
					return nil, nil, false, nil, base.ErrUpdateCancel // No new revisions to add
				}
				// update hlv for all newer incoming source version pairs
				doc.HLV.UpdateWithIncomingHLV(newDocHLV)
				if err != nil {
					return nil, nil, false, nil, err
				}
				// the new document has a dominating hlv, so we can ignore any legacy rev revtree information on the incoming document
				revTreeConflictChecked = true
				if !alignRevTrees {
					previousRevTreeID = doc.GetRevTreeID()
				} else {
					// align rev tree here for ISGR replications
					err = doc.alignRevTreeHistory(ctx, newDoc, revTreeHistory)
					if err != nil {
						return nil, nil, false, nil, err
					}
				}
			case HLVConflict:
				// if the legacy rev list is from ISGR property, we should not do a conflict check
				if len(revTreeHistory) > 0 && !alignRevTrees {
					// conflict check on rev tree history, if there is a rev in rev tree history we have the parent of locally we are not in conflict
					parent, currentRevIndex, err = db.revTreeConflictCheck(ctx, revTreeHistory, doc, newDoc.Deleted)
					if err != nil {
						base.InfofCtx(ctx, base.KeyCRUD, "conflict detected between the two HLV's for doc %s, incoming version %s, local version %s, and conflict found in rev tree history", base.UD(doc.ID), newDocHLV.GetCurrentVersionString(), doc.HLV.GetCurrentVersionString())
						return nil, nil, false, nil, err
					}
					_, err = doc.addNewerRevisionsToRevTreeHistory(newDoc, currentRevIndex, parent, revTreeHistory)
					if err != nil {
						return nil, nil, false, nil, err
					}
					revTreeConflictChecked = true
					doc.HLV.UpdateWithIncomingHLV(newDocHLV)
					if err != nil {
						return nil, nil, false, nil, err
					}
				} else {
					// temp remove history alignment here, pending CBG-4791
					alignRevTrees = false
					revTreeConflictChecked = true
					previousRevTreeID = doc.GetRevTreeID()
					newGeneration = prevGeneration + 1
					base.InfofCtx(ctx, base.KeyCRUD, "conflict detected between the two HLV's for doc %s, incoming version %v, local version %v", base.UD(doc.ID), newDocHLV.ExtractCurrentVersionFromHLV(), doc.HLV.ExtractCurrentVersionFromHLV())
					if conflictResolver.hlvConflictResolver == nil {
						// cancel rest of update, HLV is in conflict and no resolver is present
						return nil, nil, false, nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
					}
					// resolve conflict
					var newHLV *HybridLogicalVector
					newHLV, err = db.resolveHLVConflict(ctx, doc, newDoc, conflictResolver.hlvConflictResolver)
					if err != nil {
						base.InfofCtx(ctx, base.KeyCRUD, "Failed to resolve HLV conflict for doc %s, error: %v", base.UD(doc.ID), err)
						return nil, nil, false, nil, err
					}
					// overwrite the existing HLV with the new one
					doc.HLV = newHLV
				}
			}
		}
		// rev tree conflict check if we have rev tree history to check against + finds current rev index to allow us
		// to add any new revision to rev tree below.
		// Only check for rev tree conflicts if we haven't already checked above AND if alignRevTrees is false given
		// alignRevTrees can only be true for non legacy rev writes meaning when this is true we should be doing our
		// conflict checking with incoming HLV
		if !revTreeConflictChecked && len(revTreeHistory) > 0 && !alignRevTrees {
			parent, currentRevIndex, err = db.revTreeConflictCheck(ctx, revTreeHistory, doc, newDoc.Deleted)
			if err != nil {
				return nil, nil, false, nil, err
			}
			_, err = doc.addNewerRevisionsToRevTreeHistory(newDoc, currentRevIndex, parent, revTreeHistory)
			if err != nil {
				return nil, nil, false, nil, err
			}
		}

		// Process the attachments, replacing bodies with digests.
		newAttachments, err := db.storeAttachments(ctx, doc, newDoc.Attachments(), newGeneration, previousRevTreeID, nil)
		if err != nil {
			return nil, nil, false, nil, err
		}

		// generate rev id for new arriving doc
		var newRev string
		if !alignRevTrees {
			// create a new revID for incoming write
			strippedBody, _ := stripInternalProperties(newDoc._body)
			encoding, err := base.JSONMarshalCanonical(strippedBody)
			if err != nil {
				return nil, nil, false, nil, err
			}
			newRev = CreateRevIDWithBytes(newGeneration, previousRevTreeID, encoding)
			if err := doc.History.addRevision(newDoc.ID, RevInfo{ID: newRev, Parent: previousRevTreeID, Deleted: newDoc.Deleted}); err != nil {
				base.InfofCtx(ctx, base.KeyCRUD, "Failed to add revision ID: %s, for doc: %s, error: %v", newRev, base.UD(newDoc.ID), err)
				return nil, nil, false, nil, base.ErrRevTreeAddRevFailure
			}
		} else {
			// for ISGR, incoming writes current rev should be the most recent rev in history. This aligns the
			// rev history each of the replication
			newRev = previousRevTreeID
		}

		newDoc.RevID = newRev

		return newDoc, newAttachments, false, nil, nil
	})

	if doc != nil && doc.HLV != nil {
		if cv == nil {
			cv = &Version{}
		}
		source, version := doc.HLV.GetCurrentVersion()
		cv.SourceID = source
		cv.Value = version
	}

	return doc, cv, newRevID, err
}

// Adds an existing revision to a document along with its history (list of rev IDs.)
func (db *DatabaseCollectionWithUser) PutExistingRev(ctx context.Context, newDoc *Document, docHistory []string, noConflicts bool, forceAllConflicts bool, existingDoc *sgbucket.BucketDocument, docUpdateEvent DocUpdateType) (doc *Document, newRevID string, err error) {
	return db.PutExistingRevWithConflictResolution(ctx, newDoc, docHistory, noConflicts, nil, forceAllConflicts, existingDoc, docUpdateEvent)
}

// PutExistingRevWithConflictResolution Adds an existing revision to a document along with its history (list of rev IDs.)
// If this new revision would result in a conflict:
//  1. If noConflicts == false, the revision will be added to the rev tree as a conflict
//  2. If noConflicts == true and a conflictResolverFunc is not provided, a 409 conflict error will be returned
//  3. If noConflicts == true and a conflictResolverFunc is provided, conflicts will be resolved and the result added to the document.
func (db *DatabaseCollectionWithUser) PutExistingRevWithConflictResolution(ctx context.Context, newDoc *Document, docHistory []string, noConflicts bool, conflictResolver *ConflictResolver, forceAllowConflictingTombstone bool, existingDoc *sgbucket.BucketDocument, docUpdateEvent DocUpdateType) (doc *Document, newRevID string, err error) {
	newRev := docHistory[0]
	generation, _ := ParseRevID(ctx, newRev)
	if generation < 0 {
		return nil, "", base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}

	allowImport := db.UseXattrs()
	updateRevCache := true
	doc, _, err = db.updateAndReturnDoc(ctx, newDoc.ID, allowImport, &newDoc.DocExpiry, nil, docUpdateEvent, existingDoc, false, updateRevCache, func(doc *Document) (resultDoc *Document, resultAttachmentData updatedAttachments, createNewRevIDSkipped bool, updatedExpiry *uint32, resultErr error) {
		// (Be careful: this block can be invoked multiple times if there are races!)

		var isSgWrite bool
		var crc32Match bool

		// Is this doc an sgWrite?
		if doc != nil {
			isSgWrite, crc32Match, _ = doc.IsSGWrite(ctx, nil)
			if crc32Match {
				db.dbStats().Database().Crc32MatchCount.Add(1)
			}
		}

		// If the existing doc isn't an SG write, import prior to updating
		if doc != nil && !isSgWrite && db.UseXattrs() {
			err := db.OnDemandImportForWrite(ctx, newDoc.ID, doc, newDoc.Deleted)
			if err != nil {
				return nil, nil, false, nil, err
			}
		}

		// Find the point where this doc's history branches from the current rev:
		currentRevIndex := len(docHistory)
		parent := ""
		for i, revid := range docHistory {
			if doc.History.contains(revid) {
				currentRevIndex = i
				parent = revid
				break
			}
		}
		if currentRevIndex == 0 {
			base.DebugfCtx(ctx, base.KeyCRUD, "PutExistingRevWithBody(%q): No new revisions to add", base.UD(newDoc.ID))
			newDoc.RevID = newRev
			return nil, nil, false, nil, base.ErrUpdateCancel // No new revisions to add
		}

		// Conflict-free mode check

		// We only bypass conflict resolution for incoming tombstones if the local doc is also a tombstone
		allowConflictingTombstone := forceAllowConflictingTombstone && doc.IsDeleted()

		if !allowConflictingTombstone && db.IsIllegalConflict(ctx, doc, parent, newDoc.Deleted, noConflicts, docHistory) {
			if conflictResolver == nil {
				return nil, nil, false, nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
			}
			_, updatedHistory, err := db.resolveConflict(ctx, doc, newDoc, docHistory, conflictResolver)
			if err != nil {
				base.InfofCtx(ctx, base.KeyCRUD, "Error resolving conflict for %s: %v", base.UD(doc.ID), err)
				return nil, nil, false, nil, err
			}
			if updatedHistory != nil {
				docHistory = updatedHistory
				// Recalculate the point where this doc's history branches from the current rev.
				// TODO: The only current scenario for conflict resolution is to add one entry to the history,
				//       could shorthand this to a +1?
				newRev = docHistory[0]
				currentRevIndex = len(docHistory)
				parent = ""
				for i, revid := range docHistory {
					if doc.History.contains(revid) {
						currentRevIndex = i
						parent = revid
						break
					}
				}
			}
		}

		// Add all the new-to-me revisions to the rev tree:
		for i := currentRevIndex - 1; i >= 0; i-- {
			err := doc.History.addRevision(newDoc.ID,
				RevInfo{
					ID:      docHistory[i],
					Parent:  parent,
					Deleted: i == 0 && newDoc.Deleted})

			if err != nil {
				return nil, nil, false, nil, err
			}
			parent = docHistory[i]
		}

		// Process the attachments, replacing bodies with digests.
		parentRevID := doc.History[newRev].Parent
		newAttachments, err := db.storeAttachments(ctx, doc, newDoc.Attachments(), generation, parentRevID, docHistory)
		if err != nil {
			return nil, nil, false, nil, err
		}

		newDoc.RevID = newRev

		return newDoc, newAttachments, false, nil, nil
	})

	return doc, newRev, err
}

func (db *DatabaseCollectionWithUser) PutExistingRevWithBody(ctx context.Context, docid string, body Body, docHistory []string, noConflicts bool, docUpdateEvent DocUpdateType) (doc *Document, newRev string, err error) {
	err = validateAPIDocUpdate(body)
	if err != nil {
		return nil, "", err
	}

	expiry, _ := body.ExtractExpiry()
	deleted := body.ExtractDeleted()
	revid := body.ExtractRev()

	newDoc := &Document{
		ID:        docid,
		Deleted:   deleted,
		DocExpiry: expiry,
		RevID:     revid,
	}

	delete(body, BodyId)
	delete(body, BodyRevisions)

	newDoc.SetAttachments(GetBodyAttachments(body))
	delete(body, BodyAttachments)

	newDoc.UpdateBody(body)

	doc, newRevID, putExistingRevErr := db.PutExistingRev(ctx, newDoc, docHistory, noConflicts, false, nil, docUpdateEvent)

	if putExistingRevErr != nil {
		return nil, "", putExistingRevErr
	}

	return doc, newRevID, err

}

// SyncFnDryrun Runs a document through the sync function and returns expiry, channels doc was placed in, access map for users, roles, handler errors and sync fn exceptions
func (db *DatabaseCollectionWithUser) SyncFnDryrun(ctx context.Context, body Body, docID string) (*channels.ChannelMapperOutput, error, error) {
	doc := &Document{
		ID:    docID,
		_body: body,
	}
	oldDoc := doc
	if docID != "" {
		if docInBucket, err := db.GetDocument(ctx, docID, DocUnmarshalAll); err == nil {
			oldDoc = docInBucket
			if doc._body == nil {
				body = oldDoc.Body(ctx)
				doc._body = body
				// If no body is given, use doc in bucket as doc with no old doc
				oldDoc._body = nil
			}
			doc._body[BodyRev] = oldDoc.SyncData.GetRevTreeID()
		} else {
			return nil, err, nil
		}
	} else {
		oldDoc._body = nil
	}

	delete(body, BodyId)

	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body[BodyRev].(string)
	generation, _ := ParseRevID(ctx, matchRev)
	if generation < 0 {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID"), nil
	}
	generation++

	// Create newDoc which will be used to pass around Body
	newDoc := &Document{
		ID: docID,
	}
	// Pull out attachments
	newDoc.SetAttachments(GetBodyAttachments(body))
	delete(body, BodyAttachments)

	delete(body, BodyRevisions)

	err := validateAPIDocUpdate(body)
	if err != nil {
		return nil, err, nil
	}
	bodyWithoutInternalProps, wasStripped := stripInternalProperties(body)
	canonicalBytesForRevID, err := base.JSONMarshalCanonical(bodyWithoutInternalProps)
	if err != nil {
		return nil, err, nil
	}

	// We needed to keep _deleted around in the body until we generated a rev ID, but now we can ditch it.
	_, isDeleted := body[BodyDeleted]
	if isDeleted {
		delete(body, BodyDeleted)
	}

	// and now we can finally update the newDoc body to be without any special properties
	newDoc.UpdateBody(body)

	// If no special properties were stripped and document wasn't deleted, the canonical bytes represent the current
	// body.  In this scenario, store canonical bytes as newDoc._rawBody
	if !wasStripped && !isDeleted {
		newDoc._rawBody = canonicalBytesForRevID
	}

	newRev := CreateRevIDWithBytes(generation, matchRev, canonicalBytesForRevID)
	newDoc.RevID = newRev
	mutableBody, metaMap, _, err := db.prepareSyncFn(oldDoc, newDoc)
	if err != nil {
		base.InfofCtx(ctx, base.KeyDiagnostic, "Failed to prepare to run sync function: %v", err)
		return nil, err, nil
	}

	output, err := db.ChannelMapper.MapToChannelsAndAccess(ctx, mutableBody, string(oldDoc._rawBody), metaMap,
		MakeUserCtx(db.user, db.ScopeName, db.Name))

	return output, nil, err
}

// revTreeConflictCheck checks for conflicts in the rev tree history and returns the parent revid, currentRevIndex
// (index of parent rev), and an error if the document is in conflict
func (db *DatabaseCollectionWithUser) revTreeConflictCheck(ctx context.Context, revTreeHistory []string, doc *Document, newDocDeleted bool) (string, int, error) {
	currentRevIndex := len(revTreeHistory)
	parent := ""
	if currentRevIndex > 0 {
		for i, revid := range revTreeHistory {
			if doc.History.contains(revid) {
				currentRevIndex = i
				parent = revid
				break
			}
		}
		// conflict check on rev tree history
		if db.IsIllegalConflict(ctx, doc, parent, newDocDeleted, true, revTreeHistory) {
			return "", 0, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
		}
	}
	return parent, currentRevIndex, nil
}

func buildResolverBody(localDoc, incomingDoc *Document) (Body, Body, error) {
	localRevID := localDoc.SyncData.GetRevTreeID()
	localAttachments := localDoc.Attachments()
	localExpiry := localDoc.SyncData.Expiry

	remoteAttachments := incomingDoc.Attachments()

	localDocBody, err := localDoc.GetDeepMutableBody()
	if err != nil {
		return nil, nil, err
	}
	localDocBody[BodyRev] = localRevID
	localDocBody[BodyId] = localDoc.ID
	localDocBody[BodyAttachments] = localAttachments
	localDocBody[BodyExpiry] = localExpiry
	localDocBody[BodyDeleted] = localDoc.IsDeleted()
	localDocBody[BodyCV] = localDoc.HLV.GetCurrentVersionString()

	remoteDocBody, err := incomingDoc.GetDeepMutableBody()
	if err != nil {
		return nil, nil, err
	}
	remoteDocBody[BodyRev] = incomingDoc.RevID
	remoteDocBody[BodyId] = incomingDoc.ID
	remoteDocBody[BodyAttachments] = remoteAttachments
	remoteDocBody[BodyDeleted] = incomingDoc.Deleted
	remoteDocBody[BodyCV] = incomingDoc.HLV.GetCurrentVersionString()

	return localDocBody, remoteDocBody, nil
}

func (db *DatabaseCollectionWithUser) resolveHLVConflict(ctx context.Context, localDoc, incomingDoc *Document, resolver *ConflictResolver) (*HybridLogicalVector, error) {
	if resolver == nil {
		return nil, errors.New("Conflict resolution function is nil for resolveConflict")
	}

	localDocBody, remoteDocBody, err := buildResolverBody(localDoc, incomingDoc)
	if err != nil {
		base.InfofCtx(ctx, base.KeyReplicate, "Error when building local and remote documents for conflict resolution for doc %s: %v", base.UD(localDoc.ID), err)
		return nil, err
	}

	conflict := Conflict{
		LocalDocument:  localDocBody,
		RemoteDocument: remoteDocBody,
		LocalHLV:       localDoc.HLV,
		RemoteHLV:      incomingDoc.HLV,
	}

	_, resolutionType, resolveFuncError := resolver.ResolveForHLV(ctx, conflict)
	if resolveFuncError != nil {
		base.InfofCtx(ctx, base.KeyReplicate, "Error when running conflict resolution for doc %s: %v", base.UD(localDoc.ID), resolveFuncError)
		return nil, resolveFuncError
	}

	var resolvedError error
	var newHLV *HybridLogicalVector
	switch resolutionType {
	case ConflictResolutionLocal:
		newHLV, resolvedError = db.resolveLocalWinsHLV(ctx, localDoc, incomingDoc)
		return newHLV, resolvedError
	case ConflictResolutionRemote:
		newHLV, resolvedError = remoteWinsConflictResolutionForHLV(ctx, incomingDoc.ID, localDoc.HLV, incomingDoc.HLV)
		return newHLV, resolvedError
	case ConflictResolutionMerge:
		// not yet implemented (custom conflict resolution CBG-4779)
		return nil, fmt.Errorf("Conflict resolution type %v not implemented for HLV", resolutionType)
	default:
		return nil, fmt.Errorf("Unexpected conflict resolution type: %v", resolutionType)
	}
}

// resolveConflict runs the conflictResolverFunction with doc and newDoc.  doc and newDoc's bodies and revision trees
// may be changed based on the outcome of conflict resolution - see resolveDocLocalWins, resolveDocRemoteWins and
// resolveDocMerge for specifics on what is changed under each scenario.
func (db *DatabaseCollectionWithUser) resolveConflict(ctx context.Context, localDoc *Document, remoteDoc *Document, docHistory []string, resolver *ConflictResolver) (resolvedRevID string, updatedHistory []string, resolveError error) {

	if resolver == nil {
		return "", nil, errors.New("Conflict resolution function is nil for resolveConflict")
	}

	localDocBody, remoteDocBody, err := buildResolverBody(localDoc, remoteDoc)
	if err != nil {
		base.InfofCtx(ctx, base.KeyReplicate, "Error when building local and remote documents for conflict resolution for doc %s: %v", base.UD(localDoc.ID), err)
		return "", nil, err
	}

	conflict := Conflict{
		LocalDocument:  localDocBody,
		RemoteDocument: remoteDocBody,
	}

	resolvedBody, resolutionType, resolveFuncError := resolver.Resolve(ctx, conflict)
	if resolveFuncError != nil {
		base.InfofCtx(ctx, base.KeyReplicate, "Error when running conflict resolution for doc %s: %v", base.UD(localDoc.ID), resolveFuncError)
		return "", nil, resolveFuncError
	}

	switch resolutionType {
	case ConflictResolutionLocal:
		resolvedRevID, updatedHistory, resolveError = db.resolveDocLocalWins(ctx, localDoc, remoteDoc, conflict, docHistory)
		return resolvedRevID, updatedHistory, resolveError
	case ConflictResolutionRemote:
		resolvedRevID, resolveError = db.resolveDocRemoteWins(ctx, localDoc, conflict)
		return resolvedRevID, nil, resolveError
	case ConflictResolutionMerge:
		resolvedRevID, updatedHistory, resolveError = db.resolveDocMerge(ctx, localDoc, remoteDoc, conflict, docHistory, resolvedBody)
		return resolvedRevID, updatedHistory, resolveError
	default:
		return "", nil, fmt.Errorf("Unexpected conflict resolution type: %v", resolutionType)
	}
}

// resolveDocRemoteWins makes the following changes to the document:
//   - Tombstones the local revision
//
// The remote revision is added to the revision tree by the standard update processing.
func (db *DatabaseCollectionWithUser) resolveDocRemoteWins(ctx context.Context, localDoc *Document, conflict Conflict) (resolvedRevID string, err error) {

	// Tombstone the local revision
	localRevID := localDoc.GetRevTreeID()
	tombstoneRevID, tombstoneErr := db.tombstoneActiveRevision(ctx, localDoc, localRevID)
	if err != nil {
		return "", tombstoneErr
	}
	remoteRevID := conflict.RemoteDocument.ExtractRev()
	base.DebugfCtx(ctx, base.KeyReplicate, "Resolved conflict for doc %s as remote wins - remote rev is %s, previous local rev %s tombstoned by %s, ", base.UD(localDoc.ID), remoteRevID, localRevID, tombstoneRevID)
	return remoteRevID, nil
}

// resolveDocLocalWins makes the following updates to the revision tree:
//   - Adds the remote revision to the rev tree
//   - Makes a copy of the local revision as a child of the remote revision
//   - Tombstones the (original) local revision
//
// TODO: This is CBL 2.x handling, and is compatible with the current version of the replicator, but
//
//	results in additional replication work for clients that have previously replicated the local
//	revision.  This will be addressed post-Hydrogen with version vector work, but additional analysis
//	of options for Hydrogen should be completed.
func (db *DatabaseCollectionWithUser) resolveDocLocalWins(ctx context.Context, localDoc *Document, remoteDoc *Document, conflict Conflict, docHistory []string) (resolvedRevID string, updatedHistory []string, err error) {

	// Clone the local revision as a child of the remote revision
	docBodyBytes, err := localDoc.BodyBytes(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("Unable to retrieve local document body while resolving conflict: %w", err)
	}

	remoteRevID := remoteDoc.RevID
	remoteGeneration, _ := ParseRevID(ctx, remoteRevID)
	var newRevID string

	if !localDoc.Deleted {
		// If the local doc is not a tombstone, we're just rewriting it as a child of the remote
		newRevID = CreateRevIDWithBytes(remoteGeneration+1, remoteRevID, docBodyBytes)
	} else {
		// If the local doc is a tombstone, we're going to end up with both the local and remote branches tombstoned,
		// and need to ensure the remote branch is the winning branch. To do that, we inject entries into the remote
		// branch's history until it's generation is longer than the local branch.
		remoteDoc.Deleted = localDoc.Deleted
		localGeneration, _ := ParseRevID(ctx, localDoc.GetRevTreeID())

		requiredAdditionalRevs := localGeneration - remoteGeneration
		injectedRevBody := []byte("{}")
		injectedGeneration := remoteGeneration
		for i := 0; i < requiredAdditionalRevs; i++ {
			injectedGeneration++
			remoteLeafRevID := docHistory[0]
			injectedRevID := CreateRevIDWithBytes(injectedGeneration, remoteLeafRevID, injectedRevBody)
			docHistory = append([]string{injectedRevID}, docHistory...)
		}
		newRevID = CreateRevIDWithBytes(injectedGeneration+1, docHistory[0], docBodyBytes)
	}

	// Update the history for the incoming doc to prepend the cloned revID
	docHistory = append([]string{newRevID}, docHistory...)
	remoteDoc.RevID = newRevID

	// Set the incoming document's rev, body, deleted flag and attachment to the cloned local revision.
	// Note: not setting expiry, as syncData.expiry is reference only and isn't guaranteed to match the bucket doc expiry
	remoteDoc.RemoveBody()
	remoteDoc.Deleted = localDoc.IsDeleted()
	remoteDoc.SetAttachments(localDoc.Attachments().ShallowCopy())

	// If the local doc had attachments, any with revpos more recent than the common ancestor will need
	// to have their revpos updated when we rewrite the rev as a child of the remote branch.
	if remoteDoc.Attachments() != nil {
		// Identify generation of common ancestor and new rev
		commonAncestorRevID := localDoc.SyncData.History.findAncestorFromSet(localDoc.GetRevTreeID(), docHistory)
		commonAncestorGen := 0
		if commonAncestorRevID != "" {
			commonAncestorGen, _ = ParseRevID(ctx, commonAncestorRevID)
		}
		newRevIDGen, _ := ParseRevID(ctx, newRevID)

		// If attachment revpos is older than common ancestor, or common ancestor doesn't exist, set attachment's
		// revpos to the generation of newRevID (i.e. treat as previously unknown to this revtree branch)
		for _, value := range remoteDoc.Attachments() {
			attachmentMeta, ok := value.(map[string]interface{})
			if !ok {
				base.WarnfCtx(ctx, "Unable to parse attachment meta during conflict resolution for %s/%s: %v", base.UD(localDoc.ID), localDoc.SyncData.GetRevTreeID(), value)
				continue
			}
			revpos, _ := base.ToInt64(attachmentMeta["revpos"])
			if revpos > int64(commonAncestorGen) || commonAncestorGen == 0 {
				attachmentMeta["revpos"] = newRevIDGen
			}
		}
	}

	remoteDoc._rawBody = docBodyBytes

	// Tombstone the local revision
	localRevID := localDoc.GetRevTreeID()
	tombstoneRevID, tombstoneErr := db.tombstoneActiveRevision(ctx, localDoc, localRevID)
	if tombstoneErr != nil {
		return "", nil, tombstoneErr
	}

	base.DebugfCtx(ctx, base.KeyReplicate, "Resolved conflict for doc %s as localWins - local rev %s moved to %s, and tombstoned with %s", base.UD(localDoc.ID), localRevID, newRevID, tombstoneRevID)
	return newRevID, docHistory, nil
}

// resolveDocMerge makes the following updates to the revision tree
//   - Tombstones the local revision
//   - Modifies the incoming document body to the merged body
//   - Modifies the incoming history to prepend the merged revid (retaining the previous remote revID as its parent)
func (db *DatabaseCollectionWithUser) resolveDocMerge(ctx context.Context, localDoc *Document, remoteDoc *Document, conflict Conflict, docHistory []string, mergedBody Body) (resolvedRevID string, updatedHistory []string, err error) {

	// Move attachments from the merged body to the incoming DocAttachments for normal processing.
	bodyAtts, ok := mergedBody[BodyAttachments]
	if ok {
		attsMap, ok := bodyAtts.(map[string]interface{})
		if ok {
			remoteDoc.SetAttachments(attsMap)
			delete(mergedBody, BodyAttachments)
		}
	}

	// Tombstone the local revision
	localRevID := localDoc.GetRevTreeID()
	tombstoneRevID, tombstoneErr := db.tombstoneActiveRevision(ctx, localDoc, localRevID)
	if tombstoneErr != nil {
		return "", nil, tombstoneErr
	}

	remoteRevID := remoteDoc.RevID
	remoteGeneration, _ := ParseRevID(ctx, remoteRevID)
	mergedRevID, err := CreateRevID(remoteGeneration+1, remoteRevID, mergedBody)
	if err != nil {
		return "", nil, err
	}

	// Update the remote document's body to the merge result
	remoteDoc.RevID = mergedRevID
	remoteDoc.RemoveBody()
	remoteDoc._body = mergedBody

	// Update the history for the remote doc to prepend the merged revID
	docHistory = append([]string{mergedRevID}, docHistory...)

	base.DebugfCtx(ctx, base.KeyReplicate, "Resolved conflict for doc %s as merge - merged rev %s added as child of %s, previous local rev %s tombstoned by %s", base.UD(localDoc.ID), mergedRevID, remoteRevID, localRevID, tombstoneRevID)
	return mergedRevID, docHistory, nil
}

// resolveLocalWinsHLV will update remote doc's body and attachments to match the local doc, and return a new HLV for local wins
func (db *DatabaseCollectionWithUser) resolveLocalWinsHLV(ctx context.Context, localDoc, remoteDoc *Document) (*HybridLogicalVector, error) {

	docBodyBytes, err := localDoc.BodyBytes(ctx)
	if err != nil {
		return nil, fmt.Errorf("Unable to retrieve local document body while resolving conflict: %w", err)
	}

	newHLV, err := localWinsConflictResolutionForHLV(ctx, localDoc.HLV, remoteDoc.HLV, localDoc.ID, db.dbCtx.EncodedSourceID)
	if err != nil {
		return nil, err
	}

	remoteDoc.RemoveBody()
	remoteDoc.Deleted = localDoc.IsDeleted()
	remoteDoc.SetAttachments(localDoc.Attachments().ShallowCopy())

	remoteDoc._rawBody = docBodyBytes
	return newHLV, nil
}

// tombstoneRevision updates the document's revision tree to add a tombstone revision as a child of the specified revID
func (db *DatabaseCollectionWithUser) tombstoneActiveRevision(ctx context.Context, doc *Document, revID string) (tombstoneRevID string, err error) {

	if doc.GetRevTreeID() != revID {
		return "", fmt.Errorf("Attempted to tombstone active revision for doc (%s), but provided rev (%s) doesn't match current rev(%s)", base.UD(doc.ID), revID, doc.GetRevTreeID())
	}

	// Don't tombstone an already deleted revision, return the incoming revID instead.
	if doc.IsDeleted() {
		base.DebugfCtx(ctx, base.KeyReplicate, "Active revision %s/%s is already tombstoned.", base.UD(doc.ID), revID)
		return revID, nil
	}

	// Create tombstone
	newGeneration := genOfRevID(ctx, revID) + 1
	newRevID := CreateRevIDWithBytes(newGeneration, revID, []byte(DeletedDocument))
	err = doc.History.addRevision(doc.ID,
		RevInfo{
			ID:      newRevID,
			Parent:  revID,
			Deleted: true,
		})
	if err != nil {
		return "", err
	}

	// Backup previous revision body, then remove the current body from the doc
	bodyBytes, err := doc.BodyBytes(ctx)
	if err == nil {
		_ = db.setOldRevisionJSONBody(ctx, doc.ID, revID, bodyBytes, db.oldRevExpirySeconds())
	}
	doc.RemoveBody()

	return newRevID, nil
}

func (doc *Document) updateWinningRevAndSetDocFlags(ctx context.Context) {
	revtreeID, branched, inConflict := doc.History.winningRevision(ctx)
	doc.SetRevTreeID(revtreeID)
	doc.setFlag(channels.Deleted, doc.History[revtreeID].Deleted)
	doc.setFlag(channels.Conflict, inConflict)
	doc.setFlag(channels.Branched, branched)
	if doc.hasFlag(channels.Deleted) {
		doc.SyncData.TombstonedAt = time.Now().Unix()
	} else {
		doc.SyncData.TombstonedAt = 0
	}
}

func (db *DatabaseCollectionWithUser) storeOldBodyInRevTreeAndUpdateCurrent(ctx context.Context, doc *Document, prevCurrentRev string, newRevID string, newDoc *Document, newDocHasAttachments bool) {
	if doc.HasBody() && doc.GetRevTreeID() != prevCurrentRev && prevCurrentRev != "" {
		// Store the doc's previous body into the revision tree:
		oldBodyJson, marshalErr := doc.BodyBytes(ctx)
		if marshalErr != nil {
			base.WarnfCtx(ctx, "Unable to marshal document body for storage in rev tree: %v", marshalErr)
		}

		var kvPairs []base.KVPair
		oldDocHasAttachments := false

		// Stamp _attachments into the old body we're about to backup
		// We need to do a revpos check here because doc actually contains the new attachments
		if len(doc.Attachments()) > 0 {
			prevCurrentRevGen, _ := ParseRevID(ctx, prevCurrentRev)
			bodyAtts := make(AttachmentsMeta)
			for attName, attMeta := range doc.Attachments() {
				if attMetaMap, ok := attMeta.(map[string]interface{}); ok {
					var attRevposInt int
					if attRevpos, ok := attMetaMap["revpos"].(int); ok {
						attRevposInt = attRevpos
					} else if attRevPos, ok := attMetaMap["revpos"].(float64); ok {
						attRevposInt = int(attRevPos)
					}
					if attRevposInt <= prevCurrentRevGen {
						bodyAtts[attName] = attMeta
					}

					version, _ := GetAttachmentVersion(attMetaMap)
					if version == AttVersion2 {
						oldDocHasAttachments = true
					}
				}
			}
			if len(bodyAtts) > 0 {
				kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: bodyAtts})
			}
		}

		if ancestorRev, ok := doc.History[prevCurrentRev]; ok && ancestorRev != nil && ancestorRev.Deleted {
			kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
		}

		// Stamp _attachments and _deleted into rev tree bodies
		oldBodyJson, marshalErr = base.InjectJSONProperties(oldBodyJson, kvPairs...)
		if marshalErr != nil {
			base.WarnfCtx(ctx, "Unable to marshal document body properties for storage in rev tree: %v", marshalErr)
		}
		doc.setNonWinningRevisionBody(prevCurrentRev, oldBodyJson, db.AllowExternalRevBodyStorage(), oldDocHasAttachments)
	}
	// Store the new revision body into the doc:
	doc.setRevisionBody(ctx, newRevID, newDoc, db.AllowExternalRevBodyStorage(), newDocHasAttachments)
	doc.SetAttachments(newDoc.Attachments())
	doc.MetadataOnlyUpdate = newDoc.MetadataOnlyUpdate

	if doc.GetRevTreeID() == newRevID {
		doc.NewestRev = ""
		doc.setFlag(channels.Hidden, false)
	} else {
		doc.NewestRev = newRevID
		doc.setFlag(channels.Hidden, true)
		if doc.GetRevTreeID() != prevCurrentRev {
			doc.promoteNonWinningRevisionBody(ctx, doc.GetRevTreeID(), db.RevisionBodyLoader)
			// If the update resulted in promoting a previous non-winning revision body to winning, this isn't a metadata only update.
			doc.MetadataOnlyUpdate = nil
		}
	}
}

func (db *DatabaseCollectionWithUser) prepareSyncFn(doc *Document, newDoc *Document) (mutableBody Body, metaMap map[string]interface{}, newRevID string, err error) {
	// Marshal raw user xattrs for use in Sync Fn. If this fails we can bail out so we should do early as possible.
	metaMap, err = doc.GetMetaMap(db.userXattrKey())
	if err != nil {
		return
	}

	mutableBody, err = newDoc.GetDeepMutableBody()
	if err != nil {
		return
	}

	err = validateNewBody(mutableBody)
	if err != nil {
		return
	}

	newRevID = newDoc.RevID

	mutableBody[BodyId] = doc.ID
	mutableBody[BodyRev] = newRevID
	if newDoc.Deleted {
		mutableBody[BodyDeleted] = true
	}

	return
}

// Run the sync function on the given document and body. Need to inject the document ID and rev ID temporarily to run
// the sync function.
func (db *DatabaseCollectionWithUser) runSyncFn(ctx context.Context, doc *Document, body Body, metaMap map[string]interface{}, newRevId string) (*uint32, string, base.Set, channels.AccessMap, channels.AccessMap, error) {
	channelSet, access, roles, syncExpiry, oldBody, err := db.getChannelsAndAccess(ctx, doc, body, metaMap, newRevId)
	if err != nil {
		return nil, ``, nil, nil, nil, err
	}
	db.checkDocChannelsAndGrantsLimits(ctx, doc.ID, channelSet, access, roles)
	return syncExpiry, oldBody, channelSet, access, roles, nil
}

func (db *DatabaseCollectionWithUser) recalculateSyncFnForActiveRev(ctx context.Context, doc *Document, metaMap map[string]interface{}, newRevID string) (channelSet base.Set, access, roles channels.AccessMap, syncExpiry *uint32, oldBodyJSON string, err error) {
	// In some cases an older revision might become the current one. If so, get its
	// channels & access, for purposes of updating the doc:
	curBodyBytes, err := db.getAvailable1xRev(ctx, doc, doc.GetRevTreeID())
	if err != nil {
		return
	}

	var curBody Body
	err = curBody.Unmarshal(curBodyBytes)
	if err != nil {
		return
	}

	if curBody != nil {
		base.DebugfCtx(ctx, base.KeyCRUD, "updateDoc(%q): Rev %q causes %q to become current again",
			base.UD(doc.ID), newRevID, doc.GetRevTreeID())
		channelSet, access, roles, syncExpiry, oldBodyJSON, err = db.getChannelsAndAccess(ctx, doc, curBody, metaMap, doc.GetRevTreeID())
		if err != nil {
			return
		}
	} else {
		// Shouldn't be possible (CurrentRev is a leaf so won't have been compacted)
		base.WarnfCtx(ctx, "updateDoc(%q): Rev %q missing, can't call getChannelsAndAccess "+
			"on it (err=%v)", base.UD(doc.ID), doc.GetRevTreeID(), err)
		channelSet = nil
		access = nil
		roles = nil
	}
	return
}

func (db *DatabaseCollectionWithUser) addAttachments(ctx context.Context, newAttachments updatedAttachments) error {
	// Need to check and add attachments here to ensure the attachment is within size constraints
	err := db.setAttachments(ctx, newAttachments)
	if err != nil {
		if errors.Is(err, ErrAttachmentTooLarge) || err.Error() == "document value was too large" {
			err = base.HTTPErrorf(http.StatusRequestEntityTooLarge, "Attachment too large")
		} else {
			err = errors.Wrap(err, "Error adding attachment")
		}
	}
	return err
}

// assignSequence assigns a global sequence number from database.
func (c *DatabaseCollectionWithUser) assignSequence(ctx context.Context, docSequence uint64, doc *Document, unusedSequences []uint64) ([]uint64, error) {
	return c.dbCtx.assignSequence(ctx, docSequence, doc, unusedSequences)
}

// Sequence processing :
// Assigns provided sequence to the document
// Update unusedSequences in the event that there is a conflict and we have to provide a new sequence number
// Update and prune RecentSequences
func (db *DatabaseContext) assignSequence(ctx context.Context, docSequence uint64, doc *Document, unusedSequences []uint64) ([]uint64, error) {

	// Assign the next sequence number, for _changes feed.
	// Be careful not to request a second sequence # on a retry if we don't need one.
	if docSequence <= doc.Sequence {
		if docSequence > 0 {
			// Oops: we're on our second iteration thanks to a conflict, but the sequence
			// we previously allocated is unusable now. We have to allocate a new sequence
			// instead, but we add the unused one(s) to the document so when the changeCache
			// reads the doc it won't freak out over the break in the sequence numbering.
			base.InfofCtx(ctx, base.KeyCache, "updateDoc %q: Unused sequence #%d", base.UD(doc.ID), docSequence)
			unusedSequences = append(unusedSequences, docSequence)
		}

		var err error
		if docSequence, err = db.sequences.nextSequence(ctx); err != nil {
			return unusedSequences, err
		}
		firstAllocatedSequence := docSequence

		// If the assigned sequence is less than or equal to the previous sequence on the document, release
		// the assigned sequence and acquire one using nextSequenceGreaterThan
		if docSequence <= doc.Sequence {
			if err = db.sequences.releaseSequence(ctx, docSequence); err != nil {
				base.WarnfCtx(ctx, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", docSequence, err)
			}
			var releasedSequenceCount uint64
			docSequence, releasedSequenceCount, err = db.sequences.nextSequenceGreaterThan(ctx, doc.Sequence)
			if err != nil {
				return unusedSequences, err
			}
			if releasedSequenceCount > unusedSequenceWarningThreshold {
				base.WarnfCtx(ctx, "Doc %s / %s had an existing sequence %d that is higher than the next db sequence value %d, resulting in the release of %d unused sequences. This may indicate documents being migrated between databases by an external process.", base.UD(doc.ID), doc.GetRevTreeID(), doc.Sequence, firstAllocatedSequence, releasedSequenceCount)
			}

		}
	}

	doc.Sequence = docSequence
	doc.UnusedSequences = unusedSequences

	// The server DCP feed will deduplicate multiple revisions for the same doc if they occur in
	// the same mutation queue processing window. This results in missing sequences on the change listener.
	// To account for this, we track the recent sequence numbers for the document.
	if doc.RecentSequences == nil {
		doc.RecentSequences = make([]uint64, 0, 1+len(unusedSequences))
	}

	if len(doc.RecentSequences) >= kMaxRecentSequences {
		// Prune recent sequences that are earlier than the nextSequence.  The dedup window
		// on the feed is small - sub-second, so we usually shouldn't care about more than
		// a few recent sequences.  However, the pruning has some overhead (read lock on nextSequence),
		// so we're allowing more 'recent sequences' on the doc (20) before attempting pruning
		stableSequence := db.changeCache.GetStableSequence(doc.ID).Seq
		count := 0
		// we want to keep at least kMinRecentSequences recent sequences in the recent sequences list to reduce likelihood
		// races between compaction of resent sequences and a coalesced DCP mutation resulting in skipped/abandoned sequences
		maxToCompact := len(doc.RecentSequences) - kMinRecentSequences
		for _, seq := range doc.RecentSequences {
			// Only remove sequences if they are higher than a sequence that's been seen on the
			// feed. This is valid across SG nodes (which could each have a different nextSequence),
			// as the mutations that this node used to rev nextSequence will at some point be delivered
			// to each node.
			if seq < stableSequence {
				count++
				if count == maxToCompact {
					break
				}
			} else {
				break
			}
		}
		if count > 0 {
			doc.RecentSequences = doc.RecentSequences[count:]
		}
	}

	// Append current sequence and unused sequences to recent sequence history.
	// CAS failures can result in unusedSequences being older than existing recentSequences,
	// so sorting is required
	doc.RecentSequences = append(doc.RecentSequences, unusedSequences...)
	doc.RecentSequences = append(doc.RecentSequences, docSequence)
	if len(doc.RecentSequences) > 1 {
		base.SortedUint64Slice(doc.RecentSequences).Sort()
	}

	return unusedSequences, nil
}

func (doc *Document) updateExpiry(syncExpiry, updatedExpiry *uint32, expiry *uint32) (finalExp *uint32) {
	if syncExpiry != nil {
		finalExp = syncExpiry
	} else if updatedExpiry != nil {
		finalExp = updatedExpiry
	} else if expiry != nil {
		finalExp = expiry
	}

	if finalExp != nil {
		doc.UpdateExpiry(*finalExp)
	} else {
		doc.UpdateExpiry(0)

	}

	return finalExp

}

// IsIllegalConflict returns true if the given operation is forbidden due to conflicts.
// AllowConflicts is whether or not the database allows conflicts,
// and 'noConflicts' is whether or not the request should allow conflicts to occur.
/*
Truth table for AllowConflicts and noConflicts combinations:

                       AllowConflicts=true     AllowConflicts=false
   noConflicts=true    continue checks         continue checks
   noConflicts=false   return false            continue checks */
func (db *DatabaseCollectionWithUser) IsIllegalConflict(ctx context.Context, doc *Document, parentRevID string, deleted, noConflicts bool, docHistory []string) bool {
	if db.AllowConflicts() && !noConflicts {
		return false
	}

	// Conflict-free mode: If doc exists, it must satisfy one of the following:
	//   (a) its current rev is the new rev's parent
	//   (b) the new rev is a tombstone, whose parent is an existing non-tombstoned leaf
	//   (c) the current rev is a tombstone, and the new rev is a non-tombstone disconnected branch

	// case a: If the parent is the current rev, it's not a conflict.
	if parentRevID == doc.GetRevTreeID() || doc.GetRevTreeID() == "" {
		return false
	}

	// case b: If it's a tombstone, it's allowed if it's tombstoning an existing non-tombstoned leaf
	if deleted {
		for _, leafRevId := range doc.History.GetLeaves() {
			if leafRevId == parentRevID && doc.History[leafRevId].Deleted == false {
				return false
			}
		}
		base.DebugfCtx(ctx, base.KeyCRUD, "Conflict - tombstone updates to non-leaf or already tombstoned revisions aren't valid when allow_conflicts=false")
		return true
	}

	// case c: If current doc is a tombstone, disconnected branch resurrections are allowed
	if doc.IsDeleted() {
		for _, ancestorRevID := range docHistory {
			_, ok := doc.History[ancestorRevID]
			if ok {
				base.DebugfCtx(ctx, base.KeyCRUD, "Conflict - document is deleted, but update would branch from existing revision.")
				return true
			}
		}
		return false
	}

	// If we haven't found a valid conflict scenario by this point, flag as invalid
	base.DebugfCtx(ctx, base.KeyCRUD, "Conflict - non-tombstone updates to non-winning revisions aren't valid when allow_conflicts=false")
	return true
}

func (col *DatabaseCollectionWithUser) documentUpdateFunc(
	ctx context.Context,
	docExists bool,
	doc *Document,
	allowImport bool,
	previousDocSequenceIn uint64,
	unusedSequences []uint64,
	callback updateAndReturnDocCallback,
	expiry *uint32,
	docUpdateEvent DocUpdateType,
) (
	retSyncFuncExpiry *uint32,
	retNewRevID string,
	retStoredDoc *Document,
	retOldBodyJSON string,
	retUnusedSequences []uint64,
	changedAccessPrincipals []string,
	changedRoleAccessUsers []string,
	createNewRevIDSkipped bool,
	revokedChannelsRequiringExpansion []string,
	err error) {

	err = validateExistingDoc(doc, allowImport, docExists)
	if err != nil {
		return
	}

	// compute mouMatch before the callback modifies doc.MetadataOnlyUpdate
	mouMatch := false
	if doc.MetadataOnlyUpdate != nil && doc.MetadataOnlyUpdate.CAS() == doc.Cas {
		mouMatch = doc.MetadataOnlyUpdate.CAS() == doc.Cas
		base.DebugfCtx(ctx, base.KeyVV, "updateDoc(%q): _mou:%+v Metadata-only update match:%t", base.UD(doc.ID), doc.MetadataOnlyUpdate, mouMatch)
	} else {
		base.DebugfCtx(ctx, base.KeyVV, "updateDoc(%q): has no _mou", base.UD(doc.ID))
	}
	// Invoke the callback to update the document and with a new revision body to be used by the Sync Function:
	newDoc, newAttachments, createNewRevIDSkipped, updatedExpiry, err := callback(doc)
	if err != nil {
		return
	}

	mutableBody, metaMap, newRevID, err := col.prepareSyncFn(doc, newDoc)
	if err != nil {
		return
	}

	prevCurrentRev := doc.GetRevTreeID()
	doc.updateWinningRevAndSetDocFlags(ctx)
	newDocHasAttachments := len(newAttachments) > 0
	col.storeOldBodyInRevTreeAndUpdateCurrent(ctx, doc, prevCurrentRev, newRevID, newDoc, newDocHasAttachments)

	syncExpiry, oldBodyJSON, channelSet, access, roles, err := col.runSyncFn(ctx, doc, mutableBody, metaMap, newRevID)
	if err != nil {
		if col.ForceAPIForbiddenErrors() {
			base.InfofCtx(ctx, base.KeyCRUD, "Sync function rejected update to %s %s due to %v",
				base.UD(doc.ID), base.MD(doc.RevID), err)
			err = ErrForbidden
		}
		return
	}

	if len(channelSet) > 0 {
		doc.History[newRevID].Channels = channelSet
	}

	if newAttachments != nil {
		err = col.addAttachments(ctx, newAttachments)
		if err != nil {
			return
		}
		for _, att := range newAttachments {
			auditFields := base.AuditFields{
				base.AuditFieldDocID:        doc.ID,
				base.AuditFieldDocVersion:   newRevID,
				base.AuditFieldAttachmentID: att.name,
			}
			if att.created {
				base.Audit(ctx, base.AuditIDAttachmentCreate, auditFields)
			} else {
				base.Audit(ctx, base.AuditIDAttachmentUpdate, auditFields)
			}
		}
	}

	col.backupAncestorRevs(ctx, doc, newDoc)

	unusedSequences, err = col.assignSequence(ctx, previousDocSequenceIn, doc, unusedSequences)
	if err != nil {
		if errors.Is(err, base.ErrMaxSequenceReleasedExceeded) {
			base.ErrorfCtx(ctx, "Doc %s / %s had a much larger sequence (%d) than the current sequence number. Document update will be cancelled, since we don't want to allocate sequences to fill a gap this large. This may indicate document metadata being migrated between databases where it should've been stripped and re-imported.", base.UD(newDoc.ID), prevCurrentRev, doc.Sequence)
		}
		return
	}

	// The callback has updated the HLV for mutations coming from CBL.  Update the HLV so that the current version is set before
	// we call updateChannels, which needs to set the current version for removals
	// update the HLV values
	doc, err = col.updateHLV(ctx, doc, docUpdateEvent, mouMatch)
	if err != nil {
		return
	}

	if doc.GetRevTreeID() != prevCurrentRev || createNewRevIDSkipped {
		// Most of the time this update will change the doc's current rev. (The exception is
		// if the new rev is a conflict that doesn't win the revid comparison.) If so, we
		// need to update the doc's top-level Channels and Access properties to correspond
		// to the current rev's state.
		if newRevID != doc.GetRevTreeID() {
			channelSet, access, roles, syncExpiry, oldBodyJSON, err = col.recalculateSyncFnForActiveRev(ctx, doc, metaMap, newRevID)
			if err != nil {
				return
			}
		}
		_, revokedChannelsRequiringExpansion, err = doc.updateChannels(ctx, channelSet)
		if err != nil {
			return
		}
		changedAccessPrincipals = doc.Access.updateAccess(ctx, doc, access)
		changedRoleAccessUsers = doc.RoleAccess.updateAccess(ctx, doc, roles)
	} else {

		base.DebugfCtx(ctx, base.KeyCRUD, "updateDoc(%q): Rev %q leaves %q still current",
			base.UD(doc.ID), newRevID, prevCurrentRev)
	}

	// Prune old revision history to limit the number of revisions:
	if pruned := doc.pruneRevisions(ctx, col.revsLimit(), doc.GetRevTreeID()); pruned > 0 {
		base.DebugfCtx(ctx, base.KeyCRUD, "updateDoc(%q): Pruned %d old revisions", base.UD(doc.ID), pruned)
	}

	updatedExpiry = doc.updateExpiry(syncExpiry, updatedExpiry, expiry)
	err = doc.persistModifiedRevisionBodies(col.dataStore)
	if err != nil {
		return
	}

	doc.ClusterUUID = col.serverUUID()
	doc.TimeSaved = time.Now()
	return updatedExpiry, newRevID, newDoc, oldBodyJSON, unusedSequences, changedAccessPrincipals, changedRoleAccessUsers, createNewRevIDSkipped, revokedChannelsRequiringExpansion, err
}

// Function type for the callback passed into updateAndReturnDoc
type updateAndReturnDocCallback func(*Document) (resultDoc *Document, resultAttachmentData updatedAttachments, createNewRevIDSkipped bool, updatedExpiry *uint32, resultErr error)

// Calling updateAndReturnDoc directly allows callers to:
//  1. Receive the updated document body in the response
//  2. Specify the existing document body/xattr/cas, to avoid initial retrieval of the doc in cases that the current contents are already known (e.g. import).
//     On cas failure, the document will still be reloaded from the bucket as usual.
//  3. If isImport=true, document body will not be updated - only metadata xattr(s)

func (db *DatabaseCollectionWithUser) updateAndReturnDoc(ctx context.Context, docid string, allowImport bool, expiry *uint32, opts *sgbucket.MutateInOptions, docUpdateEvent DocUpdateType, existingDoc *sgbucket.BucketDocument, isImport bool, updateRevCache bool, callback updateAndReturnDocCallback) (doc *Document, newRevID string, err error) {
	key := realDocID(docid)
	if key == "" {
		return nil, "", base.HTTPErrorf(400, "Invalid doc ID")
	}

	var prevCurrentRev string
	var storedDoc *Document
	var changedAccessPrincipals, changedRoleAccessUsers []string // Returned by documentUpdateFunc
	var docSequence uint64                                       // Must be scoped outside callback, used over multiple iterations
	var unusedSequences []uint64                                 // Must be scoped outside callback, used over multiple iterations
	var oldBodyJSON string                                       // Stores previous revision body for use by DocumentChangeEvent
	var createNewRevIDSkipped bool
	var previousAttachments map[string][]string

	// Update the document
	inConflict := false
	upgradeInProgress := false
	docBytes := 0   // Track size of document written, for write stats
	xattrBytes := 0 // Track size of xattr written, for write stats
	skipObsoleteAttachmentsRemoval := false
	isNewDocCreation := false

	if db.UseXattrs() || upgradeInProgress {
		var casOut uint64
		// Update the document, storing metadata in extended attribute
		if opts == nil {
			opts = &sgbucket.MutateInOptions{}
		}
		opts.MacroExpansion = macroExpandSpec(base.SyncXattrName)
		var initialExpiry uint32
		if expiry != nil {
			initialExpiry = *expiry
		}
		casOut, err = db.dataStore.WriteUpdateWithXattrs(ctx, key, db.syncGlobalSyncMouRevSeqNoAndUserXattrKeys(), initialExpiry, existingDoc, opts, func(currentValue []byte, currentXattrs map[string][]byte, cas uint64) (updatedDoc sgbucket.UpdatedDoc, err error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if doc, err = db.unmarshalDocumentWithXattrs(ctx, docid, currentValue, currentXattrs, cas, DocUnmarshalAll); err != nil {
				return
			}

			prevCurrentRev = doc.GetRevTreeID()

			// Check whether Sync Data originated in body
			currentSyncXattr := currentXattrs[base.SyncXattrName]
			if currentSyncXattr == nil && doc.Sequence > 0 {
				doc.inlineSyncData = true
			}

			previousAttachments, err = getAttachmentIDsForLeafRevisions(ctx, db, doc, newRevID)
			if err != nil {
				skipObsoleteAttachmentsRemoval = true
				base.ErrorfCtx(ctx, "Error retrieving previous leaf attachments of doc: %s, Error: %v", base.UD(docid), err)
			}

			isNewDocCreation = currentValue == nil
			var revokedChannelsRequiringExpansion []string
			updatedDoc.Expiry, newRevID, storedDoc, oldBodyJSON, unusedSequences, changedAccessPrincipals, changedRoleAccessUsers, createNewRevIDSkipped, revokedChannelsRequiringExpansion, err = db.documentUpdateFunc(ctx, !isNewDocCreation, doc, allowImport, docSequence, unusedSequences, callback, expiry, docUpdateEvent)
			if err != nil {
				return
			}
			// If importing and the sync function has modified the expiry, allow sgbucket.MutateInOptions to modify the expiry
			if db.dataStore.IsSupported(sgbucket.BucketStoreFeaturePreserveExpiry) && updatedDoc.Expiry != nil {
				opts.PreserveExpiry = false
			}
			docSequence = doc.Sequence
			inConflict = doc.hasFlag(channels.Conflict)
			currentRevFromHistory, ok := doc.History[doc.GetRevTreeID()]
			if !ok {
				err = base.RedactErrorf("WriteUpdateWithXattr() not able to find revision (%v) in history of doc: %+v.  Cannot update doc.", doc.GetRevTreeID(), base.UD(doc))
				return
			}

			// update the mutate in options based on the above logic
			updatedDoc.Spec = doc.HLV.computeMacroExpansions()

			updatedDoc.Spec = appendRevocationMacroExpansions(updatedDoc.Spec, revokedChannelsRequiringExpansion)

			updatedDoc.IsTombstone = currentRevFromHistory.Deleted
			if doc.MetadataOnlyUpdate != nil {
				if doc.MetadataOnlyUpdate.HexCAS != "" {
					updatedDoc.Spec = append(updatedDoc.Spec, sgbucket.NewMacroExpansionSpec(XattrMouCasPath(), sgbucket.MacroCas))
				}
			} else {
				if currentXattrs[base.MouXattrName] != nil && !isNewDocCreation {
					updatedDoc.XattrsToDelete = append(updatedDoc.XattrsToDelete, base.MouXattrName)
				}
			}

			// Return the new raw document value for the bucket to store.
			doc.SetCrc32cUserXattrHash()

			var rawSyncXattr, rawMouXattr, rawVvXattr, rawGlobalSync, rawDocBody []byte
			rawDocBody, rawSyncXattr, rawVvXattr, rawMouXattr, rawGlobalSync, err = doc.MarshalWithXattrs()
			if err != nil {
				return updatedDoc, err
			}

			// If isImport is true, we don't generally want to update the document body, only the xattrs. One exception
			// being when a import is resurrecting a document then we need a body to write back
			if (!isImport && len(rawDocBody) > 0) || (isImport && doc.Deleted) {
				updatedDoc.Doc = rawDocBody
				docBytes = len(updatedDoc.Doc)
			}

			updatedDoc.Xattrs = map[string][]byte{base.SyncXattrName: rawSyncXattr, base.VvXattrName: rawVvXattr}
			if rawMouXattr != nil && db.useMou() {
				updatedDoc.Xattrs[base.MouXattrName] = rawMouXattr
			}
			if rawGlobalSync != nil {
				updatedDoc.Xattrs[base.GlobalXattrName] = rawGlobalSync
			} else {
				if currentXattrs[base.GlobalXattrName] != nil && !isNewDocCreation {
					updatedDoc.XattrsToDelete = append(updatedDoc.XattrsToDelete, base.GlobalXattrName)
				}
			}

			// Warn when sync data is larger than a configured threshold
			if db.unsupportedOptions() != nil && db.unsupportedOptions().WarningThresholds != nil {
				if xattrBytesThreshold := db.unsupportedOptions().WarningThresholds.XattrSize; xattrBytesThreshold != nil {
					xattrBytes = len(rawSyncXattr)
					if uint32(xattrBytes) >= *xattrBytesThreshold {
						db.dbStats().Database().WarnXattrSizeCount.Add(1)
						base.WarnfCtx(ctx, "Doc id: %v sync metadata size: %d bytes exceeds %d bytes for sync metadata warning threshold", base.UD(doc.ID), xattrBytes, *xattrBytesThreshold)
					}
				}
			}

			// Prior to saving doc, remove the revision in cache
			if createNewRevIDSkipped {
				db.revisionCache.RemoveRevOnly(ctx, doc.ID, doc.GetRevTreeID())
			}

			base.DebugfCtx(ctx, base.KeyCRUD, "Saving doc (seq: #%d, id: %v rev: %v)", doc.Sequence, base.UD(doc.ID), doc.GetRevTreeID())
			return updatedDoc, err
		})
		if err != nil {
			if err == base.ErrDocumentMigrated {
				base.DebugfCtx(ctx, base.KeyCRUD, "Migrated document %q to use xattr.", base.UD(key))
			} else {
				base.DebugfCtx(ctx, base.KeyCRUD, "Did not update document %q w/ xattr: %v", base.UD(key), err)
			}
		} else if doc != nil {
			// Update the in-memory CAS values to match macro-expanded values
			doc.Cas = casOut
			if doc.MetadataOnlyUpdate != nil && doc.MetadataOnlyUpdate.HexCAS == expandMacroCASValueString {
				doc.MetadataOnlyUpdate.HexCAS = base.CasToString(casOut)
			}
			// update the doc's HLV defined post macro expansion
			doc = db.postWriteUpdateHLV(ctx, doc, casOut)
		}
	}

	// If the WriteUpdate didn't succeed, check whether there are unused, allocated sequences that need to be accounted for
	if err != nil {
		// For timeout errors, the write may or may not have succeeded so we cannot release the sequence as unused
		if !base.IsTimeoutError(err) {
			if docSequence > 0 {
				if seqErr := db.sequences().releaseSequence(ctx, docSequence); seqErr != nil {
					base.WarnfCtx(ctx, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", docSequence, seqErr)
				}

			}
			for _, sequence := range unusedSequences {
				if seqErr := db.sequences().releaseSequence(ctx, sequence); seqErr != nil {
					base.WarnfCtx(ctx, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", sequence, seqErr)
				}
			}
		}
	}

	// ErrUpdateCancel is returned when the incoming revision is already known
	if err == base.ErrUpdateCancel {
		return nil, "", nil
	} else if err != nil {
		return nil, "", err
	}

	if !isImport {
		auditFields := base.AuditFields{
			base.AuditFieldDocID:      docid,
			base.AuditFieldDocVersion: newRevID,
		}
		if doc.IsDeleted() {
			base.Audit(ctx, base.AuditIDDocumentDelete, auditFields)
		} else {
			if isNewDocCreation {
				base.Audit(ctx, base.AuditIDDocumentCreate, auditFields)
			} else {
				base.Audit(ctx, base.AuditIDDocumentUpdate, auditFields)
			}
		}
	}

	db.collectionStats.NumDocWrites.Add(1)
	db.collectionStats.DocWritesBytes.Add(int64(docBytes))
	db.dbStats().Database().NumDocWrites.Add(1)
	db.dbStats().Database().DocWritesBytes.Add(int64(docBytes))
	db.dbStats().Database().DocWritesXattrBytes.Add(int64(xattrBytes))
	if inConflict {
		db.dbStats().Database().ConflictWriteCount.Add(1)
	}
	if doc.IsDeleted() {
		db.dbStats().Database().TombstoneCount.Add(1)
	}

	if doc.History[newRevID] != nil {
		// Store the new revision in the cache
		history, getHistoryErr := doc.History.getHistory(newRevID)
		if getHistoryErr != nil {
			return nil, "", getHistoryErr
		}

		// Lazily marshal bytes for storage in revcache
		storedDocBytes, err := storedDoc.BodyBytes(ctx)
		if err != nil {
			return nil, "", err
		}

		revChannels := doc.History[newRevID].Channels
		documentRevision := DocumentRevision{
			DocID:       docid,
			RevID:       newRevID,
			BodyBytes:   storedDocBytes,
			History:     encodeRevisions(ctx, docid, history),
			Channels:    revChannels,
			Attachments: doc.Attachments(),
			Expiry:      doc.Expiry,
			Deleted:     doc.History[newRevID].Deleted,
			hlvHistory:  doc.HLV.ToHistoryForHLV(),
			CV:          &Version{SourceID: doc.HLV.SourceID, Value: doc.HLV.Version},
		}

		if updateRevCache {
			if createNewRevIDSkipped {
				db.revisionCache.Upsert(ctx, documentRevision)
			} else {
				db.revisionCache.Put(ctx, documentRevision)
			}
		}

		if db.eventMgr().HasHandlerForEvent(DocumentChange) {
			webhookJSON, err := doc.BodyWithSpecialProperties(ctx)
			if err != nil {
				base.WarnfCtx(ctx, "Error marshalling doc with id %s and revid %s for webhook post: %v", base.UD(docid), base.UD(newRevID), err)
			} else {
				winningRevChange := prevCurrentRev != doc.GetRevTreeID()
				err = db.eventMgr().RaiseDocumentChangeEvent(ctx, webhookJSON, docid, oldBodyJSON, revChannels, winningRevChange)
				if err != nil {
					base.DebugfCtx(ctx, base.KeyCRUD, "Error raising document change event: %v", err)
				}
			}
		}
	} else {
		// Revision has been pruned away so won't be added to cache
		base.InfofCtx(ctx, base.KeyCRUD, "doc %q / %q, has been pruned, it has not been inserted into the revision cache", base.UD(docid), newRevID)
	}

	// Now that the document has successfully been stored, we can make other db changes:
	base.DebugfCtx(ctx, base.KeyCRUD, "Stored doc %q / %q as #%v", base.UD(docid), newRevID, doc.Sequence)

	leafAttachments := make(map[string][]string)
	if !skipObsoleteAttachmentsRemoval {
		leafAttachments, err = getAttachmentIDsForLeafRevisions(ctx, db, doc, newRevID)
		if err != nil {
			skipObsoleteAttachmentsRemoval = true
			base.ErrorfCtx(ctx, "Error retrieving current leaf attachments of doc: %s, Error: %v", base.UD(docid), err)
		}
	}

	if !skipObsoleteAttachmentsRemoval {
		var obsoleteAttachments []string
		for previousAttachmentID, previousAttachmentName := range previousAttachments {
			if _, found := leafAttachments[previousAttachmentID]; !found {
				err = db.dataStore.Delete(previousAttachmentID)
				if err != nil {
					base.ErrorfCtx(ctx, "Error deleting obsolete attachment %q of doc %q, Error: %v", previousAttachmentID, base.UD(doc.ID), err)
				} else {
					obsoleteAttachments = append(obsoleteAttachments, previousAttachmentID)
					if !isImport {
						for _, previousAttachmentName := range previousAttachmentName {
							_, exists := doc.Attachments()[previousAttachmentName]
							if !exists {
								base.Audit(ctx, base.AuditIDAttachmentDelete, base.AuditFields{
									base.AuditFieldDocID:        doc.ID,
									base.AuditFieldDocVersion:   newRevID,
									base.AuditFieldAttachmentID: previousAttachmentName,
								})
							}
						}
					}
				}
			}
		}
		if len(obsoleteAttachments) > 0 {
			base.DebugfCtx(ctx, base.KeyCRUD, "Deleted obsolete attachments (key: %v, doc: %q)", obsoleteAttachments, base.UD(doc.ID))
		}
	}

	// Remove any obsolete non-winning revision bodies
	doc.deleteRemovedRevisionBodies(ctx, db.dataStore)

	// Mark affected users/roles as needing to recompute their channel access:
	db.MarkPrincipalsChanged(ctx, docid, newRevID, changedAccessPrincipals, changedRoleAccessUsers, doc.Sequence)
	return doc, newRevID, nil
}

func (db *DatabaseCollectionWithUser) postWriteUpdateHLV(ctx context.Context, doc *Document, casOut uint64) *Document {
	if doc.HLV == nil {
		return doc
	}
	if doc.HLV.Version == expandMacroCASValueUint64 {
		doc.HLV.Version = casOut
	}
	if doc.HLV.CurrentVersionCAS == expandMacroCASValueUint64 {
		doc.HLV.CurrentVersionCAS = casOut
	}
	doc.SyncData.SetCV(doc.HLV)

	// backup new revision to the bucket now we have a doc assigned a CV (post macro expansion) for delta generation purposes
	// we don't need to store revision body backups without delta sync in 4.0, since all clients know how to use the sendReplacementRevs feature
	backupRev := db.deltaSyncEnabled() && db.deltaSyncRevMaxAgeSeconds() != 0
	if db.UseXattrs() && backupRev {
		var newBodyWithAtts = doc._rawBody
		if len(doc.Attachments()) > 0 {
			var err error
			newBodyWithAtts, err = base.InjectJSONProperties(doc._rawBody, base.KVPair{
				Key: BodyAttachments,
				Val: doc.Attachments(),
			})
			if err != nil {
				base.WarnfCtx(ctx, "Unable to marshal new revision body during backupRevisionJSON: doc=%q rev=%q cv=%q err=%v ", base.UD(doc.ID), doc.GetRevTreeID(), doc.HLV.GetCurrentVersionString(), err)
				return doc
			}
		}
		revHash := base.Crc32cHashString([]byte(doc.HLV.GetCurrentVersionString()))
		_ = db.setOldRevisionJSONBody(ctx, doc.ID, revHash, newBodyWithAtts, db.deltaSyncRevMaxAgeSeconds())
		// Optionally store a lookup document to find the CV-based revHash by legacy RevTree ID
		if db.deltaSyncStoreLegacyRevs() {
			_ = db.setOldRevisionJSONPtr(ctx, doc, db.deltaSyncRevMaxAgeSeconds())
		}
	}
	return doc
}

// getAttachmentIDsForLeafRevisions returns a map of attachment docids with values of attachment names.
func getAttachmentIDsForLeafRevisions(ctx context.Context, db *DatabaseCollectionWithUser, doc *Document, newRevID string) (map[string][]string, error) {
	leafAttachments := make(map[string][]string)

	currentAttachments, err := retrieveV2Attachments(doc.ID, doc.Attachments())
	if err != nil {
		return nil, err
	}

	for docid, names := range currentAttachments {
		leafAttachments[docid] = names
	}

	// Grab leaf revisions that have attachments and aren't the currently being added rev
	// Currently handled rev won't have information set properly on it yet so we handle this above
	// Can safely ignore the getInfo error as the only event this should happen in is if there is no entry for the given
	// rev, however, given we have just got that rev from GetLeavesFiltered we can be sure that rev exists in history
	documentLeafRevisions := doc.History.GetLeavesFiltered(func(revId string) bool {
		revInfo, _ := doc.History.getInfo(revId)
		return revInfo.HasAttachments && revId != newRevID
	})

	for _, leafRevision := range documentLeafRevisions {
		_, attachmentMeta, err := db.getRevision(ctx, doc, leafRevision)
		if err != nil {
			return nil, err
		}

		attachmentKeys, err := retrieveV2Attachments(doc.ID, attachmentMeta)
		if err != nil {
			return nil, err
		}

		for attachmentID, attachmentNames := range attachmentKeys {
			leafAttachments[attachmentID] = attachmentNames
		}

	}

	return leafAttachments, nil
}
func (db *DatabaseCollectionWithUser) checkDocChannelsAndGrantsLimits(ctx context.Context, docID string, channels base.Set, accessGrants channels.AccessMap, roleGrants channels.AccessMap) {
	if db.unsupportedOptions() == nil || db.unsupportedOptions().WarningThresholds == nil {
		return
	}

	// Warn when channel count is larger than a configured threshold
	if channelCountThreshold := db.unsupportedOptions().WarningThresholds.ChannelsPerDoc; channelCountThreshold != nil {
		channelCount := len(channels)
		if uint32(channelCount) >= *channelCountThreshold {
			db.dbStats().Database().WarnChannelsPerDocCount.Add(1)
			base.WarnfCtx(ctx, "Doc id: %v channel count: %d exceeds %d for channels per doc warning threshold", base.UD(docID), channelCount, *channelCountThreshold)
		}
	}

	// Warn when grants are larger than a configured threshold
	if grantThreshold := db.unsupportedOptions().WarningThresholds.GrantsPerDoc; grantThreshold != nil {
		grantCount := len(accessGrants) + len(roleGrants)
		if uint32(grantCount) >= *grantThreshold {
			db.dbStats().Database().WarnGrantsPerDocCount.Add(1)
			base.WarnfCtx(ctx, "Doc id: %v access and role grants count: %d exceeds %d for grants per doc warning threshold", base.UD(docID), grantCount, *grantThreshold)
		}
	}

	// Warn when channel names are larger than a configured threshold
	if channelNameSizeThreshold := db.unsupportedOptions().WarningThresholds.ChannelNameSize; channelNameSizeThreshold != nil {
		for c := range channels {
			if uint32(len(c)) > *channelNameSizeThreshold {
				db.dbStats().Database().WarnChannelNameSizeCount.Add(1)
				base.WarnfCtx(ctx, "Doc: %q channel %q exceeds %d characters for channel name size warning threshold", base.UD(docID), base.UD(c), *channelNameSizeThreshold)
			}
		}
	}
}

func (db *DatabaseCollectionWithUser) MarkPrincipalsChanged(ctx context.Context, docid string, newRevID string, changedPrincipals, changedRoleUsers []string, invalSeq uint64) {

	reloadActiveUser := false

	// Mark affected users/roles as needing to recompute their channel access:
	if len(changedPrincipals) > 0 {
		base.InfofCtx(ctx, base.KeyAccess, "Rev %q / %q invalidates channels of %s", base.UD(docid), newRevID, changedPrincipals)
		for _, changedAccessPrincipalName := range changedPrincipals {
			db.invalUserOrRoleChannels(ctx, changedAccessPrincipalName, invalSeq)
			// Check whether the active user needs to be recalculated.  Skip check if reload has already been identified
			// as required for a previous changedPrincipal
			if db.user != nil && reloadActiveUser == false {
				// If role changed, check if active user has been granted the role
				changedPrincipalName, isRole := channels.AccessNameToPrincipalName(changedAccessPrincipalName)
				if isRole {
					for roleName := range db.user.RoleNames() {
						if roleName == changedPrincipalName {
							base.DebugfCtx(ctx, base.KeyAccess, "Active user belongs to role %q with modified channel access - user %q will be reloaded.", base.UD(roleName), base.UD(db.user.Name()))
							reloadActiveUser = true
							break
						}
					}
				} else if db.user.Name() == changedPrincipalName {
					// User matches
					base.DebugfCtx(ctx, base.KeyAccess, "Channel set for active user has been modified - user %q will be reloaded.", base.UD(db.user.Name()))
					reloadActiveUser = true
				}

			}
		}
	}

	if len(changedRoleUsers) > 0 {
		base.InfofCtx(ctx, base.KeyAccess, "Rev %q / %q invalidates roles of %s", base.UD(docid), newRevID, base.UD(changedRoleUsers))
		for _, name := range changedRoleUsers {
			db.dbCtx.invalUserRoles(ctx, name, invalSeq)
			// If this is the current in memory db.user, reload to generate updated roles
			if db.user != nil && db.user.Name() == name {
				base.DebugfCtx(ctx, base.KeyAccess, "Role set for active user has been modified - user %q will be reloaded.", base.UD(db.user.Name()))
				reloadActiveUser = true

			}
		}
	}

	if reloadActiveUser {
		user, err := db.Authenticator(ctx).GetUser(db.user.Name())
		if err != nil {
			base.WarnfCtx(ctx, "Error reloading active db.user[%s], security information will not be recalculated until next authentication --> %+v", base.UD(db.user.Name()), err)
		} else {
			db.user = user
		}
	}

}

// Creates a new document, assigning it a random doc ID.
func (db *DatabaseCollectionWithUser) Post(ctx context.Context, body Body) (docid string, rev string, doc *Document, err error) {
	// This error isn't very accurate, you just _cannot_ use POST to update an existing document - even if it does exist. We don't even bother checking for existence.
	if body[BodyRev] != nil || body[BodyCV] != nil {
		return "", "", nil, base.HTTPErrorf(http.StatusNotFound, "No previous revision to replace")
	}

	// If there's an incoming _id property, use that as the doc ID.
	docid, idFound := body[BodyId].(string)
	if !idFound {
		docid, err = base.GenerateRandomID()
		if err != nil {
			return "", "", nil, err
		}
	}

	rev, doc, err = db.Put(ctx, docid, body)
	if err != nil {
		docid = ""
	}
	return docid, rev, doc, err
}

// Deletes a document, by adding a new revision whose _deleted property is true.
func (db *DatabaseCollectionWithUser) DeleteDoc(ctx context.Context, docid string, docVersion DocVersion) (string, *Document, error) {
	versionKey, versionStr := docVersion.Body1xKVPair()
	body := Body{BodyDeleted: true, versionKey: versionStr}
	newRevID, doc, err := db.Put(ctx, docid, body)
	return newRevID, doc, err
}

// Purges a document from the bucket (no tombstone)
func (db *DatabaseCollectionWithUser) Purge(ctx context.Context, key string, needsAudit bool) error {
	doc, rawBucketDoc, err := db.GetDocumentWithRaw(ctx, key, DocUnmarshalAll)
	if err != nil {
		return err
	}

	attachments, err := getAttachmentIDsForLeafRevisions(ctx, db, doc, "")
	if err != nil {
		return err
	}

	for attachmentID, attachmentNames := range attachments {
		err = db.dataStore.Delete(attachmentID)
		if err != nil {
			base.WarnfCtx(ctx, "Unable to delete attachment %q. Error: %v", attachmentID, err)
		}
		for _, attachmentName := range attachmentNames {
			base.Audit(ctx, base.AuditIDAttachmentDelete, base.AuditFields{
				base.AuditFieldDocID:        doc.ID,
				base.AuditFieldAttachmentID: attachmentName,
			})
		}

	}

	if db.UseXattrs() {
		// Clean up _sync and _globalSync (if present). Leave _vv and _mou since they are also shared by XDCR/Eventing.
		xattrsToDelete := []string{base.SyncXattrName, base.GlobalXattrName}
		// TODO: CBG-4796 - we currently need to determine a list of present xattrs before we delete to avoid differences
		// between Rosmar and Couchbase Server implementations of DeleteWithXattrs and GetWithXattrs.
		var presentXattrsToDelete []string
		if rawBucketDoc != nil && rawBucketDoc.Xattrs != nil {
			presentXattrsToDelete = base.KeysPresent(rawBucketDoc.Xattrs, xattrsToDelete)
		}
		if err := db.dataStore.DeleteWithXattrs(ctx, key, presentXattrsToDelete); err != nil {
			return err
		}
	} else {
		err := db.dataStore.Delete(key)
		if err != nil {
			return err
		}
	}
	if needsAudit {
		base.Audit(ctx, base.AuditIDDocumentDelete, base.AuditFields{
			base.AuditFieldDocID:  key,
			base.AuditFieldPurged: true,
		})
	}
	return nil
}

// ////// CHANNELS:

// Calls the JS sync function to assign the doc to channels, grant users
// access to channels, and reject invalid documents.
func (col *DatabaseCollectionWithUser) getChannelsAndAccess(ctx context.Context, doc *Document, body Body, metaMap map[string]interface{}, revID string) (
	result base.Set,
	access channels.AccessMap,
	roles channels.AccessMap,
	expiry *uint32,
	oldJson string,
	err error) {
	base.DebugfCtx(ctx, base.KeyCRUD, "Invoking sync on doc %q rev %s", base.UD(doc.ID), body[BodyRev])

	// Low-level protection against writes for read-only guest.  Handles write pathways that don't fail-fast
	if col.user != nil && col.user.Name() == "" && col.isGuestReadOnly() {
		return result, access, roles, expiry, oldJson, base.HTTPErrorf(403, auth.GuestUserReadOnly)
	}

	// Get the parent revision, to pass to the sync function:
	var oldJsonBytes []byte
	if oldJsonBytes, err = col.getAncestorJSON(ctx, doc, revID); err != nil {
		return
	}
	oldJson = string(oldJsonBytes)

	if col.ChannelMapper != nil {
		// Call the ChannelMapper:
		col.dbStats().Database().SyncFunctionCount.Add(1)
		col.collectionStats.SyncFunctionCount.Add(1)

		var output *channels.ChannelMapperOutput

		startTime := time.Now()
		output, err = col.ChannelMapper.MapToChannelsAndAccess(ctx, body, oldJson, metaMap,
			MakeUserCtx(col.user, col.ScopeName, col.Name))
		syncFunctionTimeNano := time.Since(startTime).Nanoseconds()

		col.dbStats().Database().SyncFunctionTime.Add(syncFunctionTimeNano)
		col.collectionStats.SyncFunctionTime.Add(syncFunctionTimeNano)

		if err == nil {
			result = output.Channels
			access = output.Access
			roles = output.Roles
			expiry = output.Expiry
			err = output.Rejection
			if err != nil {
				base.InfofCtx(ctx, base.KeyAll, "Sync fn rejected doc %q / %q --> %s", base.UD(doc.ID), base.UD(doc.NewestRev), err)
				base.DebugfCtx(ctx, base.KeyAll, "    rejected doc %q / %q : new=%+v  old=%s", base.UD(doc.ID), base.UD(doc.NewestRev), base.UD(body), base.UD(oldJson))
				col.dbStats().Security().NumDocsRejected.Add(1)
				col.collectionStats.SyncFunctionRejectCount.Add(1)
				if isAccessError(err) {
					col.dbStats().Security().NumAccessErrors.Add(1)
					col.collectionStats.SyncFunctionRejectAccessCount.Add(1)
				}
			} else if !validateAccessMap(ctx, access) || !validateRoleAccessMap(ctx, roles) {
				err = base.HTTPErrorf(500, "Error in JS sync function")
			}

		} else {
			base.WarnfCtx(ctx, "Sync fn exception: %+v; doc %q / %q", err, base.UD(doc.ID), base.MD(doc.GetRevTreeID()))
			if errors.Is(err, sgbucket.ErrJSTimeout) {
				err = base.HTTPErrorf(500, "JS sync function timed out")
			} else {
				err = base.HTTPErrorf(500, "Exception in JS sync function")
				col.collectionStats.SyncFunctionExceptionCount.Add(1)
				col.dbStats().Database().SyncFunctionExceptionCount.Add(1)
			}
		}

	} else {
		if base.IsDefaultCollection(col.ScopeName, col.Name) {
			// No ChannelMapper so by default use the "channels" property:
			value := body["channels"]
			if value != nil {
				array, nonStrings := base.ValueToStringArray(value)
				if nonStrings != nil {
					base.WarnfCtx(ctx, "Channel names must be string values only. Ignoring non-string channels: %s", base.UD(nonStrings))
				}
				result, err = channels.SetFromArray(array, channels.KeepStar)
			}
		} else {
			result = base.SetOf(col.Name)
		}
	}
	return result, access, roles, expiry, oldJson, err
}

// Creates a userCtx object to be passed to the sync function
func MakeUserCtx(user auth.User, scopeName string, collectionName string) map[string]interface{} {
	if user == nil {
		return nil
	}
	return map[string]interface{}{
		"name":     user.Name(),
		"roles":    user.RoleNames(),
		"channels": user.InheritedCollectionChannels(scopeName, collectionName).AllKeys(),
	}
}

// Are the principal and role names in an AccessMap all valid?
func validateAccessMap(ctx context.Context, access channels.AccessMap) bool {
	for name := range access {
		principalName, _ := channels.AccessNameToPrincipalName(name)
		if !auth.IsValidPrincipalName(principalName) {
			base.WarnfCtx(ctx, "Invalid principal name %q in access() or role() call", base.UD(principalName))
			return false
		}
	}
	return true
}

func validateRoleAccessMap(ctx context.Context, roleAccess channels.AccessMap) bool {
	if !validateAccessMap(ctx, roleAccess) {
		return false
	}
	for _, roles := range roleAccess {
		for rolename := range roles {
			if !auth.IsValidPrincipalName(rolename) {
				base.WarnfCtx(ctx, "Invalid role name %q in role() call", base.UD(rolename))
				return false
			}
		}
	}
	return true
}

func isAccessError(err error) bool {
	return base.ContainsString(base.SyncFnAccessErrors, err.Error())
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions for the default collection
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeChannelsForPrincipal(ctx context.Context, princ auth.Principal, scope string, collection string) (channels.TimedSet, error) {
	key := princ.Name()
	if _, ok := princ.(auth.User); !ok {
		key = channels.RoleAccessPrefix + key // Roles are identified in access view by a "role:" prefix
	}

	dbCollection, err := context.GetDatabaseCollection(scope, collection)
	if err != nil {
		return nil, err
	}

	results, err := dbCollection.QueryAccess(ctx, key)
	if err != nil {
		base.WarnfCtx(ctx, "QueryAccess returned error: %v", err)
		return nil, err
	}

	var accessRow QueryAccessRow
	channelSet := channels.TimedSet{}
	for results.Next(ctx, &accessRow) {
		channelSet.Add(accessRow.Value)
	}

	closeErr := results.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	return channelSet, nil
}

// Recomputes the set of channels a User/Role has been granted access to by sync() function for all collections.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeRolesForUser(ctx context.Context, user auth.User) (channels.TimedSet, error) {

	roles := channels.TimedSet{}

	for _, collection := range context.CollectionByID {
		collectionRoles, err := collection.ComputeRolesForUser(ctx, user)
		if err != nil {
			return nil, err
		}
		roles.Add(collectionRoles)
	}
	return roles, nil
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions for a single collection.
func (c *DatabaseCollection) ComputeRolesForUser(ctx context.Context, user auth.User) (channels.TimedSet, error) {
	results, err := c.QueryRoleAccess(ctx, user.Name())
	if err != nil {
		return nil, err
	}

	// Merge the TimedSets from the view result:
	roleChannelSet := channels.TimedSet{}
	var roleAccessRow QueryAccessRow
	for results.Next(ctx, &roleAccessRow) {
		roleChannelSet.Add(roleAccessRow.Value)
	}
	closeErr := results.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	return roleChannelSet, nil
}

// Checks whether a document has a mobile xattr.  Used when running in non-xattr mode to support no downtime upgrade.
func (c *DatabaseCollection) checkForUpgrade(ctx context.Context, key string, unmarshalLevel DocumentUnmarshalLevel) (*Document, *sgbucket.BucketDocument) {
	// If we are using xattrs or Couchbase Server doesn't support them, an upgrade isn't going to be in progress
	if c.UseXattrs() || !c.dataStore.IsSupported(sgbucket.BucketStoreFeatureXattrs) {
		return nil, nil
	}

	doc, rawDocument, err := c.GetDocWithXattrs(ctx, key, unmarshalLevel)
	if err != nil || doc == nil || !doc.HasValidSyncData() {
		return nil, nil
	}
	return doc, rawDocument
}

func (db *DatabaseCollectionWithUser) CheckChangeVersion(ctx context.Context, docid, rev string) (missing, possible []string) {
	if strings.HasPrefix(docid, "_design/") && db.user != nil {
		return // Users can't upload design docs, so ignore them
	}

	syncData, hlv, err := db.GetDocSyncDataNoImport(ctx, docid, DocUnmarshalSync)
	if err != nil {
		if !base.IsDocNotFoundError(err) && !base.IsXattrNotFoundError(err) {
			base.WarnfCtx(ctx, "Error fetching doc %s during changes handling: %v", base.UD(docid), err)
		}
		missing = append(missing, rev)
		return
	}
	if hlv == nil {
		// no hlv on local doc, mark as missing but send current rev as known rev (will be handled as legacy
		// rev document on changes response handler)
		base.TracefCtx(ctx, base.KeyChanges, "Doc %s has no HLV, marking change version %s as missing", base.UD(docid), base.UD(rev))
		missing = append(missing, rev)
		possible = append(possible, syncData.GetRevTreeID())
		return
	}
	// parse in coming version, if it's not known to local doc hlv then it is marked as missing, if it is and is a newer version
	// then it is also marked as missing
	cvValue, err := ParseVersion(rev)
	if err != nil {
		base.WarnfCtx(ctx, "error parse change version for doc %s: %v", base.UD(docid), err)
		missing = append(missing, rev)
		return
	}
	// CBG-4792: enhance here for conflict check - return conflict rev similar to propose changes here link ticket
	if hlv.DominatesSource(cvValue) {
		// incoming version is dominated by local doc hlv, so it is not missing
		return
	}

	// return the local current rev as known rev, this will mean if you have rev 1,2,3 and remote has rev 1,2,3,4,5 then
	// remote should only send rev 4,5 in rev tree property on the subsequent rev message for this document, we also need to
	// send cv as first element for delta sync purposes
	possible = append(possible, hlv.GetCurrentVersionString())
	possible = append(possible, syncData.GetRevTreeID())

	missing = append(missing, rev)
	return
}

// ////// REVS_DIFF:

// Given a document ID and a set of revision IDs, looks up which ones are not known. Returns an
// array of the unknown revisions, and an array of known revisions that might be recent ancestors.
func (db *DatabaseCollectionWithUser) RevDiff(ctx context.Context, docid string, revids []string) (missing, possible []string) {
	if strings.HasPrefix(docid, "_design/") && db.user != nil {
		return // Users can't upload design docs, so ignore them
	}

	syncData, _, err := db.GetDocSyncDataNoImport(ctx, docid, DocUnmarshalHistory)
	if err != nil {
		if !base.IsDocNotFoundError(err) && !base.IsXattrNotFoundError(err) {
			base.WarnfCtx(ctx, "RevDiff(%q) --> %T %v", base.UD(docid), err, err)
		}
		missing = revids
		return
	}
	// Check each revid to see if it's in the doc's rev tree:
	revidsSet := base.SetFromArray(revids)
	possibleSet := make(map[string]bool)
	for _, revid := range revids {
		if !syncData.History.contains(revid) {
			missing = append(missing, revid)
			// Look at the doc's leaves for a known possible ancestor:
			if gen, _ := ParseRevID(ctx, revid); gen > 1 {
				syncData.History.forEachLeaf(func(possible *RevInfo) {
					if !revidsSet.Contains(possible.ID) {
						possibleGen, _ := ParseRevID(ctx, possible.ID)
						if possibleGen < gen && possibleGen >= gen-100 {
							possibleSet[possible.ID] = true
						} else if possibleGen == gen && possible.Parent != "" {
							possibleSet[possible.Parent] = true // since parent is < gen
						}
					}
				})
			}
		}
	}

	// Convert possibleSet to an array (possible)
	if len(possibleSet) > 0 {
		possible = make([]string, 0, len(possibleSet))
		for revid, _ := range possibleSet {
			possible = append(possible, revid)
		}
	}
	return
}

// Status code returned by CheckProposedRev
type ProposedRevStatus int

const (
	ProposedRev_OK_IsNew ProposedRevStatus = 201 // Rev can be added, doc does not exist locally
	ProposedRev_OK       ProposedRevStatus = 0   // Rev can be added without conflict
	ProposedRev_Exists   ProposedRevStatus = 304 // Rev already exists locally
	ProposedRev_Conflict ProposedRevStatus = 409 // Rev would cause conflict
	ProposedRev_Error    ProposedRevStatus = 500 // Error occurred reading local doc
)

// CheckProposedRev checks withether revid can be pushed without conflict.
func (db *DatabaseCollectionWithUser) CheckProposedRev(ctx context.Context, docid string, revid string, parentRevID string) (status ProposedRevStatus, currentRev string) {
	if strings.HasPrefix(docid, "_design/") && db.user != nil {
		return ProposedRev_OK, "" // Users can't upload design docs, so ignore them
	}

	level := DocUnmarshalRev
	if parentRevID == "" {
		level = DocUnmarshalHistory // doc.History only needed in this case (see below)
	}
	syncData, _, err := db.GetDocSyncDataNoImport(ctx, docid, level)
	if err != nil {
		if !base.IsDocNotFoundError(err) && !base.IsXattrNotFoundError(err) {
			base.WarnfCtx(ctx, "CheckProposedRev(%q) --> %T %v", base.UD(docid), err, err)
			return ProposedRev_Error, ""
		}
		// Doc doesn't exist locally; adding it is OK (even if it has a history)
		return ProposedRev_OK_IsNew, ""
	} else if syncData.GetRevTreeID() == revid {
		// Proposed rev already exists here:
		return ProposedRev_Exists, ""
	} else if syncData.GetRevTreeID() == parentRevID {
		// Proposed rev's parent is my current revision; OK to add:
		return ProposedRev_OK, ""
	} else if parentRevID == "" && syncData.History[syncData.GetRevTreeID()].Deleted {
		// Proposed rev has no parent and doc is currently deleted; OK to add:
		return ProposedRev_OK, ""
	} else {
		// Parent revision mismatch, so this is a conflict:
		return ProposedRev_Conflict, syncData.GetRevTreeID()
	}
}

// CheckProposedVersion - given DocID and a version in string form, check whether it can be added without conflict.
// proposedVersionStr is the string representation of the proposed version's CV.
// previousRev is the string representation of the CV of the last known parent of the proposed version.
// proposedHLVString is the string representation of the proposed version's full HLV.
func (db *DatabaseCollectionWithUser) CheckProposedVersion(ctx context.Context, docid, proposedVersionStr string, previousRev string, proposedHLVString string) (status ProposedRevStatus, currentVersion string) {

	proposedVersion, err := ParseVersion(proposedVersionStr)
	if err != nil {
		base.WarnfCtx(ctx, "Couldn't parse proposed version for doc %q / %q: %v", base.UD(docid), proposedVersionStr, err)
		return ProposedRev_Error, ""
	}

	// previousRev may be revTreeID or version
	var previousVersion Version
	previousRevFormat := "version"
	// TODO: CBG-4812 Use base.IsRevTreeID
	if !strings.Contains(previousRev, "@") {
		previousRevFormat = "revTreeID"
	}
	if previousRev != "" && previousRevFormat == "version" {
		var err error
		previousVersion, err = ParseVersion(previousRev)
		if err != nil {
			base.WarnfCtx(ctx, "Couldn't parse previous version for doc %q / %q: %v", base.UD(docid), previousRev, err)
			return ProposedRev_Error, ""
		}
	}

	localDocCV := Version{}
	syncData, hlv, err := db.GetDocSyncDataNoImport(ctx, docid, DocUnmarshalNoHistory)
	if hlv != nil {
		localDocCV.SourceID, localDocCV.Value = hlv.GetCurrentVersion()
	}
	if err != nil {
		if !base.IsDocNotFoundError(err) && !errors.Is(err, base.ErrXattrNotFound) {
			base.WarnfCtx(ctx, "CheckProposedRev(%q) --> %T %v", base.UD(docid), err, err)
			return ProposedRev_Error, ""
		}
		// New document not found on server
		return ProposedRev_OK_IsNew, ""
	} else if previousRevFormat == "revTreeID" && syncData.GetRevTreeID() == previousRev {
		// Non-conflicting update, client's previous legacy revTreeID is server's currentRev
		return ProposedRev_OK, ""
	} else if previousRevFormat == "version" && localDocCV == previousVersion {
		// Non-conflicting update, client's previous version is server's CV
		return ProposedRev_OK, ""
	} else if hlv.DominatesSource(proposedVersion) {
		// SGW already has this version
		return ProposedRev_Exists, ""
	} else if localDocCV.SourceID == proposedVersion.SourceID && localDocCV.Value < proposedVersion.Value {
		// previousVersion didn't match, but proposed version and server CV have matching source, and proposed version is newer
		return ProposedRev_OK, ""
	} else {
		// Temporary (CBG-4466): check the full HLV that's being sent by CBL with proposeChanges messages.
		// If the current server cv is dominated by the incoming HLV (i.e. the incoming HLV has an entry for the same source
		// with a version that's greater than or equal to the server's cv), then we can accept the proposed version.
		proposedHLV, _, err := extractHLVFromBlipString(proposedHLVString)
		if err != nil {
			base.InfofCtx(ctx, base.KeyCRUD, "CheckProposedVersion for doc %s unable to extract proposedHLV from rev message, will be treated as conflict: %v", base.UD(docid), err)
		} else if proposedHLV.DominatesSource(localDocCV) {
			base.DebugfCtx(ctx, base.KeyCRUD, "CheckProposedVersion returning OK for doc %s because incoming HLV dominates cv", base.UD(docid))
			return ProposedRev_OK, ""
		}

		// In conflict cases, return the current cv.  This may be a false positive conflict if the client has replicated
		// the server cv via a different peer and so is not sending previousRev.  The client is responsible for performing this check based on the
		// returned localDocCV
		return ProposedRev_Conflict, localDocCV.String()
	}
}

// alignRevTreeHistory will take incoming rev tree list and add any newer revisions to the local document. If there is
// history between the incoming rev tree and local rev tree differs then the local docs rev tree will be overwritten to
// the incoming rev tree.
func (doc *Document) alignRevTreeHistory(ctx context.Context, newDoc *Document, revTreeHistory []string) error {
	currentRevIndex := len(revTreeHistory)
	parent := ""
	for i, revid := range revTreeHistory {
		if doc.History.contains(revid) {
			currentRevIndex = i
			parent = revid
			break
		}
	}

	if parent != doc.GetRevTreeID() {
		base.DebugfCtx(ctx, base.KeyCRUD, "incoming rev tree history has different history than local doc %s, overwriting the revision history to match the incoming history", base.UD(doc.ID))
		// clean local history and make way for incoming history to replace it
		doc.History = make(RevTree)
		// reset current rev index and parent given we are building a new rev tree now
		currentRevIndex = len(revTreeHistory)
		parent = ""
	}

	newRev, err := doc.addNewerRevisionsToRevTreeHistory(newDoc, currentRevIndex, parent, revTreeHistory)
	if err != nil {
		return err
	}
	doc.SetRevTreeID(newRev)
	return nil
}

// addNewerRevisionsToRevTreeHistory will add any newer rev tree id's to the local document history
func (doc *Document) addNewerRevisionsToRevTreeHistory(newDoc *Document, currentRevIndex int, parent string, docHistory []string) (string, error) {
	// currentRevIndex here is the index of the incoming rev tree list to start from.
	for i := currentRevIndex - 1; i >= 0; i-- {
		err := doc.History.addRevision(newDoc.ID,
			RevInfo{
				ID:      docHistory[i],
				Parent:  parent, // set the parent of this revision to the element of docHistory from the last iteration
				Deleted: i == 0 && newDoc.Deleted})

		if err != nil {
			return "", err
		}
		parent = docHistory[i]
	}
	// return last element added from docHistory, this will be the doc's new current rev for writes that are aligning rev tree
	return parent, nil
}

const (
	xattrMacroCas               = "cas"          // SyncData.Cas
	xattrMacroValueCrc32c       = "value_crc32c" // SyncData.Crc32c
	xattrMacroCurrentRevVersion = "rev.ver"      // SyncData.RevAndVersion.CurrentVersion
	versionVectorVrsMacro       = "ver"          // PersistedHybridLogicalVector.Version
	versionVectorCVCASMacro     = "cvCas"        // PersistedHybridLogicalVector.CurrentVersionCAS

	expandMacroCASValueUint64 = math.MaxUint64 // static value that indicates that a CAS macro expansion should be applied to a property
	expandMacroCASValueString = "expand"
)

func macroExpandSpec(xattrName string) []sgbucket.MacroExpansionSpec {
	macroExpansion := []sgbucket.MacroExpansionSpec{
		sgbucket.NewMacroExpansionSpec(xattrCasPath(xattrName), sgbucket.MacroCas),
		sgbucket.NewMacroExpansionSpec(xattrCrc32cPath(xattrName), sgbucket.MacroCrc32c),
	}

	return macroExpansion
}

func xattrCasPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroCas
}

func xattrCrc32cPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroValueCrc32c
}

// XattrMouCasPath returns the xattr path for the CAS value for expansion, _mou.cas
func XattrMouCasPath() string {
	return base.MouXattrName + "." + xattrMacroCas
}

func xattrCurrentRevVersionPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroCurrentRevVersion
}

func xattrCurrentVersionPath(xattrKey string) string {
	return xattrKey + "." + versionVectorVrsMacro
}

func xattrCurrentVersionCASPath(xattrKey string) string {
	return xattrKey + "." + versionVectorCVCASMacro
}

func xattrRevokedChannelVersionPath(xattrKey string, channelName string) string {
	return xattrKey + ".channels." + channelName + "." + xattrMacroCurrentRevVersion
}

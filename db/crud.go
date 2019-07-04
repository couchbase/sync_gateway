//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/go-couchbase"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/pkg/errors"
)

const (
	kMaxRecentSequences = 20 // Maximum number of sequences stored in RecentSequences before pruning is triggered
)

//////// READING DOCUMENTS:

func realDocID(docid string) string {
	if len(docid) > 250 {
		return "" // Invalid doc IDs
	}
	if strings.HasPrefix(docid, "_") {
		return "" // Disallow "_" prefix, which is for special docs
	}
	return docid
}

// Lowest-level method that reads a document from the bucket
func (db *DatabaseContext) GetDocument(docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	key := realDocID(docid)
	if key == "" {
		return nil, base.HTTPErrorf(400, "Invalid doc ID")
	}
	if db.UseXattrs() {
		var rawBucketDoc *sgbucket.BucketDocument
		doc, rawBucketDoc, err = db.GetDocWithXattr(key, unmarshalLevel)
		if err != nil {
			return nil, err
		}

		isSgWrite, crc32Match := doc.IsSGWrite(rawBucketDoc.Body)
		if crc32Match {
			db.DbStats.StatsDatabase().Add(base.StatKeyCrc32cMatchCount, 1)
		}

		// If existing doc wasn't an SG Write, import the doc.
		if !isSgWrite {
			var importErr error
			doc, importErr = db.OnDemandImportForGet(docid, rawBucketDoc.Body, rawBucketDoc.Xattr, rawBucketDoc.Cas)
			if importErr != nil {
				return nil, importErr
			}
		}
		if !doc.HasValidSyncData(db.writeSequences()) {
			return nil, base.HTTPErrorf(404, "Not imported")
		}
	} else {
		rawDoc, _, getErr := db.Bucket.GetRaw(key)
		if getErr != nil {
			return nil, getErr
		}

		doc, err = unmarshalDocument(key, rawDoc)
		if err != nil {
			return nil, err
		}

		if !doc.HasValidSyncData(db.writeSequences()) {
			// Check whether doc has been upgraded to use xattrs
			upgradeDoc, _ := db.checkForUpgrade(docid, unmarshalLevel)
			if upgradeDoc == nil {
				return nil, base.HTTPErrorf(404, "Not imported")
			}
			doc = upgradeDoc
		}
	}

	return doc, nil
}

func (db *DatabaseContext) GetDocWithXattr(key string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, rawBucketDoc *sgbucket.BucketDocument, err error) {
	rawBucketDoc = &sgbucket.BucketDocument{}
	var getErr error
	rawBucketDoc.Cas, getErr = db.Bucket.GetWithXattr(key, base.SyncXattrName, &rawBucketDoc.Body, &rawBucketDoc.Xattr)
	if getErr != nil {
		return nil, nil, getErr
	}

	var unmarshalErr error
	doc, unmarshalErr = unmarshalDocumentWithXattr(key, rawBucketDoc.Body, rawBucketDoc.Xattr, rawBucketDoc.Cas, unmarshalLevel)
	if unmarshalErr != nil {
		return nil, nil, unmarshalErr
	}
	return doc, rawBucketDoc, nil
}

// This gets *just* the Sync Metadata (_sync field) rather than the entire doc, for efficiency reasons.
func (db *DatabaseContext) GetDocSyncData(docid string) (SyncData, error) {

	emptySyncData := SyncData{}
	key := realDocID(docid)
	if key == "" {
		return emptySyncData, base.HTTPErrorf(400, "Invalid doc ID")
	}

	if db.UseXattrs() {
		// Retrieve doc and xattr from bucket, unmarshal only xattr.
		// Triggers on-demand import when document xattr doesn't match cas.
		var rawDoc, rawXattr []byte
		cas, getErr := db.Bucket.GetWithXattr(key, base.SyncXattrName, &rawDoc, &rawXattr)
		if getErr != nil {
			return emptySyncData, getErr
		}

		// Unmarshal xattr only
		doc, unmarshalErr := unmarshalDocumentWithXattr(docid, nil, rawXattr, cas, DocUnmarshalSync)
		if unmarshalErr != nil {
			return emptySyncData, unmarshalErr
		}

		isSgWrite, crc32Match := doc.IsSGWrite(rawDoc)
		if crc32Match {
			db.DbStats.StatsDatabase().Add(base.StatKeyCrc32cMatchCount, 1)
		}

		// If existing doc wasn't an SG Write, import the doc.
		if !isSgWrite {
			var importErr error

			doc, importErr = db.OnDemandImportForGet(docid, rawDoc, rawXattr, cas)
			if importErr != nil {
				return emptySyncData, importErr
			}
		}

		return doc.SyncData, nil

	} else {
		// Non-xattr.  Retrieve doc from bucket, unmarshal metadata only.
		rawDocBytes, _, err := db.Bucket.GetRaw(key)
		if err != nil {
			return emptySyncData, err
		}

		docRoot := documentRoot{
			SyncData: &SyncData{History: make(RevTree)},
		}
		if err := json.Unmarshal(rawDocBytes, &docRoot); err != nil {
			return emptySyncData, err
		}

		return *docRoot.SyncData, nil
	}

}

// OnDemandImportForGet.  Attempts to import the doc based on the provided id, contents and cas.  ImportDocRaw does cas retry handling
// if the document gets updated after the initial retrieval attempt that triggered this.
func (db *DatabaseContext) OnDemandImportForGet(docid string, rawDoc []byte, rawXattr []byte, cas uint64) (docOut *Document, err error) {
	isDelete := rawDoc == nil
	importDb := Database{DatabaseContext: db, user: nil}
	var importErr error

	docOut, importErr = importDb.ImportDocRaw(docid, rawDoc, rawXattr, isDelete, cas, nil, ImportOnDemand)
	if importErr == base.ErrImportCancelledFilter {
		// If the import was cancelled due to filter, treat as not found
		return nil, base.HTTPErrorf(404, "Not imported")
	} else if importErr != nil {
		return nil, importErr
	}
	return docOut, nil
}

// Returns the body of the current revision of a document
func (db *Database) Get(docid string) (Body, error) {
	return db.GetRevWithHistory(docid, "", 0, nil, nil, false)
}

// Get Rev with all-or-none history based on specified 'history' flag
func (db *Database) GetRev(docid, revid string, history bool, attachmentsSince []string) (Body, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}
	return db.getRev(docid, revid, maxHistory, nil, attachmentsSince, false, BodyShallowCopy)
}

// Retrieves rev with the specified body copy policy, to support efficient retrieval of deep/shallow/non-copied bodies.
func (db *Database) GetRevCopy(docid, revid string, history bool, attachmentsSince []string, copyType BodyCopyType) (Body, error) {

	// Requesting history and/or attachments is incompatible with BodyNoCopy, as getRev will always mutate the body when these
	// are specified.
	if copyType == BodyNoCopy && (history || len(attachmentsSince) > 0) {
		return nil, fmt.Errorf("GetRevCopy called with incompatible properties for key:%s rev:%s", base.UD(docid), revid)
	}

	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}
	return db.getRev(docid, revid, maxHistory, nil, attachmentsSince, false, copyType)

}

// Retrieves rev with request history specified as collection of revids (historyFrom)
func (db *Database) GetRevWithHistory(docid, revid string, maxHistory int, historyFrom []string, attachmentsSince []string, showExp bool) (Body, error) {
	return db.getRev(docid, revid, maxHistory, historyFrom, attachmentsSince, showExp, BodyShallowCopy)
}

// Underlying revision retrieval used by GetRev, GetRevWithHistory, GetRevCopy.
// Returns the body of a revision of a document. Uses the revision cache.
// * revid may be "", meaning the current revision.
// * maxHistory is >0 if the caller wants a revision history; it's the max length of the history.
// * historyFrom is an optional list of revIDs the client already has. If any of these are found
//   in the revision's history, it will be trimmed after that revID.
// * attachmentsSince is nil to return no attachment bodies, otherwise a (possibly empty) list of
//   revisions for which the client already has attachments and doesn't need bodies. Any attachment
//   that hasn't changed since one of those revisions will be returned as a stub.
func (db *Database) getRev(docid, revid string, maxHistory int, historyFrom []string, attachmentsSince []string, showExp bool, copyType BodyCopyType) (Body, error) {
	var err error
	var revision DocumentRevision
	revIDGiven := (revid != "")
	if revIDGiven {
		// Get a specific revision body and history from the revision cache
		// (which will load them if necessary, by calling revCacheLoader, above)
		revision, err = db.revisionCache.Get(docid, revid, copyType)
	} else {
		// No rev ID given, so load active revision
		revision, err = db.revisionCache.GetActive(docid, copyType)
		if revision.Body != nil {
			revid = revision.RevID
		}
	}

	if revision.Body == nil {
		if err == nil {
			err = base.HTTPErrorf(404, "missing")
		}
		return nil, err
	}

	// RequestedHistory is the _revisions returned in the body.  Avoids mutating revision.History, in case it's needed
	// during attachment processing below
	requestedHistory := revision.History
	if maxHistory == 0 {
		requestedHistory = nil
	}
	if requestedHistory != nil {
		_, requestedHistory = trimEncodedRevisionsToAncestor(requestedHistory, historyFrom, maxHistory)
	}

	// Authorize the access:
	if db.user != nil {
		if err := db.user.AuthorizeAnyChannel(revision.Channels); err != nil {
			if !revIDGiven {
				return nil, base.HTTPErrorf(403, "forbidden")
			}
			// On access failure, return (only) the doc history and deletion/removal
			// status instead of returning an error. For justification see the comment in
			// the getRevFromDoc method, below
			deleted, _ := revision.Body[BodyDeleted].(bool)
			redactedBody := Body{BodyId: docid, BodyRev: revid}
			if deleted {
				redactedBody[BodyDeleted] = true
			} else {
				redactedBody["_removed"] = true
			}
			if requestedHistory != nil {
				redactedBody[BodyRevisions] = requestedHistory
			}
			return redactedBody, nil
		}
	}

	if !revIDGiven {
		if deleted, _ := revision.Body[BodyDeleted].(bool); deleted {
			return nil, base.HTTPErrorf(404, "deleted")
		}
	}

	// Add revision metadata:
	if requestedHistory != nil {
		revision.Body[BodyRevisions] = requestedHistory
	}

	if showExp && revision.Expiry != nil && !revision.Expiry.IsZero() {
		revision.Body["_exp"] = revision.Expiry.Format(time.RFC3339)
	}

	// Stamp attachment metadata back into the body
	if revision.Attachments != nil {
		revision.Body[BodyAttachments] = revision.Attachments
	}

	// Add attachment bodies if requested:
	if attachmentsSince != nil && len(GetBodyAttachments(revision.Body)) > 0 {
		minRevpos := 1
		if len(attachmentsSince) > 0 {
			ancestor := revision.History.findAncestor(attachmentsSince)
			if ancestor != "" {
				minRevpos, _ = ParseRevID(ancestor)
				minRevpos++
			}
		}
		revision.Body, err = db.loadBodyAttachments(revision.Body, minRevpos, docid)
		if err != nil {
			return nil, err
		}
	}

	return revision.Body, nil
}

// GetDelta attempts to return the delta between fromRevId and toRevId.  If the delta can't be generated,
// returns nil.
func (db *Database) GetDelta(docID, fromRevID, toRevID string) (delta *RevisionDelta, err error) {

	if docID == "" || fromRevID == "" || toRevID == "" {
		return nil, nil
	}

	fromRevision, err := db.revisionCache.Get(docID, fromRevID, BodyNoCopy)

	// If neither body nor delta is available for fromRevId, the delta can't be generated
	if fromRevision.Body == nil && fromRevision.Delta == nil {
		return nil, err
	}

	// If delta is found, check whether it is a delta for the toRevID we want
	if fromRevision.Delta != nil {
		if fromRevision.Delta.ToRevID == toRevID {
			// Case 2a. 'some rev' is the rev we're interested in - return the delta
			db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaCacheHits, 1)
			return fromRevision.Delta, nil
		} else {
			// TODO: Recurse and merge deltas when gen(revCacheDelta.toRevID) < gen(toRevId)
			// until then, fall through to generating delta for given rev pair
		}
	}

	// Delta is unavailable, but the body is available.
	if fromRevision.Body != nil {

		db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaCacheMisses, 1)
		toBody, err := db.revisionCache.Get(docID, toRevID, BodyDeepCopy)
		if err != nil {
			return nil, err
		}
		// We didn't copy fromBody earlier (in case we could get by with just the delta), so need do it now
		fromBodyCopy := fromRevision.Body.DeepCopy()

		// If attachments have changed between these revisions, we'll stamp the metadata into the bodies before diffing
		// so that the resulting delta also contains attachment metadata changes
		if fromRevision.Attachments != nil {
			// the delta library does not handle deltas in non builtin types,
			// so we need the map[string]interface{} type conversion here
			fromBodyCopy[BodyAttachments] = map[string]interface{}(fromRevision.Attachments)
		}
		if toBody.Attachments != nil {
			toBody.Body[BodyAttachments] = map[string]interface{}(toBody.Attachments)
		}

		deltaBytes, err := base.Diff(fromBodyCopy, toBody.Body)
		if err != nil {
			return nil, err
		}
		revCacheDelta := newRevCacheDelta(deltaBytes, fromRevID, toBody)

		// Write the newly calculated delta back into the cache before returning
		db.revisionCache.UpdateDelta(docID, fromRevID, revCacheDelta)
		return revCacheDelta, nil
	}

	return nil, nil
}

// Returns the body of the active revision of a document, as well as the document's current channels
// and the user/roles it grants channel access to.
func (db *Database) GetDocAndActiveRev(docid string) (populatedDoc *Document, body Body, err error) {
	populatedDoc, err = db.GetDocument(docid, DocUnmarshalAll)
	if populatedDoc == nil {
		return
	}
	body, err = db.getRevFromDoc(populatedDoc, populatedDoc.CurrentRev, false)
	if err != nil {
		return
	}
	return
}

// Returns the body of a revision of a document, as well as the document's current channels
// and the user/roles it grants channel access to.
func (db *Database) GetRevAndChannels(docid string, revid string, listRevisions bool) (body Body, channels channels.ChannelMap, access UserAccessMap, roleAccess UserAccessMap, flags uint8, sequence uint64, err error) {
	doc, err := db.GetDocument(docid, DocUnmarshalAll)
	if doc == nil {
		return
	}
	body, err = db.getRevFromDoc(doc, revid, listRevisions)
	if err != nil {
		return
	}
	channels = doc.Channels
	access = doc.Access
	roleAccess = doc.RoleAccess
	sequence = doc.Sequence
	flags = doc.Flags
	return
}

// Returns an HTTP 403 error if the User is not allowed to access any of this revision's channels.
func (db *Database) AuthorizeDocID(docid, revid string) error {
	doc, err := db.GetDocument(docid, DocUnmarshalSync)
	if doc == nil {
		return err
	}
	return db.authorizeDoc(doc, revid)
}

// Returns an HTTP 403 error if the User is not allowed to access any of this revision's channels.
func (db *Database) authorizeDoc(doc *Document, revid string) error {
	user := db.user
	if doc == nil || user == nil {
		return nil // A nil User means access control is disabled
	}
	if revid == "" {
		revid = doc.CurrentRev
	}
	if rev := doc.History[revid]; rev != nil {
		// Authenticate against specific revision:
		return db.user.AuthorizeAnyChannel(rev.Channels)
	} else {
		// No such revision; let the caller proceed and return a 404
		return nil
	}
}

// Gets a revision of a document. If it's obsolete it will be loaded from the database if possible.
// This method adds the magic _id, _rev and _attachments properties.
func (db *DatabaseContext) getRevision(doc *Document, revid string) (Body, error) {
	var body Body
	if body = doc.getRevisionBody(revid, db.RevisionBodyLoader); body == nil {
		// No inline body, so look for separate doc:
		if !doc.History.contains(revid) {
			return nil, base.HTTPErrorf(404, "missing")
		} else if data, err := db.getOldRevisionJSON(doc.ID, revid); data == nil {
			return nil, err
		} else if err = body.Unmarshal(data); err != nil {
			return nil, err
		}
	}
	body[BodyId] = doc.ID
	body[BodyRev] = revid

	if doc.CurrentRev == revid && doc.Attachments != nil {
		body[BodyAttachments] = doc.Attachments
	}

	return body, nil
}

// Gets a revision of a document as raw JSON.
// If it's obsolete it will be loaded from the database if possible.
// Does not add _id or _rev properties.
func (db *Database) getRevisionBodyJSON(doc *Document, revid string) ([]byte, error) {
	if body := doc.getRevisionBodyJSON(revid, db.RevisionBodyLoader); body != nil {
		return body, nil
	} else if !doc.History.contains(revid) {
		return nil, base.HTTPErrorf(404, "missing")
	} else {
		return db.getOldRevisionJSON(doc.ID, revid)
	}
}

// Gets the body of a revision's nearest ancestor, as raw JSON (without _id or _rev.)
// If no ancestor has any JSON, returns nil but no error.
func (db *Database) getAncestorJSON(doc *Document, revid string) ([]byte, error) {
	for {
		if revid = doc.History.getParent(revid); revid == "" {
			return nil, nil
		} else if body, err := db.getRevisionBodyJSON(doc, revid); body != nil {
			return body, nil
		} else if !base.IsDocNotFoundError(err) {
			return nil, err
		}
	}
}

// Returns the body of a revision given a document struct. Checks user access.
func (db *Database) getRevFromDoc(doc *Document, revid string, listRevisions bool) (Body, error) {
	var body Body
	if err := db.authorizeDoc(doc, revid); err != nil {
		// As a special case, you don't need channel access to see a deletion revision,
		// otherwise the client's replicator can't process the deletion (since deletions
		// usually aren't on any channels at all!) But don't show the full body. (See #59)
		// Update: this applies to non-deletions too, since the client may have lost access to
		// the channel and gotten a "removed" entry in the _changes feed. It then needs to
		// incorporate that tombstone and for that it needs to see the _revisions property.
		if revid == "" || doc.History[revid] == nil /*|| !doc.History[revid].Deleted*/ {
			return nil, err
		}
		body = Body{BodyId: doc.ID, BodyRev: revid}
		if !doc.History[revid].Deleted {
			body["_removed"] = true
		}
	} else {
		if revid == "" {
			revid = doc.CurrentRev
			if doc.History[revid].Deleted == true {
				return nil, base.HTTPErrorf(404, "deleted")
			}
		}
		var err error
		if body, err = db.getRevision(doc, revid); err != nil {
			return nil, err
		}
	}
	if doc.History[revid].Deleted {
		body[BodyDeleted] = true
	}
	if listRevisions {
		validatedHistory, getHistoryErr := doc.History.getHistory(revid)
		if getHistoryErr != nil {
			return nil, getHistoryErr
		}
		body[BodyRevisions] = encodeRevisions(validatedHistory)
	}
	return body, nil
}

// Returns the body of the asked-for revision or the most recent available ancestor.
func (db *Database) getAvailableRev(doc *Document, revid string) (Body, error) {
	for ; revid != ""; revid = doc.History[revid].Parent {
		if body, _ := db.getRevision(doc, revid); body != nil {
			return body, nil
		}
	}
	return nil, base.HTTPErrorf(404, "missing")
}

// Moves a revision's ancestor's body out of the document object and into a separate db doc.
func (db *Database) backupAncestorRevs(doc *Document, newRevId string, newBody Body) {
	// Find an ancestor that still has JSON in the document:
	var json []byte
	ancestorRevId := newRevId
	for {
		if ancestorRevId = doc.History.getParent(ancestorRevId); ancestorRevId == "" {
			// No ancestors with JSON found.  Check if we need to back up current rev for delta sync, then return
			db.backupRevisionJSON(doc.ID, newRevId, "", newBody, nil)
			return
		} else if json = doc.getRevisionBodyJSON(ancestorRevId, db.RevisionBodyLoader); json != nil {
			break
		}
	}

	// Back up the revision JSON as a separate doc in the bucket:
	db.backupRevisionJSON(doc.ID, newRevId, ancestorRevId, newBody, json)

	// Nil out the ancestor rev's body in the document struct:
	if ancestorRevId == doc.CurrentRev {
		doc.RemoveBody()
	} else {
		doc.removeRevisionBody(ancestorRevId)
	}
}

//////// UPDATING DOCUMENTS:

// Initializes the gateway-specific "_sync_" metadata of a new document.
// Used when importing an existing Couchbase doc that hasn't been seen by the gateway before.
func (db *Database) initializeSyncData(doc *Document) (err error) {
	body := doc.Body()
	doc.CurrentRev, err = createRevID(1, "", body)
	if err != nil {
		return err
	}
	body[BodyRev] = doc.CurrentRev
	doc.setFlag(channels.Deleted, false)
	doc.History = make(RevTree)
	if err = doc.History.addRevision(doc.ID, RevInfo{ID: doc.CurrentRev, Parent: "", Deleted: false}); err != nil {
		return err
	}
	if db.writeSequences() {
		doc.Sequence, err = db.sequences.nextSequence()
	}
	return
}

func (db *Database) OnDemandImportForWrite(docid string, doc *Document, body Body) error {

	// Check whether the doc requiring import is an SDK delete
	isDelete := false
	if doc.Body() == nil {
		isDelete = true
	} else {
		deletedInBody, ok := body[BodyDeleted].(bool)
		if ok {
			isDelete = deletedInBody
		}
	}
	// Use an admin-scoped database for import
	importDb := Database{DatabaseContext: db.DatabaseContext, user: nil}

	importedDoc, importErr := importDb.ImportDoc(docid, doc, isDelete, nil, ImportOnDemand)

	if importErr == base.ErrImportCancelledFilter {
		// Document exists, but existing doc wasn't imported based on import filter.  Treat write as insert
		doc.SyncData = SyncData{History: make(RevTree)}
	} else if importErr != nil {
		return importErr
	} else {
		doc = importedDoc
	}
	return nil
}

// Updates or creates a document.
// The new body's BodyRev property must match the current revision's, if any.
func (db *Database) Put(docid string, body Body) (newRevID string, doc *Document, err error) {
	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body[BodyRev].(string)
	generation, _ := ParseRevID(matchRev)
	if generation < 0 {
		return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++
	deleted, _ := body[BodyDeleted].(bool)

	expiry, err := body.extractExpiry()
	if err != nil {
		return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}

	allowImport := db.UseXattrs()

	doc, newRevID, err = db.updateAndReturnDoc(docid, allowImport, expiry, nil, func(doc *Document) (resultBody Body, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {

		var isSgWrite bool
		var crc32Match bool

		// Is this doc an sgWrite?
		if doc != nil {
			isSgWrite, crc32Match = doc.IsSGWrite(nil)
			if crc32Match {
				db.DbStats.StatsDatabase().Add(base.StatKeyCrc32cMatchCount, 1)
			}
		}

		// (Be careful: this block can be invoked multiple times if there are races!)
		// If the existing doc isn't an SG write, import prior to updating
		if doc != nil && !isSgWrite && db.UseXattrs() {
			err := db.OnDemandImportForWrite(docid, doc, body)
			if err != nil {
				return nil, nil, nil, err
			}
		}

		// First, make sure matchRev matches an existing leaf revision:
		if matchRev == "" {
			matchRev = doc.CurrentRev
			if matchRev != "" {
				// PUT with no parent rev given, but there is an existing current revision.
				// This is OK as long as the current one is deleted.
				if !doc.History[matchRev].Deleted {
					return nil, nil, nil, base.HTTPErrorf(http.StatusConflict, "Document exists")
				}
				generation, _ = ParseRevID(matchRev)
				generation++
			}
		} else if !doc.History.isLeaf(matchRev) || db.IsIllegalConflict(doc, matchRev, deleted, false) {
			return nil, nil, nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
		}

		// Process the attachments, and populate _sync with metadata. This alters 'body' so it has to
		// be done before calling createRevID (the ID is based on the digest of the body.)
		newAttachments, err := db.storeAttachments(doc, body, generation, matchRev, nil)
		if err != nil {
			return nil, nil, nil, err
		}

		// Make up a new _rev, and add it to the history:
		newRev, err := createRevID(generation, matchRev, body)
		if err != nil {
			return nil, nil, nil, err
		}
		body[BodyRev] = newRev
		if err := doc.History.addRevision(docid, RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted}); err != nil {
			base.InfofCtx(db.Ctx, base.KeyCRUD, "Failed to add revision ID: %s, for doc: %s, error: %v", newRev, base.UD(docid), err)
			return nil, nil, nil, base.ErrRevTreeAddRevFailure
		}

		// move _attachment metadata to syncdata of doc after rev-id generation
		doc.SyncData.Attachments = GetBodyAttachments(body)
		delete(body, BodyAttachments)

		return body, newAttachments, nil, nil
	})

	return newRevID, doc, err
}

// Adds an existing revision to a document along with its history (list of rev IDs.)
// This is equivalent to the "new_edits":false mode of CouchDB.
//
// The docHistory should be a list of previous revisions in reverse order, not including the revision itself.
// For example, if the rev tree is currently:
//
// 1-abc
//  |
// 2-bcd
//
// and the new revision being added is "3-cde", then docHistory passed in should be ["2-bcd", "1-abc"].
//
func (db *Database) PutExistingRev(docid string, body Body, docHistory []string, noConflicts bool) (doc *Document, err error) {
	newRev := docHistory[0]
	generation, _ := ParseRevID(newRev)
	if generation < 0 {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	deleted, _ := body[BodyDeleted].(bool)

	expiry, err := body.extractExpiry()
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}

	allowImport := db.UseXattrs()
	doc, _, err = db.updateAndReturnDoc(docid, allowImport, expiry, nil, func(doc *Document) (resultBody Body, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {
		// (Be careful: this block can be invoked multiple times if there are races!)

		var isSgWrite bool
		var crc32Match bool

		// Is this doc an sgWrite?
		if doc != nil {
			isSgWrite, crc32Match = doc.IsSGWrite(nil)
			if crc32Match {
				db.DbStats.StatsDatabase().Add(base.StatKeyCrc32cMatchCount, 1)
			}
		}

		// If the existing doc isn't an SG write, import prior to updating
		if doc != nil && !isSgWrite && db.UseXattrs() {
			err := db.OnDemandImportForWrite(docid, doc, body)
			if err != nil {
				return nil, nil, nil, err
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
			base.DebugfCtx(db.Ctx, base.KeyCRUD, "PutExistingRev(%q): No new revisions to add", base.UD(docid))
			body[BodyRev] = newRev                     // The _rev field is expected by some callers.  If missing, may cause problems for callers.
			return nil, nil, nil, base.ErrUpdateCancel // No new revisions to add
		}

		// Conflict-free mode check
		if db.IsIllegalConflict(doc, parent, deleted, noConflicts) {
			return nil, nil, nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
		}

		// Add all the new-to-me revisions to the rev tree:
		for i := currentRevIndex - 1; i >= 0; i-- {
			err := doc.History.addRevision(docid,
				RevInfo{
					ID:      docHistory[i],
					Parent:  parent,
					Deleted: (i == 0 && deleted)})

			if err != nil {
				return nil, nil, nil, err
			}
			parent = docHistory[i]
		}

		// Process the attachments, replacing bodies with digests.
		parentRevID := doc.History[newRev].Parent
		newAttachments, err := db.storeAttachments(doc, body, generation, parentRevID, docHistory)
		if err != nil {
			return nil, nil, nil, err
		}
		// move _attachment metadata to syncdata of doc
		doc.SyncData.Attachments = GetBodyAttachments(body)
		delete(body, BodyAttachments)

		body[BodyRev] = newRev
		return body, newAttachments, nil, nil
	})

	return doc, err
}

// IsIllegalConflict returns true if the given operation is forbidden due to conflicts.
// AllowConflicts is whether or not the database allows conflicts,
// and 'noConflicts' is whether or not the request should allow conflicts to occurr.
/*
Truth table for AllowConflicts and noConflicts combinations:

                       AllowConflicts=true     AllowConflicts=false
   noConflicts=true    continue checks         continue checks
   noConflicts=false   return false            continue checks */
func (db *Database) IsIllegalConflict(doc *Document, parentRevID string, deleted, noConflicts bool) bool {

	if db.AllowConflicts() && !noConflicts {
		return false
	}

	// Conflict-free mode: If doc exists, its current rev must be the new rev's parent, unless it's a tombstone.

	// If the parent is the current rev, it's not a conflict.
	if parentRevID == doc.CurrentRev || doc.CurrentRev == "" {
		return false
	}

	// If the parent isn't the current rev, reject as a conflict unless this is tombstoning an existing non-winning leaf
	if !deleted {
		base.DebugfCtx(db.Ctx, base.KeyCRUD, "Conflict - non-tombstone updates to non-winning revisions aren't valid when allow_conflicts=false")
		return true
	}

	// If it's a tombstone, it's allowed if it's tombstoning an existing non-tombstoned leaf
	for _, leafRevId := range doc.History.GetLeaves() {
		if leafRevId == parentRevID && doc.History[leafRevId].Deleted == false {
			return false
		}
	}

	// If we haven't found a valid conflict scenario by this point, flag as invalid
	base.DebugfCtx(db.Ctx, base.KeyCRUD, "Conflict - tombstone updates to non-leaf or already tombstoned revisions aren't valid when allow_conflicts=false")
	return true
}

// Function type for the callback passed into updateAndReturnDoc
type updateAndReturnDocCallback func(*Document) (resultBody Body, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error)

// Calling updateAndReturnDoc directly allows callers to:
//   1. Receive the updated document body in the response
//   2. Specify the existing document body/xattr/cas, to avoid initial retrieval of the doc in cases that the current contents are already known (e.g. import).
//      On cas failure, the document will still be reloaded from the bucket as usual.
func (db *Database) updateAndReturnDoc(
	docid string,
	allowImport bool,
	expiry uint32,
	existingDoc *sgbucket.BucketDocument, // If existing is present, passes these to WriteUpdateWithXattr to allow bypass of initial GET
	callback updateAndReturnDocCallback) (docOut *Document, newRevID string, err error) {
	key := realDocID(docid)
	if key == "" {
		return nil, "", base.HTTPErrorf(400, "Invalid doc ID")
	}

	// Added annotation to the following variable declarations for reference during future refactoring of documentUpdateFunc into a standalone function
	var doc *Document                                // Passed to documentUpdateFunc as pointer, may be possible to define in documentUpdateFunc
	var body Body                                    // Could be returned by documentUpdateFunc
	var storedBody Body                              // Persisted revision body, used to update rev cache
	var changedPrincipals, changedRoleUsers []string // Could be returned by documentUpdateFunc
	var docSequence uint64                           // Must be scoped outside callback, used over multiple iterations
	var unusedSequences []uint64                     // Must be scoped outside callback, used over multiple iterations
	var oldBodyJSON string                           // Could be returned by documentUpdateFunc.  Stores previous revision body for use by DocumentChangeEvent

	// documentUpdateFunc applies the changes to the document.  Called by either WriteUpdate or WriteUpdateWithXATTR below.
	documentUpdateFunc := func(doc *Document, docExists bool, importAllowed bool) (updatedDoc *Document, writeOpts sgbucket.WriteOptions, shadowerEcho bool, updatedExpiry *uint32, err error) {

		var newAttachments AttachmentData
		// Be careful: this block can be invoked multiple times if there are races!
		if !importAllowed && docExists && !doc.HasValidSyncData(db.writeSequences()) {
			err = base.HTTPErrorf(409, "Not imported")
			return
		}

		// Invoke the callback to update the document and return a new revision body:
		body, newAttachments, updatedExpiry, err = callback(doc)
		if err != nil {
			return
		}

		//Test for shadower echo here before UpstreamRev gets set to CurrentRev
		//Ignore new docs (doc.CurrentRev == "")
		shadowerEcho = doc.CurrentRev != "" && doc.CurrentRev == doc.UpstreamRev

		//Reject a body that contains the "_removed" property, this means that the user
		//is trying to update a document they do not have read access to.
		if body["_removed"] != nil {
			err = base.HTTPErrorf(http.StatusNotFound, "Document revision is not accessible")
			return
		}

		//Reject bodies containing user special properties for compatibility with CouchDB
		if containsUserSpecialProperties(body) {
			err = base.HTTPErrorf(400, "user defined top level properties beginning with '_' are not allowed in document body")
			return
		}

		// Determine which is the current "winning" revision (it's not necessarily the new one):
		newRevID = body[BodyRev].(string)
		prevCurrentRev := doc.CurrentRev
		var branched, inConflict bool
		doc.CurrentRev, branched, inConflict = doc.History.winningRevision()
		doc.setFlag(channels.Deleted, doc.History[doc.CurrentRev].Deleted)
		doc.setFlag(channels.Conflict, inConflict)
		doc.setFlag(channels.Branched, branched)

		// If tombstone, write tombstone time
		if doc.hasFlag(channels.Deleted) {
			doc.SyncData.TombstonedAt = time.Now().Unix()
		} else {
			doc.SyncData.TombstonedAt = 0
		}

		if doc.CurrentRev != prevCurrentRev && prevCurrentRev != "" && doc.Body() != nil {
			// Store the doc's previous body into the revision tree:
			bodyJSON, marshalErr := doc.MarshalBody()
			if marshalErr != nil {
				base.WarnfCtx(db.Ctx, base.KeyAll, "Unable to marshal document body for storage in rev tree: %v", marshalErr)
			}
			doc.setNonWinningRevisionBody(prevCurrentRev, bodyJSON, db.AllowExternalRevBodyStorage())
		}

		// Store the new revision body into the doc:
		storedBody = doc.setRevisionBody(newRevID, body, db.AllowExternalRevBodyStorage())

		if doc.CurrentRev == newRevID {
			doc.NewestRev = ""
			doc.setFlag(channels.Hidden, false)
		} else {
			doc.NewestRev = newRevID
			doc.setFlag(channels.Hidden, true)
			if doc.CurrentRev != prevCurrentRev {
				doc.promoteNonWinningRevisionBody(doc.CurrentRev, db.RevisionBodyLoader)
			}
		}

		// Run the sync function, to validate the update and compute its channels/access:
		body[BodyId] = doc.ID
		channelSet, access, roles, syncExpiry, oldBody, err := db.getChannelsAndAccess(doc, body, newRevID)
		if err != nil {
			return
		}
		db.checkDocChannelsAndGrantsLimits(docid, channelSet, access, roles)

		//Assign old revision body to variable in method scope
		oldBodyJSON = oldBody

		if len(channelSet) > 0 {
			doc.History[newRevID].Channels = channelSet
		}

		//Need to check and add attachments here to ensure the attachment is within size constraints
		attachmentErr := db.setAttachments(newAttachments)
		if attachmentErr != nil {

			if attachmentErr.Error() == "document value was too large" {
				err = base.HTTPErrorf(http.StatusRequestEntityTooLarge, "Attachment too large")
			} else {
				err = errors.Wrap(attachmentErr, "Error adding attachment")
			}
			return
		}

		// Move the body of the replaced revision out of the document so it can be compacted later.
		db.backupAncestorRevs(doc, newRevID, storedBody)

		// Sequence processing
		if db.writeSequences() {
			// Now that we know doc is valid, assign it the next sequence number, for _changes feed.
			// But be careful not to request a second sequence # on a retry if we don't need one.
			if docSequence <= doc.Sequence {
				if docSequence > 0 {
					// Oops: we're on our second iteration thanks to a conflict, but the sequence
					// we previously allocated is unusable now. We have to allocate a new sequence
					// instead, but we add the unused one(s) to the document so when the changeCache
					// reads the doc it won't freak out over the break in the sequence numbering.
					base.InfofCtx(db.Ctx, base.KeyCache, "updateDoc %q: Unused sequence #%d", base.UD(docid), docSequence)
					unusedSequences = append(unusedSequences, docSequence)
				}

				for {
					if docSequence, err = db.sequences.nextSequence(); err != nil {
						return
					}

					if docSequence > doc.Sequence {
						break
					} else {
						releaseErr := db.sequences.releaseSequence(docSequence)
						if releaseErr != nil {
							base.Warnf(base.KeyCRUD, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", docSequence, err)
						}
					}
					// Could add a db.Sequences.nextSequenceGreaterThan(doc.Sequence) to push the work down into the sequence allocator
					//  - sequence allocator is responsible for releasing unused sequences, could optimize to do that in bulk if needed

				}
			}
			doc.Sequence = docSequence
			doc.UnusedSequences = unusedSequences

			// The server TAP/DCP feed will deduplicate multiple revisions for the same doc if they occur in
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
				stableSequence := db.changeCache.GetStableSequence(docid).Seq
				count := 0
				for _, seq := range doc.RecentSequences {
					// Only remove sequences if they are higher than a sequence that's been seen on the
					// feed. This is valid across SG nodes (which could each have a different nextSequence),
					// as the mutations that this node used to rev nextSequence will at some point be delivered
					// to each node.
					if seq < stableSequence {
						count++
					} else {
						break
					}
				}
				if count > 0 {
					doc.RecentSequences = doc.RecentSequences[count:]
				}
			}

			// Append current sequence and unused sequences to recent sequence history
			doc.RecentSequences = append(doc.RecentSequences, unusedSequences...)
			doc.RecentSequences = append(doc.RecentSequences, docSequence)
		}

		if doc.CurrentRev != prevCurrentRev {
			// Most of the time this update will change the doc's current rev. (The exception is
			// if the new rev is a conflict that doesn't win the revid comparison.) If so, we
			// need to update the doc's top-level Channels and Access properties to correspond
			// to the current rev's state.
			if newRevID != doc.CurrentRev {
				// In some cases an older revision might become the current one. If so, get its
				// channels & access, for purposes of updating the doc:
				var curBody Body
				if curBody, err = db.getAvailableRev(doc, doc.CurrentRev); curBody != nil {
					base.DebugfCtx(db.Ctx, base.KeyCRUD, "updateDoc(%q): Rev %q causes %q to become current again",
						base.UD(docid), newRevID, doc.CurrentRev)
					channelSet, access, roles, syncExpiry, oldBody, err = db.getChannelsAndAccess(doc, curBody, doc.CurrentRev)

					//Assign old revision body to variable in method scope
					oldBodyJSON = oldBody
					if err != nil {
						return
					}
				} else {
					// Shouldn't be possible (CurrentRev is a leaf so won't have been compacted)
					base.WarnfCtx(db.Ctx, base.KeyAll, "updateDoc(%q): Rev %q missing, can't call getChannelsAndAccess "+
						"on it (err=%v)", base.UD(docid), doc.CurrentRev, err)
					channelSet = nil
					access = nil
					roles = nil
				}
			}

			// Update the document struct's channel assignment and user access.
			// (This uses the new sequence # so has to be done after updating doc.Sequence)
			_, err := doc.updateChannels(channelSet) //FIX: Incorrect if new rev is not current!
			if err != nil {
				return doc, writeOpts, shadowerEcho, updatedExpiry, err
			}
			changedPrincipals = doc.Access.updateAccess(doc, access)
			changedRoleUsers = doc.RoleAccess.updateAccess(doc, roles)

			if len(changedPrincipals) > 0 || len(changedRoleUsers) > 0 {
				major, _, _ := db.Bucket.CouchbaseServerVersion()
				if major < 3 {
					// If the couchbase server version is less than 3, then do an indexable write.
					// Prior to couchbase 3, a stale=false query required an indexable write (due to race).
					// If this update affects user/role access privileges, make sure the write blocks till
					// the new value is indexable, otherwise when a User/Role updates (using a view) it
					// might not incorporate the effects of this change.
					writeOpts |= sgbucket.Indexable
				}
			}

		} else {
			base.DebugfCtx(db.Ctx, base.KeyCRUD, "updateDoc(%q): Rev %q leaves %q still current",
				base.UD(docid), newRevID, prevCurrentRev)
		}

		// Prune old revision history to limit the number of revisions:
		if pruned := doc.pruneRevisions(db.RevsLimit, doc.CurrentRev); pruned > 0 {
			base.DebugfCtx(db.Ctx, base.KeyCRUD, "updateDoc(%q): Pruned %d old revisions", base.UD(docid), pruned)
		}

		doc.TimeSaved = time.Now()
		if syncExpiry != nil {
			updatedExpiry = syncExpiry
			doc.UpdateExpiry(*updatedExpiry)
		} else {
			doc.UpdateExpiry(expiry)
		}

		// Now that the document has been successfully validated, we can update externally stored revision bodies
		revisionBodyError := doc.persistModifiedRevisionBodies(db.Bucket)
		if revisionBodyError != nil {
			return doc, writeOpts, shadowerEcho, updatedExpiry, revisionBodyError
		}

		return doc, writeOpts, shadowerEcho, updatedExpiry, err
	}

	var shadowerEcho bool

	// Update the document
	inConflict := false
	upgradeInProgress := false
	docBytes := 0   // Track size of document written, for write stats
	xattrBytes := 0 // Track size of xattr written, for write stats
	if !db.UseXattrs() {
		// Update the document, storing metadata in _sync property
		_, err = db.Bucket.WriteUpdate(key, expiry, func(currentValue []byte) (raw []byte, writeOpts sgbucket.WriteOptions, syncFuncExpiry *uint32, err error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if doc, err = unmarshalDocument(docid, currentValue); err != nil {
				return
			}
			docOut, writeOpts, shadowerEcho, syncFuncExpiry, err = documentUpdateFunc(doc, currentValue != nil, allowImport)
			if err != nil {
				return
			}
			inConflict = docOut.hasFlag(channels.Conflict)
			// Return the new raw document value for the bucket to store.
			raw, err = json.Marshal(docOut)
			base.DebugfCtx(db.Ctx, base.KeyCRUD, "Saving doc (seq: #%d, id: %v rev: %v)", doc.Sequence, base.UD(doc.ID), doc.CurrentRev)
			docBytes = len(raw)
			return raw, writeOpts, syncFuncExpiry, err
		})

		// If we can't find sync metadata in the document body, check for upgrade.  If upgrade, retry write using WriteUpdateWithXattr
		if err != nil && err.Error() == "409 Not imported" {
			_, bucketDocument := db.checkForUpgrade(key, DocUnmarshalAll)
			if bucketDocument != nil && bucketDocument.Xattr != nil {
				existingDoc = bucketDocument
				upgradeInProgress = true
			}
		}
	}

	if db.UseXattrs() || upgradeInProgress {
		var casOut uint64
		// Update the document, storing metadata in extended attribute
		casOut, err = db.Bucket.WriteUpdateWithXattr(key, base.SyncXattrName, expiry, existingDoc, func(currentValue []byte, currentXattr []byte, cas uint64) (raw []byte, rawXattr []byte, deleteDoc bool, syncFuncExpiry *uint32, err error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if doc, err = unmarshalDocumentWithXattr(docid, currentValue, currentXattr, cas, DocUnmarshalAll); err != nil {
				return
			}

			docOut, _, _, syncFuncExpiry, err = documentUpdateFunc(doc, currentValue != nil, true)
			if err != nil {
				return
			}
			inConflict = docOut.hasFlag(channels.Conflict)

			currentRevFromHistory, ok := docOut.History[docOut.CurrentRev]
			if !ok {
				err = base.RedactErrorf("WriteUpdateWithXattr() not able to find revision (%v) in history of doc: %+v.  Cannot update doc.", docOut.CurrentRev, base.UD(docOut))
				return
			}

			deleteDoc = currentRevFromHistory.Deleted

			// Return the new raw document value for the bucket to store.
			raw, rawXattr, err = docOut.MarshalWithXattr()
			docBytes = len(raw)

			// Warn when sync data is larger than a configured threshold
			if xattrBytesThreshold := db.Options.UnsupportedOptions.WarningThresholds.XattrSize; xattrBytesThreshold != nil {
				xattrBytes = len(rawXattr)
				if uint32(xattrBytes) >= *xattrBytesThreshold {
					db.DbStats.StatsDatabase().Add(base.StatKeyWarnXattrSizeCount, 1)
					base.WarnfCtx(db.Ctx, base.KeyAll, "Doc id: %v sync metadata size: %d bytes exceeds %d bytes for sync metadata warning threshold", base.UD(docOut.ID), xattrBytes, *xattrBytesThreshold)
				}
			}

			base.DebugfCtx(db.Ctx, base.KeyCRUD, "Saving doc (seq: #%d, id: %v rev: %v)", docOut.Sequence, base.UD(docOut.ID), docOut.CurrentRev)
			return raw, rawXattr, deleteDoc, syncFuncExpiry, err
		})
		if err != nil {
			if err == base.ErrDocumentMigrated {
				base.DebugfCtx(db.Ctx, base.KeyCRUD, "Migrated document %q to use xattr.", base.UD(key))
			} else {
				base.DebugfCtx(db.Ctx, base.KeyCRUD, "Did not update document %q w/ xattr: %v", base.UD(key), err)
			}
		} else if docOut != nil {
			docOut.Cas = casOut
		}
	}

	// If the WriteUpdate didn't succeed, check whether there are unused, allocated sequences that need to be accounted for
	if err != nil && db.writeSequences() {
		if docSequence > 0 {
			if seqErr := db.sequences.releaseSequence(docSequence); seqErr != nil {
				base.WarnfCtx(db.Ctx, base.KeyAll, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", docSequence, seqErr)
			}

		}
		for _, sequence := range unusedSequences {
			if seqErr := db.sequences.releaseSequence(sequence); seqErr != nil {
				base.WarnfCtx(db.Ctx, base.KeyAll, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", sequence, seqErr)
			}
		}
	}

	if err == base.ErrUpdateCancel {
		return nil, "", nil
	} else if err == couchbase.ErrOverwritten {
		// ErrOverwritten is ok; if a later revision got persisted, that's fine too
		base.DebugfCtx(db.Ctx, base.KeyCRUD, "Note: Rev %q/%q was overwritten in RAM before becoming indexable",
			base.UD(docid), newRevID)
	} else if err != nil {
		return nil, "", err
	}

	db.DbStats.StatsDatabase().Add(base.StatKeyNumDocWrites, 1)
	db.DbStats.StatsDatabase().Add(base.StatKeyDocWritesBytes, int64(docBytes))
	db.DbStats.StatsDatabase().Add(base.StatKeyDocWritesXattrBytes, int64(xattrBytes))
	if inConflict {
		db.DbStats.StatsDatabase().Add(base.StatKeyConflictWriteCount, 1)
	}

	if doc.History[newRevID] != nil {
		// Store the new revision in the cache
		history, getHistoryErr := doc.History.getHistory(newRevID)
		if getHistoryErr != nil {
			return nil, "", getHistoryErr
		}

		if doc.History[newRevID].Deleted {
			body[BodyDeleted] = true
		}
		revChannels := doc.History[newRevID].Channels
		documentRevision := DocumentRevision{
			RevID:       newRevID,
			Body:        storedBody,
			History:     encodeRevisions(history),
			Channels:    revChannels,
			Attachments: doc.Attachments,
			Expiry:      doc.Expiry,
		}
		db.revisionCache.Put(doc.ID, documentRevision)

		// Raise event if this is not an echo from a shadow bucket
		if !shadowerEcho {
			if db.EventMgr.HasHandlerForEvent(DocumentChange) {
				db.EventMgr.RaiseDocumentChangeEvent(body, oldBodyJSON, revChannels)
			}
		}
	} else {
		//Revision has been pruned away so won't be added to cache
		base.InfofCtx(db.Ctx, base.KeyCRUD, "doc %q / %q, has been pruned, it has not been inserted into the revision cache", base.UD(docid), newRevID)
	}

	// Now that the document has successfully been stored, we can make other db changes:
	base.DebugfCtx(db.Ctx, base.KeyCRUD, "Stored doc %q / %q as #%v", base.UD(docid), newRevID, doc.Sequence)

	// Remove any obsolete non-winning revision bodies
	doc.deleteRemovedRevisionBodies(db.Bucket)

	// Mark affected users/roles as needing to recompute their channel access:
	db.MarkPrincipalsChanged(docid, newRevID, changedPrincipals, changedRoleUsers)
	return docOut, newRevID, nil
}

func (db *Database) checkDocChannelsAndGrantsLimits(docID string, channels base.Set, accessGrants channels.AccessMap, roleGrants channels.AccessMap) {
	// Warn when channel count is larger than a configured threshold
	if channelCountThreshold := db.Options.UnsupportedOptions.WarningThresholds.ChannelsPerDoc; channelCountThreshold != nil {
		channelCount := len(channels)
		if uint32(channelCount) >= *channelCountThreshold {
			db.DbStats.StatsDatabase().Add(base.StatKeyWarnChannelsPerDocCount, 1)
			base.WarnfCtx(db.Ctx, base.KeyAll, "Doc id: %v channel count: %d exceeds %d for channels per doc warning threshold", base.UD(docID), channelCount, *channelCountThreshold)
		}
	}

	// Warn when grants are larger than a configured threshold
	if grantThreshold := db.Options.UnsupportedOptions.WarningThresholds.ChannelsPerDoc; grantThreshold != nil {
		grantCount := len(accessGrants) + len(roleGrants)
		if uint32(grantCount) >= *grantThreshold {
			db.DbStats.StatsDatabase().Add(base.StatKeyWarnGrantsPerDocCount, 1)
			base.WarnfCtx(db.Ctx, base.KeyAll, "Doc id: %v access and role grants count: %d exceeds %d for grants per doc warning threshold", base.UD(docID), grantCount, *grantThreshold)
		}
	}
}

func (db *Database) MarkPrincipalsChanged(docid string, newRevID string, changedPrincipals, changedRoleUsers []string) {

	reloadActiveUser := false

	// Mark affected users/roles as needing to recompute their channel access:
	if len(changedPrincipals) > 0 {
		base.InfofCtx(db.Ctx, base.KeyAccess, "Rev %q / %q invalidates channels of %s", base.UD(docid), newRevID, changedPrincipals)
		for _, changedAccessPrincipalName := range changedPrincipals {
			db.invalUserOrRoleChannels(changedAccessPrincipalName)
			// Check whether the active user needs to be recalculated.  Skip check if reload has already been identified
			// as required for a previous changedPrincipal
			if db.user != nil && reloadActiveUser == false {
				// If role changed, check if active user has been granted the role
				changedPrincipalName, isRole := channels.AccessNameToPrincipalName(changedAccessPrincipalName)
				if isRole {
					for roleName := range db.user.RoleNames() {
						if roleName == changedPrincipalName {
							base.DebugfCtx(db.Ctx, base.KeyAccess, "Active user belongs to role %q with modified channel access - user %q will be reloaded.", base.UD(roleName), base.UD(db.user.Name()))
							reloadActiveUser = true
							break
						}
					}
				} else if db.user.Name() == changedPrincipalName {
					// User matches
					base.DebugfCtx(db.Ctx, base.KeyAccess, "Channel set for active user has been modified - user %q will be reloaded.", base.UD(db.user.Name()))
					reloadActiveUser = true
				}

			}
		}
	}

	if len(changedRoleUsers) > 0 {
		base.InfofCtx(db.Ctx, base.KeyAccess, "Rev %q / %q invalidates roles of %s", base.UD(docid), newRevID, base.UD(changedRoleUsers))
		for _, name := range changedRoleUsers {
			db.invalUserRoles(name)
			//If this is the current in memory db.user, reload to generate updated roles
			if db.user != nil && db.user.Name() == name {
				base.DebugfCtx(db.Ctx, base.KeyAccess, "Role set for active user has been modified - user %q will be reloaded.", base.UD(db.user.Name()))
				reloadActiveUser = true

			}
		}
	}

	if reloadActiveUser {
		user, err := db.Authenticator().GetUser(db.user.Name())
		if err != nil {
			base.WarnfCtx(db.Ctx, base.KeyAll, "Error reloading active db.user[%s], security information will not be recalculated until next authentication --> %+v", base.UD(db.user.Name()), err)
		} else {
			db.user = user
		}
	}

}

// Creates a new document, assigning it a random doc ID.
func (db *Database) Post(body Body) (string, string, *Document, error) {
	if body[BodyRev] != nil {
		return "", "", nil, base.HTTPErrorf(http.StatusNotFound, "No previous revision to replace")
	}

	// If there's an incoming _id property, use that as the doc ID.
	docid, idFound := body[BodyId].(string)
	if !idFound {
		docid = base.CreateUUID()
	}

	rev, doc, err := db.Put(docid, body)
	if err != nil {
		docid = ""
	}
	return docid, rev, doc, err
}

// Deletes a document, by adding a new revision whose _deleted property is true.
func (db *Database) DeleteDoc(docid string, revid string) (string, error) {
	body := Body{BodyDeleted: true, BodyRev: revid}
	newRevID, _, err := db.Put(docid, body)
	return newRevID, err
}

// Purges a document from the bucket (no tombstone)
func (db *Database) Purge(key string) error {
	if db.UseXattrs() {
		return db.Bucket.DeleteWithXattr(key, base.SyncXattrName)
	} else {
		return db.Bucket.Delete(key)
	}
}

//////// CHANNELS:

// Calls the JS sync function to assign the doc to channels, grant users
// access to channels, and reject invalid documents.
func (db *Database) getChannelsAndAccess(doc *Document, body Body, revID string) (
	result base.Set,
	access channels.AccessMap,
	roles channels.AccessMap,
	expiry *uint32,
	oldJson string,
	err error) {
	base.DebugfCtx(db.Ctx, base.KeyCRUD, "Invoking sync on doc %q rev %s", base.UD(doc.ID), body[BodyRev])

	// Get the parent revision, to pass to the sync function:
	var oldJsonBytes []byte
	if oldJsonBytes, err = db.getAncestorJSON(doc, revID); err != nil {
		return
	}
	oldJson = string(oldJsonBytes)

	if db.ChannelMapper != nil {
		// Call the ChannelMapper:
		startTime := time.Now()
		db.DbStats.CblReplicationPush().Add(base.StatKeySyncFunctionCount, 1)

		var output *channels.ChannelMapperOutput
		output, err = db.ChannelMapper.MapToChannelsAndAccess(body, oldJson,
			makeUserCtx(db.user))

		db.DbStats.CblReplicationPush().Add(base.StatKeySyncFunctionTime, time.Since(startTime).Nanoseconds())

		if err == nil {
			result = output.Channels
			access = output.Access
			roles = output.Roles
			expiry = output.Expiry
			err = output.Rejection
			if err != nil {
				base.InfofCtx(db.Ctx, base.KeyAll, "Sync fn rejected doc %q / %q --> %s", base.UD(doc.ID), base.UD(doc.NewestRev), err)
				base.DebugfCtx(db.Ctx, base.KeyAll, "    rejected doc %q / %q : new=%+v  old=%s", base.UD(doc.ID), base.UD(doc.NewestRev), base.UD(body), base.UD(oldJson))
				db.DbStats.StatsSecurity().Add(base.StatKeyNumDocsRejected, 1)
				if isAccessError(err) {
					db.DbStats.StatsSecurity().Add(base.StatKeyNumAccessErrors, 1)
				}
			} else if !validateAccessMap(access) || !validateRoleAccessMap(roles) {
				err = base.HTTPErrorf(500, "Error in JS sync function")
			}

		} else {
			base.WarnfCtx(db.Ctx, base.KeyAll, "Sync fn exception: %+v; doc = %s", err, base.UD(body))
			err = base.HTTPErrorf(500, "Exception in JS sync function")
		}

	} else {
		// No ChannelMapper so by default use the "channels" property:
		value := body["channels"]
		if value != nil {
			array := base.ValueToStringArray(value)
			result, err = channels.SetFromArray(array, channels.KeepStar)
		}
	}
	return result, access, roles, expiry, oldJson, err
}

// Creates a userCtx object to be passed to the sync function
func makeUserCtx(user auth.User) map[string]interface{} {
	if user == nil {
		return nil
	}
	return map[string]interface{}{
		"name":     user.Name(),
		"roles":    user.RoleNames(),
		"channels": user.InheritedChannels().AllChannels(),
	}
}

// Are the principal and role names in an AccessMap all valid?
func validateAccessMap(access channels.AccessMap) bool {
	for name := range access {
		principalName, _ := channels.AccessNameToPrincipalName(name)
		if !auth.IsValidPrincipalName(principalName) {
			base.Warnf(base.KeyAll, "Invalid principal name %q in access() or role() call", base.UD(principalName))
			return false
		}
	}
	return true
}

func validateRoleAccessMap(roleAccess channels.AccessMap) bool {
	if !validateAccessMap(roleAccess) {
		return false
	}
	for _, roles := range roleAccess {
		for rolename := range roles {
			if !auth.IsValidPrincipalName(rolename) {
				base.Warnf(base.KeyAll, "Invalid role name %q in role() call", base.UD(rolename))
				return false
			}
		}
	}
	return true
}

func isAccessError(err error) bool {
	return base.ContainsString(base.SyncFnAccessErrors, err.Error())
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeChannelsForPrincipal(princ auth.Principal) (channels.TimedSet, error) {

	if context.UseGlobalSequence() {
		return context.ComputeSequenceChannelsForPrincipal(princ)
	} else {
		return context.ComputeVbSequenceChannelsForPrincipal(princ)
	}
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeSequenceChannelsForPrincipal(princ auth.Principal) (channels.TimedSet, error) {

	key := princ.Name()
	if _, ok := princ.(auth.User); !ok {
		key = channels.RoleAccessPrefix + key // Roles are identified in access view by a "role:" prefix
	}

	results, err := context.QueryAccess(key)
	if err != nil {
		base.Warnf(base.KeyAll, "QueryAccess returned error: %v", err)
		return nil, err
	}

	var accessRow QueryAccessRow
	channelSet := channels.TimedSet{}
	for results.Next(&accessRow) {
		channelSet.Add(accessRow.Value)
	}

	closeErr := results.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	return channelSet, nil
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeVbSequenceChannelsForPrincipal(princ auth.Principal) (channels.TimedSet, error) {
	key := princ.Name()
	if _, ok := princ.(auth.User); !ok {
		key = channels.RoleAccessPrefix + key // Roles are identified in access view by a "role:" prefix
	}

	var vres struct {
		Rows []struct {
			Value channels.TimedSet
		}
	}

	opts := map[string]interface{}{"stale": false, "key": key}
	if verr := context.Bucket.ViewCustom(DesignDocSyncGateway(), ViewAccessVbSeq, opts, &vres); verr != nil {
		return nil, verr
	}

	channelSet := channels.TimedSet{}
	for _, row := range vres.Rows {
		channelSet.Add(row.Value)
	}
	return channelSet, nil
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeRolesForUser(user auth.User) (channels.TimedSet, error) {
	if context.UseGlobalSequence() {
		return context.ComputeSequenceRolesForUser(user)
	} else {
		return context.ComputeVbSequenceRolesForUser(user)
	}
}

// Recomputes the set of roles a User has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeSequenceRolesForUser(user auth.User) (channels.TimedSet, error) {
	results, err := context.QueryRoleAccess(user.Name())
	if err != nil {
		return nil, err
	}

	// Merge the TimedSets from the view result:
	roleChannelSet := channels.TimedSet{}
	var roleAccessRow QueryAccessRow
	for results.Next(&roleAccessRow) {
		roleChannelSet.Add(roleAccessRow.Value)
	}
	closeErr := results.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	return roleChannelSet, nil
}

// Recomputes the set of channels a User/Role has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeVbSequenceRolesForUser(user auth.User) (channels.TimedSet, error) {
	var vres struct {
		Rows []struct {
			Value channels.TimedSet
		}
	}

	opts := map[string]interface{}{"stale": false, "key": user.Name()}
	if verr := context.Bucket.ViewCustom(DesignDocSyncGateway(), ViewRoleAccessVbSeq, opts, &vres); verr != nil {
		return nil, verr
	}

	roleSet := channels.TimedSet{}
	for _, row := range vres.Rows {
		roleSet.Add(row.Value)
	}
	return roleSet, nil
}

// Checks whether a document has a mobile xattr.  Used when running in non-xattr mode to support no downtime upgrade.
func (context *DatabaseContext) checkForUpgrade(key string, unmarshalLevel DocumentUnmarshalLevel) (*Document, *sgbucket.BucketDocument) {
	// If we are using xattrs or Couchbase Server doesn't support them, an upgrade isn't going to be in progress
	if context.UseXattrs() || !context.Bucket.IsSupported(sgbucket.BucketFeatureXattrs) {
		return nil, nil
	}

	doc, rawDocument, err := context.GetDocWithXattr(key, unmarshalLevel)
	if err != nil || doc == nil || !doc.HasValidSyncData(context.writeSequences()) {
		return nil, nil
	}
	return doc, rawDocument
}

//////// REVS_DIFF:

// Given a document ID and a set of revision IDs, looks up which ones are not known. Returns an
// array of the unknown revisions, and an array of known revisions that might be recent ancestors.
func (db *Database) RevDiff(docid string, revids []string) (missing, possible []string) {
	if strings.HasPrefix(docid, "_design/") && db.user != nil {
		return // Users can't upload design docs, so ignore them
	}

	var history RevTree

	if db.UseXattrs() {
		var xattrValue []byte
		cas, err := db.Bucket.GetXattr(docid, base.SyncXattrName, &xattrValue)

		if err != nil {
			if !base.IsDocNotFoundError(err) {
				base.WarnfCtx(db.Ctx, base.KeyAll, "RevDiff(%q) --> %T %v", base.UD(docid), err, err)
			}
			missing = revids
			return
		}
		doc, err := unmarshalDocumentWithXattr(docid, nil, xattrValue, cas, DocUnmarshalSync)
		if err != nil {
			base.ErrorfCtx(db.Ctx, base.KeyAll, "RevDiff(%q) Doc Unmarshal Failed: %T %v", base.UD(docid), err,
				err)
		}
		history = doc.History
	} else {
		doc, err := db.GetDocument(docid, DocUnmarshalSync)
		if err != nil {
			if !base.IsDocNotFoundError(err) {
				base.WarnfCtx(db.Ctx, base.KeyAll, "RevDiff(%q) --> %T %v", base.UD(docid), err, err)
				// If something goes wrong getting the doc, treat it as though it's nonexistent.
			}
			missing = revids
			return
		}
		history = doc.History
	}

	// Check each revid to see if it's in the doc's rev tree:
	revtree := history
	revidsSet := base.SetFromArray(revids)
	possibleSet := make(map[string]bool)
	for _, revid := range revids {
		if !revtree.contains(revid) {
			missing = append(missing, revid)
			// Look at the doc's leaves for a known possible ancestor:
			if gen, _ := ParseRevID(revid); gen > 1 {
				revtree.forEachLeaf(func(possible *RevInfo) {
					if !revidsSet.Contains(possible.ID) {
						possibleGen, _ := ParseRevID(possible.ID)
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
	ProposedRev_OK       ProposedRevStatus = 0   // Rev can be added without conflict
	ProposedRev_Exists   ProposedRevStatus = 304 // Rev already exists locally
	ProposedRev_Conflict ProposedRevStatus = 409 // Rev would cause conflict
	ProposedRev_Error    ProposedRevStatus = 500 // Error occurred reading local doc
)

// Given a docID/revID to be pushed by a client, check whether it can be added _without conflict_.
// This is used by the BLIP replication code in "allow_conflicts=false" mode.
func (db *Database) CheckProposedRev(docid string, revid string, parentRevID string) ProposedRevStatus {
	doc, err := db.GetDocument(docid, DocUnmarshalAll)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			base.WarnfCtx(db.Ctx, base.KeyAll, "CheckProposedRev(%q) --> %T %v", base.UD(docid), err, err)
			return ProposedRev_Error
		}
		// Doc doesn't exist locally; adding it is OK (even if it has a history)
		return ProposedRev_OK
	} else if doc.CurrentRev == revid {
		// Proposed rev already exists here:
		return ProposedRev_Exists
	} else if doc.CurrentRev == parentRevID {
		// Proposed rev's parent is my current revision; OK to add:
		return ProposedRev_OK
	} else if parentRevID == "" && doc.History[doc.CurrentRev].Deleted {
		// Proposed rev has no parent and doc is currently deleted; OK to add:
		return ProposedRev_OK
	} else {
		// Parent revision mismatch, so this is a conflict:
		return ProposedRev_Conflict
	}
}

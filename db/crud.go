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
	"bytes"
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

// ErrForbidden is returned when the user requests a document without a revision that they do not have access to.
// this is different from a client specifically requesting a revision they know about, which are treated as a _removal.
var ErrForbidden = base.HTTPErrorf(403, "forbidden")

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
		if !doc.HasValidSyncData() {
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

		if !doc.HasValidSyncData() {
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
		if err := base.JSONUnmarshal(rawDocBytes, &docRoot); err != nil {
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
func (db *Database) GetRev(docid, revid string, history bool, attachmentsSince []string) (*DocumentRevision, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}

	rev, err := db.getRev(docid, revid, maxHistory, nil)
	if err != nil {
		return nil, err
	}

	return rev, nil
}

// Returns the body of the current revision of a document
func (db *Database) Get1xBody(docid string) (Body, error) {
	return db.Get1xRevBody(docid, "", false, nil)
}

// Get Rev with all-or-none history based on specified 'history' flag
func (db *Database) Get1xRevBody(docid, revid string, history bool, attachmentsSince []string) (Body, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}

	return db.Get1xRevBodyWithHistory(docid, revid, maxHistory, nil, attachmentsSince, false)
}

// Retrieves rev with request history specified as collection of revids (historyFrom)
func (db *Database) Get1xRevBodyWithHistory(docid, revid string, maxHistory int, historyFrom []string, attachmentsSince []string, showExp bool) (Body, error) {
	rev, err := db.getRev(docid, revid, maxHistory, historyFrom)
	if err != nil {
		return nil, err
	}

	// RequestedHistory is the _revisions returned in the body.  Avoids mutating revision.History, in case it's needed
	// during attachment processing below
	requestedHistory := rev.History
	if maxHistory == 0 {
		requestedHistory = nil
	}
	if requestedHistory != nil {
		_, requestedHistory = trimEncodedRevisionsToAncestor(requestedHistory, historyFrom, maxHistory)
	}

	return rev.Mutable1xBody(db, requestedHistory, attachmentsSince, showExp)
}

// Underlying revision retrieval used by Get1xRevBody, Get1xRevBodyWithHistory, GetRevCopy.
// Returns the body of a revision of a document. Uses the revision cache.
// * revid may be "", meaning the current revision.
// * maxHistory is >0 if the caller wants a revision history; it's the max length of the history.
// * historyFrom is an optional list of revIDs the client already has. If any of these are found
//   in the revision's history, it will be trimmed after that revID.
// * attachmentsSince is nil to return no attachment bodies, otherwise a (possibly empty) list of
//   revisions for which the client already has attachments and doesn't need bodies. Any attachment
//   that hasn't changed since one of those revisions will be returned as a stub.
func (db *Database) getRev(docid, revid string, maxHistory int, historyFrom []string) (revision *DocumentRevision, err error) {
	if revid != "" {
		// Get a specific revision body and history from the revision cache
		// (which will load them if necessary, by calling revCacheLoader, above)
		revision, err = db.revisionCache.Get(docid, revid)
	} else {
		// No rev ID given, so load active revision
		revision, err = db.revisionCache.GetActive(docid)
	}

	if err != nil {
		return nil, err
	}

	if revision == nil {
		return nil, base.HTTPErrorf(404, "missing")
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

	isAuthorized, redactedRev := db.authorizeUserForChannels(docid, revision.RevID, revision.Channels, revision.Deleted, requestedHistory)
	if !isAuthorized {
		if revid == "" {
			return nil, ErrForbidden
		}
		return redactedRev, nil
	}

	if revision.Deleted && revid == "" {
		return nil, base.HTTPErrorf(404, "deleted")
	}

	return revision, nil
}

// GetDelta attempts to return the delta between fromRevId and toRevId.  If the delta can't be generated,
// returns nil.
func (db *Database) GetDelta(docID, fromRevID, toRevID string) (delta *RevisionDelta, redactedRev *DocumentRevision, err error) {

	if docID == "" || fromRevID == "" || toRevID == "" {
		return nil, nil, nil
	}

	fromRevision, err := db.revisionCache.Get(docID, fromRevID)

	// If both body and delta are not available for fromRevId, the delta can't be generated
	if fromRevision.BodyBytes == nil && fromRevision.Delta == nil {
		return nil, nil, err
	}

	// If delta is found, check whether it is a delta for the toRevID we want
	if fromRevision.Delta != nil {
		if fromRevision.Delta.ToRevID == toRevID {

			isAuthorized, redactedBody := db.authorizeUserForChannels(docID, toRevID, fromRevision.Delta.ToChannels, fromRevision.Delta.ToDeleted, encodeRevisions(fromRevision.Delta.RevisionHistory))
			if !isAuthorized {
				return nil, redactedBody, nil
			}

			// Case 2a. 'some rev' is the rev we're interested in - return the delta
			db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaCacheHits, 1)
			return fromRevision.Delta, nil, nil
		} else {
			// TODO: Recurse and merge deltas when gen(revCacheDelta.toRevID) < gen(toRevId)
			// until then, fall through to generating delta for given rev pair
		}
	}

	// Delta is unavailable, but the body is available.
	if fromRevision.BodyBytes != nil {

		db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaCacheMisses, 1)
		toRevision, err := db.revisionCache.Get(docID, toRevID)
		if err != nil {
			return nil, nil, err
		}

		deleted := toRevision.Deleted
		isAuthorized, redactedBody := db.authorizeUserForChannels(docID, toRevID, toRevision.Channels, deleted, toRevision.History)
		if !isAuthorized {
			return nil, redactedBody, nil
		}

		// If the revision we're generating a delta to is a tombstone, mark it as such and don't bother generating a delta
		if deleted {
			revCacheDelta := newRevCacheDelta([]byte(`{}`), fromRevID, toRevision, deleted)
			db.revisionCache.UpdateDelta(docID, fromRevID, revCacheDelta)
			return revCacheDelta, nil, nil
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
			fromBodyCopy[BodyAttachments] = map[string]interface{}(fromRevision.Attachments)
		}
		if toRevision.Attachments != nil {
			toBodyCopy[BodyAttachments] = map[string]interface{}(toRevision.Attachments)
		}

		deltaBytes, err := base.Diff(fromBodyCopy, toBodyCopy)
		if err != nil {
			return nil, nil, err
		}
		revCacheDelta := newRevCacheDelta(deltaBytes, fromRevID, toRevision, deleted)

		// Write the newly calculated delta back into the cache before returning
		db.revisionCache.UpdateDelta(docID, fromRevID, revCacheDelta)
		return revCacheDelta, nil, nil
	}

	return nil, nil, nil
}

func (db *Database) authorizeUserForChannels(docID, revID string, channels base.Set, isDeleted bool, history Revisions) (isAuthorized bool, redactedRev *DocumentRevision) {
	if db.user != nil {
		if err := db.user.AuthorizeAnyChannel(channels); err != nil {
			// On access failure, return (only) the doc history and deletion/removal
			// status instead of returning an error. For justification see the comment in
			// the getRevFromDoc method, below
			redactedRev = &DocumentRevision{
				DocID:   docID,
				RevID:   revID,
				History: history,
				Deleted: isDeleted,
			}
			if isDeleted {
				// Deletions are denoted by the deleted message property during 2.x replication
				redactedRev.BodyBytes = []byte(`{}`)
			} else {
				// ... but removals are still denoted by the _removed property in the body, even for 2.x replication
				redactedRev.BodyBytes = []byte(`{"` + BodyRemoved + `":true}`)
			}
			return false, redactedRev
		}
	}

	return true, nil
}

// Returns the body of a revision of a document, as well as the document's current channels
// and the user/roles it grants channel access to.
func (db *Database) Get1xRevAndChannels(docid string, revid string, listRevisions bool) (bodyBytes []byte, channels channels.ChannelMap, access UserAccessMap, roleAccess UserAccessMap, flags uint8, sequence uint64, gotRevID string, removed bool, err error) {
	doc, err := db.GetDocument(docid, DocUnmarshalAll)
	if doc == nil {
		return
	}
	bodyBytes, removed, err = db.get1xRevFromDoc(doc, revid, listRevisions)
	if err != nil {
		return
	}
	channels = doc.Channels
	access = doc.Access
	roleAccess = doc.RoleAccess
	sequence = doc.Sequence
	flags = doc.Flags
	gotRevID = doc.RevID
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
func (db *DatabaseContext) getRevision(doc *Document, revid string) ([]byte, error) {
	var bodyBytes []byte
	var err error

	bodyBytes = doc.getRevisionBodyJSON(revid, db.RevisionBodyLoader)

	// No inline body, so look for separate doc:
	if bodyBytes == nil {
		if !doc.History.contains(revid) {
			return nil, base.HTTPErrorf(404, "missing")
		}

		bodyBytes, err = db.getOldRevisionJSON(doc.ID, revid)
		if err != nil {
			return nil, err
		}
	}

	if doc.CurrentRev == revid && doc.Attachments != nil {
		bodyBytes, err = base.InjectJSONProperties(bodyBytes, base.KVPair{
			Key: BodyAttachments,
			Val: doc.Attachments,
		})
		if err != nil {
			return nil, err
		}
	}

	return bodyBytes, nil
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
// If the user is not authorized to see the specific revision they asked for,
// instead returns a minimal deletion or removal revision to let them know it's gone.
func (db *Database) get1xRevFromDoc(doc *Document, revid string, listRevisions bool) (bodyBytes []byte, removed bool, err error) {
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
			bodyBytes = []byte(`{}`)
		} else {
			bodyBytes = []byte(`{"` + BodyRemoved + `":true}`)
			removed = true
		}
	} else {
		if revid == "" {
			revid = doc.CurrentRev
			if doc.History[revid].Deleted == true {
				return nil, false, base.HTTPErrorf(404, "deleted")
			}
		}
		if bodyBytes, err = db.getRevision(doc, revid); err != nil {
			return nil, false, err
		}
	}

	kvPairs := []base.KVPair{
		{Key: BodyId, Val: doc.ID},
		{Key: BodyRev, Val: revid},
	}

	if doc.History[revid].Deleted {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
	}

	if listRevisions {
		validatedHistory, getHistoryErr := doc.History.getHistory(revid)
		if getHistoryErr != nil {
			return nil, removed, getHistoryErr
		}
		kvPairs = append(kvPairs, base.KVPair{Key: BodyRevisions, Val: encodeRevisions(validatedHistory)})
	}

	bodyBytes, err = base.InjectJSONProperties(bodyBytes, kvPairs...)
	if err != nil {
		return nil, removed, err
	}

	return bodyBytes, removed, nil
}

// Returns the body and rev ID of the asked-for revision or the most recent available ancestor.
func (db *Database) getAvailableRev(doc *Document, revid string) ([]byte, string, error) {
	for ; revid != ""; revid = doc.History[revid].Parent {
		if bodyBytes, _ := db.getRevision(doc, revid); bodyBytes != nil {
			return bodyBytes, revid, nil
		}
	}
	return nil, "", base.HTTPErrorf(404, "missing")
}

// Returns the 1x-style body of the asked-for revision or the most recent available ancestor.
func (db *Database) getAvailable1xRev(doc *Document, revid string) ([]byte, error) {
	bodyBytes, ancestorRevID, err := db.getAvailableRev(doc, revid)
	if err != nil {
		return nil, err
	}

	kvPairs := []base.KVPair{
		{Key: BodyId, Val: doc.ID},
		{Key: BodyRev, Val: ancestorRevID},
	}

	// TODO: do we need _deleted here too? - maybe not because by definition a rev is not available if deleted
	if doc.CurrentRev == revid && doc.Attachments != nil {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: doc.Attachments})
	}

	bodyBytes, err = base.InjectJSONProperties(bodyBytes, kvPairs...)
	if err != nil {
		return nil, err
	}

	return bodyBytes, nil
}

// Returns the attachments of the asked-for revision or the most recent available ancestor.
// Returns nil if no attachments or ancestors are found.
func (db *Database) getAvailableRevAttachments(doc *Document, revid string) (ancestorAttachments AttachmentsMeta, foundAncestor bool) {
	bodyBytes, ancestorRevID, err := db.getAvailableRev(doc, revid)
	if err != nil {
		return nil, false
	}

	// If the ancestor rev is the current rev, we can pull attachments directly from the doc
	if doc.CurrentRev == ancestorRevID {
		return doc.Attachments, true
	}

	// Otherwise, we need to go and extract them from a backup revision's stamped _attachments property

	// exit early if we know we have no attachments with a simple byte-contains check
	if !bytes.Contains(bodyBytes, []byte(BodyAttachments)) {
		return nil, true
	}

	// Unmarshal attachments into a struct
	var parentAttachmentsStruct struct {
		Attachments AttachmentsMeta `json:"_attachments"`
	}
	if err := base.JSONUnmarshal(bodyBytes, &parentAttachmentsStruct); err != nil {
		base.Warnf(base.KeyAll, "Error unmarshaling attachments metadata: %s", err)
		return nil, true
	}

	return parentAttachmentsStruct.Attachments, true
}

// Moves a revision's ancestor's body out of the document object and into a separate db doc.
func (db *Database) backupAncestorRevs(doc *Document, newDoc *Document) {
	newBodyBytes, err := newDoc.BodyBytes()
	if err != nil {
		base.Warnf(base.KeyAll, "Error getting body bytes when backing up ancestor revs")
		return
	}

	// Find an ancestor that still has JSON in the document:
	var json []byte
	ancestorRevId := newDoc.RevID
	for {
		if ancestorRevId = doc.History.getParent(ancestorRevId); ancestorRevId == "" {
			// No ancestors with JSON found.  Check if we need to back up current rev for delta sync, then return
			db.backupRevisionJSON(doc.ID, newDoc.RevID, "", newBodyBytes, nil, doc.Attachments)
			return
		} else if json = doc.getRevisionBodyJSON(ancestorRevId, db.RevisionBodyLoader); json != nil {
			break
		}
	}

	// Back up the revision JSON as a separate doc in the bucket:
	db.backupRevisionJSON(doc.ID, newDoc.RevID, ancestorRevId, newBodyBytes, json, doc.Attachments)

	// Nil out the ancestor rev's body in the document struct:
	if ancestorRevId == doc.CurrentRev {
		doc.RemoveBody()
	} else {
		doc.removeRevisionBody(ancestorRevId)
	}
}

//////// UPDATING DOCUMENTS:

func (db *Database) OnDemandImportForWrite(docid string, doc *Document, deleted bool) error {

	// Check whether the doc requiring import is an SDK delete
	isDelete := false
	if doc.Body() == nil {
		isDelete = true
	} else {
		isDelete = deleted
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

	delete(body, BodyId)

	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body[BodyRev].(string)
	generation, _ := ParseRevID(matchRev)
	if generation < 0 {
		return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++
	delete(body, BodyRev)

	// Not extracting it yet because we need this property around to generate a rev ID
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
	newDoc.DocAttachments = GetBodyAttachments(body)
	delete(body, BodyAttachments)

	delete(body, BodyRevisions)

	allowImport := db.UseXattrs()
	doc, newRevID, err = db.updateAndReturnDoc(newDoc.ID, allowImport, expiry, nil, func(doc *Document) (resultDoc *Document, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {

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
			err := db.OnDemandImportForWrite(newDoc.ID, doc, deleted)
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
		newAttachments, err := db.storeAttachments(doc, newDoc.DocAttachments, generation, matchRev, nil)
		if err != nil {
			return nil, nil, nil, err
		}

		// Make up a new _rev, and add it to the history:
		newRev, err := createRevID(generation, matchRev, body)
		if err != nil {
			return nil, nil, nil, err
		}

		// We needed to keep _deleted around in the body until we generated a rev ID, but now we can ditch it.
		delete(body, BodyDeleted)
		// and now we can finally update the newDoc body to be without any special properties
		newDoc.UpdateBody(body)

		if err := doc.History.addRevision(newDoc.ID, RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted}); err != nil {
			base.InfofCtx(db.Ctx, base.KeyCRUD, "Failed to add revision ID: %s, for doc: %s, error: %v", newRev, base.UD(docid), err)
			return nil, nil, nil, base.ErrRevTreeAddRevFailure
		}

		// move _attachment metadata to syncdata of doc after rev-id generation
		doc.SyncData.Attachments = newDoc.DocAttachments
		newDoc.RevID = newRev
		newDoc.Deleted = deleted

		return newDoc, newAttachments, nil, nil
	})

	body[BodyId] = docid
	body[BodyRev] = newRevID

	return newRevID, doc, err
}

// Adds an existing revision to a document along with its history (list of rev IDs.)
func (db *Database) PutExistingRev(newDoc *Document, docHistory []string, noConflicts bool) (doc *Document, newRevID string, err error) {
	newRev := docHistory[0]
	generation, _ := ParseRevID(newRev)
	if generation < 0 {
		return nil, "", base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}

	allowImport := db.UseXattrs()
	doc, _, err = db.updateAndReturnDoc(newDoc.ID, allowImport, newDoc.DocExpiry, nil, func(doc *Document) (resultDoc *Document, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {
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
			err := db.OnDemandImportForWrite(newDoc.ID, doc, newDoc.Deleted)
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
			base.DebugfCtx(db.Ctx, base.KeyCRUD, "PutExistingRevWithBody(%q): No new revisions to add", base.UD(newDoc.ID))
			newDoc.RevID = newRev
			return nil, nil, nil, base.ErrUpdateCancel // No new revisions to add
		}

		// Conflict-free mode check
		if db.IsIllegalConflict(doc, parent, newDoc.Deleted, noConflicts) {
			return nil, nil, nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
		}

		// Add all the new-to-me revisions to the rev tree:
		for i := currentRevIndex - 1; i >= 0; i-- {
			err := doc.History.addRevision(newDoc.ID,
				RevInfo{
					ID:      docHistory[i],
					Parent:  parent,
					Deleted: i == 0 && newDoc.Deleted})

			if err != nil {
				return nil, nil, nil, err
			}
			parent = docHistory[i]
		}

		// Process the attachments, replacing bodies with digests.
		parentRevID := doc.History[newRev].Parent
		newAttachments, err := db.storeAttachments(doc, newDoc.DocAttachments, generation, parentRevID, docHistory)
		if err != nil {
			return nil, nil, nil, err
		}

		doc.SyncData.Attachments = newDoc.DocAttachments
		newDoc.RevID = newRev

		return newDoc, newAttachments, nil, nil
	})

	return doc, newRev, err
}

func (db *Database) PutExistingRevWithBody(docid string, body Body, docHistory []string, noConflicts bool) (doc *Document, err error) {
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

	newDoc.DocAttachments = GetBodyAttachments(body)
	delete(body, BodyAttachments)
	newDoc.UpdateBody(body)

	doc, newRevID, putExistingRevErr := db.PutExistingRev(newDoc, docHistory, noConflicts)

	// Callers expect the below properties to be in the body even if PutExistingRev returns error
	body[BodyId] = docid
	body[BodyRev] = newRevID

	if deleted {
		body[BodyDeleted] = deleted
	}

	if putExistingRevErr != nil {
		return nil, putExistingRevErr
	}

	return doc, err

}

func (db *Database) validateExistingDoc(doc *Document, importAllowed, docExists bool) error {
	if !importAllowed && docExists && !doc.HasValidSyncData() {
		return base.HTTPErrorf(409, "Not imported")
	}
	return nil
}

func validateNewBody(body Body) error {
	// Reject a body that contains the "_removed" property, this means that the user
	// is trying to update a document they do not have read access to.
	if body[BodyRemoved] != nil {
		return base.HTTPErrorf(http.StatusNotFound, "Document revision is not accessible")
	}

	// Reject bodies containing user special properties for compatibility with CouchDB
	if containsUserSpecialProperties(body) {
		return base.HTTPErrorf(400, "user defined top level properties beginning with '_' are not allowed in document body")
	}
	return nil
}

func (doc *Document) updateWinningRevAndSetDocFlags() {
	var branched, inConflict bool
	doc.CurrentRev, branched, inConflict = doc.History.winningRevision()
	doc.setFlag(channels.Deleted, doc.History[doc.CurrentRev].Deleted)
	doc.setFlag(channels.Conflict, inConflict)
	doc.setFlag(channels.Branched, branched)
	if doc.hasFlag(channels.Deleted) {
		doc.SyncData.TombstonedAt = time.Now().Unix()
	} else {
		doc.SyncData.TombstonedAt = 0
	}
}

func (db *Database) storeOldBodyInRevTreeAndUpdateCurrent(doc *Document, prevCurrentRev string, newRevID string, newDoc *Document) {
	if doc.CurrentRev != prevCurrentRev && prevCurrentRev != "" {
		// Store the doc's previous body into the revision tree:
		oldBodyJson, marshalErr := doc.BodyBytes()
		if marshalErr != nil {
			base.WarnfCtx(db.Ctx, base.KeyAll, "Unable to marshal document body for storage in rev tree: %v", marshalErr)
		}

		var kvPairs []base.KVPair

		// Stamp _attachments into the old body we're about to backup
		// We need to do a revpos check here because doc actually contains the new attachments
		if len(doc.SyncData.Attachments) > 0 {
			prevCurrentRevGen, _ := ParseRevID(prevCurrentRev)
			bodyAtts := make(AttachmentsMeta)
			for attName, attMeta := range doc.SyncData.Attachments {
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
				}
			}
			if len(bodyAtts) > 0 {
				kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: bodyAtts})
			}
		}

		if doc.Deleted {
			kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
		}

		// Stamp _attachments and _deleted into rev tree bodies
		oldBodyJson, marshalErr = base.InjectJSONProperties(oldBodyJson, kvPairs...)
		if marshalErr != nil {
			base.WarnfCtx(db.Ctx, base.KeyAll, "Unable to marshal document body properties for storage in rev tree: %v", marshalErr)
		}
		doc.setNonWinningRevisionBody(prevCurrentRev, oldBodyJson, db.AllowExternalRevBodyStorage())
	}
	// Store the new revision body into the doc:
	doc.setRevisionBody(newRevID, newDoc, db.AllowExternalRevBodyStorage())

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
}

// Run the sync function on the given document and body. Need to inject the document ID and rev ID temporarily to run
// the sync function.
func (db *Database) runSyncFn(doc *Document, body Body, newRevId string) (*uint32, string, base.Set, channels.AccessMap, channels.AccessMap, error) {
	channelSet, access, roles, syncExpiry, oldBody, err := db.getChannelsAndAccess(doc, body, newRevId)
	if err != nil {
		return nil, ``, nil, nil, nil, err
	}
	db.checkDocChannelsAndGrantsLimits(doc.ID, channelSet, access, roles)
	return syncExpiry, oldBody, channelSet, access, roles, nil
}

func (db *Database) recalculateSyncFnForActiveRev(doc *Document, newRevID string) (channelSet base.Set, access, roles channels.AccessMap, syncExpiry *uint32, oldBodyJSON string, err error) {
	// In some cases an older revision might become the current one. If so, get its
	// channels & access, for purposes of updating the doc:
	curBodyBytes, err := db.getAvailable1xRev(doc, doc.CurrentRev)
	if err != nil {
		return
	}

	var curBody Body
	err = curBody.Unmarshal(curBodyBytes)
	if err != nil {
		return
	}

	if curBody != nil {
		base.DebugfCtx(db.Ctx, base.KeyCRUD, "updateDoc(%q): Rev %q causes %q to become current again",
			base.UD(doc.ID), newRevID, doc.CurrentRev)
		channelSet, access, roles, syncExpiry, oldBodyJSON, err = db.getChannelsAndAccess(doc, curBody, doc.CurrentRev)
		if err != nil {
			return
		}
	} else {
		// Shouldn't be possible (CurrentRev is a leaf so won't have been compacted)
		base.WarnfCtx(db.Ctx, base.KeyAll, "updateDoc(%q): Rev %q missing, can't call getChannelsAndAccess "+
			"on it (err=%v)", base.UD(doc.ID), doc.CurrentRev, err)
		channelSet = nil
		access = nil
		roles = nil
	}
	return
}

func (db *Database) addAttachments(newAttachments AttachmentData) error {
	// Need to check and add attachments here to ensure the attachment is within size constraints
	err := db.setAttachments(newAttachments)
	if err != nil {
		if err.Error() == "document value was too large" {
			err = base.HTTPErrorf(http.StatusRequestEntityTooLarge, "Attachment too large")
		} else {
			err = errors.Wrap(err, "Error adding attachment")
		}
	}
	return err
}

// Sequence processing :
// Assigns provided sequence to the document
// Update unusedSequences in the event that there is a conflict and we have to provide a new sequence number
// Update and prune RecentSequences
func (db *Database) assignSequence(docSequence uint64, doc *Document, unusedSequences []uint64) ([]uint64, error) {
	var err error

	// Assign the next sequence number, for _changes feed.
	// Be careful not to request a second sequence # on a retry if we don't need one.
	if docSequence <= doc.Sequence {
		if docSequence > 0 {
			// Oops: we're on our second iteration thanks to a conflict, but the sequence
			// we previously allocated is unusable now. We have to allocate a new sequence
			// instead, but we add the unused one(s) to the document so when the changeCache
			// reads the doc it won't freak out over the break in the sequence numbering.
			base.InfofCtx(db.Ctx, base.KeyCache, "updateDoc %q: Unused sequence #%d", base.UD(doc.ID), docSequence)
			unusedSequences = append(unusedSequences, docSequence)
		}

		for {
			if docSequence, err = db.sequences.nextSequence(); err != nil {
				return unusedSequences, err
			}

			if docSequence > doc.Sequence {
				break
			} else {
				releaseErr := db.sequences.releaseSequence(docSequence)
				if releaseErr != nil {
					base.Warnf(base.KeyCRUD, "Error returned when releasing sequence %d. Falling back to skipped sequence handling.  Error:%v", docSequence, err)
				}
			}
		}
		// Could add a db.Sequences.nextSequenceGreaterThan(doc.Sequence) to push the work down into the sequence allocator
		//  - sequence allocator is responsible for releasing unused sequences, could optimize to do that in bulk if needed
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
		stableSequence := db.changeCache.GetStableSequence(doc.ID).Seq
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

	return unusedSequences, nil
}

func (doc *Document) updateExpiry(syncExpiry, updatedExpiry *uint32, expiry uint32) (finalExp *uint32) {
	if syncExpiry != nil {
		finalExp = syncExpiry
	} else if updatedExpiry != nil {
		finalExp = updatedExpiry
	} else {
		finalExp = &expiry
	}

	doc.UpdateExpiry(*finalExp)

	return finalExp

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

func (db *Database) documentUpdateFunc(docExists bool, doc *Document, allowImport bool, previousDocSequenceIn uint64, unusedSequences []uint64, callback updateAndReturnDocCallback, expiry uint32) (retSyncFuncExpiry *uint32, retNewRevID string, retStoredDoc *Document, retOldBodyJSON string, retUnusedSequences []uint64, changedAccessPrincipals []string, changedRoleAccessUsers []string, err error) {

	err = db.validateExistingDoc(doc, allowImport, docExists)
	if err != nil {
		return
	}

	// Invoke the callback to update the document and return a new revision body:
	newDoc, newAttachments, updatedExpiry, err := callback(doc)
	if err != nil {
		return
	}

	syncFnBody := newDoc.GetMutableBody()

	// FIXME: seems a bit late to do this. Could we move it earlier?
	err = validateNewBody(syncFnBody)
	if err != nil {
		return
	}

	// FIXME: Used to strip special properties here... do we still need to?

	newRevID := newDoc.RevID
	prevCurrentRev := doc.CurrentRev
	doc.updateWinningRevAndSetDocFlags()
	db.storeOldBodyInRevTreeAndUpdateCurrent(doc, prevCurrentRev, newRevID, newDoc)

	syncFnBody[BodyId] = doc.ID
	syncFnBody[BodyRev] = newRevID
	syncFnBody[BodyDeleted] = newDoc.Deleted

	syncExpiry, oldBodyJSON, channelSet, access, roles, err := db.runSyncFn(doc, syncFnBody, newRevID)
	if err != nil {
		return
	}

	if len(channelSet) > 0 {
		doc.History[newRevID].Channels = channelSet
	}

	err = db.addAttachments(newAttachments)
	if err != nil {
		return
	}

	db.backupAncestorRevs(doc, newDoc)

	unusedSequences, err = db.assignSequence(previousDocSequenceIn, doc, unusedSequences)
	if err != nil {
		return
	}

	if doc.CurrentRev != prevCurrentRev {
		// Most of the time this update will change the doc's current rev. (The exception is
		// if the new rev is a conflict that doesn't win the revid comparison.) If so, we
		// need to update the doc's top-level Channels and Access properties to correspond
		// to the current rev's state.
		if newRevID != doc.CurrentRev {
			channelSet, access, roles, syncExpiry, oldBodyJSON, err = db.recalculateSyncFnForActiveRev(doc, newRevID)
		}
		_, err = doc.updateChannels(channelSet)
		if err != nil {
			return
		}
		changedAccessPrincipals = doc.Access.updateAccess(doc, access)
		changedRoleAccessUsers = doc.RoleAccess.updateAccess(doc, roles)
	} else {
		base.DebugfCtx(db.Ctx, base.KeyCRUD, "updateDoc(%q): Rev %q leaves %q still current",
			base.UD(doc.ID), newRevID, prevCurrentRev)
	}

	// Prune old revision history to limit the number of revisions:
	if pruned := doc.pruneRevisions(db.RevsLimit, doc.CurrentRev); pruned > 0 {
		base.DebugfCtx(db.Ctx, base.KeyCRUD, "updateDoc(%q): Pruned %d old revisions", base.UD(doc.ID), pruned)
	}

	updatedExpiry = doc.updateExpiry(syncExpiry, updatedExpiry, expiry)
	err = doc.persistModifiedRevisionBodies(db.Bucket)
	if err != nil {
		return
	}

	doc.TimeSaved = time.Now()
	return updatedExpiry, newRevID, newDoc, oldBodyJSON, unusedSequences, changedAccessPrincipals, changedRoleAccessUsers, err
}

// Function type for the callback passed into updateAndReturnDoc
type updateAndReturnDocCallback func(*Document) (resultDoc *Document, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error)

// Calling updateAndReturnDoc directly allows callers to:
//   1. Receive the updated document body in the response
//   2. Specify the existing document body/xattr/cas, to avoid initial retrieval of the doc in cases that the current contents are already known (e.g. import).
//      On cas failure, the document will still be reloaded from the bucket as usual.
func (db *Database) updateAndReturnDoc(docid string, allowImport bool, expiry uint32, existingDoc *sgbucket.BucketDocument, callback updateAndReturnDocCallback) (doc *Document, newRevID string, err error) {

	key := realDocID(docid)
	if key == "" {
		return nil, "", base.HTTPErrorf(400, "Invalid doc ID")
	}

	var storedDoc *Document
	var changedAccessPrincipals, changedRoleAccessUsers []string // Returned by documentUpdateFunc
	var docSequence uint64                                       // Must be scoped outside callback, used over multiple iterations
	var unusedSequences []uint64                                 // Must be scoped outside callback, used over multiple iterations
	var oldBodyJSON string                                       // Stores previous revision body for use by DocumentChangeEvent

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
			docExists := currentValue != nil
			syncFuncExpiry, newRevID, storedDoc, oldBodyJSON, unusedSequences, changedAccessPrincipals, changedRoleAccessUsers, err = db.documentUpdateFunc(docExists, doc, allowImport, docSequence, unusedSequences, callback, expiry)
			if err != nil {
				return
			}

			docSequence = doc.Sequence
			inConflict = doc.hasFlag(channels.Conflict)
			// Return the new raw document value for the bucket to store.
			raw, err = doc.MarshalBodyAndSync()
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
			docExists := currentValue != nil
			syncFuncExpiry, newRevID, storedDoc, oldBodyJSON, unusedSequences, changedAccessPrincipals, changedRoleAccessUsers, err = db.documentUpdateFunc(docExists, doc, allowImport, docSequence, unusedSequences, callback, expiry)
			if err != nil {
				return
			}
			docSequence = doc.Sequence
			inConflict = doc.hasFlag(channels.Conflict)

			currentRevFromHistory, ok := doc.History[doc.CurrentRev]
			if !ok {
				err = base.RedactErrorf("WriteUpdateWithXattr() not able to find revision (%v) in history of doc: %+v.  Cannot update doc.", doc.CurrentRev, base.UD(doc))
				return
			}

			deleteDoc = currentRevFromHistory.Deleted

			// Return the new raw document value for the bucket to store.
			raw, rawXattr, err = doc.MarshalWithXattr()
			docBytes = len(raw)

			// Warn when sync data is larger than a configured threshold
			if xattrBytesThreshold := db.Options.UnsupportedOptions.WarningThresholds.XattrSize; xattrBytesThreshold != nil {
				xattrBytes = len(rawXattr)
				if uint32(xattrBytes) >= *xattrBytesThreshold {
					db.DbStats.StatsDatabase().Add(base.StatKeyWarnXattrSizeCount, 1)
					base.WarnfCtx(db.Ctx, base.KeyAll, "Doc id: %v sync metadata size: %d bytes exceeds %d bytes for sync metadata warning threshold", base.UD(doc.ID), xattrBytes, *xattrBytesThreshold)
				}
			}

			base.DebugfCtx(db.Ctx, base.KeyCRUD, "Saving doc (seq: #%d, id: %v rev: %v)", doc.Sequence, base.UD(doc.ID), doc.CurrentRev)
			return raw, rawXattr, deleteDoc, syncFuncExpiry, err
		})
		if err != nil {
			if err == base.ErrDocumentMigrated {
				base.DebugfCtx(db.Ctx, base.KeyCRUD, "Migrated document %q to use xattr.", base.UD(key))
			} else {
				base.DebugfCtx(db.Ctx, base.KeyCRUD, "Did not update document %q w/ xattr: %v", base.UD(key), err)
			}
		} else if doc != nil {
			doc.Cas = casOut
		}
	}

	// If the WriteUpdate didn't succeed, check whether there are unused, allocated sequences that need to be accounted for
	if err != nil {
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

		// Lazily marshal bytes for storage in revcache
		storedDocBytes, err := storedDoc.BodyBytes()
		if err != nil {
			return nil, "", err
		}

		revChannels := doc.History[newRevID].Channels
		documentRevision := DocumentRevision{
			DocID:       docid,
			RevID:       newRevID,
			BodyBytes:   storedDocBytes,
			History:     encodeRevisions(history),
			Channels:    revChannels,
			Attachments: doc.Attachments,
			Expiry:      doc.Expiry,
			Deleted:     doc.History[newRevID].Deleted,
		}
		db.revisionCache.Put(documentRevision)
		if db.EventMgr.HasHandlerForEvent(DocumentChange) {
			webhookJSON, err := doc.MarshalBodyForWebhook()
			if err != nil {
				base.Warnf(base.KeyAll, "Error marshalling doc with id %s and revid %s for webhook post: %v", base.UD(docid), base.UD(newRevID), err)
			} else {
				db.EventMgr.RaiseDocumentChangeEvent(webhookJSON, docid, oldBodyJSON, revChannels)
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
	db.MarkPrincipalsChanged(docid, newRevID, changedAccessPrincipals, changedRoleAccessUsers)
	return doc, newRevID, nil
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
	return context.ComputeSequenceChannelsForPrincipal(princ)
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
	return context.ComputeSequenceRolesForUser(user)
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
	if err != nil || doc == nil || !doc.HasValidSyncData() {
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

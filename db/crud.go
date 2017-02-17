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
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/go-couchbase"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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

// Lowest-level method that reads a document from the bucket.
func (db *DatabaseContext) GetDoc(docid string) (*document, error) {
	key := realDocID(docid)
	if key == "" {
		return nil, base.HTTPErrorf(400, "Invalid doc ID")
	}
	dbExpvars.Add("document_gets", 1)
	doc := newDocument(docid)
	_, err := db.Bucket.Get(key, doc)
	if err != nil {
		return nil, err
	} else if !doc.HasValidSyncData(db.writeSequences()) {
		return nil, base.HTTPErrorf(404, "Not imported")
	}
	return doc, nil
}

// This is the RevisionCacheLoaderFunc callback for the context's RevisionCache.
// Its job is to load a revision from the bucket when there's a cache miss.
func (context *DatabaseContext) revCacheLoader(id IDAndRev) (body Body, history Body, channels base.Set, err error) {
	var doc *document
	if doc, err = context.GetDoc(id.DocID); doc == nil {
		return
	}

	if body, err = context.getRevision(doc, id.RevID); err != nil {
		return
	}
	if doc.History[id.RevID].Deleted {
		body["_deleted"] = true
	}
	history = encodeRevisions(doc.History.getHistory(id.RevID))
	channels = doc.History[id.RevID].Channels
	return
}

// Returns the body of the current revision of a document
func (db *Database) Get(docid string) (Body, error) {
	return db.GetRevWithHistory(docid, "", 0, nil, nil, false)
}

// Shortcut for GetRevWithHistory
func (db *Database) GetRev(docid, revid string, history bool, attachmentsSince []string) (Body, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}
	return db.GetRevWithHistory(docid, revid, maxHistory, nil, attachmentsSince, false)
}

// Returns the body of a revision of a document. Uses the revision cache.
// * revid may be "", meaning the current revision.
// * maxHistory is >0 if the caller wants a revision history; it's the max length of the history.
// * historyFrom is an optional list of revIDs the client already has. If any of these are found
//   in the revision's history, it will be trimmed after that revID.
// * attachmentsSince is nil to return no attachment bodies, otherwise a (possibly empty) list of
//   revisions for which the client already has attachments and doesn't need bodies. Any attachment
//   that hasn't changed since one of those revisions will be returned as a stub.
func (db *Database) GetRevWithHistory(docid, revid string, maxHistory int, historyFrom []string, attachmentsSince []string, showExp bool) (Body, error) {
	var doc *document
	var body Body
	var revisions Body
	var inChannels base.Set
	var err error
	revIDGiven := (revid != "")
	if revIDGiven {
		// Get a specific revision body and history from the revision cache
		// (which will load them if necessary, by calling revCacheLoader, above)
		body, revisions, inChannels, err = db.revisionCache.Get(docid, revid)
		if body == nil {
			if err == nil {
				err = base.HTTPErrorf(404, "missing")
			}
			return nil, err
		}
		if maxHistory == 0 {
			revisions = nil
		}
	} else {
		// No rev ID given, so load doc and get its current revision:
		if doc, err = db.GetDoc(docid); doc == nil {
			return nil, err
		}
		revid = doc.CurrentRev
		if body, err = db.getRevision(doc, revid); err != nil {
			return nil, err
		}
		if doc.hasFlag(channels.Deleted) {
			body["_deleted"] = true
		}
		if maxHistory != 0 {
			revisions = encodeRevisions(doc.History.getHistory(revid))
		}
		inChannels = doc.History[revid].Channels
	}

	if revisions != nil {
		trimEncodedRevisionsToAncestor(revisions, historyFrom, maxHistory)
	}

	// Authorize the access:
	if db.user != nil {
		if err := db.user.AuthorizeAnyChannel(inChannels); err != nil {
			if !revIDGiven {
				return nil, base.HTTPErrorf(403, "forbidden")
			}
			// On access failure, return (only) the doc history and deletion/removal
			// status instead of returning an error. For justification see the comment in
			// the getRevFromDoc method, below
			deleted, _ := body["_deleted"].(bool)
			redactedBody := Body{"_id": docid, "_rev": revid}
			if deleted {
				redactedBody["_deleted"] = true
			} else {
				redactedBody["_removed"] = true
			}
			if revisions != nil {
				redactedBody["_revisions"] = revisions
			}
			return redactedBody, nil
		}
	}

	if !revIDGiven {
		if deleted, _ := body["_deleted"].(bool); deleted {
			return nil, base.HTTPErrorf(404, "deleted")
		}
	}

	// Add revision metadata:
	if revisions != nil {
		body["_revisions"] = revisions
	}

	if showExp {
		// If doc is nil (we got the rev from the rev cache), look up the doc to find the exp on the doc
		if doc == nil {
			if doc, err = db.GetDoc(docid); doc == nil {
				return nil, err
			}
		}
		if doc.Expiry != nil && !doc.Expiry.IsZero() {
			body["_exp"] = doc.Expiry.Format(time.RFC3339)
		}
	}

	// Add attachment bodies:
	if attachmentsSince != nil && len(BodyAttachments(body)) > 0 {
		minRevpos := 1
		if len(attachmentsSince) > 0 {
			if doc == nil { // if rev was in the cache, we don't have the document struct yet
				if doc, err = db.GetDoc(docid); doc == nil {
					return nil, err
				}
			}
			ancestor := doc.History.findAncestorFromSet(revid, attachmentsSince)
			if ancestor != "" {
				minRevpos, _ = ParseRevID(ancestor)
				minRevpos++
			}
		}
		body, err = db.loadBodyAttachments(body, minRevpos)
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

// Returns the body of the active revision of a document, as well as the document's current channels
// and the user/roles it grants channel access to.
func (db *Database) GetDocAndActiveRev(docid string) (populatedDoc *document, body Body, err error) {
	populatedDoc, err = db.GetDoc(docid)
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
	doc, err := db.GetDoc(docid)
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
	doc, err := db.GetDoc(docid)
	if doc == nil {
		return err
	}
	return db.authorizeDoc(doc, revid)
}

// Returns an HTTP 403 error if the User is not allowed to access any of this revision's channels.
func (db *Database) authorizeDoc(doc *document, revid string) error {
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
// This method adds the magic _id and _rev properties.
func (db *DatabaseContext) getRevision(doc *document, revid string) (Body, error) {
	var body Body
	if body = doc.getRevision(revid); body == nil {
		// No inline body, so look for separate doc:
		if !doc.History.contains(revid) {
			return nil, base.HTTPErrorf(404, "missing")
		} else if data, err := db.getOldRevisionJSON(doc.ID, revid); data == nil {
			return nil, err
		} else if err = json.Unmarshal(data, &body); err != nil {
			return nil, err
		}
	}
	body.FixJSONNumbers() // Make sure big ints won't get output in scientific notation
	body["_id"] = doc.ID
	body["_rev"] = revid
	return body, nil
}

// Gets a revision of a document as raw JSON.
// If it's obsolete it will be loaded from the database if possible.
// Does not add _id or _rev properties.
func (db *Database) getRevisionJSON(doc *document, revid string) ([]byte, error) {
	if body := doc.getRevisionJSON(revid); body != nil {
		return body, nil
	} else if !doc.History.contains(revid) {
		return nil, base.HTTPErrorf(404, "missing")
	} else {
		return db.getOldRevisionJSON(doc.ID, revid)
	}
}

// Gets the body of a revision's nearest ancestor, as raw JSON (without _id or _rev.)
// If no ancestor has any JSON, returns nil but no error.
func (db *Database) getAncestorJSON(doc *document, revid string) ([]byte, error) {
	for {
		if revid = doc.History.getParent(revid); revid == "" {
			return nil, nil
		} else if body, err := db.getRevisionJSON(doc, revid); body != nil {
			return body, nil
		} else if !base.IsDocNotFoundError(err) {
			return nil, err
		}
	}
}

// Returns the body of a revision given a document struct. Checks user access.
func (db *Database) getRevFromDoc(doc *document, revid string, listRevisions bool) (Body, error) {
	var body Body
	if err := db.authorizeDoc(doc, revid); err != nil {
		// As a special case, you don't need channel access to see a deletion revision,
		// otherwise the client's replicator can't process the deletion (since deletions
		// usually aren't on any channels at all!) But don't show the full body. (See #59)
		// Update: this applies to non-deletions too, since the client may have lost access to
		// the channel and gotten a "removed" entry in the _changes feed. It then needs to
		// incorporate that tombsone and for that it needs to see the _revisions property.
		if revid == "" || doc.History[revid] == nil /*|| !doc.History[revid].Deleted*/ {
			return nil, err
		}
		body = Body{"_id": doc.ID, "_rev": revid}
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
		body["_deleted"] = true
	}
	if listRevisions {
		history := doc.History.getHistory(revid)
		body["_revisions"] = encodeRevisions(history)
	}
	return body, nil
}

// Returns the body of the asked-for revision or the most recent available ancestor.
// Does NOT fill in _id, _rev, etc.
func (db *Database) getAvailableRev(doc *document, revid string) (Body, error) {
	for ; revid != ""; revid = doc.History[revid].Parent {
		if body, _ := db.getRevision(doc, revid); body != nil {
			return body, nil
		}
	}
	return nil, base.HTTPErrorf(404, "missing")
}

// Moves a revision's ancestor's body out of the document object and into a separate db doc.
func (db *Database) backupAncestorRevs(doc *document, revid string) error {
	// Find an ancestor that still has JSON in the document:
	var json []byte
	for {
		if revid = doc.History.getParent(revid); revid == "" {
			return nil // No ancestors with JSON found
		} else if json = doc.getRevisionJSON(revid); json != nil {
			break
		}
	}

	// Store the JSON as a separate doc in the bucket:
	if err := db.setOldRevisionJSON(doc.ID, revid, json); err != nil {
		// This isn't fatal since we haven't lost any information; just warn about it.
		base.Warn("backupAncestorRevs failed: doc=%q rev=%q err=%v", doc.ID, revid, err)
		return err
	}

	// Nil out the rev's body in the document struct:
	if revid == doc.CurrentRev {
		doc.body = nil
	} else {
		doc.History.setRevisionBody(revid, nil)
	}
	base.LogTo("CRUD+", "Backed up obsolete rev %q/%q", doc.ID, revid)
	return nil
}

//////// UPDATING DOCUMENTS:

// Initializes the gateway-specific "_sync_" metadata of a new document.
// Used when importing an existing Couchbase doc that hasn't been seen by the gateway before.
func (db *Database) initializeSyncData(doc *document) (err error) {
	body := doc.body
	doc.CurrentRev = createRevID(1, "", body)
	body["_rev"] = doc.CurrentRev
	doc.setFlag(channels.Deleted, false)
	doc.History = make(RevTree)
	doc.History.addRevision(RevInfo{ID: doc.CurrentRev, Parent: "", Deleted: false})
	if db.writeSequences() {
		doc.Sequence, err = db.sequences.nextSequence()
	}
	return
}

// Updates or creates a document.
// The new body's "_rev" property must match the current revision's, if any.
func (db *Database) Put(docid string, body Body) (string, error) {
	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body["_rev"].(string)
	generation, _ := ParseRevID(matchRev)
	if generation < 0 {
		return "", base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++
	deleted, _ := body["_deleted"].(bool)

	expiry, err := body.extractExpiry()
	if err != nil {
		return "", base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}

	return db.updateDoc(docid, false, expiry, func(doc *document) (Body, AttachmentData, error) {
		// (Be careful: this block can be invoked multiple times if there are races!)
		// First, make sure matchRev matches an existing leaf revision:
		if matchRev == "" {
			matchRev = doc.CurrentRev
			if matchRev != "" {
				// PUT with no parent rev given, but there is an existing current revision.
				// This is OK as long as the current one is deleted.
				if !doc.History[matchRev].Deleted {
					return nil, nil, base.HTTPErrorf(http.StatusConflict, "Document exists")
				}
				generation, _ = ParseRevID(matchRev)
				generation++
			}
		} else if !doc.History.isLeaf(matchRev) {
			return nil, nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
		}

		// Process the attachments, replacing bodies with digests. This alters 'body' so it has to
		// be done before calling createRevID (the ID is based on the digest of the body.)
		newAttachments, err := db.storeAttachments(doc, body, generation, matchRev, nil)
		if err != nil {
			return nil, nil, err
		}

		// Make up a new _rev, and add it to the history:
		newRev := createRevID(generation, matchRev, body)
		body["_rev"] = newRev
		doc.History.addRevision(RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted})
		return body, newAttachments, nil
	})
}

// Adds an existing revision to a document along with its history (list of rev IDs.)
// This is equivalent to the "new_edits":false mode of CouchDB.
func (db *Database) PutExistingRev(docid string, body Body, docHistory []string) error {
	newRev := docHistory[0]
	generation, _ := ParseRevID(newRev)
	if generation < 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	deleted, _ := body["_deleted"].(bool)

	expiry, err := body.extractExpiry()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}

	_, err = db.updateDoc(docid, false, expiry, func(doc *document) (Body, AttachmentData, error) {
		// (Be careful: this block can be invoked multiple times if there are races!)
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
			base.LogTo("CRUD+", "PutExistingRev(%q): No new revisions to add", docid)
			return nil, nil, couchbase.UpdateCancel // No new revisions to add
		}

		// Add all the new-to-me revisions to the rev tree:
		for i := currentRevIndex - 1; i >= 0; i-- {
			doc.History.addRevision(RevInfo{
				ID:      docHistory[i],
				Parent:  parent,
				Deleted: (i == 0 && deleted)})
			parent = docHistory[i]
		}

		// Process the attachments, replacing bodies with digests.
		parentRevID := doc.History[newRev].Parent
		newAttachments, err := db.storeAttachments(doc, body, generation, parentRevID, docHistory)
		if err != nil {
			return nil, nil, err
		}
		body["_rev"] = newRev
		return body, newAttachments, nil
	})
	return err
}

// Common subroutine of Put and PutExistingRev: a shell that loads the document, lets the caller
// make changes to it in a callback and supply a new body, then saves the body and document.
func (db *Database) updateDoc(docid string, allowImport bool, expiry uint32, callback func(*document) (Body, AttachmentData, error)) (string, error) {
	key := realDocID(docid)
	if key == "" {
		return "", base.HTTPErrorf(400, "Invalid doc ID")
	}

	var newRevID, parentRevID string
	var doc *document
	var body Body
	var changedChannels base.Set
	var changedPrincipals, changedRoleUsers []string
	var docSequence uint64
	var unusedSequences []uint64
	var oldBodyJSON string
	var newAttachments AttachmentData

	err := db.Bucket.WriteUpdate(key, int(expiry), func(currentValue []byte) (raw []byte, writeOpts sgbucket.WriteOptions, err error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if doc, err = unmarshalDocument(docid, currentValue); err != nil {
			return
		} else if !allowImport && currentValue != nil && !doc.HasValidSyncData(db.writeSequences()) {
			err = base.HTTPErrorf(409, "Not imported")
			return
		}

		// Invoke the callback to update the document and return a new revision body:
		body, newAttachments, err = callback(doc)
		if err != nil {
			return
		}

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
		newRevID = body["_rev"].(string)
		parentRevID = doc.History[newRevID].Parent
		prevCurrentRev := doc.CurrentRev
		var branched, inConflict bool
		doc.CurrentRev, branched, inConflict = doc.History.winningRevision()
		doc.setFlag(channels.Deleted, doc.History[doc.CurrentRev].Deleted)
		doc.setFlag(channels.Conflict, inConflict)
		doc.setFlag(channels.Branched, branched)

		if doc.CurrentRev != prevCurrentRev && prevCurrentRev != "" && doc.body != nil {
			// Store the doc's previous body into the revision tree:
			bodyJSON, _ := json.Marshal(doc.body)
			doc.History.setRevisionBody(prevCurrentRev, bodyJSON)
		}

		// Store the new revision body into the doc:
		doc.setRevision(newRevID, body)

		if doc.CurrentRev == newRevID {
			doc.NewestRev = ""
			doc.setFlag(channels.Hidden, false)
		} else {
			doc.NewestRev = newRevID
			doc.setFlag(channels.Hidden, true)
			if doc.CurrentRev != prevCurrentRev {
				// If the new revision is not current, transfer the current revision's
				// body to the top level doc.body:
				doc.body = doc.History.getParsedRevisionBody(doc.CurrentRev)
				doc.History.setRevisionBody(doc.CurrentRev, nil)
			}
		}

		// Run the sync function, to validate the update and compute its channels/access:
		body["_id"] = doc.ID
		channelSet, access, roles, oldBody, err := db.getChannelsAndAccess(doc, body, newRevID)

		//Assign old revision body to variable in method scope
		oldBodyJSON = oldBody
		if err != nil {
			return
		}

		if len(channelSet) > 0 {
			doc.History[newRevID].Channels = channelSet
		}

		// Move the body of the replaced revision out of the document so it can be compacted later.
		db.backupAncestorRevs(doc, newRevID)

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
					base.LogTo("Cache", "updateDoc %q: Unused sequence #%d", docid, docSequence)
					unusedSequences = append(unusedSequences, docSequence)
				}
				if docSequence, err = db.sequences.nextSequence(); err != nil {
					return
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

			if len(doc.RecentSequences) > kMaxRecentSequences {
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
					base.LogTo("CRUD+", "updateDoc(%q): Rev %q causes %q to become current again",
						docid, newRevID, doc.CurrentRev)
					channelSet, access, roles, oldBody, err = db.getChannelsAndAccess(doc, curBody, doc.CurrentRev)

					//Assign old revision body to variable in method scope
					oldBodyJSON = oldBody
					if err != nil {
						return
					}
				} else {
					// Shouldn't be possible (CurrentRev is a leaf so won't have been compacted)
					base.Warn("updateDoc(%q): Rev %q missing, can't call getChannelsAndAccess "+
						"on it (err=%v)", docid, doc.CurrentRev, err)
					channelSet = nil
					access = nil
					roles = nil
				}
			}

			// Update the document struct's channel assignment and user access.
			// (This uses the new sequence # so has to be done after updating doc.Sequence)
			changedChannels = doc.updateChannels(channelSet) //FIX: Incorrect if new rev is not current!
			changedPrincipals = doc.Access.updateAccess(doc, access)
			changedRoleUsers = doc.RoleAccess.updateAccess(doc, roles)

			if len(changedPrincipals) > 0 || len(changedRoleUsers) > 0 {
				if cbb, ok := db.Bucket.(base.CouchbaseBucket); ok { //Backing store is Couchbase Server
					if major, _, _, err := cbb.CBSVersion(); err == nil && major >= 3 {
						base.LogTo("CRUD+", "Optimizing write for Couchbase Server >= 3.0")
					} else {
						// make sure the write blocks till
						// the new value is indexable, otherwise when a User/Role updates (using a view) it
						// might not incorporate the effects of this change.
						writeOpts |= sgbucket.Indexable
					}
				} else {
					// If this update affects user/role access privileges, make sure the write blocks till
					// the new value is indexable, otherwise when a User/Role updates (using a view) it
					// might not incorporate the effects of this change.
					writeOpts |= sgbucket.Indexable
				}
			}

		} else {
			base.LogTo("CRUD+", "updateDoc(%q): Rev %q leaves %q still current",
				docid, newRevID, prevCurrentRev)
		}

		// Prune old revision history to limit the number of revisions:
		if pruned := doc.History.pruneRevisions(db.RevsLimit, doc.CurrentRev); pruned > 0 {
			base.LogTo("CRUD+", "updateDoc(%q): Pruned %d old revisions", docid, pruned)
		}

		doc.TimeSaved = time.Now()
		doc.UpdateExpiry(expiry)

		// Now that the document has been successfully validated, we can store any new attachments
		db.setAttachments(newAttachments)

		// Return the new raw document value for the bucket to store.
		raw, err = json.Marshal(doc)
		base.LogTo("CRUD+", "SAVING #%d", doc.Sequence) //TEMP?
		return
	})

	if err == couchbase.UpdateCancel {
		return "", nil
	} else if err == couchbase.ErrOverwritten {
		// ErrOverwritten is ok; if a later revision got persisted, that's fine too
		base.LogTo("CRUD+", "Note: Rev %q/%q was overwritten in RAM before becoming indexable",
			docid, newRevID)
	} else if err != nil {
		return "", err
	}

	dbExpvars.Add("revs_added", 1)

	if doc.History[newRevID] != nil {
		// Store the new revision in the cache
		history := doc.History.getHistory(newRevID)

		if doc.History[newRevID].Deleted {
			body["_deleted"] = true
		}
		revChannels := doc.History[newRevID].Channels
		db.revisionCache.Put(body, encodeRevisions(history), revChannels)

		// Raise event if this is not an echo from a shadow bucket
		if newRevID != doc.UpstreamRev {
			if db.EventMgr.HasHandlerForEvent(DocumentChange) {
				db.EventMgr.RaiseDocumentChangeEvent(body, oldBodyJSON, revChannels)
			}
		}
	} else {
		//Revision has been pruned away so won't be added to cache
		base.LogTo("CRUD", "doc %q / %q, has been pruned, it has not been inserted into the revision cache", docid, newRevID)
	}

	// Now that the document has successfully been stored, we can make other db changes:
	base.LogTo("CRUD", "Stored doc %q / %q", docid, newRevID)

	// Mark affected users/roles as needing to recompute their channel access:
	if len(changedPrincipals) > 0 {
		base.LogTo("Access", "Rev %q/%q invalidates channels of %s", docid, newRevID, changedPrincipals)
		for _, name := range changedPrincipals {
			db.invalUserOrRoleChannels(name)
			//If this is the current in memory db.user, reload to generate updated channels
			if db.user != nil && db.user.Name() == name {
				user, err := db.Authenticator().GetUser(db.user.Name())
				if err != nil {
					base.Warn("Error reloading db.user[%s], channels list is out of date --> %+v", db.user.Name(), err)
				} else {
					db.user = user
				}
			}
		}
	}

	if len(changedRoleUsers) > 0 {
		base.LogTo("Access", "Rev %q/%q invalidates roles of %s", docid, newRevID, changedRoleUsers)
		for _, name := range changedRoleUsers {
			db.invalUserRoles(name)
			//If this is the current in memory db.user, reload to generate updated roles
			if db.user != nil && db.user.Name() == name {
				user, err := db.Authenticator().GetUser(db.user.Name())
				if err != nil {
					base.Warn("Error reloading db.user[%s], roles list is out of date --> %+v", db.user.Name(), err)
				} else {
					db.user = user
				}
			}
		}
	}

	return newRevID, nil
}

// Creates a new document, assigning it a random doc ID.
func (db *Database) Post(body Body) (string, string, error) {
	if body["_rev"] != nil {
		return "", "", base.HTTPErrorf(http.StatusNotFound, "No previous revision to replace")
	}

	// If there's an incoming _id property, use that as the doc ID.
	docid, idFound := body["_id"].(string)
	if !idFound {
		docid = base.CreateUUID()
	}

	rev, err := db.Put(docid, body)
	if err != nil {
		docid = ""
	}
	return docid, rev, err
}

// Deletes a document, by adding a new revision whose "_deleted" property is true.
func (db *Database) DeleteDoc(docid string, revid string) (string, error) {
	body := Body{"_deleted": true, "_rev": revid}
	return db.Put(docid, body)
}

//////// CHANNELS:

// Calls the JS sync function to assign the doc to channels, grant users
// access to channels, and reject invalid documents.
func (db *Database) getChannelsAndAccess(doc *document, body Body, revID string) (result base.Set, access channels.AccessMap, roles channels.AccessMap, oldJson string, err error) {
	base.LogTo("CRUD+", "Invoking sync on doc %q rev %s", doc.ID, body["_rev"])

	// Get the parent revision, to pass to the sync function:
	var oldJsonBytes []byte
	if oldJsonBytes, err = db.getAncestorJSON(doc, revID); err != nil {
		return
	}
	oldJson = string(oldJsonBytes)

	if db.ChannelMapper != nil {
		// Call the ChannelMapper:
		var output *channels.ChannelMapperOutput
		output, err = db.ChannelMapper.MapToChannelsAndAccess(body, oldJson,
			makeUserCtx(db.user))
		if err == nil {
			result = output.Channels
			access = output.Access
			roles = output.Roles
			err = output.Rejection
			if err != nil {
				base.Logf("Sync fn rejected: new=%+v  old=%s --> %s", body, oldJson, err)
			} else if !validateAccessMap(access) || !validateRoleAccessMap(roles) {
				err = base.HTTPErrorf(500, "Error in JS sync function")
			}

		} else {
			base.Warn("Sync fn exception: %+v; doc = %s", err, body)
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
	return
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
		if strings.HasPrefix(name, "role:") {
			name = name[5:] // Roles are identified in access view by a "role:" prefix
		}
		if !auth.IsValidPrincipalName(name) {
			base.Warn("Invalid principal name %q in access() or role() call", name)
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
				base.Warn("Invalid role name %q in role() call", rolename)
				return false
			}
		}
	}
	return true
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
		key = "role:" + key // Roles are identified in access view by a "role:" prefix
	}

	var vres struct {
		Rows []struct {
			Value channels.TimedSet
		}
	}

	opts := map[string]interface{}{"stale": false, "key": key}
	if verr := context.Bucket.ViewCustom(DesignDocSyncGateway, ViewAccess, opts, &vres); verr != nil {
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
func (context *DatabaseContext) ComputeVbSequenceChannelsForPrincipal(princ auth.Principal) (channels.TimedSet, error) {
	key := princ.Name()
	if _, ok := princ.(auth.User); !ok {
		key = "role:" + key // Roles are identified in access view by a "role:" prefix
	}

	var vres struct {
		Rows []struct {
			Value channels.TimedSet
		}
	}

	opts := map[string]interface{}{"stale": false, "key": key}
	if verr := context.Bucket.ViewCustom(DesignDocSyncGateway, ViewAccessVbSeq, opts, &vres); verr != nil {
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
	var vres struct {
		Rows []struct {
			Value channels.TimedSet
		}
	}

	opts := map[string]interface{}{"stale": false, "key": user.Name()}
	if verr := context.Bucket.ViewCustom(DesignDocSyncGateway, ViewRoleAccess, opts, &vres); verr != nil {
		return nil, verr
	}
	// Merge the TimedSets from the view result:
	var result channels.TimedSet
	for _, row := range vres.Rows {
		if result == nil {
			result = row.Value
		} else {
			result.Add(row.Value)
		}
	}
	return result, nil
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
	if verr := context.Bucket.ViewCustom(DesignDocSyncGateway, ViewRoleAccessVbSeq, opts, &vres); verr != nil {
		return nil, verr
	}

	roleSet := channels.TimedSet{}
	for _, row := range vres.Rows {
		roleSet.Add(row.Value)
	}
	return roleSet, nil
}

//////// REVS_DIFF:

// Given a document ID and a set of revision IDs, looks up which ones are not known. Returns an
// array of the unknown revisions, and an array of known revisions that might be recent ancestors.
func (db *Database) RevDiff(docid string, revids []string) (missing, possible []string) {
	if strings.HasPrefix(docid, "_design/") && db.user != nil {
		return // Users can't upload design docs, so ignore them
	}
	doc, err := db.GetDoc(docid)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			base.Warn("RevDiff(%q) --> %T %v", docid, err, err)
			// If something goes wrong getting the doc, treat it as though it's nonexistent.
		}
		missing = revids
		return
	}
	// Check each revid to see if it's in the doc's rev tree:
	revtree := doc.History
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

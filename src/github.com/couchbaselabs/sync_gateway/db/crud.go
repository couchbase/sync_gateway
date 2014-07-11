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
	"github.com/couchbaselabs/walrus"
	"net/http"
	"strings"
	"time"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

//////// READING DOCUMENTS:

func realDocID(docid string) string {
	if len(docid) > 250 {
		return "" // Invalid doc IDs
	}
	if strings.HasPrefix(docid, "_") && !strings.HasPrefix(docid, "_design/") {
		return "" // Disallow "_" prefix, except in "_design/"" (for CouchDB compatibility)
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
	err := db.Bucket.Get(key, doc)
	if err != nil {
		return nil, err
	} else if !doc.hasValidSyncData() {
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
	return db.GetRev(docid, "", false, nil)
}

// Returns the body of a revision of a document. Uses the revision cache.
// revid may be "", meaning the current revision.
func (db *Database) GetRev(docid, revid string, listRevisions bool, attachmentsSince []string) (Body, error) {
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
		revisions = encodeRevisions(doc.History.getHistory(revid))
		inChannels = doc.History[revid].Channels
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
			if listRevisions {
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
	if listRevisions {
		body["_revisions"] = revisions
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
				minRevpos, _ = parseRevID(ancestor)
				minRevpos++
			}
		}
		err = db.loadBodyAttachments(body, minRevpos)
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

// Returns the body of a revision of a document, as well as the document's current channels
// and the user/roles it grants channel access to.
func (db *Database) GetRevAndChannels(docid, revid string, listRevisions bool) (body Body, channels channels.ChannelMap, access UserAccessMap, roleAccess UserAccessMap, err error) {
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
	doc.Sequence, err = db.sequences.nextSequence()
	return
}

// Updates or creates a document.
// The new body's "_rev" property must match the current revision's, if any.
func (db *Database) Put(docid string, body Body) (string, error) {
	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body["_rev"].(string)
	generation, _ := parseRevID(matchRev)
	if generation < 0 {
		return "", base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++
	deleted, _ := body["_deleted"].(bool)

	return db.updateDoc(docid, false, func(doc *document) (Body, error) {
		// (Be careful: this block can be invoked multiple times if there are races!)
		// First, make sure matchRev matches an existing leaf revision:
		if matchRev == "" {
			matchRev = doc.CurrentRev
			if matchRev != "" {
				// PUT with no parent rev given, but there is an existing current revision.
				// This is OK as long as the current one is deleted.
				if !doc.History[matchRev].Deleted {
					return nil, base.HTTPErrorf(http.StatusConflict, "Document exists")
				}
				generation, _ = parseRevID(matchRev)
				generation++
			}
		} else if !doc.History.isLeaf(matchRev) {
			return nil, base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
		}

		// Process the attachments, replacing bodies with digests. This alters 'body' so it has to
		// be done before calling createRevID (the ID is based on the digest of the body.)
		if err := db.storeAttachments(doc, body, generation, matchRev); err != nil {
			return nil, err
		}

		// Make up a new _rev, and add it to the history:
		newRev := createRevID(generation, matchRev, body)
		body["_rev"] = newRev
		doc.History.addRevision(RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted})
		return body, nil
	})
}

// Adds an existing revision to a document along with its history (list of rev IDs.)
// This is equivalent to the "new_edits":false mode of CouchDB.
func (db *Database) PutExistingRev(docid string, body Body, docHistory []string) error {
	newRev := docHistory[0]
	generation, _ := parseRevID(newRev)
	if generation < 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	deleted, _ := body["_deleted"].(bool)
	_, err := db.updateDoc(docid, false, func(doc *document) (Body, error) {
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
			return nil, couchbase.UpdateCancel // No new revisions to add
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
		if err := db.storeAttachments(doc, body, generation, parentRevID); err != nil {
			return nil, err
		}
		body["_rev"] = newRev
		return body, nil
	})
	return err
}

// Common subroutine of Put and PutExistingRev: a shell that loads the document, lets the caller
// make changes to it in a callback and supply a new body, then saves the body and document.
func (db *Database) updateDoc(docid string, allowImport bool, callback func(*document) (Body, error)) (string, error) {
	// As a special case, it's illegal to put a design document except in admin mode:
	if strings.HasPrefix(docid, "_design/") && db.user != nil {
		return "", base.HTTPErrorf(403, "Forbidden to update design doc")
	}

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

	err := db.Bucket.WriteUpdate(key, 0, func(currentValue []byte) (raw []byte, writeOpts walrus.WriteOptions, err error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if doc, err = unmarshalDocument(docid, currentValue); err != nil {
			return
		} else if !allowImport && currentValue != nil && !doc.hasValidSyncData() {
			err = base.HTTPErrorf(409, "Not imported")
			return
		}

		// Invoke the callback to update the document and return a new revision body:
		body, err = callback(doc)
		if err != nil {
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
		channels, access, roles, err := db.getChannelsAndAccess(doc, body, newRevID)
		if err != nil {
			return
		}
		if len(channels) > 0 {
			doc.History[newRevID].Channels = channels
		}

		// Move the body of the replaced revision out of the document so it can be compacted later.
		db.backupAncestorRevs(doc, newRevID)

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
			// base.TEMP("Allocated seq #%d for (%q / %q)", docSequence, docid, newRevID)
		}
		doc.Sequence = docSequence
		doc.UnusedSequences = unusedSequences

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
					channels, access, roles, err = db.getChannelsAndAccess(doc, curBody, doc.CurrentRev)
					if err != nil {
						return
					}
				} else {
					// Shouldn't be possible (CurrentRev is a leaf so won't have been compacted)
					base.Warn("updateDoc(%q): Rev %q missing, can't call getChannelsAndAccess "+
						"on it (err=%v)", docid, doc.CurrentRev, err)
					channels = nil
					access = nil
					roles = nil
				}
			}

			// Update the document struct's channel assignment and user access.
			// (This uses the new sequence # so has to be done after updating doc.Sequence)
			changedChannels = doc.updateChannels(channels) //FIX: Incorrect if new rev is not current!
			changedPrincipals = doc.Access.updateAccess(doc, access)
			changedRoleUsers = doc.RoleAccess.updateAccess(doc, roles)
			if len(changedPrincipals) > 0 || len(changedRoleUsers) > 0 {
				// If this update affects user/role access privileges, make sure the write blocks till
				// the new value is indexable, otherwise when a User/Role updates (using a view) it
				// might not incorporate the effects of this change.
				writeOpts |= walrus.Indexable
			}
		} else {
			base.LogTo("CRUD+", "updateDoc(%q): Rev %q leaves %q still current",
				docid, newRevID, prevCurrentRev)
		}

		// Prune old revision history to limit the number of revisions:
		if pruned := doc.History.pruneRevisions(db.RevsLimit); pruned > 0 {
			base.LogTo("CRUD+", "updateDoc(%q): Pruned %d old revisions", docid, pruned)
		}

		doc.TimeSaved = time.Now()

		// Return the new raw document value for the bucket to store.
		raw, err = json.Marshal(doc)
		base.LogTo("Cache", "SAVING #%d", doc.Sequence) //TEMP?
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

	// Store the new revision in the cache
	history := doc.History.getHistory(newRevID)
	if doc.History[newRevID].Deleted {
		body["_deleted"] = true
	}
	revChannels := doc.History[newRevID].Channels
	db.revisionCache.Put(body, encodeRevisions(history), revChannels)

	// Now that the document has successfully been stored, we can make other db changes:
	base.LogTo("CRUD", "Stored doc %q / %q", docid, newRevID)

	// Mark affected users/roles as needing to recompute their channel access:
	if len(changedPrincipals) > 0 {
		base.LogTo("Access", "Rev %q/%q invalidates channels of %s", docid, newRevID, changedPrincipals)
		for _, name := range changedPrincipals {
			db.invalUserOrRoleChannels(name)
		}
	}
	if len(changedRoleUsers) > 0 {
		base.LogTo("Access", "Rev %q/%q invalidates roles of %s", docid, newRevID, changedRoleUsers)
		for _, name := range changedRoleUsers {
			db.invalUserRoles(name)
		}
	}

	return newRevID, nil
}

// Creates a new document, assigning it a random doc ID.
func (db *Database) Post(body Body) (string, string, error) {
	if body["_rev"] != nil {
		return "", "", base.HTTPErrorf(http.StatusNotFound, "No previous revision to replace")
	}
	docid := base.CreateUUID()
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
func (db *Database) getChannelsAndAccess(doc *document, body Body, revID string) (result base.Set, access channels.AccessMap, roles channels.AccessMap, err error) {
	base.LogTo("CRUD+", "Invoking sync on doc %q rev %s", doc.ID, body["_rev"])

	// Get the parent revision, to pass to the sync function:
	var oldJsonBytes []byte
	if oldJsonBytes, err = db.getAncestorJSON(doc, revID); err != nil {
		return
	}
	oldJson := string(oldJsonBytes)

	if db.ChannelMapper != nil {
		// Call the ChannelMapper:
		var output *channels.ChannelMapperOutput
		output, err = db.ChannelMapper.MapToChannelsAndAccess(body, oldJson,
			makeUserCtx(db.user))
		if err == nil {
			result = output.Channels
			if !doc.hasFlag(channels.Deleted) { // deleted docs can't grant access
				access = output.Access
				roles = output.Roles
			}
			err = output.Rejection
			if err != nil {
				base.Log("Sync fn rejected: new=%+v  old=%s --> %s", body, oldJson, err)
			} else if !validateAccessMap(access) || !validateRoleAccessMap(roles) {
				err = base.HTTPErrorf(500, "Error in JS sync function")
			}

		} else {
			base.Warn("Sync fn exception: %+v; doc = %s", err, body)
			err = base.HTTPErrorf(500, "Exception in JS sync function")
		}

	} else {
		// No ChannelMapper so by default use the "channels" property:
		value, _ := body["channels"].([]interface{})
		if value != nil {
			array := make([]string, 0, len(value))
			for _, channel := range value {
				channelStr, ok := channel.(string)
				if ok && len(channelStr) > 0 {
					array = append(array, channelStr)
				}
			}
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
	for name, _ := range access {
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
		for rolename, _ := range roles {
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
	if verr := context.Bucket.ViewCustom("sync_gateway", "access", opts, &vres); verr != nil {
		return nil, verr
	}
	channelSet := channels.TimedSet{}
	for _, row := range vres.Rows {
		channelSet.Add(row.Value)
	}
	return channelSet, nil
}

// Recomputes the set of roles a User has been granted access to by sync() functions.
// This is part of the ChannelComputer interface defined by the Authenticator.
func (context *DatabaseContext) ComputeRolesForUser(user auth.User) (channels.TimedSet, error) {
	var vres struct {
		Rows []struct {
			Value channels.TimedSet
		}
	}

	opts := map[string]interface{}{"stale": false, "key": user.Name()}
	if verr := context.Bucket.ViewCustom("sync_gateway", "role_access", opts, &vres); verr != nil {
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

//////// REVS_DIFF:

// Given a document ID and a set of revision IDs, looks up which ones are not known.
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
	revmap := doc.History
	found := make(map[string]bool)
	maxMissingGen := 0
	for _, revid := range revids {
		if revmap.contains(revid) {
			found[revid] = true
		} else {
			if missing == nil {
				missing = make([]string, 0, 5)
			}
			gen, _ := parseRevID(revid)
			if gen > 0 {
				missing = append(missing, revid)
				if gen > maxMissingGen {
					maxMissingGen = gen
				}
			}
		}
	}
	if missing != nil {
		possible = make([]string, 0, 5)
		for revid, _ := range revmap {
			gen, _ := parseRevID(revid)
			if !found[revid] && gen < maxMissingGen {
				possible = append(possible, revid)
			}
		}
		if len(possible) == 0 {
			possible = nil
		}
	}
	return
}

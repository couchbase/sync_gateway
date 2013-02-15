// document.go -- document-oriented Database methods

package db

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
)

type ChannelRemoval struct {
	Seq uint64 `json:"seq"`
	Rev string `json:"rev"`
}
type ChannelMap map[string]*ChannelRemoval

type AccessMap map[string][]string

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
type document struct {
	ID         string     `json:"id"`
	CurrentRev string     `json:"rev"`
	Deleted    bool       `json:"deleted,omitempty"`
	Sequence   uint64     `json:"sequence"`
	History    RevTree    `json:"history"`
	Channels   ChannelMap `json:"channels,omitempty"`
	Access     AccessMap  `json:"access,omitempty"`
}

func (db *Database) realDocID(docid string) string {
	if docid == "" {
		return ""
	}
	docid = "doc:" + docid
	if len(docid) > 250 {
		return ""
	}
	return docid
}

func newDocument() *document {
	return &document{History: make(RevTree)}
}

func (db *Database) getDoc(docid string) (*document, error) {
	key := db.realDocID(docid)
	if key == "" {
		return nil, &base.HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	doc := newDocument()
	err := db.Bucket.Get(key, doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (db *Database) setDoc(docid string, doc *document) error {
	key := db.realDocID(docid)
	if key == "" {
		return &base.HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	return db.Bucket.Set(key, 0, doc)
}

// Returns the body of the current revision of a document
func (db *Database) Get(docid string) (Body, error) {
	return db.GetRev(docid, "", false, nil)
}

// Returns the body of a revision of a document.
func (db *Database) GetRev(docid, revid string,
	listRevisions bool,
	attachmentsSince []string) (Body, error) {
	doc, err := db.getDoc(docid)
	if doc == nil {
		return nil, err
	}
	body, err := db.getRevFromDoc(doc, revid, listRevisions)
	if err != nil {
		return nil, err
	}

	if attachmentsSince != nil {
		minRevpos := 1
		if len(attachmentsSince) > 0 {
			ancestor := doc.History.findAncestorFromSet(body["_rev"].(string), attachmentsSince)
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

// Returns an HTTP 403 error if the User is not allowed to access any of the document's channels.
// A nil User means access control is disabled, so the function will return nil.
func AuthorizeAnyDocChannels(user *auth.User, channels ChannelMap) error {
	if user == nil {
		return nil
	}
	for _, channel := range user.AllChannels() {
		if channel == "*" {
			return nil
		}
		value, exists := channels[channel]
		if exists && value == nil {
			return nil // yup, it's in this channel
		}
	}
	return user.UnauthError("You are not allowed to see this")
}

// Returns the body of a revision given a document struct
func (db *Database) getRevFromDoc(doc *document, revid string, listRevisions bool) (Body, error) {
	if err := AuthorizeAnyDocChannels(db.user, doc.Channels); err != nil {
		// FIX: This only authorizes vs the current revision, not the one the client asked for!
		return nil, err
	}
	if revid == "" {
		revid = doc.CurrentRev
		if doc.History[revid].Deleted == true {
			return nil, &base.HTTPError{Status: 404, Message: "deleted"}
		}
	}
	info, exists := doc.History[revid]
	if !exists || info.Key == "" {
		return nil, &base.HTTPError{Status: 404, Message: "missing"}
	}

	body, err := db.getRevision(doc.ID, revid, info.Key)
	if err != nil {
		return nil, err
	}
	if info.Deleted {
		body["_deleted"] = true
	}
	if listRevisions {
		history := doc.History.getHistory(revid)
		body["_revisions"] = encodeRevisions(history)
	}
	return body, nil
}

// Returns the body of the asked-for revision or the most recent available ancestor.
// Does NOT fill in _attachments, _deleted, etc.
func (db *Database) getAvailableRev(doc *document, revid string) (Body, error) {
	for ; revid != ""; revid = doc.History[revid].Parent {
		key := doc.History[revid].Key
		if key != "" {
			return db.getRevision(doc.ID, revid, key)
		}
	}
	return nil, &base.HTTPError{404, "missing"}
}

//////// UPDATING DOCUMENTS:

// Updates or creates a document.
// The new body's "_rev" property must match the current revision's, if any.
func (db *Database) Put(docid string, body Body) (string, error) {
	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body["_rev"].(string)
	generation, _ := parseRevID(matchRev)
	if generation < 0 {
		return "", &base.HTTPError{Status: http.StatusBadRequest, Message: "Invalid revision ID"}
	}
	generation++
	deleted, _ := body["_deleted"].(bool)

	return db.updateDoc(docid, func(doc *document) (Body, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		// First, make sure matchRev matches an existing leaf revision:
		if !(len(doc.History) == 0 && matchRev == "") && !doc.History.isLeaf(matchRev) {
			return nil, &base.HTTPError{Status: http.StatusConflict, Message: "Document update conflict"}
		}
		// Process the attachments, replacing bodies with digests. This alters 'body' so it has to
		// be done before calling createRevID (the ID is based on the digest of the body.)
		if err := db.storeAttachments(doc, body, generation, matchRev); err != nil {
			return nil, err
		}
		// Make up a new _rev, and add it to the history:
		newRev := createRevID(generation, matchRev, body)
		body["_rev"] = newRev
		if err := db.validateDoc(doc, body, matchRev); err != nil {
			return nil, err
		}
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
		return &base.HTTPError{Status: http.StatusBadRequest, Message: "Invalid revision ID"}
	}
	deleted, _ := body["_deleted"].(bool)
	_, err := db.updateDoc(docid, func(doc *document) (Body, error) {
		// Be careful: this block can be invoked multiple times if there are races!
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
			return nil, couchbase.UpdateCancel // No new revisions to add
		}

		//FIX: Should call validateDoc? What if the parent rev doesn't exist locally?

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
		return body, nil
	})
	return err
}

func (db *Database) validateDoc(doc *document, newRev Body, oldRevID string) error {
	if db.Validator == nil { //TEMP: Move to top of fn
		return nil
	}
	newRev["_id"] = doc.ID
	newJson, _ := json.Marshal(newRev)
	oldJson := ""
	if oldRevID != "" {
		var err error
		oldJson, err = db.getRevisionJSON(doc.ID, oldRevID, doc.History[oldRevID].Key)
		if err != nil {
			return err
		}
	}
	status, msg, err := db.Validator.Validate(string(newJson), oldJson, db.user)
	if err == nil && status >= 300 {
		log.Printf("Validator rejected: new=%s  old=%s --> %d %q", newJson, oldJson, status, msg)
		err = &base.HTTPError{status, msg}
	}
	return err
}

// Common subroutine of Put and PutExistingRev: a shell that loads the document, lets the caller
// make changes to it in a callback and supply a new body, then saves the body and document.
func (db *Database) updateDoc(docid string, callback func(*document) (Body, error)) (string, error) {
	key := db.realDocID(docid)
	if key == "" {
		return "", &base.HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	var newRev string
	var body Body

	err := db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		doc := newDocument()
		if len(currentValue) == 0 { // New document:
			doc.ID = docid
		} else { // Updating document:
			if err := json.Unmarshal(currentValue, doc); err != nil {
				return nil, err
			}
		}

		// Invoke the callback to update the document and return a new revision body:
		var err error
		body, err = callback(doc)
		if err != nil {
			return nil, err
		}

		if body != nil {
			// Store the new revision:
			newRev = body["_rev"].(string)
			key, err := db.setRevision(body)
			if err != nil {
				return nil, err
			}
			doc.History.setRevisionKey(newRev, key)
		}

		// Determine which is the current "winning" revision (it's not necessarily the new one):
		doc.CurrentRev = doc.History.winningRevision()
		doc.Deleted = doc.History[doc.CurrentRev].Deleted

		// Assign the document the next sequence number, for the _changes feed:
		doc.Sequence, err = db.sequences.nextSequence()
		if err != nil {
			return nil, err
		}

		channels, access := db.getChannelsAndAccess(body)
		db.updateDocChannels(doc, channels) //FIX: Incorrect if new rev is not current!
		db.updateDocAccess(doc, access)     //FIX: Incorrect if new rev is not current!

		// Tell Couchbase to store the document:
		return json.Marshal(doc)
	})

	if err == couchbase.UpdateCancel {
		return "", nil
	} else if err != nil {
		return "", err
	}
	if base.Logging && newRev != "" {
		log.Printf("\tAdded doc %q / %q", docid, newRev)
	}

	// Ugly hack to detect changes to the channel-mapper function:
	if docid == "_design/channels" {
		src, ok := body["channelmap"].(string)
		if ok {
			if changed, _ := db.ChannelMapper.SetFunction(src); changed {
				db.UpdateAllDocChannels()
			}
		}
	}

	db.NotifyRevision()
	return newRev, nil
}

// Creates a new document, assigning it a random doc ID.
func (db *Database) Post(body Body) (string, string, error) {
	if body["_rev"] != nil {
		return "", "", &base.HTTPError{Status: http.StatusNotFound,
			Message: "No previous revision to replace"}
	}
	docid := createUUID()
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

// Determines which channels a document body belongs to
func (db *Database) getChannelsAndAccess(body Body) (result []string, access map[string][]string) {
	if db.ChannelMapper != nil {
		jsonStr, _ := json.Marshal(body)
		result, access, _ = db.ChannelMapper.MapToChannelsAndAccess(string(jsonStr))

	} else {
		// No ChannelMapper so by default use the "channels" property:
		value, _ := body["channels"].([]interface{})
		if value == nil {
			return nil, nil
		}
		result = make([]string, 0, len(value))
		for _, channel := range value {
			channelStr, ok := channel.(string)
			if ok && len(channelStr) > 0 {
				result = append(result, channelStr)
			}
		}
	}
	return result, access
}

// Updates the Channels property of a document object with current & past channels
func (db *Database) updateDocChannels(doc *document, newChannels []string) (changed bool) {
	log.Printf("\tAssigning doc %q to channels %q", doc.ID, newChannels)
	channels := doc.Channels
	if channels == nil {
		channels = ChannelMap{}
		doc.Channels = channels
	} else {
		// Mark every previous channel as unsubscribed:
		curSequence := doc.Sequence
		for channel, seq := range channels {
			if seq == nil {
				channels[channel] = &ChannelRemoval{curSequence, doc.CurrentRev}
				changed = true
			}
		}
	}

	// Mark every current channel as subscribed:
	for _, channel := range newChannels {
		if value, exists := channels[channel]; value != nil || !exists {
			channels[channel] = nil
			changed = true
		}
	}
	return changed
}

// Updates the Channels property of a document object with current & past channels
func (db *Database) updateDocAccess(doc *document, newAccess AccessMap) (changed bool) {
	log.Printf("updateDocAccess doc %v map %+v\n", doc.ID, newAccess)
	doc.Access = newAccess
	for name, value := range newAccess {
		log.Printf("name %v value %v\n", name, value)
		// load the document for the user with name
		// and delete the derived_channels field
		// save it back
	}
	return true
}

//////// REVS_DIFF:

type RevsDiffInput map[string][]string

// Given a set of documents and revisions, looks up which ones are not known.
// The input is a map from doc ID to array of revision IDs.
// The output is a map from doc ID to a map with "missing" and "possible_ancestors" arrays of rev IDs.
func (db *Database) RevsDiff(input RevsDiffInput) (map[string]interface{}, error) {
	// http://wiki.apache.org/couchdb/HttpPostRevsDiff
	output := make(map[string]interface{})
	for docid, revs := range input {
		missing, possible, err := db.RevDiff(docid, revs)
		if err != nil {
			return nil, err
		}
		if missing != nil {
			docOutput := map[string]interface{}{"missing": missing}
			if possible != nil {
				docOutput["possible_ancestors"] = possible
			}
			output[docid] = docOutput
		}
	}
	return output, nil
}

// Given a document ID and a set of revision IDs, looks up which ones are not known.
func (db *Database) RevDiff(docid string, revids []string) (missing, possible []string, err error) {
	doc, err := db.getDoc(docid)
	if err != nil {
		if !isMissingDocError(err) {
			log.Printf("WARNING: RevDiff(%q) --> %T %v", docid, err, err)
			// If something goes wrong getting the doc, treat it as though it's nonexistent.
		}
		missing = revids
		err = nil
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

//////// HELPER FUNCTIONS:

func createUUID() string {
	bytes := make([]byte, 16)
	n, err := rand.Read(bytes)
	if n < 16 {
		log.Panicf("Failed to generate random ID: %s", err)
	}
	return fmt.Sprintf("%x", bytes)
}

func isMissingDocError(err error) bool {
	httpstatus, _ := base.ErrorAsHTTPStatus(err)
	return httpstatus == 404
}

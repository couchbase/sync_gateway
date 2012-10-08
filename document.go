// document.go -- document-oriented Database methods

package basecouch

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
)

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
type document struct {
	ID         string  `json:"id"`
	CurrentRev string  `json:"rev"`
	Deleted    bool    `json:"deleted,omitempty"`
	Sequence   uint64  `json:"sequence"`
	History    RevTree `json:"history"`
}

func (db *Database) realDocID(docid string) string {
	if docid == "" {
		return ""
	}
	docid = db.DocPrefix + docid
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
		return nil, &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	doc := newDocument()
	err := db.bucket.Get(key, doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (db *Database) setDoc(docid string, doc *document) error {
	key := db.realDocID(docid)
	if key == "" {
		return &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	return db.bucket.Set(key, 0, doc)
}

// Returns the body of a revision of a document.
func (db *Database) GetRev(docid, revid string, listRevisions bool) (Body, error) {
	doc, err := db.getDoc(docid)
	if doc == nil {
		return nil, err
	}
	if revid == "" {
		revid = doc.CurrentRev
		if doc.History[revid].Deleted == true {
			return nil, &HTTPError{Status: 404, Message: "deleted"}
		}
	}
	info, exists := doc.History[revid]
	if !exists || info.Key == "" {
		return nil, &HTTPError{Status: 404, Message: "missing"}
	}

	body, err := db.getRevision(docid, revid, info.Key)
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

// Returns the body of the current revision of a document
func (db *Database) Get(docid string) (Body, error) {
	return db.GetRev(docid, "", false)
}

// Updates or creates a document.
// The new body's "_rev" property must match the current revision's, if any.
func (db *Database) Put(docid string, body Body) (string, error) {
	// Verify that the _rev key in the body matches the current stored value:
	var matchRev string
	if body["_rev"] != nil {
		matchRev = body["_rev"].(string)
	}
	doc, err := db.getDoc(docid)
	if err != nil {
		if !isMissingDocError(err) {
			return "", err
		}
		if matchRev != "" {
			return "", &HTTPError{Status: http.StatusNotFound,
				Message: "No previous revision to replace"}
		}
		doc = newDocument()
		doc.ID = docid
	} else {
		if !doc.History.isLeaf(matchRev) {
			return "", &HTTPError{Status: http.StatusConflict, Message: "Document update conflict"}
		}
	}

	// Make up a new _rev:
	generation, _ := parseRevID(matchRev)
	if generation < 0 {
		return "", &HTTPError{Status: http.StatusBadRequest, Message: "Invalid revision ID"}
	}
	newRev := createRevID(generation+1, matchRev, body)

	body["_rev"] = newRev
	deleted, _ := body["_deleted"].(bool)
	doc.History.addRevision(RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted})
	doc.CurrentRev = doc.History.winningRevision()
	doc.Deleted = doc.History[doc.CurrentRev].Deleted
	err = db.putDocAndBody(docid, newRev, doc, body)
	if err != nil {
		return "", err
	}
	return newRev, nil
}

func (db *Database) putDocAndBody(docid string, revid string, doc *document, body Body) error {
	var err error
	doc.Sequence, err = db.generateSequence()
	if err != nil {
		return err
	}
	key, err := db.setRevision(body)
	if err != nil {
		return err
	}

	doc.History.setRevisionKey(revid, key)
	return db.setDoc(docid, doc)
}

// Creates a new document, assigning it a random doc ID.
func (db *Database) Post(body Body) (string, string, error) {
	if body["_rev"] != nil {
		return "", "", &HTTPError{Status: http.StatusNotFound,
			Message: "No previous revision to replace"}
	}
	docid := createUUID()
	rev, err := db.Put(docid, body)
	if err != nil {
		docid = ""
	}
	return docid, rev, err
}

// Adds an existing revision to a document along with its history (list of rev IDs.)
// This is equivalent to the "new_edits":false mode of CouchDB.
func (db *Database) PutExistingRev(docid string, body Body, docHistory []string) error {
	currentRevIndex := len(docHistory)
	doc, err := db.getDoc(docid)
	if err != nil {
		if !isMissingDocError(err) {
			return err
		}
		// Creating new document:
		doc = newDocument()
		doc.ID = docid
	} else {
		// Find the point where this doc's history branches from the current rev:
		for i, revid := range docHistory {
			if doc.History.contains(revid) {
				currentRevIndex = i
				break
			}
		}
	}

	if currentRevIndex == 0 {
		return nil // No new revisions to add
	}

	deleted, _ := body["_deleted"].(bool)

	// Add all the new-to-me revisions to the rev tree:
	for i := currentRevIndex - 1; i >= 0; i-- {
		parent := ""
		if i+1 < len(docHistory) {
			parent = docHistory[i+1]
		}
		doc.History.addRevision(RevInfo{
			ID:      docHistory[i],
			Parent:  parent,
			Deleted: (i == 0 && deleted)})
	}
	doc.CurrentRev = doc.History.winningRevision()
	doc.Deleted = doc.History[doc.CurrentRev].Deleted

	// Save the document and body:
	return db.putDocAndBody(docid, docHistory[0], doc, body)
}

// Deletes a document, by adding a new revision whose "_deleted" property is true.
func (db *Database) DeleteDoc(docid string, revid string) (string, error) {
	body := Body{"_deleted": true, "_rev": revid}
	return db.Put(docid, body)
}

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
			log.Printf("WARNING: RevDiff(%q) --> %T %v", docid, err, err) //TEMP
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
		log.Panic("Failed to generate random ID: %s", err)
	}
	return fmt.Sprintf("%x", bytes)
}

func isMissingDocError(err error) bool {
	httpstatus, _ := ErrorAsHTTPStatus(err)
	return httpstatus == 404
}

// document.go -- document-oriented Database methods

package basecouch

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// The body of a CouchDB document as decoded from JSON.
type Body map[string]interface{}

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
type document struct {
	History  RevTree `json:"history"`
	Body     Body    `json:"current"`
	Sequence uint64  `json:"sequence"`
}

func (db *Database) realDocID(docid string) string {
    if docid=="" {
        return ""
    }
	docid = db.DocPrefix + docid
    if len(docid) > 250 {
        return ""
    }
    return docid
}

func newDocument() *document {
	return &document{History: make(RevTree), Body: make(Body)}
}

func (db *Database) getDoc(docid string) (*document, error) {
    key := db.realDocID(docid)
    if key == "" {
        return nil, &HTTPError{Status:400, Message: "Invalid doc ID"}
    }
	doc := newDocument()
	err := db.bucket.Get(key, doc)
	if err != nil {
		return nil, err
	}
	doc.Body["_id"] = docid
	return doc, nil
}

func (db *Database) setDoc(docid string, doc *document) error {
    key := db.realDocID(docid)
    if key == "" {
        return &HTTPError{Status:400, Message: "Invalid doc ID"}
    }
	return db.bucket.Set(key, 0, doc)
}

// Returns the body of a revision of a document.
func (db *Database) GetRev(docid, revid string, listRevisions bool) (Body, error) {
	doc, err := db.getDoc(docid)
	if doc == nil {
		return nil, err
	}
    if revid != "" {
        if revid != doc.Body["_rev"].(string) {
            //FIX: Need to support storing multiple revisions of docs
    		return nil, &HTTPError{Status: 404, Message: "missing"}
        }
    } else {
        revid = doc.Body["_rev"].(string)
    }
	if doc.Body["_deleted"] == true {
		return nil, &HTTPError{Status: 404, Message: "deleted"}
	}
    
    if listRevisions {
        history := doc.History.getHistory(revid)
        doc.Body["_revisions"] = encodeRevisions(history)
    }
	return doc.Body, nil
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
	} else {
		parentRev, _ := doc.Body["_rev"].(string)
		if matchRev != parentRev {
			return "", &HTTPError{Status: http.StatusConflict,
				Message: "Incorrect revision ID; should be " + parentRev}
		}
	}

	delete(body, "_id")

	// Make up a new _rev:
	generation,_ := parseRevID(matchRev)
	if generation < 0 {
		return "", &HTTPError{Status: http.StatusBadRequest, Message: "Invalid revision ID"}
	}
	newRev := createRevID(generation+1, body)

	stripSpecialProperties(body)
	body["_id"] = docid
	body["_rev"] = newRev
	doc.Body = body
	doc.History.addRevision(newRev, matchRev)
	doc.Sequence, err = db.generateSequence()
	if err != nil {
		return "", err
	}

	// Now finally put the new value:
	err = db.setDoc(docid, doc)
	if err != nil {
		return "", err
	}
	return newRev, nil
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
	currentRevIndex := -1
	doc, err := db.getDoc(docid)
	if err != nil {
		if !isMissingDocError(err) {
			return err
		}
		// Creating new document:
		doc = newDocument()
		currentRevIndex = len(docHistory)
		docHistory = append(docHistory, "")
	} else {
		// Find the point where this doc's history branches from the current rev:
		currentRev := doc.Body["_rev"]
		for i, revid := range docHistory {
			if revid == currentRev {
				currentRevIndex = i
				break
			}
		}
		if currentRevIndex < 0 {
			// Ouch. The input rev doesn't inherit from my current revision, so it creates a branch.
			// My data structure doesn't support storing multiple revision bodies yet.
			return &HTTPError{Status: http.StatusNotImplemented,
				Message: "Sorry, can't branch yet"}
		}
	}

	for i := currentRevIndex - 1; i >= 0; i-- {
		doc.History.addRevision(docHistory[i], docHistory[i+1])
	}
	stripSpecialProperties(body)
	body["_id"] = docid
	body["_rev"] = docHistory[0]
	doc.Body = body
	doc.Sequence, err = db.generateSequence()
	if err != nil {
		return err
	}
	return db.setDoc(docid, doc)
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
            log.Printf("WARNING: RevDiff(%q) --> %T %v", docid, err, err)//TEMP
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
			gen,_ := parseRevID(revid)
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
            gen,_ := parseRevID(revid)
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

// Gets a local document.
func (db *Database) GetLocal(docid string) (Body, error) {
    key := db.realLocalDocID(docid)
    if key == "" {
        return nil, &HTTPError{Status:400, Message: "Invalid doc ID"}
    }
    
	body := Body{}
	err := db.bucket.Get(key, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Updates a local document.
func (db *Database) PutLocal(docid string, body Body) error {
    key := db.realLocalDocID(docid)
    if key == "" {
        return &HTTPError{Status:400, Message: "Invalid doc ID"}
    }
    
	return db.bucket.Set(db.realLocalDocID(docid), 0, body)
}

// Deletes a local document.
func (db *Database) DeleteLocal(docid string) error {
    key := db.realLocalDocID(docid)
    if key == "" {
        return &HTTPError{Status:400, Message: "Invalid doc ID"}
    }
	return db.bucket.Delete(db.realLocalDocID(docid))
}

func (db *Database) realLocalDocID(docid string) string {
	return db.realDocID("_local/" + docid)
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

func createRevID(generation int, body Body) string {
	//FIX: digest is not just based on body; see TouchDB code
	//FIX: Use canonical JSON encoding
	json, _ := json.Marshal(body)
	digester := sha1.New()
	digester.Write(json)
	return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}

// Removes properties we don't want to store in the doc
func stripSpecialProperties(body Body) {
	for key, _ := range body {
		if key[0] == '_' && key != "_rev" && key != "_deleted" {
			delete(body, key)
		}
	}
}

func parseRevID(revid string) (int, string) {
	if revid == "" {
		return 0,""
	}
	var generation int
    var id string
	n, _ := fmt.Sscanf(revid, "%d-%s", &generation, &id)
	if n < 1 || generation < 1 {
        log.Printf("WARNING: parseRevID failed on %q", revid)
		return -1, ""
	}
	return generation, id
}

func isMissingDocError(err error) bool {
	httpstatus, _ := ErrorAsHTTPStatus(err)
	return httpstatus == 404
}

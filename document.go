// document.go

package couchglue

import (
    "crypto/rand"
    "crypto/sha1"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
)


// The body of a CouchDB document.
type Body  map[string] interface{}
    

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
type Document struct {
    History RevMap      `json:"history"`
    Body    Body        `json:"current"`
}


func NewDocument() *Document {
    return &Document{ History: make(RevMap), Body: make(Body) }
}


func (db *Database) getDoc(docid string) (*Document, error) {
    doc := NewDocument()
    err := db.bucket.Get(db.realDocID(docid), doc)
    if err != nil { return nil, err }
    doc.Body["_id"] = docid
    return doc, nil
}


// Returns the body of a document, or nil if there isn't one.
func (db *Database) Get(docid string) (Body, error) {
    doc, err := db.getDoc(docid)
    if doc == nil { return nil, err }
    if doc.Body["_deleted"] == true {
        return nil, &HTTPError{Status: 404, Message: "Deleted"}
    }
    return doc.Body, nil
}


// Stores a raw value in a document. Returns an HTTP status code.
func (db *Database) Put(docid string, body Body) (string, error) {
    // Verify that the _rev key in the body matches the current stored value:
    var matchRev string
    if body["_rev"] != nil {
        matchRev = body["_rev"].(string)
    }
    doc, err := db.getDoc(docid)
    if err != nil {
        if !isMissingDocError(err) { return "", err }
        if matchRev != "" {
            return "", &HTTPError{Status: http.StatusNotFound, Message: "No previous revision to replace"}
        }
        doc = NewDocument()
    } else {
        parentRev, _ := doc.Body["_rev"].(string)
        if matchRev != parentRev {
            return "", &HTTPError{Status: http.StatusConflict, Message: "Incorrect revision ID; should be " + parentRev}
        }
    }

    delete(body, "_id")
    
    // Make up a new _rev:
    generation := getRevGeneration(matchRev)
    if generation < 0 {
        return "", &HTTPError{Status: http.StatusBadRequest, Message: "Invalid revision ID"}
    }
    newRev := createRevID(generation+1, body)

    stripSpecialProperties(body)    
    body["_id"] = docid
    body["_rev"] = newRev
    doc.Body = body
    doc.History.addRevision(newRev, matchRev)

    // Now finally put the new value:
    err = db.bucket.Set(db.realDocID(docid), 0, doc)
    if err != nil { return "", err }
    return newRev, nil
}


func (db *Database) Post(body Body) (string, string, error) {
    if body["_rev"] != nil {
        return "", "", &HTTPError{Status: http.StatusNotFound, Message: "No previous revision to replace"}
    }
    docid := createUUID()
    rev, err := db.Put(docid, body)
    if err != nil { docid = "" }
    return docid, rev, err
}


func (db *Database) PutExistingRev(docid string, body Body, docHistory []string) error {
    currentRevIndex := -1
    doc, err := db.getDoc(docid)
    if err != nil {
        if !isMissingDocError(err) { return err }
        // Creating new document:
        doc = NewDocument()
        currentRevIndex = len(docHistory)
        docHistory = append(docHistory, "")
    } else {
        // Find the point where this doc's history branches from the current rev:
        currentRev := doc.Body["_rev"]
        for i, revid := range(docHistory) {
            if revid == currentRev {
                currentRevIndex = i
                break
            }
        }
        if currentRevIndex < 0 {
            // Ouch. The input rev doesn't inherit from my current revision, so it creates a branch.
            // My data structure doesn't support storing multiple revision bodies yet.
            return &HTTPError{Status: http.StatusNotImplemented, Message: "Sorry, can't branch yet"}
        }
    }
    
    
    for i := currentRevIndex - 1; i >= 0; i-- {
        doc.History.addRevision(docHistory[i], docHistory[i + 1])
    }
    stripSpecialProperties(body)    
    body["_id"] = docid
    body["_rev"] = docHistory[0]
    doc.Body = body
    return db.bucket.Set(db.realDocID(docid), 0, doc)
}


func (db *Database) DeleteDoc(docid string, revid string) (string, error) {
    body := Body{"_deleted": true, "_rev": revid}
    return db.Put(docid, body)
}


type RevsDiffInput map[string] []string


// Given a set of revisions, looks up which ones are not known.
// The input is a map from doc ID to array of revision IDs.
// The output is a map from doc ID to a map with "missing" and "possible_ancestors" arrays of rev IDs.
func (db *Database) RevsDiff(input RevsDiffInput) (map[string]interface{}, error) {
    // http://wiki.apache.org/couchdb/HttpPostRevsDiff
    output := make(map[string]interface{})
    for docid,revs := range(input) {
        missing, possible, err := db.RevDiff(docid, revs)
        if err != nil {return nil, err}
        if missing != nil {
            docOutput := map[string]interface{} { "missing": missing }
            if possible != nil {
                docOutput["possible_ancestors"] = possible
            }
            output[docid] = docOutput
        }
    }
    return output, nil
}


func (db *Database) RevDiff(docid string, revids []string) (missing, possible []string, err error) {
    doc, err := db.getDoc(docid)
    if err != nil {
        if isMissingDocError(err) {
            err = nil
            missing = revids
        }
        return
    }
    revmap := doc.History
    found := make(map[string]bool)
    maxMissingGen := 0
    for _,revid := range(revids) {
        if revmap.contains(revid) {
            found[revid] = true
        } else {
            if missing == nil {
                missing = make([]string, 0, 5)
            }
            gen := getRevGeneration(revid)
            if gen > 0 {
                missing = append(missing, revid)
                if gen > maxMissingGen { maxMissingGen = gen }
            }
        }
    }
    if missing != nil {
        possible = make([]string, 0, 5)
        for revid,_ := range(revmap) {
            if !found[revid] && getRevGeneration(revid) < maxMissingGen {
                possible = append(possible, revid)
            }
        }
        if len(possible) == 0 {
            possible = nil
        }
    }
    return
}


func (db *Database) realLocalDocID(docid string) string {
    return db.realDocID("_local/"+docid)
}


func (db *Database) GetLocal(docid string) (Body, error) {
    body := Body{}
    err := db.bucket.Get(db.realLocalDocID(docid), &body)
    if err != nil { return nil, err }
    return body, nil
}


func (db *Database) PutLocal(docid string, body Body) error {
    return db.bucket.Set(db.realLocalDocID(docid), 0, body)
}

func (db *Database) DeleteLocal(docid string) error {
    return db.bucket.Delete(db.realLocalDocID(docid))
}



func createUUID() string {
    bytes := make([]byte, 16)
    n, err := rand.Read(bytes)
    if n < 16 { log.Panic("Failed to generate random ID: %s", err) }
    return fmt.Sprintf("%x", bytes)
}


func createRevID(generation int, body Body) string {
    //FIX: digest is not just based on body; see TouchDB code
    //FIX: Use canonical JSON encoding
    json,_ := json.Marshal(body)
    digester := sha1.New()
    digester.Write(json)
    return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}


func stripSpecialProperties(body Body) {
    for key,_ := range(body) {
        if key[0] == '_' && key != "_rev" {
            delete(body, key)
        }
    }
}


func getRevGeneration(revid string) int {
    if revid == "" { return 0 }
    var generation int
    n, _ := fmt.Sscanf(revid, "%d-", &generation)
    if n < 1 || generation < 1 { return -1 }
    return generation
}


func isMissingDocError(err error) bool {
    httpstatus,_ := ErrorAsHTTPStatus(err)
    return httpstatus == 404
}
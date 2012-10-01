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
    rev     string
    history RevMap
    body    Body
}


func NewDocument() *Document {
    return &Document{ history: make(RevMap), body: make(Body) }
}


func (db *Database) getDoc (docid string) (*Document, error) {
    doc := NewDocument()
    err := db.bucket.Get(db.realDocID(docid), doc)
    if err != nil { return nil, err }
    doc.body["_id"] = docid
    return doc, nil
}


// Returns the body of a document, or nil if there isn't one.
func (db *Database) Get (docid string) (Body, error) {
    doc, err := db.getDoc(docid)
    if doc == nil { return nil, err }
    return doc.body, nil
}


// Stores a raw value in a document. Returns an HTTP status code.
func (db *Database) Put (docid string, body Body) (string, error) {
    // Verify that the _rev key in the body matches the current stored value:
    var matchRev string
    if body["_rev"] != nil {
        matchRev = body["_rev"].(string)
    }
    doc, err := db.getDoc(docid)
    if err != nil {
        httpstatus,_ := ErrorAsHTTPStatus(err)
        if httpstatus != 404 { return "", err }
        if matchRev != "" {
            return "", &HTTPError{Status: http.StatusNotFound, Message: "No previous revision to replace"}
        }
        doc = NewDocument()
    } else {
        parentRev, _ := doc.body["_rev"].(string)
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
    
    body["_rev"] = newRev
    doc.body = body
    doc.history.addRevision(newRev, matchRev)
    log.Printf("PUTting Doc = %v", doc)//TEMP

    // Now finally put the new value:
    err = db.bucket.Set(db.realDocID(docid), 0, doc)
    if err != nil { return "", err }
    return newRev, nil
}


func (db *Database) Post (body Body) (string, string, error) {
    if body["_rev"] != nil {
        return "", "", &HTTPError{Status: http.StatusNotFound, Message: "No previous revision to replace"}
    }
    docid := createUUID()
    rev, err := db.Put(docid, body)
    if err != nil { docid = "" }
    return docid, rev, err
}


type RevsDiffInput map[string] []string


// Given a set of revisions, looks up which ones are not known.
// The input is a map from doc ID to array of revision IDs.
// The output is a map from doc ID to a map with "missing" and "possible_ancestors" arrays of rev IDs.
func (db *Database) RevsDiff (input RevsDiffInput) (map[string]interface{}, error) {
    // http://wiki.apache.org/couchdb/HttpPostRevsDiff
    output := make(map[string]interface{})
    for docid,revs := range(input) {
        missing, possible, err := db.RevDiff(docid, revs)
        if err != nil {return nil, err}
        if missing != nil {
            output[docid] = map[string]interface{} { "missing": missing, "possible_ancestors": possible }
        }
    }
    return output, nil
}


func (db *Database) RevDiff (docid string, revids []string) (missing, possible []string, err error) {
    doc, err := db.getDoc(docid)
    if err != nil { return }
    revmap := doc.history
    for _,revid := range(revids) {
        if !revmap.contains(revid) {
            if missing == nil {
                missing = make([]string, 0, 5)
            }
            missing = append(missing, revid)
        }
    }
    if len(missing) > 0 {
        possible = revmap.getLeaves()
    }
    return
}


func createUUID() string {
    bytes := make([]byte, 20)
    n, err := rand.Read(bytes)
    if n < 20 { log.Panic("Failed to generate random ID: %s", err) }
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


func getRevGeneration(revid string) int {
    if revid == "" { return 0 }
    var generation int
    n, _ := fmt.Sscanf(revid, "%d-", &generation)
    if n < 1 || generation < 1 { return -1 }
    return generation
}

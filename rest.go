package couchglue

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "strings"
    
    "github.com/couchbaselabs/go-couchbase"
)


// Parses a CouchDB _revisions property into a list of revision IDs
func parseRevisions(body Body) []string {
    revisions, ok := body["_revisions"].(map[string]interface{})
    if !ok {
        log.Printf("WARNING: Unable to parse _revisions: %v", body["_revisions"])
        return nil
    }
    start := int(revisions["start"].(float64))
    ids := revisions["ids"].([]interface{})
    if start <= len(ids) { return nil }
    result := make([]string, 0, len(ids))
    for _,id := range(ids) {
        result = append(result, fmt.Sprintf("%d-%s", start, id))
        start--
    }
    return result
}


func readJSONInto(rq *http.Request, into interface{}) error {
    body, err := ioutil.ReadAll(rq.Body)
    if err != nil { return &HTTPError{Status: http.StatusBadRequest} }
    err = json.Unmarshal(body, into)
    if err != nil { return &HTTPError{Status: http.StatusBadRequest, Message: "Bad JSON"} }
    return nil
}

func readJSON(rq *http.Request) (Body, error) {
    var body Body
    return body, readJSONInto(rq, &body)
}


func writeJSON(value interface{}, r http.ResponseWriter) {
    json, err := json.Marshal(value)
    if err != nil {
        log.Printf("WARNING: Couldn't serialize JSON for %v", value)
        r.WriteHeader(http.StatusInternalServerError)
    } else {
        r.Header().Set("Content-Type", "application/json")
        r.Write(json)
    }
}


func writeError(err error, r http.ResponseWriter) {
    if err != nil {
        status, message := ErrorAsHTTPStatus(err)
        r.WriteHeader(status)
        info := Body{"error": status, "reason": message}
        json,_ := json.Marshal(info)
        r.Write(json)
        log.Printf("Returning response %d: %s", status, message)
    }
}


func (db *Database) HandleGetDoc(r http.ResponseWriter, rq *http.Request, docid string) {
    value, err := db.Get(docid)
    if err != nil {
        writeError(err, r)
        return
    }
    if value == nil {
        r.WriteHeader(http.StatusNotFound)
        return
    }
    r.Header().Set("Etag", value["_rev"].(string))
    writeJSON(value, r)
}


func (db *Database) HandlePutDoc(r http.ResponseWriter, rq *http.Request, docid string) {
    body, err := readJSON(rq)
    if err != nil {
        writeError(err, r)
        return
    }
    
    query := rq.URL.Query()
    if query.Get("new_edits") != "false" {
        // Regular PUT:
        newRev, err := db.Put(docid, body)
        if err != nil {
            writeError(err, r)
            return
        }
        r.Header().Set("Etag", newRev)
        writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r)
    } else {
        // Replicator-style PUT with new_edits=false:
        revisions := parseRevisions(body)
        if revisions == nil {
            writeError(&HTTPError{Status: http.StatusBadRequest, Message: "Bad _revisions"}, r)
        }
        err := db.PutExistingRev(docid, body, revisions)
        if err != nil { writeError(err, r) }
    }    
    r.WriteHeader(http.StatusCreated)
}


func (db *Database) HandlePostDoc(r http.ResponseWriter, rq *http.Request) {
    body, err := readJSON(rq)
    if err != nil {
        writeError(err, r)
        return
    }
    docid, newRev, err := db.Post(body)
    if err != nil {
        writeError(err, r)
        return
    }
    r.Header().Set("Location", docid)
    r.Header().Set("Etag", newRev)
    writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r)
}


func (db *Database) HandleDeleteDoc(r http.ResponseWriter, rq *http.Request, docid string) {
    revid := rq.URL.Query().Get("rev")
    newRev, err := db.DeleteDoc(docid, revid)
    if err != nil {
        writeError(err, r)
        return
    }
    writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r)
}


func (db *Database) HandleBulkDocs(r http.ResponseWriter, rq *http.Request) {
    body, err := readJSON(rq)
    if err != nil {
        writeError(err, r)
        return
    }
    newEdits,ok := body["new_edits"].(bool)
    if !ok {newEdits = true}
    
    result := make([]Body, 0, 5)
    for _,item := range(body["docs"].([]interface{})) {
        doc := item.(map[string]interface{})
        docid,_ := doc["_id"].(string)
        var err error
        var revid string
        if newEdits {
            if docid != "" {
                revid, err = db.Put(docid, doc)
            } else {
                docid, revid, err = db.Post(doc)
            }
        } else {
            revisions := parseRevisions(doc)
            if revisions == nil {
                err = &HTTPError{Status: http.StatusBadRequest, Message: "Bad _revisions"}
            } else {
                revid = revisions[0]
                err = db.PutExistingRev(docid, doc, revisions)
            }
        }
        
        status:= Body{}
        if docid != "" {
            status["id"] = docid
        }
        if err != nil {
            _, msg := ErrorAsHTTPStatus(err)
            status["error"] = msg
        } else {
            status["rev"] = revid
        }
        result = append(result, status)
    }

    r.WriteHeader(http.StatusCreated)
    writeJSON(Body{"docs": result}, r)
}


// Handles HTTP requests for a database.
func (db *Database) Handle(r http.ResponseWriter, rq *http.Request, path []string) {
    method := rq.Method
    switch len(path) {
        case 0: {
            // Root level
            switch method {
            case "GET":
                response := make(map[string]interface{})
                response["db_name"] = db.Name
                response["doc_count"] = db.DocCount()
                writeJSON(response,r)
                return
            case "POST":
                db.HandlePostDoc(r, rq)
                return
            case "DELETE":
                writeError(db.Delete(), r)
                r.Write([]byte("ok"))
                return
            }
        }
        case 1: {
            docid := path[0]
            log.Printf("%s %s %s\n", db.Name, method, docid)
            switch docid {
                case "_all_docs": {
                    if method == "GET" {
                        ids, err := db.AllDocIDs()
                        if err != nil {
                            r.WriteHeader(http.StatusInternalServerError)
                            return
                        }
                        writeJSON(ids, r)
                        return
                    }
                }
                case "_bulk_docs": {
                    if method == "POST" {
                        db.HandleBulkDocs(r, rq)
                        return
                    }
                }
                case "_revs_diff": {
                    if method == "POST" {
                        var input RevsDiffInput
                        err := readJSONInto(rq, &input)
                        if err != nil {
                            writeError(err, r)
                            return
                        }
                        output, err := db.RevsDiff(input)
                        writeJSON(output, r)
                        if err != nil {
                            writeError(err, r)
                        }
                        return
                    }
                }
                default: {
                    if docid[0] != '_' {
                        // Accessing a document:
                        switch method {
                        case "GET":
                            db.HandleGetDoc(r, rq, docid)
                            return
                        case "PUT":
                            db.HandlePutDoc(r, rq, docid)
                            return
                        case "DELETE":
                            db.HandleDeleteDoc(r, rq, docid)
                            return
                        }
                    }
                }
            }
        }
    }
    // Fall through to here if the request was not recognized:
    log.Printf("WARNING: Unhandled %s %s\n", method, rq.URL)
    r.WriteHeader(http.StatusBadRequest)
}


func handleRoot(r http.ResponseWriter, rq *http.Request) {
    if rq.Method == "GET" {
        response := map[string]string {
            "couchdb": "welcome",
            "version": "CouchGlue 0.0",
        }
        writeJSON(response, r)
    } else {
        r.WriteHeader(http.StatusBadRequest)
    }
}





// Creates an http.Handler that will handle the REST API for the given bucket.
func NewRESTHandler(bucket *couchbase.Bucket) http.Handler {
    return http.HandlerFunc(func (r http.ResponseWriter, rq *http.Request) {
        path := strings.Split(rq.URL.Path[1:], "/")
        for len(path) > 0 && path[len(path)-1] == "" {
            path = path[0:len(path)-1]
        }
        if len(path) == 0 {
            handleRoot(r, rq)
            return
        }
        dbName := path[0]
        
        if rq.Method == "PUT" && len(path) == 1 {
            _, err := CreateDatabase(bucket, dbName)
            if err != nil {
                writeError(err, r)
                return
            }
            r.WriteHeader(http.StatusCreated)
            return
        } else {
            db, err := GetDatabase(bucket, dbName)
            if err != nil {
                writeError(err, r)
                return
            }
            db.Handle(r, rq, path[1:])
        }
    })
}


// Initialize REST handlers. Call this once on launch.
func InitREST(bucket *couchbase.Bucket) {
    http.Handle("/", NewRESTHandler(bucket))
}

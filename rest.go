package couchglue

import "encoding/json"
import "io/ioutil"
import "log"
import "net/http"
import "strings"

import "github.com/couchbaselabs/go-couchbase"


func readJSONInto(rq *http.Request, into interface{}) *HTTPError {
    body, err := ioutil.ReadAll(rq.Body)
    if err != nil { return &HTTPError{Status: http.StatusBadRequest} }
    err = json.Unmarshal(body, into)
    if err != nil { return &HTTPError{Status: http.StatusBadRequest, Message: "Bad JSON"} }
    return nil
}

func readJSON(rq *http.Request) (Body, *HTTPError) {
    var body Body
    err := readJSONInto(rq, &body)
    if err != nil { return nil, err }
    return body, nil
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


func writeError(err *HTTPError, r http.ResponseWriter) {
    if err != nil {
        r.WriteHeader(err.Status)
        info := Body{"error": err.Status, "reason": err.Error()}
        json,_ := json.Marshal(info)
        r.Write(json)
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
    newRev, err := db.Put(docid, body)
    if err != nil {
        writeError(err, r)
        return
    }
    r.Header().Set("Etag", newRev)
    writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r)
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


// Handles HTTP requests for a database.
func (db *Database) Handle(r http.ResponseWriter, rq *http.Request, path []string) {
    method := rq.Method
    switch len(path) {
        case 0: {
            // Root level
            if method == "GET" {
                response := make(map[string]interface{})
                response["db_name"] = db.Name
                response["doc_count"] = db.DocCount()
                writeJSON(response,r)
                return
            } else if method == "POST" {
                db.HandlePostDoc(r, rq)
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
                        if method == "GET" {
                            db.HandleGetDoc(r, rq, docid)
                            return
                        } else if method == "PUT" {
                            db.HandlePutDoc(r, rq, docid)
                            return
                        }
                    }
                }
            }
        }
        default:
    }
    // Fall through to here if the request was not recognized:
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


// Initialize REST handlers. Call this once on launch.
func InitREST(bucket *couchbase.Bucket) {
    http.HandleFunc("/", func (r http.ResponseWriter, rq *http.Request) {
        path := strings.Split(rq.URL.Path[1:], "/")
        for len(path) > 0 && path[len(path)-1] == "" {
            path = path[0:len(path)-1]
        }
        log.Printf("%s %v (%d)", rq.Method, path, len(path))
        if len(path) == 0 {
            handleRoot(r, rq)
            return
        }
        
        dbName := path[0]
        log.Printf("dbName = %s", dbName)
        
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

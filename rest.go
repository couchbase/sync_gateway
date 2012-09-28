package couchglue

import "encoding/json"
import "io/ioutil"
import "log"
import "net/http"
import "strings"

import "github.com/couchbaselabs/go-couchbase"


func readJSON(rq *http.Request) (Body, *HTTPError) {
    body, err := ioutil.ReadAll(rq.Body)
    if err != nil { return nil, &HTTPError{Status: http.StatusBadRequest} }
    var parsed Body
    err = json.Unmarshal(body, &parsed)
    if err != nil { return nil, &HTTPError{Status: http.StatusBadRequest} }
    return parsed, nil
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




// Handles HTTP requests for a database.
func (db *Database) Handle(r http.ResponseWriter, rq *http.Request, path []string) {
    method := rq.Method
    docid := path[0]
    log.Printf("%s %s %s\n", db.Name, method, docid)
    switch docid {
        case "": {
            // Root level
            if method == "GET" {
                response := make(map[string]interface{})
                response["db_name"] = db.Name
                response["doc_count"] = db.DocCount()
                writeJSON(response,r)
                return
            }
        }
        case "_all_docs": {
            ids, err := db.AllDocIDs()
            if err != nil {
                r.WriteHeader(http.StatusInternalServerError)
                return
            }
            writeJSON(ids, r)
            return
        }
        case "_revs_diff": {
            if method == "POST" {
                revs, err := readJSON(rq)
                if err != nil {
                    r.WriteHeader(http.StatusBadRequest)
                    return
                }
                writeJSON(db.RevsDiff(revs), r)
                return
            }
        }
        default: {
            // Accessing a document:
            if method == "GET" {
                value, err := db.Get(docid)
                if err != nil {
                    r.WriteHeader(http.StatusInternalServerError)
                    return
                }
                if value == nil {
                    r.WriteHeader(http.StatusNotFound)
                    return
                }
                writeJSON(value, r)
                return
            } else if method == "PUT" {
                body, err := readJSON(rq)
                if err != nil {
                    r.WriteHeader(http.StatusBadRequest)
                    return
                }
                err = db.Put(docid, body)
                if err != nil { r.WriteHeader(err.Status) }
                return
            }
        }
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
        log.Printf("%s %v", rq.Method, path)
        if len(path) == 0 || path[0] == "" {
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

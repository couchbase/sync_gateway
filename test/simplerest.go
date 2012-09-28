package couchglue

import "encoding/json"
import "io/ioutil"
import "log"
import "net/http"


func ReadJSON(rq *http.Request) (interface{}, error) {
    body, err := ioutil.ReadAll(rq.Body)
    if err != nil { return nil, err }
    var parsed interface{}
    err = json.Unmarshal(body, &parsed)
    if err != nil { return nil, err }
    return parsed, nil
}


type database struct {
    name string
    docs map[string][]byte
}


func NewDB(name string) *database {
    db := new(database)
    db.name = name
    db.docs = make(map[string][]byte)
    return db
}


// Registers an HTTP handler for a database.
func (db *database) AddHandler() {
    path := "/" + db.name
    http.Handle(path + "/", http.StripPrefix(path, db))
}


// Returns all document IDs as an array.
func (db *database) AllDocIDs() []string {
    result := make([]string, 0, len(db.docs))
    for docid,_ := range(db.docs) {
        result = append(result, docid)
    }
    return result
}


// Stores a raw value in a document. Returns an HTTP status code.
func (db *database) PutDocument (docid string, value []byte) int {
    var doc interface{}
    err := json.Unmarshal(value, &doc)
    if err != nil {
        log.Printf("PutDocument: JSON error %s\n", err)
        return http.StatusBadRequest
    }

    value, _ = json.Marshal(doc)
    db.docs[docid] = value
    return http.StatusCreated
}


// Returns the raw value of a document, or nil if there isn't one.
func (db *database) GetDocument (docid string) []byte {
    return db.docs[docid]
}


func (db *database) RevsDiff (revs interface{}) interface{} {
    // http://wiki.apache.org/couchdb/HttpPostRevsDiff
    return nil //TEMP
}


// Handles HTTP requests for a database.
func (db *database) ServeHTTP (r http.ResponseWriter, rq *http.Request) {
    method := rq.Method
    docid := rq.URL.Path[1:]
    log.Printf("%s %s %s\n", db.name, method, docid)
    switch docid {
        case "": {
            // Root level
            if method == "GET" {
                response := make(map[string]interface{})
                response["db_name"] = db.name
                response["doc_count"] = len(db.docs)
                json, _ := json.Marshal(response)
                r.Write(json)
                return
            }
        }
        case "_all_docs": {
            json, _ := json.Marshal(db.AllDocIDs())
            r.Write(json)
            return
        }
        case "_revs_diff": {
            if method == "POST" {
                revs, err = ReadJSON(rq)
                if err != nil {
                    r.WriteHeader(http.BadRequest)
                    return
                }
                json, _ := json.Marshal(db.RevsDiff(revs))
                r.Write(json)
            }
        }
        default: {
            if method == "GET" {
                value := db.GetDocument(docid)
                if value == nil {
                    r.WriteHeader(http.StatusNotFound)
                    return
                }
                r.Write(value)
                return
            } else if method == "PUT" {
                value, err := ioutil.ReadAll(rq.Body)
                if err != nil {
                    r.WriteHeader(http.StatusInternalServerError)
                    return
                }
                status := db.PutDocument(docid, value)
                r.WriteHeader(status)
                return
            }
        }
    }
    // Fall through to here if the request was not recognized:
    r.WriteHeader(http.StatusBadRequest)
}


// Handles requests for the root or for nonexistent database names.
func handleNoDb (r http.ResponseWriter, rq *http.Request) {
    method := rq.Method
    dbname:= rq.URL.Path[1:]
    if dbname == "" {
        if method == "GET" {
            response := make(map[string]string)
            response["couchdb"] = "welcome"
            response["version"] = "0.0"
            response["CouchGlue"] = "howdy"
            json, _ := json.Marshal(response)
            r.Write(json)
            return
        }
    } else if method == "PUT" {
        NewDB(dbname).AddHandler()
        r.WriteHeader(http.StatusCreated)
        return
    } else if method == "GET" {
        r.WriteHeader(http.StatusNotFound)
        return
    }
    r.WriteHeader(http.StatusBadRequest)
}


func main() {
    http.HandleFunc("/", handleNoDb)
    NewDB("db1").AddHandler()
    NewDB("db2").AddHandler()
    log.Printf("Starting...\n")
    err := http.ListenAndServe(":8000", nil)
    if err != nil {
        log.Fatal("Server failed: ", err.Error());
    }
}

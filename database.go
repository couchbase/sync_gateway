// Database.go -- simulates a CouchDB Database with Couchbase Server

package couchglue

import "crypto/rand"
import "crypto/sha1"
import "encoding/json"
import "fmt"
import "log"
import "net/http"
import "regexp"

import "github.com/couchbaselabs/go-couchbase"
import "github.com/dustin/gomemcached"


const kExpiration = 0  //FIX: What should this be??

var kDBNameMatch = regexp.MustCompile("[-%+()$_a-z0-9]+")

var DefaultBucket *couchbase.Bucket


type Body  map[string] interface{}
    
type HTTPError struct {
    error
    Status  int
    Message string
}

func (err *HTTPError) Error() string {
    return err.Message
}


type Database struct {
    Name    string
    bucket  *couchbase.Bucket
}


func makeDatabase(bucket *couchbase.Bucket, name string) *Database {
    if (bucket == nil) { bucket = DefaultBucket; }
    if !kDBNameMatch.MatchString(name) { return nil }
    db := new(Database)
    db.bucket = bucket
    db.Name = name
    return db
}


func GetDatabase(bucket *couchbase.Bucket, name string) (*Database, *HTTPError) {
    if (bucket == nil) { bucket = DefaultBucket; }
    if !kDBNameMatch.MatchString(name) {
         return nil, &HTTPError{Status: 400, Message: "Illegal database name"}
    }
    var body Body
    err := bucket.Get(name, &body)
    if err != nil { return nil, convertError(err) }
    return makeDatabase(bucket, name), nil
}


func CreateDatabase(bucket *couchbase.Bucket, name string) (*Database, *HTTPError) {
    if (bucket == nil) { bucket = DefaultBucket; }
    if !kDBNameMatch.MatchString(name) {
        return nil, &HTTPError{Status: 400, Message: "Illegal database name"}
    }
    var body Body
    err := bucket.Get(name, &body)
    if err == nil { return nil, &HTTPError{Status: 412} }
    body = make(Body)
    err = bucket.Set(name, kExpiration, body)
    if err != nil {
        return nil, convertError(err)
    }
    return makeDatabase(bucket, name), nil
}


func (db *Database) realDocID (docid string) string {
    return db.Name + ":" + docid
}


func (db *Database) DocCount() int {
    vres, err := db.bucket.View("couchdb", "all_docs", db.allDocIDsOpts())
    if (err != nil) { return -1}
    return vres.TotalRows
}

func (db *Database) allDocIDsOpts() Body {
    startkey := [1]string{db.Name}
    endkey := [2]interface{}{db.Name, make(Body)}
    return Body{"startkey": startkey, "endkey": endkey}
}


// Returns all document IDs as an array.
func (db *Database) AllDocIDs() ([]string, *HTTPError) {
    vres, err := db.bucket.View("couchdb", "all_docs", db.allDocIDsOpts())
    if (err != nil) { return nil, convertError(err)}
    
    rows := vres.Rows
    docids := make([]string, len(rows))
    for _,row := range(rows) {
        key := row.Key.([]interface{})
        docids = append(docids, key[1].(string))
    }
    return docids, nil
}


func (db *Database) RevsDiff (revs interface{}) interface{} {
    // http://wiki.apache.org/couchdb/HttpPostRevsDiff
    return nil //TEMP
}


// Returns the raw value of a document, or nil if there isn't one.
func (db *Database) Get (docid string) (Body, *HTTPError) {
    var body Body
    err := db.bucket.Get(db.realDocID(docid), &body)
    if err != nil { return nil, convertError(err) }
    body["_id"] = docid
    return body, nil
}


// Stores a raw value in a document. Returns an HTTP status code.
func (db *Database) Put (docid string, body Body) (string, *HTTPError) {
    // Verify that the _rev key in the body matches the current stored value:
    var matchRev string
    if body["_rev"] != nil {
        matchRev = body["_rev"].(string)
    }
    oldBody, err := db.Get(docid)
    if err != nil {
        if err.Status != 404 { return "", err }
        if matchRev != "" {
            return "", &HTTPError{Status: http.StatusNotFound, Message: "No previous revision to replace"}
        }
    } else {
        if matchRev != oldBody["_rev"] {
            return "", &HTTPError{Status: http.StatusConflict, Message: "Incorrect revision ID"}
        }
    }

    delete(body, "_id")
    
    // Make up a new _rev:
    generation := 0
    if matchRev != "" {
        n, _ := fmt.Sscanf(matchRev, "%d-", &generation)
        if n < 1 || generation < 1 {
            return "", &HTTPError{Status: http.StatusBadRequest, Message: "Invalid revision ID"}
        }
    }
    json,_ := json.Marshal(body)
    digester := sha1.New()
    digester.Write(json)
    digest := digester.Sum(nil)
    
    newRev := fmt.Sprintf("%d-%x", generation+1, digest)
    
    body["_rev"] = newRev

    // Now finally put the new value:
    err = convertError( db.bucket.Set(db.realDocID(docid), kExpiration, body) )
    if err != nil { return "", err }
    return newRev, nil
}


func createUUID() string {
    bytes := make([]byte, 20)
    n, err := rand.Read(bytes)
    if n < 20 { log.Panic("Failed to generate random ID: %s", err) }
    return fmt.Sprintf("%x", bytes)
}


func (db *Database) Post (body Body) (string, string, *HTTPError) {
    if body["_rev"] != nil {
        return "", "", &HTTPError{Status: http.StatusNotFound, Message: "No previous revision to replace"}
    }
    docid := createUUID()
    rev, err := db.Put(docid, body)
    if err != nil { docid = "" }
    return docid, rev, err
}


func convertError(err error) *HTTPError {
    if err == nil { return nil }
    var status int
	switch err.(type) {
    	case *gomemcached.MCResponse:
    		switch err.(*gomemcached.MCResponse).Status {
                case gomemcached.KEY_ENOENT:     status = http.StatusNotFound
                case gomemcached.KEY_EEXISTS:    status = http.StatusConflict
                default:                         status = http.StatusBadGateway
            }
    	default:
            log.Printf("WARNING: Couldn't interpret error type %T, value %v", err, err)
    		status = http.StatusInternalServerError
    }
    return &HTTPError{Status: status, Message: err.Error()}
}

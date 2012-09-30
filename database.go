// Database.go -- simulates a CouchDB Database with Couchbase Server

package couchglue

import "log"
import "net/http"
import "regexp"

import "github.com/couchbaselabs/go-couchbase"
import "github.com/dustin/gomemcached"


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
    err = bucket.Set(name, 0, body)
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

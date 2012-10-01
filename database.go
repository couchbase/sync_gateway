// Database.go -- simulates a CouchDB Database with Couchbase Server

package couchglue

import (
    "fmt"
    "log"
    "net/http"
    "regexp"

    "github.com/couchbaselabs/go-couchbase"
    "github.com/dustin/gomemcached"
)

var kDBNameMatch = regexp.MustCompile("[-%+()$_a-z0-9]+")

var DefaultBucket *couchbase.Bucket


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


func ConnectToBucket(couchbaseURL, poolName, bucketName string) (bucket *couchbase.Bucket, err error) {
	c, err := couchbase.Connect(couchbaseURL )
	if err != nil {return}
	pool, err := c.GetPool(poolName)
	if err != nil {return}
	bucket, err = pool.GetBucket(bucketName)
	if err != nil {return}
    fmt.Printf("Connected to <%s>, pool %s, bucket %s", couchbaseURL, poolName, bucketName)
    err = nil
    return
}


func makeDatabase(bucket *couchbase.Bucket, name string) *Database {
    if (bucket == nil) { bucket = DefaultBucket; }
    if !kDBNameMatch.MatchString(name) { return nil }
    db := new(Database)
    db.bucket = bucket
    db.Name = name
    return db
}


func GetDatabase(bucket *couchbase.Bucket, name string) (*Database, error) {
    if (bucket == nil) { bucket = DefaultBucket; }
    if !kDBNameMatch.MatchString(name) {
         return nil, &HTTPError{Status: 400, Message: "Illegal database name"}
    }
    var body Body
    err := bucket.Get(name, &body)
    if err != nil { return nil, err }
    return makeDatabase(bucket, name), nil
}


func CreateDatabase(bucket *couchbase.Bucket, name string) (*Database, error) {
    if (bucket == nil) { bucket = DefaultBucket; }
    if !kDBNameMatch.MatchString(name) {
        return nil, &HTTPError{Status: 400, Message: "Illegal database name"}
    }
    var body Body
    err := bucket.Get(name, &body)
    if err == nil { return nil, &HTTPError{Status: 412, Message: "Database already exists"} }
    body = make(Body)
    err = bucket.Set(name, 0, body)
    if err != nil {
        return nil, err
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
func (db *Database) AllDocIDs() ([]string, error) {
    vres, err := db.bucket.View("couchdb", "all_docs", db.allDocIDsOpts())
    if (err != nil) { return nil, err}
    
    rows := vres.Rows
    docids := make([]string, len(rows))
    for _,row := range(rows) {
        key := row.Key.([]interface{})
        docids = append(docids, key[1].(string))
    }
    return docids, nil
}


func (db *Database) Delete() error {
    docIDs, err := db.AllDocIDs()
    if err != nil {return err}
    //FIX: Is there a way to do this in one operation?
    err = db.bucket.Delete(db.Name)
    if err != nil {return err}
    for _,docID := range(docIDs) {
        db.bucket.Delete(docID)
    }
    return nil
}


func ErrorAsHTTPStatus(err error) (int, string) {
    if err == nil { return 200, "OK" }
	switch err := err.(type) {
        case *HTTPError:
            return err.Status, err.Message
    	case *gomemcached.MCResponse:
    		switch err.Status {
                case gomemcached.KEY_ENOENT:     return http.StatusNotFound, "Not Found"
                case gomemcached.KEY_EEXISTS:    return http.StatusConflict, "Conflict"
                default:                         return http.StatusBadGateway, fmt.Sprintf("MC status %d", err.Status)
            }
    	default:
            log.Printf("WARNING: Couldn't interpret error type %T, value %v", err, err)
    		return http.StatusInternalServerError, fmt.Sprintf("Go error: %v", err)
    }
    panic("unreachable")
}

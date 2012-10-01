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


type HTTPError struct {
    error
    Status  int
    Message string
}

func (err *HTTPError) Error() string {
    return err.Message
}


type Database struct {
    Name        string              `json:"name"`
    DocPrefix   string              `json:"docPrefix"`
    bucket      *couchbase.Bucket
}


// Helper function to open a Couchbase connection and return a specific bucket.
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


func dbInternalDocName(dbName string) string {
    if !kDBNameMatch.MatchString(dbName) {
         return ""
    }
    return "cdb:" + dbName
}


// Makes a Database object given its name and bucket. Returns nil if there is no such database.
func GetDatabase(bucket *couchbase.Bucket, name string) (*Database, error) {
    docname := dbInternalDocName(name)
    if docname == "" {
         return nil, &HTTPError{Status: 400, Message: "Illegal database name"}
    }
    var db Database
    err := bucket.Get(docname, &db)
    if err != nil { return nil, err }
    db.bucket = bucket
    return &db, nil
}


// Creates a new database in a bucket and returns a Database object for it. Fails if the database exists.
func CreateDatabase(bucket *couchbase.Bucket, name string) (*Database, error) {
    docname := dbInternalDocName(name)
    if docname == "" {
         return nil, &HTTPError{Status: 400, Message: "Illegal database name"}
    }
    var db Database
    err := bucket.Get(docname, &db)
    if err == nil { return nil, &HTTPError{Status: 412, Message: "Database already exists"} }

    db = Database{bucket: bucket, Name: name, DocPrefix: fmt.Sprintf("doc:%s/%s:", name, createUUID())}
    err = bucket.Set(docname, 0, db)
    if err != nil {
        return nil, err
    }
    return &db, nil
}


func (db *Database) realDocID (docid string) string {
    return db.DocPrefix + docid
}


func (db *Database) DocCount() int {
    vres, err := db.bucket.View("couchdb", "all_docs", db.allDocIDsOpts())
    if (err != nil) { return -1}
    return vres.TotalRows
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


func (db *Database) allDocIDsOpts() Body {
    startkey := [1]string{db.DocPrefix}
    endkey := [2]interface{}{db.DocPrefix, make(Body)}
    return Body{"startkey": startkey, "endkey": endkey}
}


// Deletes a database (and all documents)
func (db *Database) Delete() error {
    docIDs, err := db.AllDocIDs()
    if err != nil {return err}
    //FIX: Is there a way to do this in one operation?
    err = db.bucket.Delete(dbInternalDocName(db.Name))
    if err != nil {return err}
    for _,docID := range(docIDs) {
        db.bucket.Delete(docID)
    }
    return nil
}


// Attempts to map an error to an HTTP status code and message. Defaults to 500.
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
    		return http.StatusInternalServerError, fmt.Sprintf("Internal error: %v", err)
    }
    panic("unreachable")
}

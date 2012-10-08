// database.go -- simulates a CouchDB Database with Couchbase Server

package basecouch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/gomemcached"
)

var kDBNameMatch = regexp.MustCompile("[-%+()$_a-z0-9]+")

// Simple error implementation wrapping an HTTP response status.
type HTTPError struct {
	Status  int
	Message string
}

func (err *HTTPError) Error() string {
	return err.Message
}

// Represents a simulated CouchDB database.
type Database struct {
	Name      string `json:"name"`
	DocPrefix string `json:"docPrefix"`
	bucket    *couchbase.Bucket
}

// Helper function to open a Couchbase connection and return a specific bucket.
func ConnectToBucket(couchbaseURL, poolName, bucketName string) (bucket *couchbase.Bucket, err error) {
	bucket, err = couchbase.GetBucket(couchbaseURL, poolName, bucketName)
	if err != nil {
		return
	}
	log.Printf("Connected to <%s>, pool %s, bucket %s", couchbaseURL, poolName, bucketName)
	err = installViews(bucket)
	return
}

// Returns the Couchbase docID of the database's main document
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
	if err != nil {
		return nil, err
	}
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
	if err == nil {
		return nil, &HTTPError{Status: 412, Message: "Database already exists"}
	}

	db = Database{bucket: bucket, Name: name, DocPrefix: fmt.Sprintf("doc:%s/%s:", name, createUUID())}
	err = bucket.Set(docname, 0, db)
	if err != nil {
		return nil, err
	}
	return &db, nil
}

// The UUID assigned to this database.
func (db *Database) UUID() string {
	return db.DocPrefix[4 : len(db.DocPrefix)-1]
}

//////// ALL DOCUMENTS:

// The number of documents in the database.
func (db *Database) DocCount() int {
	vres, err := db.bucket.View("couchdb", "all_docs", db.allDocIDsOpts(true))
	if err != nil {
		return -1
	}
	return int(vres.Rows[0].Value.(float64))
}

func installViews(bucket *couchbase.Bucket) error {
	node := bucket.Nodes[rand.Intn(len(bucket.Nodes))]
	u, err := url.Parse(node.CouchAPIBase)
	if err != nil {
		fmt.Printf("Failed to parse %s", node.CouchAPIBase)
		return err
	}
	u.Path = fmt.Sprintf("/%s/_design/%s", bucket.Name, "couchdb")

	//FIX: This view includes local docs; it shouldn't!
	alldbs_map := `function (doc, meta) {
                     var pieces = meta.id.split(":");
                     if (pieces.length != 2 || pieces[0] != "cdb")
                        return;
                     emit(doc.name, doc.docPrefix); }`
	alldocs_map := `function (doc, meta) {
                     var pieces = meta.id.split(":", 3);
                     if (pieces.length < 3 || pieces[0] != "doc")
                       return;
                     if (doc.deleted || doc.id===undefined)
                       return;
                     emit([pieces[1], doc.id], doc.rev); }`
	changes_map := `function (doc, meta) {
                    if (doc.sequence === undefined)
                        return;
                    var pieces = meta.id.split(":", 3);
                    if (pieces.length < 3 || pieces[0] != "doc" || doc.id===undefined)
                        return;
                    var value = [doc.id, doc.rev];
                    if (doc.deleted)
                        value.push(true)
                    emit([pieces[1], doc.sequence], value); }`

	ddoc := Body{
		"language": "javascript",
		"views": Body{
			"all_dbs":  Body{"map": alldbs_map},
			"all_docs": Body{"map": alldocs_map, "reduce": "_count"},
			"changes":  Body{"map": changes_map}}}
	payload, err := json.Marshal(ddoc)
	rq, err := http.NewRequest("PUT", u.String(), bytes.NewBuffer(payload))
	rq.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(rq)

	if err == nil && response.StatusCode > 299 {
		err = &HTTPError{Status: response.StatusCode, Message: response.Status}
	}
	if err == nil {
		log.Printf("Installed design doc <%s>", u)
	} else {
		log.Printf("WARNING: Error installing design doc: %v", err)
	}
	return err
}

// Returns all database names as an array.
func AllDbNames(bucket *couchbase.Bucket) ([]string, error) {
	vres, err := bucket.View("couchdb", "all_dbs", nil)
	if err != nil {
		log.Printf("WARNING: View returned %v", err)
		return nil, err
	}

	rows := vres.Rows
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		result = append(result, row.Key.(string))
	}
	return result, nil
}

type IDAndRev struct {
	DocID string
	RevID string
}

// Returns all document IDs as an array.
func (db *Database) AllDocIDs() ([]IDAndRev, error) {
	vres, err := db.bucket.View("couchdb", "all_docs", db.allDocIDsOpts(false))
	if err != nil {
		log.Printf("WARNING: View returned %v", err)
		return nil, err
	}

	rows := vres.Rows
	result := make([]IDAndRev, 0, len(rows))
	for _, row := range rows {
		key := row.Key.([]interface{})
		result = append(result, IDAndRev{DocID: key[1].(string), RevID: row.Value.(string)})
	}
	return result, nil
}

func (db *Database) allDocIDsOpts(reduce bool) Body {
	uuid := db.UUID()
	startkey := [1]string{uuid}
	endkey := [2]interface{}{uuid, make(Body)}
	return Body{"startkey": startkey, "endkey": endkey, "reduce": reduce}
}

// Deletes a database (and all documents)
func (db *Database) Delete() error {
	docIDs, err := db.AllDocIDs()
	if err != nil {
		return err
	}
	//FIX: Is there a way to do this in one operation?
	err = db.bucket.Delete(dbInternalDocName(db.Name))
	if err != nil {
		return err
	}
	db.bucket.Delete(db.sequenceDocID())
	for _, doc := range docIDs {
		db.bucket.Delete(doc.DocID)
	}
	return nil
}

//////// SEQUENCES & CHANGES:

func (db *Database) sequenceDocID() string {
	return dbInternalDocName(db.Name) + ":nextsequence"
}

func (db *Database) LastSequence() (uint64, error) {
	return db.bucket.Incr(db.sequenceDocID(), 0, 0, 0)
}

func (db *Database) generateSequence() (uint64, error) {
	return db.bucket.Incr(db.sequenceDocID(), 1, 1, 0)
}

// Options for Database.getChanges
type ChangesOptions struct {
	Since      uint64
	Limit      int
	Descending bool
	Conflicts  bool
}

// A changes entry; Database.getChanges returns an array of these.
// Marshals into the standard CouchDB _changes format.
type ChangeEntry struct {
	Seq     uint64      `json:"seq"`
	ID      string      `json:"id"`
	Changes []ChangeRev `json:"changes"`
	Deleted bool        `json:"deleted,omitempty"`
}

type ChangeRev map[string]string

// Returns a list of all the changes made to the database, a la the _changes feed.
func (db *Database) GetChanges(options ChangesOptions) ([]ChangeEntry, error) {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
	uuid := db.UUID()
	startkey := [2]interface{}{uuid, options.Since + 1}
	endkey := [2]interface{}{uuid, make(Body)}
	opts := Body{"startkey": startkey, "endkey": endkey,
		"descending": options.Descending}
	if options.Limit > 0 {
		opts["limit"] = options.Limit
	}

	vres, err := db.bucket.View("couchdb", "changes", opts)
	if err != nil {
		log.Printf("Error from 'changes' view: %v", err)
		return nil, err
	}

	rows := vres.Rows
	changes := make([]ChangeEntry, 0, len(rows))
	for _, row := range rows {
		key := row.Key.([]interface{})
		value := row.Value.([]interface{})
		docID := value[0].(string)
		revID := value[1].(string)

		entry := ChangeEntry{
			Seq:     uint64(key[1].(float64)),
			ID:      docID,
			Changes: []ChangeRev{{"rev": revID}},
			Deleted: (len(value) >= 3 && value[2].(bool)),
		}

		if options.Conflicts {
			doc, _ := db.getDoc(docID)
			if doc != nil {
				for _, leafID := range doc.History.getLeaves() {
					if leafID != revID {
						entry.Changes = append(entry.Changes, ChangeRev{"rev": leafID})
					}
				}
			}
		}

		changes = append(changes, entry)
	}
	return changes, nil
}

//////// UTILITIES:

// Attempts to map an error to an HTTP status code and message.
// Defaults to 500 if it doesn't recognize the error. Returns 200 for a nil error.
func ErrorAsHTTPStatus(err error) (int, string) {
	if err == nil {
		return 200, "OK"
	}
	switch err := err.(type) {
	case *HTTPError:
		return err.Status, err.Message
	case *gomemcached.MCResponse:
		switch err.Status {
		case gomemcached.KEY_ENOENT:
			return http.StatusNotFound, "missing"
		case gomemcached.KEY_EEXISTS:
			return http.StatusConflict, "Conflict"
		default:
			return http.StatusBadGateway, fmt.Sprintf("MC status %d", err.Status)
		}
	default:
		log.Printf("WARNING: Couldn't interpret error type %T, value %v", err, err)
		return http.StatusInternalServerError, fmt.Sprintf("Internal error: %v", err)
	}
	panic("unreachable")
}

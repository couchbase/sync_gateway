package basecouch

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
)

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

// HTTP handler for a GET of a document
func (db *Database) HandleGetDoc(r http.ResponseWriter, rq *http.Request, docid string) {
	query := rq.URL.Query()
	revid := query.Get("rev")

	value, err := db.GetRev(docid, revid, query.Get("revs") == "true")
	if err != nil {
		writeError(err, r)
		return
	}
	if value == nil {
		r.WriteHeader(http.StatusNotFound)
		return
	}
	r.Header().Set("Etag", value["_rev"].(string))
	writeJSON(value, r, rq)
}

// HTTP handler for a PUT of a document
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
		writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r, rq)
	} else {
		// Replicator-style PUT with new_edits=false:
		revisions := parseRevisions(body)
		if revisions == nil {
			writeError(&HTTPError{Status: http.StatusBadRequest, Message: "Bad _revisions"}, r)
		}
		err := db.PutExistingRev(docid, body, revisions)
		if err != nil {
			writeError(err, r)
		}
	}
	r.WriteHeader(http.StatusCreated)
}

// HTTP handler for a POST to a database (creating a document)
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
	writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r, rq)
}

// HTTP handler for a DELETE of a document
func (db *Database) HandleDeleteDoc(r http.ResponseWriter, rq *http.Request, docid string) {
	revid := rq.URL.Query().Get("rev")
	newRev, err := db.DeleteDoc(docid, revid)
	if err != nil {
		writeError(err, r)
		return
	}
	writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r, rq)
}

// HTTP handler for _all_docs
func (db *Database) HandleAllDocs(r http.ResponseWriter, rq *http.Request) {
	// http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
	includeDocs := rq.URL.Query().Get("include_docs") == "true"
	var ids []IDAndRev
	var err error

	// Get the doc IDs:
	if rq.Method == "GET" {
		ids, err = db.AllDocIDs()
	} else {
		input, err := readJSON(rq)
		if err == nil {
			keys, ok := input["keys"].([]interface{})
			ids = make([]IDAndRev, len(keys))
			for i := 0; i < len(keys); i++ {
				ids[i].DocID, ok = keys[i].(string)
				if !ok {
					break
				}
			}
			if !ok {
				err = &HTTPError{Status: http.StatusBadRequest, Message: "Bad/missing keys"}
			}
		}
	}
	if err != nil {
		writeError(err, r)
		return
	}

	type viewRow struct {
		ID    string            `json:"id"`
		Key   string            `json:"key"`
		Value map[string]string `json:"value"`
		Doc   Body              `json:"doc,omitempty"`
	}
	type viewResult struct {
		TotalRows int       `json:"total_rows"`
		Offset    int       `json:"offset"`
		Rows      []viewRow `json:"rows"`
	}

	// Assemble the result (and read docs if includeDocs is set)
	result := viewResult{TotalRows: len(ids), Rows: make([]viewRow, 0, len(ids))}
	for _, id := range ids {
		row := viewRow{ID: id.DocID, Key: id.DocID}
		if includeDocs || id.RevID == "" {
			// Fetch the document body:
			body, err := db.Get(id.DocID)
			if err == nil {
				id.RevID = body["_rev"].(string)
				if includeDocs {
					row.Doc = body
				}
			} else {
				continue
			}
		}
		row.Value = map[string]string{"rev": id.RevID}
		result.Rows = append(result.Rows, row)
	}

	writeJSON(result, r, rq)
}

// HTTP handler for a POST to _bulk_docs
func (db *Database) HandleBulkDocs(r http.ResponseWriter, rq *http.Request) {
	body, err := readJSON(rq)
	if err != nil {
		writeError(err, r)
		return
	}
	newEdits, ok := body["new_edits"].(bool)
	if !ok {
		newEdits = true
	}

	result := make([]Body, 0, 5)
	for _, item := range body["docs"].([]interface{}) {
		doc := item.(map[string]interface{})
		docid, _ := doc["_id"].(string)
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

		status := Body{}
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
		log.Printf("\t%v", status)
	}

	r.WriteHeader(http.StatusCreated)
	writeJSON(Body{"docs": result}, r, rq)
}

func (db *Database) HandleChanges(r http.ResponseWriter, rq *http.Request) {
	var options ChangesOptions
	options.Since = getIntQuery(rq, "since")
	options.Limit = int(getIntQuery(rq, "limit"))
	options.Conflicts = (rq.URL.Query().Get("style") == "all_docs")

	changes, err := db.GetChanges(options)
	var lastSeq uint64
	if err == nil {
		lastSeq, err = db.LastSequence()
	}
	if err != nil {
		writeError(err, r)
		return
	}
	writeJSON(Body{"results": changes, "last_seq": lastSeq}, r, rq)
}

// HTTP handler for a GET of a _local document
func (db *Database) HandleGetLocalDoc(r http.ResponseWriter, rq *http.Request, docid string) {
	value, err := db.GetLocal(docid)
	if err != nil {
		writeError(err, r)
		return
	}
	if value == nil {
		r.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(value, r, rq)
}

// HTTP handler for a PUT of a _local document
func (db *Database) HandlePutLocalDoc(r http.ResponseWriter, rq *http.Request, docid string) {
	body, err := readJSON(rq)
	if err != nil {
		writeError(err, r)
		return
	}

	err = db.PutLocal(docid, body)
	if err != nil {
		writeError(err, r)
		return
	}
	r.WriteHeader(http.StatusCreated)
}

// HTTP handler for a DELETE of a _local document
func (db *Database) HandleDeleteLocalDoc(r http.ResponseWriter, rq *http.Request, docid string) {
	writeError(db.DeleteLocal(docid), r)
}

// HTTP handler for a database.
func (db *Database) Handle(r http.ResponseWriter, rq *http.Request, path []string) {
    pathLen := len(path)
    if pathLen >= 2 && path[0] == "_design" {
        path[0] += "/" + path[1]
        pathLen--
    }
	method := rq.Method
	switch pathLen {
	case 0:
		// Root level
		log.Printf("%s %s\n", method, db.Name)
		switch method {
		case "GET":
			response := make(map[string]interface{})
			response["db_name"] = db.Name
			response["doc_count"] = db.DocCount()
			writeJSON(response, r, rq)
			return
		case "POST":
			db.HandlePostDoc(r, rq)
			return
		case "DELETE":
			writeError(db.Delete(), r)
			r.Write([]byte("ok"))
			return
		}
	case 1:
		docid := path[0]
		log.Printf("%s %s %s\n", method, db.Name, docid)
		switch docid {
		case "_all_docs":
			if method == "GET" || method == "POST" {
				db.HandleAllDocs(r, rq)
				return
			}
		case "_bulk_docs":
			if method == "POST" {
				db.HandleBulkDocs(r, rq)
				return
			}
		case "_changes":
			if method == "GET" {
				db.HandleChanges(r, rq)
				return
			}
		case "_revs_diff":
			if method == "POST" {
				var input RevsDiffInput
				err := readJSONInto(rq, &input)
				if err != nil {
					writeError(err, r)
					return
				}
				output, err := db.RevsDiff(input)
				if err != nil {
					writeError(err, r)
					return
				}
				writeJSON(output, r, rq)
				return
			}
		default:
			if docid[0] != '_' || strings.HasPrefix(docid, "_design/") {
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
	case 2:
		if path[0] == "_local" {
			docid := path[1]
			log.Printf("%s %s local doc %q", db.Name, method, docid)
			switch method {
			case "GET":
				db.HandleGetLocalDoc(r, rq, docid)
				return
			case "PUT":
				db.HandlePutLocalDoc(r, rq, docid)
				return
			case "DELETE":
				db.HandleDeleteLocalDoc(r, rq, docid)
				return
			}
		}
	}
	// Fall through to here if the request was not recognized:
	log.Printf("WARNING: Unhandled %s %s\n", method, rq.URL)
	r.WriteHeader(http.StatusBadRequest)
}

// HTTP handler for the root ("/")
func handleRoot(r http.ResponseWriter, rq *http.Request) {
	if rq.Method == "GET" {
		response := map[string]string{
			"couchdb": "welcome",
			"version": "CouchGlue 0.0",
		}
		writeJSON(response, r, rq)
	} else {
		r.WriteHeader(http.StatusBadRequest)
	}
}

func handleAllDbs(bucket *couchbase.Bucket, r http.ResponseWriter, rq *http.Request) {
	if rq.Method == "GET" {
		response, err := AllDbNames(bucket)
		if err != nil {
			writeError(err, r)
		} else {
			writeJSON(response, r, rq)
		}
	} else {
		r.WriteHeader(http.StatusBadRequest)
	}
}

// Creates an http.Handler that will handle the REST API for the given bucket.
func NewRESTHandler(bucket *couchbase.Bucket) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		path := strings.Split(rq.URL.Path[1:], "/")
		for len(path) > 0 && path[len(path)-1] == "" {
			path = path[0 : len(path)-1]
		}
		if len(path) == 0 {
			handleRoot(r, rq)
			return
		}
		dbName := path[0]

		if dbName == "_all_dbs" {
			handleAllDbs(bucket, r, rq)
		} else if rq.Method == "PUT" && len(path) == 1 {
			// Create a database:
			log.Printf("%s %s", rq.Method, dbName)
			_, err := CreateDatabase(bucket, dbName)
			if err != nil {
				writeError(err, r)
				return
			}
			r.WriteHeader(http.StatusCreated)
		} else {
			// Handle a request aimed at a database:
			db, err := GetDatabase(bucket, dbName)
			if err != nil {
				log.Printf("%s %s", rq.Method, dbName)
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

// Main entry point for a simple server; you can have your main() function just call this.
func ServerMain() {
	addr := flag.String("addr", ":4984", "Address to bind to")
	couchbaseURL := flag.String("url", "http://localhost:8091", "Address of Couchbase server")
	poolName := flag.String("pool", "default", "Name of pool")
	bucketName := flag.String("bucket", "couchdb", "Name of bucket")
	pretty := flag.Bool("pretty", false, "Pretty-print JSON responses")
	flag.Parse()

	bucket, err := ConnectToBucket(*couchbaseURL, *poolName, *bucketName)
	if err != nil {
		log.Fatalf("Error getting bucket '%s':  %v\n", *bucketName, err)
	}

	InitREST(bucket)
	PrettyPrint = *pretty

	log.Printf("Starting server on %s", *addr)
	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("Server failed: ", err.Error())
	}
}

//////// HELPER FUNCTIONS:

// Returns the integer value of a URL query, defaulting to 0 if missing or unparseable
func getIntQuery(rq *http.Request, query string) (value uint64) {
	q := rq.URL.Query().Get(query)
	if q != "" {
		value, _ = strconv.ParseUint(q, 10, 64)
	}
	return
}

// Parses a JSON request body, unmarshaling it into "into".
func readJSONInto(rq *http.Request, into interface{}) error {
	contentType := rq.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "application/json") {
		return &HTTPError{Status: http.StatusUnsupportedMediaType,
			Message: "Invalid content type " + contentType}
	}
	body, err := ioutil.ReadAll(rq.Body)
	if err != nil {
		return &HTTPError{Status: http.StatusBadRequest}
	}
	err = json.Unmarshal(body, into)
	if err != nil {
		log.Printf("WARNING: Couldn't parse JSON:\n%s", body)
		return &HTTPError{Status: http.StatusBadRequest, Message: "Bad JSON"}
	}
	return nil
}

// Parses a JSON request body, returning it as a Body map.
func readJSON(rq *http.Request) (Body, error) {
	var body Body
	return body, readJSONInto(rq, &body)
}

// Writes an object to the response in JSON format.
func writeJSON(value interface{}, r http.ResponseWriter, rq *http.Request) {
	if rq != nil {
		accept := rq.Header.Get("Accept")
		if accept != "" && !strings.Contains(accept, "application/json") &&
			!strings.Contains(accept, "*/*") {
			log.Printf("WARNING: Client won't accept JSON, only %s", accept)
			r.WriteHeader(http.StatusNotAcceptable)
			return
		}
	}

	jsonOut, err := json.Marshal(value)
	if err != nil {
		log.Printf("WARNING: Couldn't serialize JSON for %v", value)
		r.WriteHeader(http.StatusInternalServerError)
		return
	}
	if PrettyPrint {
		var buffer bytes.Buffer
		json.Indent(&buffer, jsonOut, "", "  ")
		jsonOut = append(buffer.Bytes(), '\n')
	}
	r.Header().Set("Content-Type", "application/json")
	r.Write(jsonOut)
}

// If the error parameter is non-nil, sets the response status code appropriately and
// writes a CouchDB-style JSON description to the body.
func writeError(err error, r http.ResponseWriter) {
	if err != nil {
		status, message := ErrorAsHTTPStatus(err)
		r.WriteHeader(status)
		writeJSON(Body{"error": status, "reason": message}, r, nil)
		log.Printf("Returning response %d: %s", status, message)
	}
}

//  Copyright (c) 2011 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

import (
	"bytes"
	"encoding/json"
	"flag"
    "fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
)

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

// If set to true, HTTP requests will be logged
var LogRequests bool = true
var LogRequestsVerbose bool = false

// HTTP handler for a GET of a document
func (db *Database) HandleGetDoc(r http.ResponseWriter, rq *http.Request, docid string) (error) {
	query := rq.URL.Query()
	revid := query.Get("rev")
    
    // What attachment bodies should be included?
    var attachmentsSince []string = nil
    if query.Get("attachments") == "true" {
        atts := query.Get("atts_since")
        if atts != "" {
            var revids []string
            err := json.Unmarshal([]byte(atts), &revids)
            if err != nil {
        		err = &HTTPError{http.StatusBadRequest, "bad atts_since"}
            }
        } else {
            attachmentsSince = []string{}
        }
    }

	value, err := db.GetRev(docid, revid,
                             query.Get("revs") == "true",
                             attachmentsSince)
	if err != nil {
		return err
	}
	if value == nil {
		err = &HTTPError{http.StatusNotFound, "missing"}
		return err
	}
	r.Header().Set("Etag", value["_rev"].(string))
	writeJSON(value, r, rq)
    return nil
}

// HTTP handler for a PUT of a document
func (db *Database) HandlePutDoc(r http.ResponseWriter, rq *http.Request, docid string) (error) {
	body, err := readJSON(rq)
	if err != nil {
		return err
	}

	query := rq.URL.Query()
	if query.Get("new_edits") != "false" {
		// Regular PUT:
		newRev, err := db.Put(docid, body)
		if err != nil {
			return err
		}
		r.Header().Set("Etag", newRev)
		writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r, rq)
	} else {
		// Replicator-style PUT with new_edits=false:
		revisions := parseRevisions(body)
		if revisions == nil {
			return &HTTPError{http.StatusBadRequest, "Bad _revisions"}
		}
		err := db.PutExistingRev(docid, body, revisions)
		if err != nil {
			return err
		}
	}
	r.WriteHeader(http.StatusCreated)
    return nil
}

// HTTP handler for a POST to a database (creating a document)
func (db *Database) HandlePostDoc(r http.ResponseWriter, rq *http.Request) (error) {
	body, err := readJSON(rq)
	if err != nil {
		return err
	}
	docid, newRev, err := db.Post(body)
	if err != nil {
		return err
	}
	r.Header().Set("Location", docid)
	r.Header().Set("Etag", newRev)
	writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r, rq)
    return nil
}

// HTTP handler for a DELETE of a document
func (db *Database) HandleDeleteDoc(r http.ResponseWriter, rq *http.Request, docid string) (error) {
	revid := rq.URL.Query().Get("rev")
	newRev, err := db.DeleteDoc(docid, revid)
	if err == nil {
        writeJSON(Body{"ok": true, "id": docid, "rev": newRev}, r, rq)
    }
    return err
}

// HTTP handler for _all_docs
func (db *Database) HandleAllDocs(r http.ResponseWriter, rq *http.Request) (error) {
	// http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
	includeDocs := rq.URL.Query().Get("include_docs") == "true"
	var ids []IDAndRev
    var err error

	// Get the doc IDs:
	if rq.Method == "GET" || rq.Method == "HEAD" {
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
				err = &HTTPError{http.StatusBadRequest, "Bad/missing keys"}
			}
		}
	}
	if err != nil {
		return err
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
    return nil
}

// HTTP handler for a POST to _bulk_docs
func (db *Database) HandleBulkDocs(r http.ResponseWriter, rq *http.Request) (error) {
	body, err := readJSON(rq)
	if err != nil {
		return err
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
				err = &HTTPError{http.StatusBadRequest, "Bad _revisions"}
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
            err = nil // wrote it to output already; not going to return it
		} else {
			status["rev"] = revid
		}
		result = append(result, status)
	}

	r.WriteHeader(http.StatusCreated)
	writeJSON(result, r, rq)
    return nil
}

func (db *Database) HandleChanges(r http.ResponseWriter, rq *http.Request) (error) {
	var options ChangesOptions
	options.Since = getIntQuery(rq, "since")
	options.Limit = int(getIntQuery(rq, "limit"))
	options.Conflicts = (rq.URL.Query().Get("style") == "all_docs")
	options.IncludeDocs = (rq.URL.Query().Get("include_docs") == "true")

	changes, err := db.GetChanges(options)
	var lastSeq uint64
	if err == nil {
		lastSeq, err = db.LastSequence()
	}
	if err == nil {
        writeJSON(Body{"results": changes, "last_seq": lastSeq}, r, rq)
    }
    return err
}

func (db *Database) HandleRevsDiff(r http.ResponseWriter, rq *http.Request) (error) {
	var input RevsDiffInput
	err := readJSONInto(rq, &input)
	if err != nil {
		return err
	}
    if (LogRequestsVerbose) {
        log.Printf("\t%v", input)
    }
	output, err := db.RevsDiff(input)
	if err == nil {
    	writeJSON(output, r, rq)
	}
	return err
}

// HTTP handler for a GET of a _local document
func (db *Database) HandleGetLocalDoc(r http.ResponseWriter, rq *http.Request, docid string) (error) {
	value, err := db.GetLocal(docid)
	if err != nil {
		return err
	}
	if value == nil {
		return &HTTPError{http.StatusNotFound, "missing"}
	}
	writeJSON(value, r, rq)
    return nil
}

// HTTP handler for a PUT of a _local document
func (db *Database) HandlePutLocalDoc(r http.ResponseWriter, rq *http.Request, docid string) (error) {
	body, err := readJSON(rq)
	if err == nil {
        err = db.PutLocal(docid, body)
    }
	if err == nil {
    	r.WriteHeader(http.StatusCreated)
	}
    return err
}

// HTTP handler for a DELETE of a _local document
func (db *Database) HandleDeleteLocalDoc(r http.ResponseWriter, rq *http.Request, docid string) (error) {
	return db.DeleteLocal(docid)
}

// HTTP handler for a database.
func (db *Database) Handle(r http.ResponseWriter, rq *http.Request, path []string) (error) {
    pathLen := len(path)
    if pathLen >= 2 && path[0] == "_design" {
        path[0] += "/" + path[1]
        pathLen--
    }
	method := rq.Method
    if method == "HEAD" {
        method = "GET"
    }
	switch pathLen {
	case 0:
		// Root level
		//log.Printf("%s %s\n", method, db.Name)
		switch method {
		case "GET":
            lastSeq,_ := db.LastSequence()
            response := Body{
                "db_name": db.Name,
                "doc_count": db.DocCount(),
                "update_seq": lastSeq,
            }
			writeJSON(response, r, rq)
			return nil
		case "POST":
			return db.HandlePostDoc(r, rq)
		case "DELETE":
			return db.Delete()
		}
	case 1:
		docid := path[0]
		switch docid {
		case "_all_docs":
			if method == "GET" || method == "POST" {
				return db.HandleAllDocs(r, rq)
			}
		case "_bulk_docs":
			if method == "POST" {
				return db.HandleBulkDocs(r, rq)
			}
		case "_changes":
			if method == "GET" {
				return db.HandleChanges(r, rq)
			}
		case "_revs_diff":
			if method == "POST" {
                return db.HandleRevsDiff(r, rq)
			}
        case "_ensure_full_commit":
            if method == "POST": {
                // no-op. CouchDB's replicator sends this, so don't barf. Status must be 202.
                return &HTTPError{201, "Committed"}
            }
		default:
			if docid[0] != '_' || strings.HasPrefix(docid, "_design/") {
				// Accessing a document:
				switch method {
				case "GET":
					return db.HandleGetDoc(r, rq, docid)
				case "PUT":
					return db.HandlePutDoc(r, rq, docid)
				case "DELETE":
					return db.HandleDeleteDoc(r, rq, docid)
				}
			}
		}
	case 2:
		if path[0] == "_local" {
			docid := path[1]
			switch method {
			case "GET":
				return db.HandleGetLocalDoc(r, rq, docid)
			case "PUT":
				return db.HandlePutLocalDoc(r, rq, docid)
			case "DELETE":
				return db.HandleDeleteLocalDoc(r, rq, docid)
			}
		}
	}
	// Fall through to here if the request was not recognized:
	log.Printf("WARNING: Unhandled %s %s\n", method, rq.URL)
	return &HTTPError{http.StatusBadRequest, "bad request"}
}

// HTTP handler for the root ("/")
func handleRoot(r http.ResponseWriter, rq *http.Request) error {
	if rq.Method == "GET" || rq.Method == "HEAD" {
		response := map[string]string{
			"couchdb": "welcome",
			"version": "BaseCouch 0.1",
		}
		writeJSON(response, r, rq)
        return nil
	}
	return &HTTPError{http.StatusBadRequest, "bad request"}
}

func handleAllDbs(bucket *couchbase.Bucket, r http.ResponseWriter, rq *http.Request) (error) {
	if rq.Method == "GET" || rq.Method == "HEAD" {
		response, err := AllDbNames(bucket)
		if err != nil {
			return err
		} else {
			writeJSON(response, r, rq)
            return nil
		}
	}
	return &HTTPError{http.StatusBadRequest, "bad request"}
}

// Creates an http.Handler that will handle the REST API for the given bucket.
func NewRESTHandler(bucket *couchbase.Bucket) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
        if ( LogRequests) {
            log.Printf("%s %s", rq.Method, rq.URL)
        }
		path := strings.Split(rq.URL.Path[1:], "/")
		for len(path) > 0 && path[len(path)-1] == "" {
			path = path[0 : len(path)-1]
		}
        var err error
		if len(path) == 0 {
			err =handleRoot(r, rq)
        } else if path[0] == "_all_dbs" {
			err = handleAllDbs(bucket, r, rq)
		} else if rq.Method == "PUT" && len(path) == 1 {
			// Create a database:
			_, err := CreateDatabase(bucket, path[0])
			if err == nil {
                r.WriteHeader(http.StatusCreated)
            }
		} else {
			// Handle a request aimed at a database:
            var db *Database
			db, err = GetDatabase(bucket, path[0])
			if err == nil {
    			err = db.Handle(r, rq, path[1:])
            }
		}
        if err != nil {
			writeError(err, r)
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
    verbose := flag.Bool("verbose", false, "Log more info about requests")
	flag.Parse()

	bucket, err := ConnectToBucket(*couchbaseURL, *poolName, *bucketName)
	if err != nil {
		log.Fatalf("Error getting bucket '%s':  %v\n", *bucketName, err)
	}

	InitREST(bucket)
	PrettyPrint = *pretty
    LogRequestsVerbose = *verbose

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
		return &HTTPError{http.StatusUnsupportedMediaType, "Invalid content type " + contentType}
	}
	body, err := ioutil.ReadAll(rq.Body)
	if err != nil {
		return &HTTPError{http.StatusBadRequest, ""}
	}
	err = json.Unmarshal(body, into)
	if err != nil {
		log.Printf("WARNING: Couldn't parse JSON:\n%s", body)
		return &HTTPError{http.StatusBadRequest, "Bad JSON"}
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
			writeStatus(http.StatusNotAcceptable, "only application/json available", r)
			return
		}
	}

	jsonOut, err := json.Marshal(value)
	if err != nil {
		log.Printf("WARNING: Couldn't serialize JSON for %v", value)
		writeStatus(http.StatusInternalServerError, "JSON serialization failed", r)
		return
	}
	if PrettyPrint {
		var buffer bytes.Buffer
		json.Indent(&buffer, jsonOut, "", "  ")
		jsonOut = append(buffer.Bytes(), '\n')
	}
	r.Header().Set("Content-Type", "application/json")
    if rq == nil || rq.Method != "HEAD" {
    	r.Header().Set("Content-Length", fmt.Sprintf("%d", len(jsonOut)))
        r.Write(jsonOut)
    }
}

// If the error parameter is non-nil, sets the response status code appropriately and
// writes a CouchDB-style JSON description to the body.
func writeError(err error, r http.ResponseWriter) {
	if err != nil {
		status, message := ErrorAsHTTPStatus(err)
		writeStatus(status, message, r)
	}
}

// Writes the response status code, and if it's an error writes a JSON description to the body.
func writeStatus(status int, message string, r http.ResponseWriter) {
	r.WriteHeader(status)
    if status < 300 {
        return
    }
    var errorStr string
    switch status {
        case 404:
        errorStr = "not_found"
        default:
        errorStr = fmt.Sprintf("%d", status)
    }
	writeJSON(Body{"error": errorStr, "reason": message}, r, nil)
	log.Printf("\t*** %d: %s", status, message)
}

//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

import (
	"encoding/json"
	"flag"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/couchbaselabs/go-couchbase"
)

const VersionString = "BaseCouch/0.2"

// Shared context of HTTP handlers. It's important that this remain immutable, because the
// handlers will access it from multiple goroutines.
type context struct {
	bucket        *couchbase.Bucket
	dbName        string
	channelMapper *ChannelMapper
	auth          *Authenticator
	serverURL	  string
}

// HTTP handler for a GET of a document
func (h *handler) handleGetDoc(docid string) error {
	revid := h.getQuery("rev")
	includeRevs := h.getBoolQuery("revs")
	openRevs := h.getQuery("open_revs")

	// What attachment bodies should be included?
	var attachmentsSince []string = nil
	if h.getBoolQuery("attachments") {
		atts := h.getQuery("atts_since")
		if atts != "" {
			var revids []string
			err := json.Unmarshal([]byte(atts), &revids)
			if err != nil {
				return &HTTPError{http.StatusBadRequest, "bad atts_since"}
			}
		} else {
			attachmentsSince = []string{}
		}
	}

	if openRevs == "" {
		// Single-revision GET:
		value, err := h.db.GetRev(docid, revid, includeRevs, attachmentsSince)
		if err != nil {
			return err
		}
		if value == nil {
			return kNotFoundError
		}
		h.setHeader("Etag", value["_rev"].(string))

		if h.requestAccepts("application/json") {
			h.writeJSON(value)
		} else {
			return h.writeMultipart(func(writer *multipart.Writer) error {
				h.db.writeMultipartDocument(value, writer)
				return nil
			})
		}

	} else if openRevs == "all" {
		return &HTTPError{http.StatusNotImplemented, "open_revs=all unimplemented"} // TODO

	} else {
		var revids []string
		err := json.Unmarshal([]byte(openRevs), &revids)
		if err != nil {
			return &HTTPError{http.StatusBadRequest, "bad open_revs"}
		}

		err = h.writeMultipart(func(writer *multipart.Writer) error {
			for _, revid := range revids {
				contentType := "application/json"
				value, err := h.db.GetRev(docid, revid, includeRevs, attachmentsSince)
				if err != nil {
					value = Body{"missing": revid} //TODO: More specific error
					contentType += `; error="true"`
				}
				jsonOut, _ := json.Marshal(value)
				partHeaders := textproto.MIMEHeader{}
				partHeaders.Set("Content-Type", contentType)
				part, _ := writer.CreatePart(partHeaders)
				part.Write(jsonOut)
			}
			return nil
		})
		return err
	}
	return nil
}

// HTTP handler for a PUT of a document
func (h *handler) handlePutDoc(docid string) error {
	body, err := h.readDocument()
	if err != nil {
		return err
	}
	var newRev string

	if h.getQuery("new_edits") != "false" {
		// Regular PUT:
		newRev, err = h.db.Put(docid, body)
		if err != nil {
			return err
		}
		h.setHeader("Etag", newRev)
	} else {
		// Replicator-style PUT with new_edits=false:
		revisions := parseRevisions(body)
		if revisions == nil {
			return &HTTPError{http.StatusBadRequest, "Bad _revisions"}
		}
		err = h.db.PutExistingRev(docid, body, revisions)
		if err != nil {
			return err
		}
		newRev = body["_rev"].(string)
	}
	h.writeJSONStatus(http.StatusCreated, Body{"ok": true, "id": docid, "rev": newRev})
	return nil
}

// HTTP handler for a POST to a database (creating a document)
func (h *handler) handlePostDoc() error {
	body, err := h.readDocument()
	if err != nil {
		return err
	}
	docid, newRev, err := h.db.Post(body)
	if err != nil {
		return err
	}
	h.setHeader("Location", docid)
	h.setHeader("Etag", newRev)
	h.writeJSON(Body{"ok": true, "id": docid, "rev": newRev})
	return nil
}

// HTTP handler for a DELETE of a document
func (h *handler) handleDeleteDoc(docid string) error {
	revid := h.getQuery("rev")
	newRev, err := h.db.DeleteDoc(docid, revid)
	if err == nil {
		h.writeJSON(Body{"ok": true, "id": docid, "rev": newRev})
	}
	return err
}

// HTTP handler for _all_docs
func (h *handler) handleAllDocs() error {
	// http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
	includeDocs := h.getBoolQuery("include_docs")
	includeRevs := h.getBoolQuery("revs")
	var ids []IDAndRev
	var err error

	// Get the doc IDs:
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		ids, err = h.db.AllDocIDs()
	} else {
		input, err := h.readJSON()
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
			body, err := h.db.GetRev(id.DocID, id.RevID, includeRevs, nil)
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

	h.writeJSON(result)
	return nil
}

// HTTP handler for a POST to _bulk_get
func (h *handler) handleBulkGet() error {
	includeRevs := h.getBoolQuery("revs")
	includeAttachments := h.getBoolQuery("attachments")
	body, err := h.readJSON()
	if err != nil {
		return err
	}

	result := make([]Body, 0, 5)
	for _, item := range body["docs"].([]interface{}) {
		doc := item.(map[string]interface{})
		docid, _ := doc["id"].(string)
		revid := ""
		revok := true
		if doc["rev"] != nil {
			revid, revok = doc["rev"].(string)
		}
		if docid == "" || !revok {
			return &HTTPError{http.StatusBadRequest, "Invalid doc/rev ID"}
		}

		var attsSince []string = nil
		if includeAttachments {
			if doc["atts_since"] != nil {
				raw, ok := doc["atts_since"].([]interface{})
				if ok {
					attsSince = make([]string, len(raw))
					for i := 0; i < len(raw); i++ {
						attsSince[i], ok = raw[i].(string)
						if !ok {
							break
						}
					}
				}
				if !ok {
					return &HTTPError{http.StatusBadRequest, "Invalid atts_since"}
				}
			} else {
				attsSince = []string{}
			}
		}

		body, err := h.db.GetRev(docid, revid, includeRevs, attsSince)
		if err != nil {
			status, msg := ErrorAsHTTPStatus(err)
			body = Body{"id": docid, "error": msg, "status": status}
			if revid != "" {
				body["rev"] = revid
			}
		}
		result = append(result, body)
	}

	h.writeJSONStatus(http.StatusOK, result)
	return nil
}

// HTTP handler for a POST to _bulk_docs
func (h *handler) handleBulkDocs() error {
	body, err := h.readJSON()
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
				revid, err = h.db.Put(docid, doc)
			} else {
				docid, revid, err = h.db.Post(doc)
			}
		} else {
			revisions := parseRevisions(doc)
			if revisions == nil {
				err = &HTTPError{http.StatusBadRequest, "Bad _revisions"}
			} else {
				revid = revisions[0]
				err = h.db.PutExistingRev(docid, doc, revisions)
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

	h.writeJSONStatus(http.StatusCreated, result)
	return nil
}

func (h *handler) handleChanges() error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
	var options ChangesOptions
	options.Since = h.getIntQuery("since", 0)
	options.Limit = int(h.getIntQuery("limit", 0))
	options.Conflicts = (h.getQuery("style") == "all_docs")
	options.IncludeDocs = (h.getBoolQuery("include_docs"))

	// Get the channels as parameters to an imaginary "bychannel" filter.
	// The default is all channels the user can access.
	channels := h.db.user.Channels
	filter := h.getQuery("filter")
	if filter != "" {
		if filter != "basecouch/bychannel" {
			return &HTTPError{http.StatusBadRequest, "Unknown filter; try basecouch/bychannel"}
		}
		channels = SimplifyChannels(strings.Split(h.getQuery("channels"), ","), true)
	}

	switch h.getQuery("feed") {
	case "longpoll":
		options.Wait = true
	case "continuous":
		return h.handleContinuousChanges(channels, options)
	}

	changes, err := h.db.GetChanges(channels, options)
	var lastSeq uint64
	if err == nil {
		lastSeq, err = h.db.LastSequence()
	}
	if err == nil {
		h.writeJSON(Body{"results": changes, "last_seq": lastSeq})
	}
	return err
}

func (h *handler) handleContinuousChanges(channels []string, options ChangesOptions) error {
	var timeout <-chan time.Time
	var heartbeat <-chan time.Time
	if ms := h.getIntQuery("heartbeat", 0); ms > 0 {
		ticker := time.NewTicker(time.Duration(ms) * time.Millisecond)
		defer ticker.Stop()
		heartbeat = ticker.C
	} else if ms := h.getIntQuery("timeout", 60); ms > 0 {
		timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
		defer timer.Stop()
		timeout = timer.C
	}

	options.Wait = true // we want the feed channel to wait for changes
	var feed <-chan *ChangeEntry
	var err error
loop:
	for {
		if feed == nil {
			// Refresh the feed of all current changes:
			feed, err = h.db.MultiChangesFeed(channels, options)
			if err != nil || feed == nil {
				return err
			}
		}

		// Wait for either a new change, or a heartbeat:
		select {
		case entry := <-feed:
			if entry == nil {
				feed = nil
			} else {
				str, _ := json.Marshal(entry)
				if LogRequestsVerbose {
					log.Printf("\tchange: %s", str)
				}
				err = h.writeln(str)

				options.Since = entry.Seq // so next call to ChangesFeed will start from end
				if options.Limit > 0 {
					options.Limit--
					if options.Limit == 0 {
						break loop
					}
				}
			}
		case <-heartbeat:
			err = h.writeln([]byte{})
		case <-timeout:
			break loop
		}
		if err != nil {
			return nil // error is probably because the client closed the connection
		}
	}
	return nil
}

func (h *handler) handleGetAttachment(docid, attachmentName string) error {
	revid := h.getQuery("rev")
	body, err := h.db.GetRev(docid, revid, false, nil)
	if err != nil {
		return err
	}
	if body == nil {
		return kNotFoundError
	}
	meta, ok := bodyAttachments(body)[attachmentName].(map[string]interface{})
	if !ok {
		return &HTTPError{http.StatusNotFound, "missing " + attachmentName}
	}
	digest := meta["digest"].(string)
	data, err := h.db.getAttachment(AttachmentKey(digest))
	if err != nil {
		return err
	}

	h.setHeader("Etag", digest)
	if contentType, ok := meta["content_type"].(string); ok {
		h.setHeader("Content-Type", contentType)
	}
	if encoding, ok := meta["encoding"].(string); ok {
		h.setHeader("Content-Encoding", encoding)
	}
	h.response.Write(data)
	return nil
}

func (h *handler) handleRevsDiff() error {
	var input RevsDiffInput
	err := readJSONInto(h.rq.Header, h.rq.Body, &input)
	if err != nil {
		return err
	}
	output, err := h.db.RevsDiff(input)
	if err == nil {
		h.writeJSON(output)
	}
	return err
}

// HTTP handler for a GET of a _local document
func (h *handler) handleGetLocalDoc(docid string) error {
	value, err := h.db.GetLocal(docid)
	if err != nil {
		return err
	}
	if value == nil {
		return kNotFoundError
	}
	h.writeJSON(value)
	return nil
}

// HTTP handler for a PUT of a _local document
func (h *handler) handlePutLocalDoc(docid string) error {
	body, err := h.readJSON()
	if err == nil {
		var revid string
		revid, err = h.db.PutLocal(docid, body)
		if err == nil {
			h.writeJSONStatus(http.StatusCreated, Body{"ok": true, "id": docid, "rev": revid})
		}
	}
	return err
}

// HTTP handler for a DELETE of a _local document
func (h *handler) handleDeleteLocalDoc(docid string) error {
	return h.db.DeleteLocal(docid, h.getQuery("rev"))
}

// HTTP handler for a database.
func (h *handler) handle(path []string) error {
	pathLen := len(path)
	if pathLen >= 2 && path[0] == "_design" {
		path[0] += "/" + path[1]
		path = append(path[0:1], path[2:]...)
		pathLen--
	}
	method := h.rq.Method
	if method == "HEAD" {
		method = "GET"
	}
	switch pathLen {
	case 0:
		// Root level
		//log.Printf("%s %s\n", method, h.db.Name)
		switch method {
		case "GET":
			lastSeq, _ := h.db.LastSequence()
			response := Body{
				"db_name":    h.db.Name,
				"doc_count":  h.db.DocCount(),
				"update_seq": lastSeq,
			}
			h.writeJSON(response)
			return nil
		case "POST":
			return h.handlePostDoc()
		case "DELETE":
			return h.db.Delete()
		}
	case 1:
		docid := path[0]
		switch docid {
		case "_all_docs":
			if method == "GET" || method == "POST" {
				return h.handleAllDocs()
			}
		case "_bulk_docs":
			if method == "POST" {
				return h.handleBulkDocs()
			}
		case "_bulk_get":
			if method == "POST" {
				return h.handleBulkGet()
			}
		case "_changes":
			if method == "GET" {
				return h.handleChanges()
			}
		case "_revs_diff":
			if method == "POST" {
				return h.handleRevsDiff()
			}
		case "_ensure_full_commit":
			if method == "POST" {
				// no-op. CouchDB's replicator sends this, so don't barf. Status must be 201.
				h.writeJSONStatus(http.StatusCreated, Body{"ok": true})
				return nil
			}
		case "_design/basecouch":
			// we serve this content here so that CouchDB 1.2 has something to
			// hash into the replication-id, to correspond to our filter.
			if method == "GET" {
				h.writeJSON(Body{"filters": Body{"bychannel": "ok"}})
				return nil
			}
		default:
			if docid[0] != '_' || strings.HasPrefix(docid, "_design/") {
				// Accessing a document:
				switch method {
				case "GET":
					return h.handleGetDoc(docid)
				case "PUT":
					return h.handlePutDoc(docid)
				case "DELETE":
					return h.handleDeleteDoc(docid)
				default:
					return kBadMethodError
				}
			}
			return kNotFoundError
		}
		return kBadMethodError
	case 2:
		docid := path[0]
		if docid == "_local" {
			docid = path[1]
			switch method {
			case "GET":
				return h.handleGetLocalDoc(docid)
			case "PUT":
				return h.handlePutLocalDoc(docid)
			case "DELETE":
				return h.handleDeleteLocalDoc(docid)
			default:
				return kBadMethodError
			}
		} else if docid[0] != '_' || strings.HasPrefix(docid, "_design/") {
			// Accessing a document:
			switch method {
			case "GET":
				return h.handleGetAttachment(docid, path[1])
			//TODO: PUT, DELETE
			default:
				return kBadMethodError
			}
		}
	}
	// Fall through to here if the request was not recognized:
	log.Printf("WARNING: Unhandled %s %s\n", method, h.rq.URL)
	return kNotFoundError
}

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		response := map[string]string{
			"couchdb": "welcome",
			"version": VersionString,
		}
		h.writeJSON(response)
		return nil
	}
	return kBadMethodError
}

func (h *handler) handleAllDbs() error {
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		h.writeJSON([]string{h.context.dbName})
		return nil
	}
	return kBadMethodError
}

type ReplicateInput struct {
	Source        string
	Target        string
	Create_target bool
}

func (h *handler) handleVacuum() error {
	revsDeleted, err := VacuumRevisions(h.context.bucket)
	if err != nil {
		return err
	}
	attsDeleted, err := VacuumAttachments(h.context.bucket)
	if err != nil {
		return err
	}
	h.writeJSON(Body{"revs": revsDeleted, "atts": attsDeleted})
	return nil
}

func (h *handler) checkAuth() error {
	if h.context.auth == nil {
		return nil
	}
	userName, password := h.getBasicAuth()
	
	if userName == "" {
		var err error
		h.user, err = h.context.auth.AuthenticateCookie(h.rq)
		if h.user != nil || err != nil {
			return err
		}
	}
	
	h.user = h.context.auth.AuthenticateUser(userName, password)
	if h.user == nil || h.user.Channels == nil {
		h.response.Header().Set("WWW-Authenticate", `Basic realm="BaseCouch"`)
		return &HTTPError{http.StatusUnauthorized, "Invalid login"}
	}
	return nil
}

func (h *handler) run() {
	if LogRequests {
		log.Printf("%s %s", h.rq.Method, h.rq.URL)
	}
	h.setHeader("Server", VersionString)

	// Authentication -- either /_session, or regular auth check
	var err error
	if h.rq.URL.Path == "/_session" {
		switch h.rq.Method {
		case "GET":
			err = h.handleSessionGET()
		case "POST":
			err = h.handleSessionPOST()
		default:
			err = kBadMethodError
		}
		h.writeError(err)
		return
	} else {
		if err = h.checkAuth(); err != nil {
			h.writeError(err)
			return
		}
	}

	path := strings.Split(h.rq.URL.Path[1:], "/")
	for len(path) > 0 && path[len(path)-1] == "" {
		path = path[0 : len(path)-1]
	}
	if len(path) == 0 {
		err = h.handleRoot()
	} else if path[0] == "_all_dbs" {
		err = h.handleAllDbs()
	} else if path[0] == "_vacuum" {
		err = h.handleVacuum()
	} else if h.rq.Method == "PUT" && len(path) == 1 {
		// Create a database:
		if path[0] == h.context.dbName {
			err = &HTTPError{http.StatusConflict, "already exists"}
		} else {
			err = &HTTPError{http.StatusForbidden, "can't create any databases"}
		}
	} else {
		// Handle a request aimed at a database:
		if path[0] == h.context.dbName {
			h.db, err = GetDatabase(h.context.bucket, path[0])
			if err == nil {
				h.db.channelMapper = h.context.channelMapper
				h.db.user = h.user
				err = h.handle(path[1:])
			}
		} else {
			err = &HTTPError{http.StatusNotFound, "no such database"}
		}
	}
	h.writeError(err)
}

// Initialize REST handlers. Call this once on launch.
func InitREST(bucket *couchbase.Bucket, dbName string, serverURL string) *context {
	if dbName == "" {
		dbName = bucket.Name
	}

	db, _ := GetDatabase(bucket, dbName)
	channelMapper, err := db.LoadChannelMapper()
	if err != nil {
		log.Printf("Warning: Couldn't load channelmap fn: %s", err)
		channelMapper, err = NewChannelMapper("")
		if err != nil {
			log.Printf("Warning: Couldn't load channelmap fn: %s", err)
		}
	} else if channelMapper == nil {
		log.Printf("No channelmap fn found; default algorithm in use")
	}

	c := &context{
		bucket:        bucket,
		dbName:        dbName,
		channelMapper: channelMapper,
		auth:          NewAuthenticator(bucket),
		serverURL:     serverURL,
	}
	http.Handle("/", NewRESTHandler(c))
	return c
}

// Main entry point for a simple server; you can have your main() function just call this.
func ServerMain() {
	siteURL := flag.String("site", "", "Server's official URL")
	addr := flag.String("addr", ":4984", "Address to bind to")
	authAddr := flag.String("authaddr", ":4985", "Address to bind the auth interface to")
	couchbaseURL := flag.String("url", "http://localhost:8091", "Address of Couchbase server")
	poolName := flag.String("pool", "default", "Name of pool")
	bucketName := flag.String("bucket", "basecouch", "Name of bucket")
	dbName := flag.String("dbname", "", "Name of CouchDB database (defaults to name of bucket)")
	pretty := flag.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := flag.Bool("verbose", false, "Log more info about requests")
	flag.Parse()

	bucket, err := ConnectToBucket(*couchbaseURL, *poolName, *bucketName)
	if err != nil {
		log.Fatalf("Error getting bucket '%s':  %v\n", *bucketName, err)
	}

	if *dbName == "" {
		*dbName = bucket.Name
	}
	
	context := InitREST(bucket, *dbName, *siteURL)
	PrettyPrint = *pretty
	LogRequestsVerbose = *verbose

	if authAddr != nil {
		log.Printf("Starting auth server on %s", *authAddr)
		StartAuthListener(*authAddr, context.auth)
	}

	log.Printf("Starting server on %s for database %q", *addr, *dbName)
	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("Server failed: ", err.Error())
	}
}

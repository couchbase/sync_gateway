//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

const VersionString = "Couchbase Sync Gateway/0.3"

type context struct {
	dbcontext *db.DatabaseContext
	auth      *auth.Authenticator
}

// HTTP handler for a GET of a document
func (h *handler) handleGetDoc() error {
	docid := h.PathVars()["docid"]
	revid := h.getQuery("rev")
	includeRevs := h.getBoolQuery("revs")
	openRevs := h.getQuery("open_revs")

	// What attachment bodies should be included?
	var attachmentsSince []string = nil
	if h.getBoolQuery("attachments") {
		atts := h.getQuery("atts_since")
		if atts != "" {
			err := json.Unmarshal([]byte(atts), &attachmentsSince)
			if err != nil {
				return &base.HTTPError{http.StatusBadRequest, "bad atts_since"}
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
				h.db.WriteMultipartDocument(value, writer)
				return nil
			})
		}

	} else if openRevs == "all" {
		return &base.HTTPError{http.StatusNotImplemented, "open_revs=all unimplemented"} // TODO

	} else {
		var revids []string
		err := json.Unmarshal([]byte(openRevs), &revids)
		if err != nil {
			return &base.HTTPError{http.StatusBadRequest, "bad open_revs"}
		}

		err = h.writeMultipart(func(writer *multipart.Writer) error {
			for _, revid := range revids {
				contentType := "application/json"
				value, err := h.db.GetRev(docid, revid, includeRevs, attachmentsSince)
				if err != nil {
					value = db.Body{"missing": revid} //TODO: More specific error
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
func (h *handler) handlePutDoc() error {
	docid := h.PathVars()["docid"]
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
		revisions := db.ParseRevisions(body)
		if revisions == nil {
			return &base.HTTPError{http.StatusBadRequest, "Bad _revisions"}
		}
		err = h.db.PutExistingRev(docid, body, revisions)
		if err != nil {
			return err
		}
		newRev = body["_rev"].(string)
	}
	h.writeJSONStatus(http.StatusCreated, db.Body{"ok": true, "id": docid, "rev": newRev})
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
	h.writeJSON(db.Body{"ok": true, "id": docid, "rev": newRev})
	return nil
}

// HTTP handler for a DELETE of a document
func (h *handler) handleDeleteDoc() error {
	docid := h.PathVars()["docid"]
	revid := h.getQuery("rev")
	newRev, err := h.db.DeleteDoc(docid, revid)
	if err == nil {
		h.writeJSON(db.Body{"ok": true, "id": docid, "rev": newRev})
	}
	return err
}

// HTTP handler for _all_docs
func (h *handler) handleAllDocs() error {
	// http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
	includeDocs := h.getBoolQuery("include_docs")
	includeRevs := h.getBoolQuery("revs")
	var ids []db.IDAndRev
	var err error

	// Get the doc IDs:
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		ids, err = h.db.AllDocIDs()
	} else {
		input, err := h.readJSON()
		if err == nil {
			keys, ok := input["keys"].([]interface{})
			ids = make([]db.IDAndRev, len(keys))
			for i := 0; i < len(keys); i++ {
				ids[i].DocID, ok = keys[i].(string)
				if !ok {
					break
				}
			}
			if !ok {
				err = &base.HTTPError{http.StatusBadRequest, "Bad/missing keys"}
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
		Doc   db.Body           `json:"doc,omitempty"`
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

	result := make([]db.Body, 0, 5)
	for _, item := range body["docs"].([]interface{}) {
		doc := item.(map[string]interface{})
		docid, _ := doc["id"].(string)
		revid := ""
		revok := true
		if doc["rev"] != nil {
			revid, revok = doc["rev"].(string)
		}
		if docid == "" || !revok {
			return &base.HTTPError{http.StatusBadRequest, "Invalid doc/rev ID"}
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
					return &base.HTTPError{http.StatusBadRequest, "Invalid atts_since"}
				}
			} else {
				attsSince = []string{}
			}
		}

		body, err := h.db.GetRev(docid, revid, includeRevs, attsSince)
		if err != nil {
			status, msg := base.ErrorAsHTTPStatus(err)
			body = db.Body{"id": docid, "error": msg, "status": status}
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

	docs := body["docs"].([]interface{})
	h.db.ReserveSequences(uint64(len(docs)))

	result := make([]db.Body, 0, len(docs))
	for _, item := range docs {
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
			revisions := db.ParseRevisions(doc)
			if revisions == nil {
				err = &base.HTTPError{http.StatusBadRequest, "Bad _revisions"}
			} else {
				revid = revisions[0]
				err = h.db.PutExistingRev(docid, doc, revisions)
			}
		}

		status := db.Body{}
		if docid != "" {
			status["id"] = docid
		}
		if err != nil {
			_, msg := base.ErrorAsHTTPStatus(err)
			status["error"] = msg
			base.Log("\tBulkDocs: Doc %q --> %v", docid, err)
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
	var options db.ChangesOptions
	options.Since = h.getIntQuery("since", 0)
	options.Limit = int(h.getIntQuery("limit", 0))
	options.Conflicts = (h.getQuery("style") == "all_docs")
	options.IncludeDocs = (h.getBoolQuery("include_docs"))

	// Get the channels as parameters to an imaginary "bychannel" filter.
	// The default is all channels the user can access.
	userChannels := h.user.Channels()
	filter := h.getQuery("filter")
	if filter != "" {
		if filter != "sync_gateway/bychannel" {
			return &base.HTTPError{http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel"}
		}
		channelsParam := h.getQuery("channels")
		if channelsParam == "" {
			return &base.HTTPError{http.StatusBadRequest, "Missing 'channels' filter parameter"}
		}
		userChannels, err := channels.SetFromArray(strings.Split(channelsParam, ","),
			channels.ExpandStar)
		if err != nil {
			return err
		}
		if err := h.user.AuthorizeAllChannels(userChannels); err != nil {
			return err
		}
	}

	if len(userChannels) == 0 {
		return &base.HTTPError{http.StatusForbidden, "You don't have access to these channels"}
	}

	switch h.getQuery("feed") {
	case "continuous":
		return h.handleContinuousChanges(userChannels, options)
	case "longpoll":
		options.Wait = true
	}
	return h.handleSimpleChanges(userChannels, options)
}

func (h *handler) handleSimpleChanges(channels channels.Set, options db.ChangesOptions) error {
	var lastSeq uint64 = 0
	var first bool = true
	feed, err := h.db.MultiChangesFeed(channels, options)
	if err == nil {
		h.setHeader("Content-Type", "text/plain; charset=utf-8")
		h.writeln([]byte("{\"results\":["))
		if feed != nil {
			for entry := range feed {
				if lastSeq < entry.Seq {
					lastSeq = entry.Seq
				}
				str, _ := json.Marshal(entry)
				var buf []byte
				if first {
					first = false
					buf = str
				} else {
					buf = []byte{','}
					buf = append(buf, str...)
				}
				if err = h.writeln(buf); err != nil {
					err = nil
					break
				}
			}
		}
		s := fmt.Sprintf("],\n\"last_seq\":%d}", lastSeq)
		h.writeln([]byte(s))
	}
	return err
}

func (h *handler) handleContinuousChanges(channels channels.Set, options db.ChangesOptions) error {
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
	var feed <-chan *db.ChangeEntry
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
				base.LogTo("Changes", "send change: %s", str)
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

func (h *handler) handleGetAttachment() error {
	docid := h.PathVars()["docid"]
	attachmentName := h.PathVars()["attach"]
	revid := h.getQuery("rev")
	body, err := h.db.GetRev(docid, revid, false, nil)
	if err != nil {
		return err
	}
	if body == nil {
		return kNotFoundError
	}
	meta, ok := db.BodyAttachments(body)[attachmentName].(map[string]interface{})
	if !ok {
		return &base.HTTPError{http.StatusNotFound, "missing " + attachmentName}
	}
	digest := meta["digest"].(string)
	data, err := h.db.GetAttachment(db.AttachmentKey(digest))
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
	var input db.RevsDiffInput
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &input)
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
func (h *handler) handleGetLocalDoc() error {
	docid := h.PathVars()["docid"]
	value, err := h.db.GetSpecial("local", docid)
	if err != nil {
		return err
	}
	if value == nil {
		return kNotFoundError
	}
	value["_id"] = "_local/" + docid
	value.FixJSONNumbers()
	h.writeJSON(value)
	return nil
}

// HTTP handler for a PUT of a _local document
func (h *handler) handlePutLocalDoc() error {
	docid := h.PathVars()["docid"]
	body, err := h.readJSON()
	if err == nil {
		body.FixJSONNumbers()
		var revid string
		revid, err = h.db.PutSpecial("local", docid, body)
		if err == nil {
			h.writeJSONStatus(http.StatusCreated, db.Body{"ok": true, "id": "_local/" + docid, "rev": revid})
		}
	}
	return err
}

// HTTP handler for a DELETE of a _local document
func (h *handler) handleDelLocalDoc() error {
	docid := h.PathVars()["docid"]
	return h.db.DeleteSpecial("local", docid, h.getQuery("rev"))
}

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	response := map[string]string{
		"couchdb": "welcome",
		"version": VersionString,
	}
	h.writeJSON(response)
	return nil
}

func (h *handler) handleOptions() error {
	//FIX: This is inaccurate; should figure out what methods the requested URL handles.
	h.setHeader("Accept", "GET, HEAD, PUT, DELETE, POST")
	return nil
}

func (h *handler) handleBadRoute() error {
	return &base.HTTPError{http.StatusMethodNotAllowed, "unknown route"}
}

func (h *handler) handleAllDbs() error {
	dbs := []string{}
	for _, db := range h.server.databases {
		dbs = append(dbs, db.dbcontext.Name)
	}
	h.writeJSON(dbs)
	return nil
}

func (h *handler) handleVacuum() error {
	attsDeleted, err := db.VacuumAttachments(h.context.dbcontext.Bucket)
	if err != nil {
		return err
	}
	h.writeJSON(db.Body{"atts": attsDeleted})
	return nil
}

func (h *handler) handleCreateDB() error {
	if h.server.databases[h.PathVars()["newdb"]] != nil {
		return &base.HTTPError{http.StatusConflict, "already exists"}
	} else {
		return &base.HTTPError{http.StatusForbidden, "can't create any databases"}
	}
	return nil // unreachable
}

func (h *handler) handleGetDB() error {
	lastSeq, _ := h.db.LastSequence()
	response := db.Body{
		"db_name":              h.db.Name,
		"doc_count":            h.db.DocCount(),
		"update_seq":           lastSeq,
		"committed_update_seq": lastSeq,
	}
	h.writeJSON(response)
	return nil
}

func (h *handler) handleDeleteDB() error {
	if !h.admin {
		return &base.HTTPError{http.StatusForbidden, "forbidden (admins only)"}
	}
	return h.db.Delete()
}

func (h *handler) handleEFC() error { // Handles _ensure_full_commit.
	// no-op. CouchDB's replicator sends this, so don't barf. Status must be 201.
	h.writeJSONStatus(http.StatusCreated, db.Body{"ok": true})
	return nil
}

func (h *handler) handleDesign() error {
	// we serve this content here so that CouchDB 1.2 has something to
	// hash into the replication-id, to correspond to our filter.
	h.writeJSON(db.Body{"filters": db.Body{"bychannel": "ok"}})
	return nil
}

// Creates a GorillaMux router containing the HTTP handlers for a server.
func createHandler(sc *serverContext) http.Handler {
	r := mux.NewRouter()
	r.StrictSlash(true)
	// Global operations:
	r.Handle("/", makeHandler(sc, (*handler).handleRoot)).Methods("GET", "HEAD")
	r.Handle("/_all_dbs", makeHandler(sc, (*handler).handleAllDbs)).Methods("GET", "HEAD")

	if len(sc.databases) == 1 {
		// If there is exactly one database we can handle the standard /_session by just redirecting
		// it to that database's _session handler.
		for _, db := range sc.databases {
			path := "/" + db.dbcontext.Name
			r.Handle("/_session", http.RedirectHandler(path+"/_session", http.StatusTemporaryRedirect))
			r.Handle("/_browserid", http.RedirectHandler(path+"/_browserid", http.StatusTemporaryRedirect))
		}
	} else {
		r.Handle("/_session", http.NotFoundHandler())
		r.Handle("/_browserid", http.NotFoundHandler())
	}

	// Operations on databases:
	r.Handle("/{newdb}/", makeHandler(sc, (*handler).handleCreateDB)).Methods("PUT")
	r.Handle("/{db}/", makeHandler(sc, (*handler).handleGetDB)).Methods("GET", "HEAD")
	r.Handle("/{db}/", makeHandler(sc, (*handler).handleDeleteDB)).Methods("DELETE")
	r.Handle("/{db}/", makeHandler(sc, (*handler).handlePostDoc)).Methods("POST")

	// Special database URLs:
	dbr := r.PathPrefix("/{db}/").Subrouter()
	dbr.Handle("/_all_docs", makeHandler(sc, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_bulk_docs", makeHandler(sc, (*handler).handleBulkDocs)).Methods("POST")
	dbr.Handle("/_bulk_get", makeHandler(sc, (*handler).handleBulkGet)).Methods("GET", "HEAD")
	dbr.Handle("/_changes", makeHandler(sc, (*handler).handleChanges)).Methods("GET", "HEAD")
	dbr.Handle("/_design/sync_gateway", makeHandler(sc, (*handler).handleDesign)).Methods("GET", "HEAD")
	dbr.Handle("/_ensure_full_commit", makeHandler(sc, (*handler).handleEFC)).Methods("POST")
	dbr.Handle("/_revs_diff", makeHandler(sc, (*handler).handleRevsDiff)).Methods("POST")

	// Session/login URLs are per-database (unlike in CouchDB)
	dbr.Handle("/_session", makeAdminHandler(sc, (*handler).handleSessionGET)).Methods("GET", "HEAD")
	dbr.Handle("/_session", makeAdminHandler(sc, (*handler).handleSessionPOST)).Methods("POST")
	dbr.Handle("/_browserid", makeAdminHandler(sc, (*handler).handleBrowserIDPOST)).Methods("POST")

	// Document URLs:
	dbr.Handle("/_local/{docid}", makeHandler(sc, (*handler).handleGetLocalDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_local/{docid}", makeHandler(sc, (*handler).handlePutLocalDoc)).Methods("PUT")
	dbr.Handle("/_local/{docid}", makeHandler(sc, (*handler).handleDelLocalDoc)).Methods("DELETE")

	dbr.Handle("/{docid}", makeHandler(sc, (*handler).handleGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docid}", makeHandler(sc, (*handler).handlePutDoc)).Methods("PUT")
	dbr.Handle("/{docid}", makeHandler(sc, (*handler).handleDeleteDoc)).Methods("DELETE")

	dbr.Handle("/{docid}/{attach}", makeHandler(sc, (*handler).handleGetAttachment)).Methods("GET", "HEAD")

	// Fallbacks that have to be added last:
	r.PathPrefix("/").Methods("OPTIONS").Handler(makeHandler(sc, (*handler).handleOptions))
	r.PathPrefix("/").Handler(makeHandler(sc, (*handler).handleBadRoute))

	return r
}

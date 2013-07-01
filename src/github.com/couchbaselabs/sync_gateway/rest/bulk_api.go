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
	"html"
	"net/http"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

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
	h.setHeader("Content-Type", "application/json")
	h.response.Write([]byte(`{"rows":[` + "\n"))

	// Assemble the result (and read docs if includeDocs is set)
	totalRows := 0
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
		} else if err := h.db.AuthorizeDocID(id.DocID, id.RevID); err != nil {
			continue
		}
		row.Value = map[string]string{"rev": id.RevID}

		if totalRows > 0 {
			h.response.Write([]byte(",\n"))
		}
		totalRows++
		h.addJSON(row)
	}

	h.response.Write([]byte(fmt.Sprintf("],\n"+`"total_rows":%d}`, totalRows)))
	return nil
}

// HTTP handler for _dump
func (h *handler) handleDump() error {
	viewName := h.PathVars()["view"]
	base.LogTo("HTTP", "Dump view %q", viewName)
	opts := db.Body{"stale": false, "reduce": false}
	result, err := h.db.Bucket.View("sync_gateway", viewName, opts)
	if err != nil {
		return err
	}
	title := fmt.Sprintf("/%s: “%s” View", html.EscapeString(h.db.Name), html.EscapeString(viewName))
	h.setHeader("Content-Type", `text/html; charset="UTF-8"`)
	h.response.Write([]byte(fmt.Sprintf(
		`<!DOCTYPE html><html><head><title>%s</title></head><body>
		<h1>%s</h1><code>
		<table border=1>
		`,
		title, title)))
	h.response.Write([]byte("\t<tr><th>Key</th><th>Value</th><th>ID</th></tr>\n"))
	for _, row := range result.Rows {
		key, _ := json.Marshal(row.Key)
		value, _ := json.Marshal(row.Value)
		h.response.Write([]byte(fmt.Sprintf("\t<tr><td>%s</td><td>%s</td><td><em>%s</em></td>",
			html.EscapeString(string(key)), html.EscapeString(string(value)), html.EscapeString(row.ID))))
		h.response.Write([]byte("</tr>\n"))
	}
	h.response.Write([]byte("</table>\n</code></html></body>"))
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

	h.setHeader("Content-Type", "application/json")
	h.response.Write([]byte(`[`))

	for i, item := range body["docs"].([]interface{}) {
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
			status, reason := base.ErrorAsHTTPStatus(err)
			errStr := base.CouchHTTPErrorName(status)
			body = db.Body{"id": docid, "error": errStr, "reason": reason, "status": status}
			if revid != "" {
				body["rev"] = revid
			}
		}

		if i > 0 {
			h.response.Write([]byte(",\n"))
		}
		h.addJSON(body)
	}

	h.response.Write([]byte(`]`))
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
			code, msg := base.ErrorAsHTTPStatus(err)
			status["status"] = code
			status["error"] = base.CouchHTTPErrorName(code)
			status["reason"] = msg
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

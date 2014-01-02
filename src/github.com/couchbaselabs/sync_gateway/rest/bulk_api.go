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
	"mime/multipart"
	"net/http"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

// HTTP handler for _all_docs
func (h *handler) handleAllDocs() error {
	// http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
	includeDocs := h.getBoolQuery("include_docs")
	includeChannels := h.getBoolQuery("channels") && h.user == nil
	includeAccess := h.getBoolQuery("access") && h.user == nil
	includeRevs := h.getBoolQuery("revs")
	includeSeqs := h.getBoolQuery("update_seq")
	var ids []db.IDAndRev
	var err error
	var docCount int

	// Get the doc IDs:
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		ids, err = h.db.AllDocIDs()
		docCount = len(ids)
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
				err = base.HTTPErrorf(http.StatusBadRequest, "Bad/missing keys")
			}
		}
		docCount = h.db.DocCount()
	}
	if err != nil {
		return err
	}

	type viewRowValue struct {
		Rev      string              `json:"rev"`
		Channels base.Set            `json:"channels,omitempty"` // for admins only
		Access   map[string]base.Set `json:"access,omitempty"`   // for admins only
	}
	type viewRow struct {
		ID        string       `json:"id"`
		Key       string       `json:"key"`
		Value     viewRowValue `json:"value"`
		Doc       db.Body      `json:"doc,omitempty"`
		UpdateSeq uint64       `json:"update_seq,omitempty"`
	}
	h.setHeader("Content-Type", "application/json")
	h.response.Write([]byte(`{"rows":[` + "\n"))

	// Assemble the result (and read docs if includeDocs is set)
	totalRows := 0
	for _, id := range ids {
		row := viewRow{ID: id.DocID, Key: id.DocID}
		if includeDocs || id.RevID == "" || includeChannels || includeAccess {
			// Fetch the document body and other metadata that lives with it:
			body, channels, access, roleAccess, err := h.db.GetRevAndChannels(id.DocID, id.RevID, includeRevs)
			if err != nil || body["_removed"] != nil {
				continue
			}
			id.RevID = body["_rev"].(string)
			if includeDocs {
				row.Doc = body
			}
			if includeChannels && channels != nil {
				row.Value.Channels = base.Set{}
				for channelName, _ := range channels {
					row.Value.Channels[channelName] = struct{}{}
				}
			}
			if includeAccess && (access != nil || roleAccess != nil) {
				row.Value.Access = map[string]base.Set{}
				for userName, channels := range access {
					row.Value.Access[userName] = channels.AsSet()
				}
				for roleName, channels := range roleAccess {
					row.Value.Access["role:"+roleName] = channels.AsSet()
				}
			}
		} else if err := h.db.AuthorizeDocID(id.DocID, id.RevID); err != nil {
			continue
		}
		if includeSeqs {
			row.UpdateSeq = id.Sequence
		}
		row.Value.Rev = id.RevID

		if totalRows > 0 {
			h.response.Write([]byte(",\n"))
		}
		totalRows++
		h.addJSON(row)
	}

	lastSeq, _ := h.db.LastSequence()
	h.response.Write([]byte(fmt.Sprintf("],\n"+`"total_rows":%d,"update_seq":%d}`,
		docCount, lastSeq)))
	return nil
}

// HTTP handler for _dump
func (h *handler) handleDump() error {
	viewName := h.PathVar("view")
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

// HTTP handler for _view
func (h *handler) handleView() error {
	viewName := h.PathVar("view")
	base.LogTo("HTTP", "JSON view %q", viewName)
	opts := db.Body{ // for now we always return the full result set
		// "stale": h.getBoolQuery("stale"),
		"reduce": false,
		// "startkey" : h.getQuery("startkey"),
		// "endkey" : h.getQuery("endkey"),
		// "group_level" : h.getIntQuery("group_level", 1),
	}
	result, err := h.db.Bucket.View("sync_gateway", viewName, opts)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.response.Write([]byte(`{"rows":[`))
	first := true
	for _, row := range result.Rows {
		if (first) {
			first = false
		} else {
			h.response.Write([]byte(",\n"))
		}
		key, _ := json.Marshal(row.Key)
		value, _ := json.Marshal(row.Value)
		id, _ := json.Marshal(row.ID)
		h.response.Write([]byte(fmt.Sprintf("{\"key\":%s, \"value\":%s , \"id\":%s}",
			string(key), string(value), string(id))))
	}
	h.response.Write([]byte("]}\n"))
	return nil
}


// HTTP handler for _dumpchannel
func (h *handler) handleDumpChannel() error {
	channelName := h.PathVar("channel")
	base.LogTo("HTTP", "Dump channel %q", channelName)

	chanLog, err := h.db.GetChangeLog(channelName, 0)
	if err != nil {
		return err
	} else if chanLog == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no such channel")
	}
	title := fmt.Sprintf("/%s: “%s” Channel", html.EscapeString(h.db.Name), html.EscapeString(channelName))
	h.setHeader("Content-Type", `text/html; charset="UTF-8"`)
	h.response.Write([]byte(fmt.Sprintf(
		`<!DOCTYPE html><html><head><title>%s</title></head><body>
		<h1>%s</h1><code>
		<p>Since = %d</p>
		<table border=1>
		`,
		title, title, chanLog.Since)))
	h.response.Write([]byte("\t<tr><th>Seq</th><th>Doc</th><th>Rev</th><th>Flags</th></tr>\n"))
	for _, entry := range chanLog.Entries {
		h.response.Write([]byte(fmt.Sprintf("\t<tr><td>%d</td><td>%s</td><td>%s</td><td>%08b</td>",
			entry.Sequence,
			html.EscapeString(entry.DocID), html.EscapeString(entry.RevID), entry.Flags)))
		h.response.Write([]byte("</tr>\n"))
	}
	h.response.Write([]byte("</table>\n</code></html></body>"))
	return nil
}

// HTTP handler for a POST to _bulk_get
// Request looks like POST /db/_bulk_get?revs=___&attachments=___
// where the boolean ?revs parameter adds a revision history to each doc
// and the boolean ?attachments parameter includes attachment bodies.
// The body of the request is JSON and looks like:
// {
//   "docs": [
//		{"id": "docid", "rev": "revid", "atts_since": [12,...]}, ...
// 	 ]
// }
func (h *handler) handleBulkGet() error {
	includeRevs := h.getBoolQuery("revs")
	includeAttachments := h.getBoolQuery("attachments")
	body, err := h.readJSON()
	if err != nil {
		return err
	}

	err = h.writeMultipart(func(writer *multipart.Writer) error {
		for _, item := range body["docs"].([]interface{}) {
			doc := item.(map[string]interface{})
			docid, _ := doc["id"].(string)
			revid := ""
			revok := true
			if doc["rev"] != nil {
				revid, revok = doc["rev"].(string)
			}
			if docid == "" || !revok {
				return base.HTTPErrorf(http.StatusBadRequest, "Invalid doc/rev ID")
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
						return base.HTTPErrorf(http.StatusBadRequest, "Invalid atts_since")
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

			h.db.WriteRevisionAsPart(body, err != nil, writer)
		}
		return nil
	})

	return err
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
				err = base.HTTPErrorf(http.StatusBadRequest, "Bad _revisions")
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
			base.Log("\tBulkDocs: Doc %q --> %d %s (%v)", docid, code, msg, err)
			err = nil // wrote it to output already; not going to return it
		} else {
			status["rev"] = revid
		}
		result = append(result, status)
	}

	h.writeJSONStatus(http.StatusCreated, result)
	return nil
}

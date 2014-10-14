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
	"strings"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

// HTTP handler for _all_docs
func (h *handler) handleAllDocs() error {
	// http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
	includeDocs := h.getBoolQuery("include_docs")
	includeChannels := h.getBoolQuery("channels")
	includeAccess := h.getBoolQuery("access") && h.user == nil
	includeRevs := h.getBoolQuery("revs")
	includeSeqs := h.getBoolQuery("update_seq")

	// Get the doc IDs if this is a POST request:
	var explicitDocIDs []string
	if h.rq.Method == "POST" {
		input, err := h.readJSON()
		if err == nil {
			keys, ok := input["keys"].([]interface{})
			explicitDocIDs = make([]string, len(keys))
			for i := 0; i < len(keys); i++ {
				if explicitDocIDs[i], ok = keys[i].(string); !ok {
					break
				}
			}
			if !ok {
				err = base.HTTPErrorf(http.StatusBadRequest, "Bad/missing keys")
			}
		}
	}

	// Get the set of channels the user has access to; nil if user is admin or has access to "*"
	var availableChannels channels.TimedSet
	if h.user != nil {
		availableChannels = h.user.InheritedChannels()
		if availableChannels == nil {
			panic("no channels for user?")
		}
		if availableChannels.Contains("*") {
			availableChannels = nil
		}
	}

	// Subroutines that filter a channel list down to the ones that the user has access to:
	filterChannels := func(channels []string) []string {
		if availableChannels == nil {
			return channels
		}
		dst := 0
		for _, ch := range channels {
			if availableChannels.Contains(ch) {
				channels[dst] = ch
				dst++
			}
		}
		if dst == 0 {
			return nil
		}
		return channels[0:dst]
	}
	filterChannelSet := func(channelMap channels.ChannelMap) []string {
		var result []string
		if availableChannels == nil {
			result = []string{}
		}
		for ch, rm := range channelMap {
			if availableChannels == nil || availableChannels.Contains(ch) {
				//Do not include channels doc removed from in this rev
				if rm == nil {
					result = append(result, ch)
				}
			}
		}
		return result
	}

	type allDocsRowValue struct {
		Rev      string              `json:"rev"`
		Channels []string            `json:"channels,omitempty"`
		Access   map[string]base.Set `json:"access,omitempty"` // for admins only
	}
	type allDocsRow struct {
		Key       string           `json:"key"`
		ID        string           `json:"id,omitempty"`
		Value     *allDocsRowValue `json:"value,omitempty"`
		Doc       db.Body          `json:"doc,omitempty"`
		UpdateSeq uint64           `json:"update_seq,omitempty"`
		Error     string           `json:"error,omitempty"`
		Status    int              `json:"status,omitempty"`
	}

	// Subroutine that creates a response row for a document:
	totalRows := 0
	createRow := func(doc db.IDAndRev, channels []string) *allDocsRow {
		row := &allDocsRow{Key: doc.DocID}
		value := allDocsRowValue{}

		// Filter channels to ones available to user, and bail out if inaccessible:
		if explicitDocIDs == nil {
			if channels = filterChannels(channels); channels == nil {
				return nil // silently skip this doc
			}
		}

		if explicitDocIDs != nil || includeDocs || includeAccess {
			// Fetch the document body and other metadata that lives with it:
			body, channelSet, access, roleAccess, err := h.db.GetRevAndChannels(doc.DocID, doc.RevID, includeRevs)
			if err != nil {
				row.Status, _ = base.ErrorAsHTTPStatus(err)
				return row
			} else if body["_removed"] != nil {
				row.Status = http.StatusForbidden
				return row
			}
			if explicitDocIDs != nil {
				if channels = filterChannelSet(channelSet); channels == nil {
					row.Status = http.StatusForbidden
					return row
				}
				doc.RevID = body["_rev"].(string)
			}
			if includeDocs {
				row.Doc = body
			}
			if includeAccess && (access != nil || roleAccess != nil) {
				value.Access = map[string]base.Set{}
				for userName, channels := range access {
					value.Access[userName] = channels.AsSet()
				}
				for roleName, channels := range roleAccess {
					value.Access["role:"+roleName] = channels.AsSet()
				}
			}
		}

		row.Value = &value
		row.ID = doc.DocID
		value.Rev = doc.RevID
		if includeSeqs {
			row.UpdateSeq = doc.Sequence
		}
		if includeChannels {
			row.Value.Channels = channels
		}
		return row
	}

	// Subroutine that writes a response entry for a document:
	writeDoc := func(doc db.IDAndRev, channels []string) error {
		row := createRow(doc, channels)
		if row != nil {
			if row.Status >= 300 {
				row.Error = base.CouchHTTPErrorName(row.Status)
			}
			if totalRows > 0 {
				h.response.Write([]byte(","))
			}
			totalRows++
			h.addJSON(row)
		}
		return nil
	}

	// Now it's time to actually write the response!
	lastSeq, _ := h.db.LastSequence()
	h.setHeader("Content-Type", "application/json")
	h.response.Write([]byte(`{"rows":[` + "\n"))

	if explicitDocIDs != nil {
		for _, docID := range explicitDocIDs {
			writeDoc(db.IDAndRev{DocID: docID, RevID: "", Sequence: 0}, nil)
		}
	} else {
		if err := h.db.ForEachDocID(writeDoc); err != nil {
			return err
		}
	}

	h.response.Write([]byte(fmt.Sprintf("],\n"+`"total_rows":%d,"update_seq":%d}`,
		totalRows, lastSeq)))
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

// HTTP handler for _dumpchannel
func (h *handler) handleDumpChannel() error {
	channelName := h.PathVar("channel")
	since := h.getIntQuery("since", 0)
	base.LogTo("HTTP", "Dump channel %q", channelName)

	chanLog := h.db.GetChangeLog(channelName, since)
	if chanLog == nil {
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
		title, title, chanLog[0].Sequence-1)))
	h.response.Write([]byte("\t<tr><th>Seq</th><th>Doc</th><th>Rev</th><th>Flags</th></tr>\n"))
	for _, entry := range chanLog {
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
	canCompress := strings.Contains(h.rq.Header.Get("X-Accept-Part-Encoding"), "gzip")
	body, err := h.readJSON()
	if err != nil {
		return err
	}

	err = h.writeMultipart("mixed", func(writer *multipart.Writer) error {
		for _, item := range body["docs"].([]interface{}) {
			var body db.Body
			var attsSince []string
			var err error

			doc := item.(map[string]interface{})
			docid, _ := doc["id"].(string)
			revid := ""
			revok := true
			if doc["rev"] != nil {
				revid, revok = doc["rev"].(string)
			}
			if docid == "" || !revok {
				err = base.HTTPErrorf(http.StatusBadRequest, "Invalid doc/rev ID in _bulk_get")
			} else {
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
							err = base.HTTPErrorf(http.StatusBadRequest, "Invalid atts_since")
						}
					} else {
						attsSince = []string{}
					}
				}
			}

			if err == nil {
				body, err = h.db.GetRev(docid, revid, includeRevs, attsSince)
			}

			if err != nil {
				// Report error in the response for this doc:
				status, reason := base.ErrorAsHTTPStatus(err)
				errStr := base.CouchHTTPErrorName(status)
				body = db.Body{"id": docid, "error": errStr, "reason": reason, "status": status}
				if revid != "" {
					body["rev"] = revid
				}
			}

			h.db.WriteRevisionAsPart(body, err != nil, canCompress, writer)
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

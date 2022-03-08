//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	pkgerrors "github.com/pkg/errors"
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
			if explicitDocIDs, _ = db.GetStringArrayProperty(input, "keys"); explicitDocIDs == nil {
				err = base.HTTPErrorf(http.StatusBadRequest, "Bad/missing keys")
			}
		}
		if err != nil {
			return err
		}
	} else if h.rq.Method == "GET" {
		var err error
		if explicitDocIDs, err = h.getJSONStringArrayQuery("keys"); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Bad keys")
		}
	}

	// Get the set of channels the user has access to; nil if user is admin or has access to user "*"
	var availableChannels ch.TimedSet
	if h.user != nil {
		availableChannels = h.user.InheritedChannels()
		if availableChannels == nil {
			// TODO: CBG-1948
			panic("no channels for user?")
		}
		if availableChannels.Contains(ch.UserStarChannel) {
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
	filterChannelSet := func(channelMap ch.ChannelMap) []string {
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
		Doc       json.RawMessage  `json:"doc,omitempty"`
		UpdateSeq uint64           `json:"update_seq,omitempty"`
		Error     string           `json:"error,omitempty"`
		Status    int              `json:"status,omitempty"`
	}

	// Subroutine that creates a response row for a document:
	totalRows := 0
	createRow := func(doc db.IDRevAndSequence, channels []string) *allDocsRow {
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
			bodyBytes, channelSet, access, roleAccess, _, _, currentRevID, removed, err := h.db.Get1xRevAndChannels(doc.DocID, doc.RevID, includeRevs)
			if err != nil {
				row.Status, _ = base.ErrorAsHTTPStatus(err)
				return row
			} else if removed {
				row.Status = http.StatusForbidden
				return row
			}
			if explicitDocIDs != nil {
				if channels = filterChannelSet(channelSet); channels == nil {
					row.Status = http.StatusForbidden
					return row
				}
				// handle the case where the incoming doc.RevID == ""
				// and Get1xRevAndChannels returns the current revision
				doc.RevID = currentRevID
			}
			if includeDocs {
				row.Doc = bodyBytes
			}
			if includeAccess && (access != nil || roleAccess != nil) {
				value.Access = map[string]base.Set{}
				for userName, channels := range access {
					value.Access[userName] = channels.AsSet()
				}
				for roleName, channels := range roleAccess {
					value.Access[ch.RoleAccessPrefix+roleName] = channels.AsSet()
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
	writeDoc := func(doc db.IDRevAndSequence, channels []string) (bool, error) {
		row := createRow(doc, channels)
		if row != nil {
			if row.Status >= 300 {
				row.Error = base.CouchHTTPErrorName(row.Status)
			}
			if totalRows > 0 {
				_, _ = h.response.Write([]byte(","))
			}
			totalRows++
			var err error
			err = h.addJSON(row)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}

	var options db.ForEachDocIDOptions
	options.Startkey = h.getJSONStringQuery("startkey")
	options.Endkey = h.getJSONStringQuery("endkey")
	options.Limit = h.getIntQuery("limit", 0)

	// Now it's time to actually write the response!
	lastSeq, _ := h.db.LastSequence()
	h.setHeader("Content-Type", "application/json")
	//response.Write below would set Status OK implicitly. We manually do it here to ensure that our handler knows
	//that the header has been written to, meaning we can prevent it from attempting to set the header again later on.
	h.writeStatus(http.StatusOK, http.StatusText(http.StatusOK))
	_, _ = h.response.Write([]byte(`{"rows":[` + "\n"))
	if explicitDocIDs != nil {
		count := uint64(0)
		for _, docID := range explicitDocIDs {
			_, _ = writeDoc(db.IDRevAndSequence{DocID: docID, RevID: "", Sequence: 0}, nil)
			count++
			if options.Limit > 0 && count == options.Limit {
				break
			}

		}
	} else {
		if err := h.db.ForEachDocID(writeDoc, options); err != nil {
			return err
		}
	}

	_, _ = h.response.Write([]byte(fmt.Sprintf("],\n"+`"total_rows":%d,"update_seq":%d}`,
		totalRows, lastSeq)))

	return nil
}

// HTTP handler for _dump
func (h *handler) handleDump() error {
	viewName := h.PathVar("view")
	base.InfofCtx(h.ctx(), base.KeyHTTP, "Dump view %q", base.MD(viewName))
	opts := db.Body{"stale": false, "reduce": false}
	result, err := h.db.Bucket.View(db.DesignDocSyncGateway(), viewName, opts)
	if err != nil {
		return err
	}
	title := fmt.Sprintf("/%s: “%s” View", html.EscapeString(h.db.Name), html.EscapeString(viewName))
	h.setHeader("Content-Type", `text/html; charset="UTF-8"`)
	_, _ = h.response.Write([]byte(fmt.Sprintf(
		`<!DOCTYPE html><html><head><title>%s</title></head><body>
		<h1>%s</h1><code>
		<table border=1>
		`,
		title, title)))
	_, _ = h.response.Write([]byte("\t<tr><th>Key</th><th>Value</th><th>ID</th></tr>\n"))
	for _, row := range result.Rows {
		key, _ := base.JSONMarshal(row.Key)
		value, _ := base.JSONMarshal(row.Value)
		_, _ = h.response.Write([]byte(fmt.Sprintf("\t<tr><td>%s</td><td>%s</td><td><em>%s</em></td>",
			html.EscapeString(string(key)), html.EscapeString(string(value)), html.EscapeString(row.ID))))
		_, _ = h.response.Write([]byte("</tr>\n"))
	}
	_, _ = h.response.Write([]byte("</table>\n</code></html></body>"))
	return nil
}

// HTTP handler for _repair
func (h *handler) handleRepair() error {

	// TODO: If repair is re-enabled, it may need to be modified to support xattrs and GSI
	if true == true {
		return errors.New("_repair endpoint disabled")
	}

	base.InfofCtx(h.ctx(), base.KeyHTTP, "Repair bucket")

	// Todo: is this actually needed or does something else in the handler do it?  I can't find that..
	defer func() {
		_ = h.requestBody.Close()
	}()

	body, err := h.readBody()
	if err != nil {
		return err
	}

	repairBucketParams := db.RepairBucketParams{}
	if err := base.JSONUnmarshal(body, &repairBucketParams); err != nil {
		return pkgerrors.Wrapf(err, "Error unmarshalling %v into RepairJobParams.", string(body))
	}

	repairBucket := db.NewRepairBucket(h.db.Bucket)

	repairBucket.InitFrom(repairBucketParams)

	repairBucketResult, repairDocsErr := repairBucket.RepairBucket()
	if repairDocsErr != nil {
		return err
	}

	resultMarshalled, err := base.JSONMarshal(repairBucketResult)
	if err != nil {
		return pkgerrors.Wrapf(err, "Error marshalling repairBucketResult: %+v", repairBucketResult)
	}

	h.setHeader("Content-Type", "application/json")
	_, err = h.response.Write(resultMarshalled)

	return err
}

// HTTP handler for _dumpchannel
func (h *handler) handleDumpChannel() error {
	channelName := h.PathVar("channel")
	since := h.getIntQuery("since", 0)
	base.InfofCtx(h.ctx(), base.KeyHTTP, "Dump channel %q", base.UD(channelName))

	chanLog := h.db.GetChangeLog(channelName, since)
	if chanLog == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no such channel")
	}
	title := fmt.Sprintf("/%s: “%s” Channel", html.EscapeString(h.db.Name), html.EscapeString(channelName))
	h.setHeader("Content-Type", `text/html; charset="UTF-8"`)
	_, _ = h.response.Write([]byte(fmt.Sprintf(
		`<!DOCTYPE html><html><head><title>%s</title></head><body>
		<h1>%s</h1><code>
		<p>Since = %d</p>
		<table border=1>
		`,
		title, title, chanLog[0].Sequence-1)))
	_, _ = h.response.Write([]byte("\t<tr><th>Seq</th><th>Doc</th><th>Rev</th><th>Flags</th></tr>\n"))
	for _, entry := range chanLog {
		_, _ = h.response.Write([]byte(fmt.Sprintf("\t<tr><td>%d</td><td>%s</td><td>%s</td><td>%08b</td>",
			entry.Sequence,
			html.EscapeString(entry.DocID), html.EscapeString(entry.RevID), entry.Flags)))
		_, _ = h.response.Write([]byte("</tr>\n"))
	}
	_, _ = h.response.Write([]byte("</table>\n</code></html></body>"))
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

	includeAttachments := h.getBoolQuery("attachments")
	showExp := h.getBoolQuery("show_exp")

	showRevs := h.getBoolQuery("revs")
	globalRevsLimit := int(h.getIntQuery("revs_limit", math.MaxInt32))

	// If a client passes the HTTP header "Accept-Encoding: gzip" then the header "X-Accept-Part-Encoding: gzip" will be
	// ignored and the entire HTTP response will be gzip compressed.  (aside from exception mentioned below for issue 1419)
	acceptGzipPartEncoding := strings.Contains(h.rq.Header.Get("X-Accept-Part-Encoding"), "gzip")
	acceptGzipEncoding := strings.Contains(h.rq.Header.Get("Accept-Encoding"), "gzip")
	canCompressParts := acceptGzipPartEncoding && !acceptGzipEncoding

	// Exception: if the user agent is empty or earlier than 1.2, and X-Accept-Part-Encoding=gzip, then we actually
	// DO want to compress the parts since the full response will not be gzipped, since those clients can't handle it.
	// See https://github.com/couchbase/sync_gateway/issues/1419 and encoded_response_writer.go
	userAgentVersion := NewUserAgentVersion(h.rq.Header.Get("User-Agent"))
	if userAgentVersion.IsBefore(1, 2) && acceptGzipPartEncoding {
		canCompressParts = true
	}

	body, err := h.readJSON()
	if err != nil {
		return err
	}

	docs, ok := body["docs"].([]interface{})
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "missing 'docs' property")
	}

	return h.writeMultipart("mixed", func(writer *multipart.Writer) error {
		for _, item := range docs {
			var body db.Body
			var revsFrom, attsSince []string
			var docRevsLimit int
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
				attsSince, err = db.GetStringArrayProperty(doc, "atts_since")

				if showRevs {
					docRevsLimit = globalRevsLimit

					// Try to pull out a per-doc revs limit that can override the global one.
					if raw, isSet := doc["revs_limit"]; isSet {
						if val, ok := base.ToInt64(raw); ok && val >= 0 {
							docRevsLimit = int(val)
						} else {
							err = base.HTTPErrorf(http.StatusBadRequest, "Invalid revs_limit for doc: %s in _bulk_get", docid)
						}
					}

					if docRevsLimit > 0 {
						revsFrom, err = db.GetStringArrayProperty(doc, "revs_from")
						if revsFrom == nil {
							revsFrom = attsSince // revs_from defaults to same value as atts_since
						}
					}
				}

				if !includeAttachments {
					attsSince = nil
				} else if attsSince == nil {
					attsSince = []string{}
				}

			}

			if err == nil {
				body, err = h.db.Get1xRevBodyWithHistory(docid, revid, docRevsLimit, revsFrom, attsSince, showExp)
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

			_ = WriteRevisionAsPart(h.rq.Context(), h.db.DatabaseContext.DbStats.CBLReplicationPull(), body, err != nil, canCompressParts, writer)

			h.db.DbStats.Database().NumDocReadsRest.Add(1)
		}
		return nil
	})
}

// HTTP handler for a POST to _bulk_docs
func (h *handler) handleBulkDocs() error {

	startTime := time.Now()
	defer func() {
		h.db.DbStats.CBLReplicationPush().WriteProcessingTime.Add(time.Since(startTime).Nanoseconds())
	}()

	body, err := h.readJSON()
	if err != nil {
		return err
	}

	newEdits, ok := body["new_edits"].(bool)
	if !ok {
		newEdits = true
	}

	userDocs, ok := body["docs"].([]interface{})
	if !ok {
		err = base.HTTPErrorf(http.StatusBadRequest, "missing 'docs' property")
		return err
	}
	lenDocs := len(userDocs)

	// split out local docs, save them on their own
	localDocs := make([]interface{}, 0, lenDocs)
	docs := make([]interface{}, 0, lenDocs)
	for _, item := range userDocs {
		doc, ok := item.(map[string]interface{})
		if !ok {
			err = base.HTTPErrorf(http.StatusBadRequest, "Document body must be JSON")
			return err
		}

		// If ID is present, check whether local doc. (note: if _id is absent or non-string, docid will be
		// empty string and handled during normal doc processing)
		docid, _ := doc[db.BodyId].(string)

		if strings.HasPrefix(docid, "_local/") {
			localDocs = append(localDocs, doc)
		} else {
			docs = append(docs, doc)
		}
	}

	result := make([]db.Body, 0, len(docs))
	for _, item := range docs {
		doc := item.(map[string]interface{})
		docid, _ := doc[db.BodyId].(string)
		var err error
		var revid string
		if newEdits {
			if docid != "" {
				revid, _, err = h.db.Put(docid, doc)
			} else {
				docid, revid, _, err = h.db.Post(doc)
			}
		} else {
			revisions := db.ParseRevisions(doc)
			if revisions == nil {
				err = base.HTTPErrorf(http.StatusBadRequest, "Bad _revisions")
			} else {
				revid = revisions[0]
				_, _, err = h.db.PutExistingRevWithBody(docid, doc, revisions, false)
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
			base.InfofCtx(h.ctx(), base.KeyAll, "\tBulkDocs: Doc %q --> %d %s (%v)", base.UD(docid), code, msg, err)
			err = nil // wrote it to output already; not going to return it
		} else {
			status["rev"] = revid
		}
		result = append(result, status)
	}

	for _, item := range localDocs {
		doc := item.(map[string]interface{})
		for k, v := range doc {
			doc[k] = base.FixJSONNumbers(v)
		}
		var err error
		var revid string
		offset := len("_local/")
		docid, _ := doc[db.BodyId].(string)
		idslug := docid[offset:]
		revid, err = h.db.PutSpecial(db.DocTypeLocal, idslug, doc)
		status := db.Body{}
		status["id"] = docid
		if err != nil {
			code, msg := base.ErrorAsHTTPStatus(err)
			status["status"] = code
			status["error"] = base.CouchHTTPErrorName(code)
			status["reason"] = msg
			base.InfofCtx(h.ctx(), base.KeyAll, "\tBulkDocs: Local Doc %q --> %d %s (%v)", base.UD(docid), code, msg, err)
			err = nil
		} else {
			status["rev"] = revid
		}
		result = append(result, status)
	}

	h.writeJSONStatus(http.StatusCreated, result)
	return nil
}

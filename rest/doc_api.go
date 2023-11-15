//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// HTTP handler for a GET of a document
func (h *handler) handleGetDoc() error {
	docid := h.PathVar("docid")
	revid := h.getQuery("rev")
	openRevs := h.getQuery("open_revs")
	showExp := h.getBoolQuery("show_exp")

	if replicator2, _ := h.getOptBoolQuery("replicator2", false); replicator2 {
		return h.handleGetDocReplicator2(docid, revid)
	}

	// Check whether the caller wants a revision history, or attachment bodies, or both:
	var revsLimit = 0
	var revsFrom, attachmentsSince []string
	{
		var err error
		var attsSinceParam, revsFromParam []string
		if revsFromParam, err = h.getJSONStringArrayQuery("revs_from"); err != nil {
			return err
		}
		if attsSinceParam, err = h.getJSONStringArrayQuery("atts_since"); err != nil {
			return err
		}

		if h.getBoolQuery("revs") {
			revsLimit = int(h.getIntQuery("revs_limit", math.MaxInt32))
			if revsFromParam != nil {
				revsFrom = revsFromParam
			} else {
				revsFrom = attsSinceParam // revs_from defaults to same value as atts_since
			}
		}

		if h.getBoolQuery("attachments") {
			if attsSinceParam != nil {
				attachmentsSince = attsSinceParam
			} else {
				attachmentsSince = []string{}
			}
		}
	}

	if openRevs == "" {
		// Single-revision GET:
		value, err := h.collection.Get1xRevBodyWithHistory(h.ctx(), docid, revid, revsLimit, revsFrom, attachmentsSince, showExp)
		if err != nil {
			if err == base.ErrImportCancelledPurged {
				base.DebugfCtx(h.ctx(), base.KeyImport, fmt.Sprintf("Import cancelled as document %v is purged", base.UD(docid)))
				return nil
			}
			if h.collection.ForceAPIForbiddenErrors() && base.IsDocNotFoundError(err) {
				base.InfofCtx(h.ctx(), base.KeyCRUD, "Doc %q not found: %v", base.UD(docid), err)
				return db.ErrForbidden
			}
			return err
		}
		if value == nil {
			if h.collection.ForceAPIForbiddenErrors() {
				base.InfofCtx(h.ctx(), base.KeyCRUD, "Doc %q missing", base.UD(docid))
				return db.ErrForbidden
			}
			return kNotFoundError
		}
		h.setEtag(value[db.BodyRev].(string))

		h.db.DbStats.Database().NumDocReadsRest.Add(1)
		hasBodies := attachmentsSince != nil && value[db.BodyAttachments] != nil
		if h.requestAccepts("multipart/") && (hasBodies || !h.requestAccepts("application/json")) {
			canCompress := strings.Contains(h.rq.Header.Get("X-Accept-Part-Encoding"), "gzip")
			return h.writeMultipart("related", func(writer *multipart.Writer) error {
				WriteMultipartDocument(h.ctx(), h.db.DatabaseContext.DbStats.CBLReplicationPull(), value, writer, canCompress)
				return nil
			})
		} else {
			h.writeJSON(value)
		}
	} else {
		var revids []string
		attachmentsSince = []string{}

		if openRevs == "all" {
			// open_revs=all
			doc, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalSync)
			if err != nil {
				return err
			}
			if doc == nil {
				return kNotFoundError
			}
			revids = doc.History.GetLeaves()
		} else {
			// open_revs=["id1", "id2", ...]
			err := base.JSONUnmarshal([]byte(openRevs), &revids)
			if err != nil {
				return base.HTTPErrorf(http.StatusBadRequest, "bad open_revs")
			}
		}

		if h.requestAccepts("multipart/") {
			err := h.writeMultipart("mixed", func(writer *multipart.Writer) error {
				for _, revid := range revids {
					revBody, err := h.collection.Get1xRevBodyWithHistory(h.ctx(), docid, revid, revsLimit, revsFrom, attachmentsSince, showExp)
					if err != nil {
						revBody = db.Body{"missing": revid} // TODO: More specific error
					}
					_ = WriteRevisionAsPart(h.ctx(), h.db.DatabaseContext.DbStats.CBLReplicationPull(), revBody, err != nil, false, writer)
					h.db.DbStats.Database().NumDocReadsRest.Add(1)
				}
				return nil
			})
			return err
		} else {
			base.DebugfCtx(h.ctx(), base.KeyHTTP, "Fallback to non-multipart for open_revs")
			h.setHeader("Content-Type", "application/json")
			_, _ = h.response.Write([]byte(`[` + "\n"))
			separator := []byte(``)
			for _, revid := range revids {
				revBody, err := h.collection.Get1xRevBodyWithHistory(h.ctx(), docid, revid, revsLimit, revsFrom, attachmentsSince, showExp)
				if err != nil {
					revBody = db.Body{"missing": revid} // TODO: More specific error
				} else {
					revBody = db.Body{"ok": revBody}
				}
				_, _ = h.response.Write(separator)
				separator = []byte(",")
				err = h.addJSON(revBody)
				if err != nil {
					return err
				}
			}
			_, _ = h.response.Write([]byte(`]`))
			h.db.DbStats.Database().NumDocReadsRest.Add(1)
		}
	}
	return nil
}

func (h *handler) handleGetDocReplicator2(docid, revid string) error {
	if !base.IsEnterpriseEdition() {
		return base.HTTPErrorf(http.StatusNotImplemented, "replicator2 endpoints are only supported in EE")
	}

	rev, err := h.collection.GetRev(h.ctx(), docid, revid, true, nil)
	if err != nil {
		return err
	}

	// Stamp _attachments into message to match BLIP sendRevision behaviour
	bodyBytes := rev.BodyBytes
	if len(rev.Attachments) > 0 {
		bodyBytes, err = base.InjectJSONProperties(bodyBytes, base.KVPair{Key: db.BodyAttachments, Val: rev.Attachments})
		if err != nil {
			return err
		}
	}

	h.setHeader("Content-Type", "application/json")
	_, _ = h.response.Write(bodyBytes)
	h.db.DbStats.Database().NumDocReadsRest.Add(1)

	return nil
}

// HTTP handler for a GET of a specific doc attachment
func (h *handler) handleGetAttachment() error {
	docid := h.PathVar("docid")
	attachmentName := h.PathVar("attach")
	revid := h.getQuery("rev")
	rev, err := h.collection.GetRev(h.ctx(), docid, revid, false, nil)
	if err != nil {
		return err
	}
	if rev.BodyBytes == nil {
		return kNotFoundError
	}

	meta, ok := rev.Attachments[attachmentName].(map[string]interface{})
	if !ok {
		return base.HTTPErrorf(http.StatusNotFound, "missing attachment %s", attachmentName)
	}
	digest := meta["digest"].(string)
	version, ok := db.GetAttachmentVersion(meta)
	if !ok {
		return db.ErrAttachmentVersion
	}
	attachmentKey := db.MakeAttachmentKey(version, docid, digest)
	data, err := h.collection.GetAttachment(attachmentKey)
	if err != nil {
		return err
	}

	metaOption := h.getBoolQuery("meta")
	if metaOption {
		meta["key"] = attachmentKey
		h.writeJSONStatus(http.StatusOK, meta)
		return nil
	}

	status, start, end := h.handleRange(uint64(len(data)))
	if status > 299 {
		return base.HTTPErrorf(status, "")
	} else if status == http.StatusPartialContent {
		data = data[start:end]
	}
	h.setHeader("Content-Length", strconv.FormatUint(uint64(len(data)), 10))

	// #720
	setContentDisposition := h.privs == adminPrivs

	h.setEtag(digest)

	// Request will be returned with the same content type as is set on the attachment. The caveat to this is if the
	// attachment has a content type which is vulnerable to a phishing attack. If this is the case we will return with
	// the Content Disposition header so that browsers will download the attachment rather than attempt to render it
	// unless overridden by config option. CBG-1004
	contentType, contentTypeSet := meta["content_type"].(string)
	if contentTypeSet {
		h.setHeader("Content-Type", contentType)
	}

	if !h.db.ServeInsecureAttachmentTypes {

		if contentTypeSet {
			// This split is required as the content type field can have other elements after a ; such as charset,
			// however, we only care about checking the first part. In the event that there is no ';' strings.Split just
			// takes the full contentType string
			contentTypeFirst := strings.Split(contentType, ";")[0]
			switch contentTypeFirst {
			case
				"",
				"text/html",
				"application/xhtml+xml",
				"image/svg+xml":
				setContentDisposition = true
			}
		} else {
			setContentDisposition = true
		}
	}

	if encoding, ok := meta["encoding"].(string); ok {
		if result, _ := h.getOptBoolQuery("content_encoding", true); result {
			h.setHeader("Content-Encoding", encoding)
		} else {
			// Couchbase Lite wants to download the encoded form directly and store it that way,
			// but some HTTP client libraries like NSURLConnection will automatically decompress
			// the HTTP response if it has a Content-Encoding header. As a workaround, allow the
			// client to add ?content_encoding=false to the request URL to disable setting this
			// header.
			h.setHeader("X-Content-Encoding", encoding)
			h.setHeader("Content-Type", "application/gzip")
		}
	}
	if setContentDisposition {
		h.setHeader("Content-Disposition", "attachment")

	}
	h.db.DbStats.CBLReplicationPull().AttachmentPullCount.Add(1)
	h.db.DbStats.CBLReplicationPull().AttachmentPullBytes.Add(int64(len(data)))
	h.response.WriteHeader(status)
	_, _ = h.response.Write(data)
	return nil

}

// HTTP handler for a PUT of an attachment
func (h *handler) handlePutAttachment() error {

	if h.isReadOnlyGuest() {
		return base.HTTPErrorf(http.StatusForbidden, auth.GuestUserReadOnly)
	}

	docid := h.PathVar("docid")
	attachmentName := h.PathVar("attach")
	attachmentContentType := h.rq.Header.Get("Content-Type")
	if attachmentContentType == "" {
		attachmentContentType = "application/octet-stream"
	}
	revid := h.getQuery("rev")
	if revid == "" {
		var err error
		revid, err = h.getEtag("If-Match")
		if err != nil {
			return err
		}
	}
	attachmentData, err := h.readBody()
	if err != nil {
		return err
	}

	body, err := h.collection.Get1xRevBody(h.ctx(), docid, revid, false, nil)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			// couchdb creates empty body on attachment PUT
			// for non-existent doc id
			body = db.Body{db.BodyRev: revid}
		} else if err != nil {
			return err
		}
	} else if body != nil {
		if revid == "" {
			// If a revid is not specified and an active revision was found,
			// return a conflict now, rather than letting db.Put do it further down...
			return base.HTTPErrorf(http.StatusConflict, "Cannot modify attachments without a specific rev ID")
		}
	}

	// find attachment (if it existed)
	attachments := db.GetBodyAttachments(body)
	if attachments == nil {
		attachments = make(map[string]interface{})
	}

	// create new attachment
	attachment := make(map[string]interface{})
	attachment["data"] = attachmentData
	attachment["content_type"] = attachmentContentType

	// attach it
	attachments[attachmentName] = attachment
	body[db.BodyAttachments] = attachments

	newRev, cv, _, err := h.collection.Put(h.ctx(), docid, body)
	if err != nil {
		return err
	}
	h.setEtag(newRev)

	h.writeRawJSONStatus(http.StatusCreated, []byte(fmt.Sprintf(`{"id":`+base.ConvertToJSONString(docid)+`,"ok":true,"rev":"`+newRev+`", "current_version": {"source_id":"`+cv.SourceID+`", "version": %d}}`, cv.Version)))
	return nil
}

func (h *handler) handleDeleteAttachment() error {
	if h.isReadOnlyGuest() {
		return base.HTTPErrorf(http.StatusForbidden, auth.GuestUserReadOnly)
	}

	docid := h.PathVar("docid")
	attachmentName := h.PathVar("attach")
	revid := h.getQuery("rev")
	if revid == "" {
		var err error
		revid, err = h.getEtag("If-Match")
		if err != nil {
			return err
		}
	}

	body, err := h.collection.Get1xRevBody(h.ctx(), docid, revid, false, nil)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			// Check here if error is relating to incorrect revid, if so return 409 code else return 404 code
			if strings.Contains(err.Error(), "404 missing") {
				return base.HTTPErrorf(http.StatusConflict, "Incorrect revision ID specified")
			}
			// Need to return an error if a document is not found
			return base.HTTPErrorf(http.StatusNotFound, "Document specified is not found")
		} else if err != nil {
			return err
		}
	} else if body != nil {
		if revid == "" {
			// If a revid is not specified and an active revision was found,
			// return a conflict now, rather than letting db.Put do it further down...
			return base.HTTPErrorf(http.StatusConflict, "Cannot modify attachments without a specific rev ID")
		}
	}

	// get document attachments and check if attachment exists
	attachments := db.GetBodyAttachments(body)
	if _, ok := attachments[attachmentName]; !ok {
		return base.HTTPErrorf(http.StatusNotFound, "Attachment %s is not found", attachmentName)
	}
	// delete specified attachment from the map
	delete(attachments, attachmentName)
	body[db.BodyAttachments] = attachments

	newRev, _, _, err := h.collection.Put(h.ctx(), docid, body)
	if err != nil {
		return err
	}
	h.setEtag(newRev)

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"id":`+base.ConvertToJSONString(docid)+`,"ok":true,"rev":"`+newRev+`"}`))

	return nil
}

// HTTP handler for a PUT of a document
func (h *handler) handlePutDoc() error {

	if h.isReadOnlyGuest() {
		return base.HTTPErrorf(http.StatusForbidden, auth.GuestUserReadOnly)
	}

	startTime := time.Now()
	defer func() {
		h.db.DbStats.CBLReplicationPush().WriteProcessingTime.Add(time.Since(startTime).Nanoseconds())
	}()

	docid := h.PathVar("docid")

	roundTrip := h.getBoolQuery("roundtrip")

	if replicator2, _ := h.getOptBoolQuery("replicator2", false); replicator2 {
		return h.handlePutDocReplicator2(docid, roundTrip)
	}

	body, err := h.readDocument()
	if err != nil {
		return err
	}

	if body == nil {
		return base.ErrEmptyDocument
	}

	if bodyDocID, ok := body[db.BodyId].(string); ok && bodyDocID != docid {
		return base.HTTPErrorf(http.StatusBadRequest, "The document ID provided in the body does not match the document ID in the path")
	}

	var newRev string
	var doc *db.Document
	var cv db.SourceAndVersion

	if h.getQuery("new_edits") != "false" {
		// Regular PUT:
		bodyRev := body[db.BodyRev]
		if oldRev := h.getQuery("rev"); oldRev != "" {
			body[db.BodyRev] = oldRev
		} else if ifMatch, _ := h.getEtag("If-Match"); ifMatch != "" {
			body[db.BodyRev] = ifMatch
		}
		if bodyRev != nil && bodyRev != body[db.BodyRev] {
			return base.HTTPErrorf(http.StatusBadRequest, "Revision IDs provided do not match")
		}

		newRev, cv, doc, err = h.collection.Put(h.ctx(), docid, body)
		if err != nil {
			return err
		}
		h.setEtag(newRev)
	} else {
		// Replicator-style PUT with new_edits=false:
		revisions := db.ParseRevisions(h.ctx(), body)
		if revisions == nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Bad _revisions")
		}
		doc, newRev, err = h.collection.PutExistingRevWithBody(h.ctx(), docid, body, revisions, false, db.ExistingVersionWithUpdateToHLV)
		if err != nil {
			return err
		}
	}

	if doc != nil && roundTrip {
		if err := h.collection.WaitForSequenceNotSkipped(h.ctx(), doc.Sequence); err != nil {
			return err
		}
	}

	h.writeRawJSONStatus(http.StatusCreated, []byte(fmt.Sprintf(`{"id":`+base.ConvertToJSONString(docid)+`,"ok":true,"rev":"`+newRev+`", "current_version": {"source_id":"`+cv.SourceID+`", "version": %d}}`, cv.Version)))
	return nil
}

func (h *handler) handlePutDocReplicator2(docid string, roundTrip bool) (err error) {
	if !base.IsEnterpriseEdition() {
		return base.HTTPErrorf(http.StatusNotImplemented, "replicator2 endpoints are only supported in EE")
	}
	if h.isReadOnlyGuest() {
		return base.HTTPErrorf(http.StatusForbidden, auth.GuestUserReadOnly)
	}

	bodyBytes, err := h.readBody()
	if err != nil {
		return err
	}
	if bodyBytes == nil || len(bodyBytes) == 0 {
		return base.ErrEmptyDocument
	}

	newDoc := &db.Document{
		ID: docid,
	}
	newDoc.UpdateBodyBytes(bodyBytes)

	var parentRev string
	if oldRev := h.getQuery("rev"); oldRev != "" {
		parentRev = oldRev
	} else if ifMatch, _ := h.getEtag("If-Match"); ifMatch != "" {
		parentRev = ifMatch
	}

	generation, _ := db.ParseRevID(h.ctx(), parentRev)
	generation++

	deleted, _ := h.getOptBoolQuery("deleted", false)
	newDoc.Deleted = deleted

	newDoc.RevID = db.CreateRevIDWithBytes(generation, parentRev, bodyBytes)
	history := []string{newDoc.RevID}

	if parentRev != "" {
		history = append(history, parentRev)
	}

	// Handle and pull out expiry
	if bytes.Contains(bodyBytes, []byte(db.BodyExpiry)) {
		body := newDoc.Body(h.ctx())
		expiry, err := body.ExtractExpiry()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
		}
		newDoc.DocExpiry = expiry
		newDoc.UpdateBody(body)
	}

	// Pull out attachments
	if bytes.Contains(bodyBytes, []byte(db.BodyAttachments)) {
		body := newDoc.Body(h.ctx())

		newDoc.DocAttachments = db.GetBodyAttachments(body)
		delete(body, db.BodyAttachments)
		newDoc.UpdateBody(body)
	}

	doc, rev, err := h.collection.PutExistingRev(h.ctx(), newDoc, history, true, false, nil, db.ExistingVersionWithUpdateToHLV)

	if err != nil {
		return err
	}

	if doc != nil && roundTrip {
		if err := h.collection.WaitForSequenceNotSkipped(h.ctx(), doc.Sequence); err != nil {
			return err
		}
	}

	h.writeRawJSONStatus(http.StatusCreated, []byte(`{"id":`+base.ConvertToJSONString(docid)+`,"ok":true,"rev":"`+rev+`"}`))
	return nil
}

// HTTP handler for a POST to a database (creating a document)
func (h *handler) handlePostDoc() error {
	if h.isReadOnlyGuest() {
		return base.HTTPErrorf(http.StatusForbidden, auth.GuestUserReadOnly)
	}

	roundTrip := h.getBoolQuery("roundtrip")
	body, err := h.readDocument()
	if err != nil {
		return err
	}

	docid, newRev, doc, err := h.collection.Post(h.ctx(), body)
	if err != nil {
		return err
	}

	if doc != nil && roundTrip {
		err := h.collection.WaitForSequenceNotSkipped(h.ctx(), doc.Sequence)
		if err != nil {
			return err
		}
	}

	h.setHeader("Location", docid)
	h.setEtag(newRev)
	h.writeRawJSON([]byte(`{"id":"` + docid + `","ok":true,"rev":"` + newRev + `"}`))
	return nil
}

// HTTP handler for a DELETE of a document
func (h *handler) handleDeleteDoc() error {
	docid := h.PathVar("docid")
	revid := h.getQuery("rev")
	if revid == "" {
		var err error
		revid, err = h.getEtag("If-Match")
		if err != nil {
			return err
		}
	}
	var cv db.SourceAndVersion
	newRev, cv, err := h.collection.DeleteDoc(h.ctx(), docid, revid)
	if err == nil {
		h.writeRawJSONStatus(http.StatusOK, []byte(fmt.Sprintf(`{"id":`+base.ConvertToJSONString(docid)+`,"ok":true,"rev":"`+newRev+`", "current_version": {"source_id":"`+cv.SourceID+`", "version": %d}}`, cv.Version)))
	}
	return err
}

// ////// LOCAL DOCS:

// HTTP handler for a GET of a _local document
func (h *handler) handleGetLocalDoc() error {
	docid := h.PathVar("docid")
	value, err := h.collection.GetSpecial(db.DocTypeLocal, docid)
	if err != nil {
		return err
	}
	if value == nil {
		return kNotFoundError
	}
	value[db.BodyId] = "_local/" + docid
	h.writeJSON(value)
	return nil
}

// HTTP handler for a PUT of a _local document
func (h *handler) handlePutLocalDoc() error {
	docid := h.PathVar("docid")
	body, err := h.readJSON()
	if err == nil {
		var revid string
		revid, err = h.collection.PutSpecial(db.DocTypeLocal, docid, body)
		if err == nil {
			h.writeRawJSONStatus(http.StatusCreated, []byte(`{"id":`+base.ConvertToJSONString("_local/"+docid)+`,"ok":true,"rev":"`+revid+`"}`))
		}
	}
	return err
}

// HTTP handler for a DELETE of a _local document
func (h *handler) handleDelLocalDoc() error {
	docid := h.PathVar("docid")
	return h.collection.DeleteSpecial(db.DocTypeLocal, docid, h.getQuery("rev"))
}

// helper for read only check
func (h *handler) isReadOnlyGuest() bool {
	if h.db.IsGuestReadOnly() && h.db.User() != nil && h.db.User().Name() == "" {
		return true
	}
	return false
}

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
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// HTTP handler for a GET of a document
func (h *handler) handleGetDoc() error {
	docid := h.PathVar("docid")
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
				return base.HTTPErrorf(http.StatusBadRequest, "bad atts_since")
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

		hasBodies := (attachmentsSince != nil && value["_attachments"] != nil)
		if h.requestAccepts("multipart/") && (hasBodies || !h.requestAccepts("application/json")) {
			canCompress := strings.Contains(h.rq.Header.Get("X-Accept-Part-Encoding"), "gzip")
			return h.writeMultipart("related", func(writer *multipart.Writer) error {
				h.db.WriteMultipartDocument(value, writer, canCompress)
				return nil
			})
		} else {
			h.writeJSON(value)
		}

	} else if openRevs == "all" {
		// open_revs=all:
		return base.HTTPErrorf(http.StatusNotImplemented, "open_revs=all unimplemented") // TODO

	} else {
		// open_revs=["id1", "id2", ...]
		attachmentsSince = []string{}
		var revids []string
		err := json.Unmarshal([]byte(openRevs), &revids)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "bad open_revs")
		}

		err = h.writeMultipart("mixed", func(writer *multipart.Writer) error {
			for _, revid := range revids {
				revBody, err := h.db.GetRev(docid, revid, includeRevs, attachmentsSince)
				if err != nil {
					revBody = db.Body{"missing": revid} //TODO: More specific error
				}
				h.db.WriteRevisionAsPart(revBody, err != nil, false, writer)
			}
			return nil
		})
		return err
	}
	return nil
}

// HTTP handler for a GET of a specific doc attachment
func (h *handler) handleGetAttachment() error {
	docid := h.PathVar("docid")
	attachmentName := h.PathVar("attach")
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
		return base.HTTPErrorf(http.StatusNotFound, "missing attachment %s", attachmentName)
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

// HTTP handler for a PUT of an attachment
func (h *handler) handlePutAttachment() error {
	docid := h.PathVar("docid")
	attachmentName := h.PathVar("attach")
	attachmentContentType := h.rq.Header.Get("Content-Type")
	if attachmentContentType == "" {
		attachmentContentType = "application/octet-stream"
	}
	revid := h.getQuery("rev")
	if revid == "" {
		revid = h.rq.Header.Get("If-Match")
	}
	attachmentData, err := h.readBody()
	if err != nil {
		return err
	}

	body, err := h.db.GetRev(docid, revid, false, nil)
	if err != nil && base.IsDocNotFoundError(err) {
		// couchdb creates empty body on attachment PUT
		// for non-existant doc id
		body = db.Body{}
		body["_rev"] = revid
	} else if err != nil {
		return err
	} else if body != nil {
		body["_rev"] = revid
	}

	// find attachment (if it existed)
	attachments := db.BodyAttachments(body)
	if attachments == nil {
		attachments = make(map[string]interface{})
	}

	// create new attachment
	attachment := make(map[string]interface{})
	attachment["data"] = attachmentData
	attachment["content_type"] = attachmentContentType

	//attach it
	attachments[attachmentName] = attachment
	body["_attachments"] = attachments

	newRev, err := h.db.Put(docid, body)
	if err != nil {
		return err
	}
	h.setHeader("Etag", newRev)

	h.writeJSONStatus(http.StatusCreated, db.Body{"ok": true, "id": docid, "rev": newRev})
	return nil
}

// HTTP handler for a PUT of a document
func (h *handler) handlePutDoc() error {
	docid := h.PathVar("docid")
	body, err := h.readDocument()
	if err != nil {
		return err
	}
	var newRev string

	if h.getQuery("new_edits") != "false" {
		// Regular PUT:
		if oldRev := h.getQuery("rev"); oldRev != "" {
			body["_rev"] = oldRev
		} else if ifMatch := h.rq.Header.Get("If-Match"); ifMatch != "" {
			body["_rev"] = ifMatch
		}
		newRev, err = h.db.Put(docid, body)
		if err != nil {
			return err
		}
		h.setHeader("Etag", newRev)
	} else {
		// Replicator-style PUT with new_edits=false:
		revisions := db.ParseRevisions(body)
		if revisions == nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Bad _revisions")
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
	docid := h.PathVar("docid")
	revid := h.getQuery("rev")
	if revid == "" {
		revid = h.rq.Header.Get("If-Match")
	}
	newRev, err := h.db.DeleteDoc(docid, revid)
	if err == nil {
		h.writeJSON(db.Body{"ok": true, "id": docid, "rev": newRev})
	}
	return err
}

//////// LOCAL DOCS:

// HTTP handler for a GET of a _local document
func (h *handler) handleGetLocalDoc() error {
	docid := h.PathVar("docid")
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
	docid := h.PathVar("docid")
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
	docid := h.PathVar("docid")
	return h.db.DeleteSpecial("local", docid, h.getQuery("rev"))
}

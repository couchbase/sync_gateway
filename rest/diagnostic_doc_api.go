/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type SyncFnDryRun struct {
	Channels  base.Set           `json:"channels"`
	Access    channels.AccessMap `json:"access"`
	Roles     channels.AccessMap `json:"roles"`
	Exception string             `json:"exception"`
	Expiry    *uint32            `json:"expiry,omitempty"`
}

type ImportFilterDryRun struct {
	ShouldImport bool   `json:"shouldImport"`
	Error        string `json:"error"`
}

type SyncFnDryRunPayload struct {
	Function string  `json:"sync_function"`
	Doc      db.Body `json:"doc,omitempty"`
}

func populateDocChannelInfo(doc db.Document) map[string][]auth.GrantHistorySequencePair {
	resp := make(map[string][]auth.GrantHistorySequencePair, len(doc.Channels))

	for _, chanSetInfo := range doc.SyncData.ChannelSet {
		resp[chanSetInfo.Name] = append(resp[chanSetInfo.Name], auth.GrantHistorySequencePair{StartSeq: chanSetInfo.Start, EndSeq: chanSetInfo.End})
	}
	for _, hist := range doc.SyncData.ChannelSetHistory {
		resp[hist.Name] = append(resp[hist.Name], auth.GrantHistorySequencePair{StartSeq: hist.Start, EndSeq: hist.End, Compacted: hist.Compacted})
		continue
	}
	return resp
}

// HTTP handler for a GET of a document's channels and their sequence spans
func (h *handler) handleGetDocChannels() error {
	docid := h.PathVar("docid")

	doc, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalSync)
	if err != nil {
		return err
	}
	if doc == nil {
		return kNotFoundError
	}
	resp := populateDocChannelInfo(*doc)
	h.writeJSON(resp)
	return nil
}

// HTTP handler for running a document through the sync function and returning the results
// body only provided, the sync function will run with no oldDoc provided
// body and doc ID provided, the sync function will run using the current revision in the bucket as oldDoc
// docid only provided, the sync function will run using the current revision in the bucket as doc
// If docid is specified and the document does not exist in the bucket, it will return error
func (h *handler) handleSyncFnDryRun() error {
	docid := h.getQuery("doc_id")

	var syncDryRunPayload SyncFnDryRunPayload
	err := h.readJSONInto(&syncDryRunPayload)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error reading sync function payload: %v", err)
	}

	if syncDryRunPayload.Doc == nil && docid == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "no docid or document provided")
	}

	oldDoc := &db.Document{ID: docid}
	oldDoc.UpdateBody(syncDryRunPayload.Doc)
	if docid != "" {
		if docInbucket, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalAll); err == nil {
			oldDoc = docInbucket
			if len(syncDryRunPayload.Doc) == 0 {
				syncDryRunPayload.Doc = oldDoc.Body(h.ctx())
				oldDoc.UpdateBody(nil)
			}
		} else {
			return base.HTTPErrorf(http.StatusNotFound, "Error reading document: %v", err)
		}
	} else {
		oldDoc.UpdateBody(nil)
	}

	delete(syncDryRunPayload.Doc, db.BodyId)

	// Get the revision ID to match, and the new generation number:
	matchRev, _ := syncDryRunPayload.Doc[db.BodyRev].(string)
	generation, _ := db.ParseRevID(h.ctx(), matchRev)
	if generation < 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid revision ID")
	}
	generation++

	// Create newDoc which will be used to pass around Body
	newDoc := &db.Document{
		ID: docid,
	}
	// Pull attachments
	newDoc.SetAttachments(db.GetBodyAttachments(syncDryRunPayload.Doc))
	delete(syncDryRunPayload.Doc, db.BodyAttachments)
	delete(syncDryRunPayload.Doc, db.BodyRevisions)

	if _, ok := syncDryRunPayload.Doc[base.SyncPropertyName]; ok {
		return base.HTTPErrorf(http.StatusBadRequest, "document-top level property '_sync' is a reserved internal property")
	}

	db.StripInternalProperties(syncDryRunPayload.Doc)

	// We needed to keep _deleted around in the body until we generate rev ID, but now it can be removed
	_, isDeleted := syncDryRunPayload.Doc[db.BodyDeleted]
	if isDeleted {
		delete(syncDryRunPayload.Doc, db.BodyDeleted)
	}

	//update the newDoc body to be without any special properties
	newDoc.UpdateBody(syncDryRunPayload.Doc)

	rawDocBytes, err := newDoc.BodyBytes(h.ctx())
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error marshalling document: %v", err)
	}

	newRev := db.CreateRevIDWithBytes(generation, matchRev, rawDocBytes)
	newDoc.RevID = newRev

	output, err := h.collection.SyncFnDryrun(h.ctx(), newDoc, oldDoc, syncDryRunPayload.Function)
	if err != nil {
		var syncFnDryRunErr *base.SyncFnDryRunError
		if !errors.As(err, &syncFnDryRunErr) {
			return err
		}

		errMsg := syncFnDryRunErr.Error()
		resp := SyncFnDryRun{
			Exception: errMsg,
		}
		h.writeJSON(resp)
		return nil
	}
	errorMsg := ""
	if output.Rejection != nil {
		errorMsg = output.Rejection.Error()
	}

	resp := SyncFnDryRun{
		output.Channels,
		output.Access,
		output.Roles,
		errorMsg,
		output.Expiry,
	}
	h.writeJSON(resp)
	return nil
}

// HTTP handler for running a document through the import filter and returning the results
func (h *handler) handleImportFilterDryRun() error {
	docid := h.getQuery("doc_id")

	body, err := h.readDocument()
	if err != nil {
		if docid == "" {
			return fmt.Errorf("Error reading body: %s, no doc id provided for dry run", err)
		}
	}

	if docid != "" && body != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "doc body and doc id provided. Please provide either the body or a doc id for the import filter dry run")
	}
	shouldImport, err := h.collection.ImportFilterDryRun(h.ctx(), body, docid)
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}
	resp := ImportFilterDryRun{
		shouldImport,
		errorMsg,
	}
	h.writeJSON(resp)
	return nil
}

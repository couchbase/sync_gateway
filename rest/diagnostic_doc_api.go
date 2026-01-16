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
	"net/http"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type DryRunLogging struct {
	Errors []string `json:"errors"`
	Info   []string `json:"info"`
}

type SyncFnDryRun struct {
	Channels  base.Set           `json:"channels"`
	Access    channels.AccessMap `json:"access"`
	Roles     channels.AccessMap `json:"roles"`
	Exception string             `json:"exception,omitempty"`
	Expiry    *uint32            `json:"expiry,omitempty"`
	Logging   DryRunLogging      `json:"logging"`
}

type ImportFilterDryRun struct {
	ShouldImport bool          `json:"shouldImport"`
	Error        string        `json:"error"`
	Logging      DryRunLogging `json:"logging"`
}

type SyncFnDryRunMetaMap struct {
	Xattrs map[string]any `json:"xattrs"`
}
type SyncFnDryRunPayload struct {
	Function string              `json:"sync_function"`
	Doc      db.Body             `json:"doc,omitempty"`
	Meta     SyncFnDryRunMetaMap `json:"meta,omitempty"`
}

type ImportFilterDryRunPayload struct {
	Function string  `json:"import_filter"`
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
		return base.HTTPErrorf(http.StatusBadRequest, "no doc_id or document provided")
	}

	var userXattrs map[string]any
	// checking user defined metadata
	if syncDryRunPayload.Meta.Xattrs != nil {
		xattrs := syncDryRunPayload.Meta.Xattrs
		if len(xattrs) > 1 {
			return base.HTTPErrorf(http.StatusBadRequest, "Only one xattr key can be specified in meta")
		}
		userXattrKey := h.collection.UserXattrKey()
		if userXattrKey == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "no user xattr key configured for this database")
		}
		_, exists := xattrs[userXattrKey]
		if !exists {
			return base.HTTPErrorf(http.StatusBadRequest, "configured user xattr key %q not found in provided xattrs", userXattrKey)
		}
		userXattrs = make(map[string]any)
		userXattrs["xattrs"] = xattrs
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
	newDoc.DocAttachments = db.GetBodyAttachments(syncDryRunPayload.Doc)
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

	logErrors := make([]string, 0)
	logInfo := make([]string, 0)
	errorLogFn := func(s string) {
		logErrors = append(logErrors, s)
	}
	infoLogFn := func(s string) {
		logInfo = append(logInfo, s)
	}

	output, err := h.collection.SyncFnDryrun(h.ctx(), newDoc, oldDoc, userXattrs, syncDryRunPayload.Function, errorLogFn, infoLogFn)
	if err != nil {
		var syncFnDryRunErr *base.SyncFnDryRunError
		if !errors.As(err, &syncFnDryRunErr) {
			return err
		}

		errMsg := syncFnDryRunErr.Error()
		resp := SyncFnDryRun{
			Exception: errMsg,
			Logging:   DryRunLogging{Errors: logErrors, Info: logInfo},
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
		DryRunLogging{Errors: logErrors, Info: logInfo},
	}
	h.writeJSON(resp)
	return nil
}

// HTTP handler for running a document through the import filter and returning the results
func (h *handler) handleImportFilterDryRun() error {
	docid := h.getQuery("doc_id")

	var importFilterPayload ImportFilterDryRunPayload
	err := h.readJSONInto(&importFilterPayload)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error reading import filter payload: %v", err)
	}

	// Cannot pass both doc_id and body in the request body
	if len(importFilterPayload.Doc) > 0 && docid != "" {
		return base.HTTPErrorf(http.StatusBadRequest, "doc body and doc id provided. Please provide either the body or a doc id for the import filter dry run")
	}

	if len(importFilterPayload.Doc) == 0 && docid == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "no doc body and doc id provided. Please provide either the body or a doc id for the import filter dry run")
	}

	var doc db.Body
	if docid != "" {
		docInBucket, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalAll)
		if err != nil {
			return err
		}
		doc = docInBucket.Body(h.ctx())
	} else {
		doc = importFilterPayload.Doc
	}

	logErrors := make([]string, 0)
	logInfo := make([]string, 0)
	errorLogFn := func(s string) {
		logErrors = append(logErrors, s)
	}
	infoLogFn := func(s string) {
		logInfo = append(logInfo, s)
	}
	shouldImport, err := h.collection.ImportFilterDryRun(h.ctx(), doc, importFilterPayload.Function, errorLogFn, infoLogFn)
	errorMsg := ""
	if err != nil {
		var importFilterDryRunErr *base.ImportFilterDryRunError
		if !errors.As(err, &importFilterDryRunErr) {
			return err
		}
		errorMsg = err.Error()
	}
	resp := ImportFilterDryRun{
		shouldImport,
		errorMsg,
		DryRunLogging{Errors: logErrors, Info: logInfo},
	}
	h.writeJSON(resp)
	return nil
}

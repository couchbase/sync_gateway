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
	"cmp"
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
	Exception    string        `json:"exception"`
	Logging      DryRunLogging `json:"logging"`
}

type SyncFnDryRunMetaMap struct {
	Xattrs map[string]any `json:"xattrs"`
}
type SyncDryRunUserCtx struct {
	Name     string   `json:"name"`
	Roles    []string `json:"roles,omitempty"`
	Channels []string `json:"channels,omitempty"`
}
type SyncFnDryRunPayload struct {
	DocID    string              `json:"doc_id"`
	Function string              `json:"sync_function"`
	Doc      db.Body             `json:"doc,omitempty"`
	OldDoc   db.Body             `json:"oldDoc,omitempty"`
	Meta     SyncFnDryRunMetaMap `json:"meta,omitempty"`
	UserCtx  *SyncDryRunUserCtx  `json:"userCtx,omitempty"`
}

const defaultSyncDryRunDocID = "sync_dryrun"

type ImportFilterDryRunPayload struct {
	DocID    string  `json:"doc_id"`
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
//
// The sync function can be dry-run with the following combinations of doc properties:
//   - If 'doc' only provided, the sync function will run with the provided doc (and no oldDoc)
//   - If 'doc' and 'oldDoc' provided, the sync function will run using the provided doc and oldDoc
//   - If 'doc_id' only provided, the sync function will run using the current revision in the bucket as doc
//   - If 'doc' and 'doc_id' provided, the sync function will run using the provided doc and the current revision in the bucket as oldDoc
func (h *handler) handleSyncFnDryRun() error {

	var syncDryRunPayload SyncFnDryRunPayload
	err := h.readJSONInto(&syncDryRunPayload)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error reading sync function payload: %v", err)
	}

	bucketDocID := syncDryRunPayload.DocID
	if syncDryRunPayload.Doc == nil && bucketDocID == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "must provide either doc_id or doc")
	}
	if syncDryRunPayload.OldDoc != nil && bucketDocID != "" {
		return base.HTTPErrorf(http.StatusBadRequest, "cannot specify doc_id with a provided oldDoc")
	}

	var userXattrs map[string]any
	// checking user defined metadata
	if syncDryRunPayload.Meta.Xattrs != nil {
		xattrs := syncDryRunPayload.Meta.Xattrs
		if len(xattrs) > 1 {
			return base.HTTPErrorf(http.StatusBadRequest, "only one xattr key can be specified in meta")
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

	var userCtx map[string]any
	if syncDryRunPayload.UserCtx != nil {
		userCtx = make(map[string]any)
		if syncDryRunPayload.UserCtx.Name == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "no user name provided")
		}
		userCtx["name"] = syncDryRunPayload.UserCtx.Name
		userCtx["channels"] = syncDryRunPayload.UserCtx.Channels
		/*
			The user role defined in the User interface is of type ch.TimedSet .
			TimedSet is basically a map of string and the sequence of when the
			role was added to the DB. The sequence is never really used in
			the definition of requireRole. requireRole however needs roles to be
			of the same structure. The below code assigns a default sequence of 1
			to all the roles passed by the user in the userCtx object.
		*/
		rolesMap := make(map[string]int)
		for _, role := range syncDryRunPayload.UserCtx.Roles {
			rolesMap[role] = 1
		}
		userCtx["roles"] = rolesMap
	}

	inlineDocID, _ := syncDryRunPayload.Doc[db.BodyId].(string)
	docID := cmp.Or(bucketDocID, inlineDocID, defaultSyncDryRunDocID)

	oldDoc := &db.Document{ID: docID}
	// Read the document from the bucket
	if bucketDocID != "" {
		bucketDoc, err := h.collection.GetDocument(h.ctx(), bucketDocID, db.DocUnmarshalAll)
		if err != nil {
			return err
		}
		if len(syncDryRunPayload.Doc) == 0 {
			// use bucket doc as current doc value
			syncDryRunPayload.Doc = bucketDoc.Body(h.ctx())
			// set oldDoc for any xattrs that may be present for use in `meta`, but nil the body
			oldDoc = bucketDoc
			oldDoc.UpdateBody(nil)
		} else {
			// use bucket doc as oldDoc value
			oldDoc = bucketDoc
		}
	} else if syncDryRunPayload.OldDoc != nil {
		// use inline oldDoc value
		oldDoc.UpdateBody(syncDryRunPayload.OldDoc)
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
		ID: docID,
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

	logErrors := make([]string, 0)
	logInfo := make([]string, 0)
	errorLogFn := func(s string) {
		logErrors = append(logErrors, s)
	}
	infoLogFn := func(s string) {
		logInfo = append(logInfo, s)
	}

	output, err := h.collection.SyncFnDryRun(h.ctx(), newDoc, oldDoc, userXattrs, userCtx, syncDryRunPayload.Function, errorLogFn, infoLogFn)
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

	var importFilterPayload ImportFilterDryRunPayload
	err := h.readJSONInto(&importFilterPayload)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error reading import filter payload: %v", err)
	}

	docid := importFilterPayload.DocID
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

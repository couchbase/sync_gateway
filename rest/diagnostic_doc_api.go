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
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type SyncFnDryRun struct {
	Channels  base.Set           `json:"Channels"`
	Access    channels.AccessMap `json:"Access"`
	Roles     channels.AccessMap `json:"Roles"`
	Exception string             `json:"Exception"`
	Expiry    *uint32            `json:"Expiry,omitempty"`
}

type ImportFilterDryRun struct {
	ShouldImport bool   `json:"shouldImport"`
	Error        string `json:"error"`
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
	resp := make(map[string][]auth.GrantHistorySequencePair, len(doc.Channels))

	for _, chanSetInfo := range doc.SyncData.ChannelSet {
		resp[chanSetInfo.Name] = append(resp[chanSetInfo.Name], auth.GrantHistorySequencePair{StartSeq: chanSetInfo.Start, EndSeq: chanSetInfo.End})
	}
	for _, hist := range doc.SyncData.ChannelSetHistory {
		resp[hist.Name] = append(resp[hist.Name], auth.GrantHistorySequencePair{StartSeq: hist.Start, EndSeq: hist.End, Compacted: hist.Compacted})
		continue
	}

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

	body, err := h.readDocument()
	if err != nil {
		if docid == "" {
			return fmt.Errorf("no doc id provided for dry run and error reading body: %s", err)
		}
	}

	output, err, syncFnErr := h.collection.SyncFnDryrun(h.ctx(), body, docid)
	if err != nil {
		return err
	}
	if syncFnErr != nil {
		resp := SyncFnDryRun{
			Exception: syncFnErr.Error(),
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

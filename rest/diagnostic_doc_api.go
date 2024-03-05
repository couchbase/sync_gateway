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
}

type ImportFilterDryRun struct {
	ShouldImport bool   `json:"shouldImport"`
	Error        string `json:"error"`
}

// HTTP handler for a GET of a document
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

// HTTP handler for a GET of a document
func (h *handler) handleSyncFnDryRun() error {
	docid := h.PathVar("docid")
	body, err := h.readDocument()
	if err != nil {
		return err
	}
	_, _, channelSet, access, roles, err := h.collection.SyncFnDryrun(h.ctx(), body, docid)
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}
	resp := SyncFnDryRun{
		channelSet,
		access,
		roles,
		errorMsg,
	}

	h.writeJSON(resp)
	return nil
}

// HTTP handler for a GET of a document
func (h *handler) handleImportFilterDryRun() error {
	body, err := h.readDocument()
	if err != nil {
		return err
	}
	shouldImport, err := h.collection.ImportFilterDryRun(h.ctx(), body)
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

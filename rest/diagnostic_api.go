//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
)

type allChannels struct {
	Channels map[string]map[string]auth.GrantHistory `json:"all_channels,omitempty"`
}

func (h *handler) handleGetAllChannels() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator(h.ctx()).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if err != nil {
		return err
	}
	if user == nil {
		return kNotFoundError
	}

	resp := make(map[string]map[string]auth.GrantHistory)

	// handles deleted collections, default/ single named collection
	for _, dsName := range h.db.DataStoreNames() {
		keyspace := dsName.ScopeName() + "." + dsName.CollectionName()

		resp[keyspace] = make(map[string]auth.GrantHistory)
		channels := user.CollectionChannels(dsName.ScopeName(), dsName.CollectionName())
		chanHistory := user.CollectionChannelHistory(dsName.ScopeName(), dsName.CollectionName())
		for chanName, chanEntry := range channels {
			resp[keyspace][chanName] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.Sequence, EndSeq: 0}}}
		}
		for chanName, chanEntry := range chanHistory {
			chanHistoryEntry := auth.GrantHistory{Entries: chanEntry.Entries, UpdatedAt: chanEntry.UpdatedAt}
			// if channel is also in history, append current entry to history entries
			if _, chanCurrentlyAssigned := resp[chanName]; chanCurrentlyAssigned {
				var newEntries []auth.GrantHistorySequencePair
				copy(newEntries, chanHistory[chanName].Entries)
				newEntries = append(newEntries, chanEntry.Entries...)
				chanHistoryEntry.Entries = newEntries
			}
			resp[keyspace][chanName] = chanHistoryEntry
		}

	}
	channels := allChannels{Channels: resp}

	bytes, err := base.JSONMarshal(channels)
	h.writeRawJSON(bytes)
	return err
}

//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	channels "github.com/couchbase/sync_gateway/channels"
	"github.com/gorilla/mux"
)

type allChannels struct {
	Channels map[string]map[string]channelHistory `json:"all_channels,omitempty"`
}
type channelHistory struct {
	Entries []auth.GrantHistorySequencePair `json:"entries"` // Entry for a specific grant period
}

func (h *handler) handleGetAllChannels() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator(h.ctx()).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if err != nil {
		return fmt.Errorf("could not get user %s: %w", user.Name(), err)
	}
	if user == nil {
		return kNotFoundError
	}

	resp := make(map[string]map[string]channelHistory)

	// handles deleted collections, default/ single named collection
	for _, dsName := range h.db.DataStoreNames() {
		keyspace := dsName.ScopeName() + "." + dsName.CollectionName()

		currentChannels := user.InheritedCollectionChannels(dsName.ScopeName(), dsName.CollectionName())
		chanHistory := user.CollectionChannelHistory(dsName.ScopeName(), dsName.CollectionName())
		// If no channels aside from public and no channels in history, don't make a key for this keyspace
		if len(currentChannels) == 1 && len(chanHistory) == 0 {
			continue
		}
		resp[keyspace] = make(map[string]channelHistory)
		for chanName, chanEntry := range currentChannels {
			if chanName == channels.DocumentStarChannel {
				continue
			}
			resp[keyspace][chanName] = channelHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.Sequence, EndSeq: 0}}}
		}
		for chanName, chanEntry := range chanHistory {
			chanHistoryEntry := channelHistory{Entries: chanEntry.Entries}
			// if channel is also in history, append current entry to history entries
			if _, chanCurrentlyAssigned := resp[keyspace][chanName]; chanCurrentlyAssigned {
				var newEntries []auth.GrantHistorySequencePair
				newEntries = append(chanEntry.Entries, resp[keyspace][chanName].Entries...)
				chanHistoryEntry.Entries = newEntries
			}
			resp[keyspace][chanName] = chanHistoryEntry
		}

	}
	allChannels := allChannels{Channels: resp}

	bytes, err := base.JSONMarshal(allChannels)
	if err != nil {
		return err
	}
	h.writeRawJSON(bytes)
	return err
}

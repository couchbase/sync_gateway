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
	"net/http"
	"strings"

	"golang.org/x/exp/maps"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/gorilla/mux"
)

type allChannels struct {
	Channels map[string]map[string]channelHistory `json:"all_channels,omitempty"`
}
type channelHistory struct {
	Entries []auth.GrantHistorySequencePair `json:"entries"` // Entry for a specific grant period
}

func (h *handler) getAllUserChannelsResponse(user auth.User) (map[string]map[string]channelHistory, error) {
	resp := make(map[string]map[string]channelHistory)

	_, err := h.db.Authenticator(h.ctx()).RebuildChannels(user)
	if err != nil {
		return nil, err
	}
	err = h.db.Authenticator(h.ctx()).RebuildRoles(user)
	if err != nil {
		return nil, err
	}

	for _, dsName := range h.db.DataStoreNames() {
		keyspace := dsName.ScopeName() + "." + dsName.CollectionName()
		currentChannels, err := user.InheritedCollectionChannels(dsName.ScopeName(), dsName.CollectionName())
		if err != nil {
			return nil, fmt.Errorf("error retrieving inherited user channels: %w", err)
		}
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

	for roleName, roleVbSeq := range user.RoleNames() {
		role, err := h.db.Authenticator(h.ctx()).GetRoleIncDeleted(roleName)
		if err != nil {
			return nil, fmt.Errorf("error getting role: %w", err)
		}
		if role == nil {
			continue
		}

		// default keyspace chan history
		defaultKs := "_default._default"
		if _, ok := resp[defaultKs]; !ok && len(maps.Keys(role.ChannelHistory())) != 0 {
			resp[defaultKs] = make(map[string]channelHistory)
		}
		for chanName, chanEntry := range role.ChannelHistory() {
			entries := getCurrentRoleChanEntryOverlap(roleVbSeq.Sequence, chanEntry.Entries)
			chanHistoryEntry := channelHistory{Entries: entries}
			resp[defaultKs][chanName] = chanHistoryEntry
		}

		collAccess := role.GetCollectionsAccess()
		for scopeName, scope := range collAccess {
			for collName, coll := range scope {
				keyspace := scopeName + "." + collName
				if _, ok := resp[keyspace]; !ok && len(maps.Keys(coll.ChannelHistory_)) != 0 {
					resp[keyspace] = make(map[string]channelHistory)
				}
				for chanName, chanEntry := range coll.ChannelHistory_ {
					entries := getCurrentRoleChanEntryOverlap(roleVbSeq.Sequence, chanEntry.Entries)
					chanHistoryEntry := channelHistory{Entries: entries}
					resp[keyspace][chanName] = chanHistoryEntry
				}
			}
		}
	}

	for roleName, roleHistory := range user.RoleHistory() {
		role, err := h.db.Authenticator(h.ctx()).GetRoleIncDeleted(roleName)
		if err != nil {
			return nil, fmt.Errorf("error getting role: %w", err)
		}
		if role == nil {
			continue
		}
		if !role.IsDeleted() {
			continue
		}

		defaultKs := "_default._default"
		if _, ok := resp[defaultKs]; !ok && len(maps.Keys(role.ChannelHistory())) != 0 {
			resp[defaultKs] = make(map[string]channelHistory)
		}
		for chanName, chanEntry := range role.ChannelHistory() {
			entries := getHistoricRoleChanEntryOverlap(roleHistory.Entries, chanEntry.Entries)
			chanHistoryEntry := channelHistory{Entries: entries}
			resp[defaultKs][chanName] = chanHistoryEntry
		}

		collAccess := role.GetCollectionsAccess()
		for scopeName, scope := range collAccess {
			for collName, coll := range scope {
				keyspace := scopeName + "." + collName
				for chanName, chanEntry := range coll.ChannelHistory_ {
					entries := getHistoricRoleChanEntryOverlap(roleHistory.Entries, chanEntry.Entries)
					chanHistoryEntry := channelHistory{Entries: entries}
					resp[keyspace][chanName] = chanHistoryEntry
				}
			}
		}
	}

	return resp, nil
}

// Only used for deleted role, logic is for channels in channel_history and roles in role_history
func getCurrentRoleChanEntryOverlap(roleVbSeq uint64, chanEntries []auth.GrantHistorySequencePair) []auth.GrantHistorySequencePair {
	var overlapEntries []auth.GrantHistorySequencePair
	var entry auth.GrantHistorySequencePair
	for _, chanEntry := range chanEntries {
		// default
		entry = auth.GrantHistorySequencePair{StartSeq: roleVbSeq, EndSeq: chanEntry.EndSeq}
		// role was assigned to user after chan was removed from role, no entry
		if roleVbSeq > chanEntry.EndSeq {
			continue
			// role was assigned to user before chan was assigned to role
		} else if roleVbSeq < chanEntry.StartSeq {
			entry = auth.GrantHistorySequencePair{StartSeq: chanEntry.StartSeq, EndSeq: chanEntry.EndSeq}
		}
		overlapEntries = append(overlapEntries, entry)
	}

	return overlapEntries
}

// Only used for deleted role, logic is for channels in channel_history and roles in role_history
func getHistoricRoleChanEntryOverlap(roleEntries []auth.GrantHistorySequencePair, chanEntries []auth.GrantHistorySequencePair) []auth.GrantHistorySequencePair {
	var overlapEntries []auth.GrantHistorySequencePair
	var entry auth.GrantHistorySequencePair
	for _, roleEntry := range roleEntries {
		for _, chanEntry := range chanEntries {
			// default
			entry = auth.GrantHistorySequencePair{StartSeq: chanEntry.StartSeq, EndSeq: roleEntry.EndSeq}
			// role was assigned to user after chan was removed from role, no entry
			if roleEntry.StartSeq > chanEntry.EndSeq {
				continue
				// role was removed from user before chan was assigned to role, no entry
			} else if roleEntry.EndSeq < chanEntry.StartSeq {
				continue
				// chan was removed from role after role was removed from user
			} else if chanEntry.EndSeq > roleEntry.EndSeq {
				entry = auth.GrantHistorySequencePair{StartSeq: chanEntry.StartSeq, EndSeq: roleEntry.EndSeq}
				// role was assigned to user after chan was assigned to role
			} else if roleEntry.StartSeq > chanEntry.StartSeq {
				entry = auth.GrantHistorySequencePair{StartSeq: roleEntry.StartSeq, EndSeq: chanEntry.EndSeq}
			}
			overlapEntries = append(overlapEntries, entry)
		}
	}

	return overlapEntries
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

	resp, err := h.getAllUserChannelsResponse(user)
	if err != nil {
		return err
	}
	allChannels := allChannels{Channels: resp}

	bytes, err := base.JSONMarshal(allChannels)
	if err != nil {
		return err
	}
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) handleGetUserDocAccessSpan() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator(h.ctx()).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if err != nil {
		return fmt.Errorf("could not get user %s: %w", user.Name(), err)
	}
	if user == nil {
		return kNotFoundError
	}
	ks := h.PathVar("keyspace")

	docids := h.getQuery("docids")
	docidsList := strings.Split(docids, ",")
	var docList []*db.Document

	var scope, coll string
	parts := strings.Split(ks, base.ScopeCollectionSeparator)
	switch len(parts) {
	case 1:
		// if default collection, some calls using {{.keyspace}} will call /db/
		scope = "_default"
		coll = "_default"
	case 3:
		scope = parts[1]
		coll = parts[2]
	default:
		return fmt.Errorf("unable to get scope and collection from keyspace")
	}

	if err != nil {
		return err
	}

	keyspace := scope + "." + coll

	for _, docID := range docidsList {
		if docID == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "empty doc id given in request")
		}
		doc, err := h.collection.GetDocument(h.ctx(), docID, db.DocUnmarshalSync)
		if doc == nil {
			return base.HTTPErrorf(http.StatusNotFound, "doc %s not found", docID)
		}
		if err != nil {
			return err
		}
		docList = append(docList, doc)
	}

	resp := make(map[string]map[string]channelHistory)
	for _, doc := range docList {
		docChannelToSeqs := populateDocChannelInfo(*doc)
		userChannels, err := h.getAllUserChannelsResponse(user)
		if err != nil {
			return err
		}

		// Only keep current keyspace
		channelsToSeqs := userChannels[keyspace]
		// loop through all chans in keyspace
		docResponse := make(map[string]channelHistory)
		for channelName, userChanHistory := range channelsToSeqs {
			if docSeqs, ok := docChannelToSeqs[channelName]; ok {
				var chanIntersection []auth.GrantHistorySequencePair

				for _, userSeq := range userChanHistory.Entries {
					for _, docSeq := range docSeqs {

						startSeq := max(userSeq.StartSeq, docSeq.StartSeq)
						endSeq := min(userSeq.EndSeq, docSeq.EndSeq)

						if userSeq.EndSeq == 0 {
							endSeq = docSeq.EndSeq
						} else if docSeq.EndSeq == 0 {
							endSeq = userSeq.EndSeq
						}

						if endSeq == 0 || startSeq < endSeq {
							chanIntersection = append(chanIntersection, auth.GrantHistorySequencePair{
								StartSeq: startSeq,
								EndSeq:   endSeq,
							})
						}
					}
				}

				if len(chanIntersection) > 0 {
					docResponse[channelName] = channelHistory{
						Entries: chanIntersection,
					}
				}
			}
		}

		if len(docResponse) > 0 {
			resp[doc.ID] = docResponse
		}
	}
	bytes, err := base.JSONMarshal(resp)
	if err != nil {
		return err
	}
	h.writeRawJSON(bytes)
	return err
}

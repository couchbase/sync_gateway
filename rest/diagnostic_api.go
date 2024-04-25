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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/gorilla/mux"
)

// each field in this response is a map of collection names to a map of channel names to grant history info
type getAllChannelsResponse struct {
	AdminGrants       map[string]map[string]auth.GrantHistory            `json:"admin_grants,omitempty"`
	DynamicGrants     map[string]map[string]auth.GrantHistory            `json:"dynamic_grants,omitempty"`
	JWTGrants         map[string]map[string]auth.GrantHistory            `json:"jwt_grants,omitempty"`
	AdminRoleGrants   map[string]map[string]map[string]auth.GrantHistory `json:"admin_role_grants,omitempty"`
	DynamicRoleGrants map[string]map[string]map[string]auth.GrantHistory `json:"dynamic_role_grants,omitempty"`
}

func newGetAllChannelsResponse() *getAllChannelsResponse {
	var resp getAllChannelsResponse
	resp.AdminGrants = make(map[string]map[string]auth.GrantHistory)
	resp.DynamicGrants = make(map[string]map[string]auth.GrantHistory)
	resp.DynamicRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory)
	resp.AdminRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory)
	return &resp
}

func (h *handler) handleGetAllChannels() error {
	h.assertAdminOnly()
	authenticator := h.db.Authenticator(h.ctx())
	username := mux.Vars(h.rq)["name"]
	user, err := authenticator.GetUser(internalUserName(username))
	if err != nil {
		return fmt.Errorf("Error getting user %s: %w", username, err)
	}
	if user == nil {
		return base.HTTPErrorf(http.StatusNotFound, "User not found %s", username)
	}
	resp := newGetAllChannelsResponse()

	// Rebuild roles
	if err := authenticator.RebuildRoles(user); err != nil {
		return fmt.Errorf("error rebuilding roles: %w", err)
	}

	for roleName, roleEntry := range user.RoleNames() {
		role, err := authenticator.GetRole(roleName)
		if err != nil {
			return fmt.Errorf("error getting role %s: %w", roleName, err)
		}
		// deleted role will return nil with no error
		if role == nil {
			continue
		}
		for _, dsName := range h.db.DataStoreNames() {
			keyspace := fmt.Sprintf("%s.%s", dsName.Scope, dsName.Collection)
			collectionChannels := role.CollectionChannels(dsName.Scope, dsName.Collection)
			channelHistory := role.CollectionChannelHistory(dsName.Scope, dsName.Collection)

			for channel, chanEntry := range collectionChannels {
				// loop over current role channels
				if channel == channels.DocumentStarChannel {
					continue
				}
				// if channel was assigned after user got the role, the correct sequence is in chanEntry
				sequence := roleEntry.VbSequence.Sequence
				if chanEntry.VbSequence.Sequence > sequence {
					sequence = chanEntry.VbSequence.Sequence
				}
				grantInfo := auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: sequence}}, Source: chanEntry.Source}
				resp.addRoleGrants(roleName, roleEntry.Source, keyspace, channel, grantInfo)
			}
			// loop over previous role channels
			for channel, chanHistory := range channelHistory {
				// if channel is in history for a sequence span before the user had access to the role, skip
				if chanHistory.Entries[len(chanHistory.Entries)-1].EndSeq < roleEntry.VbSequence.Sequence {
					continue
				}
				var newEntries []auth.GrantHistorySequencePair
				// only take last entry
				newEntries = append(newEntries, chanHistory.Entries[len(chanHistory.Entries)-1])
				grantInfo := auth.GrantHistory{Entries: newEntries, Source: chanHistory.Source, UpdatedAt: chanHistory.UpdatedAt}

				// if channel was assigned before user got the role, the correct sequence is in roleEntry
				if chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq < roleEntry.VbSequence.Sequence {
					newEntries[0].StartSeq = roleEntry.VbSequence.Sequence
				}
				resp.addRoleGrants(roleName, roleEntry.Source, keyspace, channel, grantInfo)
			}
		}
	}

	for roleName, roleHist := range user.RoleHistory() {
		role, err := h.db.Authenticator(h.ctx()).GetRole(roleName)
		if err != nil {
			return err
		}
		if role == nil {
			continue
		}

		for _, dsName := range h.db.DataStoreNames() {
			keyspace := fmt.Sprintf("%s.%s", dsName.Scope, dsName.Collection)
			collectionChannels := role.CollectionChannels(dsName.Scope, dsName.Collection)
			channelHistory := role.CollectionChannelHistory(dsName.Scope, dsName.Collection)

			// loop over previous role channels
			for channel, chanEntry := range collectionChannels {
				if channel == channels.DocumentStarChannel {
					continue
				}
				roleChanHistory := roleHist
				if chanEntry.VbSequence.Sequence > roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
					roleChanHistory.Entries = append(roleChanHistory.Entries, auth.GrantHistorySequencePair{StartSeq: chanEntry.VbSequence.Sequence, EndSeq: roleHist.Entries[len(roleHist.Entries)-1].EndSeq})
				}
				// If role is currently assigned, append entries to existing role info in response
				if roleHist.Source == channels.DynamicGrant {
					if entry, ok := resp.DynamicRoleGrants[roleName][keyspace][channel]; ok {
						roleChanHistory.Entries = append(entry.Entries, roleChanHistory.Entries...)
					}
				} else {
					if entry, ok := resp.AdminRoleGrants[roleName][keyspace][channel]; ok {
						roleChanHistory.Entries = append(entry.Entries, roleChanHistory.Entries...)
					}
				}
				resp.addRoleGrants(roleName, roleHist.Source, keyspace, channel, roleChanHistory)
			}

			for channel, chanHistory := range channelHistory {
				// if channel is in history for a sequence span before the user had access to the role
				if chanHistory.Entries[len(chanHistory.Entries)-1].EndSeq < roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
					continue
				}
				var newEntries []auth.GrantHistorySequencePair
				_ = copy(newEntries, roleHist.Entries)
				// if channel was assigned to role after user had access to the role, take channel start seq
				if chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq > roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
					newEntries[len(roleHist.Entries)-1].StartSeq = chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq
					chanHistory = auth.GrantHistory{Entries: newEntries, Source: chanHistory.Source, UpdatedAt: chanHistory.UpdatedAt}
				}
				resp.addRoleGrants(roleName, roleHist.Source, keyspace, channel, chanHistory)
			}
		}
	}

	// Loop over current and past channels
	for _, dsName := range h.db.DataStoreNames() {
		err = authenticator.RebuildCollectionChannels(user, dsName.Scope, dsName.Collection)
		if err != nil {
			return fmt.Errorf("could not rebuild channels for %s: %w", dsName.String(), err)
		}
		keyspace := fmt.Sprintf("%s.%s", dsName.Scope, dsName.Collection)
		collectionChannels := user.CollectionChannels(dsName.Scope, dsName.Collection)
		channelHistory := user.CollectionChannelHistory(dsName.Scope, dsName.Collection)
		// skip making maps if collections are empty
		if len(collectionChannels) == 0 && len(channelHistory) == 0 {
			continue
		}
		for channel, chanHistory := range channelHistory {
			resp.addGrants(chanHistory.Source, keyspace, channel, chanHistory)
		}
		for channel, chanEntry := range collectionChannels {
			history := auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			// If channel is in history, add grant history to current info
			if _, ok := channelHistory[channel]; ok {
				history.Entries = append(history.Entries, channelHistory[channel].Entries...)
			}
			resp.addGrants(chanEntry.Source, keyspace, channel, history)
		}
	}

	bytes, err := base.JSONMarshal(resp)
	if err != nil {
		return fmt.Errorf("Error marshaling getAllChannelsResponse %w", err)
	}
	h.writeRawJSON(bytes)
	return err
}

// addGrants creates maps for each keyspace if they're not there and adds grant info to the response
func (resp *getAllChannelsResponse) addGrants(source string, keyspace string, channelName string, grantInfo auth.GrantHistory) {
	if source == channels.AdminGrant {
		if _, ok := resp.AdminGrants[keyspace]; !ok {
			resp.AdminGrants[keyspace] = make(map[string]auth.GrantHistory)
		}
		resp.AdminGrants[keyspace][channelName] = grantInfo
	} else if source == channels.DynamicGrant {
		if _, ok := resp.DynamicGrants[keyspace]; !ok {
			resp.DynamicGrants[keyspace] = make(map[string]auth.GrantHistory)
		}
		resp.DynamicGrants[keyspace][channelName] = grantInfo
	} else if source == channels.JWTGrant {
		if _, ok := resp.JWTGrants[keyspace]; !ok {
			resp.JWTGrants[keyspace] = make(map[string]auth.GrantHistory)
		}
		resp.JWTGrants[keyspace][channelName] = grantInfo
	}
	grantInfo.Source = ""
}

// addGrants creates maps for each role and keyspace if they're not there and adds grant info to the response
func (resp *getAllChannelsResponse) addRoleGrants(roleName string, source string, keyspace string, channelName string, grantInfo auth.GrantHistory) {
	if source == channels.AdminGrant {
		if _, ok := resp.AdminRoleGrants[roleName]; !ok {
			resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		}
		if _, ok := resp.AdminRoleGrants[roleName][keyspace]; !ok {
			resp.AdminRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
		}
		resp.AdminRoleGrants[roleName][keyspace][channelName] = grantInfo
	} else if source == channels.DynamicGrant {
		if _, ok := resp.DynamicRoleGrants[roleName]; !ok {
			resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		}
		if _, ok := resp.DynamicRoleGrants[roleName][keyspace]; !ok {
			resp.DynamicRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
		}
		resp.DynamicRoleGrants[roleName][keyspace][channelName] = grantInfo
	}
}

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

func (h *handler) handleGetAllChannels() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator(h.ctx()).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if err != nil {
		return err
	}
	if user == nil {
		return kNotFoundError
	}
	var resp getAllChannelsResponse
	resp.AdminGrants = make(map[string]map[string]auth.GrantHistory, len(user.ExplicitChannels()))
	resp.DynamicGrants = make(map[string]map[string]auth.GrantHistory, len(user.Channels())-len(user.ExplicitChannels()))
	resp.DynamicRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.RoleNames())-len(user.ExplicitRoles()))
	resp.AdminRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.ExplicitRoles()))
	authenticator := h.db.Authenticator(h.ctx())
	if err != nil {
		return err
	}

	// Rebuild roles and channels
	for scope, collectionConfig := range user.GetCollectionsAccess() {
		// skip if scope/collection has been removed
		if _, ok := h.db.CollectionNames[scope]; !ok {
			continue
		}
		for collectionName, _ := range collectionConfig {
			if _, ok := h.db.CollectionNames[scope][collectionName]; !ok {
				continue
			}
			err = authenticator.RebuildCollectionChannels(user, scope, collectionName)
			if err != nil {
				return err
			}
		}
	}
	if err := authenticator.RebuildRoles(user); err != nil {
		return err
	}

	if err != nil {
		return err
	}
	for roleName, roleEntry := range user.RoleNames() {
		role, err := h.db.Authenticator(h.ctx()).GetRole(roleName)
		if err != nil {
			return err
		}
		if role == nil {
			continue
		}
		if roleEntry.Source == channels.AdminGrant {
			resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		} else if roleEntry.Source == channels.DynamicGrant {
			resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		}
		for _, dsName := range h.db.DataStoreNames() {
			keyspace := fmt.Sprintf("%s.%s", dsName.Scope, dsName.Collection)
			collectionChannels := role.CollectionChannels(dsName.Scope, dsName.Collection)
			channelHistory := role.CollectionChannelHistory(dsName.Scope, dsName.Collection)
			if len(collectionChannels) == 0 && len(channelHistory) == 0 {
				continue
			}
			if roleEntry.Source == channels.AdminGrant {
				resp.AdminRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
			} else if roleEntry.Source == channels.DynamicGrant {
				resp.DynamicRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
			}
			for channel, chanEntry := range collectionChannels {
				// loop over current role channels
				if channel == channels.DocumentStarChannel {
					continue
				}
				grantInfo := auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}, Source: chanEntry.Source}
				if roleEntry.VbSequence.Sequence > chanEntry.VbSequence.Sequence {
					grantInfo.Entries[len(grantInfo.Entries)-1].StartSeq = roleEntry.VbSequence.Sequence
				}
				resp.addRoleGrants(roleName, roleEntry.Source, keyspace, channel, grantInfo)
				// loop over previous role channels
			}
			for channel, chanHistory := range channelHistory {
				if roleEntry.VbSequence.Sequence > chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq {
					chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = roleEntry.VbSequence.Sequence
				}
				resp.addRoleGrants(roleName, roleEntry.Source, keyspace, channel, chanHistory)
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
		if _, ok := resp.AdminRoleGrants[roleName]; !ok && roleHist.Source == channels.AdminGrant {
			resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		} else if _, ok := resp.DynamicRoleGrants[roleName]; !ok && roleHist.Source == channels.DynamicGrant {
			resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		}
		for _, dsName := range h.db.DataStoreNames() {
			keyspace := fmt.Sprintf("%s.%s", dsName.Scope, dsName.Collection)
			collectionChannels := role.CollectionChannels(dsName.Scope, dsName.Collection)
			channelHistory := role.CollectionChannelHistory(dsName.Scope, dsName.Collection)
			if len(collectionChannels) == 0 && len(channelHistory) == 0 {
				continue
			}
			if _, ok := resp.AdminRoleGrants[roleName][keyspace]; !ok && roleHist.Source == channels.AdminGrant {
				resp.AdminRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
			} else if _, ok := resp.DynamicRoleGrants[roleName][keyspace]; !ok && roleHist.Source == channels.DynamicGrant {
				resp.DynamicRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
			}
			// loop over previous role channels
			for channel, chanEntry := range collectionChannels {
				if channel == channels.DocumentStarChannel {
					continue
				}
				roleChanHistory := roleHist
				if chanEntry.VbSequence.Sequence > roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
					roleChanHistory.Entries = append(roleChanHistory.Entries, auth.GrantHistorySequencePair{StartSeq: chanEntry.VbSequence.Sequence, EndSeq: roleHist.Entries[len(roleHist.Entries)-1].EndSeq})
				}
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
				if chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq < roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
					chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = roleHist.Entries[len(roleHist.Entries)-1].StartSeq
				}
				resp.addRoleGrants(roleName, roleHist.Source, keyspace, channel, chanHistory)
			}
		}
	}

	// Loop over current and past channels
	for _, dsName := range h.db.DataStoreNames() {
		keyspace := fmt.Sprintf("%s.%s", dsName.Scope, dsName.Collection)
		collectionChannels := user.CollectionChannels(dsName.Scope, dsName.Collection)
		channelHistory := user.CollectionChannelHistory(dsName.Scope, dsName.Collection)
		if len(collectionChannels) == 0 && len(channelHistory) == 0 {
			continue
		}
		resp.AdminGrants[keyspace] = make(map[string]auth.GrantHistory)
		resp.DynamicGrants[keyspace] = make(map[string]auth.GrantHistory)
		for channel, chanHistory := range channelHistory {
			resp.addGrants(chanHistory.Source, keyspace, channel, chanHistory)
		}
		for channel, chanEntry := range collectionChannels {
			history := auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			// If channel is in history, add grant history to current info
			if _, ok := channelHistory[channel]; ok {
				for _, entry := range channelHistory[channel].Entries {
					history.Entries = append(history.Entries, entry)
				}
			}
			resp.addGrants(chanEntry.Source, keyspace, channel, history)
		}
	}

	bytes, err := base.JSONMarshal(resp)
	if err != nil {
		return err
	}
	h.writeRawJSON(bytes)
	return err
}

func (resp getAllChannelsResponse) addGrants(source string, keyspace string, channelName string, grantInfo auth.GrantHistory) {
	if source == channels.AdminGrant {
		resp.AdminGrants[keyspace][channelName] = grantInfo
	} else if source == channels.DynamicGrant {
		resp.DynamicGrants[keyspace][channelName] = grantInfo
	} else if source == channels.JWTGrant {
		resp.JWTGrants[keyspace][channelName] = grantInfo
	}
	grantInfo.Source = ""
}

func (resp getAllChannelsResponse) addRoleGrants(roleName string, source string, keyspace string, channelName string, grantInfo auth.GrantHistory) {
	if source == channels.AdminGrant {
		resp.AdminRoleGrants[roleName][keyspace][channelName] = grantInfo
	} else if source == channels.DynamicGrant {
		resp.DynamicRoleGrants[roleName][keyspace][channelName] = grantInfo
	}
}

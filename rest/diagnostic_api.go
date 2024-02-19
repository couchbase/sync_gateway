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

// each field in this response is a map of collection names to a map of channel names to grant history info
type getAllChannelsResponse struct {
	AdminGrants                          map[string]map[string]auth.GrantHistory            `json:"admin_grants,omitempty"`
	DynamicGrants                        map[string]map[string]auth.GrantHistory            `json:"dynamic_grants,omitempty"`
	DefaultScopeCollectionChannelHistory map[string]auth.GrantHistory                       `json:"_default._default_channel_history,omitempty"`
	AdminRoleGrants                      map[string]map[string]map[string]auth.GrantHistory `json:"admin_role_grants,omitempty"`
	DynamicRoleGrants                    map[string]map[string]map[string]auth.GrantHistory `json:"dynamic_role_grants,omitempty"`
	RoleHistoryGrants                    map[string]map[string]map[string]auth.GrantHistory `json:"role_history_grants,omitempty"`
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

	if h.db.OnlyDefaultCollection() {
		for channel, seq := range user.Channels() {
			if user.ExplicitChannels().Contains(channel) {
				resp.AdminGrants["_default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
			} else {
				resp.AdminGrants["_default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
			}
		}
		resp.DefaultScopeCollectionChannelHistory = make(map[string]auth.GrantHistory)
		resp.DefaultScopeCollectionChannelHistory = user.ChannelHistory()
		bytes, err := base.JSONMarshal(resp)
		if err != nil {
			return err
		}
		h.writeRawJSON(bytes)
		return err
	}

	resp.DynamicRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.RoleNames())-len(user.ExplicitRoles()))
	resp.AdminRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.ExplicitRoles()))
	resp.RoleHistoryGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.RoleNames())-len(user.ExplicitRoles()))

	for roleName, seq := range user.RoleNames() {
		role, err := h.db.Authenticator(h.ctx()).GetRole(roleName)
		if err != nil {
			return err
		}
		if role == nil {
			continue
		}
		collAccessAll := role.GetCollectionsAccess()
		resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)

		for scopeName, collections := range collAccessAll {
			for collectionName, collectionAccess := range collections {
				keyspace := scopeName + "." + collectionName
				resp.AdminRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
				resp.DynamicRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)

				// loop over current role channels
				for channel, _ := range collectionAccess.Channels() {
					if _, ok := user.ExplicitRoles()[roleName]; ok {
						resp.AdminRoleGrants[roleName][keyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
					} else {
						resp.DynamicRoleGrants[roleName][keyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
					}
				}
				// loop over previous role channels
				for channel, chanHistory := range collectionAccess.ChannelHistory() {
					if seq.Sequence > chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq {
						chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = seq.Sequence
					}
					if _, ok := user.ExplicitRoles()[roleName]; ok {
						resp.AdminRoleGrants[roleName][keyspace][channel] = chanHistory
					} else {
						resp.DynamicRoleGrants[roleName][keyspace][channel] = chanHistory
					}
				}
			}
		}
	}

	// Loop over previous roles, no way to determine admin and dynamic atm
	for roleName, roleHist := range user.RoleHistory() {
		role, err := h.db.Authenticator(h.ctx()).GetRole(roleName)
		if err != nil {
			return err
		}
		if role == nil {
			continue
		}
		collAccessAll := role.GetCollectionsAccess()
		resp.RoleHistoryGrants[roleName] = make(map[string]map[string]auth.GrantHistory)

		for scopeName, collections := range collAccessAll {
			for collectionName, collectionAccess := range collections {
				keyspace := scopeName + "." + collectionName
				resp.RoleHistoryGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
				// loop over current role channels
				for channel, _ := range collectionAccess.Channels() {
					resp.RoleHistoryGrants[roleName][keyspace][channel] = roleHist
				}
				// loop over previous role channels
				for channel, chanHistory := range collectionAccess.ChannelHistory() {
					if chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq < roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
						chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = roleHist.Entries[len(roleHist.Entries)-1].StartSeq
					}
					resp.RoleHistoryGrants[roleName][keyspace][channel] = chanHistory
				}
			}
		}
	}

	// Loop over current and past channels
	for scope, collectionConfig := range user.GetCollectionsAccess() {
		for collectionName, CAConfig := range collectionConfig {
			keyspace := scope + "." + collectionName
			resp.AdminGrants[keyspace] = make(map[string]auth.GrantHistory)
			resp.DynamicGrants[keyspace] = make(map[string]auth.GrantHistory)
			// current channels
			for channel, seq := range CAConfig.Channels() {
				var history auth.GrantHistory
				// If channel is in history, copy grant history
				if _, ok := CAConfig.ChannelHistory_[channel]; ok {
					history = CAConfig.ChannelHistory_[channel]
					// Else, assign sequence channel was granted as startSeq on new grant history
				} else {
					history = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
				}

				if CAConfig.ExplicitChannels_.Contains(channel) {
					resp.AdminGrants[keyspace][channel] = history
				} else {
					resp.DynamicGrants[keyspace][channel] = history
				}
			}
			for channel, chanHistory := range CAConfig.ChannelHistory() {
				if chanHistory.AdminAssigned {
					resp.AdminGrants[keyspace][channel] = CAConfig.ChannelHistory_[channel]
				} else {
					resp.DynamicGrants[keyspace][channel] = CAConfig.ChannelHistory_[channel]
				}
			}
		}
	}

	// Single named collection + default collection handling, if all_channels includes more than one channel ("!"), its using the default collection.
	if len(user.Channels().AllKeys()) != 1 {
		resp.AdminGrants["_default._default"] = make(map[string]auth.GrantHistory)
		resp.DynamicGrants["_default._default"] = make(map[string]auth.GrantHistory)
		for channel, seq := range user.Channels() {
			if user.ExplicitChannels().Contains(channel) {
				resp.AdminGrants["_default._default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
			} else {
				resp.DynamicGrants["_default._default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
			}
		}
		resp.DefaultScopeCollectionChannelHistory = make(map[string]auth.GrantHistory)
		resp.DefaultScopeCollectionChannelHistory = user.ChannelHistory()
	}

	bytes, err := base.JSONMarshal(resp)
	if err != nil {
		return err
	}
	h.writeRawJSON(bytes)
	return err
}

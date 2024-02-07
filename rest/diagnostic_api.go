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
	"golang.org/x/exp/maps"
)

type allChannels struct {
	Channels auth.TimedSetHistory `json:"all_channels,omitempty"`
}

// each field in this response is a map of collection names to a map of channel names to grant history info
type getAllChannelsResponse struct {
	AdminGrants       map[string]map[string]auth.GrantHistory            `json:"admin_grants,omitempty"`
	DynamicGrants     map[string]map[string]auth.GrantHistory            `json:"dynamic_grants,omitempty"`
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

	adminRoleChannelTimedHistory := map[string]auth.GrantHistory{}
	dynamicRoleChannelTimedHistory := map[string]auth.GrantHistory{}

	resp.DynamicRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.RoleNames())-len(user.ExplicitRoles()))
	resp.AdminRoleGrants = make(map[string]map[string]map[string]auth.GrantHistory, len(user.ExplicitRoles()))
	resp.AdminGrants = make(map[string]map[string]auth.GrantHistory, len(user.ExplicitChannels()))
	resp.DynamicGrants = make(map[string]map[string]auth.GrantHistory, len(user.Channels())-len(user.ExplicitChannels()))
	//
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
				resp.AdminRoleGrants[roleName][scopeName+"."+collectionName] = make(map[string]auth.GrantHistory)
				resp.DynamicRoleGrants[roleName][scopeName+"."+collectionName] = make(map[string]auth.GrantHistory)
				maps.Clear(dynamicRoleChannelTimedHistory)
				maps.Clear(adminRoleChannelTimedHistory)
				// loop over current role channels
				for channel, _ := range collectionAccess.Channels() {
					if _, ok := user.ExplicitRoles()[roleName]; ok {
						adminRoleChannelTimedHistory[channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
					} else {
						dynamicRoleChannelTimedHistory[channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
					}
				}
				// loop over previous role channels
				for channel, chanHistory := range collectionAccess.ChannelHistory() {
					if seq.Sequence > chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq {
						chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = seq.Sequence
					}
					if _, ok := user.ExplicitRoles()[roleName]; ok {
						adminRoleChannelTimedHistory[channel] = chanHistory
					} else {
						dynamicRoleChannelTimedHistory[channel] = chanHistory
					}
				}

				resp.AdminRoleGrants[roleName][scopeName+"."+collectionName] = adminRoleChannelTimedHistory
				resp.DynamicRoleGrants[roleName][scopeName+"."+collectionName] = dynamicRoleChannelTimedHistory
			}
		}
	}

	adminChannelTimedHistory := map[string]auth.GrantHistory{}
	dynamicChannelTimedHistory := map[string]auth.GrantHistory{}
	for scope, collectionConfig := range user.GetCollectionsAccess() {
		for collectionName, CAConfig := range collectionConfig {
			maps.Clear(dynamicChannelTimedHistory)
			maps.Clear(adminChannelTimedHistory)
			for channel, seq := range CAConfig.Channels() {
				if CAConfig.ExplicitChannels_.Contains(channel) {
					// If channel is in history, copy grant history
					if _, ok := CAConfig.ChannelHistory_[channel]; ok {
						adminChannelTimedHistory[channel] = CAConfig.ChannelHistory_[channel]
						// Else, assign sequence channel was granted as startSeq on new grant history
					} else {
						adminChannelTimedHistory[channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
					}
				} else {
					if _, ok := CAConfig.ChannelHistory_[channel]; ok {
						dynamicChannelTimedHistory[channel] = CAConfig.ChannelHistory_[channel]
					} else {
						dynamicChannelTimedHistory[channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: seq.Sequence}}}
					}
				}
			}
			for channel, chanHistory := range CAConfig.ChannelHistory() {
				if chanHistory.AdminAssigned {
					adminChannelTimedHistory[channel] = CAConfig.ChannelHistory_[channel]
				} else {
					dynamicChannelTimedHistory[channel] = CAConfig.ChannelHistory_[channel]
				}
			}

			resp.AdminGrants[scope+"."+collectionName] = adminChannelTimedHistory
			resp.DynamicGrants[scope+"."+collectionName] = dynamicChannelTimedHistory
		}
	}

	if !h.db.OnlyDefaultCollection() {
		bytes, err := base.JSONMarshal(resp)
		h.writeRawJSON(bytes)
		return err
	}
	info := marshalPrincipal(h.db, user, true)
	bytes, err := base.JSONMarshal(info.Channels)
	h.writeRawJSON(bytes)
	return err
}

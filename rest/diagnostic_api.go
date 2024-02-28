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
	userPrinc, err := authenticator.GetPrincipal(user.Name(), true)

	if h.db.OnlyDefaultCollection() {
		defaultKeyspace := "_default._default"
		resp.AdminGrants[defaultKeyspace] = make(map[string]auth.GrantHistory)
		resp.DynamicGrants[defaultKeyspace] = make(map[string]auth.GrantHistory)
		err = authenticator.RebuildCollectionChannels(userPrinc, "_default", "_default")
		if err != nil {
			return err
		}
		for channel, chanEntry := range userPrinc.Channels() {
			if chanEntry.Source == channels.AdminGrant {
				resp.AdminGrants[defaultKeyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			} else if chanEntry.Source == channels.DynamicGrant {
				resp.DynamicGrants[defaultKeyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			} else if chanEntry.Source == channels.JWTGrant {
				resp.JWTGrants[defaultKeyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			}
		}
		for channel, chanHistory := range userPrinc.ChannelHistory() {
			if chanHistory.Source == channels.AdminGrant {
				resp.AdminGrants[defaultKeyspace][channel] = chanHistory
			} else if chanHistory.Source == channels.DynamicGrant {
				resp.DynamicGrants[defaultKeyspace][channel] = chanHistory
			} else if chanHistory.Source == channels.JWTGrant {
				resp.JWTGrants[defaultKeyspace][channel] = chanHistory
			}
		}
		for roleName, roleEntry := range user.RoleNames() {
			role, err := h.db.Authenticator(h.ctx()).GetRole(roleName)
			if err != nil {
				return err
			}
			resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
			resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
			resp.AdminRoleGrants[roleName][defaultKeyspace] = make(map[string]auth.GrantHistory)
			resp.DynamicRoleGrants[roleName][defaultKeyspace] = make(map[string]auth.GrantHistory)
			for channel, chanEntry := range role.Channels() {
				if roleEntry.Source == channels.AdminGrant {
					resp.AdminRoleGrants[roleName][defaultKeyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}, Source: chanEntry.Source}
				} else if roleEntry.Source == channels.DynamicGrant {
					resp.DynamicRoleGrants[roleName][defaultKeyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}, Source: chanEntry.Source}
				}
			}
		}
		bytes, err := base.JSONMarshal(resp)
		if err != nil {
			return err
		}
		h.writeRawJSON(bytes)
		return err
	}

	// Rebuild roles and channels
	for scope, collectionConfig := range user.GetCollectionsAccess() {
		for collectionName, _ := range collectionConfig {
			err = authenticator.RebuildCollectionChannels(userPrinc, scope, collectionName)
			if err != nil {
				return err
			}
		}
	}
	if err := authenticator.RebuildRoles(user); err != nil {
		return err
	}

	user, err = h.db.Authenticator(h.ctx()).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if err != nil {
		return err
	}
	for roleName, roleEntry := range user.RoleNames() {
		role, err := h.db.Authenticator(h.ctx()).GetRole(roleName)
		rolePrinc, err := authenticator.GetPrincipal(roleName, false)
		if err != nil {
			return err
		}
		if role == nil {
			continue
		}
		collAccessAll := rolePrinc.GetCollectionsAccess()
		resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
		resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)

		for scopeName, collections := range collAccessAll {
			for collectionName, collectionAccess := range collections {
				keyspace := scopeName + "." + collectionName
				resp.AdminRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
				resp.DynamicRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)

				// loop over current role channels
				for channel, chanEntry := range collectionAccess.Channels() {
					if roleEntry.Source == channels.AdminGrant {
						resp.AdminRoleGrants[roleName][keyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}, Source: chanEntry.Source}
					} else {
						resp.DynamicRoleGrants[roleName][keyspace][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}, Source: chanEntry.Source}
					}
				}
				// loop over previous role channels
				for channel, chanHistory := range collectionAccess.ChannelHistory() {
					if roleEntry.VbSequence.Sequence > chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq {
						chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = roleEntry.VbSequence.Sequence
					}
					if roleEntry.Source == channels.AdminGrant {
						resp.AdminRoleGrants[roleName][keyspace][channel] = chanHistory
					} else if roleEntry.Source == channels.DynamicGrant {
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

		for scopeName, collections := range collAccessAll {
			for collectionName, collectionAccess := range collections {
				keyspace := scopeName + "." + collectionName
				resp.AdminRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
				resp.DynamicRoleGrants[roleName] = make(map[string]map[string]auth.GrantHistory)
				resp.AdminRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
				resp.DynamicRoleGrants[roleName][keyspace] = make(map[string]auth.GrantHistory)
				// loop over current role channels
				for channel, chanEntry := range collectionAccess.Channels() {
					roleChanHistory := roleHist
					if chanEntry.VbSequence.Sequence > roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
						roleChanHistory = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence, EndSeq: roleHist.Entries[len(roleHist.Entries)-1].EndSeq}}, Source: chanEntry.Source}
					}
					if roleHist.Source == channels.AdminGrant {
						resp.AdminRoleGrants[roleName][keyspace][channel] = roleChanHistory
					} else {
						resp.DynamicRoleGrants[roleName][keyspace][channel] = roleChanHistory
					}
				}
				// loop over previous role channels
				for channel, chanHistory := range collectionAccess.ChannelHistory() {
					if chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq < roleHist.Entries[len(roleHist.Entries)-1].StartSeq {
						chanHistory.Entries[len(chanHistory.Entries)-1].StartSeq = roleHist.Entries[len(roleHist.Entries)-1].StartSeq
					}
					if roleHist.Source == channels.AdminGrant {
						resp.AdminRoleGrants[roleName][keyspace][channel] = chanHistory
					} else {
						resp.DynamicRoleGrants[roleName][keyspace][channel] = chanHistory
					}
				}
			}
		}
	}

	// Loop over current and past channels
	for scope, collectionConfig := range userPrinc.GetCollectionsAccess() {
		for collectionName, CAConfig := range collectionConfig {
			keyspace := scope + "." + collectionName
			resp.AdminGrants[keyspace] = make(map[string]auth.GrantHistory)
			resp.DynamicGrants[keyspace] = make(map[string]auth.GrantHistory)
			// current channels
			for channel, chanEntry := range CAConfig.Channels() {
				var history auth.GrantHistory
				// If channel is in history, copy grant history
				if _, ok := CAConfig.ChannelHistory_[channel]; ok {
					history = CAConfig.ChannelHistory_[channel]
					// Else, assign sequence channel was granted as startSeq on new grant history
				} else {
					history = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
				}

				if chanEntry.Source == channels.AdminGrant {
					resp.AdminGrants[keyspace][channel] = history
				} else if chanEntry.Source == channels.DynamicGrant {
					resp.DynamicGrants[keyspace][channel] = history
				} else if chanEntry.Source == channels.JWTGrant {
					resp.JWTGrants[keyspace][channel] = history
				}
			}
			for channel, chanHistory := range CAConfig.ChannelHistory() {
				if chanHistory.Source == channels.AdminGrant {
					resp.AdminGrants[keyspace][channel] = CAConfig.ChannelHistory_[channel]
				} else if chanHistory.Source == channels.DynamicGrant {
					resp.DynamicGrants[keyspace][channel] = CAConfig.ChannelHistory_[channel]
				} else if chanHistory.Source == channels.JWTGrant {
					resp.JWTGrants[keyspace][channel] = CAConfig.ChannelHistory_[channel]
				}
			}
		}
	}

	// Single named collection + default collection handling, if all_channels includes more than one channel ("!"), its using the default collection.
	if len(user.Channels().AllKeys()) != 1 {
		err = authenticator.RebuildCollectionChannels(userPrinc, "_default", "_default")

		resp.AdminGrants["_default._default"] = make(map[string]auth.GrantHistory)
		resp.DynamicGrants["_default._default"] = make(map[string]auth.GrantHistory)
		for channel, chanEntry := range userPrinc.Channels() {
			if chanEntry.Source == channels.AdminGrant {
				resp.AdminGrants["_default._default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			} else if chanEntry.Source == channels.DynamicGrant {
				resp.DynamicGrants["_default._default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			} else if chanEntry.Source == channels.JWTGrant {
				resp.JWTGrants["_default._default"][channel] = auth.GrantHistory{Entries: []auth.GrantHistorySequencePair{{StartSeq: chanEntry.VbSequence.Sequence}}}
			}
		}
		for channel, chanHistory := range userPrinc.ChannelHistory() {
			if chanHistory.Source == channels.AdminGrant {
				resp.AdminGrants["_default._default"][channel] = chanHistory
			} else if chanHistory.Source == channels.DynamicGrant {
				resp.DynamicGrants["_default._default"][channel] = chanHistory
			} else if chanHistory.Source == channels.JWTGrant {
				resp.JWTGrants["_default._default"][channel] = chanHistory
			}
		}
	}

	bytes, err := base.JSONMarshal(resp)
	if err != nil {
		return err
	}
	h.writeRawJSON(bytes)
	return err
}

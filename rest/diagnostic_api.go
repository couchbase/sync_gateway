//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
)

type allChannels struct {
	Channels base.Set `json:"all_channels,omitempty"`
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
	info := marshalPrincipal(h.db, user, true)

	channels := allChannels{Channels: info.Channels}
	if !h.db.OnlyDefaultCollection() {
		bytes, err := base.JSONMarshal(info.CollectionAccess)
		h.writeRawJSON(bytes)
		return err
	}
	bytes, err := base.JSONMarshal(channels)
	h.writeRawJSON(bytes)
	return err
}

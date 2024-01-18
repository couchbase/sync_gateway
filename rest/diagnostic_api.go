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
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	info := marshalPrincipal(h.db, user, true)

	channels := allChannels{Channels: info.Channels}
	bytes, err := base.JSONMarshal(channels)
	h.writeRawJSON(bytes)
	return err
}

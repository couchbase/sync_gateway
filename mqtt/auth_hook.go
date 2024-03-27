//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"context"
	"slices"

	"github.com/couchbase/sync_gateway/base"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Hook that uses SG's user database for authentication.
type authHook struct {
	mochi.HookBase
	ctx    context.Context
	server *Server
}

func newAuthHook(server *Server) *authHook {
	return &authHook{ctx: server.ctx, server: server}
}

// ID returns the ID of the hook.
func (h *authHook) ID() string {
	return "SGAuthHook"
}

// Provides indicates which hook methods this hook provides.
func (h *authHook) Provides(b byte) bool {
	return b == mochi.OnConnectAuthenticate || b == mochi.OnACLCheck
}

// Authenticates a connection request.
func (h *authHook) OnConnectAuthenticate(client *mochi.Client, pk packets.Packet) bool {
	defer base.FatalPanicHandler()
	dbc, username := h.server.clientDatabaseContext(client)
	if dbc == nil {
		base.WarnfCtx(h.ctx, "MQTT connection attempt failed with username %q -- does not begin with a database name", client.Properties.Username)
		return false
	}

	user, _ := dbc.Authenticator(h.ctx).AuthenticateUser(username, string(pk.Connect.Password))
	if user == nil {
		base.WarnfCtx(h.ctx, "MQTT auth failure for username %q", client.Properties.Username)
		return false
	}

	existing, _ := h.server.broker.Clients.Get(pk.Connect.ClientIdentifier)
	if existing != nil && !slices.Equal(existing.Properties.Username, client.Properties.Username) {
		// This connection ID is already in use by a different user's client.
		// If we continue, the broker will take over that session, which is a possible
		// security issue: this client would inherit its topic subscriptions.
		base.WarnfCtx(h.ctx, "MQTT auth failure for username %q: reusing session ID %q already belonging to user %q",
			client.Properties.Username, pk.Connect.ClientIdentifier, existing.Properties.Username)
		return false
	}

	base.InfofCtx(h.ctx, base.KeyMQTT, "Client connection by user %q to db %q (session ID %q)", username, dbc.Name, client.ID)
	return true
}

// Authorizes access to a topic.
func (h *authHook) OnACLCheck(client *mochi.Client, topic string, write bool) (allowed bool) {
	defer base.FatalPanicHandler()
	// The topic name has to have the DB name as its first component:
	dbc, username := h.server.clientDatabaseContext(client)
	topic, ok := stripDbNameFromTopic(dbc, topic)
	if !ok {
		base.WarnfCtx(h.ctx, "MQTT: DB %s user %q tried to access topic %q not in that DB", dbc.Name, username, topic)
		return false
	}

	user, err := dbc.Authenticator(h.ctx).GetUser(username)
	if err != nil {
		base.WarnfCtx(h.ctx, "MQTT: OnACLCheck: Can't find DB user for MQTT client username %q", username)
		return false
	}

	allowed = dbcSettings(dbc).Authorize(user, topic, write)
	if allowed {
		base.InfofCtx(h.ctx, base.KeyMQTT, "DB %s user %q accessing topic %q, write=%v", dbc.Name, username, topic, write)
	} else {
		base.InfofCtx(h.ctx, base.KeyMQTT, "DB %s user %q blocked from accessing topic %q, write=%v", dbc.Name, username, topic, write)
	}
	return
}

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
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Persistent storage hook that intercepts publishing messages.
type publishHook struct {
	mochi.HookBase

	ctx    context.Context
	server *Server // My owner, the public `Server` object
}

// Creates a new publishHook.
func newPublishHook(server *Server) (*publishHook, error) {
	hook := &publishHook{ctx: server.ctx, server: server}
	return hook, nil
}

func (h *publishHook) ID() string {
	return "CouchbaseBucket"
}

func (h *publishHook) Provides(b byte) bool {
	return b == mochi.OnPublish
}

// `OnPublish` is called before a client publishes a message.
func (h *publishHook) OnPublish(client *mochi.Client, packet packets.Packet) (packets.Packet, error) {
	defer base.FatalPanicHandler()

	if !client.Net.Inline {
		dbc, username := h.server.clientDatabaseContext(client)
		if dbc == nil {
			return packet, fmt.Errorf("can't get DatabaseContext from client username %q", client.Properties.Username)
		}

		// Strip the db name from the topic name:
		topicName, ok := stripDbNameFromTopic(dbc, packet.TopicName)
		if !ok {
			base.ErrorfCtx(h.ctx, "MQTT: OnPublish received mismatched topic %q for client %q",
				topicName, client.Properties.Username)
			return packet, nil
		}

		if config, topic := dbcSettings(dbc).MatchIngest(topicName); config != nil {
			base.InfofCtx(h.ctx, base.KeyMQTT, "Ingesting message from client %q for db %q, topic %q", username, dbc.Name, topicName)
			err := IngestMessage(h.ctx, *topic, packet.Payload, config, dbc, packet.Properties.MessageExpiryInterval)
			if err != nil {
				base.WarnfCtx(h.ctx, "MQTT broker failed to save message in db %q from topic %q: %v", dbc.Name, topicName, err)
			}
		} else {
			base.DebugfCtx(h.ctx, base.KeyMQTT, "Client %q published non-persistent message in db %q, topic %q", username, dbc.Name, topicName)
		}

		if agent := h.server.clusterAgent; agent != nil {
			agent.broadcastPublish(&packet)
		}

	} else {
		base.DebugfCtx(h.ctx, base.KeyMQTT, "Relayed peer message to topic %q", packet.TopicName)
	}

	return packet, nil
}

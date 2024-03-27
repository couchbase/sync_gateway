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
	"strings"

	"github.com/couchbase/sync_gateway/base"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Hook that renames topics so that internally they are prefixed with the database name.
type topicRenameHook struct {
	mochi.HookBase
	ctx    context.Context
	server *Server
}

func newTopicRenameHook(server *Server) *topicRenameHook {
	return &topicRenameHook{ctx: server.ctx, server: server}
}

func (h *topicRenameHook) ID() string {
	return "TopicRenameHook"
}

// `Provides` indicates which hook methods this hook provides.
func (h *topicRenameHook) Provides(b byte) bool {
	return b == mochi.OnPacketRead || b == mochi.OnPacketEncode
}

// `OnPacketRead` is called after an MQTT packet is received and parsed, but before it's handled.
func (h *topicRenameHook) OnPacketRead(client *mochi.Client, packet packets.Packet) (packets.Packet, error) {
	defer base.FatalPanicHandler()
	if prefix, _ := h.server.clientDatabaseName(client); prefix != "" {
		prefix += "/"
		fix := func(name *string) {
			if *name != "" && (*name)[0] != '$' {
				base.DebugfCtx(h.ctx, base.KeyMQTT, "Incoming packet: renamed %q -> %q",
					*name, prefix+*name)
				*name = prefix + *name
			}
		}

		fix(&packet.TopicName)
		fix(&packet.Properties.ResponseTopic)
		fix(&packet.Connect.WillTopic)
		fix(&packet.Connect.WillProperties.ResponseTopic)
		for i := 0; i < len(packet.Filters); i++ {
			fix(&packet.Filters[i].Filter)
		}
	}
	return packet, nil
}

// `OnPacketEncode` is called just before a Packet struct is encoded to MQTT and sent to a client.
func (h *topicRenameHook) OnPacketEncode(client *mochi.Client, packet packets.Packet) packets.Packet {
	defer base.FatalPanicHandler()
	if prefix, _ := h.server.clientDatabaseName(client); prefix != "" {
		prefix += "/"
		fix := func(name *string) {
			if strings.HasPrefix(*name, prefix) {
				base.DebugfCtx(h.ctx, base.KeyMQTT, "Outgoing packet: renamed %q -> %q",
					*name, (*name)[len(prefix):])
				*name = (*name)[len(prefix):]
			} else if *name != "" && (*name)[0] != '$' {
				base.WarnfCtx(h.ctx, "Unprefixed topic in outgoing packet: %q", *name)
			}
		}

		fix(&packet.TopicName)
		fix(&packet.Properties.ResponseTopic)
		fix(&packet.Connect.WillTopic)                    // unnecessary?
		fix(&packet.Connect.WillProperties.ResponseTopic) // unnecessary?
		for i := 0; i < len(packet.Filters); i++ {
			fix(&packet.Filters[i].Filter)
		}
	}
	return packet
}

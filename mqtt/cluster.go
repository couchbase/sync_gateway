//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/hashicorp/memberlist"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

const kDefaultDiscoveryPort = 7946
const kHeartbeatKeyPrefix = "_sync:MQTT_Discovery"

// Manages communication with other brokers in the cluster.
type clusterAgent struct {
	ctx              context.Context
	clusteringID     string
	metadataStore    sgbucket.DataStore // Bucket that stores broker metadata
	broker           *mochi.Server
	heartbeater      base.Heartbeater // Publishes my address for other nodes to discover on startup
	getPeerNodesFunc func() ([]string, error)
	peers            *memberlist.Memberlist
	broadcastQueue   memberlist.TransmitLimitedQueue
}

func startClusterAgent(server *Server) (agent *clusterAgent, err error) {
	// Determine which ports to use for cluster membership and Raft:
	clusterCfg := server.config.Cluster
	if clusterCfg == nil {
		return
	} else if !clusterCfg.IsEnabled() {
		base.InfofCtx(server.ctx, base.KeyMQTT, "MQTT clustering is disabled per config")
		return
	}

	discoveryAddr := clusterCfg.DiscoveryAddr
	discoveryPort := kDefaultDiscoveryPort
	if discoveryAddr != "" {
		var portStr string
		discoveryAddr, portStr, err = net.SplitHostPort(discoveryAddr)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse cluster.discovery_addr %q", clusterCfg.DiscoveryAddr)
		}
		discoveryPort, _ = strconv.Atoi(portStr)
	}
	if discoveryAddr == "" {
		discoveryAddr = "0.0.0.0"
	}

	agent = &clusterAgent{
		ctx:           server.ctx,
		clusteringID:  net.JoinHostPort(server.host, strconv.Itoa(discoveryPort)),
		metadataStore: server.persister.metadataStore,
		broker:        server.broker,
	}

	if err = agent.startHeartbeat(); err != nil {
		return nil, fmt.Errorf("error starting Heartbeater: %w", err)
	}

	defer func() {
		if err != nil {
			agent.stop()
			agent = nil
		}
	}()

	peerAddrs, err := agent.getPeerAddresses()
	if err != nil {
		return
	}
	base.InfofCtx(agent.ctx, base.KeyMQTT, "Starting MQTT cluster agent %s, binding to %s; known peers: %v", agent.clusteringID, discoveryAddr, peerAddrs)

	// Create the Memberlist:
	memberCfg := memberlist.DefaultLANConfig()
	memberCfg.Name = agent.clusteringID
	memberCfg.BindAddr = discoveryAddr
	memberCfg.BindPort = discoveryPort
	memberCfg.Delegate = agent
	memberCfg.Events = agent
	memberCfg.Logger = newLogLogger(server.ctx, base.KeyMQTT, "")
	agent.peers, err = memberlist.Create(memberCfg)
	if err != nil {
		return
	}

	agent.broadcastQueue.NumNodes = agent.peers.NumMembers
	agent.broadcastQueue.RetransmitMult = 1 // ????

	if len(peerAddrs) > 0 {
		n, _ := agent.peers.Join(peerAddrs)
		base.InfofCtx(agent.ctx, base.KeyMQTT, "Cluster agent made contact with %d peers", n)
	}
	return
}

// Shuts down the cluster agent.
func (agent *clusterAgent) stop() {
	if agent.peers != nil {
		base.InfofCtx(agent.ctx, base.KeyMQTT, "Cluster agent is saying goodbye...")
		agent.peers.Leave(5 * time.Second)
		agent.peers.Shutdown()
		agent.peers = nil
	}
	if agent.heartbeater != nil {
		agent.heartbeater.Stop(agent.ctx)
		agent.heartbeater = nil
	}
}

//======== HEARTBEAT STUFF

// Starts the `Heartbeater`, which lets new nodes discover this one so they can join the cluster.
// This shouldn't be called until the MQTT broker is up and clustered.
func (agent *clusterAgent) startHeartbeat() (err error) {
	base.InfofCtx(agent.ctx, base.KeyMQTT, "Starting Heartbeater")
	// Initialize node heartbeater. This node must start sending heartbeats before registering
	// itself to the cfg, to avoid triggering immediate removal by other active nodes.
	heartbeater, err := base.NewCouchbaseHeartbeater(agent.metadataStore, kHeartbeatKeyPrefix, agent.clusteringID)
	if err != nil {
		return
	} else if err = heartbeater.Start(agent.ctx); err != nil {
		return
	}

	defer func() {
		if err != nil {
			heartbeater.Stop(agent.ctx)
		}
	}()

	listener, err := base.NewDocumentBackedListener(agent.metadataStore, kHeartbeatKeyPrefix)
	if err != nil {
		return
	} else if err = listener.AddNode(agent.ctx, agent.clusteringID); err != nil {
		return
	} else if err = heartbeater.RegisterListener(listener); err != nil {
		return
	}

	agent.heartbeater = heartbeater
	agent.getPeerNodesFunc = listener.GetNodes
	return
}

// Gets a list of addresses of other SG nodes in the MQTT cluster
func (agent *clusterAgent) getPeerAddresses() ([]string, error) {
	peers, err := agent.getPeerNodesFunc()
	if peers == nil {
		peers = []string{}
	} else {
		peers = slices.Clone(peers) // the original array belongs to the Heartbeater
		peers = slices.DeleteFunc(peers, func(a string) bool {
			return a == agent.clusteringID
		})
	}
	return peers, err
}

//======== PACKET BROADCAST

func (agent *clusterAgent) broadcastPublish(packet *packets.Packet) error {
	if agent.peers.NumMembers() > 0 {
		var buf bytes.Buffer
		buf.WriteByte('P') // means Publish
		buf.WriteByte(packet.ProtocolVersion)
		if err := packet.PublishEncode(&buf); err != nil {
			return err
		}
		b := &packetBroadcast{data: buf.Bytes()}
		base.InfofCtx(agent.ctx, base.KeyMQTT, "Broadcasting Publish packet for topic %q", packet.TopicName)
		agent.broadcastQueue.QueueBroadcast(b)
	}
	return nil
}

type packetBroadcast struct {
	data []byte
}

func (b *packetBroadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

// Returns a byte form of the message
func (b *packetBroadcast) Message() []byte {
	return b.data
}

// Finished is invoked when the message will no longer
// be broadcast, either due to invalidation or to the
// transmit limit being reached
func (b *packetBroadcast) Finished() {}

//======== MEMBERLIST DELEGATE

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (agent *clusterAgent) NodeMeta(limit int) []byte {
	return nil
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (agent *clusterAgent) NotifyMsg(message []byte) {
	if len(message) <= 1 {
		base.ErrorfCtx(agent.ctx, "MQTT cluster received empty message")
		return
	}
	switch message[0] {
	case 'P':
		// This is a Publish packet
		if packet, err := decodePacket(message[2:], message[1]); err == nil {
			base.InfofCtx(agent.ctx, base.KeyMQTT, "Relaying PUBLISH packet from peer for topic %q (%d bytes)", packet.TopicName, len(packet.Payload))
			agent.broker.Publish(packet.TopicName, packet.Payload, packet.FixedHeader.Retain, packet.FixedHeader.Qos)
		} else {
			base.ErrorfCtx(agent.ctx, "MQTT cluster error decoding packet: %v", err)
		}
	default:
		base.ErrorfCtx(agent.ctx, "MQTT cluster received message with unknown start byte '%c'", message[0])
	}
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (agent *clusterAgent) GetBroadcasts(overhead, limit int) [][]byte {
	return agent.broadcastQueue.GetBroadcasts(overhead, limit)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (agent *clusterAgent) LocalState(join bool) []byte {
	return nil
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (agent *clusterAgent) MergeRemoteState(buf []byte, join bool) {
}

//======== MEMBERLIST EVENTS INTERFACE

// NotifyJoin is invoked when a node is detected to have joined.
func (agent *clusterAgent) NotifyJoin(node *memberlist.Node) {
	base.InfofCtx(agent.ctx, base.KeyMQTT, "Cluster node %s joined", node.Name)
}

// NotifyLeave is invoked when a node is detected to have left.
func (agent *clusterAgent) NotifyLeave(node *memberlist.Node) {
	base.InfofCtx(agent.ctx, base.KeyMQTT, "Cluster node %s left", node.Name)
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data.
func (agent *clusterAgent) NotifyUpdate(node *memberlist.Node) {
}

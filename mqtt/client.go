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
	"net/url"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

//======== MQTT CLIENT:

// MQTT client for Sync Gateway. (Implements db.MQTTClient)
type Client struct {
	ctx        context.Context             // Context
	config     *ClientConfig               // Configuration
	subs       TopicMap[*IngestConfig]     // Maps topic filters to SubscribeConfig
	database   *db.DatabaseContext         // Database
	conn       *autopaho.ConnectionManager // Actual MQTT client
	cancelFunc context.CancelFunc          // Call this to stop `conn`
}

func (config *PerDBConfig) Start(ctx context.Context, dbc *db.DatabaseContext) ([]db.MQTTClient, error) {
	var clients []db.MQTTClient
	for _, cfg := range config.Clients {
		client, err := StartClient(ctx, cfg, dbc)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}

// Creates and starts an MQTT client.
func StartClient(ctx context.Context, config *ClientConfig, dbc *db.DatabaseContext) (*Client, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	client := &Client{
		ctx:      ctx,
		config:   config,
		database: dbc,
	}

	brokerURL, err := url.Parse(config.Broker.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid MQTT broker URL %v", config.Broker.URL)
	}

	client.subs, err = MakeTopicMap(config.Ingest)
	if err != nil {
		return nil, err
	}

	pahoCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		CleanStartOnInitialConnection: false, // Use persistent connection state
		KeepAlive:                     20,    // Interval (secs) to send keepalive messages
		SessionExpiryInterval:         3600,  // Seconds that a session will survive after disconnection.

		OnConnectionUp: client.onConnectionUp,
		OnConnectError: client.onConnectError,

		ClientConfig: paho.ClientConfig{
			ClientID:           config.Broker.ClientID,
			OnClientError:      client.onClientError,
			OnServerDisconnect: client.onServerDisconnect,
			OnPublishReceived:  []func(paho.PublishReceived) (bool, error){client.onPublishReceived},
		},
	}

	pahoCtx, cancelFunc := context.WithCancel(ctx)
	client.conn, err = autopaho.NewConnection(pahoCtx, pahoCfg)
	client.cancelFunc = cancelFunc
	return client, err
}

// Stops the MQTT client.
func (client *Client) Stop() error {
	if cf := client.cancelFunc; cf != nil {
		base.InfofCtx(client.ctx, base.KeyMQTT, "Stopping client %q", client.config.Broker.ClientID)
		client.cancelFunc = nil
		cf()
	}
	return nil
}

//======= HANDLER METHODS:

// Called after connecting to the broker.
func (client *Client) onConnectionUp(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
	base.InfofCtx(client.ctx, base.KeyMQTT, "Client connected to %s as %q", client.config.Broker.URL, client.config.Broker.ClientID)
	// Subscribing in the OnConnectionUp callback is recommended (ensures the subscription is
	// reestablished if the connection drops)
	var sub paho.Subscribe
	for topicPat, opt := range client.config.Ingest {
		qos := byte(0)
		if opt.QoS != nil {
			qos = byte(*opt.QoS)
		}
		sub.Subscriptions = append(sub.Subscriptions, paho.SubscribeOptions{Topic: topicPat, QoS: qos})
	}

	if _, err := cm.Subscribe(client.ctx, &sub); err != nil {
		base.ErrorfCtx(client.ctx, "Client %q failed to subscribe: %v", client.config.Broker.ClientID, err)
	}
}

func (client *Client) onConnectError(err error) {
	base.ErrorfCtx(client.ctx, "MQTT Client %q failed to connect to %s: %v", client.config.Broker.ClientID, client.config.Broker.URL, err)
}

func (client *Client) onClientError(err error) {
	base.ErrorfCtx(client.ctx, "MQTT Client %q got an error from %s: %v", client.config.Broker.ClientID, client.config.Broker.URL, err)
}

func (client *Client) onServerDisconnect(d *paho.Disconnect) {
	base.WarnfCtx(client.ctx, "MQTT Client %q disconnected from %s; reason=%d", client.config.Broker.ClientID, client.config.Broker.URL, d.ReasonCode)
}

// Handles an incoming MQTT message from a subscription.
func (client *Client) onPublishReceived(pub paho.PublishReceived) (bool, error) {
	base.InfofCtx(client.ctx, base.KeyMQTT, "Client %q received message in topic %s", client.config.Broker.ClientID, pub.Packet.Topic)

	// Look up the subscription:
	sub, match := client.subs.Match(pub.Packet.Topic)
	if sub == nil {
		base.WarnfCtx(client.ctx, "MQTT Client %q received message in unexpected topic %q", client.config.Broker.ClientID, pub.Packet.Topic)
		return false, nil
	}

	var exp uint32 = 0
	if msgExp := pub.Packet.Properties.MessageExpiry; msgExp != nil {
		exp = base.SecondsToCbsExpiry(int(*msgExp))
	}

	err := IngestMessage(client.ctx, *match, pub.Packet.Payload, sub, client.database, exp)
	if err != nil {
		base.WarnfCtx(client.ctx, "MQTT Client %q failed to save message from topic %q: %v", client.config.Broker.ClientID, pub.Packet.Topic, err)
	}
	return (err == nil), err
}

var (
	// Enforce interface conformance:
	_ db.MQTTConfig = &PerDBConfig{}
	_ db.MQTTClient = &Client{}
)

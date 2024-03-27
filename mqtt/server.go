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
	"crypto/tls"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
)

//======== CONFIG:

const kDefaultMaximumMessageExpiryInterval = 60 * 60 * 24
const kDefaultMaximumSessionExpiryInterval = 60 * 60 * 24

// Top-level MQTT broker/server configuration.
type ServerConfig struct {
	Enabled                      *bool          `json:"enabled,omitempty"`
	PublicInterface              string         `json:"public_interface,omitempty"  help:"Network interface to bind MQTT server to"`
	MetadataDB                   *string        `json:"metadata_db,omitempty" help:"Name of database to persist MQTT state to"`
	Cluster                      *ClusterConfig `json:"cluster,omitempty" help:"Cluster configuration (omit for no clustering)"`
	MaximumMessageExpiryInterval int64          `json:"maximum_message_expiry_interval,omitempty" help:"Maximum message lifetime, in seconds; 0 means default"`
	MaximumSessionExpiryInterval uint32         `json:"maximum_session_expiry_interval,omitempty" help:"Maximum disconnected session lifetime, in seconds; 0 means default"`
}

type ClusterConfig struct {
	Enabled       *bool  `json:"enabled,omitempty" help:"Set to false to disable clustering"`
	DiscoveryAddr string `json:"discovery_address" help:"Address+port for peer discovery and gossip"`
}

func (config *ClusterConfig) IsEnabled() bool {
	return config != nil && (config.Enabled == nil || *config.Enabled)
}

func (config *ServerConfig) IsEnabled() bool {
	return config != nil && (config.Enabled == nil || *config.Enabled)
}

func (config *ServerConfig) ParseMetadataStore() (db string, ds sgbucket.DataStoreName, err error) {
	if config.MetadataDB != nil {
		pieces := strings.Split(*config.MetadataDB, sgbucket.ScopeCollectionSeparator)
		db = pieces[0]
		scope := sgbucket.DefaultScope
		collection := sgbucket.DefaultCollection
		switch len(pieces) {
		case 1:
		case 2:
			collection = pieces[1]
		case 3:
			scope, collection = pieces[1], pieces[2]
		default:
			err = fmt.Errorf("MQTT: invalid metadata store: too many components")
			return
		}
		ds, err = sgbucket.NewValidDataStoreName(scope, collection)
	}
	return
}

func (config *ServerConfig) Validate() error {
	_, _, err := config.ParseMetadataStore()
	return err
}

//========== API:

// MQTT server for Sync Gateway.
type Server struct {
	ctx          context.Context // Go context
	config       *ServerConfig   // Configuration struct
	delegate     ServerDelegate  // Maps db names to DatabaseContexts
	host         string          // Real IP address string (no port#)
	broker       *mochi.Server   // Mochi MQTT server
	persister    *persister      // Manages persistent state (client sessions, messages)
	clusterAgent *clusterAgent   // Manages cluster communication
}

// Interface that gives the Server access to databases. (Implemented by rest.ServerContext.)
type ServerDelegate interface {
	Database(ctx context.Context, dbName string) *db.DatabaseContext
}

// Creates a new MQTT Server instance from:
// - ctx: The context.Context
// - config: The configuration struct
// - tlsConfig: Optional TLS configuration
// - metadataStore: A DataStore for persisting non-database-specific MQTT state.
// - delegate: Maps db names to DatabaseContexts
func NewServer(
	ctx context.Context,
	config *ServerConfig,
	tlsConfig *tls.Config,
	metadataStore sgbucket.DataStore,
	delegate ServerDelegate,
) (*Server, error) {
	// Get a real IP address for this computer from the server address configuration:
	host, err := makeRealIPAddress(config.PublicInterface, false)
	if err != nil {
		return nil, fmt.Errorf("couldn't look up a real IP address from %q: %w", config.PublicInterface, err)
	}

	// Create the new MQTT Server.
	opts := mochi.Options{
		Capabilities: mochi.NewDefaultServerCapabilities(),
		InlineClient: true,
		Logger:       newSlogger(base.KeyMQTT, "mqtt"),
	}
	if config.MaximumMessageExpiryInterval <= 0 {
		config.MaximumMessageExpiryInterval = kDefaultMaximumMessageExpiryInterval
	}
	opts.Capabilities.MaximumMessageExpiryInterval = config.MaximumMessageExpiryInterval

	if config.MaximumSessionExpiryInterval <= 0 {
		config.MaximumSessionExpiryInterval = kDefaultMaximumSessionExpiryInterval
	}
	opts.Capabilities.MaximumSessionExpiryInterval = config.MaximumSessionExpiryInterval

	persister := &persister{
		ctx:           ctx,
		metadataStore: metadataStore,
		config:        config,
	}

	server := &Server{
		ctx:       ctx,
		config:    config,
		delegate:  delegate,
		host:      host,
		broker:    mochi.New(&opts),
		persister: persister,
	}

	// Topic renaming:
	if err := server.broker.AddHook(newTopicRenameHook(server), nil); err != nil {
		return nil, err
	}

	// Authentication:
	if err := server.broker.AddHook(newAuthHook(server), nil); err != nil {
		return nil, err
	}

	// Publishing:
	if publishHook, err := newPublishHook(server); err != nil {
		return nil, err
	} else if err := server.broker.AddHook(publishHook, nil); err != nil {
		return nil, err
	}

	// Persistence:
	if metadataStore != nil {
		if persistHook, err := newPersistHook(persister); err != nil {
			return nil, err
		} else if err := server.broker.AddHook(persistHook, nil); err != nil {
			return nil, err
		}
	}

	// Create a TCP listener.
	listener := listeners.NewTCP(listeners.Config{
		ID:        "SyncGateway",
		Address:   config.PublicInterface,
		TLSConfig: tlsConfig,
	})
	if err := server.broker.AddListener(listener); err != nil {
		return nil, err
	}
	return server, nil
}

// Starts the MQTT server.
func (server *Server) Start() error {
	var err error
	server.clusterAgent, err = startClusterAgent(server)
	if err != nil {
		base.ErrorfCtx(server.ctx, "MQTT: error joining cluster: %v", err)
		return fmt.Errorf("error joining MQTT cluster: %w", err)
	}
	if err := server.broker.Serve(); err != nil {
		base.ErrorfCtx(server.ctx, "MQTT: starting MQTT broker: %v", err)
		server.Stop()
		return fmt.Errorf("error starting MQTT broker: %w", err)
	}
	return nil
}

// Stops the MQTT server.
func (server *Server) Stop() error {
	if server.clusterAgent != nil {
		server.clusterAgent.stop()
		server.clusterAgent = nil
	}
	err := server.broker.Close()
	server.broker = nil
	return err
}

// Splits the Client's MQTT username into database name and username at a '/'.
// If there is no '/', both will be an empty string.
func (server *Server) clientDatabaseName(client *mochi.Client) (dbName string, username string) {
	dbName, username, ok := strings.Cut(string(client.Properties.Username), "/")
	if !ok {
		dbName = ""
		username = ""
	}
	return
}

// Strips the database name and a '/' from an internal topic name.
// Returns the remaining topic name, and true on success or false if it didn't have the right prefix.
func stripDbNameFromTopic(dbc *db.DatabaseContext, topicName string) (string, bool) {
	prefix := dbc.Name + "/"
	if strings.HasPrefix(topicName, prefix) {
		return topicName[len(prefix):], true
	} else {
		return topicName, false
	}
}

// Looks up the DatabaseContext and username for a Client.
// DOES NOT authenticate! That's done by authHook.OnConnectAuthenticate.)
func (server *Server) clientDatabaseContext(client *mochi.Client) (*db.DatabaseContext, string) {
	dbName, username := server.clientDatabaseName(client)
	if dbName == "" || username == "" {
		return nil, "" // wrong format
	}
	dbc := server.delegate.Database(server.ctx, dbName)
	if dbcSettings(dbc) == nil {
		dbc = nil
	}
	return dbc, username
}

// Given a DatabaseContext, returns its MQTT BrokerConfig, if it exists and is enabled.
func dbcSettings(dbc *db.DatabaseContext) *BrokerConfig {
	if dbc == nil {
		return nil
	} else if opts := dbc.Options.MQTT; opts.IsEnabled() {
		if broker := opts.(*PerDBConfig).Broker; broker.IsEnabled() {
			return broker
		}
	}
	return nil
}

//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"context"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/mqtt"
)

// Starts MQTT server. Called by StartServer.
func (sc *ServerContext) StartMQTTServer(ctx context.Context, config *mqtt.ServerConfig, httpsConfig *HTTPSConfig) error {
	if !config.IsEnabled() {
		return nil
	}

	base.ConsolefCtx(ctx, base.LevelInfo, base.KeyAll, "Starting MQTT server on %s", config.PublicInterface)
	// TLS config:
	tlsMinVersion := GetTLSVersionFromString(&httpsConfig.TLSMinimumVersion)
	tlsConfig, err := base.MakeTLSConfig(httpsConfig.TLSCertPath, httpsConfig.TLSKeyPath, tlsMinVersion)
	if err != nil {
		return err
	}

	// Metadata store:
	var metadataStore sgbucket.DataStore
	if config.MetadataDB != nil {
		dbName, dsName, err := config.ParseMetadataStore()
		if err != nil {
			return err
		}
		db := sc.Database(ctx, dbName)
		if db == nil {
			return fmt.Errorf("MQTT: invalid metadata store: database %q does not exist", dbName)
		}
		metadataStore, err = db.Bucket.NamedDataStore(dsName)
		if err != nil {
			return fmt.Errorf("MQTT: invalid metadata store: %w", err)
		}
	}

	server, err := mqtt.NewServer(ctx, config, tlsConfig, metadataStore, sc)
	if err != nil {
		return err
	}
	sc.mqttServer = server
	go server.Start()
	return nil
}

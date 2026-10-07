// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

// RuntimeDatabaseConfig is the non-persisted database config that has the persisted DatabaseConfig embedded
type RuntimeDatabaseConfig struct {
	DatabaseConfig
}

// DatabaseConfig is a 3.x/persisted database config that represents a config stored in the bucket.
type DatabaseConfig struct {
	// cas is the Couchbase Server CAS for the last mutation of the database config in the bucket
	// used to skip applying configs to SG nodes that already have an up-to-date config.
	// This value can be explicitly set to 0 before applyConfig to force reload.
	cfgCas uint64

	// Version is a generated Rev ID used for optimistic concurrency control using ETags/If-Match headers.
	Version string `json:"version,omitempty"`

	// SGVersion is a base.ComparableBuildVersion of the Sync Gateway node that wrote the config.
	SGVersion string `json:"sg_version,omitempty"`

	// MetadataID is the prefix used to store database metadata
	MetadataID string `json:"metadata_id"`

	// ImportVersion is included in the prefix used to store import checkpoints.
	// Incremented when collections are added to a db, to trigger import of existing data in those collections.
	ImportVersion uint64 `json:"import_version,omitempty"`

	// DbConfig embeds database config properties
	DbConfig
}

func (dbc *DatabaseConfig) Redacted(ctx context.Context) (*DatabaseConfig, error) {
	var config DatabaseConfig

	err := base.DeepCopyInefficient(&config, dbc)
	if err != nil {
		return nil, err
	}

	err = config.DbConfig.redactInPlace(ctx)
	if err != nil {
		return nil, err
	}

	if config.Guest != nil && config.Guest.Password != nil && *config.Guest.Password != "" {
		config.Guest.Password = new(base.RedactedStr)
	}

	return &config, nil
}

func (dbc *DatabaseConfig) GetCollectionNames() base.ScopeAndCollectionNames {
	collections := make(base.ScopeAndCollectionNames, 0)
	for scopeName, scopeConfig := range dbc.Scopes {
		for collectionName, _ := range scopeConfig.Collections {
			collections = append(collections, base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName})
		}
	}
	return collections
}

func GenerateDatabaseConfigVersionID(ctx context.Context, previousRevID string, dbConfig *DbConfig) (string, error) {
	encodedBody, err := base.JSONMarshalCanonical(dbConfig)
	if err != nil {
		return "", err
	}

	previousGen, previousRev := db.ParseRevID(ctx, previousRevID)
	generation := previousGen + 1

	hash := db.CreateRevIDWithBytes(generation, previousRev, encodedBody)
	return hash, nil
}

func DefaultPerDBLogging(bootstrapLoggingCnf base.LoggingConfig) *DbLoggingConfig {
	dblc := &DbLoggingConfig{}
	if bootstrapLoggingCnf.Console != nil {
		if *bootstrapLoggingCnf.Console.Enabled {
			dblc.Console = &DbConsoleLoggingConfig{
				LogLevel: bootstrapLoggingCnf.Console.LogLevel,
				LogKeys:  bootstrapLoggingCnf.Console.LogKeys,
			}
		}
	}
	dblc.Audit = &DbAuditLoggingConfig{
		Enabled:       new(base.DefaultDbAuditEnabled),
		EnabledEvents: &base.DefaultDbAuditEventIDs,
	}
	return dblc
}

// MergeDatabaseConfigWithDefaults merges the passed in config onto a DefaultDbConfig which results in returned value
// being populated with defaults when not set
func MergeDatabaseConfigWithDefaults(sc *StartupConfig, dbConfig *DbConfig) (*DbConfig, error) {
	defaultDbConfig := DefaultDbConfig(sc)

	err := base.ConfigMerge(defaultDbConfig, dbConfig)
	if err != nil {
		return nil, err
	}

	return defaultDbConfig, nil
}

// DefaultDbConfig provides a DbConfig with all the default values populated. Used with MergeDatabaseConfigWithDefaults
// to provide defaults to  include_runtime config endpoints.
// Note that this does not include unsupported options
func DefaultDbConfig(sc *StartupConfig) *DbConfig {
	dbConfig := DbConfig{
		BucketConfig:       BucketConfig{},
		Name:               "",
		Sync:               new(channels.DocChannelsSyncFunction),
		Users:              nil,
		Roles:              nil,
		RevsLimit:          nil, // Set this below struct
		ImportFilter:       nil,
		ImportBackupOldRev: new(false),
		EventHandlers:      nil,
		FeedType:           "",
		AllowEmptyPassword: new(false),
		CacheConfig: &CacheConfig{
			RevCacheConfig: &RevCacheConfig{
				MaxItemCount: new(db.DefaultRevisionCacheSize),
				ShardCount:   new(db.DefaultRevisionCacheShardCount),
			},
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxNumber:            new(db.DefaultChannelCacheMaxNumber),
				HighWatermarkPercent: new(db.DefaultCompactHighWatermarkPercent),
				LowWatermarkPercent:  new(db.DefaultCompactLowWatermarkPercent),
				MaxWaitPending:       new(uint32(db.DefaultCachePendingSeqMaxWait.Milliseconds())),
				MaxNumPending:        new(db.DefaultCachePendingSeqMaxNum),
				MaxWaitSkipped:       new(uint32(db.DefaultSkippedSeqMaxWait.Milliseconds())),
				EnableStarChannel:    new(true),
				MaxLength:            new(db.DefaultChannelCacheMaxLength),
				MinLength:            new(db.DefaultChannelCacheMinLength),
				ExpirySeconds:        new(int(db.DefaultChannelCacheAge.Seconds())),
			},
		},
		StartOffline:          new(false),
		OIDCConfig:            nil,
		OldRevExpirySeconds:   new(base.DefaultOldRevExpirySeconds),
		ViewQueryTimeoutSecs:  new(uint32(base.DefaultViewTimeout.Seconds())),
		LocalDocExpirySecs:    new(base.DefaultLocalDocExpirySecs),
		EnableXattrs:          new(base.DefaultUseXattrs),
		SecureCookieOverride:  new(sc.API.HTTPS.TLSCertPath != ""),
		SessionCookieName:     auth.DefaultCookieName,
		SessionCookieHTTPOnly: new(false),
		AllowConflicts:        new(base.DefaultAllowConflicts),
		Index: &IndexConfig{
			NumReplicas:   new(DefaultNumIndexReplicas),
			NumPartitions: new(db.DefaultNumIndexPartitions),
		},
		UseViews:                    new(false),
		SendWWWAuthenticateHeader:   new(true),
		DisablePasswordAuth:         new(false),
		BucketOpTimeoutMs:           new(uint32(base.DefaultGocbV2OperationTimeout.Milliseconds())),
		SlowQueryWarningThresholdMs: new(kDefaultSlowQueryWarningThreshold),
		DeltaSync: &DeltaSyncConfig{
			Enabled:          new(db.DefaultDeltaSyncEnabled),
			RevMaxAgeSeconds: new(db.DefaultDeltaSyncRevMaxAge),
		},
		StoreLegacyRevTreeData:            new(db.DefaultStoreLegacyRevTreeData),
		CompactIntervalDays:               new(float32(db.DefaultCompactInterval.Hours() / 24)),
		SGReplicateEnabled:                new(db.DefaultSGReplicateEnabled),
		SGReplicateWebsocketPingInterval:  new(int(db.DefaultSGReplicateWebsocketPingInterval.Seconds())),
		Replications:                      nil,
		ServeInsecureAttachmentTypes:      new(false),
		QueryPaginationLimit:              new(db.DefaultQueryPaginationLimit),
		UserXattrKey:                      nil,
		ClientPartitionWindowSecs:         new(int(base.DefaultClientPartitionWindow.Seconds())),
		Guest:                             &auth.PrincipalConfig{Disabled: new(true)},
		JavascriptTimeoutSecs:             new(base.DefaultJavascriptTimeoutSecs),
		ChangesRequestPlus:                new(false),
		Logging:                           DefaultPerDBLogging(sc.Logging),
		DisablePublicAllDocs:              new(false),
		UseSystemMobileMetadataCollection: new(DefaultUseSystemMetadataCollection),
		AutoImport:                        new(base.DefaultAutoImport),
	}

	if base.IsEnterpriseEdition() {
		dbConfig.ImportPartitions = new(uint16(base.DefaultImportPartitions))
	} else {
		dbConfig.ImportPartitions = nil
	}

	revsLimit := db.DefaultRevsLimitNoConflicts
	if dbConfig.AllowConflicts != nil && *dbConfig.AllowConflicts {
		revsLimit = db.DefaultRevsLimitConflicts
	}
	dbConfig.RevsLimit = new(uint32(revsLimit))

	return &dbConfig
}

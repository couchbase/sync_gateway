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
	isSuspended bool
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
		config.Guest.Password = base.Ptr(base.RedactedStr)
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
		Enabled:       base.Ptr(base.DefaultDbAuditEnabled),
		EnabledEvents: &base.DefaultDbAuditEventIDs,
	}
	return dblc
}

// MergeDatabaseConfigWithDefaults merges the passed in config onto a DefaultDbConfig which results in returned value
// being populated with defaults when not set
func MergeDatabaseConfigWithDefaults(sc *StartupConfig, dbConfig *DbConfig) (*DbConfig, error) {
	defaultDbConfig := DefaultDbConfig(sc, dbConfig.UseXattrs())

	err := base.ConfigMerge(defaultDbConfig, dbConfig)
	if err != nil {
		return nil, err
	}

	return defaultDbConfig, nil
}

// DefaultDbConfig provides a DbConfig with all the default values populated. Used with MergeDatabaseConfigWithDefaults
// to provide defaults to  include_runtime config endpoints.
// Note that this does not include unsupported options
func DefaultDbConfig(sc *StartupConfig, useXattrs bool) *DbConfig {
	dbConfig := DbConfig{
		BucketConfig:       BucketConfig{},
		Name:               "",
		Sync:               base.Ptr(channels.DocChannelsSyncFunction),
		Users:              nil,
		Roles:              nil,
		RevsLimit:          nil, // Set this below struct
		ImportFilter:       nil,
		ImportBackupOldRev: base.Ptr(false),
		EventHandlers:      nil,
		FeedType:           "",
		AllowEmptyPassword: base.Ptr(false),
		CacheConfig: &CacheConfig{
			RevCacheConfig: &RevCacheConfig{
				MaxItemCount: base.Ptr(db.DefaultRevisionCacheSize),
				ShardCount:   base.Ptr(db.DefaultRevisionCacheShardCount),
			},
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxNumber:            base.Ptr(db.DefaultChannelCacheMaxNumber),
				HighWatermarkPercent: base.Ptr(db.DefaultCompactHighWatermarkPercent),
				LowWatermarkPercent:  base.Ptr(db.DefaultCompactLowWatermarkPercent),
				MaxWaitPending:       base.Ptr(uint32(db.DefaultCachePendingSeqMaxWait.Milliseconds())),
				MaxNumPending:        base.Ptr(db.DefaultCachePendingSeqMaxNum),
				MaxWaitSkipped:       base.Ptr(uint32(db.DefaultSkippedSeqMaxWait.Milliseconds())),
				EnableStarChannel:    base.Ptr(true),
				MaxLength:            base.Ptr(db.DefaultChannelCacheMaxLength),
				MinLength:            base.Ptr(db.DefaultChannelCacheMinLength),
				ExpirySeconds:        base.Ptr(int(db.DefaultChannelCacheAge.Seconds())),
			},
		},
		StartOffline:          base.Ptr(false),
		OIDCConfig:            nil,
		OldRevExpirySeconds:   base.Ptr(base.DefaultOldRevExpirySeconds),
		ViewQueryTimeoutSecs:  base.Ptr(uint32(base.DefaultViewTimeout.Seconds())),
		LocalDocExpirySecs:    base.Ptr(base.DefaultLocalDocExpirySecs),
		EnableXattrs:          base.Ptr(base.DefaultUseXattrs),
		SecureCookieOverride:  base.Ptr(sc.API.HTTPS.TLSCertPath != ""),
		SessionCookieName:     auth.DefaultCookieName,
		SessionCookieHTTPOnly: base.Ptr(false),
		AllowConflicts:        base.Ptr(base.DefaultAllowConflicts),
		Index: &IndexConfig{
			NumReplicas: base.Ptr(DefaultNumIndexReplicas),
		},
		UseViews:                    base.Ptr(false),
		SendWWWAuthenticateHeader:   base.Ptr(true),
		DisablePasswordAuth:         base.Ptr(false),
		BucketOpTimeoutMs:           base.Ptr(uint32(base.DefaultGocbV2OperationTimeout.Milliseconds())),
		SlowQueryWarningThresholdMs: base.Ptr(kDefaultSlowQueryWarningThreshold),
		DeltaSync: &DeltaSyncConfig{
			Enabled:          base.Ptr(db.DefaultDeltaSyncEnabled),
			RevMaxAgeSeconds: base.Ptr(db.DefaultDeltaSyncRevMaxAge),
		},
		StoreLegacyRevTreeData:           base.Ptr(db.DefaultStoreLegacyRevTreeData),
		CompactIntervalDays:              base.Ptr(float32(db.DefaultCompactInterval.Hours() / 24)),
		SGReplicateEnabled:               base.Ptr(db.DefaultSGReplicateEnabled),
		SGReplicateWebsocketPingInterval: base.Ptr(int(db.DefaultSGReplicateWebsocketPingInterval.Seconds())),
		Replications:                     nil,
		ServeInsecureAttachmentTypes:     base.Ptr(false),
		QueryPaginationLimit:             base.Ptr(db.DefaultQueryPaginationLimit),
		UserXattrKey:                     nil,
		ClientPartitionWindowSecs:        base.Ptr(int(base.DefaultClientPartitionWindow.Seconds())),
		Guest:                            &auth.PrincipalConfig{Disabled: base.Ptr(true)},
		JavascriptTimeoutSecs:            base.Ptr(base.DefaultJavascriptTimeoutSecs),
		Suspendable:                      base.Ptr(sc.IsServerless()),
		ChangesRequestPlus:               base.Ptr(false),
		Logging:                          DefaultPerDBLogging(sc.Logging),
		DisablePublicAllDocs:             base.Ptr(false),
	}

	if useXattrs {
		dbConfig.AutoImport = base.Ptr(base.DefaultAutoImport)
		if base.IsEnterpriseEdition() {
			dbConfig.ImportPartitions = base.Ptr(base.GetDefaultImportPartitions(sc.IsServerless()))
		} else {
			dbConfig.ImportPartitions = base.Ptr(uint16(0))
		}
		dbConfig.Index.NumPartitions = base.Ptr(db.DefaultNumIndexPartitions)
	} else {
		dbConfig.AutoImport = base.Ptr(false)
	}

	revsLimit := db.DefaultRevsLimitNoConflicts
	if dbConfig.AllowConflicts != nil && *dbConfig.AllowConflicts {
		revsLimit = db.DefaultRevsLimitConflicts
	}
	dbConfig.RevsLimit = base.Ptr(uint32(revsLimit))

	return &dbConfig
}

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

	// SGVersion is a base.ComparableVersion of the Sync Gateway node that wrote the config.
	SGVersion string `json:"sg_version,omitempty"`

	// MetadataID is the prefix used to store database metadata
	MetadataID string `json:"metadata_id"`

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
		config.Guest.Password = base.StringPtr(base.RedactedStr)
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
		Sync:               base.StringPtr(channels.DocChannelsSyncFunction),
		Users:              nil,
		Roles:              nil,
		RevsLimit:          nil, // Set this below struct
		AutoImport:         base.BoolPtr(base.DefaultAutoImport),
		ImportPartitions:   base.Uint16Ptr(base.GetDefaultImportPartitions(sc.IsServerless())),
		ImportFilter:       nil,
		ImportBackupOldRev: base.BoolPtr(false),
		EventHandlers:      nil,
		FeedType:           "",
		AllowEmptyPassword: base.BoolPtr(false),
		CacheConfig: &CacheConfig{
			RevCacheConfig: &RevCacheConfig{
				Size:       base.Uint32Ptr(db.DefaultRevisionCacheSize),
				ShardCount: base.Uint16Ptr(db.DefaultRevisionCacheShardCount),
			},
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxNumber:            base.IntPtr(db.DefaultChannelCacheMaxNumber),
				HighWatermarkPercent: base.IntPtr(db.DefaultCompactHighWatermarkPercent),
				LowWatermarkPercent:  base.IntPtr(db.DefaultCompactLowWatermarkPercent),
				MaxWaitPending:       base.Uint32Ptr(uint32(db.DefaultCachePendingSeqMaxWait.Milliseconds())),
				MaxNumPending:        base.IntPtr(db.DefaultCachePendingSeqMaxNum),
				MaxWaitSkipped:       base.Uint32Ptr(uint32(db.DefaultSkippedSeqMaxWait.Milliseconds())),
				EnableStarChannel:    base.BoolPtr(false),
				MaxLength:            base.IntPtr(db.DefaultChannelCacheMaxLength),
				MinLength:            base.IntPtr(db.DefaultChannelCacheMinLength),
				ExpirySeconds:        base.IntPtr(int(db.DefaultChannelCacheAge.Seconds())),
			},
		},
		StartOffline:                base.BoolPtr(false),
		OIDCConfig:                  nil,
		OldRevExpirySeconds:         base.Uint32Ptr(base.DefaultOldRevExpirySeconds),
		ViewQueryTimeoutSecs:        base.Uint32Ptr(base.DefaultViewTimeoutSecs),
		LocalDocExpirySecs:          base.Uint32Ptr(base.DefaultLocalDocExpirySecs),
		EnableXattrs:                base.BoolPtr(base.DefaultUseXattrs),
		SecureCookieOverride:        base.BoolPtr(sc.API.HTTPS.TLSCertPath != ""),
		SessionCookieName:           "",
		SessionCookieHTTPOnly:       base.BoolPtr(false),
		AllowConflicts:              base.BoolPtr(base.DefaultAllowConflicts),
		NumIndexReplicas:            base.UintPtr(DefaultNumIndexReplicas),
		UseViews:                    base.BoolPtr(false),
		SendWWWAuthenticateHeader:   base.BoolPtr(true),
		BucketOpTimeoutMs:           nil,
		SlowQueryWarningThresholdMs: base.Uint32Ptr(kDefaultSlowQueryWarningThreshold),
		DeltaSync: &DeltaSyncConfig{
			Enabled:          base.BoolPtr(db.DefaultDeltaSyncEnabled),
			RevMaxAgeSeconds: base.Uint32Ptr(db.DefaultDeltaSyncRevMaxAge),
		},
		CompactIntervalDays:              base.Float32Ptr(float32(db.DefaultCompactInterval)),
		SGReplicateEnabled:               base.BoolPtr(db.DefaultSGReplicateEnabled),
		SGReplicateWebsocketPingInterval: base.IntPtr(int(db.DefaultSGReplicateWebsocketPingInterval.Seconds())),
		Replications:                     nil,
		ServeInsecureAttachmentTypes:     base.BoolPtr(false),
		QueryPaginationLimit:             base.IntPtr(db.DefaultQueryPaginationLimit),
		UserXattrKey:                     "",
		ClientPartitionWindowSecs:        base.IntPtr(int(base.DefaultClientPartitionWindow.Seconds())),
		JavascriptTimeoutSecs:            base.Uint32Ptr(base.DefaultJavascriptTimeoutSecs),
		Suspendable:                      base.BoolPtr(sc.IsServerless()),
	}

	revsLimit := db.DefaultRevsLimitNoConflicts
	if dbConfig.AllowConflicts != nil && *dbConfig.AllowConflicts {
		revsLimit = db.DefaultRevsLimitConflicts
	}
	dbConfig.RevsLimit = base.Uint32Ptr(uint32(revsLimit))

	return &dbConfig
}

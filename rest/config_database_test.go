// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultDbConfig(t *testing.T) {
	useXattrs := true
	sc := DefaultStartupConfig("")
	compactIntervalDays := *(DefaultDbConfig(&sc, useXattrs).CompactIntervalDays)
	require.Equal(t, db.DefaultCompactInterval, time.Duration(compactIntervalDays)*time.Hour*24)
}

// TestDefaultDbConfigFieldCoverage ensures that all fields in DbConfig are either:
// 1. Set to a non-zero value in DefaultDbConfig(), or
// 2. Explicitly listed in the allowlist as intentionally unset
//
// This prevents new fields from being added to DbConfig without considering whether
// they should have a default value exposed in include_runtime=true config output.
func TestDefaultDbConfigFieldCoverage(t *testing.T) {
	sc := DefaultStartupConfig("/default/log/file/path")
	defaultConfig := DefaultDbConfig(&sc, true)

	// Fields that are intentionally left unset in DefaultDbConfig.
	// When adding a field here, add a comment explaining why it doesn't need a default.
	intentionallyUnsetFields := map[string]string{
		// Embedded struct - fields checked separately
		"BucketConfig":           "embedded struct, fields checked separately",
		"Scopes":                 "opt-in with no default",
		"Replications":           "opt-in with no default",
		"UserFunctions":          "opt-in with no default",
		"OIDCConfig":             "opt-in with no default",
		"LocalJWTConfig":         "opt-in with no default",
		"CORS":                   "opt-in with no default",
		"EventHandlers":          "opt-in with no default",
		"ImportFilter":           "opt-in with no default",
		"UserXattrKey":           "opt-in with no default",
		"Name":                   "stamped at runtime",
		"UpdatedAt":              "persisted timestamp",
		"CreatedAt":              "persisted timestamp",
		"Unsupported":            "unsupported",
		"DeprecatedRevCacheSize": "deprecated",
		"NumIndexReplicas":       "deprecated",
		"FeedType":               "deprecated",
		"Users":                  "legacy config",
		"Roles":                  "legacy config",
	}

	configType := reflect.TypeFor[DbConfig]()
	configValue := reflect.ValueOf(*defaultConfig)

	for i := 0; i < configType.NumField(); i++ {
		field := configType.Field(i)
		fieldName := field.Name
		fieldValue := configValue.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			t.Logf("%q: Skipping unexported field", fieldName)
			continue
		}

		// Check if field is in the allowlist
		if reason, ok := intentionallyUnsetFields[fieldName]; ok {
			if testing.Verbose() {
				t.Logf("%q: intentionally unset: %s", fieldName, reason)
			}
			continue
		}

		// Check if the field has a non-zero value
		if fieldValue.IsZero() {
			assert.Failf(t, "Field not present in DefaultDbConfig", "%q: missing - either add a default value in DefaultDbConfig() or add it to the allowlist with a reason", fieldName)
		}
	}
}

func TestDbConfigUpdatedAtField(t *testing.T) {
	b := base.GetTestBucket(t)
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: b,
		PersistentConfig: true,
	})
	defer rt.Close()
	ctx := base.TestCtx(t)

	dbConfig := rt.NewDbConfig()
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	sc := rt.ServerContext()

	resp := rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)
	var unmarshaledConfig DbConfig
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &unmarshaledConfig))

	registry := &GatewayRegistry{}
	bName := b.GetName()
	_, err := sc.BootstrapContext.Connection.GetMetadataDocument(ctx, bName, base.SGRegistryKey, registry)
	require.NoError(t, err)

	// Check that the config has an updatedAt field
	require.NotNil(t, unmarshaledConfig.UpdatedAt)
	require.NotNil(t, unmarshaledConfig.CreatedAt)
	currUpdatedTime := unmarshaledConfig.UpdatedAt
	currCreatedTime := unmarshaledConfig.CreatedAt
	registryUpdated := registry.UpdatedAt
	registryCreated := registry.CreatedAt

	// avoid flake where update at seems to be the same (possibly running to fast)
	time.Sleep(500 * time.Nanosecond)

	// Update the config
	dbConfig = rt.NewDbConfig()
	RequireStatus(t, rt.UpsertDbConfig("db1", dbConfig), http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)
	unmarshaledConfig = DbConfig{}
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &unmarshaledConfig))

	registry = &GatewayRegistry{}
	_, err = sc.BootstrapContext.Connection.GetMetadataDocument(ctx, b.GetName(), base.SGRegistryKey, registry)
	require.NoError(t, err)

	// asser that the db config timestamps are as expected
	assert.Greater(t, unmarshaledConfig.UpdatedAt.UnixNano(), currUpdatedTime.UnixNano())
	assert.Equal(t, unmarshaledConfig.CreatedAt.UnixNano(), currCreatedTime.UnixNano())
	// assert that registry timestamps are as expected
	assert.Equal(t, registry.CreatedAt.UnixNano(), registryCreated.UnixNano())
	assert.Greater(t, registry.UpdatedAt.UnixNano(), registryUpdated.UnixNano())
}

func TestConfigToBucketPointName(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Need cbs bucket for this test")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTPResp, base.KeyHTTP)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	testBucketName := base.CreateTestBucketName(fmt.Sprintf(".%d", time.Now().Unix()))

	// create db config to point to bucket with . in the name
	dbConfig := rt.NewDbConfig()
	dbConfig.Bucket = base.Ptr(testBucketName)
	dbConfig.Username = base.TestClusterUsername()
	dbConfig.Password = base.TestClusterPassword()
	dbConfig.Scopes = nil

	// create bucket with . in the name
	v2Bucket, err := base.AsGocbV2Bucket(rt.TestBucket)
	require.NoError(t, err)
	cluster := v2Bucket.GetCluster()
	settings := gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:       testBucketName,
			RAMQuotaMB: uint64(256),
			BucketType: gocb.CouchbaseBucketType,
		},
	}
	require.NoError(t, v2Bucket.GetCluster().Buckets().CreateBucket(settings, nil))
	// cleanup this bucket
	defer func() {
		require.NoError(t, v2Bucket.GetCluster().Buckets().DropBucket(testBucketName, nil))
	}()
	// wait till bucket is ready
	bucket := cluster.Bucket(testBucketName)
	require.NoError(t, bucket.WaitUntilReady(10*time.Second, nil))

	// create db pointing to bucket with . in it
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	// assert that we can create a user (pre CBG-4512 access query fails)
	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_user/", `{"name":"user1", "password":"password", "admin_channels":["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)
}

func TestDatabaseConfigValidation(t *testing.T) {
	testCases := []struct {
		name          string
		dbConfig      DbConfig
		numReplicas   uint
		expectedError string
	}{
		{
			name: "Empty index",
			dbConfig: DbConfig{
				Name:  "db",
				Index: &IndexConfig{},
			},
			numReplicas:   1,
			expectedError: "",
		},
		{
			name: "valid num_replicas, legacy",
			dbConfig: DbConfig{
				Name:             "db",
				NumIndexReplicas: base.Ptr(uint(2)),
			},
			numReplicas: 2,
		},
		{
			name: "valid num_replicas, new",
			dbConfig: DbConfig{
				Name: "db",
				Index: &IndexConfig{
					NumReplicas: base.Ptr(uint(2)),
				},
			},
			numReplicas: 2,
		},
		{
			name: "duplicate index replica definitions, same value",
			dbConfig: DbConfig{
				Name:             "db",
				NumIndexReplicas: base.Ptr(uint(2)),
				Index: &IndexConfig{
					NumReplicas: base.Ptr(uint(2)),
				},
			},
			expectedError: "mutually exclusive",
		},
		{
			name: "duplicate index replica definitions, diff value",
			dbConfig: DbConfig{
				Name:             "db",
				NumIndexReplicas: base.Ptr(uint(3)),
				Index: &IndexConfig{
					NumReplicas: base.Ptr(uint(2)),
				},
			},
			expectedError: "mutually exclusive",
		},
		{
			name: "explicit 0 partitions",
			dbConfig: DbConfig{
				Name: "db",
				Index: &IndexConfig{
					NumPartitions: base.Ptr(uint32(0)),
				},
			},
			expectedError: "num_partitions must be greater than 0",
		},
		{
			name: "partitions with xattrs=false",
			dbConfig: DbConfig{
				Name: "db",
				Index: &IndexConfig{
					NumPartitions: base.Ptr(uint32(2)),
				},
				EnableXattrs: base.Ptr(false),
			},
			expectedError: "incompatible with enable_shared_bucket_access=false",
		},
		{
			name: "allowing conflicts with allow_conflicts=true",
			dbConfig: DbConfig{
				Name:           "db",
				AllowConflicts: base.Ptr(true),
			},
			expectedError: "allow_conflicts cannot be set to true",
		},
		{
			name: "setting enable_star_channel to false",
			dbConfig: DbConfig{
				Name: "db",
				CacheConfig: &CacheConfig{
					ChannelCacheConfig: &ChannelCacheConfig{
						EnableStarChannel: base.Ptr(false),
					},
				},
			},
			expectedError: "enable_star_channel cannot be set to false",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			validateOIDC := false
			validateReplications := true
			err := tc.dbConfig.validate(ctx, validateOIDC, validateReplications)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.numReplicas, tc.dbConfig.numIndexReplicas())
		})
	}
}

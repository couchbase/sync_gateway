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
	"net/http"
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// create db config to point to bucket with . in the name
	dbConfig := rt.NewDbConfig()
	dbConfig.Bucket = base.StringPtr("my.Bucket")
	dbConfig.Username = base.TestClusterUsername()
	dbConfig.Password = base.TestClusterPassword()
	dbConfig.Scopes = nil

	// create bucket with . in the name
	v2Bucket, err := base.AsGocbV2Bucket(rt.TestBucket)
	require.NoError(t, err)
	cluster := v2Bucket.GetCluster()
	settings := gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:       "my.Bucket",
			RAMQuotaMB: uint64(256),
			BucketType: gocb.CouchbaseBucketType,
		},
	}
	require.NoError(t, v2Bucket.GetCluster().Buckets().CreateBucket(settings, nil))
	// cleanup this bucket
	defer func() {
		require.NoError(t, v2Bucket.GetCluster().Buckets().DropBucket("my.Bucket", nil))
	}()
	// wait till bucket is ready
	bucket := cluster.Bucket("my.Bucket")
	require.NoError(t, bucket.WaitUntilReady(10*time.Second, nil))

	// create db pointing to bucket with . in it
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	// assert that we can create a user (pre CBG-4512 access query fails)
	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_user/", `{"name":"user1", "password":"password", "admin_channels":["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)
}

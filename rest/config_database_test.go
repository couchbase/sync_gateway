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

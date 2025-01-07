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
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	resp := rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)
	var unmarshaledConfig DbConfig
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &unmarshaledConfig))

	// Check that the config has an updatedAt field
	require.NotNil(t, unmarshaledConfig.UpdatedAt)
	currTime := unmarshaledConfig.UpdatedAt

	// avoid flake where update at seems to be the same (possibly running to fast)
	time.Sleep(500 * time.Nanosecond)

	// Update the config
	dbConfig = rt.NewDbConfig()
	RequireStatus(t, rt.UpsertDbConfig("db1", dbConfig), http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)
	unmarshaledConfig = DbConfig{}
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &unmarshaledConfig))

	assert.Greater(t, unmarshaledConfig.UpdatedAt.UnixNano(), currTime.UnixNano())
}

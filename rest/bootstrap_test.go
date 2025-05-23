// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBootstrapRESTAPISetup will bootstrap against a cluster with no databases,
// and will use the SG REST API to create and persist a database in the cluster.
// Then Sync Gateway restarts to ensure that a subsequent bootstrap picks up the
// database created in the first step.
func TestBootstrapRESTAPISetup(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	config := BootstrapStartupConfigForTest(t) // share config between both servers in test to share a groupID
	sc, closeFn := StartServerWithConfig(t, &config)

	ctx := base.TestCtx(t)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	resp := BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// upsert 1 config field
	resp = BootstrapAdminRequest(t, sc, http.MethodPost, "/db1/_config",
		`{"cache": {"rev_cache":{"size":1234}}}`,
	)
	resp.RequireStatus(http.StatusCreated)

	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/db1/", ``)
	resp.RequireStatus(http.StatusOK)
	var dbRootResp DatabaseRoot
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbRootResp))
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect the config
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/db1/_config", ``)
	resp.RequireStatus(http.StatusOK)
	var dbConfigResp DatabaseConfig
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbConfigResp))
	assert.Equal(t, "db1", dbConfigResp.Name)
	require.NotNil(t, dbConfigResp.Bucket)
	assert.Equal(t, tb.GetName(), *dbConfigResp.Bucket)
	assert.Nil(t, dbConfigResp.Server)
	assert.Empty(t, dbConfigResp.Username)
	assert.Empty(t, dbConfigResp.Password)
	require.Nil(t, dbConfigResp.Sync)
	require.Equal(t, uint32(1234), *dbConfigResp.CacheConfig.RevCacheConfig.MaxItemCount)

	// Sanity check to use the database
	resp = BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/doc1", `{"foo":"bar"}`)
	resp.RequireResponse(http.StatusCreated, `{"id":"doc1","ok":true,"rev":"1-cd809becc169215072fd567eebd8b8de"}`)
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/db1/doc1", ``)
	resp.RequireResponse(http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)

	// Restart Sync Gateway
	closeFn()

	sc, closeFn = StartServerWithConfig(t, &config)
	defer closeFn()

	// Ensure the database was bootstrapped on startup
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/db1/", ``)
	resp.RequireStatus(http.StatusOK)
	dbRootResp = DatabaseRoot{}
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbRootResp))
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect config again, and ensure no changes since bootstrap
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/db1/_config", ``)
	resp.RequireStatus(http.StatusOK)
	dbConfigResp = DatabaseConfig{}
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbConfigResp))
	assert.Equal(t, "db1", dbConfigResp.Name)
	require.NotNil(t, dbConfigResp.Bucket)
	assert.Equal(t, tb.GetName(), *dbConfigResp.Bucket)
	assert.Nil(t, dbConfigResp.Server)
	assert.Empty(t, dbConfigResp.Username)
	assert.Empty(t, dbConfigResp.Password)
	require.Nil(t, dbConfigResp.Sync)
	require.Equal(t, uint32(1234), *dbConfigResp.CacheConfig.RevCacheConfig.MaxItemCount)

	// Ensure it's _actually_ the same bucket
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/db1/doc1", ``)
	resp.RequireResponse(http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)
}

// TestBootstrapDuplicateBucket will attempt to create two databases sharing the same collections and ensure this isn't allowed.
func TestBootstrapDuplicateCollections(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	ctx := base.TestCtx(t)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	resp := BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Create db2 using the same collection (on the same bucket) and expect it to fail
	resp = BootstrapAdminRequest(t, sc, http.MethodPut, "/db2/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusConflict)
}

// TestBootstrapDuplicateDatabase will attempt to create a second database and ensure this isn't allowed.
func TestBootstrapDuplicateDatabase(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	// Get a test bucket, and use it to create the database.
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	dbConfig := fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
	)

	resp := BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Write a doc, we'll rely on it for later stat assertions to ensure the database isn't being reloaded.
	resp = BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/doc1", `{"test": true}`)
	resp.RequireStatus(http.StatusCreated)

	// check to see we have a doc written stat
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/_expvar", "")
	resp.RequireStatus(http.StatusOK)
	assert.Contains(t, resp.Body, `"num_doc_writes":1`)

	// Create db1 again and expect it to fail
	resp = BootstrapAdminRequest(t, sc, http.MethodPut, "/db1/", dbConfig)
	resp.RequireStatus(http.StatusPreconditionFailed)
	assert.Contains(t, resp.Body, fmt.Sprintf(`Duplicate database name \"%s\"`, "db1"))

	// check to see we still have a doc written stat (as a proxy to determine if the database restarted)
	resp = BootstrapAdminRequest(t, sc, http.MethodGet, "/_expvar", "")
	resp.RequireStatus(http.StatusOK)
	assert.Contains(t, resp.Body, `"num_doc_writes":1`)
}

// TestBootstrapDiagnosticAPI asserts on diagnostic API endpoints
func TestBootstrapPingAPI(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	resp := doBootstrapRequest(t, sc, http.MethodGet, "/_ping/", "", nil, diagnosticServer)
	resp.RequireStatus(http.StatusOK)
}

// Development-time test, expects locally running Couchbase Server and designed for long-running memory profiling
func DevTestFetchConfigManual(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error)

	config := DefaultStartupConfig("")

	logLevel := base.LevelInfo
	config.Logging.Console = &base.ConsoleLoggerConfig{
		LogLevel: &logLevel,
		LogKeys:  []string{"HTTP", "Config", "CRUD", "DCP", "Sync"},
	}

	config.API.AdminInterfaceAuthentication = base.Ptr(false)

	config.API.PublicInterface = "127.0.0.1:4984"
	config.API.AdminInterface = "127.0.0.1:4985"
	config.API.MetricsInterface = "127.0.0.1:4986"

	config.Bootstrap.Server = "couchbase://localhost"
	config.Bootstrap.Username = "configUser"
	config.Bootstrap.Password = "password"
	config.Bootstrap.ServerTLSSkipVerify = base.Ptr(true)
	config.Bootstrap.UseTLSServer = base.Ptr(false)

	// Start SG with no databases, high frequency polling
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Second)

	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)

	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

	// Sleep to wait for bucket polling iterations, or allow manual modification to server accessibility

	time.Sleep(15 * time.Second)

}

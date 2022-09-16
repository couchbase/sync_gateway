package rest

import (
	"fmt"
	"net/http"
	"testing"

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
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Bootstrap works with Couchbase Server only")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := BootstrapStartupConfigForTest(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	ctx = sc.SetContextLogID(ctx, "initial")

	// sc closed and serverErr read later in the test
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := BootstrapAdminRequest(t, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// upsert 1 config field
	resp = BootstrapAdminRequest(t, http.MethodPost, "/db1/_config",
		`{"cache": {"rev_cache":{"size":1234}}}`,
	)
	resp.RequireStatus(http.StatusCreated)

	resp = BootstrapAdminRequest(t, http.MethodGet, "/db1/", ``)
	resp.RequireStatus(http.StatusOK)
	var dbRootResp DatabaseRoot
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbRootResp))
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect the config
	resp = BootstrapAdminRequest(t, http.MethodGet, "/db1/_config", ``)
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
	require.Equal(t, uint32(1234), *dbConfigResp.CacheConfig.RevCacheConfig.Size)

	// Sanity check to use the database
	resp = BootstrapAdminRequest(t, http.MethodPut, "/db1/doc1", `{"foo":"bar"}`)
	resp.RequireResponse(http.StatusCreated, `{"id":"doc1","ok":true,"rev":"1-cd809becc169215072fd567eebd8b8de"}`)
	resp = BootstrapAdminRequest(t, http.MethodGet, "/db1/doc1", ``)
	resp.RequireResponse(http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)

	// Restart Sync Gateway
	sc.Close(ctx)
	require.NoError(t, <-serverErr)

	ctx = base.TestCtx(t)
	sc, err = SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	ctx = sc.SetContextLogID(ctx, "loaddatabase")

	serverErr = make(chan error, 0)
	go func() {
		serverErr <- StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	// Ensure the database was bootstrapped on startup
	resp = BootstrapAdminRequest(t, http.MethodGet, "/db1/", ``)
	resp.RequireStatus(http.StatusOK)
	dbRootResp = DatabaseRoot{}
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbRootResp))
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect config again, and ensure no changes since bootstrap
	resp = BootstrapAdminRequest(t, http.MethodGet, "/db1/_config", ``)
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
	require.Equal(t, uint32(1234), *dbConfigResp.CacheConfig.RevCacheConfig.Size)

	// Ensure it's _actually_ the same bucket
	resp = BootstrapAdminRequest(t, http.MethodGet, "/db1/doc1", ``)
	resp.RequireResponse(http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)
}

// TestBootstrapDuplicateBucket will attempt to create two databases sharing the same bucket and ensure this isn't allowed.
func TestBootstrapDuplicateBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Bootstrap works with Couchbase Server only")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := BootstrapStartupConfigForTest(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)

	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()
	resp := BootstrapAdminRequest(t, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Create db2 using the same bucket and expect it to fail
	resp = BootstrapAdminRequest(t, http.MethodPut, "/db2/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusConflict)

	// CBG-1785 - Check the error has been changed from the original misleading error to a more informative one.
	assert.NotContains(t, resp.Body, fmt.Sprintf(`Database \"%s\" already exists`, "db2"))
	assert.Contains(t, resp.Body, fmt.Sprintf(`Bucket \"%s\" already in use by database \"%s\"`, tb.GetName(), "db1"))
}

// TestBootstrapDuplicateDatabase will attempt to create a second database and ensure this isn't allowed.
func TestBootstrapDuplicateDatabase(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Bootstrap works with Couchbase Server only")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := BootstrapStartupConfigForTest(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)

	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	dbConfig := fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
	)

	resp := BootstrapAdminRequest(t, http.MethodPut, "/db1/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Write a doc, we'll rely on it for later stat assertions to ensure the database isn't being reloaded.
	resp = BootstrapAdminRequest(t, http.MethodPut, "/db1/doc1", `{"test": true}`)
	resp.RequireStatus(http.StatusCreated)

	// check to see we have a doc written stat
	resp = BootstrapAdminRequest(t, http.MethodGet, "/_expvar", "")
	resp.RequireStatus(http.StatusOK)
	assert.Contains(t, resp.Body, `"num_doc_writes":1`)

	// Create db1 again and expect it to fail
	resp = BootstrapAdminRequest(t, http.MethodPut, "/db1/", dbConfig)
	resp.RequireStatus(http.StatusPreconditionFailed)
	assert.Contains(t, resp.Body, fmt.Sprintf(`Duplicate database name \"%s\"`, "db1"))

	// check to see we still have a doc written stat (as a proxy to determine if the database restarted)
	resp = BootstrapAdminRequest(t, http.MethodGet, "/_expvar", "")
	resp.RequireStatus(http.StatusOK)
	assert.Contains(t, resp.Body, `"num_doc_writes":1`)
}

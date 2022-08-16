package rest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// offset for standard port numbers to avoid conflicts 4984 -> 14984
	bootstrapTestPortOffset = 10000
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
	config := bootstrapStartupConfigForTest(t)
	sc, err := setupServerContext(&config, true)
	require.NoError(t, err)

	// sc closed and serverErr read later in the test
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := bootstrapAdminRequest(t, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.requireStatus(http.StatusCreated)

	// upsert 1 config field
	resp = bootstrapAdminRequest(t, http.MethodPost, "/db1/_config",
		`{"cache": {"rev_cache":{"size":1234}}}`,
	)
	resp.requireStatus(http.StatusCreated)

	resp = bootstrapAdminRequest(t, http.MethodGet, "/db1/", ``)
	resp.requireStatus(http.StatusOK)
	var dbRootResp DatabaseRoot
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbRootResp))
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect the config
	resp = bootstrapAdminRequest(t, http.MethodGet, "/db1/_config", ``)
	resp.requireStatus(http.StatusOK)
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
	resp = bootstrapAdminRequest(t, http.MethodPut, "/db1/doc1", `{"foo":"bar"}`)
	resp.requireResponse(http.StatusCreated, `{"id":"doc1","ok":true,"rev":"1-cd809becc169215072fd567eebd8b8de"}`)
	resp = bootstrapAdminRequest(t, http.MethodGet, "/db1/doc1", ``)
	resp.requireResponse(http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)

	// Restart Sync Gateway
	sc.Close()
	require.NoError(t, <-serverErr)

	sc, err = setupServerContext(&config, true)
	require.NoError(t, err)
	serverErr = make(chan error, 0)
	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs())
	defer func() {
		sc.Close()
		require.NoError(t, <-serverErr)
	}()

	// Ensure the database was bootstrapped on startup
	resp = bootstrapAdminRequest(t, http.MethodGet, "/db1/", ``)
	resp.requireStatus(http.StatusOK)
	dbRootResp = DatabaseRoot{}
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbRootResp))
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect config again, and ensure no changes since bootstrap
	resp = bootstrapAdminRequest(t, http.MethodGet, "/db1/_config", ``)
	resp.requireStatus(http.StatusOK)
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
	resp = bootstrapAdminRequest(t, http.MethodGet, "/db1/doc1", ``)
	resp.requireResponse(http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)
}

// TestBootstrapDuplicateBucket will attempt to create two databases sharing the same bucket and ensure this isn't allowed.
func TestBootstrapDuplicateBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Bootstrap works with Couchbase Server only")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	config := bootstrapStartupConfigForTest(t)
	sc, err := setupServerContext(&config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close()
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()
	resp := bootstrapAdminRequest(t, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.requireStatus(http.StatusCreated)

	// Create db2 using the same bucket and expect it to fail
	resp = bootstrapAdminRequest(t, http.MethodPut, "/db2/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.requireStatus(http.StatusConflict)

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
	config := bootstrapStartupConfigForTest(t)
	sc, err := setupServerContext(&config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close()
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	dbConfig := fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
	)

	resp := bootstrapAdminRequest(t, http.MethodPut, "/db1/", dbConfig)
	resp.requireStatus(http.StatusCreated)

	// Write a doc, we'll rely on it for later stat assertions to ensure the database isn't being reloaded.
	resp = bootstrapAdminRequest(t, http.MethodPut, "/db1/doc1", `{"test": true}`)
	resp.requireStatus(http.StatusCreated)

	// check to see we have a doc written stat
	resp = bootstrapAdminRequest(t, http.MethodGet, "/_expvar", "")
	resp.requireStatus(http.StatusOK)
	assert.Contains(t, resp.Body, `"num_doc_writes":1`)

	// Create db1 again and expect it to fail
	resp = bootstrapAdminRequest(t, http.MethodPut, "/db1/", dbConfig)
	resp.requireStatus(http.StatusPreconditionFailed)
	assert.Contains(t, resp.Body, fmt.Sprintf(`Duplicate database name \"%s\"`, "db1"))

	// check to see we still have a doc written stat (as a proxy to determine if the database restarted)
	resp = bootstrapAdminRequest(t, http.MethodGet, "/_expvar", "")
	resp.requireStatus(http.StatusOK)
	assert.Contains(t, resp.Body, `"num_doc_writes":1`)
}

func bootstrapStartupConfigForTest(t *testing.T) StartupConfig {
	config := DefaultStartupConfig("")

	config.Logging.Console = &base.ConsoleLoggerConfig{
		LogLevel: base.ConsoleLogLevel(),
		LogKeys:  base.ConsoleLogKey().EnabledLogKeys(),
	}

	config.API.AdminInterfaceAuthentication = base.BoolPtr(false)

	config.API.PublicInterface = "127.0.0.1:" + strconv.FormatInt(4984+bootstrapTestPortOffset, 10)
	config.API.AdminInterface = "127.0.0.1:" + strconv.FormatInt(4985+bootstrapTestPortOffset, 10)
	config.API.MetricsInterface = "127.0.0.1:" + strconv.FormatInt(4986+bootstrapTestPortOffset, 10)

	config.Bootstrap.Server = base.UnitTestUrl()
	config.Bootstrap.Username = base.TestClusterUsername()
	config.Bootstrap.Password = base.TestClusterPassword()
	config.Bootstrap.ServerTLSSkipVerify = base.BoolPtr(base.TestTLSSkipVerify())
	config.Bootstrap.UseTLSServer = base.BoolPtr(base.ServerIsTLS(base.UnitTestUrl()))

	// avoid loading existing configs by choosing a non-default config group
	if !base.IsEnterpriseEdition() {
		t.Skipf("EE-ONLY: Skipping test %s due to requiring non-default Config Group ID", t.Name())
	}
	config.Bootstrap.ConfigGroupID = t.Name()

	return config
}

const publicPort = 4984

func bootstrapURL(basePort int) string {
	return "http://localhost:" + strconv.Itoa(basePort+bootstrapTestPortOffset)
}

type bootstrapAdminResponse struct {
	StatusCode int
	Body       string
	Header     http.Header
	t          *testing.T
}

func (r *bootstrapAdminResponse) requireStatus(status int) {
	require.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
}

func (r *bootstrapAdminResponse) requireResponse(status int, body string) {
	require.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
	require.Equal(r.t, body, r.Body, "unexpected body")
}

func bootstrapAdminRequest(t *testing.T, method, path, body string) bootstrapAdminResponse {
	return doBootstrapAdminRequest(t, method, "", path, body, nil)
}

func bootstrapAdminRequestCustomHost(t *testing.T, method, host, path, body string) bootstrapAdminResponse {
	return doBootstrapAdminRequest(t, method, host, path, body, nil)
}

func bootstrapAdminRequestWithHeaders(t *testing.T, method, path, body string, headers map[string]string) bootstrapAdminResponse {
	return doBootstrapAdminRequest(t, method, "", path, body, headers)
}

func doBootstrapAdminRequest(t *testing.T, method, host, path, body string, headers map[string]string) bootstrapAdminResponse {
	if host == "" {
		host = "http://localhost:" + strconv.FormatInt(4985+bootstrapTestPortOffset, 10)
	}
	url := host + path

	buf := bytes.NewBufferString(body)
	req, err := http.NewRequest(method, url, buf)
	require.NoError(t, err)

	for headerName, headerVal := range headers {
		req.Header.Set(headerName, headerVal)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	rBody, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	require.NoError(t, resp.Body.Close())

	return bootstrapAdminResponse{
		t:          t,
		StatusCode: resp.StatusCode,
		Body:       string(rBody),
		Header:     resp.Header,
	}
}

package rest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
	"time"

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

	// Start SG with no databases in bucket(s)
	config := bootstrapStartupConfigForTest(t)
	sc, err := setupServerContext(&config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs(time.Second*5))

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := adminRequest(t, http.MethodPut, "/db1/",
		`{"bucket": "`+tb.GetName()+`", "num_index_replicas": 0}`,
	)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = adminRequest(t, http.MethodGet, "/db1/", ``)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var dbRootResp DatabaseRoot
	require.NoError(t, base.JSONDecoder(resp.Body).Decode(&dbRootResp))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect the config
	resp = adminRequest(t, http.MethodGet, "/db1/_config?redact=false", ``)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var dbConfigResp DatabaseConfig
	require.NoError(t, base.JSONDecoder(resp.Body).Decode(&dbConfigResp))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "db1", dbConfigResp.Name)
	require.NotNil(t, dbConfigResp.Bucket)
	assert.Equal(t, tb.GetName(), *dbConfigResp.Bucket)
	require.NotNil(t, dbConfigResp.Server)
	assert.Equal(t, base.UnitTestUrl(), *dbConfigResp.Server)
	assert.Equal(t, base.TestClusterUsername(), dbConfigResp.Username)
	assert.Equal(t, base.TestClusterPassword(), dbConfigResp.Password)
	require.Nil(t, dbConfigResp.Sync)

	// Sanity check to use the database
	resp = adminRequest(t, http.MethodPut, "/db1/doc1", `{"foo":"bar"}`)
	assertResp(t, resp, http.StatusCreated, `{"id":"doc1","ok":true,"rev":"1-cd809becc169215072fd567eebd8b8de"}`)
	resp = adminRequest(t, http.MethodGet, "/db1/doc1", ``)
	assertResp(t, resp, http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)

	// Restart Sync Gateway
	sc.Close()
	require.NoError(t, <-serverErr)

	sc, err = setupServerContext(&config, true)
	require.NoError(t, err)
	serverErr = make(chan error, 0)
	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs(time.Second*5))
	defer func() {
		sc.Close()
		require.NoError(t, <-serverErr)
	}()

	// Ensure the database was bootstrapped on startup
	resp = adminRequest(t, http.MethodGet, "/db1/", ``)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	dbRootResp = DatabaseRoot{}
	require.NoError(t, base.JSONDecoder(resp.Body).Decode(&dbRootResp))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "db1", dbRootResp.DBName)
	assert.Equal(t, db.RunStateString[db.DBOnline], dbRootResp.State)

	// Inspect config again, and ensure no changes since bootstrap
	resp = adminRequest(t, http.MethodGet, "/db1/_config?redact=false", ``)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	dbConfigResp = DatabaseConfig{}
	require.NoError(t, base.JSONDecoder(resp.Body).Decode(&dbConfigResp))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "db1", dbConfigResp.Name)
	require.NotNil(t, dbConfigResp.Bucket)
	assert.Equal(t, tb.GetName(), *dbConfigResp.Bucket)
	require.NotNil(t, dbConfigResp.Server)
	assert.Equal(t, base.UnitTestUrl(), *dbConfigResp.Server)
	assert.Equal(t, base.TestClusterUsername(), dbConfigResp.Username)
	assert.Equal(t, base.TestClusterPassword(), dbConfigResp.Password)
	require.Nil(t, dbConfigResp.Sync)

	// Ensure it's _actually_ the same bucket
	resp = adminRequest(t, http.MethodGet, "/db1/doc1", ``)
	assertResp(t, resp, http.StatusOK, `{"_id":"doc1","_rev":"1-cd809becc169215072fd567eebd8b8de","foo":"bar"}`)
}

func bootstrapStartupConfigForTest(t *testing.T) StartupConfig {
	config := DefaultStartupConfig("")

	config.Bootstrap.UseTLSServer = base.BoolPtr(false)

	config.Logging.Console.LogLevel.Set(base.LevelTrace)
	config.Logging.Console.LogKeys = []string{"*"}
	config.API.AdminInterfaceAuthentication = base.BoolPtr(false)

	config.API.PublicInterface = "127.0.0.1:" + strconv.FormatInt(4984+bootstrapTestPortOffset, 10)
	config.API.AdminInterface = "127.0.0.1:" + strconv.FormatInt(4985+bootstrapTestPortOffset, 10)
	config.API.MetricsInterface = "127.0.0.1:" + strconv.FormatInt(4986+bootstrapTestPortOffset, 10)

	config.Bootstrap.Server = base.UnitTestUrl()
	config.Bootstrap.Username = base.TestClusterUsername()
	config.Bootstrap.Password = base.TestClusterPassword()

	// avoid loading existing configs by choosing a non-default config group
	config.Bootstrap.ConfigGroupID = t.Name()

	return config
}

func adminRequest(t *testing.T, method, path, body string) *http.Response {
	url := "http://localhost:" + strconv.FormatInt(4985+bootstrapTestPortOffset, 10) + path

	buf := bytes.NewBufferString(body)
	req, err := http.NewRequest(method, url, buf)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func assertResp(t *testing.T, resp *http.Response, status int, body string) {
	assert.Equal(t, status, resp.StatusCode)
	b, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, body, string(b))
}

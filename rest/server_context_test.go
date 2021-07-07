//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/connstr"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordGoroutineHighwaterMark(t *testing.T) {

	// Reset this to 0
	atomic.StoreUint64(&MaxGoroutinesSeen, 0)

	assert.Equal(t, uint64(1000), goroutineHighwaterMark(1000))
	assert.Equal(t, uint64(1000), goroutineHighwaterMark(500))
	assert.Equal(t, uint64(1500), goroutineHighwaterMark(1500))

}

//////// MOCK HTTP CLIENT: (TODO: Move this into a separate package)

// Creates a filled-in http.Response from minimal details
func MakeResponse(status int, headers map[string]string, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d", status),
		Body:          ioutil.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
	}
}

// Implementation of http.RoundTripper that does the actual work
type mockTripper struct {
	getURLs map[string]*http.Response
}

func (m *mockTripper) RoundTrip(rq *http.Request) (*http.Response, error) {
	response := m.getURLs[rq.URL.String()]
	if response == nil {
		response = MakeResponse(http.StatusNotFound, nil, "Not Found")
	}
	return response, nil
}

// Fake http.Client that returns canned responses.
type MockClient struct {
	*http.Client
}

// Creates a new MockClient.
func NewMockClient() *MockClient {
	tripper := mockTripper{
		getURLs: map[string]*http.Response{},
	}
	return &MockClient{
		Client: &http.Client{Transport: &tripper},
	}
}

// Adds a canned response. The Client will respond to a GET of the given URL with the response.
func (client *MockClient) RespondToGET(url string, response *http.Response) {
	tripper := client.Transport.(*mockTripper)
	tripper.getURLs[url] = response
}

// convenience function to get a BucketConfig for a given TestBucket.
func bucketConfigFromTestBucket(tb *base.TestBucket) BucketConfig {
	tbUser, tbPassword, _ := tb.BucketSpec.Auth.GetCredentials()
	return BucketConfig{
		Server:     &tb.BucketSpec.Server,
		Bucket:     &tb.BucketSpec.BucketName,
		Username:   tbUser,
		Password:   tbPassword,
		CertPath:   tb.BucketSpec.Certpath,
		KeyPath:    tb.BucketSpec.Keypath,
		CACertPath: tb.BucketSpec.CACertPath,
		KvTLSPort:  tb.BucketSpec.KvTLSPort,
	}
}

func TestAllDatabaseNames(t *testing.T) {
	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	tb1 := base.GetTestBucket(t)
	defer tb1.Close()
	tb2 := base.GetTestBucket(t)
	defer tb2.Close()

	serverConfig := &StartupConfig{API: APIConfig{CORS: &CORSConfig{}, AdminInterface: DefaultAdminInterface}}
	serverContext := NewServerContext(serverConfig, false)
	defer serverContext.Close()

	xattrs := base.TestUseXattrs()
	dbConfig := DbConfig{BucketConfig: bucketConfigFromTestBucket(tb1), Name: "imdb1", AllowEmptyPassword: true, NumIndexReplicas: base.UintPtr(0), EnableXattrs: &xattrs}
	_, err := serverContext.AddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig})
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")

	dbConfig = DbConfig{BucketConfig: bucketConfigFromTestBucket(tb2), Name: "imdb2", AllowEmptyPassword: true, NumIndexReplicas: base.UintPtr(0), EnableXattrs: &xattrs}
	_, err = serverContext.AddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig})
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 2)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb2")

	status := serverContext.RemoveDatabase("imdb2")
	assert.True(t, status, "Database should be removed from server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")
	assert.NotContains(t, serverContext.AllDatabaseNames(), "imdb2")
}

func TestGetOrAddDatabaseFromConfig(t *testing.T) {
	serverConfig := &StartupConfig{API: APIConfig{CORS: &CORSConfig{}, AdminInterface: DefaultAdminInterface}}
	serverContext := NewServerContext(serverConfig, false)
	defer serverContext.Close()

	oldRevExpirySeconds := uint32(600)
	localDocExpirySecs := uint32(60 * 60 * 24 * 10) // 10 days in seconds

	// Get or add database name from config without valid database name; throws 400 Illegal database name error
	dbConfig := DbConfig{OldRevExpirySeconds: &oldRevExpirySeconds, LocalDocExpirySecs: &localDocExpirySecs}
	dbContext, err := serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, false)
	assert.Nil(t, dbContext, "Can't create database context without a valid database name")
	assert.Error(t, err, "It should throw 400 Illegal database name")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Get or add database from config with duplicate database name and useExisting as false.
	server := "walrus:"
	bucketName := "imbucket"
	databaseName := "imdb"

	// Get or add database from config with unrecognized value for import_docs.
	dbConfig = DbConfig{
		Name:                "imdb",
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          "Unknown",
		BucketConfig:        BucketConfig{Server: &server, Bucket: &bucketName},
	}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, false)
	assert.Nil(t, dbContext, "Can't create database context from config with unrecognized value for import_docs")
	assert.Error(t, err, "It should throw Unrecognized value for import_docs")

	bucketConfig := BucketConfig{Server: &server, Bucket: &bucketName}
	dbConfig = DbConfig{BucketConfig: bucketConfig, Name: databaseName, AllowEmptyPassword: true}
	dbContext, err = serverContext.AddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig})

	assert.NoError(t, err, "No error while adding database to server context")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)

	dbConfig = DbConfig{
		Name:                databaseName,
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          false}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, false)
	assert.Nil(t, dbContext, "Can't create database context with duplicate database name")
	assert.Error(t, err, "It should throw 412 Duplicate database names")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusPreconditionFailed))

	// Get or add database from config with duplicate database name and useExisting as true
	// Existing database context should be returned
	dbContext, err = serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, true)
	assert.NoError(t, err, "No error while trying to get the existing database name")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)
}

func TestStatsLoggerStopped(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	sc := DefaultStartupConfig("")

	// Start up stats logger by creating server context
	ctx := NewServerContext(&sc, false)

	// Close server context which will send signal to close stats logger
	ctx.Close()

	// ensure stats terminator is closed
	_, ok := <-ctx.statsContext.terminator
	assert.False(t, ok)

	// sleep a bit to allow the "Stopping stats logging goroutine" debug logging to be printed
	time.Sleep(time.Millisecond * 10)
}

func TestObtainManagementEndpointsFromServerContext(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	eps, _, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	assert.NoError(t, err)

	clusterAddress := base.UnitTestUrl()
	baseSpec, err := connstr.Parse(clusterAddress)
	require.NoError(t, err)

	spec, err := connstr.Resolve(baseSpec)
	require.NoError(t, err)

	existsOneMatchingEndpoint := false

outerLoop:
	for _, httpHost := range spec.HttpHosts {
		for _, ep := range eps {
			formattedHttpHost := fmt.Sprintf("http://%s:%d", httpHost.Host, httpHost.Port)
			if formattedHttpHost == ep {
				existsOneMatchingEndpoint = true
				break outerLoop
			}
		}
	}

	assert.True(t, existsOneMatchingEndpoint)
}

func TestObtainManagementEndpointsFromServerContextWithX509(t *testing.T) {
	tb, teardownFn, caCertPath, certPath, keyPath := setupX509Tests(t, true)
	defer tb.Close()
	defer teardownFn()

	ctx := NewServerContext(&StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       base.UnitTestUrl(),
			X509CertPath: certPath,
			X509KeyPath:  keyPath,
			CACertPath:   caCertPath,
		},
	}, false)
	defer ctx.Close()

	eps, _, err := ctx.ObtainManagementEndpointsAndHTTPClient()
	assert.NoError(t, err)

	baseSpec, err := connstr.Parse(base.UnitTestUrl())
	require.NoError(t, err)

	spec, err := connstr.Resolve(baseSpec)
	require.NoError(t, err)

	existsOneMatchingEndpoint := false

outerLoop:
	for _, httpHost := range spec.HttpHosts {
		for _, ep := range eps {
			formattedHttpHost := fmt.Sprintf("https://%s:%d", httpHost.Host, httpHost.Port)
			if formattedHttpHost == ep {
				existsOneMatchingEndpoint = true
				break outerLoop
			}
		}
	}

	assert.True(t, existsOneMatchingEndpoint)
}

func TestStartAndStopHTTPServers(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	tb := base.GetTestBucket(t)
	defer tb.Close()

	config := DefaultStartupConfig("")

	// choose high ports to avoid port conflicts when testing
	config.API.PublicInterface = "127.0.0.1:24984"
	config.API.AdminInterface = "127.0.0.1:24985"
	config.API.MetricsInterface = "127.0.0.1:24986"

	config.Bootstrap.Server = base.UnitTestUrl()
	config.Bootstrap.Username = base.TestClusterUsername()
	config.Bootstrap.Password = base.TestClusterPassword()

	sc, err := setupServerContext(&config, false)
	require.NoError(t, err)

	serveErr := make(chan error, 0)
	go func() {
		serveErr <- startServer(&config, sc)
	}()

	err, _ = base.RetryLoop("try http request", func() (shouldRetry bool, err error, value interface{}) {
		resp, err := http.Get("http://" + config.API.PublicInterface)
		if err != nil {
			return true, err, nil
		}
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		return false, nil, nil
	}, base.CreateMaxDoublingSleeperFunc(10, 10, 1000))
	assert.NoError(t, err)

	sc.Close()

	assert.NoError(t, <-serveErr)
}

// CBG-1518 - Test CA Certificate behaviour with and with Bootstrap.ServerTLSSkipVerify
func TestTLSSkipVerifyCombinations(t *testing.T) {
	// Force teardown due to setupServerContext setting up logging
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()
	errorText := "cannot skip server TLS validation and use CA Cert"
	testCases := []struct {
		name                string
		serverTLSSkipVerify *bool
		caCert              string
		expectError         bool
	}{
		{
			name:                "CA Provided, explicitly not skipping TLS validation",
			serverTLSSkipVerify: base.BoolPtr(false),
			caCert:              "t.ca",
			expectError:         false,
		},
		{
			name:        "CA Provided only",
			caCert:      "t.ca",
			expectError: false,
		},
		{
			name:                "CA Provided and skipping TLS validation",
			serverTLSSkipVerify: base.BoolPtr(true),
			caCert:              "t.ca",
			expectError:         true,
		},
		{
			name:                "Skipping TLS validation, no CA",
			serverTLSSkipVerify: base.BoolPtr(true),
			caCert:              "",
			expectError:         false,
		},
		{
			name:        "No CA, no TLS validation skip",
			expectError: false,
		},
		{
			name:                "No CA, no TLS validation skip explicitly",
			serverTLSSkipVerify: base.BoolPtr(false),
			expectError:         false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			startupConfig := &StartupConfig{
				Bootstrap: BootstrapConfig{
					CACertPath:          test.caCert,
					ServerTLSSkipVerify: test.serverTLSSkipVerify,
				},
			}

			sc, err := setupServerContext(startupConfig, false)
			if test.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), errorText)
				assert.Empty(t, sc)
			} else if err != nil {
				// check if unrelated error
				assert.NotContains(t, err.Error(), errorText)
			}
		})
	}

}

// CBG-1518 - Test GetbucketSpec() ServerTLSSkipVerify and empty CA Cert behaviour.
// Does not test validation of having CA Cert provided and TLS Skip verify on. See TestTLSSkipVerifyCombinations for that.
func TestTLSSkipVerifyGetBucketSpec(t *testing.T) {
	testCases := []struct {
		name                string
		serverTLSSkipVerify *bool
		caCert              string
	}{
		{
			name:                "CA Provided, explicitly not skipping TLS validation",
			serverTLSSkipVerify: base.BoolPtr(false),
			caCert:              "t.ca",
		},
		{
			name:   "CA Provided only",
			caCert: "t.ca",
		},
		{
			name:                "Skipping TLS validation, no CA",
			serverTLSSkipVerify: base.BoolPtr(true),
			caCert:              "",
		},
		{
			name: "No CA, no TLS validation skip",
		},
		{
			name:                "No CA, no TLS validation skip explicitly",
			serverTLSSkipVerify: base.BoolPtr(false),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			startupConfig := &StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: test.serverTLSSkipVerify}}
			dbConfig := &DbConfig{BucketConfig: BucketConfig{CACertPath: test.caCert}}
			spec, err := GetBucketSpec(dbConfig, startupConfig)

			assert.NoError(t, err)
			assert.Equal(t, test.caCert, spec.CACertPath)
			if test.serverTLSSkipVerify == nil {
				test.serverTLSSkipVerify = base.BoolPtr(false)
			}
			assert.Equal(t, spec.TLSSkipVerify, *test.serverTLSSkipVerify)
		})
	}
}

func TestAllowInsecureServerConnections(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	testCases := []struct {
		name                           string
		allowInsecureServerConnections bool
		server                         string
		expectError                    bool
	}{
		{
			name:                           "Walrus allowed without flag",
			allowInsecureServerConnections: false,
			server:                         "walrus://",
			expectError:                    false,
		},
		{
			name:                           "couchbase: not allowed",
			allowInsecureServerConnections: false,
			server:                         "couchbase://localhost:1212",
			expectError:                    true,
		},
		{
			name:                           "http not allowed",
			allowInsecureServerConnections: false,
			server:                         "http://localhost:1212",
			expectError:                    true,
		},
		{
			name:                           "http allowed",
			allowInsecureServerConnections: true,
			server:                         "http://localhost:1212",
			expectError:                    false,
		},
		{
			name:                           "https mandatory",
			allowInsecureServerConnections: false,
			server:                         "https://localhost:1234",
			expectError:                    false,
		},
		{
			name:                           "https not mandatory",
			allowInsecureServerConnections: true,
			server:                         "https://localhost:1234",
			expectError:                    false,
		},
		{
			name:                           "couchbases:",
			allowInsecureServerConnections: false,
			server:                         "couchbases://localhost:1234",
			expectError:                    false,
		},
		{
			name:                           "ftps:", // Testing if the S at the end is what makes it secure
			allowInsecureServerConnections: false,
			server:                         "ftps://localhost:1234",
			expectError:                    true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Setup
			config := DefaultStartupConfig("")
			config.Bootstrap.AllowInsecureServerConnections = &test.allowInsecureServerConnections

			config.API.HTTPS.AllowInsecureTLSConnections = base.BoolPtr(true)
			sc := NewServerContext(&config, false)
			databaseConfig := DatabaseConfig{
				DbConfig: DbConfig{
					BucketConfig: BucketConfig{
						Server: &test.server,
					},
				},
			}
			// Run test
			_, err := sc._getOrAddDatabaseFromConfig(databaseConfig, false)

			expectedError := "couchbase server URL must use secure protocol. Current URL: " + test.server
			if test.expectError {
				assert.Error(t, err, expectedError)
			} else {
				assert.NotEqual(t, expectedError, err) // Will still error due to no DB name, or not being able to connect to bucket
			}
			sc.Close()
		})
	}
}

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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/connstr"
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
	base.RequireNumTestBuckets(t, 2)

	tb1 := base.GetTestBucket(t)
	defer tb1.Close()
	tb2 := base.GetTestBucket(t)
	defer tb2.Close()

	serverConfig := &StartupConfig{
		Bootstrap: BootstrapConfig{UseTLSServer: base.BoolPtr(base.ServerIsTLS(base.UnitTestUrl())), ServerTLSSkipVerify: base.BoolPtr(base.TestTLSSkipVerify())},
		API:       APIConfig{CORS: &CORSConfig{}, AdminInterface: DefaultAdminInterface}}
	serverContext := NewServerContext(serverConfig, false)
	defer serverContext.Close()

	xattrs := base.TestUseXattrs()
	useViews := base.TestsDisableGSI()
	dbConfig := DbConfig{
		BucketConfig:       bucketConfigFromTestBucket(tb1),
		Name:               "imdb1",
		AllowEmptyPassword: base.BoolPtr(true),
		NumIndexReplicas:   base.UintPtr(0),
		EnableXattrs:       &xattrs,
		UseViews:           &useViews,
	}
	_, err := serverContext.AddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig})
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")

	dbConfig = DbConfig{
		BucketConfig:       bucketConfigFromTestBucket(tb2),
		Name:               "imdb2",
		AllowEmptyPassword: base.BoolPtr(true),
		NumIndexReplicas:   base.UintPtr(0),
		EnableXattrs:       &xattrs,
		UseViews:           &useViews,
	}
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
	dbContext, err := serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, false, false)
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

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, false, false)
	assert.Nil(t, dbContext, "Can't create database context from config with unrecognized value for import_docs")
	assert.Error(t, err, "It should throw Unrecognized value for import_docs")

	xattrs := base.TestUseXattrs()
	useViews := base.TestsDisableGSI()
	bucketConfig := BucketConfig{Server: &server, Bucket: &bucketName}
	dbConfig = DbConfig{
		BucketConfig:       bucketConfig,
		Name:               databaseName,
		AllowEmptyPassword: base.BoolPtr(true),
		EnableXattrs:       &xattrs,
		UseViews:           &useViews,
	}
	dbContext, err = serverContext.AddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig})

	assert.NoError(t, err, "Unexpected error while adding database to server context")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)

	dbConfig = DbConfig{
		Name:                databaseName,
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          false,
	}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, false, false)
	assert.Nil(t, dbContext, "Can't create database context with duplicate database name")
	assert.Error(t, err, "It should throw 412 Duplicate database names")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusPreconditionFailed))

	// Get or add database from config with duplicate database name and useExisting as true
	// Existing database context should be returned
	dbContext, err = serverContext._getOrAddDatabaseFromConfig(DatabaseConfig{DbConfig: dbConfig}, true, false)
	assert.NoError(t, err, "No error while trying to get the existing database name")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)
}

func TestStatsLoggerStopped(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
			protocol := "http"
			if spec.UseSsl {
				protocol = "https"
			}
			formattedHttpHost := fmt.Sprintf("%s://%s:%d", protocol, httpHost.Host, httpHost.Port)

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

	goCBAgent, err := ctx.initializeGoCBAgent()
	require.NoError(t, err)
	ctx.GoCBAgent = goCBAgent

	noX509HttpClient, err := ctx.initializeNoX509HttpClient()
	require.NoError(t, err)
	ctx.NoX509HTTPClient = noX509HttpClient

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
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	config := DefaultStartupConfig("")

	// choose high ports to avoid port conflicts when testing
	config.API.PublicInterface = "127.0.0.1:24984"
	config.API.AdminInterface = "127.0.0.1:24985"
	config.API.MetricsInterface = "127.0.0.1:24986"

	config.Bootstrap.Server = base.UnitTestUrl()
	config.Bootstrap.UseTLSServer = base.BoolPtr(base.ServerIsTLS(base.UnitTestUrl()))
	config.Bootstrap.ServerTLSSkipVerify = base.BoolPtr(base.TestTLSSkipVerify())
	config.Bootstrap.Username = base.TestClusterUsername()
	config.Bootstrap.Password = base.TestClusterPassword()

	sc, err := setupServerContext(&config, false)
	require.NoError(t, err)

	serveErr := make(chan error, 0)
	go func() {
		serveErr <- startServer(&config, sc)
	}()

	defer func() {
		sc.Close()
		require.NoError(t, <-serveErr)
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
}

// CBG-1518 - Test CA Certificate behaviour with and with Bootstrap.ServerTLSSkipVerify
func TestTLSSkipVerifyCombinations(t *testing.T) {
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

			err := startupConfig.validate(base.IsEnterpriseEdition())
			if test.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), errorText)
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
			dbConfig := &DatabaseConfig{DbConfig: DbConfig{BucketConfig: BucketConfig{CACertPath: test.caCert}}}
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

// CBG-1535 - test Bootstrap.UseTLSServer option
func TestUseTLSServer(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	errorMustBeSecure := "Must use secure scheme in Couchbase Server URL, or opt out by setting bootstrap.use_tls_server to false. Current URL: %v"
	errorAllowInsecureAndBeSecure := "Couchbase server URL cannot use secure protocol when bootstrap.use_tls_server is false. Current URL: %v"
	testCases := []struct {
		name          string
		useTLSServer  bool
		server        string
		expectedError *string
	}{
		{
			name:          "Walrus allowed without flag",
			useTLSServer:  false,
			server:        "walrus://",
			expectedError: nil,
		},
		{
			name:          "Walrus allowed with flag",
			useTLSServer:  true,
			server:        "walrus://",
			expectedError: nil,
		},
		{
			name:          "couchbase: not allowed",
			useTLSServer:  true,
			server:        "couchbase://localhost:1212",
			expectedError: &errorMustBeSecure,
		},
		{
			name:          "http not allowed",
			useTLSServer:  true,
			server:        "http://localhost:1212",
			expectedError: &errorMustBeSecure,
		},
		{
			name:          "http allowed",
			useTLSServer:  false,
			server:        "http://localhost:1212",
			expectedError: nil,
		},
		{
			name:          "Https not secure (due to unsupported)",
			useTLSServer:  true,
			server:        "https://localhost:1234",
			expectedError: &errorMustBeSecure,
		},
		{
			name:          "couchbases:",
			useTLSServer:  true,
			server:        "couchbases://localhost:1234",
			expectedError: nil,
		},
		{
			name:          "ftps:", // Testing if the S at the end is what makes it secure
			useTLSServer:  true,
			server:        "ftps://localhost:1234",
			expectedError: &errorMustBeSecure,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			sc := StartupConfig{Bootstrap: BootstrapConfig{Server: test.server, UseTLSServer: &test.useTLSServer}}

			err := sc.validate(base.IsEnterpriseEdition())

			if test.expectedError != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), fmt.Sprintf(*test.expectedError, test.server))
			} else if err != nil {
				// May still error for other reasons (as multiple error messages are returned)
				// So make sure it's not the 2 errors that can happen due to secure protocol
				assert.NotContains(t, err.Error(), fmt.Sprintf(errorMustBeSecure, test.server))
				assert.NotContains(t, err.Error(), fmt.Sprintf(errorAllowInsecureAndBeSecure, test.server))
			}
		})
	}
}

func TestLogFlush(t *testing.T) {
	testCases := []struct {
		Name                 string
		ExpectedLogFileCount int
		EnableFunc           func(config StartupConfig) StartupConfig
	}{
		{
			"Default",
			4,
			func(config StartupConfig) StartupConfig {
				return config
			},
		},
		{
			"Add trace",
			5,
			func(config StartupConfig) StartupConfig {
				config.Logging.Trace = &base.FileLoggerConfig{
					Enabled: base.BoolPtr(true),
				}
				return config
			},
		},
		{
			"Add debug and trace",
			6,
			func(config StartupConfig) StartupConfig {
				config.Logging.Debug = &base.FileLoggerConfig{
					Enabled: base.BoolPtr(true),
				}
				config.Logging.Trace = &base.FileLoggerConfig{
					Enabled: base.BoolPtr(true),
				}
				return config
			},
		},
		{
			"Disable error",
			3,
			func(config StartupConfig) StartupConfig {
				config.Logging.Error = &base.FileLoggerConfig{
					Enabled: base.BoolPtr(false),
				}
				return config
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

			// Setup memory logging
			base.InitializeMemoryLoggers()

			// Add temp dir to save log files to
			tempPath, err := ioutil.TempDir("", "logs"+testCase.Name)
			require.NoError(t, err)
			testDirName := filepath.Base(tempPath)

			// Log some stuff (which will go into the memory loggers)
			ctx := base.TestCtx(t)
			base.ErrorfCtx(ctx, "error: "+testDirName)
			base.WarnfCtx(ctx, "warn: "+testDirName)
			base.InfofCtx(ctx, base.KeyAll, "info: "+testDirName)
			base.DebugfCtx(ctx, base.KeyAll, "debug: "+testDirName)
			base.TracefCtx(ctx, base.KeyAll, "trace: "+testDirName)
			base.RecordStats("{}")

			config := DefaultStartupConfig(tempPath)
			config = testCase.EnableFunc(config)

			// Setup logging
			err = config.SetupAndValidateLogging()
			assert.NoError(t, err)

			// Flush memory loggers
			base.FlushLoggerBuffers()

			// Concurrent calls to FlushLogBuffers should not cause data race or wait group reuse issues
			// Wait for concurrent calls so they don't cause issues with SetupAndValidateLogging from next t.Run
			var flushCallsWg = sync.WaitGroup{}
			for i := 0; i < 10; i++ {
				flushCallsWg.Add(1)
				go func() {
					defer flushCallsWg.Done()
					// Flush collation buffers to ensure the files that will be built do get written
					base.FlushLogBuffers()
				}()
			}
			flushCallsWg.Wait()

			// Check that the expected number of log files are created
			var files []string
			worker := func() (shouldRetry bool, err error, value interface{}) {
				files = []string{}
				err = filepath.Walk(tempPath, func(path string, info os.FileInfo, err error) error {
					if tempPath != path {
						files = append(files, filepath.Base(path))
					}
					return nil
				})

				if err != nil {
					return false, err, nil
				}

				if testCase.ExpectedLogFileCount == len(files) {
					return false, nil, files
				}

				return true, nil, files
			}

			sleeper := base.CreateSleeperFunc(200, 100)
			err, _ = base.RetryLoop("Wait for log files", worker, sleeper)
			assert.NoError(t, err)
			if !assert.Len(t, files, testCase.ExpectedLogFileCount) {
				// Try to figure who is writing to the files
				for _, filename := range files {
					if content, err := os.ReadFile(filepath.Join(tempPath, filename)); err != nil {
						t.Log("error reading file: ", filename, ": ", err)
					} else {
						t.Log(filename, ": ", string(content))
					}
				}
			}
		})
	}

}

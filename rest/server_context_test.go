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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/connstr"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
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
		Body:          io.NopCloser(bytes.NewBufferString(body)),
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

	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)
	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)

	serverConfig := &StartupConfig{
		Bootstrap: BootstrapConfig{UseTLSServer: base.Ptr(base.ServerIsTLS(base.UnitTestUrl())), ServerTLSSkipVerify: base.Ptr(base.TestTLSSkipVerify())},
		API:       APIConfig{CORS: &auth.CORSConfig{}, AdminInterface: DefaultAdminInterface}}
	serverContext := NewServerContext(ctx, serverConfig, false)
	defer serverContext.Close(ctx)

	xattrs := base.TestUseXattrs()
	useViews := base.TestsDisableGSI()
	dbConfig := DbConfig{
		BucketConfig:       bucketConfigFromTestBucket(tb1),
		Name:               "imdb1",
		AllowEmptyPassword: base.Ptr(true),
		NumIndexReplicas:   base.Ptr(uint(0)),
		EnableXattrs:       &xattrs,
		UseViews:           &useViews,
	}
	_, err := serverContext.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig})
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")

	dbConfig = DbConfig{
		BucketConfig:       bucketConfigFromTestBucket(tb2),
		Name:               "imdb2",
		AllowEmptyPassword: base.Ptr(true),
		NumIndexReplicas:   base.Ptr(uint(0)),
		EnableXattrs:       &xattrs,
		UseViews:           &useViews,
	}
	_, err = serverContext.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig})
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 2)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb2")

	status := serverContext.RemoveDatabase(ctx, "imdb2")
	assert.True(t, status, "Database should be removed from server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")
	assert.NotContains(t, serverContext.AllDatabaseNames(), "imdb2")
}

func TestGetOrAddDatabaseFromConfig(t *testing.T) {
	ctx := base.TestCtx(t)
	serverConfig := &StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{}, AdminInterface: DefaultAdminInterface}}
	serverContext := NewServerContext(ctx, serverConfig, false)
	defer serverContext.Close(ctx)

	oldRevExpirySeconds := uint32(600)
	localDocExpirySecs := uint32(60 * 60 * 24 * 10) // 10 days in seconds

	// Get or add database name from config without valid database name; throws 400 Illegal database name error
	dbConfig := DbConfig{OldRevExpirySeconds: &oldRevExpirySeconds, LocalDocExpirySecs: &localDocExpirySecs}
	dbContext, err := serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig}, getOrAddDatabaseConfigOptions{useExisting: false, failFast: false})
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

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig}, getOrAddDatabaseConfigOptions{
		failFast:    false,
		useExisting: false,
	})
	assert.Nil(t, dbContext, "Can't create database context from config with unrecognized value for import_docs")
	assert.Error(t, err, "It should throw Unrecognized value for import_docs")

	xattrs := base.TestUseXattrs()
	useViews := base.TestsDisableGSI()
	bucketConfig := BucketConfig{Server: &server, Bucket: &bucketName}
	dbConfig = DbConfig{
		BucketConfig:       bucketConfig,
		Name:               databaseName,
		AllowEmptyPassword: base.Ptr(true),
		EnableXattrs:       &xattrs,
		UseViews:           &useViews,
	}
	dbContext, err = serverContext.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig})

	assert.NoError(t, err, "Unexpected error while adding database to server context")
	assert.Equal(t, rosmar.InMemoryURL, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)

	dbConfig = DbConfig{
		Name:                databaseName,
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          false,
	}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig}, getOrAddDatabaseConfigOptions{
		failFast:    false,
		useExisting: false,
	})
	assert.Nil(t, dbContext, "Can't create database context with duplicate database name")
	assert.Error(t, err, "It should throw 412 Duplicate database names")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusPreconditionFailed))

	// Get or add database from config with duplicate database name and useExisting as true
	// Existing database context should be returned
	dbContext, err = serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig},
		getOrAddDatabaseConfigOptions{
			failFast:    false,
			useExisting: true,
		})

	assert.NoError(t, err, "No error while trying to get the existing database name")
	assert.Equal(t, rosmar.InMemoryURL, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)
}

func TestStatsLoggerStopped(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	sc := DefaultStartupConfig("")

	// Start up stats logger by creating server context
	ctx := base.TestCtx(t)
	svrctx := NewServerContext(ctx, &sc, false)

	// Close server context which will send signal to close stats logger
	svrctx.Close(ctx)

	// ensure stats terminator is closed
	_, ok := <-svrctx.statsContext.terminator
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

			t.Logf("formattedHttpHost: %s, ep: %s", formattedHttpHost, ep)
			if formattedHttpHost == ep {
				existsOneMatchingEndpoint = true
				break outerLoop
			}
		}
	}

	assert.True(t, existsOneMatchingEndpoint)
}

func TestObtainManagementEndpointsFromServerContextWithX509(t *testing.T) {
	serverURL := base.UnitTestUrl()
	if !base.ServerIsTLS(serverURL) {
		t.Skipf("URI %s needs to start with couchbases://", serverURL)
	}
	ctx := base.TestCtx(t)
	tb, caCertPath, certPath, keyPath := setupX509Tests(t, true)
	defer tb.Close(ctx)

	svrctx := NewServerContext(ctx, &StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       serverURL,
			X509CertPath: certPath,
			X509KeyPath:  keyPath,
			CACertPath:   caCertPath,
		},
	}, false)
	svrctx.Close(ctx)

	goCBAgent, err := svrctx.initializeGoCBAgent(ctx)
	require.NoError(t, err)
	svrctx.GoCBAgent = goCBAgent

	noX509HttpClient, err := svrctx.initializeNoX509HttpClient(ctx)
	require.NoError(t, err)
	svrctx.NoX509HTTPClient = noX509HttpClient

	eps, _, err := svrctx.ObtainManagementEndpointsAndHTTPClient()
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

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	resp, err := http.Get("http://" + mustGetServerAddr(t, sc, publicServer))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

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
			serverTLSSkipVerify: base.Ptr(false),
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
			serverTLSSkipVerify: base.Ptr(true),
			caCert:              "t.ca",
			expectError:         true,
		},
		{
			name:                "Skipping TLS validation, no CA",
			serverTLSSkipVerify: base.Ptr(true),
			caCert:              "",
			expectError:         false,
		},
		{
			name:        "No CA, no TLS validation skip",
			expectError: false,
		},
		{
			name:                "No CA, no TLS validation skip explicitly",
			serverTLSSkipVerify: base.Ptr(false),
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

			err := startupConfig.Validate(base.TestCtx(t), base.IsEnterpriseEdition())
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
			serverTLSSkipVerify: base.Ptr(false),
			caCert:              "t.ca",
		},
		{
			name:   "CA Provided only",
			caCert: "t.ca",
		},
		{
			name:                "Skipping TLS validation, no CA",
			serverTLSSkipVerify: base.Ptr(true),
			caCert:              "",
		},
		{
			name: "No CA, no TLS validation skip",
		},
		{
			name:                "No CA, no TLS validation skip explicitly",
			serverTLSSkipVerify: base.Ptr(false),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			startupConfig := &StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: test.serverTLSSkipVerify}}
			dbConfig := &DatabaseConfig{DbConfig: DbConfig{BucketConfig: BucketConfig{CACertPath: test.caCert}}}
			spec, err := GetBucketSpec(ctx, dbConfig, startupConfig)

			assert.NoError(t, err)
			assert.Equal(t, test.caCert, spec.CACertPath)
			if test.serverTLSSkipVerify == nil {
				test.serverTLSSkipVerify = base.Ptr(false)
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

			err := sc.Validate(base.TestCtx(t), base.IsEnterpriseEdition())

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

// Test that we correctly error out when trying to use collections against a CBS that doesn't support them. NB: this
// test only runs against Couchbase Server <7.0.
func TestServerContextSetupCollectionsSupport(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Requires Couchbase Server")
	}

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	if tb.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		t.Skip("Only runs on datastores without collections support")
	}

	serverConfig := &StartupConfig{
		Bootstrap: BootstrapConfig{
			UseTLSServer:        base.Ptr(base.ServerIsTLS(base.UnitTestUrl())),
			ServerTLSSkipVerify: base.Ptr(base.TestTLSSkipVerify()),
		},
		API: APIConfig{CORS: &auth.CORSConfig{}, AdminInterface: DefaultAdminInterface},
	}
	serverContext := NewServerContext(ctx, serverConfig, false)
	defer serverContext.Close(ctx)

	dbConfig := DbConfig{
		BucketConfig: BucketConfig{
			Server:   base.Ptr(base.UnitTestUrl()),
			Bucket:   base.Ptr(tb.GetName()),
			Username: base.TestClusterUsername(),
			Password: base.TestClusterPassword(),
		},
		Name:             tb.GetName(),
		NumIndexReplicas: base.Ptr(uint(0)),
		EnableXattrs:     base.Ptr(base.TestUseXattrs()),
		Scopes: ScopesConfig{
			"foo": ScopeConfig{
				Collections: CollectionsConfig{
					"bar": &CollectionConfig{},
				},
			},
		},
	}
	_, err := serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig},
		getOrAddDatabaseConfigOptions{
			failFast:    false,
			useExisting: false,
		})

	require.ErrorIs(t, err, errCollectionsUnsupported)
}

func TestLogFlush(t *testing.T) {
	// FIXME: CBG-1869 flaky test
	t.Skip("CBG-1869: Flaky test")

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
					Enabled: base.Ptr(true),
				}
				return config
			},
		},
		{
			"Add debug and trace",
			6,
			func(config StartupConfig) StartupConfig {
				config.Logging.Debug = &base.FileLoggerConfig{
					Enabled: base.Ptr(true),
				}
				config.Logging.Trace = &base.FileLoggerConfig{
					Enabled: base.Ptr(true),
				}
				return config
			},
		},
		{
			"Disable error",
			3,
			func(config StartupConfig) StartupConfig {
				config.Logging.Error = &base.FileLoggerConfig{
					Enabled: base.Ptr(false),
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
			tempPath := t.TempDir()
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
			err := config.SetupAndValidateLogging(base.TestCtx(t))
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
			err, _ = base.RetryLoop(ctx, "Wait for log files", worker, sleeper)
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

func TestValidateMetadataStore(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	require.NoError(t, validateMetadataStore(ctx, bucket.DefaultDataStore()))
}

func TestDisableScopesInLegacyConfig(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	for _, persistentConfig := range []bool{false, true} {
		for _, scopes := range []bool{false, true} {
			t.Run(fmt.Sprintf("persistent_config=%t", persistentConfig), func(t *testing.T) {

				ctx := base.TestCtx(t)
				startupConfig := &StartupConfig{
					Bootstrap: BootstrapConfig{
						UseTLSServer:        base.Ptr(base.ServerIsTLS(base.UnitTestUrl())),
						ServerTLSSkipVerify: base.Ptr(base.TestTLSSkipVerify()),
					},
				}

				serverContext := NewServerContext(ctx, startupConfig, persistentConfig)
				defer serverContext.Close(ctx)

				dbConfig := DbConfig{
					Name: "db",
					BucketConfig: BucketConfig{
						Server:   base.Ptr(base.UnitTestUrl()),
						Bucket:   base.Ptr(bucket.GetName()),
						Username: base.TestClusterUsername(),
						Password: base.TestClusterPassword(),
					},
					EnableXattrs: base.Ptr(base.TestUseXattrs()),
					UseViews:     base.Ptr(base.TestsDisableGSI()),
				}
				if scopes {
					if !base.TestsUseNamedCollections() {
						t.Skip("can not run collections tests in non collections configuration")
					}
					dbConfig.Scopes = GetCollectionsConfig(t, bucket, 1)
				}
				dbContext, err := serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig},
					getOrAddDatabaseConfigOptions{
						failFast:    false,
						useExisting: false,
					})
				if persistentConfig || scopes == false {
					require.NoError(t, err)
					require.NotNil(t, dbContext)
					return
				}
				require.Nil(t, dbContext)
				require.Error(t, err)
				require.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
				require.Contains(t, err.Error(), "legacy config")
			})
		}
	}
}

// TestOfflineDatabaseStartup ensures that background processes are not actually running when starting up a database in offline mode.
func TestOfflineDatabaseStartup(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("TestOfflineDatabaseStartup requires xattrs for document import")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				StartOffline: base.Ptr(true),
				AutoImport:   true,
				EnableXattrs: base.Ptr(true),
			},
		},
	})
	defer rt.Close()

	ds := rt.GetSingleDataStore()
	_, err := ds.AddRaw("doc1", 0, []byte(`{"type":"doc1"}`))
	require.NoError(t, err)

	// make sure we actually started offline (try to put a doc through the REST API)
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc2", `{"type":"doc2"}`)
	RequireStatus(t, resp, http.StatusServiceUnavailable)

	// put doc2 bypassing offline checks (this step will begin to fail with Elixir - since we're making offline more comprehensive)
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	_, _, err = collection.Put(ctx, "doc2", db.Body{"type": "doc2"})
	require.NoError(t, err)

	require.Nil(t, rt.GetDatabase().ImportListener)

	// ensure doc1 is not imported - since we started the database offline
	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	rt.ServerContext().TakeDbOnline(base.NewNonCancelCtx(), rt.GetDatabase())
	require.NotNil(t, rt.GetDatabase().ImportListener)

	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc3", `{"type":"doc3"}`)
	RequireStatus(t, resp, http.StatusCreated)

	// ensure doc1 is imported now we're online
	rt.WaitForChanges(3, "/{{.keyspace}}/_changes", "", true)
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())
}

func TestCompactIntervalFromConfig(t *testing.T) {
	testCases := []struct {
		name                        string
		compactIntervalDays         *float32
		expectedCompactIntervalSecs uint32
	}{
		{
			name:                        "compact interval days not set",
			compactIntervalDays:         nil,
			expectedCompactIntervalSecs: uint32(db.DefaultCompactInterval.Seconds()),
		},
		{
			name:                        "explicit 1",
			compactIntervalDays:         base.Ptr(float32(1)),
			expectedCompactIntervalSecs: uint32((24 * time.Hour).Seconds()),
		},
		{
			name:                        "1.5",
			compactIntervalDays:         base.Ptr(float32(1.5)),
			expectedCompactIntervalSecs: uint32((1.5 * 24 * time.Hour).Seconds()),
		},
		{
			name:                        "2",
			compactIntervalDays:         base.Ptr(float32(2)),
			expectedCompactIntervalSecs: uint32((2 * 24 * time.Hour).Seconds()),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			ctx := base.TestCtx(t)
			startupConfig := &StartupConfig{}
			sc := NewServerContext(ctx, startupConfig, false)
			defer sc.Close(ctx)
			config := &DbConfig{
				CompactIntervalDays: test.compactIntervalDays,
				Unsupported:         &db.UnsupportedOptions{},
			}
			opts, err := dbcOptionsFromConfig(base.TestCtx(t), sc, config, "fakedb")
			require.NoError(t, err)
			require.Equal(t, int(test.expectedCompactIntervalSecs), int(opts.CompactInterval))
		})
	}
}

func TestHeapProfileValuesPopulated(t *testing.T) {
	totalMemory := uint64(float64(getTotalMemory(base.TestCtx(t))) * 0.85)
	testCases := []struct {
		name                           string
		startupConfig                  *StartupConfig
		heapProfileCollectionThreshold uint64
		heapProfileCollectionEnabled   bool
	}{
		{
			name:                           "Default",
			startupConfig:                  &StartupConfig{},
			heapProfileCollectionThreshold: totalMemory,
			heapProfileCollectionEnabled:   true,
		},
		{
			name: "HeapProfileDisabled",
			startupConfig: &StartupConfig{
				HeapProfileDisableCollection: true,
			},
			heapProfileCollectionThreshold: totalMemory, // set but ignored
			heapProfileCollectionEnabled:   false,
		},
		{
			name: "HeapProfileCollectionThreshold",
			startupConfig: &StartupConfig{
				HeapProfileCollectionThreshold: base.Ptr(uint64(100)),
			},
			heapProfileCollectionThreshold: 100,
			heapProfileCollectionEnabled:   true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			sc := NewServerContext(ctx, test.startupConfig, false)
			defer sc.Close(ctx)
			require.Equal(t, test.heapProfileCollectionThreshold, sc.statsContext.heapProfileCollectionThreshold)
			require.Equal(t, test.heapProfileCollectionEnabled, sc.statsContext.heapProfileEnabled)
		})
	}
}

func TestDatabaseStartupFailure(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("EE only test, requires heartbeater error")
	}
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("LeakyBucketConfig not supported on CBS")
	}

	touchErr := errors.New("touch error")
	rt := NewRestTester(t, &RestTesterConfig{
		LeakyBucketConfig: &base.LeakyBucketConfig{
			TouchCallback: func(key string) error {
				if strings.Contains(key, "heartbeat_timeout") {
					return touchErr
				}
				return nil
			},
		},
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := DatabaseConfig{
		DbConfig: rt.NewDbConfig(),
	}

	// this call does use the leaky bucket
	dbContext, err := rt.ServerContext().AddDatabaseFromConfigWithBucket(rt.Context(), t, dbConfig, rt.Bucket())
	require.ErrorIs(t, err, touchErr)
	require.Nil(t, dbContext)
	require.Equal(t, "Offline", rt.GetDBState())

	require.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().TotalOnlineFatalErrors.Value())
	require.Equal(t, int64(0), rt.GetDatabase().DbStats.Database().TotalInitFatalErrors.Value())
	// assert that the db is reported with appropriate error state in all dbs
	allDbs := rt.ServerContext().allDatabaseSummaries()
	require.Len(t, allDbs, 1)
	invalDb := allDbs[0]
	require.NotNil(t, invalDb.DatabaseError)
	assert.Equal(t, db.RunStateString[db.DBOffline], invalDb.State)
	assert.Equal(t, invalDb.DatabaseError.ErrMsg, db.DatabaseErrorMap[db.DatabaseOnlineProcessError])

	// assert that you can attempt again to bring db back online again after failure
	resp := rt.SendAdminRequest(http.MethodPost, "/"+invalDb.Bucket+"/_online", "")
	RequireStatus(t, resp, http.StatusOK)
	rt.WaitForDBOnline()
}

func TestDatabaseCollectionDeletedErrorState(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	rt := NewRestTesterMultipleCollections(t, nil, 3)
	defer rt.Close()
	ctx := base.TestCtx(t)

	dbConfig := rt.ServerContext().GetDatabaseConfig("db")
	scopesConfig := GetCollectionsConfig(t, rt.TestBucket, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scope := dataStoreNames[0].ScopeName()

	// remove datastore
	b, err := base.AsGocbV2Bucket(rt.GetDatabase().Bucket)
	require.NoError(t, err)
	dsList, err := b.ListDataStores()
	require.NoError(t, err)
	require.NoError(t, b.DropDataStore(dsList[0]))

	// reload db
	err = rt.ServerContext().reloadDatabaseWithConfigLoadFromBucket(base.NewNonCancelCtx(), dbConfig.DatabaseConfig)
	require.Error(t, err)

	allDbs := rt.ServerContext().allDatabaseSummaries()
	require.Len(t, allDbs, 1)
	invalDb := allDbs[0]
	require.NotNil(t, invalDb.DatabaseError)
	assert.Equal(t, db.RunStateString[db.DBOffline], invalDb.State)
	assert.Equal(t, invalDb.DatabaseError.ErrMsg, db.DatabaseErrorMap[db.DatabaseInvalidDatastore])

	// fix db config
	deletedCollection := dsList[0].CollectionName()
	delete(scopesConfig[scope].Collections, deletedCollection)
	dbConfig.Scopes = scopesConfig
	resp := rt.CreateDatabase("db", dbConfig.DbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	allDbs = rt.ServerContext().allDatabaseSummaries()
	require.Len(t, allDbs, 1)
	invalDb = allDbs[0]
	require.Nil(t, invalDb.DatabaseError)
	assert.Equal(t, db.RunStateString[db.DBOnline], invalDb.State)

	// add back the datastore
	b, err = base.AsGocbV2Bucket(rt.GetDatabase().Bucket)
	require.NoError(t, err)
	err = b.CreateDataStore(ctx, dsList[0])
	require.NoError(t, err)

	// try creating db with bad collections config ensure it fails and isn't shown on all dbs
	scopesConfig[scope] = ScopeConfig{
		Collections: map[string]*CollectionConfig{
			"badCollection": {},
		},
	}
	dbConfig.Scopes = scopesConfig
	dbConfig.Name = "db2"
	resp = rt.CreateDatabase("db2", dbConfig.DbConfig)
	RequireStatus(t, resp, http.StatusForbidden)

	allDbs = rt.ServerContext().allDatabaseSummaries()
	require.Len(t, allDbs, 1)
}

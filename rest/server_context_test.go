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

// Tests the ConfigServer feature.
func TestConfigServer(t *testing.T) {
	fakeConfigURL := "http://example.com/config"
	mockClient := NewMockClient()
	mockClient.RespondToGET(fakeConfigURL+"/db2", MakeResponse(http.StatusOK, nil,
		`{
			"bucket": "fivez",
			"server": "walrus:/fake"
		}`))

	rt := NewRestTester(t, nil)
	defer rt.Close()

	sc := rt.ServerContext()
	sc.HTTPClient = mockClient.Client
	sc.config.ConfigServer = &fakeConfigURL

	dbc, err := sc.GetDatabase("db")
	assert.NoError(t, err)
	assert.Equal(t, "db", dbc.Name)

	dbc, err = sc.GetDatabase("db2")
	assert.NoError(t, err)
	assert.Equal(t, "db2", dbc.Name)
	assert.Equal(t, "fivez", dbc.Bucket.GetName())

	rt.Bucket() // no-op that just keeps rt from being GC'd/finalized (bug CBL-9)
}

// Tests the ConfigServer feature.
func TestConfigServerWithSyncFunction(t *testing.T) {
	fakeConfigURL := "http://example.com/config"
	fakeConfig := `{
			"bucket": "fivez",
			"server": "walrus:/fake",
			"sync":%s%s%s
		}`

	fakeSyncFunction := `
      function(doc, oldDoc) {
        if (doc.type == "reject_me") {
	      throw({forbidden : "Rejected document"})
        } else if (doc.type == "bar") {
	  // add "bar" docs to the "important" channel
            channel("important");
	} else if (doc.type == "secret") {
          if (!doc.owner) {
            throw({forbidden : "Secret documents must have an owner field"})
          }
	} else {
	    // all other documents just go into all channels listed in the doc["channels"] field
	    channel(doc.channels)
	}
      }
    `
	//Create config with embedded sync function in back quotes
	responseBody := fmt.Sprintf(fakeConfig, "`", fakeSyncFunction, "`")

	mockClient := NewMockClient()
	mockClient.RespondToGET(fakeConfigURL+"/db2", MakeResponse(200, nil, responseBody))

	rt := NewRestTester(t, nil)
	defer rt.Close()

	sc := rt.ServerContext()
	sc.HTTPClient = mockClient.Client
	sc.config.ConfigServer = &fakeConfigURL

	dbc, err := sc.GetDatabase("db")
	assert.NoError(t, err)
	assert.Equal(t, "db", dbc.Name)

	dbc, err = sc.GetDatabase("db2")
	assert.NoError(t, err)
	assert.Equal(t, "db2", dbc.Name)
	assert.Equal(t, "fivez", dbc.Bucket.GetName())

	rt.Bucket() // no-op that just keeps rt from being GC'd/finalized (bug CBL-9)

}

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

	serverConfig := &ServerConfig{CORS: &CORSConfig{}, AdminInterface: &DefaultAdminInterface}
	serverContext := NewServerContext(serverConfig)
	defer serverContext.Close()

	xattrs := base.TestUseXattrs()
	dbConfig := &DbConfig{BucketConfig: bucketConfigFromTestBucket(tb1), Name: "imdb1", AllowEmptyPassword: true, NumIndexReplicas: base.UintPtr(0), EnableXattrs: &xattrs}
	_, err := serverContext.AddDatabaseFromConfig(dbConfig)
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")

	dbConfig = &DbConfig{BucketConfig: bucketConfigFromTestBucket(tb2), Name: "imdb2", AllowEmptyPassword: true, NumIndexReplicas: base.UintPtr(0), EnableXattrs: &xattrs}
	_, err = serverContext.AddDatabaseFromConfig(dbConfig)
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
	serverConfig := &ServerConfig{CORS: &CORSConfig{}, AdminInterface: &DefaultAdminInterface}
	serverContext := NewServerContext(serverConfig)
	defer serverContext.Close()

	oldRevExpirySeconds := uint32(600)
	localDocExpirySecs := uint32(60 * 60 * 24 * 10) // 10 days in seconds

	// Get or add database name from config without valid database name; throws 400 Illegal database name error
	dbConfig := &DbConfig{OldRevExpirySeconds: &oldRevExpirySeconds, LocalDocExpirySecs: &localDocExpirySecs}
	dbContext, err := serverContext._getOrAddDatabaseFromConfig(dbConfig, false)
	assert.Nil(t, dbContext, "Can't create database context without a valid database name")
	assert.Error(t, err, "It should throw 400 Illegal database name")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Get or add database from config with duplicate database name and useExisting as false.
	server := "walrus:"
	bucketName := "imbucket"
	databaseName := "imdb"

	// Get or add database from config with unrecognized value for import_docs.
	dbConfig = &DbConfig{
		Name:                "imdb",
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          "Unknown",
		BucketConfig:        BucketConfig{Server: &server, Bucket: &bucketName},
	}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(dbConfig, false)
	assert.Nil(t, dbContext, "Can't create database context from config with unrecognized value for import_docs")
	assert.Error(t, err, "It should throw Unrecognized value for import_docs")

	bucketConfig := BucketConfig{Server: &server, Bucket: &bucketName}
	dbConfig = &DbConfig{BucketConfig: bucketConfig, Name: databaseName, AllowEmptyPassword: true}
	dbContext, err = serverContext.AddDatabaseFromConfig(dbConfig)

	assert.NoError(t, err, "No error while adding database to server context")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)

	dbConfig = &DbConfig{
		Name:                databaseName,
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          false}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(dbConfig, false)
	assert.Nil(t, dbContext, "Can't create database context with duplicate database name")
	assert.Error(t, err, "It should throw 412 Duplicate database names")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusPreconditionFailed))

	// Get or add database from config with duplicate database name and useExisting as true
	// Existing database context should be returned
	dbContext, err = serverContext._getOrAddDatabaseFromConfig(dbConfig, true)
	assert.NoError(t, err, "No error while trying to get the existing database name")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)
}

func TestStatsLoggerStopped(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	// Start up stats logger by creating server context
	ctx := NewServerContext(&ServerConfig{})

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

	ctx := NewServerContext(&ServerConfig{})
	defer ctx.Close()

	eps, err := ctx.ObtainManagementEndpoints()
	assert.NoError(t, err)

	clusterAddress, _, _, _, _ := ctx.tempConnectionDetails()
	baseSpec, err := connstr.Parse(clusterAddress)
	require.NoError(t, err)

	spec, err := connstr.Resolve(baseSpec)
	require.NoError(t, err)

	for _, httpHost := range spec.HttpHosts {
		formatted := fmt.Sprintf("http://%s:%d", httpHost.Host, httpHost.Port)
		assert.Contains(t, eps, formatted)
	}
}

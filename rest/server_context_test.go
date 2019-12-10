//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
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
	rt.NoFlush = true
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
	rt.NoFlush = true
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

func TestAllDatabaseNames(t *testing.T) {
	server := "walrus:"
	bucketName := "imbucket"

	serverConfig := &ServerConfig{CORS: &CORSConfig{}, AdminInterface: &DefaultAdminInterface}
	serverContext := NewServerContext(serverConfig)
	bucketConfig := BucketConfig{Server: &server, Bucket: &bucketName}

	dbConfig := &DbConfig{BucketConfig: bucketConfig, Name: "imdb1", AllowEmptyPassword: true}
	dbContext, err := serverContext.AddDatabaseFromConfig(dbConfig)
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")

	dbConfig = &DbConfig{BucketConfig: bucketConfig, Name: "imdb2", AllowEmptyPassword: true}
	dbContext, err = serverContext.AddDatabaseFromConfig(dbConfig)
	assert.NoError(t, err, "No error while adding database to server context")
	assert.Equal(t, server, dbContext.BucketSpec.Server)
	assert.Equal(t, bucketName, dbContext.BucketSpec.BucketName)
	assert.Len(t, serverContext.AllDatabaseNames(), 2)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb2")

	status := serverContext.RemoveDatabase("imdb2")
	assert.True(t, status, "Database should be removed from server context")
	assert.Len(t, serverContext.AllDatabaseNames(), 1)
	assert.Contains(t, serverContext.AllDatabaseNames(), "imdb1")
	assert.NotContains(t, serverContext.AllDatabaseNames(), "imdb2")
}

func TestStartReplicators(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyReplicate)()
	var replications []*ReplicationConfig

	// Should be skipped; create_target option is not currently supported.
	replications = append(replications, &ReplicationConfig{
		Source:        "http://127.0.0.1:4985/imdb_us",
		Target:        "http://127.0.0.1:4985/imdb_uk",
		Async:         true,
		CreateTarget:  true,
		ReplicationId: "58a632bb8d7e110445d3d65a98365d61"})

	// Should be skipped; doc_ids option is not currently supported
	replications = append(replications, &ReplicationConfig{
		Source:        "http://127.0.0.1:4985/imdb_us",
		Target:        "http://127.0.0.1:4985/imdb_uk",
		Async:         true,
		DocIds:        []string{"doc1", "doc2", "doc3", "doc4"},
		ReplicationId: "58a632bb8d7e110445d3d65a98365d62"})

	// Should be skipped; proxy option is not currently supported.
	replications = append(replications, &ReplicationConfig{
		Source:        "http://127.0.0.1:4985/imdb_us",
		Target:        "http://127.0.0.1:4985/imdb_uk",
		Async:         true,
		Proxy:         "http://127.0.0.1:443/proxy",
		ReplicationId: "58a632bb8d7e110445d3d65a98365d63"})

	// Start a continuous fake replication; it should be added to active tasks
	replications = append(replications, &ReplicationConfig{
		Source:        "http://127.0.0.1:4985/imdb_us",
		Target:        "http://127.0.0.1:4985/imdb_uk",
		Continuous:    true,
		ReplicationId: "58a632bb8d7e110445d3d65a98365d64"})

	serverConfig := &ServerConfig{AdminInterface: &DefaultAdminInterface, Replications: replications}
	serverContext := NewServerContext(serverConfig)
	serverContext.startReplicators()

	activeTasks := serverContext.replicator.ActiveTasks()
	log.Printf("activeTasks: %v", activeTasks)
	assert.Equal(t, 1, len(activeTasks))
	activeTask := activeTasks[0]
	assert.Equal(t, "58a632bb8d7e110445d3d65a98365d64", activeTask.ReplicationID)
	serverContext.Close()
}

func TestServerContextReplicators(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyReplicate)()
	var replications []*ReplicationConfig

	// Start a continuous fake replication.
	replications = append(replications, &ReplicationConfig{
		Source:        "http://127.0.0.1:4987/imdb_us",
		Target:        "http://127.0.0.1:4987/imdb_uk",
		Continuous:    true,
		ReplicationId: "58a632bb8d7e110445d3d65a98365d62"})

	serverConfig := &ServerConfig{AdminInterface: &DefaultAdminInterface, Replications: replications}
	serverContext := NewServerContext(serverConfig)
	serverContext.startReplicators()

	activeTasks := serverContext.replicator.ActiveTasks()
	log.Printf("activeTasks: %v", activeTasks)
	assert.Equal(t, 1, len(activeTasks))
	activeTask := activeTasks[0]
	assert.Equal(t, "58a632bb8d7e110445d3d65a98365d62", activeTask.ReplicationID)
	serverContext.Close()
}

func TestGetOrAddDatabaseFromConfig(t *testing.T) {
	serverConfig := &ServerConfig{CORS: &CORSConfig{}, AdminInterface: &DefaultAdminInterface}
	serverContext := NewServerContext(serverConfig)
	oldRevExpirySeconds := uint32(600)
	localDocExpirySecs := uint32(60 * 60 * 24 * 10) // 10 days in seconds

	// Get or add database name from config without valid database name; throws 400 Illegal database name error
	dbConfig := &DbConfig{OldRevExpirySeconds: &oldRevExpirySeconds, LocalDocExpirySecs: &localDocExpirySecs}
	dbContext, err := serverContext._getOrAddDatabaseFromConfig(dbConfig, false)
	assert.Nil(t, dbContext, "Can't create database context without a valid database name")
	assert.Error(t, err, "It should throw 400 Illegal database name")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Get or add database from config with unrecognized value for import_docs.
	dbConfig = &DbConfig{
		Name:                "imdb",
		OldRevExpirySeconds: &oldRevExpirySeconds,
		LocalDocExpirySecs:  &localDocExpirySecs,
		AutoImport:          "Unknown"}

	dbContext, err = serverContext._getOrAddDatabaseFromConfig(dbConfig, false)
	assert.Nil(t, dbContext, "Can't create database context from config with unrecognized value for import_docs")
	assert.Error(t, err, "It should throw Unrecognized value for import_docs")

	// Get or add database from config with duplicate database name and useExisting as false.
	server := "walrus:"
	bucketName := "imbucket"
	databaseName := "imdb"

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
	dbContext.Close()
}

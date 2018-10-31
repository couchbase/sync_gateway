package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/couchbase/mobile-service"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

// Integration tests that verify that Sync Gateway loads the correct configuration from the mobile-service

func TestGatewayLoadDbConfigBeforeStartup(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test only works with a Couchbase server")
	}

	// Create a test helper that initializes the testing bootstrap config and MetaKV Client
	testHelper := NewSGIntegrationTestHelper(t)

	// Add listener with resttester interfaces and general config to metakv
	testHelper.InsertGeneralListenerTestConfig()

	// Add metakv database config
	dbKey := fmt.Sprintf("%s/%s", mobile_service.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := testHelper.MetaKVClient.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Start a gateway in resttester mode
	gw, err := StartSyncGateway(*testHelper.BootstrapConfig)
	defer gw.Close()
	if err != nil {
		t.Fatalf("Error starting gateway: %+v", err)
	}

	// Send in-memory request to sync gateway to validate that it knows about the db config
	resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
	db := Database{}
	respBody := resp.Body.Bytes()
	if err := json.Unmarshal(respBody, &db); err != nil {
		t.Fatalf("Error getting db config.  Error: %v", err)
	}
	log.Printf("db: %+v", db)
	assert.Equals(t, db.DbName, base.DefaultTestBucketname)

}

func TestGatewayLoadDbConfigAfterStartup(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test only works with a Couchbase server")
	}

	// Create a test helper that initializes the testing bootstrap config and MetaKV Client
	testHelper := NewSGIntegrationTestHelper(t)

	// Add listener with resttester interfaces and general config to metakv
	testHelper.InsertGeneralListenerTestConfig()

	// Start a gateway in resttester mode
	gw, err := StartSyncGateway(*testHelper.BootstrapConfig)
	defer gw.Close()
	if err != nil {
		t.Fatalf("Error starting gateway: %+v", err)
	}

	// Verify that there are no db's listed under _config endpoint yet, since none have been added)
	resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
	assert.Equals(t, resp.Result().StatusCode, 404)

	// Add a database config to metakv
	dbKey := fmt.Sprintf("%s/%s", mobile_service.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := testHelper.MetaKVClient.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Polling loop until /db returns the expected db config
	getDatabaseStatus := func() *TestResponse {
		resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
		return resp
	}

	if err := WaitForResponseCode(200, getDatabaseStatus); err != nil {
		t.Fatalf("Error waiting for expected response code: %v", err)
	}

}

func TestGatewayUpdateDeleteDbConfig(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test only works with a Couchbase server")
	}

	// Create a test helper that initializes the testing bootstrap config and MetaKV Client
	testHelper := NewSGIntegrationTestHelper(t)

	// Add listener with resttester interfaces and general config to metakv
	testHelper.InsertGeneralListenerTestConfig()

	// Add metakv database config
	dbKey := fmt.Sprintf("%s/%s", mobile_service.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := testHelper.MetaKVClient.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Start a gateway in resttester mode
	gw, err := StartSyncGateway(*testHelper.BootstrapConfig)
	defer gw.Close()
	if err != nil {
		t.Fatalf("Error starting gateway: %+v", err)
	}

	// Update the db config in metakv, wait for updates to appear
	updatedRevsLimit := uint32(350)
	updatedDbConfig := fmt.Sprintf(`{
		  "num_index_replicas":0,
		  "feed_type":"DCP",
		  "bucket":"%s",
		  "enable_shared_bucket_access":false,
		  "revs_limit":%d,
		  "allow_empty_password":true,
		  "use_views": true
		}`, base.DefaultTestBucketname, updatedRevsLimit)

	if err := testHelper.MetaKVClient.Upsert(dbKey, []byte(updatedDbConfig)); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	getConfig := func() *TestResponse {
		resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/_config"), "")
		return resp
	}

	expectation := func(resp *TestResponse) bool {
		serverConfig := ServerConfig{}
		if err := json.Unmarshal(resp.Body.Bytes(), &serverConfig); err != nil {
			t.Fatalf("Error getting db config.  Error: %v", err)
		}
		dbConfig, ok := serverConfig.Databases[base.DefaultTestBucketname]
		if !ok {
			return false
		}
		return *dbConfig.RevsLimit == updatedRevsLimit
	}

	if err := WaitForExpectation(expectation, getConfig); err != nil {
		t.Fatalf("Error waiting for expected response: %v", err)
	}

	// Delete the db from metakv, wait for 404 status on the db endpoint
	if err := testHelper.MetaKVClient.Delete(dbKey); err != nil {
		t.Fatalf("Error deleting metakv key.  Error: %v", err)
	}

	// Invoke the /<db-name>/ REST API endpoint
	getDatabaseStatus := func() *TestResponse {
		resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
		return resp
	}

	if err := WaitForResponseCode(404, getDatabaseStatus); err != nil {
		t.Fatalf("Error waiting for expected response code: %v", err)
	}

}

// ----------- Test Helper

type SGIntegrationTestHelper struct {
	BootstrapConfig *BootstrapConfig
	MetaKVClient    *MetaKVClient
	Test            *testing.T
}

func NewSGIntegrationTestHelper(t *testing.T) *SGIntegrationTestHelper {

	bootstrapConfig := GetTestBootstrapConfigOrPanic()

	metakvHelper := NewMetaKVClient(*bootstrapConfig)

	// Remove all existing config from metakv
	if err := metakvHelper.RecursiveDelete(mobile_service.KeyDirMobileRoot); err != nil {
		t.Fatalf("Error deleting metakv key.  Error: %v", err)
	}

	return &SGIntegrationTestHelper{
		BootstrapConfig: bootstrapConfig,
		MetaKVClient:    metakvHelper,
		Test:            t,
	}
}

func (ith *SGIntegrationTestHelper) InsertGeneralListenerTestConfig() {

	// Add metakv general config
	if err := ith.MetaKVClient.Upsert(mobile_service.KeyMobileGatewayGeneral, []byte(DefaultMetaKVGeneralConfig())); err != nil {
		ith.Test.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Add metakv listener config
	if err := ith.MetaKVClient.Upsert(mobile_service.KeyMobileGatewayListener, []byte(InMemoryListenerConfig())); err != nil {
		ith.Test.Fatalf("Error updating metakv key.  Error: %v", err)
	}

}

type RestApiCall func() *TestResponse
type ResponseExpectation func(resp *TestResponse) bool

func WaitForResponseCode(expectedResponseCode int, apiCall RestApiCall) error {

	expectation := func(resp *TestResponse) bool {
		return resp.Result().StatusCode == expectedResponseCode
	}
	return WaitForExpectation(expectation, apiCall)

}

func WaitForExpectation(expectation ResponseExpectation, apiCall RestApiCall) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		resp := apiCall()
		if expectation(resp) {
			return false, nil, resp
		}
		return true, fmt.Errorf("Did not get expected response"), resp
	}

	err, _ := base.RetryLoop(
		"WaitForResponseCode",
		worker,
		base.CreateMaxDoublingSleeperFunc(15, 50, 1000),
	)

	return err

}

func GetTestBootstrapConfigOrPanic() (config *BootstrapConfig) {

	bootstrapConfig, err := NewGatewayBootstrapConfig(base.UnitTestUrl())

	if err != nil {
		panic(fmt.Sprintf("error getting bootstrapConfig: %v", err))
	}

	// TODO: these should use lower user privelages, but this will need to get auto-created by
	// TODO: the setup script first
	bootstrapConfig.CBUsername = base.DefaultCouchbaseAdministrator
	bootstrapConfig.CBPassword = base.DefaultCouchbasePassword

	bootstrapConfig.Uuid = fmt.Sprintf("%d", time.Now().Unix())

	return bootstrapConfig

}

func SendAdminRequest(gw *SyncGateway, method, resource string, body string) *TestResponse {

	// TODO: don't recreate admin handler for every request

	// TODO: merge w/ RestTester

	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := &TestResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	CreateAdminHandler(gw.ServerContext).ServeHTTP(response, request)
	return response
}

func DefaultMetaKVGeneralConfig() string {

	return `
{
  "logging": {
    "console": {
      "log_level": "trace",
      "log_keys": ["HTTP", "Query"]
    }
  },
  "compressResponses": false
}
`

}

// With empty interfaces, sync gateway will
func InMemoryListenerConfig() string {
	return fmt.Sprintf(`
{
    "interface": "%s",
    "adminInterface": "%s"
}
`, base.RestTesterInterface, base.RestTesterInterface)
}

func DefaultMetaKVListenerConfig() string {
	return fmt.Sprintf(`
{
    "interface": "%s:%d",
    "adminInterface": "%s:%d"
}
`, DefaultPublicInterface, DefaultPublicPort, DefaultAdminInterface, DefaultAdminPort)

}

func DefaultMetaKVDbConfig() string {

	// Using "use_views: true" due to a strange issue I'm seeing when running
	// GSI when building couchbase from source: https://gist.github.com/tleyden/46c2a2a4dfe79a2cdd759aaf22a4b88d
	// I'm not seeing this when using a toy build: http://server.jenkins.couchbase.com/view/Toys/job/toy-unix/3474/artifact/couchbase-server-enterprise-6.5.0-10002-centos7.x86_64.rpm

	return fmt.Sprintf(`
{
  "num_index_replicas":0,
  "feed_type":"DCP",
  "bucket":"%s",
  "enable_shared_bucket_access":false,
  "revs_limit":250,
  "allow_empty_password":true,
  "use_views": true  
}
`, base.DefaultTestBucketname)

}

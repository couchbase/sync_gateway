package sync_gateway

import (
	"fmt"
	"testing"
	"time"

	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/couchbase/mobile-service"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
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
	dbKey := fmt.Sprintf("%s/%s", mobile_mds.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := testHelper.MetaKVClient.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Start a gateway in resttester mode
	gw, err := StartGateway(*testHelper.BootstrapConfig)
	if err != nil {
		t.Fatalf("Error starting gateway: %+v", err)
	}

	// Send in-memory request to sync gateway to validate that it knows about the db config
	resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
	db := rest.Database{}
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
	gw, err := StartGateway(*testHelper.BootstrapConfig)
	if err != nil {
		t.Fatalf("Error starting gateway: %+v", err)
	}

	// Verify that there are no db's listed under _config endpoint yet, since none have been added)
	resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
	assert.Equals(t, resp.Result().StatusCode, 404)

	// Add a database config to metakv
	dbKey := fmt.Sprintf("%s/%s", mobile_mds.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := testHelper.MetaKVClient.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Polling loop until /db returns the expected db config
	retryFunc := func() *rest.TestResponse {
		resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
		return resp
	}

	if err := WaitForResponseCode(200, retryFunc); err != nil {
		t.Fatalf("Error waiting for expected response code: %v", err)
	}

	}


	// TODO: remove db from metakv and see if it's gone, update existing



// ----------- Test Helper

type SGIntegrationTestHelper struct {
	BootstrapConfig *GatewayBootstrapConfig
	MetaKVClient    *MetaKVClient
	Test            *testing.T
}

func NewSGIntegrationTestHelper(t *testing.T) *SGIntegrationTestHelper {

	bootstrapConfig := GetTestBootstrapConfigOrPanic()

	metakvHelper := NewMetaKVClient(*bootstrapConfig)

	// Remove all existing config from metakv
	if err := metakvHelper.RecursiveDelete(mobile_mds.KeyDirMobileRoot); err != nil {
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
	if err := ith.MetaKVClient.Upsert(mobile_mds.KeyMobileGatewayGeneral, []byte(DefaultMetaKVGeneralConfig())); err != nil {
		ith.Test.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Add metakv listener config
	if err := ith.MetaKVClient.Upsert(mobile_mds.KeyMobileGatewayListener, []byte(InMemoryListenerConfig())); err != nil {
		ith.Test.Fatalf("Error updating metakv key.  Error: %v", err)
	}

}

type RestApiCall func() *rest.TestResponse

func WaitForResponseCode(expectedResponseCode int, apiCall RestApiCall) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		resp := apiCall()
		if resp.Result().StatusCode == expectedResponseCode {
			return false, nil, resp
		}
		log.Printf("Expected response code %d but got %d. Retrying.", expectedResponseCode, resp.Result().StatusCode)
		return true, fmt.Errorf("Expected response code %d but got %d", expectedResponseCode, resp.Result().StatusCode), resp
	}

	err, _ := base.RetryLoop(
		"WaitForResponseCode",
		worker,
		base.CreateMaxDoublingSleeperFunc(25, 50, 250),
	)

	return err

}

func GetTestBootstrapConfigOrPanic() (config *GatewayBootstrapConfig) {

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

func CreateAdminHandler(gw *Gateway) http.Handler {
	return rest.CreateAdminHandler(gw.ServerContext)
}

func SendAdminRequest(gw *Gateway, method, resource string, body string) *rest.TestResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := &rest.TestResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	CreateAdminHandler(gw).ServeHTTP(response, request)
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

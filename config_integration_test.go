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

func TestGatewayLoadDbConfig(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test only works with a Couchbase server")
	}

	bootstrapConfig, err := NewGatewayBootstrapConfig(base.UnitTestUrl())

	// TODO: these should use lower user privelages, but this will need to get auto-created by
	// TODO: the setup script first
	bootstrapConfig.CBUsername = base.DefaultCouchbaseAdministrator
	bootstrapConfig.CBPassword = base.DefaultCouchbasePassword

	assert.True(t, err == nil)

	bootstrapConfig.Uuid = fmt.Sprintf("%d", time.Now().Unix())

	// Create metakv helper
	metakvHelper := NewMetaKVClient(*bootstrapConfig)

	// Remove all config from metakv
	if err := metakvHelper.RecursiveDelete(mobile_mds.KeyDirMobileRoot); err != nil {
		t.Fatalf("Error deleting metakv key.  Error: %v", err)
	}

	// Add metakv general config
	if err := metakvHelper.Upsert(mobile_mds.KeyMobileGatewayGeneral, []byte(DefaultMetaKVGeneralConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Add metakv listener config
	if err := metakvHelper.Upsert(mobile_mds.KeyMobileGatewayListener, []byte(InMemoryListenerConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Add metakv database config
	dbKey := fmt.Sprintf("%s/%s", mobile_mds.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := metakvHelper.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	gw := StartGateway(*bootstrapConfig)

	// artificial delay in case the config exchange still in progress
	// TODO: if it's updated to synchronously get db config during startup, this won't be needed
	time.Sleep(5 * time.Second)

	resp := SendAdminRequest(gw, "GET", fmt.Sprintf("/%s/", base.DefaultTestBucketname), "")
	dbConfig := rest.DbConfig{}
	respBody := resp.Body.Bytes()
	log.Printf("Raw body: %v", string(respBody))
	if err := json.Unmarshal(respBody, &dbConfig); err != nil {
		t.Fatalf("Error getting db config.  Error: %v", err)
	}
	log.Printf("dbConfig: %+v", dbConfig)

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

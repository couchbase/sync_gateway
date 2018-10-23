package sync_gateway

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/mobile-service"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbase/sync_gateway/sgclient"
	"log"
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
	if err := metakvHelper.Upsert(mobile_mds.KeyMobileGatewayListener, []byte(DefaultMetaKVListenerConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	// Add metakv database config
	dbKey := fmt.Sprintf("%s/%s", mobile_mds.KeyMobileGatewayDatabases, base.DefaultTestBucketname)
	if err := metakvHelper.Upsert(dbKey, []byte(DefaultMetaKVDbConfig())); err != nil {
		t.Fatalf("Error updating metakv key.  Error: %v", err)
	}

	go RunGateway(*bootstrapConfig, true)

	// Query the admin api at the _config endpoint, and make sure it lists the db added above
	sgClient := sgclient.NewSgClient(fmt.Sprintf("%s:%d", DefaultAdminInterface, DefaultAdminPort))
	if err := sgClient.WaitApiAvailable(); err != nil {
		t.Fatalf("Error waiting for api to become available.  Error: %v", err)

	}
	dbEndpoint, err := sgClient.GetDb(base.DefaultTestBucketname)
	if err != nil {
		t.Fatalf("Unable to get db endpoint: %v", base.DefaultTestBucketname)
	}
	log.Printf("dbEndpoint: %+v", dbEndpoint)



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

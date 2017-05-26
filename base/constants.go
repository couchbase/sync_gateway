package base

import (
	"os"
	"strings"
)

const (

	// The username of the special "GUEST" user
	GuestUsername = "GUEST"
	ISO8601Format = "2006-01-02T15:04:05.000Z07:00"

	kTestCouchbaseServerURL = "http://localhost:8091"
	kTestWalrusURL          = "walrus:"

	// These settings are used when running unit tests against a live Couchbase Server to create/flush buckets
	DefaultCouchbaseAdministrator = "Administrator"
	DefaultCouchbasePassword      = "password"

	// Couchbase 5.x instructions:
	//   1. Create an RBAC user with these credentials
	//   2. Create a default bucket (if doesn't already exist)
	// Couchbase 4.x instructions:
	//   Err: Does this even work with Couchbase 4.x?   How do you assign a password on the default bucket
	DefaultTestBucketname = "test_data_bucket"
	DefaultTestUsername   = DefaultTestBucketname
	DefaultTestPassword   = "password"

	DefaultTestShadowBucketname = "shadowbucket"
	DefaultTestShadowUsername   = DefaultTestShadowBucketname
	DefaultTestShadowPassword   = DefaultTestPassword

	DefaultTestIndexBucketname = "test_indexbucket"
	DefaultTestIndexUsername = DefaultTestIndexBucketname
	DefaultTestIndexPassword = DefaultTestPassword

	// Walrus by default, but can set to "Couchbase" to have it use http://localhost:8091
	TestEnvSyncGatewayBackingStore = "SG_TEST_BACKING_STORE"
	TestEnvBackingStoreCouchbase   = "Couchbase"

	// Don't use Xattrs by default, but provide the test runner a way to specify Xattr usage
	TestEnvSyncGatewayUseXattrs = "SG_TEST_USE_XATTRS"
	TestEnvSyncGatewayTrue      = "True"

	// Don't use an auth handler by default, but provide a way to override
	TestEnvSyncGatewayUseAuthHandler = "SG_TEST_USE_AUTH_HANDLER"

	DefaultUseXattrs = false // Whether Sync Gateway uses xattrs for metadata storage, if not specified in the config

)

func UnitTestUrl() string {
	backingStore := os.Getenv(TestEnvSyncGatewayBackingStore)
	switch {
	case strings.ToLower(backingStore) == strings.ToLower(TestEnvBackingStoreCouchbase):
		return kTestCouchbaseServerURL
	default:
		return kTestWalrusURL
	}
}

func UnitTestUrlIsWalrus() bool {
	unitTestUrl := UnitTestUrl()
	return strings.Contains(unitTestUrl, kTestWalrusURL)
}

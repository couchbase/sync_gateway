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

	// Couchbase 5.x notes:
	// For every bucket that the tests will create (DefaultTestBucketname, DefaultTestShadowBucketname, DefaultTestIndexBucketname):
	//   1. Create an RBAC user with username equal to the bucket name
	//   2. Set the password to DefaultTestPassword
	//   3. Give "Admin" RBAC rights

	DefaultTestBucketname = "test_data_bucket"
	DefaultTestUsername   = DefaultTestBucketname
	DefaultTestPassword   = "password"

	DefaultTestShadowBucketname = "test_shadowbucket"
	DefaultTestShadowUsername   = DefaultTestShadowBucketname
	DefaultTestShadowPassword   = DefaultTestPassword

	DefaultTestIndexBucketname = "test_indexbucket"
	DefaultTestIndexUsername = DefaultTestIndexBucketname
	DefaultTestIndexPassword = DefaultTestPassword

	// Env variable to enable user to override the Couchbase Server URL used in tests
	TestEnvCouchbaseServerUrl = "SG_TEST_COUCHBASE_SERVER_URL"

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
		testCouchbaseServerUrl := os.Getenv(TestEnvCouchbaseServerUrl)
		if testCouchbaseServerUrl != "" {
			// If user explicitly set a Test Couchbase Server URL, use that
			return testCouchbaseServerUrl
		}
		// Otherwise fallback to hardcoded default
		return kTestCouchbaseServerURL
	default:
		return kTestWalrusURL
	}
}

func UnitTestUrlIsWalrus() bool {
	unitTestUrl := UnitTestUrl()
	return strings.Contains(unitTestUrl, kTestWalrusURL)
}

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

	kTestUseAuthHandler = false
	kTestUsername       = "sync_gateway_tests"
	kTestPassword       = "password"
	kTestBucketname     = "sync_gateway_tests"

	// Walrus by default, but can set to "Couchbase" to have it use http://localhost:8091
	TestEnvSyncGatewayBackingStore = "SG_TEST_BACKING_STORE"
	TestEnvBackingStoreCouchbase   = "Couchbase"

	// Don't use Xattrs by default, but provide the test runner to specify Xattr usage
	TestEnvSyncGatewayUseXattrs = "SG_TEST_USE_XATTRS"
	TestEnvSyncGatewayTrue = "True"
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

func UnitTestAuthHandler() AuthHandler {
	if !kTestUseAuthHandler {
		return nil
	} else {
		return &UnitTestAuth{
			username:   kTestUsername,
			password:   kTestPassword,
			bucketname: kTestBucketname,
		}
	}
}

type UnitTestAuth struct {
	username   string
	password   string
	bucketname string
}

func (u *UnitTestAuth) GetCredentials() (string, string, string) {
	return TransformBucketCredentials(u.username, u.password, u.bucketname)
}

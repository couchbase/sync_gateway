package base

import (
	"os"
	"strings"
	"time"
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
	DefaultTestIndexUsername   = DefaultTestIndexBucketname
	DefaultTestIndexPassword   = DefaultTestPassword

	// Env variable to enable user to override the Couchbase Server URL used in tests
	TestEnvCouchbaseServerUrl = "SG_TEST_COUCHBASE_SERVER_URL"

	// Walrus by default, but can set to "Couchbase" to have it use http://localhost:8091
	TestEnvSyncGatewayBackingStore = "SG_TEST_BACKING_STORE"
	TestEnvBackingStoreCouchbase   = "Couchbase"

	// Don't use Xattrs by default, but provide the test runner a way to specify Xattr usage
	TestEnvSyncGatewayUseXattrs = "SG_TEST_USE_XATTRS"
	TestEnvSyncGatewayTrue      = "True"

	// Should the tests drop the GSI indexes?
	TestEnvSyncGatewayDropIndexes = "SG_TEST_DROP_INDEXES"

	// Don't use an auth handler by default, but provide a way to override
	TestEnvSyncGatewayUseAuthHandler = "SG_TEST_USE_AUTH_HANDLER"

	DefaultUseXattrs      = false // Whether Sync Gateway uses xattrs for metadata storage, if not specified in the config
	DefaultAllowConflicts = true  // Whether Sync Gateway allows revision conflicts, if not specified in the config

	DefaultDropIndexes = false // Whether Sync Gateway drops GSI indexes before each test while running in integration mode

	DefaultOldRevExpirySeconds = uint32(300)

	// Default value of _local document expiry
	DefaultLocalDocExpirySecs = uint32(60 * 60 * 24 * 90) //90 days in seconds

	DefaultViewQueryPageSize = 5000 // This must be greater than 1, or the code won't work due to windowing method

	// Until the sporadic integration tests failures in SG #3570 are fixed, should be GTE n1ql query timeout
	// to make it easier to identify root cause of test failures.
	DefaultWaitForSequenceTesting = time.Second * 30

	// Default the max number of idle connections per host to a relatively high number to avoid
	// excessive socket churn caused by opening short-lived connections and closing them after, which can cause
	// a high number of connections to end up in the TIME_WAIT state and exhaust system resources.  Since
	// GoCB is only connecting to a fixed set of Couchbase nodes, this number can be set relatively high and
	// still stay within a reasonable value.
	DefaultHttpMaxIdleConnsPerHost = "256"

	// This primarily depends on MaxIdleConnsPerHost as the limiting factor, but sets some upper limit just to avoid
	// being completely unlimited
	DefaultHttpMaxIdleConns = "64000"

	// Keep idle connections around for a maximimum of 90 seconds.  This is the same value used by the Go DefaultTransport.
	DefaultHttpIdleConnTimeoutMilliseconds = "90000"

	// Set this to true to dump stacktraces (for pkgerrors wrapped errors only) whenever an error is returned to
	// an API client.  Currently only works with REST API calls.
	StacktraceOnAPIErrors = false

	// The limit in Couchbase Server for total system xattr size
	couchbaseMaxSystemXattrSize = 1 * 1024 * 1024 // 1MB

	//==== Sync Prefix Documents & Keys ====
	SyncPrefix = "_sync:"

	AttPrefix              = SyncPrefix + "att:"
	BackfillCompletePrefix = SyncPrefix + "backfill:complete:"
	BackfillPendingPrefix  = SyncPrefix + "backfill:pending:"
	DCPCheckpointPrefix    = SyncPrefix + "dcp_ck:"
	RepairBackup           = SyncPrefix + "repair:backup:"
	RepairDryRun           = SyncPrefix + "repair:dryrun:"
	RevBodyPrefix          = SyncPrefix + "rb:"
	RevPrefix              = SyncPrefix + "rev:"
	RolePrefix             = SyncPrefix + "role:"
	SessionPrefix          = SyncPrefix + "session:"
	SyncSeqPrefix          = SyncPrefix + "seq:"
	UserEmailPrefix        = SyncPrefix + "useremail:"
	UserPrefix             = SyncPrefix + "user:"
	UnusedSeqPrefix        = SyncPrefix + "unusedSeq:"
	UnusedSeqRangePrefix   = SyncPrefix + "unusedSeqs:"

	DCPBackfillSeqKey = SyncPrefix + "dcp_backfill"
	SyncDataKey       = SyncPrefix + "syncdata"
	SyncSeqKey        = SyncPrefix + "seq"
	SyncXattrName     = "_sync"
)

const (
	SyncFnErrorMissingRole          = "sg missing role"
	SyncFnErrorAdminRequired        = "sg admin required"
	SyncFnErrorWrongUser            = "sg wrong user"
	SyncFnErrorMissingChannelAccess = "sg missing channel access"
)

var (
	SyncFnAccessErrors = []string{
		HTTPErrorf(403, SyncFnErrorMissingRole).Error(),
		HTTPErrorf(403, SyncFnErrorAdminRequired).Error(),
		HTTPErrorf(403, SyncFnErrorWrongUser).Error(),
		HTTPErrorf(403, SyncFnErrorMissingChannelAccess).Error(),
	}

	// Default warning thresholds
	DefaultWarnThresholdXattrSize      = 0.9 * float64(couchbaseMaxSystemXattrSize)
	DefaultWarnThresholdChannelsPerDoc = uint32(50)
	DefaultWarnThresholdGrantsPerDoc   = uint32(50)
)

func UnitTestUrl() string {
	if TestUseCouchbaseServer() {
		testCouchbaseServerUrl := os.Getenv(TestEnvCouchbaseServerUrl)
		if testCouchbaseServerUrl != "" {
			// If user explicitly set a Test Couchbase Server URL, use that
			return testCouchbaseServerUrl
		}
		// Otherwise fallback to hardcoded default
		return kTestCouchbaseServerURL
	} else {
		return kTestWalrusURL
	}
}

func UnitTestUrlIsWalrus() bool {
	unitTestUrl := UnitTestUrl()
	return strings.Contains(unitTestUrl, kTestWalrusURL)
}

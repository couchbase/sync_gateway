/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"os"
	"strings"
	"time"
)

const (

	// The username of the special "GUEST" user
	GuestUsername = "GUEST"
	ISO8601Format = "2006-01-02T15:04:05.000Z07:00"

	kTestCouchbaseServerURL = "couchbase://localhost"
	kTestWalrusURL          = "walrus:"

	// These settings are used when running unit tests against a live Couchbase Server to create/flush buckets
	DefaultCouchbaseAdministrator = "Administrator"
	DefaultCouchbasePassword      = "password"

	// Couchbase 5.x notes:
	// For every bucket that the tests will create (DefaultTestBucketname):
	//   1. Create an RBAC user with username equal to the bucket name
	//   2. Set the password to DefaultTestPassword
	//   3. Give "Admin" RBAC rights

	DefaultTestBucketname = "test_data_bucket"
	DefaultTestUsername   = DefaultTestBucketname
	DefaultTestPassword   = "password"

	// Env variable to enable user to override the Couchbase Server URL used in tests
	TestEnvCouchbaseServerUrl = "SG_TEST_COUCHBASE_SERVER_URL"

	// Env variable to enable skipping of TLS certificate verification for client and server
	TestEnvTLSSkipVerify     = "SG_TEST_TLS_SKIP_VERIFY"
	DefaultTestTLSSkipVerify = true

	// Walrus by default, but can set to "Couchbase" to have it use http://localhost:8091
	TestEnvSyncGatewayBackingStore = "SG_TEST_BACKING_STORE"
	TestEnvBackingStoreCouchbase   = "Couchbase"

	TestEnvUseExistingBucket = "SG_TEST_USE_EXISTING_BUCKET"

	// Don't use Xattrs by default, but provide the test runner a way to specify Xattr usage
	TestEnvSyncGatewayUseXattrs = "SG_TEST_USE_XATTRS"
	TestEnvSyncGatewayTrue      = "True"

	// Should the tests drop the GSI indexes?
	TestEnvSyncGatewayDropIndexes = "SG_TEST_DROP_INDEXES"

	// Should the tests use GSI instead of views?
	TestEnvSyncGatewayDisableGSI = "SG_TEST_USE_GSI"

	// Don't use an auth handler by default, but provide a way to override
	TestEnvSyncGatewayUseAuthHandler = "SG_TEST_USE_AUTH_HANDLER"

	// Can be used to set a global log level for all tests at runtime.
	TestEnvGlobalLogLevel = "SG_TEST_LOG_LEVEL"

	// Should x509 tests deploy certs to local macOS Couchbase Server?
	TestEnvX509Local = "SG_TEST_X509_LOCAL"

	// If TestEnvX509Local=true, must use SG_TEST_X509_LOCAL_USER to set macOS username to locate CBS cert inbox
	TestEnvX509LocalUser = "SG_TEST_X509_LOCAL_USER"

	// If set, corresponds to name of the docker image of couchbase server
	TestEnvCouchbaseServerDockerName = "SG_TEST_COUCHBASE_SERVER_DOCKER_NAME"

	// TestEnvGoroutineDump if set to true will capture a goroutine pprof profile and log the location at the end of each package
	TestEnvGoroutineDump = "SG_TEST_GOROUTINE_DUMP"

	DefaultUseXattrs      = true // Whether Sync Gateway uses xattrs for metadata storage, if not specified in the config
	DefaultAllowConflicts = true // Whether Sync Gateway allows revision conflicts, if not specified in the config

	DefaultDropIndexes = false // Whether Sync Gateway drops GSI indexes before each test while running in integration mode

	DefaultOldRevExpirySeconds = uint32(300)

	// Default value of _local document expiry
	DefaultLocalDocExpirySecs = uint32(60 * 60 * 24 * 90) // 90 days in seconds

	DefaultViewQueryPageSize = 5000 // This must be greater than 1, or the code won't work due to windowing method

	// Until the sporadic integration tests failures in SG #3570 are fixed, should be GTE n1ql query timeout
	// to make it easier to identify root cause of test failures.
	DefaultWaitForSequence = time.Second * 30

	// Default the max number of idle connections per host to a relatively high number to avoid
	// excessive socket churn caused by opening short-lived connections and closing them after, which can cause
	// a high number of connections to end up in the TIME_WAIT state and exhaust system resources.  Since
	// GoCB is only connecting to a fixed set of Couchbase nodes, this number can be set relatively high and
	// still stay within a reasonable value.
	DefaultHttpMaxIdleConnsPerHost = 256

	// This primarily depends on MaxIdleConnsPerHost as the limiting factor, but sets some upper limit just to avoid
	// being completely unlimited
	DefaultHttpMaxIdleConns = "64000"

	// Keep idle connections around for a maximimum of 90 seconds.  This is the same value used by the Go DefaultTransport.
	DefaultHttpIdleConnTimeoutMilliseconds = "90000"

	// Number of kv connections (pipelines) per Couchbase Server node
	DefaultGocbKvPoolSize = 2

	// Number of kv connections (pipelines) per Couchbase Server node when running in serverless
	DefaultGocbKvPoolSizeServerless = 1

	// kv connections buffer size per Couchbase Server node when running in serverless
	DefaultKvBufferSizeServerless = 1 * 1024 * 1024

	// Number of connections used by DCP agents - must be 1
	GoCBPoolSizeDCP = 1

	// The limit in Couchbase Server for total system xattr size
	couchbaseMaxSystemXattrSize = 1 * 1024 * 1024 // 1MB

	AttachmentCompactionXattrName = SyncXattrName + "-compact"

	// SyncPropertyName is used when storing sync data inline in a document.
	SyncPropertyName = "_sync"
	// SyncXattrName is used when storing sync data in a document's xattrs.
	SyncXattrName = "_sync"

	// Intended to be used in Meta Map and related tests
	MetaMapXattrsKey = "xattrs"

	// Prefix for transaction metadata documents
	TxnPrefix = "_txn:"

	// Replication filter constants
	ByChannelFilter = "sync_gateway/bychannel"

	// Increase default gocbv2 op timeout to match the standard SG backoff retry timing used for gocb v1
	DefaultGocbV2OperationTimeout = 10 * time.Second

	// RedactedStr can be substituted in place of any sensitive data being returned by an API. The 'xxxxx' pattern is the same used by Go's url.Redacted() method.
	RedactedStr = "xxxxx"

	// DefaultJavascriptTimeoutSecs is number of seconds before Javascript functions (i.e. the sync function or import filter) timeout
	// If set to zero, timeout is disabled.
	DefaultJavascriptTimeoutSecs = uint32(0)

	// ServerlessChannelLimit is hard limit on channels allowed per user when running in serverless mode
	ServerlessChannelLimit = 500

	// FromConnStrWarningThreshold determines the amount of time it should take before we warn about parsing a connstr (mostly for DNS resolution)
	FromConnStrWarningThreshold = 10 * time.Second
)

const (
	DefaultScope             = "_default"
	DefaultCollection        = "_default"
	ScopeCollectionSeparator = "."
)

const (
	SyncFnErrorMissingRole          = "sg missing role"
	SyncFnErrorAdminRequired        = "sg admin required"
	SyncFnErrorWrongUser            = "sg wrong user"
	SyncFnErrorMissingChannelAccess = "sg missing channel access"
)

const (
	// EmptyDocument denotes an empty document in JSON form.
	EmptyDocument = `{}`
)

const MiB = 1 * 1024 * 1024

var (
	SyncFnAccessErrors = []string{
		HTTPErrorf(403, SyncFnErrorMissingRole).Error(),
		HTTPErrorf(403, SyncFnErrorAdminRequired).Error(),
		HTTPErrorf(403, SyncFnErrorWrongUser).Error(),
		HTTPErrorf(403, SyncFnErrorMissingChannelAccess).Error(),
	}

	// Default warning thresholds
	DefaultWarnThresholdXattrSize       = 0.9 * float64(couchbaseMaxSystemXattrSize)
	DefaultWarnThresholdChannelsPerDoc  = uint32(50)
	DefaultWarnThresholdChannelsPerUser = uint32(50000)
	DefaultWarnThresholdGrantsPerDoc    = uint32(50)
	DefaultWarnThresholdChannelNameSize = uint32(250)
	DefaultClientPartitionWindow        = time.Hour * 24 * 30

	// ErrUnknownField is marked as the cause of the error when trying to decode a JSON snippet with unknown fields
	ErrUnknownField = errors.New("unrecognized JSON field")

	// MaxPrincipalNameLen is the maximum length for user and role names, accounting for internal prefixes, and is used to validate CRUD
	MaxPrincipalNameLen = 250 - Max(len(UserPrefixRoot), len(RolePrefixRoot))
)

// UnitTestUrl returns the configured test URL.
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

// UnitTestUrlIsWalrus returns true if we're running with a Walrus test URL.
func UnitTestUrlIsWalrus() bool {
	return ServerIsWalrus(UnitTestUrl())
}

// ServerIsTLS returns true if the server URL is using an accepted secure protocol as it's prefix
// Prefix checked: couchbases:
func ServerIsTLS(server string) bool {
	return strings.HasPrefix(server, "couchbases:")
}

// ServerIsWalrus returns true when the given server looks like a Walrus URI
// Equivalent to the old regexp: `^(walrus:|file:|/|\.)`
func ServerIsWalrus(server string) bool {
	return strings.HasPrefix(server, "walrus:") ||
		strings.HasPrefix(server, "file:") ||
		strings.HasPrefix(server, "/") ||
		strings.HasPrefix(server, ".")
}

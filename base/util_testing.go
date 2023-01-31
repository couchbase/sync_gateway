package base

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Code that is test-related that needs to be accessible from non-base packages, and therefore can't live in
// util_test.go, which is only accessible from the base package.

var TestExternalRevStorage = false
var numOpenBucketsByName map[string]int32
var mutexNumOpenBucketsByName sync.Mutex

func init() {

	// Prevent https://issues.couchbase.com/browse/MB-24237
	rand.Seed(time.Now().UTC().UnixNano())

	numOpenBucketsByName = map[string]int32{}

}

type TestBucket struct {
	Bucket
	BucketSpec BucketSpec
	closeFn    func()
}

func (tb TestBucket) Close() {
	tb.closeFn()
}

// NoCloseClone returns a new test bucket referencing the same underlying bucket and bucketspec, but
// with an IgnoreClose leaky bucket, and a no-op close function.  Used when multiple references to the same bucket are needed.
func (tb *TestBucket) NoCloseClone() *TestBucket {
	noCloseBucket := NewLeakyBucket(tb.Bucket, LeakyBucketConfig{IgnoreClose: true})
	return &TestBucket{
		Bucket:     noCloseBucket,
		BucketSpec: tb.BucketSpec,
		closeFn:    func() {},
	}
}

func GetTestBucket(t testing.TB) *TestBucket {
	bucket, spec, closeFn := GTestBucketPool.GetTestBucketAndSpec(t)
	return &TestBucket{
		Bucket:     bucket,
		BucketSpec: spec,
		closeFn:    closeFn,
	}
}

// Should Sync Gateway use XATTRS functionality when running unit tests?
func TestUseXattrs() bool {

	// First check if the SG_TEST_USE_XATTRS env variable is set
	useXattrs := os.Getenv(TestEnvSyncGatewayUseXattrs)
	if strings.ToLower(useXattrs) == strings.ToLower(TestEnvSyncGatewayTrue) {
		return true
	}

	// Otherwise fallback to hardcoded default
	return DefaultUseXattrs

}

// Should tests try to drop GSI indexes before flushing buckets?
// See SG #3422
func TestsShouldDropIndexes() bool {

	// First check if the SG_TEST_USE_XATTRS env variable is set
	dropIndexes := os.Getenv(TestEnvSyncGatewayDropIndexes)

	if strings.ToLower(dropIndexes) == strings.ToLower(TestEnvSyncGatewayTrue) {
		return true
	}

	// Otherwise fallback to hardcoded default
	return DefaultDropIndexes

}

// TestsDisableGSI returns true if tests should be forced to avoid any GSI-specific code.
func TestsDisableGSI() bool {
	// FIXME: CBG-813 - Re-enable GSI in integration tests after CB 6.5.1 Beta
	if true {
		return true
	}
	// Disable GSI when running with Walrus
	if !TestUseCouchbaseServer() && UnitTestUrlIsWalrus() {
		return true
	}

	disableGSI, _ := strconv.ParseBool(os.Getenv(TestEnvSyncGatewayDisableGSI))
	return disableGSI
}

// Check the whether tests are being run with SG_TEST_BACKING_STORE=Couchbase
func TestUseCouchbaseServer() bool {
	backingStore := os.Getenv(TestEnvSyncGatewayBackingStore)
	return strings.ToLower(backingStore) == strings.ToLower(TestEnvBackingStoreCouchbase)
}

type TestAuthenticator struct {
	Username   string
	Password   string
	BucketName string
}

func (t TestAuthenticator) GetCredentials() (username, password, bucketname string) {
	return t.Username, t.Password, t.BucketName
}

// Reset bucket state
func DropAllBucketIndexes(gocbBucket *CouchbaseBucketGoCB) error {

	// Retrieve all indexes
	indexes, err := getIndexes(gocbBucket)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(len(indexes))

	asyncErrors := make(chan error, len(indexes))
	defer close(asyncErrors)

	for _, index := range indexes {

		go func(indexToDrop string) {

			defer wg.Done()

			log.Printf("Dropping index %s on bucket %s...", indexToDrop, gocbBucket.Name())
			dropErr := gocbBucket.DropIndex(indexToDrop)
			if dropErr != nil {
				asyncErrors <- dropErr
				log.Printf("...failed to drop index %s on bucket %s: %s", indexToDrop, gocbBucket.Name(), dropErr)
				return
			}
			log.Printf("...successfully dropped index %s on bucket %s", indexToDrop, gocbBucket.Name())
		}(index)

	}

	// Wait until all goroutines finish
	wg.Wait()

	// Check if any errors were put into the asyncErrors channel.  If any, just return the first one
	select {
	case asyncError := <-asyncErrors:
		return asyncError
	default:
	}

	return nil
}

// Get a list of all index names in the bucket
func getIndexes(gocbBucket *CouchbaseBucketGoCB) (indexes []string, err error) {

	indexes = []string{}

	manager, err := gocbBucket.getBucketManager()
	if err != nil {
		return indexes, err
	}

	indexInfo, err := manager.GetIndexes()
	if err != nil {
		return indexes, err
	}

	for _, indexInfo := range indexInfo {
		if indexInfo.Keyspace == gocbBucket.GetName() {
			indexes = append(indexes, indexInfo.Name)
		}
	}

	return indexes, nil
}

// Generates a string of size int
const alphaNumeric = "0123456789abcdefghijklmnopqrstuvwxyz"

func CreateProperty(size int) (result string) {
	resultBytes := make([]byte, size)
	for i := 0; i < size; i++ {
		resultBytes[i] = alphaNumeric[i%len(alphaNumeric)]
	}
	return string(resultBytes)
}

// SetUpTestLogging will set the given log level and log keys,
// and return a function that can be deferred for teardown.
//
// This function will panic if called multiple times without running the teardownFn.
//
// To set multiple log keys, append as variadic arguments
// E.g. KeyCache,KeyDCP,KeySync
//
// Usage:
//
//	teardownFn := SetUpTestLogging(LevelDebug, KeyCache,KeyDCP,KeySync)
//	defer teardownFn()
//
// Shorthand style:
//
//	defer SetUpTestLogging(LevelDebug, KeyCache,KeyDCP,KeySync)()
func SetUpTestLogging(logLevel LogLevel, logKeys ...LogKey) (teardownFn func()) {
	caller := GetCallersName(1, false)
	Infof(KeyAll, "%s: Setup logging: level: %v - keys: %v", caller, logLevel, logKeys)
	return setTestLogging(logLevel, caller, logKeys...)
}

// DisableTestLogging is an alias for SetUpTestLogging(LevelNone, KeyNone)
// This function will panic if called multiple times without running the teardownFn.
func DisableTestLogging() (teardownFn func()) {
	caller := ""
	return setTestLogging(LevelNone, caller, KeyNone)
}

// SetUpBenchmarkLogging will set the given log level and key, and do log processing for that configuration,
// but discards the output, instead of writing it to console.
func SetUpBenchmarkLogging(logLevel LogLevel, logKeys ...LogKey) (teardownFn func()) {
	teardownFnOrig := setTestLogging(logLevel, "", logKeys...)

	// discard all logging output for benchmarking (but still execute logging as normal)
	consoleLogger.logger.SetOutput(ioutil.Discard)
	return func() {
		// revert back to original output
		if consoleLogger != nil && consoleLogger.output != nil {
			consoleLogger.logger.SetOutput(consoleLogger.output)
		} else {
			consoleLogger.logger.SetOutput(os.Stderr)
		}
		teardownFnOrig()
	}
}

func setTestLogging(logLevel LogLevel, caller string, logKeys ...LogKey) (teardownFn func()) {
	initialLogLevel := LevelInfo
	initialLogKey := logKeyMask(KeyHTTP)

	// Check that a previous invocation has not forgotten to call teardownFn
	if *consoleLogger.LogLevel != initialLogLevel ||
		*consoleLogger.LogKeyMask != *initialLogKey {
		panic("Logging is in an unexpected state! Did a previous test forget to call the teardownFn of SetUpTestLogging?")
	}

	consoleLogger.LogLevel.Set(logLevel)
	consoleLogger.LogKeyMask.Set(logKeyMask(logKeys...))

	return func() {
		// Return logging to a default state
		consoleLogger.LogLevel.Set(initialLogLevel)
		consoleLogger.LogKeyMask.Set(initialLogKey)
		if caller != "" {
			Infof(KeyAll, "%v: Reset logging", caller)
		}
	}
}

// Make a deep copy from src into dst.
// Copied from https://github.com/getlantern/deepcopy, commit 7f45deb8130a0acc553242eb0e009e3f6f3d9ce3 (Apache 2 licensed)
func DeepCopyInefficient(dst interface{}, src interface{}) error {
	if dst == nil {
		return fmt.Errorf("dst cannot be nil")
	}
	if src == nil {
		return fmt.Errorf("src cannot be nil")
	}
	b, err := JSONMarshal(src)
	if err != nil {
		return fmt.Errorf("Unable to marshal src: %s", err)
	}
	d := JSONDecoder(bytes.NewBuffer(b))
	d.UseNumber()
	err = d.Decode(dst)
	if err != nil {
		return fmt.Errorf("Unable to unmarshal into dst: %s", err)
	}
	return nil
}

// testRetryUntilTrue performs a short sleep-based retry loop until the timeout is reached or the
// criteria in RetryUntilTrueFunc is met. Intended to
// avoid arbitrarily long sleeps in tests that don't have any alternative to polling.
// Default sleep time is 50ms, timeout is 10s.  Can be customized with testRetryUntilTrueCustom
type RetryUntilTrueFunc func() bool

func testRetryUntilTrue(t *testing.T, retryFunc RetryUntilTrueFunc) {
	testRetryUntilTrueCustom(t, retryFunc, 100, 10000)
}

func testRetryUntilTrueCustom(t *testing.T, retryFunc RetryUntilTrueFunc, waitTimeMs int, timeoutMs int) {
	timeElapsedMs := 0
	for timeElapsedMs < timeoutMs {
		if retryFunc() {
			return
		}
		time.Sleep(time.Duration(waitTimeMs) * time.Millisecond)
		timeElapsedMs += waitTimeMs
	}
	assert.Fail(t, fmt.Sprintf("Retry until function didn't succeed within timeout (%d ms)", timeoutMs))
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func DirExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

// WaitForStat will retry for up to 20 seconds until the result of getStatFunc is equal to the expected value.
func WaitForStat(getStatFunc func() int64, expected int64) (int64, bool) {
	workerFunc := func() (shouldRetry bool, err error, val interface{}) {
		val = getStatFunc()
		return val != expected, nil, val
	}
	// wait for up to 20 seconds for the stat to meet the expected value
	err, val := RetryLoop("waitForStat retry loop", workerFunc, CreateSleeperFunc(200, 100))
	valInt64, ok := val.(int64)

	return valInt64, err == nil && ok
}

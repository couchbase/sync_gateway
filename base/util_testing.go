/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (tb *TestBucket) GetUnderlyingBucket() Bucket {
	return tb.Bucket
}

// LeakyBucketClone wraps the underlying bucket on the TestBucket with a LeakyBucket and returns a new TestBucket handle.
func (tb *TestBucket) LeakyBucketClone(c LeakyBucketConfig) *TestBucket {
	return &TestBucket{
		Bucket:     NewLeakyBucket(tb.Bucket, c),
		BucketSpec: tb.BucketSpec,
		closeFn:    tb.Close,
	}
}

// NoCloseClone returns a leaky bucket with a no-op close function for the given bucket.
func NoCloseClone(b Bucket) *LeakyBucket {
	return NewLeakyBucket(b, LeakyBucketConfig{IgnoreClose: true})
}

// NoCloseClone returns a new test bucket referencing the same underlying bucket and bucketspec, but
// with an IgnoreClose leaky bucket, and a no-op close function.  Used when multiple references to the same bucket are needed.
func (tb *TestBucket) NoCloseClone() *TestBucket {
	return &TestBucket{
		Bucket:     NoCloseClone(tb.Bucket),
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

// Gets a Walrus bucket which will be persisted to a temporary directory
// Returns both the test bucket which is persisted and a function which can be used to remove the created temporary
// directory once the test has finished with it.
func GetPersistentWalrusBucket(t testing.TB) (*TestBucket, func()) {
	tempDir, err := ioutil.TempDir("", "walrustemp")
	require.NoError(t, err)

	walrusFile := fmt.Sprintf("walrus:%s", tempDir)
	bucket, spec, closeFn := GTestBucketPool.GetWalrusTestBucket(t, walrusFile)

	// Return this separate to closeFn as we want to avoid this being removed on database close (/_offline handling)
	removeFileFunc := func() {
		err := os.RemoveAll(tempDir)
		require.NoError(t, err)
	}

	return &TestBucket{
		Bucket:     bucket,
		BucketSpec: spec,
		closeFn:    closeFn,
	}, removeFileFunc
}

func GetTestBucketForDriver(t testing.TB, driver CouchbaseDriver) *TestBucket {

	bucket, spec, closeFn := GTestBucketPool.GetTestBucketAndSpec(t)

	// If walrus, use bucket as-is
	if !TestUseCouchbaseServer() {
		return &TestBucket{
			Bucket:     bucket,
			BucketSpec: spec,
			closeFn:    closeFn,
		}
	}

	// If the spec being used by the test bucket pool matches the requested, use that
	if spec.CouchbaseDriver == driver {
		closeAll := func() {
			closeFn()
		}
		return &TestBucket{
			Bucket:     bucket,
			BucketSpec: spec,
			closeFn:    closeAll,
		}
	}

	// Otherwise, open a bucket for the requested driver based on the connection
	// information from the pool bucket
	spec.CouchbaseDriver = driver
	if spec.Server == kTestCouchbaseServerURL {
		spec.Server = "couchbase://localhost"
	}
	if !strings.HasPrefix(spec.Server, "couchbase") {
		closeFn()
		t.Fatalf("Server must use couchbase scheme for gocb testing")
	}

	store, err := GetBucket(spec)
	if err != nil {
		t.Fatalf("Unable to get store for driver %s: %v", driver, err)
	}

	closeAll := func() {
		store.Close()
		closeFn()
	}

	return &TestBucket{
		Bucket:     store,
		BucketSpec: spec,
		closeFn:    closeAll,
	}
}

// Should Sync Gateway use XATTRS functionality when running unit tests?
func TestUseXattrs() bool {
	useXattrs, isSet := os.LookupEnv(TestEnvSyncGatewayUseXattrs)
	if !isSet {
		return !UnitTestUrlIsWalrus()
	}

	val, err := strconv.ParseBool(useXattrs)
	if err != nil {
		panic(fmt.Sprintf("unable to parse %q value %q: %v", TestEnvSyncGatewayUseXattrs, useXattrs, err))
	}

	return val
}

// Should Sync Gateway skip TLS verification. Default: DefaultTestTLSSkipVerify
func TestTLSSkipVerify() bool {
	tlsSkipVerify, isSet := os.LookupEnv(TestEnvTLSSkipVerify)
	if !isSet {
		return DefaultTestTLSSkipVerify
	}

	val, err := strconv.ParseBool(tlsSkipVerify)
	if err != nil {
		panic(fmt.Sprintf("unable to parse %q value %q: %v", TestEnvTLSSkipVerify, tlsSkipVerify, err))
	}

	return val
}

func TestX509LocalServer() (bool, string) {
	testX509LocalServer, isSet := os.LookupEnv(TestEnvX509Local)
	if !isSet {
		return false, ""
	}

	val, err := strconv.ParseBool(testX509LocalServer)
	if err != nil {
		panic(fmt.Sprintf("unable to parse %q value %q: %v", TestEnvX509Local, testX509LocalServer, err))
	}

	username, isSet := os.LookupEnv(TestEnvX509LocalUser)
	if !isSet {
		panic(fmt.Sprintf("TestEnvX509LocalUser must be set when TestEnvX509Local=true"))
	}

	return val, username
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
	// Disable GSI when running with Walrus
	if !TestUseCouchbaseServer() && UnitTestUrlIsWalrus() {
		return true
	}

	// Default to disabling GSI, but allow with SG_TEST_USE_GSI=true
	useGSI := false
	if envUseGSI := os.Getenv(TestEnvSyncGatewayDisableGSI); envUseGSI != "" {
		useGSI, _ = strconv.ParseBool(envUseGSI)
	}

	return !useGSI
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
func DropAllBucketIndexes(bucket N1QLStore) error {

	// Retrieve all indexes
	indexes, err := bucket.getIndexes()
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

			log.Printf("Dropping index %s on bucket %s...", indexToDrop, bucket.GetName())
			dropErr := bucket.DropIndex(indexToDrop)
			if dropErr != nil {
				asyncErrors <- dropErr
				log.Printf("...failed to drop index %s on bucket %s: %s", indexToDrop, bucket.GetName(), dropErr)
				return
			}
			log.Printf("...successfully dropped index %s on bucket %s", indexToDrop, bucket.GetName())
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

// Generates a string of size int
const alphaNumeric = "0123456789abcdefghijklmnopqrstuvwxyz"

func CreateProperty(size int) (result string) {
	resultBytes := make([]byte, size)
	for i := 0; i < size; i++ {
		resultBytes[i] = alphaNumeric[i%len(alphaNumeric)]
	}
	return string(resultBytes)
}

// SetUpGlobalTestMemoryWatermark will periodically write an in-use memory watermark,
// and will cause the tests to fail on teardown if the watermark has exceeded the threshold.
func SetUpGlobalTestMemoryWatermark(m *testing.M, memWatermarkThresholdMB uint64) (teardownFn func()) {
	sampleFrequency := time.Second * 5
	if freq := os.Getenv("SG_TEST_PROFILE_FREQUENCY"); freq != "" {
		var err error
		sampleFrequency, err = time.ParseDuration(freq)
		if err != nil {
			log.Fatalf("TEST: profile frequency %q was not a valid duration: %v", freq, err)
		} else if sampleFrequency == 0 {
			// disabled
			return func() {}
		}
	}

	var inuseHighWaterMarkMB float64

	ctx, ctxCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(ctx context.Context) {
		defer wg.Done()

		sampleFn := func() {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heapInuseMB := float64(ms.HeapInuse) / float64(1024*1024)
			stackInuseMB := float64(ms.StackInuse) / float64(1024*1024)
			totalInuseMB := heapInuseMB + stackInuseMB
			// log.Printf("TEST: Memory usage recorded heap: %.2f MB stack: %.2f MB", heapInuseMB, stackInuseMB)
			if totalInuseMB > inuseHighWaterMarkMB {
				log.Printf("TEST: Memory high water mark increased to %.2f MB (heap: %.2f MB stack: %.2f MB)", totalInuseMB, heapInuseMB, stackInuseMB)
				inuseHighWaterMarkMB = totalInuseMB
			}
		}

		t := time.NewTicker(sampleFrequency)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				sampleFn() // one last reading just before we exit
				return
			case <-t.C:
				sampleFn()
			}
		}
	}(ctx)

	return func() {
		ctxCancel()
		wg.Wait()

		if inuseHighWaterMarkMB > float64(memWatermarkThresholdMB) {
			// Exit during teardown to fail the suite if they exceeded the threshold
			log.Fatalf("FATAL - TEST: Memory high water mark %.2f MB exceeded threshold (%d MB)", inuseHighWaterMarkMB, memWatermarkThresholdMB)
		} else {
			log.Printf("TEST: Memory high water mark %.2f MB", inuseHighWaterMarkMB)
		}
	}
}

// SetUpGlobalTestProfiling will cause a packages tests to periodically write a profiles to the package's directory.
func SetUpGlobalTestProfiling(m *testing.M) (teardownFn func()) {
	freq := os.Getenv("SG_TEST_PROFILE_FREQUENCY")
	if freq == "" {
		return func() {}
	}

	d, err := time.ParseDuration(freq)
	if err != nil {
		log.Fatalf("TEST: profile frequency %q was not a valid duration: %v", freq, err)
	} else if d == 0 {
		// disabled
		return func() {}
	}

	profiles := []string{
		"goroutine",
		// "threadcreate",
		"heap",
		// "allocs",
		// "block",
		// "mutex",
	}

	log.Printf("TEST: profiling for %v with frequency: %v", profiles, freq)

	ctx, ctxCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(ctx context.Context) {
		defer wg.Done()

		sampleFn := func() {
			timestamp := time.Now().Unix()
			for _, profile := range profiles {
				filename := fmt.Sprintf("test-pprof-%s-%d.pb.gz", profile, timestamp)
				f, err := os.Create(filename)
				if err != nil {
					log.Fatalf("TEST: couldn't open pprof %s file: %v", profile, err)
				}
				err = pprof.Lookup(profile).WriteTo(f, 0)
				if err != nil {
					log.Fatalf("TEST: couldn't write pprof %s file: %v", profile, err)
				}
				err = f.Close()
				if err != nil {
					log.Fatalf("TEST: couldn't close pprof %s file: %v", profile, err)
				}
				log.Printf("TEST: %s profile written to: %v", profile, filename)
			}
		}

		t := time.NewTicker(d)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				sampleFn() // one last reading just before we exit
				return
			case <-t.C:
				sampleFn()
			}
		}
	}(ctx)

	return func() {
		ctxCancel()
		wg.Wait()
	}
}

var GlobalTestLoggingSet = AtomicBool{}

// SetUpGlobalTestLogging sets a global log level at runtime by using the SG_TEST_LOG_LEVEL environment variable.
// This global level overrides any tests that specify their own test log level with SetUpTestLogging.
func SetUpGlobalTestLogging(m *testing.M) (teardownFn func()) {
	if logLevel := os.Getenv(TestEnvGlobalLogLevel); logLevel != "" {
		var l LogLevel
		err := l.UnmarshalText([]byte(logLevel))
		if err != nil {
			FatalfCtx(context.TODO(), "TEST: Invalid log level used for %q: %s", TestEnvGlobalLogLevel, err)
		}
		caller := GetCallersName(1, false)
		InfofCtx(context.Background(), KeyAll, "%s: Setup logging: level: %v - keys: %v", caller, logLevel, KeyAll)
		teardown := setTestLogging(l, caller, KeyAll)
		GlobalTestLoggingSet.Set(true)
		return func() {
			teardown()
			GlobalTestLoggingSet.Set(false)
		}
	}
	// noop
	return func() { GlobalTestLoggingSet.Set(false) }
}

// SetUpTestLogging will set the given log level and log keys, and revert the changes at the end of the current test.
//
// This function will panic if called multiple times in the same test.
func SetUpTestLogging(tb testing.TB, logLevel LogLevel, logKeys ...LogKey) {
	caller := GetCallersName(1, false)
	InfofCtx(context.Background(), KeyAll, "%s: Setup logging: level: %v - keys: %v", caller, logLevel, logKeys)
	cleanup := setTestLogging(logLevel, caller, logKeys...)
	tb.Cleanup(cleanup)
}

// DisableTestLogging is an alias for SetUpTestLogging(LevelNone, KeyNone)
// This function will panic if called multiple times in the same test.
func DisableTestLogging(tb testing.TB) {
	caller := ""
	cleanup := setTestLogging(LevelNone, caller, KeyNone)
	tb.Cleanup(cleanup)
}

// SetUpBenchmarkLogging will set the given log level and key, and do log processing for that configuration,
// but discards the output, instead of writing it to console.
func SetUpBenchmarkLogging(tb testing.TB, logLevel LogLevel, logKeys ...LogKey) {
	teardownFnOrig := setTestLogging(logLevel, "", logKeys...)

	// discard all logging output for benchmarking (but still execute logging as normal)
	consoleLogger.logger.SetOutput(ioutil.Discard)
	tb.Cleanup(func() {
		// revert back to original output
		if consoleLogger != nil && consoleLogger.output != nil {
			consoleLogger.logger.SetOutput(consoleLogger.output)
		} else {
			consoleLogger.logger.SetOutput(os.Stderr)
		}
		teardownFnOrig()
	})
}

func setTestLogging(logLevel LogLevel, caller string, logKeys ...LogKey) (teardownFn func()) {
	if GlobalTestLoggingSet.IsTrue() {
		// noop, test log level is already set globally
		return func() {
			if caller != "" {
				InfofCtx(context.Background(), KeyAll, "%v: Reset logging", caller)
			}
		}
	}

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
			InfofCtx(context.Background(), KeyAll, "%v: Reset logging", caller)
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
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func DirExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
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

// RequireWaitForStat will retry for up to 20 seconds until the result of getStatFunc is equal to the expected value.
func RequireWaitForStat(t testing.TB, getStatFunc func() int64, expected int64) {
	val, ok := WaitForStat(getStatFunc, expected)
	require.True(t, ok)
	require.Equal(t, expected, val)
}

type dataStore struct {
	name   string
	driver CouchbaseDriver
}

// ForAllDataStores is used to run a test against multiple data stores (gocb bucket, gocb collection)
func ForAllDataStores(t *testing.T, testCallback func(*testing.T, sgbucket.DataStore)) {
	dataStores := make([]dataStore, 0)

	if TestUseCouchbaseServer() {
		dataStores = append(dataStores, dataStore{
			name:   "gocb.v2",
			driver: GoCBv2,
		})
	}

	dataStores = append(dataStores, dataStore{
		name:   "gocb.v1",
		driver: GoCBCustomSGTranscoder,
	})

	for _, dataStore := range dataStores {
		t.Run(dataStore.name, func(t *testing.T) {
			bucket := GetTestBucketForDriver(t, dataStore.driver)
			defer bucket.Close()
			testCallback(t, bucket)
		})
	}
}

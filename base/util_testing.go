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
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Code that is test-related that needs to be accessible from non-base packages, and therefore can't live in
// util_test.go, which is only accessible from the base package.

var TestExternalRevStorage = false

func init() {

	// Prevent https://issues.couchbase.com/browse/MB-24237
	rand.Seed(time.Now().UTC().UnixNano())
}

type TestBucket struct {
	Bucket
	BucketSpec BucketSpec
	closeFn    func()
	t          testing.TB
}

var _ Bucket = &TestBucket{}

// DefaultDataStore is intentionally not implemented for TestBucket
// DEPRECATED: Should use GetSingleDataStore
func (b *TestBucket) DefaultDataStore() sgbucket.DataStore {
	return b.Bucket.DefaultDataStore()
}

// NamedDataStore is intentionally not implemented for TestBucket
// DEPRECATED: Should use GetNamedDataStore
func (b *TestBucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	return b.Bucket.NamedDataStore(name)
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
		t:          tb.t,
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
		t:          tb.t,
	}
}

// GetTestBucket returns a test bucket from a pool.
func GetTestBucket(t testing.TB) *TestBucket {
	return getTestBucket(t, false)
}

// GetTestBucket returns a test bucket from a pool.  If running with walrus buckets, will persist bucket data
// across bucket close.
func GetPersistentTestBucket(t testing.TB) *TestBucket {
	return getTestBucket(t, true)
}

// getTestBucket returns a bucket from the bucket pool.  Persistent flag determines behaviour for walrus
// buckets only - Couchbase bucket behaviour is defined by the bucket pool readier/init.
func getTestBucket(t testing.TB, persistent bool) *TestBucket {
	bucket, spec, closeFn := GTestBucketPool.getTestBucketAndSpec(t, persistent)
	return &TestBucket{
		Bucket:     bucket,
		BucketSpec: spec,
		closeFn:    closeFn,
		t:          t,
	}
}

// GetNamedDataStore returns a named datastore from the TestBucket. Each number (starting from 0, indicates which data store you'll get.
func (tb *TestBucket) GetNamedDataStore(count int) (DataStore, error) {
	dataStoreNames := tb.GetNonDefaultDatastoreNames()
	if len(dataStoreNames) == 0 {
		tb.t.Fatalf("You are requesting a named datastore on TestBucket that has none.")
	}
	if count > len(dataStoreNames) {
		tb.t.Fatalf("You are requesting more datastores %d than are available on this test instance %d", count, len(dataStoreNames))
	}
	return tb.Bucket.NamedDataStore(dataStoreNames[count])
}

// Return a sorted list of data store names
func (tb *TestBucket) GetNonDefaultDatastoreNames() []sgbucket.DataStoreName {
	allDataStoreNames, err := tb.ListDataStores()
	require.NoError(tb.t, err)
	var keyspaces []string
	for _, name := range allDataStoreNames {
		if IsDefaultCollection(name.ScopeName(), name.CollectionName()) {
			continue
		}
		keyspaces = append(keyspaces, fmt.Sprintf("%s.%s", name.ScopeName(), name.CollectionName()))
	}
	sort.Strings(keyspaces)
	var nonDefaultDataStoreNames []sgbucket.DataStoreName
	for _, keyspace := range keyspaces {
		scopeAndCollection := strings.Split(keyspace, ScopeCollectionSeparator)
		nonDefaultDataStoreNames = append(nonDefaultDataStoreNames,
			ScopeAndCollectionName{
				Scope:      scopeAndCollection[0],
				Collection: scopeAndCollection[1]})
	}
	return nonDefaultDataStoreNames
}

// GetSingleDataStore returns a DataStore that can be used for testing.
// This may be the default collection, or a named collection depending on whether SG_TEST_USE_DEFAULT_COLLECTION is set.
func (b *TestBucket) GetSingleDataStore() sgbucket.DataStore {
	if TestsUseNamedCollections() {
		ds, err := b.GetNamedDataStore(0)
		require.NoError(b.t, err)
		return ds
	}
	return b.Bucket.DefaultDataStore()
}

func (b *TestBucket) GetMetadataStore() sgbucket.DataStore {
	return b.Bucket.DefaultDataStore()
}

func (b *TestBucket) CreateDataStore(name sgbucket.DataStoreName) error {
	dynamicDataStore, ok := b.Bucket.(sgbucket.DynamicDataStoreBucket)
	if !ok {
		return fmt.Errorf("Bucket %T doesn't support dynamic collection creation", b.Bucket)
	}
	return dynamicDataStore.CreateDataStore(name)
}

func (b *TestBucket) DropDataStore(name sgbucket.DataStoreName) error {
	dynamicDataStore, ok := b.GetUnderlyingBucket().(sgbucket.DynamicDataStoreBucket)
	if !ok {
		return fmt.Errorf("Bucket %T doesn't support dynamic collection creation", b.GetUnderlyingBucket())
	}
	return dynamicDataStore.DropDataStore(name)
}

// GetDefaultDataStore returns the default DataStore. This is likely never actually wanted over GetSingleDataStore, so is left commented until absolutely required.
// func (b *TestBucket) GetDefaultDataStore() sgbucket.DataStore {
// 	b.t.Logf("Using default collection - Are you sure you want this instead of GetSingleDataStore() ?")
// 	return b.Bucket.DefaultDataStore()
// }

// Gets a Walrus bucket which will be persisted to a temporary directory
// Returns both the test bucket which is persisted and a function which can be used to remove the created temporary
// directory once the test has finished with it.
func GetPersistentWalrusBucket(t testing.TB) (*TestBucket, func()) {
	tempDir, err := os.MkdirTemp("", "walrustemp")
	require.NoError(t, err)

	walrusFile := fmt.Sprintf("%s:%s", rosmar.URLScheme, tempDir)
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
		t:          t,
	}, removeFileFunc
}

// Should Sync Gateway use XATTRS functionality when running unit tests?
func TestUseXattrs() bool {
	useXattrs, isSet := os.LookupEnv(TestEnvSyncGatewayUseXattrs)
	if !isSet {
		return true
	}

	val, err := strconv.ParseBool(useXattrs)
	if err != nil {
		panic(fmt.Sprintf("unable to parse %q value %q: %v", TestEnvSyncGatewayUseXattrs, useXattrs, err))
	}

	return val
}

// Should Sync Gateway use expiration functionality when running unit tests?
func TestUseExpiry() bool {
	return true // Rosmar supports expiry
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

func TestUseCouchbaseServerDockerName() (bool, string) {
	testX509CouchbaseServerDockerName, isSet := os.LookupEnv(TestEnvCouchbaseServerDockerName)
	if !isSet {
		return false, ""
	}
	return true, testX509CouchbaseServerDockerName
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

	if strings.EqualFold(dropIndexes, TestEnvSyncGatewayTrue) {
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
	return strings.EqualFold(backingStore, TestEnvBackingStoreCouchbase)
}

func TestUseWalrus() bool {
	backingStore := os.Getenv(TestEnvSyncGatewayBackingStore)
	return strings.EqualFold(backingStore, TestEnvBackingStoreWalrus)
}

// Check the whether tests are being run with SG_TEST_BACKING_STORE=Couchbase
func TestUseExistingBucket() bool {
	return TestUseExistingBucketName() != ""
}

func TestUseExistingBucketName() string {
	return os.Getenv(TestEnvUseExistingBucket)
}

type TestAuthenticator struct {
	Username   string
	Password   string
	BucketName string
}

func (t TestAuthenticator) GetCredentials() (username, password, bucketname string) {
	return t.Username, t.Password, t.BucketName
}

// DropAllIndexes removes all indexes defined on the bucket or collection
func DropAllIndexes(ctx context.Context, n1QLStore N1QLStore) error {

	// Retrieve all indexes on the bucket/collection
	indexes, err := n1QLStore.GetIndexes()
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

			InfofCtx(ctx, KeySGTest, "Dropping index %s on bucket %s...", indexToDrop, n1QLStore.GetName())
			dropErr := n1QLStore.DropIndex(ctx, indexToDrop)
			if dropErr != nil {
				// Retry dropping index if first try fails before returning error
				dropRetry := n1QLStore.DropIndex(ctx, indexToDrop)
				if dropRetry != nil {
					asyncErrors <- dropErr
					ErrorfCtx(ctx, "...failed to drop index %s on bucket %s: %s", indexToDrop, n1QLStore.GetName(), dropErr)
					return
				}
			}
			InfofCtx(ctx, KeySGTest, "...successfully dropped index %s on bucket %s", indexToDrop, n1QLStore.GetName())
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

// SetUpTestGoroutineDump will collect a goroutine pprof profile when teardownFn is called. Intended to be run at the end of TestMain to give us insight into goroutine leaks.
func SetUpTestGoroutineDump(m *testing.M) (teardownFn func()) {
	const numExpected = 1

	if ok, _ := strconv.ParseBool(os.Getenv(TestEnvGoroutineDump)); !ok {
		return func() {}
	}

	timestamp := time.Now().Unix()
	filename := fmt.Sprintf("test-pprof-%s-%d.pb.gz", "goroutine", timestamp)
	// create the file upfront so we know we're able to write to it before we run tests
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	return func() {
		if n := runtime.NumGoroutine(); n > numExpected {
			if err := pprof.Lookup("goroutine").WriteTo(os.Stderr, 2); err != nil {
				panic(err)
			}
			if err := pprof.Lookup("goroutine").WriteTo(file, 0); err != nil {
				panic(err)
			}
			log.Printf(color("\n"+
				"TEST: =================================================\n"+
				"TEST: Leaked goroutines after testing: got %d expected %d\n"+
				"TEST: =================================================\n", LevelError), n, numExpected)
			log.Printf("TEST: Written goroutine profile to: %s%c%s", wd, os.PathSeparator, file.Name())
		} else {
			log.Print(color("TEST: No leaked goroutines found", LevelDebug))
		}
	}
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
	consoleLogger.logger.SetOutput(io.Discard)
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
	updateExternalLoggers()

	return func() {
		// Return logging to a default state
		consoleLogger.LogLevel.Set(initialLogLevel)
		consoleLogger.LogKeyMask.Set(initialLogKey)
		updateExternalLoggers()
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

// TestRequiresCollections will skip the current test if the Couchbase Server version it is running against does not
// support collections.
func TestRequiresCollections(t testing.TB) {
	if ok, err := GTestBucketPool.canUseNamedCollections(); err != nil {
		t.Skipf("Skipping test - collections not supported: %v", err)
	} else if !ok {
		t.Skipf("Skipping test - collections not enabled")
	}
}

// DisableTestWithCollections will skip the current test if using named collections.
func DisableTestWithCollections(t *testing.T) {
	if TestsUseNamedCollections() {
		t.Skip("Skipping test because collections are enabled")
	}
}

// SkipImportTestsIfNotEnabled skips test that exercise import features
func SkipImportTestsIfNotEnabled(t *testing.T) {

	if !TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}
}

// CreateBucketScopesAndCollections will create the given scopes and collections within the given BucketSpec.
func CreateBucketScopesAndCollections(ctx context.Context, bucketSpec BucketSpec, scopes map[string][]string) error {
	atLeastOneScope := false
	for _, collections := range scopes {
		for range collections {
			atLeastOneScope = true
			break
		}
		break
	}
	if !atLeastOneScope {
		// nothing to do here
		return nil
	}

	un, pw, _ := bucketSpec.Auth.GetCredentials()
	var rootCAs *x509.CertPool
	if tlsConfig := bucketSpec.TLSConfig(); tlsConfig != nil {
		rootCAs = tlsConfig.RootCAs
	}
	cluster, err := gocb.Connect(bucketSpec.Server, gocb.ClusterOptions{
		Username: un,
		Password: pw,
		SecurityConfig: gocb.SecurityConfig{
			TLSSkipVerify: bucketSpec.TLSSkipVerify,
			TLSRootCAs:    rootCAs,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer func() { _ = cluster.Close(nil) }()

	cm := cluster.Bucket(bucketSpec.BucketName).Collections()

	for scopeName, collections := range scopes {
		if err := cm.CreateScope(scopeName, nil); err != nil && !errors.Is(err, gocb.ErrScopeExists) {
			return fmt.Errorf("failed to create scope %s: %w", scopeName, err)
		}
		DebugfCtx(ctx, KeySGTest, "Created scope %s", scopeName)
		for _, collectionName := range collections {
			if err := cm.CreateCollection(
				gocb.CollectionSpec{
					Name:      collectionName,
					ScopeName: scopeName,
				}, nil); err != nil && !errors.Is(err, gocb.ErrCollectionExists) {
				return fmt.Errorf("failed to create collection %s in scope %s: %w", collectionName, scopeName, err)
			}
			DebugfCtx(ctx, KeySGTest, "Created collection %s.%s", scopeName, collectionName)
			if err := WaitForNoError(func() error {
				_, err := cluster.Bucket(bucketSpec.BucketName).Scope(scopeName).Collection(collectionName).Exists("WaitForExists", nil)
				return err
			}); err != nil {
				return fmt.Errorf("failed to wait for collection %s.%s to exist: %w", scopeName, collectionName, err)
			}
			DebugfCtx(ctx, KeySGTest, "Collection now exists %s.%s", scopeName, collectionName)
		}
	}

	return nil
}

// RequireAllAssertions ensures that all assertion results were true/ok, and fails the test if any were not.
// Usage:
//
//	RequireAllAssertions(t,
//	    assert.True(t, condition1),
//	    assert.True(t, condition2),
//	)
func RequireAllAssertions(t *testing.T, assertionResults ...bool) {
	var failed bool
	for _, ok := range assertionResults {
		if !ok {
			failed = true
			break
		}
	}
	require.Falsef(t, failed, "One or more assertions failed: %v", assertionResults)
}

// LongRunningTest skips the test if running in -short mode, and logs if the test completed quickly under other circumstances.
func LongRunningTest(t *testing.T) {
	const (
		shortTestThreshold = time.Second
	)
	if testing.Short() {
		t.Skip("skipping long running test in short mode")
		return
	}
	start := time.Now()
	t.Cleanup(func() {
		testDuration := time.Since(start)
		if !t.Failed() && !t.Skipped() && testDuration < shortTestThreshold {
			t.Logf("TEST: %q was marked as long running, but finished in %v (less than %v) - consider removing LongRunningTest", t.Name(), testDuration, shortTestThreshold)
		}
	})
}

func AssertTimeGreaterThan(t *testing.T, e1, e2 time.Time, msgAndArgs ...interface{}) bool {
	return AssertTimestampGreaterThan(t, e1.UnixNano(), e2.UnixNano(), msgAndArgs...)
}

func AssertTimestampGreaterThan(t *testing.T, e1, e2 int64, msgAndArgs ...interface{}) bool {
	// time.Nanoseconds has poor precision on Windows - equal is good enough there...
	if runtime.GOOS == "windows" {
		return assert.GreaterOrEqual(t, e1, e2, msgAndArgs...)
	}
	return assert.Greater(t, e1, e2, msgAndArgs...)
}

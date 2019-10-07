package base

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb"
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
}

func (tb TestBucket) Close() {

	tb.Bucket.Close()

	DecrNumOpenBuckets(tb.Bucket.GetName())
}

func GetTestBucket(tester testing.TB) TestBucket {
	return GetBucketCommon(DataBucket, tester)
}

func GetTestIndexBucket(tester testing.TB) TestBucket {
	return GetBucketCommon(IndexBucket, tester)
}

func GetTestBucketSpec(bucketType CouchbaseBucketType) BucketSpec {

	bucketName := DefaultTestBucketname
	username := DefaultTestUsername
	password := DefaultTestPassword

	// Use a different bucket name for index buckets to avoid interference
	switch bucketType {
	case IndexBucket:
		bucketName = DefaultTestIndexBucketname
		username = DefaultTestIndexUsername
		password = DefaultTestIndexPassword
	}

	testAuth := TestAuthenticator{
		Username:   username,
		Password:   password,
		BucketName: bucketName,
	}

	spec := BucketSpec{
		Server:     UnitTestUrl(),
		BucketName: bucketName,

		CouchbaseDriver: ChooseCouchbaseDriver(bucketType),
		Auth:            testAuth,
		UseXattrs:       TestUseXattrs(),
	}

	if spec.IsWalrusBucket() {
		// Use a unique bucket name to reduce the chance of interference between temporary test walrus buckets
		spec.BucketName = fmt.Sprintf("%s-%s", spec.BucketName, CreateUUID())
	}

	return spec

}

func GetBucketCommon(bucketType CouchbaseBucketType, tester testing.TB) TestBucket {

	spec := GetTestBucketSpec(bucketType)

	if !spec.IsWalrusBucket() {

		// If this is not testing against a walrus bucket, then it's testing against a Coucbhase Server bucket,
		// and therefore needs to create the bucket if it doesn't already exist, or flush it if it does.

		tbm := NewTestBucketManager(spec)
		bucketExists, err := tbm.OpenTestBucket()
		if err != nil {
			tester.Fatalf("Error checking if bucket exists.  Spec: %+v err: %v", spec, err)
		}
		switch bucketExists {
		case true:
			// Empty it
			if err := tbm.RecreateOrEmptyBucket(); err != nil {
				panic(fmt.Sprintf("Error trying to empty bucket.  Spec: %+v.  err: %v", spec, err))

			}
		case false:
			// Create a brand new bucket
			// TODO: in this case, we should still wait until it's empty, just in case there was somehow residue
			// TODO: in between deleting and recreating it, if it happened in rapid succession
			if err := tbm.CreateTestBucket(); err != nil {
				tester.Fatalf("Could not create bucket.  Spec: %+v Err: %v", spec, err)
			}
		}

		// Close the bucket and any other temporary resources associated with the TestBucketManager
		tbm.Close()

	}

	// Now open the bucket _again_ to ensure it's open with the correct driver
	bucket, err := GetBucket(spec, nil)
	if err != nil {
		tester.Fatalf("Could not open bucket: %v", err)
	}

	return TestBucket{
		Bucket:     bucket,
		BucketSpec: spec,
	}

}

func GetBucketWithInvalidUsernamePassword(bucketType CouchbaseBucketType) (TestBucket, error) {

	spec := GetTestBucketSpec(bucketType)

	// Override spec's auth with invalid creds
	spec.Auth = TestAuthenticator{
		Username:   "invalid_username",
		Password:   "invalid_password",
		BucketName: spec.BucketName,
	}

	// Attempt to open a test bucket with invalid creds. We should expect an error.
	bucket, err := GetBucket(spec, nil)
	return TestBucket{Bucket: bucket}, err

}

// Convenience function that will cause a bucket to be created if it doesn't already exist.
func InitializeBucket(bucketType CouchbaseBucketType, tester testing.TB) {

	// Create
	tempBucket := GetBucketCommon(bucketType, tester)
	tempBucket.Close()

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

type TestBucketManager struct {
	AdministratorUsername string
	AdministratorPassword string
	BucketSpec            BucketSpec
	Bucket                *CouchbaseBucketGoCB
	AuthHandler           AuthHandler
	Cluster               *gocb.Cluster
	ClusterManager        *gocb.ClusterManager
}

func NewTestBucketManager(spec BucketSpec) *TestBucketManager {

	tbm := TestBucketManager{
		AdministratorUsername: DefaultCouchbaseAdministrator,
		AdministratorPassword: DefaultCouchbasePassword,
		AuthHandler:           spec.Auth,
		BucketSpec:            spec,
	}

	return &tbm

}

func (tbm *TestBucketManager) OpenTestBucket() (bucketExists bool, err error) {

	if NumOpenBuckets(tbm.BucketSpec.BucketName) > 0 {
		return false, fmt.Errorf("There are already %d open buckets with name: %s.  The tests expect all buckets to be closed.", NumOpenBuckets(tbm.BucketSpec.BucketName), tbm.BucketSpec.BucketName)
	}

	IncrNumOpenBuckets(tbm.BucketSpec.BucketName)

	tbm.Bucket, err = GetCouchbaseBucketGoCB(tbm.BucketSpec)
	if err != nil {
		return false, err
	}

	return true, nil

}

func (tbm *TestBucketManager) Close() {
	tbm.Bucket.Close()
}

// GOCB doesn't currently offer a way to do this, and so this is a workaround to go directly
// to Couchbase Server REST API.
// See https://forums.couchbase.com/t/is-there-a-way-to-get-the-number-of-items-in-a-bucket/12816/4
// for GOCB discussion.
func (tbm *TestBucketManager) BucketItemCount() (itemCount int, err error) {
	return tbm.Bucket.BucketItemCount()
}

func (tbm *TestBucketManager) DropIndexes() error {
	return DropAllBucketIndexes(tbm.Bucket)
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

			log.Printf("Dropping index %s...", indexToDrop)
			dropErr := gocbBucket.DropIndex(indexToDrop)
			if dropErr != nil {
				asyncErrors <- dropErr
			}
			log.Printf("...successfully dropped index %s", indexToDrop)
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
		indexes = append(indexes, indexInfo.Name)
	}

	return indexes, nil

}

func (tbm *TestBucketManager) FlushBucket() error {

	// Try to Flush the bucket in a retry loop
	// Ignore sporadic errors like:
	// Error trying to empty bucket. err: {"_":"Flush failed with unexpected error. Check server logs for details."}

	Infof(KeyAll, "Flushing bucket %s", tbm.Bucket.Name())

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		err = tbm.Bucket.Flush()
		if err != nil {
			Warnf(KeyAll, "Error flushing bucket: %v  Will retry.", err)
		}
		shouldRetry = (err != nil) // retry (until max attempts) if there was an error
		return shouldRetry, err, nil
	}

	err, _ := RetryLoop("EmptyTestBucket", workerFlush, CreateDoublingSleeperFunc(12, 10))
	if err != nil {
		return err
	}

	maxTries := 20
	numTries := 0
	for {

		itemCount, err := tbm.BucketItemCount()
		if err != nil {
			return err
		}

		if itemCount == 0 {
			// Bucket flushed, we're done
			break
		}

		if numTries > maxTries {
			return fmt.Errorf("Timed out waiting for bucket to be empty after flush.  ItemCount: %v", itemCount)
		}

		// Still items left, wait a little bit and try again
		Warnf(KeyAll, "TestBucketManager.EmptyBucket(): still %d items in bucket after flush, waiting for no items.  Will retry.", itemCount)
		time.Sleep(time.Millisecond * 500)

		numTries += 1

	}

	return nil

}

func (tbm *TestBucketManager) RecreateOrEmptyBucket() error {

	if TestsShouldDropIndexes() {
		if err := tbm.DropIndexes(); err != nil {
			return err
		}
	}

	if err := tbm.FlushBucket(); err != nil {
		return err
	}

	return nil
}

func (tbm *TestBucketManager) DeleteTestBucket() error {

	err := tbm.ClusterManager.RemoveBucket(tbm.BucketSpec.BucketName)
	if err != nil {
		return err
	}

	return nil
}

func (tbm *TestBucketManager) CreateTestBucket() error {

	username, password, _ := tbm.BucketSpec.Auth.GetCredentials()

	log.Printf("Create bucket with username: %v password: %v", username, password)

	ramQuotaMB := 100

	bucketSettings := gocb.BucketSettings{
		Name:          tbm.BucketSpec.BucketName,
		Type:          gocb.Couchbase,
		Password:      password,
		Quota:         ramQuotaMB,
		Replicas:      0,
		IndexReplicas: false,
		FlushEnabled:  true,
	}

	err := tbm.ClusterManager.InsertBucket(&bucketSettings)
	if err != nil {
		return err
	}

	// Add an RBAC user
	// TODO: This isn't working, filed a question here: https://forums.couchbase.com/t/creating-rbac-user-via-go-sdk-against-couchbase-server-5-0-0-build-2958/12983
	// TODO: This is only needed if server is 5.0 or later, but not sure how to check couchbase server version
	//roles := []gocb.UserRole{
	//	gocb.UserRole{
	//		Role:       "bucket_admin",
	//		// BucketName: tbm.BucketSpec.BucketName,
	//		BucketName: "test_data_bucket",
	//	},
	//}
	//userSettings := &gocb.UserSettings{
	//	// Name:     username,
	//	// Password: password,
	//	Name: "test_data_bucket",
	//	Password: "password",
	//	Roles:    roles,
	//}
	//err = tbm.ClusterManager.UpsertUser(username, userSettings)
	//if err != nil {
	//	log.Printf("Error UpsertUser: %v", err)
	//	return err
	//}

	numTries := 0
	maxTries := 20
	for {

		bucket, errOpen := GetBucket(tbm.BucketSpec, nil)

		if errOpen == nil {
			// We were able to open the bucket, so it worked and we're done
			bucket.Close()
			return nil
		}

		if numTries >= maxTries {
			return fmt.Errorf("Created bucket, but unable to connect to it after several attempts.  Spec: %+v", tbm.BucketSpec)
		}

		// Maybe it's not ready yet, wait a little bit and retry
		numTries += 1
		time.Sleep(time.Millisecond * 500)

	}
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

func IncrNumOpenBuckets(bucketName string) {
	MutateNumOpenBuckets(bucketName, 1)

}

func DecrNumOpenBuckets(bucketName string) {
	MutateNumOpenBuckets(bucketName, -1)
}

func MutateNumOpenBuckets(bucketName string, delta int32) {
	mutexNumOpenBucketsByName.Lock()
	defer mutexNumOpenBucketsByName.Unlock()

	numOpen, ok := numOpenBucketsByName[bucketName]
	if !ok {
		numOpen = 0
		numOpenBucketsByName[bucketName] = numOpen
	}

	numOpen += delta
	numOpenBucketsByName[bucketName] = numOpen
}

func NumOpenBuckets(bucketName string) int32 {
	mutexNumOpenBucketsByName.Lock()
	defer mutexNumOpenBucketsByName.Unlock()
	numOpen, ok := numOpenBucketsByName[bucketName]
	if !ok {
		return 0
	}
	return numOpen
}

// SetUpTestLogging will set the given log level and log keys,
// and return a function that can be deferred for teardown.
//
// This function will panic if called multiple times without running the teardownFn.
//
// To set multiple log keys, use the bitwise OR operator.
// E.g. KeyCache|KeyDCP|KeySync
//
// Usage:
//     teardownFn := SetUpTestLogging(LevelDebug, KeyCache|KeyDCP|KeySync)
//     defer teardownFn()
//
// Shorthand style:
//     defer SetUpTestLogging(LevelDebug, KeyCache|KeyDCP|KeySync)()
func SetUpTestLogging(logLevel LogLevel, logKeys LogKey) (teardownFn func()) {
	caller := GetCallersName(1, false)
	Infof(KeyAll, "%s: Setup logging: level: %v - keys: %v", caller, logLevel, logKeys)
	return setTestLogging(logLevel, logKeys, caller)
}

// DisableTestLogging is an alias for SetUpTestLogging(LevelNone, KeyNone)
// This function will panic if called multiple times without running the teardownFn.
func DisableTestLogging() (teardownFn func()) {
	caller := ""
	return setTestLogging(LevelNone, KeyNone, caller)
}

func setTestLogging(logLevel LogLevel, logKeys LogKey, caller string) (teardownFn func()) {
	initialLogLevel := LevelInfo
	initialLogKey := KeyHTTP

	// Check that a previous invocation has not forgotten to call teardownFn
	if *consoleLogger.LogLevel != initialLogLevel ||
		*consoleLogger.LogKey != initialLogKey {
		panic("Logging is in an unexpected state! Did a previous test forget to call the teardownFn of SetUpTestLogging?")
	}

	consoleLogger.LogLevel.Set(logLevel)
	consoleLogger.LogKey.Set(logKeys)

	return func() {
		// Return logging to a default state
		consoleLogger.LogLevel.Set(initialLogLevel)
		consoleLogger.LogKey.Set(initialLogKey)
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

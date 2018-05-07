package base

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb"
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
}

func (tb TestBucket) Close() {

	tb.Bucket.Close()

	DecrNumOpenBuckets(tb.Bucket.GetName())

}

func GetTestBucketOrPanic() TestBucket {
	return GetBucketOrPanicCommon(DataBucket)
}

func GetTestIndexBucketOrPanic() TestBucket {
	return GetBucketOrPanicCommon(IndexBucket)
}

func GetTestShadowBucketOrPanic() TestBucket {
	return GetBucketOrPanicCommon(ShadowBucket)
}

func GetTestBucketSpec(bucketType CouchbaseBucketType) BucketSpec {

	bucketName := DefaultTestBucketname
	username := DefaultTestUsername
	password := DefaultTestPassword

	// Use a different bucket name for index buckets or shadow buckets to avoid interference
	switch bucketType {
	case ShadowBucket:
		bucketName = DefaultTestShadowBucketname
		username = DefaultTestShadowUsername
		password = DefaultTestShadowPassword
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

func GetBucketOrPanicCommon(bucketType CouchbaseBucketType) TestBucket {

	spec := GetTestBucketSpec(bucketType)

	if !spec.IsWalrusBucket() {

		// If this is not testing against a walrus bucket, then it's testing against a Coucbhase Server bucket,
		// and therefore needs to create the bucket if it doesn't already exist, or flush it if it does.

		tbm := NewTestBucketManager(spec)
		bucketExists, err := tbm.OpenTestBucket()
		if err != nil {
			panic(fmt.Sprintf("Error checking if bucket exists.  Spec: %+v err: %v", spec, err))
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
				panic(fmt.Sprintf("Could not create bucket.  Spec: %+v Err: %v", spec, err))
			}
		}

		// Close the bucket and any other temporary resources associated with the TestBucketManager
		tbm.Close()

	}

	// Now open the bucket _again_ to ensure it's open with the correct driver
	bucket, err := GetBucket(spec, nil)
	if err != nil {
		panic(fmt.Sprintf("Could not open bucket: %v", err))
	}

	return TestBucket{Bucket: bucket}

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
func InitializeBucketOrPanic(bucketType CouchbaseBucketType) {

	// Create
	tempBucket := GetBucketOrPanicCommon(bucketType)
	tempBucket.Close()

}

// Should Sync Gateway use XATTRS functionality when running unit tests?
func TestUseXattrs() bool {

	// First check if the SG_TEST_USE_XATTRS env variable is set
	useXattrs := os.Getenv(TestEnvSyncGatewayUseXattrs)
	switch {
	case strings.ToLower(useXattrs) == strings.ToLower(TestEnvSyncGatewayTrue):
		Infof(KeyAll, "Using xattrs: strings.ToLower(useXattrs) == strings.ToLower(TestEnvSyncGatewayTrue).  |%v| == |%v|", strings.ToLower(useXattrs), strings.ToLower(TestEnvSyncGatewayTrue))
		return true
	default:
		Infof(KeyAll, "NOT Using xattrs: strings.ToLower(useXattrs) != strings.ToLower(TestEnvSyncGatewayTrue).  |%v| != |%v|", strings.ToLower(useXattrs), strings.ToLower(TestEnvSyncGatewayTrue))
	}
	// Otherwise fallback to hardcoded default
	return DefaultUseXattrs
}

// Check the whether tests are being run with SG_TEST_BACKING_STORE=Couchbase
func TestUseCouchbaseServer() bool {
	backingStore := os.Getenv(TestEnvSyncGatewayBackingStore)
	return strings.ToLower(backingStore) == strings.ToLower(TestEnvBackingStoreCouchbase)
}

// Use views for walrus testing
func TestUseViews() bool {
	return !TestUseCouchbaseServer()
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

	//cluster, err := gocb.Connect(tbm.BucketSpec.Server)
	//if err != nil {
	//	return false, err
	//}
	//tbm.Cluster = cluster
	//
	//tbm.ClusterManager = cluster.Manager(tbm.AdministratorUsername, tbm.AdministratorPassword)
	//
	//username, password, _ := tbm.BucketSpec.Auth.GetCredentials()
	//bucket, err := tbm.Cluster.OpenBucket(tbm.BucketSpec.BucketName, password)
	//if err != nil {
	//	// Authentication failure should return an explicit error as we can't continue from here.
	//	if pkgerrors.Cause(err) == gocb.ErrAuthError {
	//		log.Printf("Unable to authenticate as %s: %v", username, err)
	//		return false, err
	//	}
	//
	//	// There could be other errors here, but we assume the bucket doesn't exist, and may be able to continue.
	//	// TODO: should check returned error type
	//	log.Printf("GoCB error opening bucket: %v", err)
	//	return false, nil
	//}
	//tbm.Bucket = bucket

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
	return GoCBBucketItemCount(tbm.Bucket.Bucket, tbm.BucketSpec, tbm.AdministratorUsername, tbm.AdministratorPassword)
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


	// Retrieve all indexes
	getIndexesStatement := fmt.Sprintf("SELECT indexes.name from system:indexes where keyspace_id = %q", gocbBucket.GetName())
	n1qlQuery := gocb.NewN1qlQuery(getIndexesStatement)
	results, err := gocbBucket.ExecuteN1qlQuery(n1qlQuery, nil)
	if err != nil {
		return indexes, err
	}

	// Close the results in a defer, and set the value of the "err" return value
	defer func() {
		err = results.Close()
	}()

	var indexRow struct {
		Name string
	}

	for results.Next(&indexRow) {
		indexes = append(indexes, indexRow.Name)
	}

	return indexes, err

}


func (tbm *TestBucketManager) FlushBucket() error {

	// Try to Flush the bucket in a retry loop
	// Ignore sporadic errors like:
	// Error trying to empty bucket. err: {"_":"Flush failed with unexpected error. Check server logs for details."}

	log.Printf("Flushing bucket %s", tbm.Bucket.Name())

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

	if err := tbm.DropIndexes(); err != nil {
		return err
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

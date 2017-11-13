package base

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb"
	"github.com/pkg/errors"
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
	Bucket Bucket
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
			createBucketErr := tbm.CreateTestBucket()
			if createBucketErr != nil {
				panic(fmt.Sprintf("Could not create bucket.  Spec: %+v Err: %v", spec, createBucketErr))
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
		Log(fmt.Sprintf("Using xattrs: strings.ToLower(useXattrs) == strings.ToLower(TestEnvSyncGatewayTrue).  |%v| == |%v|", strings.ToLower(useXattrs), strings.ToLower(TestEnvSyncGatewayTrue)))
		return true
	default:
		Log(fmt.Sprintf("NOT Using xattrs: strings.ToLower(useXattrs) != strings.ToLower(TestEnvSyncGatewayTrue).  |%v| != |%v|", strings.ToLower(useXattrs), strings.ToLower(TestEnvSyncGatewayTrue)))
	}
	// Otherwise fallback to hardcoded default
	return DefaultUseXattrs
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
	Bucket                *gocb.Bucket
	AuthHandler           AuthHandler
	Cluster               *gocb.Cluster
	ClusterManager        *gocb.ClusterManager
	BucketManager         *gocb.BucketManager
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
		return false, errors.Errorf("There are already %d open buckets.  The tests expect all buckets to be closed.", NumOpenBuckets(tbm.BucketSpec.BucketName))
	}

	IncrNumOpenBuckets(tbm.BucketSpec.BucketName)

	cluster, err := gocb.Connect(tbm.BucketSpec.Server)
	if err != nil {
		return false, err
	}
	tbm.Cluster = cluster

	tbm.ClusterManager = cluster.Manager(tbm.AdministratorUsername, tbm.AdministratorPassword)

	username, password, _ := tbm.BucketSpec.Auth.GetCredentials()
	bucket, err := tbm.Cluster.OpenBucket(tbm.BucketSpec.BucketName, password)
	if err != nil {
		// Let's assume that if there is an error opening the bucket, it's just because the
		// bucket does not exist
		// TODO: should check returned error type
		log.Printf("GoCB error opening bucket: %v", err)
		return false, nil
	}
	tbm.Bucket = bucket

	tbm.BucketManager = tbm.Bucket.Manager(username, password)

	return true, nil

}

func (tbm *TestBucketManager) Close() error {
	return tbm.Bucket.Close()
}

// GOCB doesn't currently offer a way to do this, and so this is a workaround to go directly
// to Couchbase Server REST API.
// See https://forums.couchbase.com/t/is-there-a-way-to-get-the-number-of-items-in-a-bucket/12816/4
// for GOCB discussion.
func (tbm *TestBucketManager) BucketItemCount() (itemCount int, err error) {

	reqUri := fmt.Sprintf("%s/pools/default/buckets/%s", tbm.BucketSpec.Server, tbm.BucketSpec.BucketName)
	req, err := http.NewRequest("GET", reqUri, nil)
	if err != nil {
		return -1, err
	}
	req.Header.Add("Content-Type", "application/json")

	req.SetBasicAuth(tbm.AdministratorUsername, tbm.AdministratorPassword)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		_, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return -1, err
		}
		return -1, errors.Wrap(err, "Error trying to find number of items in bucket")
	}

	respJson := map[string]interface{}{}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&respJson); err != nil {
		return -1, err
	}

	basicStats := respJson["basicStats"].(map[string]interface{})

	itemCountRaw := basicStats["itemCount"]

	itemCountFloat := itemCountRaw.(float64)

	return int(itemCountFloat), nil

}

func (tbm *TestBucketManager) EmptyTestBucket() error {

	// Try to Flush the bucket in a retry loop
	// Ignore sporadic errors like:
	// Error trying to empty bucket. err: {"_":"Flush failed with unexpected error. Check server logs for details."}

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		err = tbm.BucketManager.Flush()
		if err != nil {
			Warn("Error flushing bucket: %v  Will retry.", err)
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
			return errors.Errorf("Timed out waiting for bucket to be empty after flush.  ItemCount: %v", itemCount)
		}

		// Still items left, wait a little bit and try again
		Warn("TestBucketManager.EmptyBucket(): still %d items in bucket after flush, waiting for no items.  Will retry.", itemCount)
		time.Sleep(time.Millisecond * 500)

		numTries += 1

	}

	// Wait until high seq nos are all 0

	if tbm.Bucket.IoRouter() == nil {
		return errors.Errorf("Cannot determine number of vbuckets")
	}
	maxVbno := uint16(tbm.Bucket.IoRouter().NumVbuckets())

	workerHighSeqNosZero := func() (shouldRetry bool, err error, value interface{}) {

		stats, seqErr := tbm.Bucket.Stats("vbucket-seqno")
		if seqErr != nil {
			return false, seqErr, nil
		}

		_, highSeqnos, err := GetStatsVbSeqno(stats, maxVbno, false)

		if err != nil {
			return false, err, nil
		}

		for vbNo := uint16(0); vbNo < maxVbno; vbNo++ {
			maxSeqForVb := highSeqnos[vbNo]
			if maxSeqForVb > 0 {
				log.Printf("max seq for vb %d > 0 (%d).  Retrying", vbNo, maxSeqForVb)
				return true, nil, nil
			}
		}

		// made it this far, then it succeeded
		return false, nil, nil

	}

	err, _ = RetryLoop(
		"Wait for vb sequence numbers to be 0",
		workerHighSeqNosZero,
		CreateDoublingSleeperFunc(14, 10),
	)
	if err != nil {
		return err
	}

	return nil

}

func (tbm *TestBucketManager) RecreateOrEmptyBucket() error {
	if err := tbm.EmptyTestBucket(); err != nil {
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
			return errors.Errorf("Created bucket, but unable to connect to it after several attempts.  Spec: %+v", tbm.BucketSpec)
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

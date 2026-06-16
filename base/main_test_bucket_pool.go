/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// GTestBucketPool is a global instance of a TestBucketPool used to manage a pool of buckets for integration testing.
var GTestBucketPool *TestBucketPool

// rosmarTracker is simplified implemenation of a bucketReadierQueue for rosmar buckets. The only purpose of this
// tracker is to be able to name in low sequential orders like Couchbase Server bucket pool: rosmar0, rosmar1, etc.
type rosmarTracker struct {
	lock          sync.Mutex
	activeBuckets []bool
}

// newRosmarTracker initializes a new rosmarTracker with the specified number of buckets.
func newRosmarTracker(numBuckets int) *rosmarTracker {
	r := &rosmarTracker{
		activeBuckets: make([]bool, numBuckets),
	}
	return r
}

// getNextBucketIdx returns the next available bucket index in a rosmarTracker. This will return the lowest available
// number starting at 1. If all buckets are in use, it will return an error.
func (r *rosmarTracker) GetNextBucketIdx() (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	// iterate over len of activeBuckets to find the first lexicographically in a map
	for i, active := range r.activeBuckets {
		if !active {
			r.activeBuckets[i] = true
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("no rosmar buckets available, all have been used")
}

// ReleaseBucketIdx releases a bucket index in a rosmarTracker. This should be called when a bucket is no longer in use.
func (r *rosmarTracker) ReleaseBucketIdx(idx int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.activeBuckets[idx-1] = false
}

// TestBucketPool is used to manage a pool of pre-prepared buckets for testing purposes.
type TestBucketPool struct {
	// integrationMode should be true if using Couchbase Server. If this is false, Walrus buckets are returned instead of pooled buckets.
	integrationMode bool

	// readyBucketPool contains a buffered channel of buckets ready for use
	readyBucketPool        chan Bucket
	cluster                *tbpCluster
	bucketReadierQueue     chan tbpBucketName
	bucketReadierWaitGroup *sync.WaitGroup
	// bucketCreationDoneChan is closed when all buckets have been created and run through bucketInitFunc
	bucketCreationDoneChan chan struct{}
	ctxCancelFunc          context.CancelCauseFunc

	bucketInitFunc TBPBucketInitFunc

	stats bucketPoolStats

	// preserveBuckets can be set to true to prevent removal of a bucket used in a failing test.
	preserveBuckets bool
	// preservedBucketCount keeps track of number of preserved buckets to prevent bucket exhaustion deadlock.
	preservedBucketCount uint32

	// verbose flag controls debug test pool logging.
	verbose AtomicBool

	// keep track of tests that don't close their buckets, map of test names to bucket names
	unclosedBuckets     map[string]map[string]struct{}
	unclosedBucketsLock sync.Mutex

	rosmarBuckets rosmarTracker

	// skipCollections may be true for older Couchbase Server versions that do not support collections.
	skipCollections bool
	// numCollectionsPerBucket is the number of collections to create in each bucket
	numCollectionsPerBucket int
	// numBuckets is the number of buckets managed by the pool
	numBuckets        int
	useExistingBucket bool

	// when useDefaultScope is set, named collections are created in the default scope
	useDefaultScope bool

	// clusterSpec defines how to connect to the couchbase server cluster
	clusterSpec CouchbaseClusterSpec

	// xdcrConflictResolutionStrategy defines the conflict resolution strategy to use for XDCR, defined at bucket creation time.
	xdcrConflictResolutionStrategy XDCRConflictResolutionStrategy

	// needsBucketTeardown indicates whether the test bucket pool needs to be torn down after tests are run.
	needsBucketTeardown bool
}

type TestBucketPoolOptions struct {
	MemWatermarkThresholdMB uint64
	UseDefaultScope         bool
	RequireXDCR             bool // Test buckets will be performing XDCR, needs to be Enterprise Edition of Couchbase Server
	ParallelBucketInit      bool
	NumCollectionsPerBucket int      // setting this value in main_test.go will override the default
	TeardownFuncs           []func() // functions to be run after Main is completed but before standard teardown functions run
	NeedsBucketTeardown     bool     // whether the test bucket pool needs to be torn down after tests are run, used for goroutine dump
	NumBuckets              *int     // overrides the bucket pool size; use Ptr(0) for packages that create their own buckets via CreateTestBucket
}

// XDCRConflictResolutionStrategy defines the conflict resolution strategy to use for XDCR, defined at bucket creation time.
type XDCRConflictResolutionStrategy string

const (
	XDCRConflictResolutionStrategyLWW XDCRConflictResolutionStrategy = "lww"
	XDCRConflictResolutionStrategyMWW XDCRConflictResolutionStrategy = "mww"
)

// NewTestBucketPool initializes a new TestBucketPool. To be called from TestMain for packages requiring test buckets.
func NewTestBucketPoolWithOptions(ctx context.Context, bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc, options TestBucketPoolOptions) *TestBucketPool {
	numCollectionsPerBucket := options.NumCollectionsPerBucket
	if numCollectionsPerBucket == 0 {
		numCollectionsPerBucket = tbpNumCollectionsPerBucket(ctx)
	}

	// Used to manage cancellation of worker goroutines
	ctx, ctxCancelFunc := context.WithCancelCause(ctx)

	_, err := SetMaxFileDescriptors(ctx, 5000)
	if err != nil {
		FatalfCtx(ctx, "couldn't set max file descriptors: %v", err)
	}

	numBuckets := tbpDefaultBucketPoolSize
	if TestUseExistingBucket() {
		// SG_TEST_USE_EXISTING_BUCKET only allows for one bucket name
		numBuckets = 1
	}
	if envPoolSize := os.Getenv(tbpEnvBucketPoolSize); envPoolSize != "" {
		var err error
		numBuckets, err = strconv.Atoi(envPoolSize)
		if err != nil {
			FatalfCtx(ctx, "Couldn't parse %s: %v", tbpEnvBucketPoolSize, err)
		}
	}
	if options.NumBuckets != nil {
		numBuckets = *options.NumBuckets
	}

	preserveBuckets, _ := strconv.ParseBool(os.Getenv(tbpEnvPreserve))

	conflictResolutionStrategy := XDCRConflictResolutionStrategyLWW
	switch os.Getenv(tbpEnvXDCRConflictResolutionStrategy) {
	case "":
		// use default LWW strategy
	case string(XDCRConflictResolutionStrategyMWW):
		if UnitTestUrlIsWalrus() {
			FatalfCtx(ctx, "Walrus does not support MWW conflict resolution strategy for XDCR, see CBG-4759")
		}
		conflictResolutionStrategy = XDCRConflictResolutionStrategyMWW
	case string(XDCRConflictResolutionStrategyLWW):
		conflictResolutionStrategy = XDCRConflictResolutionStrategyLWW
	default:
		FatalfCtx(ctx, "Invalid value for %s: %s. Valid values are: %s, %s", tbpEnvXDCRConflictResolutionStrategy, os.Getenv(tbpEnvXDCRConflictResolutionStrategy), XDCRConflictResolutionStrategyLWW, XDCRConflictResolutionStrategyMWW)
	}

	tbp := TestBucketPool{
		integrationMode:                !UnitTestUrlIsWalrus() && !TestUseExistingBucket(),
		numBuckets:                     numBuckets,
		readyBucketPool:                make(chan Bucket, numBuckets),
		bucketReadierQueue:             make(chan tbpBucketName, numBuckets),
		bucketReadierWaitGroup:         &sync.WaitGroup{},
		bucketCreationDoneChan:         make(chan struct{}),
		ctxCancelFunc:                  ctxCancelFunc,
		preserveBuckets:                preserveBuckets,
		bucketInitFunc:                 bucketInitFunc,
		unclosedBuckets:                make(map[string]map[string]struct{}),
		rosmarBuckets:                  *newRosmarTracker(numBuckets),
		useExistingBucket:              TestUseExistingBucket(),
		useDefaultScope:                options.UseDefaultScope,
		numCollectionsPerBucket:        numCollectionsPerBucket,
		verbose:                        *NewAtomicBool(tbpVerbose()),
		xdcrConflictResolutionStrategy: conflictResolutionStrategy,
		clusterSpec: CouchbaseClusterSpec{
			Server:        UnitTestUrl(),
			Username:      TestClusterUsername(),
			Password:      TestClusterPassword(),
			TLSSkipVerify: TestTLSSkipVerify(),
		},
		needsBucketTeardown: options.NeedsBucketTeardown,
	}

	// We can safely skip setup if using existing buckets or rosmar buckets, since they can be opened on demand.
	if !tbp.integrationMode {
		tbp.stats.TotalBucketInitCount.Add(int32(numBuckets))
		close(tbp.bucketCreationDoneChan)
		return &tbp
	}

	tbp.cluster, err = newTestCluster(ctx, tbp.clusterSpec)
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't create test cluster: %v", err)
	}

	tbp.skipCollections = !tbp.canUseNamedCollections()

	expectedCouchbaseServerVersion, err := NewComparableBuildVersionFromString("7.6.5")
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't create expected Couchbase Server version: %v", err)
	}
	if os.Getenv(tbpEnvAllowIncompatibleServerVersion) == "" && tbp.cluster.version.Less(expectedCouchbaseServerVersion) {
		overrideMsg := "Set " + tbpEnvAllowIncompatibleServerVersion + "=true to override this check."
		tbp.Fatalf(ctx, "Sync Gateway requires mobile XDCR support, but Couchbase Server %v does not support it. Couchbase Server %s is required. %s", tbp.cluster.version, expectedCouchbaseServerVersion, overrideMsg)
	}

	// Start up an async readier worker to process dirty buckets
	go tbp.bucketReadierWorker(ctx, bucketReadierFunc)

	err = tbp.removeOldTestBuckets(ctx)
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't remove old test buckets: %v", err)
	}

	go tbp.createTestBuckets(ctx, numBuckets, tbpBucketQuotaMB(ctx), bucketInitFunc, options.ParallelBucketInit)

	return &tbp
}

func (tbp *TestBucketPool) markBucketOpened(t testing.TB, b Bucket) {
	tbp.unclosedBucketsLock.Lock()
	defer tbp.unclosedBucketsLock.Unlock()

	_, ok := tbp.unclosedBuckets[t.Name()]
	if !ok {
		tbp.unclosedBuckets[t.Name()] = make(map[string]struct{})
	}

	tbp.unclosedBuckets[t.Name()][b.GetName()] = struct{}{}
}

func (tbp *TestBucketPool) markBucketClosed(t testing.TB, b Bucket) {
	tbp.unclosedBucketsLock.Lock()
	defer tbp.unclosedBucketsLock.Unlock()

	// Check for unclosed view query operations. A fatal error will occur if queue is not cleared
	testCtx := TestCtx(t)
	switch typedBucket := b.(type) {
	case *GocbV2Bucket:
		tbp.checkForViewOpsQueueEmptied(testCtx, b.GetName(), typedBucket.queryOps)
	}

	if tMap, ok := tbp.unclosedBuckets[t.Name()]; ok {
		delete(tMap, b.GetName())
		if len(tMap) == 0 {
			delete(tbp.unclosedBuckets, t.Name())
		}
	}
}

func (tbp *TestBucketPool) checkForViewOpsQueueEmptied(ctx context.Context, bucketName string, c chan struct{}) {
	if err, _ := RetryLoop(ctx, bucketName+"-emptyViewOps", func() (bool, error, any) {
		if len(c) > 0 {
			return true, fmt.Errorf("view op queue not cleared. remaining: %d", len(c)), nil
		}
		return false, nil, nil
	}, CreateSleeperFunc(90, 1000)); err != nil {
		tbp.Fatalf(ctx, "View op queue check failed: %v", err)
	}
}

// getRosmarURL returns the rosmar URL to use for a test bucket. Persistent buckets use a temp directory.
func (tbp *TestBucketPool) getRosmarURL(t testing.TB, persistent bool) string {
	if persistent {
		return rosmarUriFromPath(t.TempDir())
	}
	return kTestWalrusURL
}

// openRosmarBucket opens a rosmar bucket at the given URL with the given name, dispatching between
// in-memory and file-based URLs.
func openRosmarBucket(url, bucketName string) (*rosmar.Bucket, error) {
	if url == "walrus:" || url == rosmar.InMemoryURL {
		return rosmar.OpenBucket(url, bucketName, rosmar.CreateOrOpen)
	}
	return rosmar.OpenBucketIn(url, bucketName, rosmar.CreateOrOpen)
}

func (tbp *TestBucketPool) GetWalrusTestBucket(t testing.TB, url string) (b Bucket, s BucketSpec, teardown func(context.Context)) {
	testCtx := TestCtx(t)
	if !UnitTestUrlIsWalrus() {
		tbp.Fatalf(testCtx, "nil TestBucketPool, but not using a Walrus test URL")
	}

	bucketIdx, err := tbp.rosmarBuckets.GetNextBucketIdx()
	if err != nil {
		tbp.Fatalf(testCtx, "Couldn't get next rosmar bucket index: %v", err)
	}
	const typeName = "rosmar"
	bucketName := fmt.Sprintf("rosmar%d", bucketIdx)
	walrusBucket, err := openRosmarBucket(url, bucketName)
	if err != nil {
		tbp.Fatalf(testCtx, "couldn't get %s bucket from <%s>: %v", typeName, url, err)
	}

	b = walrusBucket
	// Wrap Walrus buckets with a leaky bucket to support vbucket IDs on feed.

	ctx := bucketCtx(testCtx, b)
	tbp.Logf(ctx, "Creating new %s test bucket", typeName)

	tbp.createCollections(ctx, walrusBucket)

	initFuncStart := time.Now()
	err = tbp.bucketInitFunc(ctx, b, tbp)
	if err != nil {
		tbp.Fatalf(ctx, "couldn't run bucket init func: %v", err)
	}
	tbp.stats.TotalBucketInitCount.Add(1)
	atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(initFuncStart).Nanoseconds())
	tbp.markBucketOpened(t, b)

	atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
	openedStart := time.Now()
	bucketClosed := &AtomicBool{}

	bucketSpec := getTestBucketSpec(tbp.clusterSpec, tbpBucketName(b.GetName()))
	bucketSpec.Server = url

	return b, bucketSpec, func(ctx context.Context) {
		if !bucketClosed.CompareAndSwap(false, true) {
			tbp.Logf(ctx, "Bucket teardown was already called. Ignoring.")
			return
		}

		tbp.Logf(ctx, "Teardown called - Closing %s test bucket", typeName)
		atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
		atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(openedStart).Nanoseconds())
		tbp.markBucketClosed(t, b)
		// Persisted buckets should call close and delete
		closeErr := walrusBucket.CloseAndDelete(ctx)
		if closeErr != nil {
			tbp.Fatalf(ctx, "Unexpected error closing persistent %s bucket: %v", typeName, closeErr)
			return
		}
		tbp.rosmarBuckets.ReleaseBucketIdx(bucketIdx)
	}
}

// GetExistingBucket opens a bucket connection to an existing bucket
func (tbp *TestBucketPool) GetExistingBucket(t testing.TB) (b Bucket, s BucketSpec, teardown func(context.Context)) {
	ctx := TestCtx(t)

	// each bucket opens its own cluster connection since Bucket.Close will close the underlying gocb.Cluster
	bucketCluster, connstr, err := getGocbClusterForTest(ctx, tbp.clusterSpec)
	require.NoError(t, err, "couldn't get gocb cluster for test")

	bucketName := tbpBucketName(TestUseExistingBucketName())
	bucketSpec := getTestBucketSpec(tbp.clusterSpec, bucketName)
	bucketFromSpec, err := GetGocbV2BucketFromCluster(ctx, bucketCluster, bucketSpec, connstr, waitForReadyBucketTimeout, false)
	require.NoError(t, err, "couldn't get bucket from cluster")

	return bucketFromSpec, bucketSpec, func(ctx context.Context) {
		tbp.Logf(ctx, "Teardown called - Closing connection to existing bucket")
		bucketFromSpec.Close(ctx)
	}
}

// GetTestBucketAndSpec returns a bucket to be used during a test.
// The returned teardownFn MUST be called once the test is done,
// which closes the bucket, readies it for a new test, and releases back into the pool.
// persistentBucket flag determines behaviour for walrus buckets only; Couchbase bucket
// behaviour is defined by the bucket pool readier/init.
func (tbp *TestBucketPool) getTestBucketAndSpec(t testing.TB, persistentBucket bool) (b Bucket, s BucketSpec, teardownFn func(context.Context)) {

	ctx := TestCtx(t)

	// Return a new Walrus bucket when tbp has not been initialized
	if !tbp.integrationMode {
		tbp.Logf(ctx, "Getting walrus test bucket - tbp.integrationMode is not set")
		return tbp.GetWalrusTestBucket(t, tbp.getRosmarURL(t, persistentBucket))
	}

	if tbp.useExistingBucket {
		tbp.Logf(ctx, "Using predefined bucket")
		return tbp.GetExistingBucket(t)
	}

	if atomic.LoadUint32(&tbp.preservedBucketCount) >= uint32(cap(tbp.readyBucketPool)) {
		tbp.Logf(ctx,
			"No more buckets available for testing. All pooled buckets have been preserved by failing tests.")
		t.Skipf("No more buckets available for testing. All pooled buckets have been preserved for failing tests.")
	}

	tbp.Logf(ctx, "Attempting to get test bucket from pool")
	waitingBucketStart := time.Now()
	var bucket Bucket
	select {
	case bucket = <-tbp.readyBucketPool:
	case <-time.After(waitForReadyBucketTimeout):
		tbp.Logf(ctx, "Timed out after %s waiting for a bucket to become available.", waitForReadyBucketTimeout)
		t.Fatalf("TEST: Timed out after %s waiting for a bucket to become available.", waitForReadyBucketTimeout)
	}
	atomic.AddInt64(&tbp.stats.TotalWaitingForReadyBucketNano, time.Since(waitingBucketStart).Nanoseconds())

	ctx = bucketCtx(ctx, bucket)
	tbp.Logf(ctx, "Got test bucket from pool")
	tbp.markBucketOpened(t, bucket)

	atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
	bucketOpenStart := time.Now()
	bucketClosed := &AtomicBool{}
	bucketSpec := getTestBucketSpec(tbp.clusterSpec, tbpBucketName(bucket.GetName()))
	return bucket, bucketSpec, func(ctx context.Context) {
		if !bucketClosed.CompareAndSwap(false, true) {
			tbp.Logf(ctx, "Bucket teardown was already called. Ignoring.")
			return
		}

		tbp.Logf(ctx, "Teardown called - closing bucket")
		atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
		atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(bucketOpenStart).Nanoseconds())
		tbp.markBucketClosed(t, bucket)
		bucket.Close(ctx)

		if tbp.preserveBuckets && t.Failed() {
			tbp.Logf(ctx, "Test using bucket failed. Preserving bucket for later inspection")
			atomic.AddUint32(&tbp.preservedBucketCount, 1)
			return
		}

		tbp.Logf(ctx, "Teardown called - Pushing into bucketReadier queue")
		tbp.addBucketToReadierQueue(ctx, tbpBucketName(bucket.GetName()))
	}
}

func (tbp *TestBucketPool) addBucketToReadierQueue(ctx context.Context, name tbpBucketName) {
	tbp.bucketReadierWaitGroup.Add(1)
	tbp.Logf(ctx, "Putting bucket onto bucketReadierQueue")
	tbp.bucketReadierQueue <- name
}

// Close waits for any buckets to be cleaned, and closes the pool.
func (tbp *TestBucketPool) Close(ctx context.Context) {
	if tbp == nil {
		// noop
		return
	}
	defer tbp.printStats()

	if !tbp.needsBucketTeardown {
		return
	}
	tbp.Logf(ctx, "Closing TestBucketPool and closing all buckets")
	// Cancel async workers
	if tbp.ctxCancelFunc != nil {
		tbp.ctxCancelFunc(errors.New("TestBucketPool.Close called"))
		tbp.Logf(ctx, "Waiting for bucket readier to finish")
		tbp.bucketReadierWaitGroup.Wait()
		tbp.Logf(ctx, "Waiting for bucket creation to finish")
		<-tbp.bucketCreationDoneChan
		tbp.Logf(ctx, "Bucket creation finished")
	}

	if tbp.cluster != nil {
	bucketLoop:
		for _ = range tbp.numBuckets {
			select {
			case bucket := <-tbp.readyBucketPool:
				tbp.Logf(ctx, "Closing bucket %s", bucket.GetName())
				bucket.Close(ctx)
			default:
				break bucketLoop
			}
		}
		if err := tbp.cluster.close(); err != nil {
			tbp.Logf(ctx, "Couldn't close cluster connection: %v", err)
		}
	}
}

// removeOldTestBuckets removes all buckets starting with testBucketNamePrefix
func (tbp *TestBucketPool) removeOldTestBuckets(ctx context.Context) error {
	buckets, err := tbp.cluster.getBucketNames()
	if err != nil {
		return errors.Wrap(err, "couldn't retrieve buckets from cluster manager")
	}

	wg := sync.WaitGroup{}

	for _, b := range buckets {
		if strings.HasPrefix(b, tbpBucketNamePrefix) {
			ctx := BucketNameCtx(ctx, b)
			tbp.Logf(ctx, "Removing old test bucket")
			wg.Add(1)

			// Run the RemoveBucket requests concurrently, as it takes a while per bucket.
			go func(b string) {
				err := tbp.cluster.removeBucket(b)
				if err != nil {
					tbp.Logf(ctx, "Error removing old test bucket: %v", err)
				} else {
					tbp.Logf(ctx, "Removed old test bucket")
				}

				wg.Done()
			}(b)
		}
	}

	wg.Wait()

	return nil
}

func (tbp *TestBucketPool) emptyPreparedStatements(ctx context.Context, b Bucket) {
	tbp.Logf(ctx, "Emptying prepared statements for bucket")
	// if the bucket is a N1QLStore, clean up prepared statements as-per the advice from the query team
	if n1qlStore, ok := AsN1QLStore(b.DefaultDataStore(ctx)); ok {
		if err := n1qlStore.waitUntilQueryServiceReady(time.Minute); err != nil {
			tbp.Fatalf(ctx, "Timed out waiting for query service to be ready: %v", err)
		}

		// search all statements for those containing a string matching the bucket name (which will also include all scope/collection-based prepared statements)
		queryRes, err := n1qlStore.Query(ctx, `DELETE FROM system:prepareds WHERE statement LIKE "%`+b.GetName()+`%";`, nil, RequestPlus, true)
		if err != nil {
			tbp.Fatalf(ctx, "Couldn't remove old prepared statements: %v", err)
		}

		if err := queryRes.Close(ctx); err != nil {
			tbp.Fatalf(ctx, "Failed to close query: %v", err)
		}
	}
}

// setXDCRBucketSetting sets the bucket setting "enableCrossClusterVersioning" for mobile XDCR
func (tbp *TestBucketPool) setXDCRBucketSetting(ctx context.Context, bucket Bucket) {

	tbp.Logf(ctx, "Setting crossClusterVersioningEnabled=true")

	// retry for 1 minute to get this bucket setting, MB-63675
	store, ok := AsCouchbaseBucketStore(bucket)
	if !ok {
		tbp.Fatalf(ctx, "unable to get server management endpoints. Underlying bucket type was not GoCBBucket")
	}

	posts := url.Values{}
	posts.Add("enableCrossClusterVersioning", "true")

	url := fmt.Sprintf("/pools/default/buckets/%s", store.GetName())
	// retry for 1 minute to get this bucket setting, MB-63675
	err, _ := RetryLoop(ctx, "setXDCRBucketSetting", func() (bool, error, any) {
		output, statusCode, err := store.MgmtRequest(ctx, http.MethodPost, url, "application/x-www-form-urlencoded", strings.NewReader(posts.Encode()))
		if err != nil {
			tbp.Fatalf(ctx, "request to mobile XDCR bucket setting failed, status code: %d error: %s output: %s", statusCode, err, string(output))
		}
		if statusCode != http.StatusOK {
			err := fmt.Errorf("request to mobile XDCR bucket setting failed with status code, %d, output: %s", statusCode, string(output))
			return true, err, nil
		}
		return false, nil, nil
	}, CreateMaxDoublingSleeperFunc(200, 500, 500))
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't set crossClusterVersioningEnabled: %v", err)
	}
}

// createCollections will create a set of test collections on the bucket, if enabled...
func (tbp *TestBucketPool) createCollections(ctx context.Context, bucket Bucket) {
	// Initialize the default and mobile system datastores. rosmar does not pre-create these on
	// bucket creation, so accessing them here ensures they exist before any test code runs.
	_ = bucket.DefaultDataStore(ctx)
	_, _ = bucket.NamedDataStore(ctx, MobileSystemScopeAndCollectionName())

	// If we're able to use collections, the test bucket pool will also create N collections per bucket - rather than just getting the default collection ready.
	if tbp.skipCollections {
		return
	}

	dynamicDataStore, ok := bucket.(sgbucket.DynamicDataStoreBucket)
	if !ok {
		tbp.Fatalf(ctx, "Bucket doesn't support dynamic collection creation %T", bucket)
	}

	for i := 0; i < tbp.NumCollectionsPerBucket(); i++ {
		scopeName := tbp.testScopeName()
		collectionName := fmt.Sprintf("%s%d", tbpCollectionPrefix, i)
		ctx := KeyspaceLogCtx(ctx, bucket.GetName(), scopeName, collectionName)

		tbp.Logf(ctx, "Creating new collection: %s.%s", scopeName, collectionName)
		dataStoreName := ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}
		err := dynamicDataStore.CreateDataStore(ctx, dataStoreName)
		if err != nil {
			tbp.Fatalf(ctx, "Couldn't create datastore %v.%v: %v", scopeName, collectionName, err)
		}
	}
}

// insertAndOpenTestBucket creates a new CBS bucket, opens a connection to it, validates its
// storage backend, and applies pool-level settings (empty prepared statements, XDCR). It does not
// call createCollections or the pool's bucketInitFunc. On any failure after the bucket has been
// created, it removes the bucket from the cluster before returning the error.
func (tbp *TestBucketPool) insertAndOpenTestBucket(ctx context.Context, bucketName tbpBucketName, bucketQuotaMB int) (Bucket, error) {
	tbp.Logf(ctx, "Creating new test bucket")
	err := tbp.cluster.insertBucket(string(bucketName), bucketQuotaMB, tbp.xdcrConflictResolutionStrategy)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't create test bucket: %w", err)
	}

	bucket, err := tbp.cluster.openTestBucket(ctx, bucketName, waitForReadyBucketTimeout)
	if err != nil {
		_ = tbp.cluster.removeBucket(string(bucketName))
		return nil, fmt.Errorf("timed out trying to open new bucket: %w", err)
	}

	b, err := AsGocbV2Bucket(bucket)
	if err != nil {
		bucket.Close(ctx)
		_ = tbp.cluster.removeBucket(string(bucketName))
		return nil, fmt.Errorf("couldn't assert bucket as GocbV2Bucket: %w", err)
	}
	storageBackend, err := b.getStorageBackend(ctx)
	if err != nil {
		bucket.Close(ctx)
		_ = tbp.cluster.removeBucket(string(bucketName))
		return nil, fmt.Errorf("couldn't get storage backend for bucket %s: %w", bucketName, err)
	}
	if storageBackend != tbp.cluster.storageBackend {
		bucket.Close(ctx)
		_ = tbp.cluster.removeBucket(string(bucketName))
		return nil, fmt.Errorf("bucket %s has storage backend %s, expected %s", bucketName, storageBackend, tbp.cluster.storageBackend)
	}

	tbp.emptyPreparedStatements(ctx, bucket)
	if tbp.cluster.ee {
		tbp.setXDCRBucketSetting(ctx, bucket)
	}
	return bucket, nil
}

// CreateTestBucket creates a fresh bucket outside the pool. Unlike pool buckets it does not call
// createCollections or the pool's bucketInitFunc, so callers get a bare bucket they can configure
// themselves. Works for both Walrus and Couchbase Server. The caller is responsible for cleanup;
// see RemoveBucket.
func tbpBucketNameAt(idx int, ts time.Time) tbpBucketName {
	return tbpBucketName(fmt.Sprintf(tbpBucketNameFormat, tbpBucketNamePrefix, idx, ts.UnixNano()))
}

func createTestBucketName() tbpBucketName {
	return tbpBucketNameAt(0, time.Now())
}

func testBucketCtx(ctx context.Context, bucketName tbpBucketName) context.Context {
	return BucketNameCtx(ctx, string(bucketName))
}

func (tbp *TestBucketPool) CreateTestBucket(t testing.TB) *TestBucket {
	ctx := TestCtx(t)
	bucketName := createTestBucketName()
	ctx = testBucketCtx(ctx, bucketName)

	if !tbp.integrationMode {
		tbp.Logf(ctx, "Creating new rosmar test bucket")
		rosmarBucket, err := openRosmarBucket(tbp.getRosmarURL(t, false), string(bucketName))
		require.NoError(t, err, "couldn't open rosmar bucket")
		return &TestBucket{
			Bucket:     rosmarBucket,
			BucketSpec: getTestBucketSpec(tbp.clusterSpec, bucketName),
			closeFn:    func(ctx context.Context) { require.NoError(t, rosmarBucket.CloseAndDelete(ctx)) },
			t:          t,
		}
	}

	bucket, err := tbp.insertAndOpenTestBucket(ctx, bucketName, tbpBucketQuotaMB(ctx))
	require.NoError(t, err)
	return &TestBucket{
		Bucket:     bucket,
		BucketSpec: getTestBucketSpec(tbp.clusterSpec, bucketName),
		closeFn:    func(ctx context.Context) { bucket.Close(ctx) },
		t:          t,
	}
}

// RemoveBucket closes a bucket and, for integration-mode buckets, removes it from the cluster.
// Intended as a t.Cleanup counterpart to CreateTestBucket.
func (tbp *TestBucketPool) RemoveBucket(tb *TestBucket) {
	ctx := TestCtx(tb.t)
	tb.Close(ctx)
	if tbp.integrationMode {
		require.NoError(tb.t, tbp.cluster.removeBucket(tb.GetName()), "Couldn't remove bucket %s", tb.GetName())
	}
}

// createTestBuckets creates a new set of integration test buckets and pushes them into the readier queue.
func (tbp *TestBucketPool) createTestBuckets(ctx context.Context, numBuckets, bucketQuotaMB int, bucketInitFunc TBPBucketInitFunc, parallelBucketInit bool) {

	defer close(tbp.bucketCreationDoneChan)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(numBuckets)

	// Append a timestamp to all of the bucket names to ensure uniqueness across a single package.
	// Not strictly required, but can help to prevent (index) resources from being incorrectly reused on the server side for recently deleted buckets.
	bucketNameTimestamp := time.Now()

	// create required number of buckets (skipping any already existing ones)
	for i := range numBuckets {
		bucketName := tbpBucketNameAt(i, bucketNameTimestamp)
		ctx := testBucketCtx(ctx, bucketName)

		bucketInit := func() {
			defer wg.Done()
			ctx := testBucketCtx(ctx, bucketName)

			bucket, err := tbp.insertAndOpenTestBucket(ctx, bucketName, bucketQuotaMB)
			if ctx.Err() != nil {
				return
			} else if err != nil {
				tbp.Fatalf(ctx, "Couldn't create test bucket: %v", err)
			}

			tbp.createCollections(ctx, bucket)

			itemName := "bucket"
			err, _ = RetryLoop(ctx, bucket.GetName()+"bucketInitRetry", func() (bool, error, any) {
				tbp.Logf(ctx, "Running %s through init function", itemName)
				err := bucketInitFunc(ctx, bucket, tbp)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return false, err, nil
					}
					tbp.Logf(ctx, "Couldn't init %s, got error: %v - Retrying", itemName, err)
					return true, err, nil
				}
				return false, nil, nil
			}, CreateSleeperFunc(5, 1000))
			if ctx.Err() != nil {
				bucket.Close(ctx)
				return
			} else if err != nil {
				tbp.Fatalf(ctx, "Couldn't init %s, got error: %v - Aborting", itemName, err)
			}
			tbp.readyBucketPool <- bucket
		}
		// Creation of buckets might be faster in parallel in the case of flush bucket, but slower if GSI is involved.
		if parallelBucketInit {
			go bucketInit()
		} else {
			bucketInit()
		}
	}
	// wait for the async bucket creation and opening of buckets to finish
	wg.Wait()

	atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(start).Nanoseconds())
	tbp.stats.TotalBucketInitCount.Add(int32(numBuckets))
}

// bucketReadierWorker reads a channel of "dirty" buckets (bucketReadierQueue), does something to get them ready, and then puts them back into the pool.
// The mechanism for getting the bucket ready can vary by package being tested (for instance, a package not requiring views or GSI can use FlushBucketEmptierFunc)
// A package requiring views or GSI, will need to pass in the db.ViewsAndGSIBucketReadier function.
func (tbp *TestBucketPool) bucketReadierWorker(ctx context.Context, bucketReadierFunc TBPBucketReadierFunc) {
	tbp.Logf(ctx, "Starting bucketReadier")

loop:
	for {
		select {
		case <-ctx.Done():
			tbp.Logf(ctx, "bucketReadier got ctx cancelled")
			break loop

		case dirtyBucket := <-tbp.bucketReadierQueue:
			atomic.AddInt32(&tbp.stats.TotalBucketReadierCount, 1)
			ctx := BucketNameCtx(ctx, string(dirtyBucket))
			tbp.Logf(ctx, "bucketReadier got bucket")

			go func(testBucketName tbpBucketName) {
				// We might not actually be "done" with the bucket if something fails,
				// but we need to release the waitgroup so tbp.Close() doesn't block forever.
				defer tbp.bucketReadierWaitGroup.Done()

				start := time.Now()
				b, err := tbp.cluster.openTestBucket(ctx, testBucketName, waitForReadyBucketTimeout)
				if err != nil {
					tbp.Logf(ctx, "Couldn't open bucket to get ready, got error: %v", err)
					return
				}
				ctx = KeyspaceLogCtx(ctx, b.GetName(), "", "")

				err, _ = RetryLoop(ctx, b.GetName()+"bucketReadierRetry", func() (bool, error, any) {
					tbp.Logf(ctx, "Running bucket through readier function")
					err = bucketReadierFunc(ctx, b, tbp)
					if err != nil {
						tbp.Logf(ctx, "Couldn't ready bucket, got error: %v - Retrying", err)
						return true, err, nil
					}
					return false, nil, nil
				}, CreateSleeperFunc(15, 2000))
				if err != nil {
					tbp.Logf(ctx, "Couldn't ready bucket, got error: %v - Aborting readier for bucket", err)
					return
				}

				tbp.Logf(ctx, "Bucket ready, putting back into ready pool")
				tbp.readyBucketPool <- b

				atomic.AddInt64(&tbp.stats.TotalBucketReadierDurationNano, time.Since(start).Nanoseconds())
			}(dirtyBucket)
		}
	}

	tbp.Logf(ctx, "Stopped bucketReadier")
}

func (tbp *TestBucketPool) testScopeName() string {
	if tbp.useDefaultScope {
		return DefaultScope
	} else {
		return fmt.Sprintf("%s%d", tbpScopePrefix, 0)
	}
}

// supportsViews returns true if the bucket type supports views, not if SG_TEST_USE_GSI is set. Magma buckets do not
// support views.
func (tbp *TestBucketPool) supportsViews() bool {
	if !tbp.integrationMode {
		return true
	}
	return tbp.cluster.storageBackend != gocb.StorageBackendMagma
}

// TBPBucketInitFunc is a function that is run once (synchronously) when creating/opening a bucket.
type TBPBucketInitFunc func(ctx context.Context, b Bucket, tbp *TestBucketPool) error

// NoopInitFunc does nothing to init a bucket. This can be used in conjunction with FlushBucketReadier when there's no requirement for views/GSI.
var NoopInitFunc TBPBucketInitFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {
	return nil
}

// TBPBucketReadierFunc is a function that runs once a test is finished with a bucket. This runs asynchronously.
type TBPBucketReadierFunc func(ctx context.Context, b Bucket, tbp *TestBucketPool) error

// NoopTBPBucketReadierFunc does nothing to ready a bucket. For use with packages that create their own buckets via CreateTestBucket.
var NoopTBPBucketReadierFunc TBPBucketReadierFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {
	return nil
}

// FlushBucketEmptierFunc ensures the bucket is empty by flushing. It is not recommended to use with GSI.
var FlushBucketEmptierFunc TBPBucketReadierFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {

	bucket, err := AsGocbV2Bucket(b)
	if err != nil {
		return err
	}
	return bucket.Flush(ctx)
}

// tbpBucketName use a strongly typed bucket name.
type tbpBucketName string

// TestBucketPoolMain is used as TestMain in main_test.go packages
func TestBucketPoolMain(ctx context.Context, m *testing.M, bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc,
	options TestBucketPoolOptions) {
	// can't use defer because of os.Exit
	teardownFuncs := make([]func(), 0)
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestLogging(ctx))
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestProfiling(m))
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestMemoryWatermark(m, options.MemWatermarkThresholdMB))
	teardownFuncs = append(teardownFuncs, options.TeardownFuncs...)
	SkipPrometheusStatsRegistration = true

	dumpGoroutines, _ := strconv.ParseBool(os.Getenv(TestEnvGoroutineDump))
	// dumpGoroutins requires bucket teardown to be run on exit, but if NeedsBucketTeardown for some other reason, we don't want to override it.
	if !options.NeedsBucketTeardown {
		options.NeedsBucketTeardown = dumpGoroutines
	}

	GTestBucketPool = NewTestBucketPoolWithOptions(ctx, bucketReadierFunc, bucketInitFunc, options)
	teardownFuncs = append(teardownFuncs, func() { GTestBucketPool.Close(ctx) })

	teardownFuncs = append(teardownFuncs, func() {
		if numAssertionFails := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().AssertionFailCount.Value(); numAssertionFails > 0 {
			panic(fmt.Sprintf("Test harness failed due to %d assertion failures. Search logs for %q", numAssertionFails, AssertionFailedPrefix))
		}
	})
	// must be the last teardown function added to the list to correctly detect leaked goroutines
	if dumpGoroutines {
		teardownFuncs = append(teardownFuncs, SetUpTestGoroutineDump(m))
	}
	if options.RequireXDCR && GTestBucketPool.cluster != nil && !GTestBucketPool.cluster.ee {
		SkipTestMain(m, "Test requires XDCR, but Couchbase Server is not Enterprise edition")
	}

	// Run the test suite
	status := m.Run()

	for _, fn := range teardownFuncs {
		fn()
	}

	os.Exit(status)
}

// TestBucketPoolNoIndexes runs a TestMain for packages that do not require creation of indexes
func TestBucketPoolNoIndexes(ctx context.Context, m *testing.M, options TestBucketPoolOptions) {
	TestBucketPoolMain(ctx, m, FlushBucketEmptierFunc, NoopInitFunc, options)
}

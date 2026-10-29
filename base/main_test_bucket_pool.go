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

// TestBucketPool is used to manage a pool of pre-prepared buckets for testing purposes.
type TestBucketPool struct {
	// integrationMode should be true if using Couchbase Server. If this is false, Walrus buckets are returned instead of pooled buckets.
	integrationMode bool

	// numBuckets is the number of buckets managed by the pool
	numBuckets int

	// readyBucketPool contains a buffered channel of buckets ready for use
	readyBucketPool        chan Bucket
	cluster                *tbpCluster
	bucketReadierQueue     chan tbpBucketName
	bucketReadierWaitGroup *sync.WaitGroup
	ctxCancelFunc          context.CancelFunc

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

	// createTestBucketIdx is incremented atomically to give each CreateTestBucket call a unique bucket name.
	createTestBucketIdx atomic.Int64

	// skipCollections may be true for older Couchbase Server versions that do not support collections.
	skipCollections bool
	// numCollectionsPerBucket is the number of collections to create in each bucket
	numCollectionsPerBucket int
	useExistingBucket       bool

	// skipMobileXDCR may be true for older versions of Couchbase Server that don't support mobile XDCR enhancements
	skipMobileXDCR bool

	// when useDefaultScope is set, named collections are created in the default scope
	useDefaultScope bool
}

type TestBucketPoolOptions struct {
	MemWatermarkThresholdMB uint64
	UseDefaultScope         bool
	RequireXDCR             bool // Test buckets will be performing XDCR, requires Server > 7 for integration test robustness
	ParallelBucketInit      bool
	NumCollectionsPerBucket int      // setting this value in main_test.go will override the default
	TeardownFuncs           []func() // functions to be run after Main is completed but before standard teardown functions run
	NumBuckets              *int     // overrides the bucket pool size; use Ptr(0) for packages that create their own buckets via CreateTestBucket
}

func NewTestBucketPool(ctx context.Context, bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc) *TestBucketPool {
	return NewTestBucketPoolWithOptions(ctx, bucketReadierFunc, bucketInitFunc, TestBucketPoolOptions{})
}

// NewTestBucketPool initializes a new TestBucketPool. To be called from TestMain for packages requiring test buckets.
func NewTestBucketPoolWithOptions(ctx context.Context, bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc, options TestBucketPoolOptions) *TestBucketPool {
	numCollectionsPerBucket := options.NumCollectionsPerBucket
	if numCollectionsPerBucket == 0 {
		numCollectionsPerBucket = tbpNumCollectionsPerBucket(ctx)
	}

	// Used to manage cancellation of worker goroutines
	ctx, ctxCancelFunc := context.WithCancel(ctx)

	_, err := SetMaxFileDescriptors(ctx, 5000)
	if err != nil {
		FatalfCtx(ctx, "couldn't set max file descriptors: %v", err)
	}

	numBuckets := tbpNumBuckets(ctx)
	if options.NumBuckets != nil {
		numBuckets = *options.NumBuckets
	}

	preserveBuckets, _ := strconv.ParseBool(os.Getenv(tbpEnvPreserve))
	tbp := TestBucketPool{
		integrationMode:         !UnitTestUrlIsWalrus() && !TestUseExistingBucket(),
		numBuckets:              numBuckets,
		readyBucketPool:         make(chan Bucket, numBuckets),
		bucketReadierQueue:      make(chan tbpBucketName, numBuckets),
		bucketReadierWaitGroup:  &sync.WaitGroup{},
		ctxCancelFunc:           ctxCancelFunc,
		preserveBuckets:         preserveBuckets,
		bucketInitFunc:          bucketInitFunc,
		unclosedBuckets:         make(map[string]map[string]struct{}),
		useExistingBucket:       TestUseExistingBucket(),
		useDefaultScope:         options.UseDefaultScope,
		skipMobileXDCR:          !UnitTestUrlIsWalrus(), // do not set up enableCrossClusterVersioning until Sync Gateway 4.x
		numCollectionsPerBucket: numCollectionsPerBucket,
		verbose:                 *NewAtomicBool(tbpVerbose()),
	}

	// We can safely skip setup if using existing buckets or rosmar buckets, since they can be opened on demand.
	if !tbp.integrationMode {
		tbp.stats.TotalBucketInitCount.Add(int32(numBuckets))
		return &tbp
	}

	tbp.cluster = newTestCluster(ctx, UnitTestUrl(), &tbp)

	useCollections, err := tbp.canUseNamedCollections(ctx)
	if err != nil {
		tbp.Fatalf(ctx, "%s", err)
	}
	tbp.skipCollections = !useCollections

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
	if err, _ := RetryLoop(ctx, bucketName+"-emptyViewOps", func() (bool, error, interface{}) {
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
// in-memory and file-based URLs, and ensures the default datastore exists.
func openRosmarBucket(url, bucketName string) (*rosmar.Bucket, error) {
	var bucket *rosmar.Bucket
	var err error
	if url == "walrus:" || url == rosmar.InMemoryURL {
		bucket, err = rosmar.OpenBucket(url, bucketName, rosmar.CreateOrOpen)
	} else {
		bucket, err = rosmar.OpenBucketIn(url, bucketName, rosmar.CreateOrOpen)
	}
	if err != nil {
		return nil, err
	}
	ensureDefaultDataStore(bucket)
	return bucket, nil
}

func (tbp *TestBucketPool) GetWalrusTestBucket(t testing.TB, url string) (b Bucket, s BucketSpec, teardown func(context.Context)) {
	testCtx := TestCtx(t)
	if !UnitTestUrlIsWalrus() {
		tbp.Fatalf(testCtx, "nil TestBucketPool, but not using a Walrus test URL")
	}

	id, err := GenerateRandomID()
	require.NoError(t, err)

	const typeName = "rosmar"
	bucketName := tbpBucketNamePrefix + "rosmar_" + id
	walrusBucket, err := openRosmarBucket(url, bucketName)
	if err != nil {
		tbp.Fatalf(testCtx, "couldn't get %s bucket from <%s>: %v", typeName, url, err)
	}

	// Wrap Walrus buckets with a leaky bucket to support vbucket IDs on feed.
	b = &LeakyBucket{bucket: walrusBucket, config: &LeakyBucketConfig{}}

	ctx := bucketCtx(testCtx, b)
	tbp.Logf(ctx, "Creating new %s test bucket", typeName)

	tbp.CreateCollections(ctx, walrusBucket, tbp.NumCollectionsPerBucket())

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

	bucketSpec := getTestBucketSpec(tbpBucketName(b.GetName()))
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
			tbp.Logf(ctx, "Unexpected error closing persistent %s bucket: %v", typeName, closeErr)
		}

	}
}

// GetExistingBucket opens a bucket connection to an existing bucket
func (tbp *TestBucketPool) GetExistingBucket(t testing.TB) (b Bucket, s BucketSpec, teardown func(context.Context)) {
	ctx := TestCtx(t)

	// each bucket opens its own cluster connection since Bucket.Close will close the underlying gocb.Cluster
	bucketCluster, connstr := getGocbClusterForTest(ctx, UnitTestUrl())

	bucketName := tbpBucketName(TestUseExistingBucketName())
	bucketSpec := getTestBucketSpec(bucketName)
	bucketFromSpec, err := GetGocbV2BucketFromCluster(ctx, bucketCluster, bucketSpec, connstr, waitForReadyBucketTimeout, false)
	if err != nil {
		tbp.Fatalf(ctx, "couldn't get existing collection from cluster: %v", err)
	}
	DebugfCtx(ctx, KeySGTest, "opened bucket %s", bucketName)

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

	if tbp.numBuckets == 0 {
		tbp.Fatalf(ctx, "GetTestBucket called on a pool configured with NumBuckets: 0; use CreateTestBucket instead")
	}

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
	bucketSpec := getTestBucketSpec(tbpBucketName(bucket.GetName()))
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

	// Cancel async workers
	if tbp.ctxCancelFunc != nil {
		tbp.ctxCancelFunc()
		tbp.bucketReadierWaitGroup.Wait()
	}

	if tbp.cluster != nil {
		if err := tbp.cluster.close(ctx); err != nil {
			tbp.Logf(ctx, "Couldn't close cluster connection: %v", err)
		}
	}

	tbp.printStats()
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
	if n1qlStore, ok := AsN1QLStore(b.DefaultDataStore()); ok {
		if err := n1qlStore.waitUntilQueryServiceReady(time.Minute); err != nil {
			tbp.Fatalf(ctx, "Timed out waiting for query service to be ready: %v", err)
		}

		// search all statements for those containing a string matching the bucket name (which will also include all scope/collection-based prepared statements)
		queryRes, err := n1qlStore.Query(ctx, `DELETE FROM system:prepareds WHERE statement LIKE "%`+b.GetName()+`%";`, nil, RequestPlus, true)
		if err != nil {
			tbp.Fatalf(ctx, "Couldn't remove old prepared statements: %v", err)
		}

		if err := queryRes.Close(); err != nil {
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
	err, _ := RetryLoop(ctx, "setXDCRBucketSetting", func() (bool, error, interface{}) {
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

// ensureDefaultDataStore creates the default datastore; rosmar doesn't pre-create it on bucket creation.
func ensureDefaultDataStore(bucket Bucket) {
	_ = bucket.DefaultDataStore()
}

// CreateCollections ensures the default datastore exists on bucket, and creates
// numCollections additional named collections, if enabled.
func (tbp *TestBucketPool) CreateCollections(ctx context.Context, bucket Bucket, numCollections int) {
	if numCollections < 0 {
		tbp.Fatalf(ctx, "CreateCollections called with numCollections: %d", numCollections)
	}
	ensureDefaultDataStore(bucket)
	if numCollections == 0 {
		return
	}
	// If we're able to use collections, the test bucket pool will also create N collections per bucket - rather than just getting the default collection ready.
	if tbp.skipCollections {
		return
	}

	dynamicDataStore, ok := bucket.(sgbucket.DynamicDataStoreBucket)
	if !ok {
		tbp.Fatalf(ctx, "Bucket doesn't support dynamic collection creation %T", bucket)
	}

	for i := range numCollections {
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

// insertAndOpenTestBucket creates and opens a new CBS bucket, validating its storage backend and
// applying pool-level settings. On failure it removes the bucket, unless ctx was cancelled, in
// which case cleanup is skipped so the test harness can exit early.
func (tbp *TestBucketPool) insertAndOpenTestBucket(ctx context.Context, bucketName tbpBucketName, bucketQuotaMB int) (bucket Bucket, err error) {
	tbp.Logf(ctx, "Creating new test bucket")
	err = tbp.cluster.insertBucket(ctx, string(bucketName), bucketQuotaMB)
	if ctx.Err() != nil {
		// don't clean up so that test harness exits early if the bucket is being created in the background
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't create test bucket: %w", err)
	}

	bucket, err = tbp.cluster.openTestBucket(ctx, bucketName, waitForReadyBucketTimeout)
	if err != nil {
		_ = tbp.cluster.removeBucket(string(bucketName))
		return nil, fmt.Errorf("timed out trying to open new bucket: %w", err)
	}
	// From here on, the bucket has been opened as well as created, so clean it up if we return with an error below.
	openBucket := bucket
	defer func() {
		if err != nil {
			openBucket.Close(ctx)
			_ = tbp.cluster.removeBucket(string(bucketName))
		}
	}()

	b, err := AsGocbV2Bucket(bucket)
	if err != nil {
		return nil, fmt.Errorf("couldn't assert bucket as GocbV2Bucket: %w", err)
	}
	storageBackend, err := b.getStorageBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't get storage backend for bucket %s: %w", bucketName, err)
	}
	if storageBackend != tbp.cluster.storageBackend {
		return nil, fmt.Errorf("bucket %s has storage backend %s, expected %s", bucketName, storageBackend, tbp.cluster.storageBackend)
	}

	tbp.emptyPreparedStatements(ctx, bucket)
	if tbp.skipMobileXDCR {
		tbp.Logf(ctx, "Not setting crossClusterVersioningEnabled")
	} else {
		tbp.setXDCRBucketSetting(ctx, bucket)
	}
	return bucket, nil
}

// createTestBucketName returns a unique bucket name, using an atomic counter rather than relying
// solely on clock resolution.
func (tbp *TestBucketPool) createTestBucketName() tbpBucketName {
	idx := int(tbp.createTestBucketIdx.Add(1))
	return tbpBucketName(fmt.Sprintf(tbpBucketNameFormat, tbpBucketNamePrefix, idx, time.Now().UnixNano()))
}

// CreateTestBucket creates a fresh bucket outside the pool, with only the default datastore
// created. The caller is responsible for cleanup; see RemoveBucket.
func (tbp *TestBucketPool) CreateTestBucket(t testing.TB) *TestBucket {
	ctx := TestCtx(t)
	bucketName := tbp.createTestBucketName()
	ctx = BucketNameCtx(ctx, string(bucketName))

	if !tbp.integrationMode {
		tbp.Logf(ctx, "Creating new rosmar test bucket")
		rosmarBucket, err := openRosmarBucket(tbp.getRosmarURL(t, false), string(bucketName))
		require.NoError(t, err, "couldn't open rosmar bucket")
		return &TestBucket{
			Bucket:     rosmarBucket,
			BucketSpec: getTestBucketSpec(bucketName),
			closeFn:    func(ctx context.Context) { require.NoError(t, rosmarBucket.CloseAndDelete(ctx)) },
			t:          t,
		}
	}

	bucket, err := tbp.insertAndOpenTestBucket(ctx, bucketName, tbpBucketQuotaMB(ctx))
	require.NoError(t, err)
	ensureDefaultDataStore(bucket)
	return &TestBucket{
		Bucket:     bucket,
		BucketSpec: getTestBucketSpec(bucketName),
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

	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(numBuckets)

	// Append a timestamp to all of the bucket names to ensure uniqueness across a single package.
	// Not strictly required, but can help to prevent (index) resources from being incorrectly reused on the server side for recently deleted buckets.
	bucketNameTimestamp := time.Now()

	// create required number of buckets (skipping any already existing ones)
	for i := range numBuckets {
		bucketInit := func() {
			defer wg.Done()
			bucketName := tbpBucketName(fmt.Sprintf(tbpBucketNameFormat, tbpBucketNamePrefix, i, bucketNameTimestamp.UnixNano()))
			ctx := BucketNameCtx(ctx, string(bucketName))

			bucket, err := tbp.insertAndOpenTestBucket(ctx, bucketName, bucketQuotaMB)
			if ctx.Err() != nil {
				return
			} else if err != nil {
				tbp.Fatalf(ctx, "Couldn't create test bucket: %v", err)
			}

			tbp.CreateCollections(ctx, bucket, tbp.NumCollectionsPerBucket())

			itemName := "bucket"
			err, _ = RetryLoop(ctx, bucket.GetName()+"bucketInitRetry", func() (bool, error, interface{}) {
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

				err, _ = RetryLoop(ctx, b.GetName()+"bucketReadierRetry", func() (bool, error, interface{}) {
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

	GTestBucketPool = NewTestBucketPoolWithOptions(ctx, bucketReadierFunc, bucketInitFunc, options)
	teardownFuncs = append(teardownFuncs, func() { GTestBucketPool.Close(ctx) })

	teardownFuncs = append(teardownFuncs, func() {
		if numAssertionFails := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().AssertionFailCount.Value(); numAssertionFails > 0 {
			panic(fmt.Sprintf("Test harness failed due to %d assertion failures. Search logs for %q", numAssertionFails, AssertionFailedPrefix))
		}
	})
	// must be the last teardown function added to the list to correctly detect leaked goroutines
	teardownFuncs = append(teardownFuncs, SetUpTestGoroutineDump(m))

	if options.RequireXDCR && GTestBucketPool.cluster != nil && GTestBucketPool.cluster.majorVersion < 7 {
		SkipTestMain(m, "Test requires XDCR, but the cluster version is less than 7. Skipping test.")
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

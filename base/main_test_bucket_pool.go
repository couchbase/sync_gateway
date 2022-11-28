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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/walrus"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// GTestBucketPool is a global instance of a TestBucketPool used to manage a pool of buckets for integration testing.
var GTestBucketPool *TestBucketPool

// a dirtyBucket is passed to bucketReadier to be cleaned
type dirtyBucket struct {
	name tbpBucketName
}

type cleanBucket struct {
	name tbpBucketName
}

type tbpBucket struct {
	bucket         Bucket
	dataStoreNames []ScopeAndCollectionName
}

// TestBucketPool is used to manage a pool of pre-prepared buckets for testing purposes.
type TestBucketPool struct {
	// integrationMode should be true if using Couchbase Server. If this is false, Walrus buckets are returned instead of pooled buckets.
	integrationMode bool

	// readyBucketPool contains a buffered channel of buckets ready for use
	readyBucketPool        chan Bucket
	cluster                tbpCluster
	bucketReadierQueue     chan dirtyBucket
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

	// useCollections may be false for older Couchbase Server versions that do not support collections.
	useCollections    bool
	useExistingBucket bool
}

// NewTestBucketPool initializes a new TestBucketPool. To be called from TestMain for packages requiring test buckets.
func NewTestBucketPool(bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc) *TestBucketPool {
	// We can safely skip setup when we want Walrus buckets to be used.
	// They'll be created on-demand via GetTestBucketAndSpec,
	// which is fast enough for Walrus that we don't need to prepare buckets ahead of time.
	if !TestUseCouchbaseServer() || TestUseExistingBucket() {
		tbp := TestBucketPool{
			bucketInitFunc:    bucketInitFunc,
			unclosedBuckets:   make(map[string]map[string]struct{}),
			integrationMode:   TestUseCouchbaseServer(),
			useExistingBucket: TestUseExistingBucket(),
		}
		tbp.verbose.Set(tbpVerbose())
		return &tbp
	}

	_, err := SetMaxFileDescriptors(5000)
	if err != nil {
		FatalfCtx(context.TODO(), "couldn't set max file descriptors: %v", err)
	}

	numBuckets := tbpNumBuckets()

	// Used to manage cancellation of worker goroutines
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	preserveBuckets, _ := strconv.ParseBool(os.Getenv(tbpEnvPreserve))

	tbp := TestBucketPool{
		integrationMode:        true,
		readyBucketPool:        make(chan Bucket, numBuckets),
		bucketReadierQueue:     make(chan dirtyBucket, numBuckets),
		bucketReadierWaitGroup: &sync.WaitGroup{},
		ctxCancelFunc:          ctxCancelFunc,
		preserveBuckets:        preserveBuckets,
		bucketInitFunc:         bucketInitFunc,
		unclosedBuckets:        make(map[string]map[string]struct{}),
		useExistingBucket:      TestUseExistingBucket(),
	}

	tbp.cluster = newTestCluster(UnitTestUrl(), tbp.Logf)

	useCollections, err := tbp.shouldUseCollections()
	if err != nil {
		tbp.Fatalf(ctx, "%s", err)
	}
	tbp.useCollections = useCollections

	tbp.verbose.Set(tbpVerbose())

	// Start up an async readier worker to process dirty buckets
	go tbp.bucketReadierWorker(ctx, bucketReadierFunc)

	err = tbp.removeOldTestBuckets()
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't remove old test buckets: %v", err)
	}

	numCollectionsPerBucket := tbpNumCollectionsPerBucket()

	// Make sure the test buckets are created and put into the readier worker queue
	start := time.Now()
	if err := tbp.createTestBuckets(numBuckets, numCollectionsPerBucket, tbpBucketQuotaMB(), bucketInitFunc); err != nil {
		tbp.Fatalf(ctx, "Couldn't create test buckets: %v", err)
	}
	atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(start).Nanoseconds())
	atomic.AddInt32(&tbp.stats.TotalBucketInitCount, int32(numBuckets))

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
	if err, _ := RetryLoop(bucketName+"-emptyViewOps", func() (bool, error, interface{}) {
		if len(c) > 0 {
			return true, fmt.Errorf("view op queue not cleared. remaining: %d", len(c)), nil
		}
		return false, nil, nil
	}, CreateSleeperFunc(90, 1000)); err != nil {
		tbp.Fatalf(ctx, "View op queue check failed: %v", err)
	}
}

func (tbp *TestBucketPool) GetWalrusTestBucket(t testing.TB, url string) (b Bucket, s BucketSpec, teardown func()) {
	testCtx := TestCtx(t)
	if !UnitTestUrlIsWalrus() {
		tbp.Fatalf(testCtx, "nil TestBucketPool, but not using a Walrus test URL")
	}

	id, err := GenerateRandomID()
	require.NoError(t, err)

	walrusBucket, err := walrus.GetCollectionBucket(url, tbpBucketNamePrefix+"walrus_"+id)
	if err != nil {
		tbp.Fatalf(testCtx, "couldn't get walrus bucket: %v", err)
	}

	// Wrap Walrus buckets with a leaky bucket to support vbucket IDs on feed.
	b = &LeakyBucket{bucket: walrusBucket, config: &LeakyBucketConfig{TapFeedVbuckets: true}}

	ctx := bucketCtx(testCtx, b)
	tbp.Logf(ctx, "Creating new walrus test bucket")

	initFuncStart := time.Now()
	err = tbp.bucketInitFunc(ctx, b, tbp)
	if err != nil {
		tbp.Fatalf(ctx, "couldn't run bucket init func: %v", err)
	}
	atomic.AddInt32(&tbp.stats.TotalBucketInitCount, 1)
	atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(initFuncStart).Nanoseconds())
	tbp.markBucketOpened(t, b)

	atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
	openedStart := time.Now()
	bucketClosed := &AtomicBool{}

	bucketSpec := getTestBucketSpec(tbpBucketName(b.GetName()))
	bucketSpec.Server = url

	return b, bucketSpec, func() {
		if !bucketClosed.CompareAndSwap(false, true) {
			tbp.Logf(ctx, "Bucket teardown was already called. Ignoring.")
			return
		}

		tbp.Logf(ctx, "Teardown called - Closing walrus test bucket")
		atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
		atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(openedStart).Nanoseconds())
		tbp.markBucketClosed(t, b)
		b.Close()
	}
}

// GetExistingBucket opens a bucket conection to an existing bucket
func (tbp *TestBucketPool) GetExistingBucket(t testing.TB) (b Bucket, s BucketSpec, teardown func()) {
	testCtx := TestCtx(t)

	bucketCluster := initV2Cluster(UnitTestUrl())

	bucketName := tbpBucketName(TestUseExistingBucketName())
	bucketSpec := getTestBucketSpec(bucketName)

	bucket := bucketCluster.Bucket(bucketSpec.BucketName)
	err := bucket.WaitUntilReady(waitForReadyBucketTimeout, nil)
	if err != nil {
		return nil, bucketSpec, nil
	}
	DebugfCtx(context.TODO(), KeySGTest, "opened bucket %s", bucketName)

	bucketFromSpec, err := GetGocbV2BucketFromCluster(bucketCluster, bucketSpec, waitForReadyBucketTimeout)
	if err != nil {
		tbp.Fatalf(testCtx, "couldn't get existing collection from cluster: %v", err)
	}

	return bucketFromSpec, bucketSpec, func() {
		tbp.Logf(testCtx, "Teardown called - Closing connection to existing bucket")
		bucketFromSpec.Close()
	}
}

// GetTestBucketAndSpec returns a bucket to be used during a test.
// The returned teardownFn MUST be called once the test is done,
// which closes the bucket, readies it for a new test, and releases back into the pool.
func (tbp *TestBucketPool) getTestBucketAndSpec(t testing.TB) (b Bucket, s BucketSpec, teardownFn func()) {

	ctx := TestCtx(t)

	// Return a new Walrus bucket when tbp has not been initialized
	if !tbp.integrationMode {
		tbp.Logf(ctx, "Getting walrus test bucket - tbp.integrationMode is not set")
		return tbp.GetWalrusTestBucket(t, kTestWalrusURL)
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
	return bucket, getTestBucketSpec(tbpBucketName(bucket.GetName())), func() {
		if !bucketClosed.CompareAndSwap(false, true) {
			tbp.Logf(ctx, "Bucket teardown was already called. Ignoring.")
			return
		}

		tbp.Logf(ctx, "Teardown called - closing bucket")
		atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
		atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(bucketOpenStart).Nanoseconds())
		tbp.markBucketClosed(t, bucket)
		bucket.Close()

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
	tbp.bucketReadierQueue <- dirtyBucket{name: name}
}

// Close waits for any buckets to be cleaned, and closes the pool.
func (tbp *TestBucketPool) Close() {
	if tbp == nil {
		// noop
		return
	}

	// Cancel async workers
	if tbp.ctxCancelFunc != nil {
		tbp.bucketReadierWaitGroup.Wait()
		tbp.ctxCancelFunc()
	}

	if tbp.cluster != nil {
		if err := tbp.cluster.close(); err != nil {
			tbp.Logf(context.Background(), "Couldn't close cluster connection: %v", err)
		}
	}

	tbp.printStats()
}

// removeOldTestBuckets removes all buckets starting with testBucketNamePrefix
func (tbp *TestBucketPool) removeOldTestBuckets() error {
	buckets, err := tbp.cluster.getBucketNames()
	if err != nil {
		return errors.Wrap(err, "couldn't retrieve buckets from cluster manager")
	}

	wg := sync.WaitGroup{}

	for _, b := range buckets {
		if strings.HasPrefix(b, tbpBucketNamePrefix) {
			ctx := bucketNameCtx(context.Background(), b)
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

func (tbp *TestBucketPool) emptyPreparedStatements(ctx context.Context, ds DataStore) {
	tbp.Logf(ctx, "Emptying prepared statements")
	// if the bucket is a N1QLStore, clean up prepared statements as-per the advice from the query team
	if n1qlStore, ok := AsN1QLStore(ds); ok {
		if err := n1qlStore.waitUntilQueryServiceReady(time.Minute); err != nil {
			tbp.Fatalf(ctx, "Timed out waiting for query service to be ready: %v", err)
		}

		queryRes, err := n1qlStore.Query(`DELETE FROM system:prepareds WHERE statement LIKE "%`+KeyspaceQueryToken+`%";`, nil, RequestPlus, true)
		if err != nil {
			tbp.Fatalf(ctx, "Couldn't remove old prepared statements: %v", err)
		}

		if err := queryRes.Close(); err != nil {
			tbp.Fatalf(ctx, "Failed to close query: %v", err)
		}
	}
}

// createTestBuckets creates a new set of integration test buckets and pushes them into the readier queue.
func (tbp *TestBucketPool) createTestBuckets(numBuckets, numCollectionsPerBucket int, bucketQuotaMB int, bucketInitFunc TBPBucketInitFunc) error {

	// keep references to opened buckets for use later in this function
	var (
		openBuckets     = make(map[string]Bucket, numBuckets)
		openBucketsLock sync.Mutex // protects openBuckets
	)

	wg := sync.WaitGroup{}
	wg.Add(numBuckets)

	// Append a timestamp to all of the bucket names to ensure uniqueness across a single package.
	// Not strictly required, but can help to prevent (index) resources from being incorrectly reused on the server side for recently deleted buckets.
	bucketNameTimestamp := time.Now().UnixNano()

	// create required number of buckets (skipping any already existing ones)
	for i := 0; i < numBuckets; i++ {
		testBucketName := fmt.Sprintf(tbpBucketNameFormat, tbpBucketNamePrefix, i, bucketNameTimestamp)
		ctx := bucketNameCtx(context.Background(), testBucketName)

		// Bucket creation takes a few seconds for each bucket,
		// so create and wait for readiness concurrently.
		go func(bucketName string) {
			ctx := bucketNameCtx(ctx, bucketName)

			tbp.Logf(ctx, "Creating new test bucket")
			err := tbp.cluster.insertBucket(bucketName, bucketQuotaMB)
			if err != nil {
				tbp.Fatalf(ctx, "Couldn't create test bucket: %v", err)
			}

			bucket, err := tbp.cluster.openTestBucket(tbpBucketName(bucketName), waitForReadyBucketTimeout)
			if err != nil {
				tbp.Fatalf(ctx, "Timed out trying to open new bucket: %v", err)
			}
			openBucketsLock.Lock()
			openBuckets[bucketName] = bucket
			openBucketsLock.Unlock()

			// If set, the test bucket pool will also create N collections per bucket - rather than just getting the default collection ready.
			if tbp.useCollections {
				dynamicDataStore, ok := bucket.(sgbucket.DynamicDataStoreBucket)
				if !ok {
					tbp.Fatalf(ctx, "Bucket doesn't support dynamic collection creation")
				}
				for i := 0; i < numCollectionsPerBucket; i++ {
					scopeName := fmt.Sprintf("%s%d", tbpScopePrefix, 0)
					collectionName := fmt.Sprintf("%s%d", tbpCollectionPrefix, i)
					ctx := keyspaceNameCtx(ctx, bucketName, scopeName, collectionName)
					tbp.Logf(ctx, "Creating new collection: %s.%s", scopeName, collectionName)
					dataStoreName := ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}
					err := dynamicDataStore.CreateDataStore(dataStoreName)
					if err != nil {
						tbp.Fatalf(ctx, "Couldn't create datastore %v.%v: %v", scopeName, collectionName, err)
					}
					tbp.emptyPreparedStatements(ctx, bucket.NamedDataStore(dataStoreName))
				}
			}

			tbp.emptyPreparedStatements(ctx, bucket.DefaultDataStore())

			wg.Done()
		}(testBucketName)
	}

	// wait for the async bucket creation and opening of buckets to finish
	wg.Wait()

	// All the buckets are created and opened, so now we can perform some synchronous setup (e.g. Creating GSI indexes)
	for i := 0; i < numBuckets; i++ {
		testBucketName := fmt.Sprintf(tbpBucketNameFormat, tbpBucketNamePrefix, i, bucketNameTimestamp)
		ctx := bucketNameCtx(context.Background(), testBucketName)

		tbp.Logf(ctx, "running bucketInitFunc")
		b := openBuckets[testBucketName]

		itemName := "bucket"
		if err, _ := RetryLoop(b.GetName()+"bucketInitRetry", func() (bool, error, interface{}) {
			tbp.Logf(ctx, "Running %s through init function", itemName)
			ctx = keyspaceNameCtx(ctx, b.GetName(), "", "")
			err := bucketInitFunc(ctx, b, tbp)
			if err != nil {
				tbp.Logf(ctx, "Couldn't init %s, got error: %v - Retrying", itemName, err)
				return true, err, nil
			}
			return false, nil, nil
		}, CreateSleeperFunc(5, 1000)); err != nil {
			tbp.Fatalf(ctx, "Couldn't init %s, got error: %v - Aborting", itemName, err)
		}

		b.Close()
		tbp.addBucketToReadierQueue(ctx, tbpBucketName(testBucketName))
	}

	return nil
}

// bucketReadierWorker reads a channel of "dirty" buckets (bucketReadierQueue), does something to get them ready, and then puts them back into the pool.
// The mechanism for getting the bucket ready can vary by package being tested (for instance, a package not requiring views or GSI can use FlushBucketEmptierFunc)
// A package requiring views or GSI, will need to pass in the db.ViewsAndGSIBucketReadier function.
func (tbp *TestBucketPool) bucketReadierWorker(ctx context.Context, bucketReadierFunc TBPBucketReadierFunc) {
	tbp.Logf(context.Background(), "Starting bucketReadier")

loop:
	for {
		select {
		case <-ctx.Done():
			tbp.Logf(context.Background(), "bucketReadier got ctx cancelled")
			break loop

		case dirtyBucket := <-tbp.bucketReadierQueue:
			atomic.AddInt32(&tbp.stats.TotalBucketReadierCount, 1)
			ctx := bucketNameCtx(ctx, string(dirtyBucket.name))
			tbp.Logf(ctx, "bucketReadier got bucket")

			go func(testBucketName tbpBucketName) {
				// We might not actually be "done" with the bucket if something fails,
				// but we need to release the waitgroup so tbp.Close() doesn't block forever.
				defer tbp.bucketReadierWaitGroup.Done()

				start := time.Now()
				b, err := tbp.cluster.openTestBucket(testBucketName, waitForReadyBucketTimeout)
				ctx = keyspaceNameCtx(ctx, b.GetName(), "", "")
				if err != nil {
					tbp.Logf(ctx, "Couldn't open bucket to get ready, got error: %v", err)
					return
				}

				err, _ = RetryLoop(b.GetName()+"bucketReadierRetry", func() (bool, error, interface{}) {
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
			}(dirtyBucket.name)
		}
	}

	tbp.Logf(context.Background(), "Stopped bucketReadier")
}

// TBPBucketInitFunc is a function that is run once (synchronously) when creating/opening a bucket.
type TBPBucketInitFunc func(ctx context.Context, b Bucket, tbp *TestBucketPool) error

// NoopInitFunc does nothing to init a bucket. This can be used in conjunction with FlushBucketReadier when there's no requirement for views/GSI.
var NoopInitFunc TBPBucketInitFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {
	return nil
}

// TBPBucketReadierFunc is a function that runs once a test is finished with a bucket. This runs asynchronously.
type TBPBucketReadierFunc func(ctx context.Context, b Bucket, tbp *TestBucketPool) error

// FlushBucketEmptierFunc ensures the bucket is empty by flushing. It is not recommended to use with GSI.
var FlushBucketEmptierFunc TBPBucketReadierFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {

	flushableBucket, ok := b.(sgbucket.FlushableStore)
	if !ok {
		return errors.New("FlushBucketEmptierFunc used with non-flushable bucket")
	}
	return flushableBucket.Flush()
}

// N1QLBucketEmptierFunc ensures the Bucket and all of its DataStore are empty by using N1QL deletes. This is the preferred approach when using GSI.
var N1QLBucketEmptierFunc TBPBucketReadierFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {
	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}
	for _, dataStoreName := range dataStores {
		n1qlStore, ok := AsN1QLStore(b.NamedDataStore(dataStoreName))
		if !ok {
			return errors.New("N1QLBucketEmptierFunc used with non-N1QL store")
		}

		if hasPrimary, _, err := getIndexMetaWithoutRetry(n1qlStore, PrimaryIndexName); err != nil {
			return err
		} else if !hasPrimary {
			return fmt.Errorf("bucket does not have primary index, so can't empty bucket using N1QL")
		}

		if itemCount, err := QueryBucketItemCount(n1qlStore); err != nil {
			return err
		} else if itemCount == 0 {
			tbp.Logf(ctx, "Bucket already empty - skipping")
		} else {
			tbp.Logf(ctx, "Bucket not empty (%d items), emptying bucket via N1QL", itemCount)
			// Use N1QL to empty bucket, with the hope that the query service is happier to deal with this than a bucket flush/rollback.
			// Requires a primary index on the bucket.
			// TODO: How can we delete xattr only docs from here too? It would avoid needing to call db.emptyAllDocsIndex in ViewsAndGSIBucketReadier
			res, err := n1qlStore.Query(fmt.Sprintf(`DELETE FROM %s`, KeyspaceQueryToken), nil, RequestPlus, true)
			if err != nil {
				return err
			}
			_ = res.Close()
		}
	}

	return nil
}

// tbpBucketName use a strongly typed bucket name.
type tbpBucketName string

// TestBucketPoolMain is used as TestMain in main_test.go packages
func TestBucketPoolMain(m *testing.M, bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc,
	memWatermarkThresholdMB uint64) {
	// can't use defer because of os.Exit
	teardownFuncs := make([]func(), 0)
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestLogging(m))
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestProfiling(m))
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestMemoryWatermark(m, memWatermarkThresholdMB))

	SkipPrometheusStatsRegistration = true

	GTestBucketPool = NewTestBucketPool(bucketReadierFunc, bucketInitFunc)
	teardownFuncs = append(teardownFuncs, GTestBucketPool.Close)

	// must be the last teardown function added to the list to correctly detect leaked goroutines
	teardownFuncs = append(teardownFuncs, SetUpTestGoroutineDump(m))

	// Run the test suite
	status := m.Run()

	for _, fn := range teardownFuncs {
		fn()
	}

	os.Exit(status)
}

// TestBucketPoolNoIndexes runs a TestMain for packages that do not require creation of indexes
func TestBucketPoolNoIndexes(m *testing.M, memWatermarkThresholdMB uint64) {
	TestBucketPoolMain(m, FlushBucketEmptierFunc, NoopInitFunc, memWatermarkThresholdMB)
}

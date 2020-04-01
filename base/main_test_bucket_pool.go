package base

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocb"
	"github.com/couchbaselabs/walrus"
	"github.com/pkg/errors"
)

const (
	testBucketQuotaMB    = 150
	testBucketNamePrefix = "sg_int_"
	testClusterUsername  = "Administrator"
	testClusterPassword  = "password"

	// Creates this many buckets in the backing store to be pooled for testing.
	defaultBucketPoolSize = 10
	testEnvPoolSize       = "SG_TEST_BUCKET_POOL_SIZE"

	// Prevents reuse and cleanup of buckets used in failed tests for later inspection.
	// When all pooled buckets are in a preserved state, any remaining tests are skipped.
	testEnvPreserve = "SG_TEST_BUCKET_POOL_PRESERVE"

	// When set to true, first, all existing test buckets are removed, before creating new ones.
	testEnvRecreate = "SG_TEST_BACKING_STORE_RECREATE"

	// Prints detailed logs from the test pooling framework.
	testEnvVerbose = "SG_TEST_BUCKET_POOL_DEBUG"
)

type bucketName string

type bucketPoolStats struct {
	TotalBucketInitDurationNano    int64
	TotalBucketInitCount           int32
	TotalBucketReadierDurationNano int64
	TotalBucketReadierCount        int32
	NumBucketsOpened               int32
	NumBucketsClosed               int32
	TotalWaitingForReadyBucketNano int64
	TotalInuseBucketNano           int64
}

type GocbTestBucketPool struct {
	integrationMode        bool
	readyBucketPool        chan *CouchbaseBucketGoCB
	bucketReadierQueue     chan bucketName
	bucketReadierWaitGroup *sync.WaitGroup
	cluster                *gocb.Cluster
	clusterMgr             *gocb.ClusterManager
	ctxCancelFunc          context.CancelFunc
	defaultBucketSpec      BucketSpec

	BucketInitFunc BucketInitFunc

	stats bucketPoolStats

	useGSI bool

	// preserveBuckets can be set to true to prevent removal of a bucket used in a failing test.
	preserveBuckets bool
	// keep track of number of preserved buckets to prevent bucket exhaustion deadlock
	preservedBucketCount uint32

	// Enables test pool logging
	verbose AtomicBool
}

// numBuckets returns the configured number of buckets to use in the pool.
func numBuckets() int {
	numBuckets := defaultBucketPoolSize
	if envPoolSize := os.Getenv(testEnvPoolSize); envPoolSize != "" {
		var err error
		numBuckets, err = strconv.Atoi(envPoolSize)
		if err != nil {
			log.Fatalf("Couldn't parse %s: %v", testEnvPoolSize, err)
		}
	}
	return numBuckets
}

func testCluster(server string) *gocb.Cluster {
	spec := BucketSpec{
		Server: server,
	}

	connStr, err := spec.GetGoCBConnString()
	if err != nil {
		log.Fatalf("error getting connection string: %v", err)
	}

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		log.Fatalf("Couldn't connect to %q: %v", server, err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: testClusterUsername,
		Password: testClusterPassword,
	})
	if err != nil {
		log.Fatalf("Couldn't authenticate with %q: %v", server, err)
	}

	return cluster
}

type BucketInitFunc func(ctx context.Context, b Bucket, tbp *GocbTestBucketPool) error

type GocbBucketReadierFunc func(ctx context.Context, b *CouchbaseBucketGoCB, tbp *GocbTestBucketPool) error

// FlushBucketEmptierFunc ensures the bucket is empty by flushing. It is not recommended to use with GSI.
var FlushBucketEmptierFunc GocbBucketReadierFunc = func(ctx context.Context, b *CouchbaseBucketGoCB, tbp *GocbTestBucketPool) error {
	return b.Flush()
}

// N1QLBucketEmptierFunc ensures the bucket is empty by using N1QL deletes. This is the preferred approach when using GSI.
var N1QLBucketEmptierFunc GocbBucketReadierFunc = func(ctx context.Context, b *CouchbaseBucketGoCB, tbp *GocbTestBucketPool) error {
	if hasPrimary, _, err := b.getIndexMetaWithoutRetry(PrimaryIndexName); err != nil {
		return err
	} else if !hasPrimary {
		return fmt.Errorf("bucket does not have primary index, so can't empty bucket using N1QL")
	}

	if itemCount, err := b.QueryBucketItemCount(); err != nil {
		return err
	} else if itemCount == 0 {
		tbp.Logf(ctx, "Bucket already empty - skipping")
	} else {
		tbp.Logf(ctx, "Bucket not empty (%d items), emptying bucket via N1QL", itemCount)
		// Use N1QL to empty bucket, with the hope that the query service is happier to deal with this than a bucket flush/rollback.
		// Requires a primary index on the bucket.
		res, err := b.Query(`DELETE FROM $_bucket`, nil, gocb.RequestPlus, false)
		if err != nil {
			return err
		}
		_ = res.Close()
	}

	return nil
}

// NoopInitFunc does nothing to init a bucket. This can be used in conjunction with FlushBucketReadier when there's no requirement for views/GSI.
var NoopInitFunc BucketInitFunc = func(ctx context.Context, b Bucket, tbp *GocbTestBucketPool) error {
	return nil
}

// PrimaryIndexInitFunc creates a primary index on the given bucket, if the bucket is a gocbbucket.
var PrimaryIndexInitFunc BucketInitFunc = func(ctx context.Context, b Bucket, tbp *GocbTestBucketPool) error {
	gocbBucket, ok := AsGoCBBucket(b)
	if !ok {
		tbp.Logf(ctx, "skipping primary index creation for non-gocb bucket")
		return nil
	}

	if hasPrimary, _, err := gocbBucket.getIndexMetaWithoutRetry(PrimaryIndexName); err != nil {
		return err
	} else if !hasPrimary {
		err := gocbBucket.CreatePrimaryIndex(PrimaryIndexName, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

var defaultBucketSpec = BucketSpec{
	Server:          UnitTestUrl(),
	CouchbaseDriver: GoCBCustomSGTranscoder,
	Auth: TestAuthenticator{
		Username: testClusterUsername,
		Password: testClusterPassword,
	},
	UseXattrs: TestUseXattrs(),
}

func tbpEnvVerbose() bool {
	verbose, _ := strconv.ParseBool(os.Getenv(testEnvVerbose))
	return verbose
}

// NewTestBucketPool initializes a new GocbTestBucketPool. To be called from TestMain for packages requiring test buckets.
// Set useGSI to false to skip index creation for packages that don't require GSI, to speed up bucket readiness.
func NewTestBucketPool(bucketReadierFunc GocbBucketReadierFunc, bucketInitFunc BucketInitFunc) *GocbTestBucketPool {
	// We can safely skip setup when we want Walrus buckets to be used. They'll be created on-demand via GetTestBucketAndSpec.
	if !TestUseCouchbaseServer() {
		tbp := GocbTestBucketPool{
			BucketInitFunc: bucketInitFunc,
		}
		tbp.verbose.Set(tbpEnvVerbose())
		return &tbp
	}

	_, err := SetMaxFileDescriptors(5000)
	if err != nil {
		panic(err)
	}

	numBuckets := numBuckets()
	// TODO: What about pooling servers too??
	// That way, we can have unlimited buckets available in a single test pool... True horizontal scalability in tests!
	cluster := testCluster(UnitTestUrl())

	// Used to manage cancellation of worker goroutines
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	preserveBuckets, _ := strconv.ParseBool(os.Getenv(testEnvPreserve))

	tbp := GocbTestBucketPool{
		integrationMode:        true,
		readyBucketPool:        make(chan *CouchbaseBucketGoCB, numBuckets),
		bucketReadierQueue:     make(chan bucketName, numBuckets),
		bucketReadierWaitGroup: &sync.WaitGroup{},
		cluster:                cluster,
		clusterMgr:             cluster.Manager(testClusterUsername, testClusterPassword),
		ctxCancelFunc:          ctxCancelFunc,
		defaultBucketSpec:      defaultBucketSpec,
		preserveBuckets:        preserveBuckets,
		BucketInitFunc:         bucketInitFunc,
	}
	tbp.verbose.Set(tbpEnvVerbose())

	// Start up an async readier worker to process dirty buckets
	go tbp.bucketReadierWorker(ctx, bucketReadierFunc)

	// Remove old test buckets (if desired)
	removeOldBuckets, _ := strconv.ParseBool(os.Getenv(testEnvRecreate))
	if removeOldBuckets {
		err := tbp.removeOldTestBuckets()
		if err != nil {
			log.Fatalf("Couldn't remove old test buckets: %v", err)
		}
	}

	// Make sure the test buckets are created and put into the readier worker queue
	start := time.Now()
	if err := tbp.createTestBuckets(numBuckets, bucketInitFunc); err != nil {
		log.Fatalf("Couldn't create test buckets: %v", err)
	}
	atomic.AddInt32(&tbp.stats.TotalBucketInitCount, int32(numBuckets))
	atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(start).Nanoseconds())

	return &tbp
}

func (tbp *GocbTestBucketPool) Logf(ctx context.Context, format string, args ...interface{}) {
	if tbp != nil && !tbp.verbose.IsTrue() {
		return
	}

	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	if colorEnabled() {
		// Green
		format = "\033[0;32m" + format + "\033[0m"
	}

	_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
}

// GetTestBucket returns a bucket to be used during a test.
// The returned teardownFn must be called once the test is done,
// which closes the bucket, readies it for a new test, and releases back into the pool.
func (tbp *GocbTestBucketPool) GetTestBucketAndSpec(t testing.TB) (b Bucket, s BucketSpec, teardownFn func()) {

	ctx := testCtx(t)

	// Return a new Walrus bucket when tbp has not been initialized
	if !tbp.integrationMode {
		if !UnitTestUrlIsWalrus() {
			tbp.Logf(ctx, "nil TestBucketPool, but not using a Walrus test URL")
			os.Exit(1)
		}

		walrusBucket := walrus.NewBucket(testBucketNamePrefix + "walrus_" + GenerateRandomID())

		// Wrap Walrus buckets with a leaky bucket to support vbucket IDs on feed.
		b = &LeakyBucket{bucket: walrusBucket, config: LeakyBucketConfig{TapFeedVbuckets: true}}

		ctx := bucketCtx(ctx, b)
		tbp.Logf(ctx, "Creating new walrus test bucket")

		initFuncStart := time.Now()
		err := tbp.BucketInitFunc(ctx, b, tbp)
		if err != nil {
			panic(err)
		}
		atomic.AddInt32(&tbp.stats.TotalBucketInitCount, 1)
		atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(initFuncStart).Nanoseconds())

		atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
		openedStart := time.Now()
		return b, getBucketSpec(bucketName(b.GetName())), func() {
			atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
			atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(openedStart).Nanoseconds())
			tbp.Logf(ctx, "Teardown called - Closing walrus test bucket")
			b.Close()
		}
	}

	if atomic.LoadUint32(&tbp.preservedBucketCount) >= uint32(cap(tbp.readyBucketPool)) {
		tbp.Logf(ctx,
			"No more buckets available for testing. All pooled buckets have been preserved by failing tests.")
		t.Skipf("No more buckets available for testing. All pooled buckets have been preserved for failing tests.")
	}

	tbp.Logf(ctx, "Attempting to get test bucket from pool")
	waitingBucketStart := time.Now()
	gocbBucket := <-tbp.readyBucketPool
	atomic.AddInt64(&tbp.stats.TotalWaitingForReadyBucketNano, time.Since(waitingBucketStart).Nanoseconds())
	ctx = bucketCtx(ctx, gocbBucket)
	tbp.Logf(ctx, "Got test bucket from pool")

	atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
	bucketOpenStart := time.Now()
	return gocbBucket, getBucketSpec(bucketName(gocbBucket.GetName())), func() {
		atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
		atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(bucketOpenStart).Nanoseconds())
		tbp.Logf(ctx, "Teardown called - closing bucket")
		gocbBucket.Close()

		if tbp.preserveBuckets && t.Failed() {
			tbp.Logf(ctx, "Test using bucket failed. Preserving bucket for later inspection")
			atomic.AddUint32(&tbp.preservedBucketCount, 1)
			return
		}

		tbp.Logf(ctx, "Teardown called - Pushing into bucketReadier queue")
		tbp.addBucketToReadierQueue(ctx, bucketName(gocbBucket.GetName()))
	}
}

func (tbp *GocbTestBucketPool) addBucketToReadierQueue(ctx context.Context, name bucketName) {
	tbp.bucketReadierWaitGroup.Add(1)
	tbp.Logf(ctx, "Putting bucket onto bucketReadierQueue")
	tbp.bucketReadierQueue <- name
}

// bucketCtx extends the parent context with a bucket name.
func bucketCtx(parent context.Context, b Bucket) context.Context {
	return bucketNameCtx(parent, b.GetName())
}

// bucketNameCtx extends the parent context with a bucket name.
func bucketNameCtx(parent context.Context, bucketName string) context.Context {
	parentLogCtx, _ := parent.Value(LogContextKey{}).(LogContext)
	newCtx := LogContext{
		TestName:       parentLogCtx.TestName,
		TestBucketName: bucketName,
	}
	return context.WithValue(parent, LogContextKey{}, newCtx)
}

// testCtx creates a log context for the given test.
func testCtx(t testing.TB) context.Context {
	return context.WithValue(context.Background(), LogContextKey{}, LogContext{TestName: t.Name()})
}

// Close cleans up any buckets, and closes the pool.
func (tbp *GocbTestBucketPool) Close() {
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
		if err := tbp.cluster.Close(); err != nil {
			tbp.Logf(context.Background(), "Couldn't close cluster connection: %v", err)
		}
	}

	tbp.printStats()
}

func (tbp *GocbTestBucketPool) printStats() {
	origVerbose := tbp.verbose.IsTrue()
	tbp.verbose.Set(true)

	ctx := context.Background()

	totalBucketInitTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalBucketInitDurationNano))
	totalBucketInitCount := time.Duration(atomic.LoadInt32(&tbp.stats.TotalBucketInitCount))

	totalBucketReadierTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalBucketReadierDurationNano))
	totalBucketReadierCount := time.Duration(atomic.LoadInt32(&tbp.stats.TotalBucketReadierCount))

	totalBucketWaitTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalWaitingForReadyBucketNano))
	numBucketsOpened := time.Duration(atomic.LoadInt32(&tbp.stats.NumBucketsOpened))

	totalBucketUseTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalInuseBucketNano))

	tbp.Logf(ctx, "==========================")
	tbp.Logf(ctx, "= Test Bucket Pool Stats =")
	tbp.Logf(ctx, "==========================")
	if totalBucketInitCount > 0 {
		tbp.Logf(ctx, "Total bucket init time: %s for %d buckets (avg: %s)", totalBucketInitTime, totalBucketInitCount, totalBucketInitTime/totalBucketInitCount)
	} else {
		tbp.Logf(ctx, "Total bucket init time: %s for %d buckets", totalBucketInitTime, totalBucketInitCount)
	}
	if totalBucketReadierCount > 0 {
		tbp.Logf(ctx, "Total bucket readier time: %s for %d buckets (avg: %s)", totalBucketReadierTime, totalBucketReadierCount, totalBucketReadierTime/totalBucketReadierCount)
	} else {
		tbp.Logf(ctx, "Total bucket readier time: %s for %d buckets", totalBucketReadierTime, totalBucketReadierCount)
	}
	tbp.Logf(ctx, "Total buckets opened/closed: %d/%d", numBucketsOpened, atomic.LoadInt32(&tbp.stats.NumBucketsClosed))
	if numBucketsOpened > 0 {
		tbp.Logf(ctx, "Total time waiting for ready bucket: %s over %d buckets (avg: %s)", totalBucketWaitTime, numBucketsOpened, totalBucketWaitTime/numBucketsOpened)
		tbp.Logf(ctx, "Total time tests using buckets: %s (avg: %s)", totalBucketUseTime, totalBucketUseTime/numBucketsOpened)
	} else {
		tbp.Logf(ctx, "Total time waiting for ready bucket: %s over %d buckets", totalBucketWaitTime, numBucketsOpened)
		tbp.Logf(ctx, "Total time tests using buckets: %s", totalBucketUseTime)
	}
	tbp.Logf(ctx, "==========================")

	tbp.verbose.Set(origVerbose)
}

// removes any integration test buckets
func (tbp *GocbTestBucketPool) removeOldTestBuckets() error {
	buckets, err := tbp.clusterMgr.GetBuckets()
	if err != nil {
		return errors.Wrap(err, "couldn't retrieve buckets from cluster manager")
	}

	wg := sync.WaitGroup{}

	for _, b := range buckets {
		if strings.HasPrefix(b.Name, testBucketNamePrefix) {
			ctx := bucketNameCtx(context.Background(), b.Name)
			tbp.Logf(ctx, "Removing old test bucket")
			wg.Add(1)

			// Run the RemoveBucket requests concurrently, as it takes a while per bucket.
			go func(b *gocb.BucketSettings) {
				err := tbp.clusterMgr.RemoveBucket(b.Name)
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

// creates a new set of integration test buckets and pushes them into the readier queue.
func (tbp *GocbTestBucketPool) createTestBuckets(numBuckets int, bucketInitFunc BucketInitFunc) error {

	wg := sync.WaitGroup{}

	existingBuckets, err := tbp.clusterMgr.GetBuckets()
	if err != nil {
		return err
	}

	openBuckets := make([]*CouchbaseBucketGoCB, numBuckets)

	// create required buckets
	for i := 0; i < numBuckets; i++ {
		testBucketName := testBucketNamePrefix + strconv.Itoa(i)
		ctx := bucketNameCtx(context.Background(), testBucketName)

		var bucketExists bool
		for _, b := range existingBuckets {
			if testBucketName == b.Name {
				tbp.Logf(ctx, "Skipping InsertBucket... Bucket already exists")
				bucketExists = true
			}
		}

		wg.Add(1)

		// Bucket creation takes a few seconds for each bucket,
		// so create and wait for readiness concurrently.
		go func(i int, bucketExists bool) {
			if !bucketExists {
				tbp.Logf(ctx, "Creating new test bucket")
				err := tbp.clusterMgr.InsertBucket(&gocb.BucketSettings{
					Name:          testBucketName,
					Quota:         testBucketQuotaMB,
					Type:          gocb.Couchbase,
					FlushEnabled:  true,
					IndexReplicas: false,
					Replicas:      0,
				})
				if err != nil {
					tbp.Logf(ctx, "Couldn't create test bucket: %v", err)
					os.Exit(1)
				}
			}

			b, err := tbp.openTestBucket(bucketName(testBucketName), CreateSleeperFunc(5*numBuckets, 1000))
			if err != nil {
				tbp.Logf(ctx, "Timed out trying to open new bucket: %v", err)
				os.Exit(1)
			}
			openBuckets[i] = b

			wg.Done()
		}(i, bucketExists)
	}

	wg.Wait()

	// All the buckets are ready, so now we can perform some synchronous setup (e.g. Creating GSI indexes)
	for i := 0; i < numBuckets; i++ {
		testBucketName := testBucketNamePrefix + strconv.Itoa(i)
		ctx := bucketNameCtx(context.Background(), testBucketName)

		tbp.Logf(ctx, "running bucketInitFunc")
		b := openBuckets[i]

		if err, _ := RetryLoop(b.GetName()+"bucketInitRetry", func() (bool, error, interface{}) {
			tbp.Logf(ctx, "Running bucket through init function")
			err = bucketInitFunc(ctx, b, tbp)
			if err != nil {
				tbp.Logf(ctx, "Couldn't init bucket, got error: %v - Retrying", err)
				return true, err, nil
			}
			return false, nil, nil
		}, CreateSleeperFunc(5, 1000)); err != nil {
			tbp.Logf(ctx, "Couldn't init bucket, got error: %v - Aborting", err)
			os.Exit(1)
		}

		b.Close()
		tbp.addBucketToReadierQueue(ctx, bucketName(testBucketName))
	}

	return nil
}

// bucketReadierWorker reads a channel of "dirty" buckets (bucketReadierQueue), does something to get them ready, and then puts them back into the pool.
// The mechanism for getting the bucket ready can vary by package being tested (for instance, a package not requiring views or GSI can use the BucketEmptierFunc function)
// A package requiring views or GSI, will need to pass in the db.ViewsAndGSIBucketReadier function.
func (tbp *GocbTestBucketPool) bucketReadierWorker(ctx context.Context, bucketReadierFunc GocbBucketReadierFunc) {
	tbp.Logf(context.Background(), "Starting bucketReadier")

loop:
	for {
		select {
		case <-ctx.Done():
			tbp.Logf(context.Background(), "bucketReadier got ctx cancelled")
			break loop

		case testBucketName := <-tbp.bucketReadierQueue:
			atomic.AddInt32(&tbp.stats.TotalBucketReadierCount, 1)
			ctx := bucketNameCtx(ctx, string(testBucketName))
			tbp.Logf(ctx, "bucketReadier got bucket")

			go func(testBucketName bucketName) {
				// We might not actually be "done" with the bucket if something fails,
				// but we need to release the waitgroup so tbp.Close() doesn't block forever.
				defer tbp.bucketReadierWaitGroup.Done()

				start := time.Now()
				b, err := tbp.openTestBucket(testBucketName, CreateSleeperFunc(5, 1000))
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
				}, CreateSleeperFunc(5, 1000))
				if err != nil {
					tbp.Logf(ctx, "Couldn't ready bucket, got error: %v - Aborting readier for bucket", err)
					return
				}

				tbp.Logf(ctx, "Bucket ready, putting back into ready pool")
				tbp.readyBucketPool <- b
				atomic.AddInt64(&tbp.stats.TotalBucketReadierDurationNano, time.Since(start).Nanoseconds())
			}(testBucketName)
		}
	}

	tbp.Logf(context.Background(), "Stopped bucketReadier")
}

func (tbp *GocbTestBucketPool) openTestBucket(testBucketName bucketName, sleeper RetrySleeper) (*CouchbaseBucketGoCB, error) {

	ctx := bucketNameCtx(context.Background(), string(testBucketName))

	bucketSpec := tbp.defaultBucketSpec
	bucketSpec.BucketName = string(testBucketName)

	waitForNewBucketWorker := func() (shouldRetry bool, err error, value interface{}) {
		gocbBucket, err := GetCouchbaseBucketGoCBFromAuthenticatedCluster(tbp.cluster, bucketSpec, "")
		if err != nil {
			tbp.Logf(ctx, "Retrying OpenBucket")
			return true, err, nil
		}
		return false, nil, gocbBucket
	}

	tbp.Logf(ctx, "Opening bucket")
	err, val := RetryLoop("waitForNewBucket", waitForNewBucketWorker,
		// The more buckets we're creating simultaneously on the cluster,
		// the longer this seems to take, so scale the wait time.
		sleeper)

	gocbBucket := val.(*CouchbaseBucketGoCB)

	return gocbBucket, err
}

func getBucketSpec(testBucketName bucketName) BucketSpec {
	bucketSpec := defaultBucketSpec
	bucketSpec.BucketName = string(testBucketName)
	return bucketSpec
}

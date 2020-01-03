package db

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// Removes any existing views and installs a new set into the given bucket.
func viewBucketReadier(ctx context.Context, b base.Bucket, tbp *base.GocbTestBucketPool) error {
	var ddocs map[string]interface{}
	err := b.GetDDocs(&ddocs)
	if err != nil {
		return err
	}

	for ddocName, _ := range ddocs {
		tbp.Logf(ctx, "removing existing view: %s", ddocName)
		if err := b.DeleteDDoc(ddocName); err != nil {
			return err
		}
	}

	tbp.Logf(ctx, "initializing bucket views")
	err = InitializeViews(b)
	if err != nil {
		return err
	}

	tbp.Logf(ctx, "bucket views initialized")
	return nil
}

// ViewsAndGSIBucketReadier empties the bucket, initializes Views, and waits until GSI indexes are empty. It is run asynchronously as soon as a test is finished with a bucket.
var ViewsAndGSIBucketReadier base.GocbBucketReadierFunc = func(ctx context.Context, b *base.CouchbaseBucketGoCB, tbp *base.GocbTestBucketPool) error {

	err := base.BucketEmptierFunc(ctx, b, tbp)
	if err != nil {
		return err
	}

	err = viewBucketReadier(ctx, b, tbp)
	if err != nil {
		return err
	}

	tbp.Logf(ctx, "waiting for empty bucket indexes")
	// we can't init indexes concurrently, so we'll just wait for them to be empty after emptying instead of recreating.
	err = WaitForIndexEmpty(b, base.TestUseXattrs())
	if err != nil {
		tbp.Logf(ctx, "WaitForIndexEmpty returned an error: %v", err)
		return err
	}
	tbp.Logf(ctx, "bucket indexes empty")

	return nil
}

// ViewsAndGSIBucketInit is run synchronously only once per-bucket to do any initial setup. For non-integration Walrus buckets, this is run for each new Walrus bucket.
var ViewsAndGSIBucketInit base.BucketInitFunc = func(ctx context.Context, b base.Bucket, tbp *base.GocbTestBucketPool) error {
	gocbBucket, ok := base.AsGoCBBucket(b)
	if !ok {
		// Check we're not running with an invalid combination of backing store and xattrs.
		if base.TestUseXattrs() {
			return fmt.Errorf("xattrs not supported when using Walrus buckets")
		}

		tbp.Logf(ctx, "bucket not a gocb bucket... skipping GSI setup")
		return viewBucketReadier(ctx, b, tbp)
	}

	if empty, err := isIndexEmpty(gocbBucket, base.TestUseXattrs()); empty && err == nil {
		tbp.Logf(ctx, "indexes already created, and already empty - skipping")
		return nil
	} else {
		tbp.Logf(ctx, "indexes not empty (or doesn't exist) - %v %v", empty, err)
	}

	tbp.Logf(ctx, "dropping existing bucket indexes")
	if err := base.DropAllBucketIndexes(gocbBucket); err != nil {
		tbp.Logf(ctx, "Failed to drop bucket indexes: %v", err)
		return err
	}

	tbp.Logf(ctx, "creating SG bucket indexes")
	if err := InitializeIndexes(gocbBucket, base.TestUseXattrs(), 0, true); err != nil {
		return err
	}

	return nil
}

func isIndexEmpty(bucket *base.CouchbaseBucketGoCB, useXattrs bool) (bool, error) {
	// Create the star channel query
	statement := fmt.Sprintf("%s LIMIT 1", QueryStarChannel.statement) // append LIMIT 1 since we only care if there are any results or not
	starChannelQueryStatement := replaceActiveOnlyFilter(statement, false)
	starChannelQueryStatement = replaceSyncTokensQuery(starChannelQueryStatement, useXattrs)
	starChannelQueryStatement = replaceIndexTokensQuery(starChannelQueryStatement, sgIndexes[IndexAllDocs], useXattrs)
	params := map[string]interface{}{}
	params[QueryParamStartSeq] = 0
	params[QueryParamEndSeq] = math.MaxInt64

	// Execute the query
	results, err := bucket.Query(starChannelQueryStatement, params, gocb.RequestPlus, true)

	// If there was an error, then retry.  Assume it's an "index rollback" error which happens as
	// the index processes the bucket flush operation
	if err != nil {
		return false, err
	}

	// If it's empty, we're done
	var queryRow AllDocsIndexQueryRow
	found := results.Next(&queryRow)
	resultsCloseErr := results.Close()
	if resultsCloseErr != nil {
		return false, err
	}

	return !found, nil
}

// Workaround SG #3570 by doing a polling loop until the star channel query returns 0 results.
// Uses the star channel index as a proxy to indicate that _all_ indexes are empty (which might not be true)
func WaitForIndexEmpty(bucket *base.CouchbaseBucketGoCB, useXattrs bool) error {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		empty, err := isIndexEmpty(bucket, useXattrs)
		if err != nil {
			return true, err, nil
		}
		return !empty, nil, empty
	}

	// Kick off the retry loop
	err, _ := base.RetryLoop(
		"Wait for index to be empty",
		retryWorker,
		base.CreateMaxDoublingSleeperFunc(60, 500, 5000),
	)
	return err

}

// Count how many rows are in gocb.QueryResults
func ResultsEmpty(results gocb.QueryResults) (resultsEmpty bool) {

	var queryRow AllDocsIndexQueryRow
	found := results.Next(&queryRow)
	return !found

}

func (db *DatabaseContext) CacheCompactActive() bool {
	channelCache := db.changeCache.getChannelCache()
	compactingCache, ok := channelCache.(*channelCacheImpl)
	if !ok {
		return false
	}
	return compactingCache.isCompactActive()
}

func (db *DatabaseContext) WaitForCaughtUp(targetCount int64) error {
	for i := 0; i < 100; i++ {
		caughtUpCount := base.ExpvarVar2Int(db.DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp))
		if caughtUpCount >= targetCount {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("WaitForCaughtUp didn't catch up")
}

type StatWaiter struct {
	initCount   int64       // Document cached count when NewStatWaiter is called
	targetCount int64       // Target count used when Wait is called
	stat        *expvar.Int // Expvar to wait on
	tb          testing.TB  // Raises tb.Fatalf on wait timeout
}

func (db *DatabaseContext) NewStatWaiter(stat *expvar.Int, tb testing.TB) *StatWaiter {
	return &StatWaiter{
		initCount:   stat.Value(),
		targetCount: stat.Value(),
		stat:        stat,
		tb:          tb,
	}
}

func (db *DatabaseContext) NewDCPCachingCountWaiter(tb testing.TB) *StatWaiter {
	stat, ok := db.DbStats.StatsDatabase().Get(base.StatKeyDcpCachingCount).(*expvar.Int)
	if !ok {
		tb.Fatalf("Unable to retrieve StatKeyDcpCachingCount during StatWaiter initialization ")
	}
	return db.NewStatWaiter(stat, tb)
}

func (db *DatabaseContext) NewPullReplicationCaughtUpWaiter(tb testing.TB) *StatWaiter {
	stat, ok := db.DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp).(*expvar.Int)
	if !ok {
		tb.Fatalf("Unable to retrieve StatKeyPullReplicationsCaughtUp during StatWaiter initialization ")
	}
	return db.NewStatWaiter(stat, tb)
}

func (db *DatabaseContext) NewCacheRevsActiveWaiter(tb testing.TB) *StatWaiter {
	stat, ok := db.DbStats.StatsCache().Get(base.StatKeyChannelCacheRevsActive).(*expvar.Int)
	if !ok {
		tb.Fatalf("Unable to retrieve StatKeyChannelCacheRevsActive during StatWaiter initialization ")
	}
	return db.NewStatWaiter(stat, tb)
}

func (sw *StatWaiter) Add(count int) {
	sw.targetCount += int64(count)
}

func (sw *StatWaiter) AddAndWait(count int) {
	sw.targetCount += int64(count)
	sw.Wait()
}

// Wait uses backoff retry for up to ~27s
func (sw *StatWaiter) Wait() {
	actualCount := sw.stat.Value()
	if actualCount >= sw.targetCount {
		return
	}

	waitTime := 1 * time.Millisecond
	for i := 0; i < 13; i++ {
		waitTime = waitTime * 2
		time.Sleep(waitTime)
		actualCount = sw.stat.Value()
		if actualCount >= sw.targetCount {
			return
		}
	}

	sw.tb.Fatalf("StatWaiter.Wait timed out waiting for stat to reach %d (actual: %d)", sw.targetCount, actualCount)
}

func AssertEqualBodies(t *testing.T, expected, actual Body) {
	expectedCanonical, err := base.JSONMarshalCanonical(expected)
	assert.NoError(t, err)
	actualCanonical, err := base.JSONMarshalCanonical(actual)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedCanonical), string(actualCanonical))
}

func WaitForUserWaiterChange(userWaiter *ChangeWaiter) bool {
	var isChanged bool
	for i := 0; i < 100; i++ {
		isChanged = userWaiter.RefreshUserCount()
		if isChanged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return isChanged
}

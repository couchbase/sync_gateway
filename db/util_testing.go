package db

import (
	"errors"
	"expvar"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
)

// Workaround SG #3570 by doing a polling loop until the star channel query returns 0 results.
// Uses the star channel index as a proxy to indicate that _all_ indexes are empty (which might not be true)
func WaitForIndexEmpty(bucket *base.CouchbaseBucketGoCB, useXattrs bool) error {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {

		var results gocb.QueryResults

		// Create the star channel query
		statement := fmt.Sprintf("%s LIMIT 1", QueryStarChannel.statement) // append LIMIT 1 since we only care if there are any results or not
		starChannelQueryStatement := replaceSyncTokensQuery(statement, useXattrs)
		params := map[string]interface{}{}
		params[QueryParamStartSeq] = 0
		params[QueryParamEndSeq] = math.MaxInt64

		// Execute the query
		results, err = bucket.Query(starChannelQueryStatement, params, gocb.RequestPlus, true)

		// If there was an error, then retry.  Assume it's an "index rollback" error which happens as
		// the index processes the bucket flush operation
		if err != nil {
			base.Infof(base.KeyAll, "Error querying star channel: %v.  Assuming it's a temp error, will retry", err)
			return true, err, nil
		}

		// If it's empty, we're done
		var queryRow AllDocsIndexQueryRow
		found := results.Next(&queryRow)
		resultsCloseErr := results.Close()
		if resultsCloseErr != nil {
			return false, resultsCloseErr, nil
		}
		if !found {
			base.Infof(base.KeyAll, "WaitForIndexEmpty found 0 results.  GSI index appears to be empty.")
			return false, nil, nil
		}

		// Otherwise, retry
		base.Infof(base.KeyAll, "WaitForIndexEmpty found non-zero results.  Retrying until the GSI index is empty.")
		return true, nil, nil

	}

	// Kick off the retry loop
	err, _ := base.RetryLoop(
		"Wait for index to be empty",
		retryWorker,
		base.CreateMaxDoublingSleeperFunc(30, 100, 2000),
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

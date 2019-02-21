package db

import (
	"fmt"
	"math"
	"time"

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

// FOR TESTS ONLY: Blocks until the given sequence has been received.
func (c *changeCache) waitForSequenceID(sequence SequenceID, maxWaitTime time.Duration) {
	c.waitForSequence(sequence.Seq, maxWaitTime)
}

func (c *changeCache) waitForSequence(sequence uint64, maxWaitTime time.Duration) {

	startTime := time.Now()

	for {

		if time.Since(startTime) >= maxWaitTime {
			panic(fmt.Sprintf("changeCache: Sequence %d did not show up after waiting %v", sequence, time.Since(startTime)))
		}

		if c.getNextSequence() >= sequence+1 {
			base.Infof(base.KeyAll, "waitForSequence(%d) took %v", sequence, time.Since(startTime))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// FOR TESTS ONLY: Blocks until the given sequence has been received.
func (c *changeCache) waitForSequenceWithMissing(sequence uint64, maxWaitTime time.Duration) {

	startTime := time.Now()

	for {

		if time.Since(startTime) >= maxWaitTime {
			panic(fmt.Sprintf("changeCache: Sequence %d did not show up after waiting %v", sequence, time.Since(startTime)))
		}

		if c.getNextSequence() >= sequence+1 {
			foundInMissing := c.skippedSeqs.Contains(sequence)
			if !foundInMissing {
				base.Infof(base.KeyAll, "waitForSequence(%d) took %v", sequence, time.Since(startTime))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

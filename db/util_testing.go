package db

import (
	"math"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
)

// Workaround SG #3570 by doing a polling loop until the star channel query returns 0 results.
func WaitForIndexEmpty(bucket *base.CouchbaseBucketGoCB, bucketSpec base.BucketSpec) error {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {

		var results gocb.QueryResults

		// Create the star channel query
		starChannelQueryStatement := replaceSyncTokensQuery(QueryStarChannel.statement, bucketSpec.UseXattrs)
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

		// Count how many results there are
		count := CountQueryResults(results)

		// If it's empty, we're done
		if count == 0 {
			base.Infof(base.KeyAll, "WaitForIndexEmpty found 0 results.  GSI index appears to be empty.")
			return false, nil, nil
		}

		// Otherwise, retry
		base.Infof(base.KeyAll, "WaitForIndexEmpty found non-zero results (%d).  Retrying until the GSI index is empty.", count)
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
func CountQueryResults(results gocb.QueryResults) (count int) {

	count = 0

	var queryRow AllDocsIndexQueryRow
	var found bool

	for {
		found = results.Next(&queryRow)
		if found {
			count += 1
		} else {
			break
		}
	}

	return count

}

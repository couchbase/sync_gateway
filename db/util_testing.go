package db

import (
	"math"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	"fmt"
)

// Workaround SG #3570 by doing a polling loop until the star channel query returns 0 results.
// Uses the star channel index as a proxy to indicate that _all_ indexes are empty (which might not be true)
func WaitForIndexEmpty(bucket *base.CouchbaseBucketGoCB, useXattrs bool) error {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {

		var results gocb.QueryResults

		// Create the star channel query
		statement := fmt.Sprintf("%s LIMIT 1", QueryStarChannel.statement)  // append LIMIT 1 since we only care if there are any results or not
		starChannelQueryStatement := replaceSyncTokensQuery(statement,useXattrs)
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



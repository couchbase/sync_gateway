package db

import (
	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	"log"
	"math"
)

// Workaround SG #3570
func WaitForIndexRollback(bucket *base.CouchbaseBucketGoCB, bucketSpec base.BucketSpec) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {

		var results gocb.QueryResults

		// Run the alldocs query
		allDocsQueryStatement := replaceSyncTokensQuery(QueryStarChannel.statement, bucketSpec.UseXattrs)
		log.Printf("allDocsQueryStatement: %s", allDocsQueryStatement)

		params := map[string]interface{}{}
		params[QueryParamStartSeq] = 0
		// params[QueryParamEndSeq] = 100 // TODO: what val should be used here?
		params[QueryParamEndSeq] = math.MaxInt64

		results, err = bucket.Query(allDocsQueryStatement, params, gocb.RequestPlus, true)

		// If there was an error, then retry.  Probably an "index rollback" error which happens as
		// the index processes the bucket flush operation
		if err != nil {
			return true, err, nil
		}

		// If there are 0 results, we're done
		//var val interface{}
		//log.Printf("val: %+v", val)
		//hasNext := results.Next(&val)
		//if !hasNext {
		//	return false, nil, nil
		//}
		count := uint64(0)
		var queryRow AllDocsIndexQueryRow
		var found bool

		for {
			found = results.Next(&queryRow)
			log.Printf("queryRow: %+v, found: %v", queryRow, found)
			if found {
				count += 1
			} else {
				break
			}
		}


		if count == 0 {
			log.Printf("WaitForIndexRollback found 0 results.  GSI index appears to be empty.")
			return false, nil, nil
		}

		// Otherwise, retry
		log.Printf("WaitForIndexRollback found non-zero results (%d).  Retrying until the GSI index is empty.", count)
		return true, nil, nil

	}

	sleeper := base.CreateMaxDoublingSleeperFunc(30, 100, 1000)

	err, _ := base.RetryLoop("Wait for index rollback", worker, sleeper)
	return err

}

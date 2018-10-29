package db

import (
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
)

// Validate stats for view query
func TestQueryChannelsStatsView(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test is walrus-only (requires views)")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	_, err := db.Put("queryTestDoc1", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put queryDoc1")
	_, err = db.Put("queryTestDoc2", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put queryDoc2")
	_, err = db.Put("queryTestDoc3", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put queryDoc3")

	// Check expvar prior to test
	queryCountExpvar := fmt.Sprintf(viewQueryCountExpvarFormat, DesignDocSyncGateway(), ViewChannels)
	errorCountExpvar := fmt.Sprintf(viewQueryErrorCountExpvarFormat, DesignDocSyncGateway(), ViewChannels)

	channelQueryCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	// Issue channels query
	results, queryErr := db.QueryChannels("ABC", 0, 10, 100)
	assertNoError(t, queryErr, "Query error")

	goassert.Equals(t, countQueryResults(results), 3)

	closeErr := results.Close()
	assertNoError(t, closeErr, "Close error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	goassert.Equals(t, channelQueryCountBefore+1, channelQueryCountAfter)
	goassert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

}

// Validate stats for n1ql query
func TestQueryChannelsStatsN1ql(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	_, err := db.Put("queryTestDoc1", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put queryDoc1")
	_, err = db.Put("queryTestDoc2", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put queryDoc2")
	_, err = db.Put("queryTestDoc3", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put queryDoc3")

	// Check expvar prior to test
	queryCountExpvar := fmt.Sprintf(n1qlQueryCountExpvarFormat, QueryTypeChannels)
	errorCountExpvar := fmt.Sprintf(n1qlQueryErrorCountExpvarFormat, QueryTypeChannels)

	channelQueryCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	// Issue channels query
	results, queryErr := db.QueryChannels("ABC", 0, 10, 100)
	assertNoError(t, queryErr, "Query error")

	goassert.Equals(t, countQueryResults(results), 3)

	closeErr := results.Close()
	assertNoError(t, closeErr, "Close error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	goassert.Equals(t, channelQueryCountBefore+1, channelQueryCountAfter)
	goassert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

}

func countQueryResults(results sgbucket.QueryResultIterator) int {

	count := 0
	var row map[string]interface{}
	for results.Next(&row) {
		count++
	}
	return count
}

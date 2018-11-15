package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err, "Put queryDoc1")
	_, err = db.Put("queryTestDoc2", Body{"channels": []string{"ABC"}})
	assert.NoError(t, err, "Put queryDoc2")
	_, err = db.Put("queryTestDoc3", Body{"channels": []string{"ABC"}})
	assert.NoError(t, err, "Put queryDoc3")

	// Check expvar prior to test
	queryCountExpvar := fmt.Sprintf(viewQueryCountExpvarFormat, DesignDocSyncGateway(), ViewChannels)
	errorCountExpvar := fmt.Sprintf(viewQueryErrorCountExpvarFormat, DesignDocSyncGateway(), ViewChannels)

	channelQueryCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	// Issue channels query
	results, queryErr := db.QueryChannels("ABC", 0, 10, 100)
	assert.NoError(t, queryErr, "Query error")

	goassert.Equals(t, countQueryResults(results), 3)

	closeErr := results.Close()
	assert.NoError(t, closeErr, "Close error")

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
	assert.NoError(t, err, "Put queryDoc1")
	_, err = db.Put("queryTestDoc2", Body{"channels": []string{"ABC"}})
	assert.NoError(t, err, "Put queryDoc2")
	_, err = db.Put("queryTestDoc3", Body{"channels": []string{"ABC"}})
	assert.NoError(t, err, "Put queryDoc3")

	// Check expvar prior to test
	queryCountExpvar := fmt.Sprintf(n1qlQueryCountExpvarFormat, QueryTypeChannels)
	errorCountExpvar := fmt.Sprintf(n1qlQueryErrorCountExpvarFormat, QueryTypeChannels)

	channelQueryCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	// Issue channels query
	results, queryErr := db.QueryChannels("ABC", 0, 10, 100)
	assert.NoError(t, queryErr, "Query error")

	goassert.Equals(t, countQueryResults(results), 3)

	closeErr := results.Close()
	assert.NoError(t, closeErr, "Close error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	goassert.Equals(t, channelQueryCountBefore+1, channelQueryCountAfter)
	goassert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

}

// Validate that channels queries (channels, starChannel) are covering
func TestCoveringQueries(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	gocbBucket, ok := base.AsGoCBBucket(testBucket)
	if !ok {
		t.Errorf("Unable to get gocbBucket for testBucket")
	}

	// channels
	channelsStatement, params := db.buildChannelsQuery("ABC", 0, 10, 100)
	plan, explainErr := gocbBucket.ExplainQuery(channelsStatement, params)
	assert.NoError(t, explainErr, "Error generating explain for channels query")
	covered := isCovered(plan)
	assert.True(t, covered, "Channel query isn't covered by index")

	// star channel
	channelStarStatement, params := db.buildChannelsQuery("*", 0, 10, 100)
	plan, explainErr = gocbBucket.ExplainQuery(channelStarStatement, params)
	assert.NoError(t, explainErr, "Error generating explain for star channel query")
	covered = isCovered(plan)
	assert.True(t, covered, "Star channel query isn't covered by index")

	// Access and roleAccess currently aren't covering, because of the need to target the user property by name
	// in the SELECT.
	// Including here for ease-of-conversion when we get an indexing enhancement to support covered queries.
	accessStatement := db.buildAccessQuery("user1")
	plan, explainErr = gocbBucket.ExplainQuery(accessStatement, nil)
	assert.NoError(t, explainErr, "Error generating explain for access query")
	covered = isCovered(plan)
	//assert.True(t, covered, "Access query isn't covered by index")

	// roleAccess
	roleAccessStatement := db.buildRoleAccessQuery("user1")
	plan, explainErr = gocbBucket.ExplainQuery(roleAccessStatement, nil)
	assert.NoError(t, explainErr, "Error generating explain for roleAccess query")
	covered = isCovered(plan)
	//assert.True(t, !covered, "RoleAccess query isn't covered by index")

}

// Parse the plan looking for use of the fetch operation (appears as the key/value pair "#operator":"Fetch")
// If there's no fetch operator in the plan, we can assume the query is covered by the index.
// The plan returned by an EXPLAIN is a nested hierarchy with operators potentially appearing at different
// depths, so need to traverse the JSON object.
// https://docs.couchbase.com/server/6.0/n1ql/n1ql-language-reference/explain.html
func isCovered(plan map[string]interface{}) bool {
	for key, value := range plan {
		switch value := value.(type) {
		case string:
			if key == "#operator" && value == "Fetch" {
				return false
			}
		case map[string]interface{}:
			if !isCovered(value) {
				return false
			}
		case []interface{}:
			for _, arrayValue := range value {
				jsonArrayValue, ok := arrayValue.(map[string]interface{})
				if ok {
					if !isCovered(jsonArrayValue) {
						return false
					}
				}
			}
		default:
		}
	}
	return true
}

func countQueryResults(results sgbucket.QueryResultIterator) int {

	count := 0
	var row map[string]interface{}
	for results.Next(&row) {
		count++
	}
	return count
}

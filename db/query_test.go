package db

import (
	"fmt"
	"github.com/couchbase/sync_gateway/channels"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
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

	assert.Equals(t, countQueryResults(results), 3)

	closeErr := results.Close()
	assertNoError(t, closeErr, "Close error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	assert.Equals(t, channelQueryCountBefore+1, channelQueryCountAfter)
	assert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

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

	assert.Equals(t, countQueryResults(results), 3)

	closeErr := results.Close()
	assertNoError(t, closeErr, "Close error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	assert.Equals(t, channelQueryCountBefore+1, channelQueryCountAfter)
	assert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

}

// Validate query and stats for sequence view query
func TestQuerySequencesStatsView(t *testing.T) {

	db, testBucket := setupTestDBWithViewsEnabled(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Add docs without channel assignment (will only be assigned to the star channel)
	for i := 1; i <= 10; i++ {
		//_, err := db.Put(fmt.Sprintf("queryTestDoc%d", i), Body{"channels": []string{"ABC"}})
		_, err := db.Put(fmt.Sprintf("queryTestDoc%d", i), Body{"nochannels": true})
		assertNoError(t, err, "Put queryDoc")
	}

	// Check expvar prior to test
	queryCountExpvar := fmt.Sprintf(viewQueryCountExpvarFormat, DesignDocSyncGateway(), ViewChannels)
	errorCountExpvar := fmt.Sprintf(viewQueryErrorCountExpvarFormat, DesignDocSyncGateway(), ViewChannels)

	channelQueryCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	// Issue channels query
	results, queryErr := db.QuerySequences([]uint64{3, 4, 6, 8})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 4)
	closeErr := results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences([]uint64{2})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 1)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with key outside keyset range
	results, queryErr = db.QuerySequences([]uint64{25})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 0)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with empty keys
	results, queryErr = db.QuerySequences([]uint64{})
	assertTrue(t, queryErr != nil, "Expect empty sequence error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	assert.Equals(t, channelQueryCountBefore+3, channelQueryCountAfter)
	assert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

	// Add some docs in different channels, to validate query handling when non-star channel docs are present
	for i := 1; i <= 10; i++ {
		_, err := db.Put(fmt.Sprintf("queryTestDocChanneled%d", i), Body{"channels": []string{fmt.Sprintf("ABC%d", i)}})
		assertNoError(t, err, "Put queryDoc")
	}
	// Issue channels query
	results, queryErr = db.QuerySequences([]uint64{3, 4, 6, 8, 15})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 5)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences([]uint64{2})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 1)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with key outside sequence range.  Note that this isn't outside the entire view key range, as
	// [*, 25] is sorted before ["ABC1", 11]
	results, queryErr = db.QuerySequences([]uint64{25})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 0)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")
}

// Validate query and stats for sequence view query
func TestQuerySequencesStatsN1ql(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Add docs without channel assignment (will only be assigned to the star channel)
	for i := 1; i <= 10; i++ {
		//_, err := db.Put(fmt.Sprintf("queryTestDoc%d", i), Body{"channels": []string{"ABC"}})
		_, err := db.Put(fmt.Sprintf("queryTestDoc%d", i), Body{"nochannels": true})
		assertNoError(t, err, "Put queryDoc")
	}

	// Check expvar prior to test
	queryCountExpvar := fmt.Sprintf(n1qlQueryCountExpvarFormat, QueryTypeSequences)
	errorCountExpvar := fmt.Sprintf(n1qlQueryErrorCountExpvarFormat, QueryTypeSequences)

	channelQueryCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountBefore, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	// Issue channels query
	results, queryErr := db.QuerySequences([]uint64{3, 4, 6, 8})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 4)
	closeErr := results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences([]uint64{2})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 1)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with key outside keyset range
	results, queryErr = db.QuerySequences([]uint64{25})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 0)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with empty keys
	results, queryErr = db.QuerySequences([]uint64{})
	assertTrue(t, queryErr != nil, "Expect empty sequence error")

	channelQueryCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", queryCountExpvar)
	channelQueryErrorCountAfter, _ := base.GetExpvarAsInt("syncGateway_query", errorCountExpvar)

	assert.Equals(t, channelQueryCountBefore+3, channelQueryCountAfter)
	assert.Equals(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

	// Add some docs in different channels, to validate query handling when non-star channel docs are present
	for i := 1; i <= 10; i++ {
		_, err := db.Put(fmt.Sprintf("queryTestDocChanneled%d", i), Body{"channels": []string{fmt.Sprintf("ABC%d", i)}})
		assertNoError(t, err, "Put queryDoc")
	}
	// Issue channels query
	results, queryErr = db.QuerySequences([]uint64{3, 4, 6, 8, 15})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 5)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences([]uint64{2})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 1)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")

	// Issue query with key outside sequence range.  Note that this isn't outside the entire view key range, as
	// [*, 25] is sorted before ["ABC1", 11]
	results, queryErr = db.QuerySequences([]uint64{25})
	assertNoError(t, queryErr, "Query error")
	assert.Equals(t, countQueryResults(results), 0)
	closeErr = results.Close()
	assertNoError(t, closeErr, "Close error")
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
	assertNoError(t, explainErr, "Error generating explain for channels query")
	covered := isCovered(plan)
	assertTrue(t, covered, "Channel query isn't covered by index")

	// star channel
	channelStarStatement, params := db.buildChannelsQuery("*", 0, 10, 100)
	plan, explainErr = gocbBucket.ExplainQuery(channelStarStatement, params)
	assertNoError(t, explainErr, "Error generating explain for star channel query")
	covered = isCovered(plan)
	assertTrue(t, covered, "Star channel query isn't covered by index")

	// Access and roleAccess currently aren't covering, because of the need to target the user property by name
	// in the SELECT.
	// Including here for ease-of-conversion when we get an indexing enhancement to support covered queries.
	accessStatement := db.buildAccessQuery("user1")
	plan, explainErr = gocbBucket.ExplainQuery(accessStatement, nil)
	assertNoError(t, explainErr, "Error generating explain for access query")
	covered = isCovered(plan)
	//assert.True(t, covered, "Access query isn't covered by index")

	// roleAccess
	roleAccessStatement := db.buildRoleAccessQuery("user1")
	plan, explainErr = gocbBucket.ExplainQuery(roleAccessStatement, nil)
	assertNoError(t, explainErr, "Error generating explain for roleAccess query")
	covered = isCovered(plan)
	//assert.True(t, !covered, "RoleAccess query isn't covered by index")

}

func TestAllDocsQuery(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Add docs with channel assignment
	for i := 1; i <= 10; i++ {
		_, err := db.Put(fmt.Sprintf("allDocsTest%d", i), Body{"channels": []string{"ABC"}})
		assert.NoError(t, err, "Put allDocsTest doc")
	}

	// Standard query
	startKey := "a"
	endKey := ""
	results, queryErr := db.QueryAllDocs(startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	var row map[string]interface{}
	rowCount := 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 10, rowCount)

	// Attempt to invalidate standard query
	startKey = "a' AND 1=0\x00"
	endKey = ""
	results, queryErr = db.QueryAllDocs(startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 10, rowCount)

	// Attempt to invalidate statement to add row to resultset
	startKey = `a' UNION ALL SELECT TOSTRING(BASE64_DECODE("SW52YWxpZERhdGE=")) as id;` + "\x00"
	endKey = ""
	results, queryErr = db.QueryAllDocs(startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		assert.NotEqual(t, row["id"], "InvalidData")
		rowCount++
	}
	assert.Equal(t, 10, rowCount)

	// Attempt to create syntax error
	startKey = `a'1`
	endKey = ""
	results, queryErr = db.QueryAllDocs(startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 10, rowCount)

}

func TestAccessQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
	access(doc.accessUser, doc.accessChannel)
}`)
	// Add docs with access grants assignment
	for i := 1; i <= 5; i++ {
		_, err := db.Put(fmt.Sprintf("accessTest%d", i), Body{"accessUser": "user1", "accessChannel": fmt.Sprintf("channel%d", i)})
		assert.NoError(t, err, "Put accessTest doc")
	}

	// Standard query
	username := "user1"
	results, queryErr := db.QueryAccess(username)
	assert.NoError(t, queryErr, "Query error")
	var row map[string]interface{}
	rowCount := 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 5, rowCount)

	// Attempt to introduce syntax error.  Should return zero rows for user `user1'`, and not return error
	username = "user1'"
	results, queryErr = db.QueryAccess(username)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 0, rowCount)

	// Attempt to introduce syntax error.  Should return zero rows for user `user1`AND`, and not return error.
	// Validates select clause protection
	username = "user1`AND"
	results, queryErr = db.QueryAccess(username)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 0, rowCount)
}

func TestRoleAccessQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
	role(doc.accessUser, "role:" + doc.accessChannel)
}`)
	// Add docs with access grants assignment
	for i := 1; i <= 5; i++ {
		_, err := db.Put(fmt.Sprintf("accessTest%d", i), Body{"accessUser": "user1", "accessChannel": fmt.Sprintf("channel%d", i)})
		assert.NoError(t, err, "Put accessTest doc")
	}

	// Standard query
	username := "user1"
	results, queryErr := db.QueryRoleAccess(username)
	assert.NoError(t, queryErr, "Query error")
	var row map[string]interface{}
	rowCount := 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 5, rowCount)

	// Attempt to introduce syntax error.  Should return zero rows for user `user1'`, and not return error
	username = "user1'"
	results, queryErr = db.QueryRoleAccess(username)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 0, rowCount)

	// Attempt to introduce syntax error.  Should return zero rows for user `user1`AND`, and not return error
	// Validates select clause protection
	username = "user1`AND"
	results, queryErr = db.QueryRoleAccess(username)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 0, rowCount)
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

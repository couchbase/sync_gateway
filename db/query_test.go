/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"strconv"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Validate stats for view query
func TestQueryChannelsStatsView(t *testing.T) {

	if !base.TestsDisableGSI() {
		t.Skip("This test is view only, but GSI is enabled")
	}

	db := setupTestDB(t)
	defer db.Close()

	// docID -> Sequence
	docSeqMap := make(map[string]uint64, 3)

	_, doc, err := db.Put("queryTestDoc1", Body{"channels": []string{"ABC"}})
	require.NoError(t, err, "Put queryDoc1")
	docSeqMap["queryTestDoc1"] = doc.Sequence
	_, doc, err = db.Put("queryTestDoc2", Body{"channels": []string{"ABC"}})
	require.NoError(t, err, "Put queryDoc2")
	docSeqMap["queryTestDoc2"] = doc.Sequence
	_, doc, err = db.Put("queryTestDoc3", Body{"channels": []string{"ABC"}})
	require.NoError(t, err, "Put queryDoc3")
	docSeqMap["queryTestDoc3"] = doc.Sequence

	// Check expvar prior to test
	queryExpvar := fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewChannels)

	channelQueryCountBefore := db.DbStats.Query(queryExpvar).QueryCount.Value()
	channelQueryTimeBefore := db.DbStats.Query(queryExpvar).QueryTime.Value()
	channelQueryErrorCountBefore := db.DbStats.Query(queryExpvar).QueryErrorCount.Value()

	// Issue channels query
	results, queryErr := db.QueryChannels(base.TestCtx(t), "ABC", docSeqMap["queryTestDoc1"], docSeqMap["queryTestDoc3"], 100, false)
	assert.NoError(t, queryErr, "Query error")

	assert.Equal(t, 3, countQueryResults(results))

	closeErr := results.Close()
	assert.NoError(t, closeErr, "Close error")

	channelQueryCountAfter := db.DbStats.Query(queryExpvar).QueryCount.Value()
	channelQueryTimeAfter := db.DbStats.Query(queryExpvar).QueryTime.Value()
	channelQueryErrorCountAfter := db.DbStats.Query(queryExpvar).QueryErrorCount.Value()

	assert.Equal(t, channelQueryCountBefore+1, channelQueryCountAfter)
	assert.True(t, channelQueryTimeAfter > channelQueryTimeBefore, "Channel query time stat didn't change")
	assert.Equal(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

}

// Validate stats for n1ql query
func TestQueryChannelsStatsN1ql(t *testing.T) {

	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server only")
	}

	db := setupTestDB(t)
	defer db.Close()

	// docID -> Sequence
	docSeqMap := make(map[string]uint64, 3)

	_, doc, err := db.Put("queryTestDoc1", Body{"channels": []string{"ABC"}})
	require.NoError(t, err, "Put queryDoc1")
	docSeqMap["queryTestDoc1"] = doc.Sequence
	_, doc, err = db.Put("queryTestDoc2", Body{"channels": []string{"ABC"}})
	require.NoError(t, err, "Put queryDoc2")
	docSeqMap["queryTestDoc2"] = doc.Sequence
	_, doc, err = db.Put("queryTestDoc3", Body{"channels": []string{"ABC"}})
	require.NoError(t, err, "Put queryDoc3")
	docSeqMap["queryTestDoc3"] = doc.Sequence

	// Check expvar prior to test
	channelQueryCountBefore := db.DbStats.Query(QueryTypeChannels).QueryCount.Value()
	channelQueryTimeBefore := db.DbStats.Query(QueryTypeChannels).QueryTime.Value()
	channelQueryErrorCountBefore := db.DbStats.Query(QueryTypeChannels).QueryErrorCount.Value()

	// Issue channels query
	results, queryErr := db.QueryChannels(base.TestCtx(t), "ABC", docSeqMap["queryTestDoc1"], docSeqMap["queryTestDoc3"], 100, false)
	assert.NoError(t, queryErr, "Query error")

	assert.Equal(t, 3, countQueryResults(results))

	closeErr := results.Close()
	assert.NoError(t, closeErr, "Close error")

	channelQueryCountAfter := db.DbStats.Query(QueryTypeChannels).QueryCount.Value()
	channelQueryTimeAfter := db.DbStats.Query(QueryTypeChannels).QueryTime.Value()
	channelQueryErrorCountAfter := db.DbStats.Query(QueryTypeChannels).QueryErrorCount.Value()

	assert.Equal(t, channelQueryCountBefore+1, channelQueryCountAfter)
	assert.True(t, channelQueryTimeAfter > channelQueryTimeBefore, "Channel query time stat didn't change")
	assert.Equal(t, channelQueryErrorCountBefore, channelQueryErrorCountAfter)

}

// Validate query and stats for sequence view query
func TestQuerySequencesStatsView(t *testing.T) {

	db := setupTestDBWithViewsEnabled(t)
	defer db.Close()

	// docID -> Sequence
	docSeqMap := make(map[string]uint64, 20)

	// Add docs without channel assignment (will only be assigned to the star channel)
	for i := 1; i <= 10; i++ {
		docID := fmt.Sprintf("queryTestDoc%d", i)
		_, doc, err := db.Put(docID, Body{"nochannels": true})
		assert.NoError(t, err, "Put queryDoc")
		docSeqMap[docID] = doc.Sequence
	}

	// Check expvar prior to test
	queryExpvar := fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewChannels)

	channelQueryCountBefore := db.DbStats.Query(queryExpvar).QueryCount.Value()
	channelQueryErrorCountBefore := db.DbStats.Query(queryExpvar).QueryErrorCount.Value()

	// Issue channels query
	results, queryErr := db.QuerySequences(base.TestCtx(t), []uint64{
		docSeqMap["queryTestDoc3"], docSeqMap["queryTestDoc4"],
		docSeqMap["queryTestDoc6"], docSeqMap["queryTestDoc8"],
	})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 4, countQueryResults(results))
	closeErr := results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{docSeqMap["queryTestDoc2"]})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 1, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with key outside keyset range
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{100})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 0, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with empty keys
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{})
	assert.Error(t, queryErr, "Expect empty sequence error")

	channelQueryCountAfter := db.DbStats.Query(queryExpvar).QueryCount.Value()
	channelQueryErrorCountAfter := db.DbStats.Query(queryExpvar).QueryErrorCount.Value()

	assert.Equal(t, channelQueryCountAfter, channelQueryCountBefore+3)
	assert.Equal(t, channelQueryErrorCountAfter, channelQueryErrorCountBefore)

	// Add some docs in different channels, to validate query handling when non-star channel docs are present
	for i := 1; i <= 10; i++ {
		docID := fmt.Sprintf("queryTestDocChanneled%d", i)
		_, doc, err := db.Put(docID, Body{"channels": []string{fmt.Sprintf("ABC%d", i)}})
		require.NoError(t, err, "Put queryDoc")
		docSeqMap[docID] = doc.Sequence
	}
	// Issue channels query
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{
		docSeqMap["queryTestDoc3"], docSeqMap["queryTestDoc4"],
		docSeqMap["queryTestDoc6"], docSeqMap["queryTestDoc8"],
		docSeqMap["queryTestDocChanneled5"],
	})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 5, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{docSeqMap["queryTestDoc2"]})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 1, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with key outside sequence range.  Note that this isn't outside the entire view key range, as
	// [*, 25] is sorted before ["ABC1", 11]
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{100})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 0, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")
}

// Validate query and stats for sequence view query
func TestQuerySequencesStatsN1ql(t *testing.T) {

	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	db := setupTestDB(t)
	defer db.Close()

	// docID -> Sequence
	docSeqMap := make(map[string]uint64, 20)

	// Add docs without channel assignment (will only be assigned to the star channel)
	for i := 1; i <= 10; i++ {
		docID := fmt.Sprintf("queryTestDoc%d", i)
		_, doc, err := db.Put(docID, Body{"nochannels": true})
		require.NoError(t, err, "Put queryDoc")
		docSeqMap[docID] = doc.Sequence
	}

	// Check expvar prior to test
	channelQueryCountBefore := db.DbStats.Query(QueryTypeSequences).QueryCount.Value()
	channelQueryErrorCountBefore := db.DbStats.Query(QueryTypeSequences).QueryErrorCount.Value()

	// Issue channels query
	results, queryErr := db.QuerySequences(base.TestCtx(t), []uint64{
		docSeqMap["queryTestDoc3"], docSeqMap["queryTestDoc4"],
		docSeqMap["queryTestDoc6"], docSeqMap["queryTestDoc8"],
	})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 4, countQueryResults(results))
	closeErr := results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{docSeqMap["queryTestDoc2"]})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 1, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with key outside keyset range
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{100})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 0, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with empty keys
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{})
	assert.Error(t, queryErr, "Expect empty sequence error")

	channelQueryCountAfter := db.DbStats.Query(QueryTypeSequences).QueryCount.Value()
	channelQueryErrorCountAfter := db.DbStats.Query(QueryTypeSequences).QueryErrorCount.Value()

	assert.Equal(t, channelQueryCountAfter, channelQueryCountBefore+3)
	assert.Equal(t, channelQueryErrorCountAfter, channelQueryErrorCountBefore)

	// Add some docs in different channels, to validate query handling when non-star channel docs are present
	for i := 1; i <= 10; i++ {
		docID := fmt.Sprintf("queryTestDocChanneled%d", i)
		_, doc, err := db.Put(docID, Body{"channels": []string{fmt.Sprintf("ABC%d", i)}})
		require.NoError(t, err, "Put queryDoc")
		docSeqMap[docID] = doc.Sequence
	}

	// Issue channels query
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{
		docSeqMap["queryTestDoc3"], docSeqMap["queryTestDoc4"],
		docSeqMap["queryTestDoc6"], docSeqMap["queryTestDoc8"],
		docSeqMap["queryTestDocChanneled5"],
	})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 5, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with single key
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{docSeqMap["queryTestDoc2"]})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 1, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")

	// Issue query with key outside sequence range.  Note that this isn't outside the entire view key range, as
	// [*, 25] is sorted before ["ABC1", 11]
	results, queryErr = db.QuerySequences(base.TestCtx(t), []uint64{100})
	assert.NoError(t, queryErr, "Query error")
	assert.Equal(t, 0, countQueryResults(results))
	closeErr = results.Close()
	assert.NoError(t, closeErr, "Close error")
}

// Validate that channels queries (channels, starChannel) are covering
func TestCoveringQueries(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	db := setupTestDB(t)
	defer db.Close()

	n1QLStore, ok := base.AsN1QLStore(db.Bucket)
	if !ok {
		t.Errorf("Unable to get n1QLStore for testBucket")
	}

	// channels
	channelsStatement, params := db.buildChannelsQuery("ABC", 0, 10, 100, false)
	plan, explainErr := n1QLStore.ExplainQuery(channelsStatement, params)
	assert.NoError(t, explainErr, "Error generating explain for channels query")
	covered := isCovered(plan)
	planJSON, err := base.JSONMarshal(plan)
	assert.NoError(t, err)
	assert.True(t, covered, "Channel query isn't covered by index: %s", planJSON)

	// star channel
	channelStarStatement, params := db.buildChannelsQuery("*", 0, 10, 100, false)
	plan, explainErr = n1QLStore.ExplainQuery(channelStarStatement, params)
	assert.NoError(t, explainErr, "Error generating explain for star channel query")
	covered = isCovered(plan)
	planJSON, err = base.JSONMarshal(plan)
	assert.NoError(t, err)
	assert.True(t, covered, "Star channel query isn't covered by index: %s", planJSON)

	// Access and roleAccess currently aren't covering, because of the need to target the user property by name
	// in the SELECT.
	// Including here for ease-of-conversion when we get an indexing enhancement to support covered queries.
	accessStatement := db.buildAccessQuery("user1")
	plan, explainErr = n1QLStore.ExplainQuery(accessStatement, nil)
	assert.NoError(t, explainErr, "Error generating explain for access query")
	covered = isCovered(plan)
	planJSON, err = base.JSONMarshal(plan)
	assert.NoError(t, err)
	// assert.True(t, covered, "Access query isn't covered by index: %s", planJSON)

	// roleAccess
	roleAccessStatement := db.buildRoleAccessQuery("user1")
	plan, explainErr = n1QLStore.ExplainQuery(roleAccessStatement, nil)
	assert.NoError(t, explainErr, "Error generating explain for roleAccess query")
	covered = isCovered(plan)
	planJSON, err = base.JSONMarshal(plan)
	assert.NoError(t, err)
	// assert.True(t, !covered, "RoleAccess query isn't covered by index: %s", planJSON)

}

func TestAllDocsQuery(t *testing.T) {

	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	db := setupTestDB(t)
	defer db.Close()

	// Add docs with channel assignment
	for i := 1; i <= 10; i++ {
		_, _, err := db.Put(fmt.Sprintf("allDocsTest%d", i), Body{"channels": []string{"ABC"}})
		assert.NoError(t, err, "Put allDocsTest doc")
	}

	// Standard query
	startKey := "a"
	endKey := ""
	results, queryErr := db.QueryAllDocs(base.TestCtx(t), startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	var row map[string]interface{}
	rowCount := 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 10, rowCount)
	assert.NoError(t, results.Close())

	// Attempt to invalidate standard query
	startKey = "a' AND 1=0\x00"
	endKey = ""
	results, queryErr = db.QueryAllDocs(base.TestCtx(t), startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 10, rowCount)
	assert.NoError(t, results.Close())

	// Attempt to invalidate statement to add row to resultset
	startKey = `a' UNION ALL SELECT TOSTRING(BASE64_DECODE("SW52YWxpZERhdGE=")) as id;` + "\x00"
	endKey = ""
	results, queryErr = db.QueryAllDocs(base.TestCtx(t), startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		assert.NotEqual(t, row["id"], "InvalidData")
		rowCount++
	}
	assert.Equal(t, 10, rowCount)
	assert.NoError(t, results.Close())

	// Attempt to create syntax error
	startKey = `a'1`
	endKey = ""
	results, queryErr = db.QueryAllDocs(base.TestCtx(t), startKey, endKey)
	assert.NoError(t, queryErr, "Query error")
	rowCount = 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 10, rowCount)
	assert.NoError(t, results.Close())

}

func TestAccessQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
	access(doc.accessUser, doc.accessChannel)
}`)
	// Add docs with access grants assignment
	for i := 1; i <= 5; i++ {
		_, _, err := db.Put(fmt.Sprintf("accessTest%d", i), Body{"accessUser": "user1", "accessChannel": fmt.Sprintf("channel%d", i)})
		assert.NoError(t, err, "Put accessTest doc")
	}

	// Standard query
	username := "user1"
	results, queryErr := db.QueryAccess(base.TestCtx(t), username)
	assert.NoError(t, queryErr, "Query error")
	var row map[string]interface{}
	rowCount := 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 5, rowCount)
	assert.NoError(t, results.Close())

	// Attempt to introduce syntax errors. Each of these should return zero rows and no error.
	// Validates select clause protection
	for _, username := range []string{"user1'", "user1`AND", "user1?", "user1 ! user2$"} {
		results, queryErr = db.QueryAccess(base.TestCtx(t), username)
		assert.NoError(t, queryErr, "Query error")
		rowCount = 0
		for results.Next(&row) {
			rowCount++
		}
		assert.Equal(t, 0, rowCount)
		assert.NoError(t, results.Close())
	}
}

func TestRoleAccessQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only")
	}

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
	role(doc.accessUser, "role:" + doc.accessChannel)
}`)
	// Add docs with access grants assignment
	for i := 1; i <= 5; i++ {
		_, _, err := db.Put(fmt.Sprintf("accessTest%d", i), Body{"accessUser": "user1", "accessChannel": fmt.Sprintf("channel%d", i)})
		assert.NoError(t, err, "Put accessTest doc")
	}

	// Standard query
	username := "user1"
	results, queryErr := db.QueryRoleAccess(base.TestCtx(t), username)
	assert.NoError(t, queryErr, "Query error")
	var row map[string]interface{}
	rowCount := 0
	for results.Next(&row) {
		rowCount++
	}
	assert.Equal(t, 5, rowCount)
	assert.NoError(t, results.Close())

	// Attempt to introduce syntax errors. Each of these should return zero rows and no error.
	// Validates select clause protection
	for _, username := range []string{"user1'", "user1`AND", "user1?", "user1 ! user2$"} {
		results, queryErr = db.QueryRoleAccess(base.TestCtx(t), username)
		assert.NoError(t, queryErr, "Query error")
		rowCount = 0
		for results.Next(&row) {
			rowCount++
		}
		assert.Equal(t, 0, rowCount)
		assert.NoError(t, results.Close())
	}
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

func TestQueryChannelsActiveOnlyWithLimit(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	db := setupTestDB(t)
	defer db.Close()

	docIdFlagMap := make(map[string]uint8)
	var startSeq, endSeq uint64
	body := Body{"channels": []string{"ABC"}}

	checkFlags := func(entries LogEntries) {
		for _, entry := range entries {
			assert.Equal(t, docIdFlagMap[entry.DocID], entry.Flags, "Flags mismatch for doc %v", entry.DocID)
		}
	}

	// Create 10 added documents
	for i := 1; i <= 10; i++ {
		id := "created" + strconv.Itoa(i)
		doc, revId, err := db.PutExistingRevWithBody(id, body, []string{"1-a"}, false)
		require.NoError(t, err, "Couldn't create document")
		require.Equal(t, "1-a", revId)
		docIdFlagMap[doc.ID] = uint8(0x0)
		if i == 1 {
			startSeq = doc.Sequence
		}
		endSeq = doc.Sequence
	}

	// Create 10 deleted documents
	for i := 1; i <= 10; i++ {
		id := "deleted" + strconv.Itoa(i)
		doc, revId, err := db.PutExistingRevWithBody(id, body, []string{"1-a"}, false)
		require.NoError(t, err, "Couldn't create document")
		require.Equal(t, "1-a", revId)

		body[BodyDeleted] = true
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-a", "1-a"}, false)
		require.NoError(t, err, "Couldn't create document")
		require.Equal(t, "2-a", revId, "Couldn't create tombstone revision")

		docIdFlagMap[doc.ID] = channels.Deleted // 1 = Deleted(1)
		endSeq = doc.Sequence
	}

	// Create 10 branched documents (two branches, one branch is tombstoned)
	for i := 1; i <= 10; i++ {
		body["sound"] = "meow"
		id := "branched" + strconv.Itoa(i)
		doc, revId, err := db.PutExistingRevWithBody(id, body, []string{"1-a"}, false)
		require.NoError(t, err, "Couldn't create document revision 1-a")
		require.Equal(t, "1-a", revId)

		body["sound"] = "bark"
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-b", "1-a"}, false)
		require.NoError(t, err, "Couldn't create revision 2-b")
		require.Equal(t, "2-b", revId)

		body["sound"] = "bleat"
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-a", "1-a"}, false)
		require.NoError(t, err, "Couldn't create revision 2-a")
		require.Equal(t, "2-a", revId)

		body[BodyDeleted] = true
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"3-a", "2-a"}, false)
		require.NoError(t, err, "Couldn't create document")
		require.Equal(t, "3-a", revId, "Couldn't create tombstone revision")

		docIdFlagMap[doc.ID] = channels.Branched | channels.Hidden // 20 = Branched (16) + Hidden(4)
		endSeq = doc.Sequence
	}

	// Create 10 branched|deleted documents (two branches, both branches tombstoned)
	for i := 1; i <= 10; i++ {
		body["sound"] = "meow"
		id := "branched|deleted" + strconv.Itoa(i)
		doc, revId, err := db.PutExistingRevWithBody(id, body, []string{"1-a"}, false)
		require.NoError(t, err, "Couldn't create document revision 1-a")
		require.Equal(t, "1-a", revId)

		body["sound"] = "bark"
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-b", "1-a"}, false)
		require.NoError(t, err, "Couldn't create revision 2-b")
		require.Equal(t, "2-b", revId)

		body["sound"] = "bleat"
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-a", "1-a"}, false)
		require.NoError(t, err, "Couldn't create revision 2-a")
		require.Equal(t, "2-a", revId)

		body[BodyDeleted] = true
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"3-a", "2-a"}, false)
		require.NoError(t, err, "Couldn't create document")
		require.Equal(t, "3-a", revId, "Couldn't create tombstone revision")

		body[BodyDeleted] = true
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"3-b", "2-b"}, false)
		require.NoError(t, err, "Couldn't create document")
		require.Equal(t, "3-b", revId, "Couldn't create tombstone revision")

		docIdFlagMap[doc.ID] = channels.Branched | channels.Deleted // 17 = Branched (16) + Deleted(1)
		endSeq = doc.Sequence
	}

	// Create 10 branched|conflict (two branched, neither branch tombstoned) documents
	for i := 1; i <= 10; i++ {
		body["sound"] = "meow"
		id := "branched|conflict" + strconv.Itoa(i)
		doc, revId, err := db.PutExistingRevWithBody(id, body, []string{"1-a"}, false)
		require.NoError(t, err, "Couldn't create document revision 1-a")
		require.Equal(t, "1-a", revId)

		body["sound"] = "bark"
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-b", "1-a"}, false)
		require.NoError(t, err, "Couldn't create revision 2-b")
		require.Equal(t, "2-b", revId)

		body["sound"] = "bleat"
		doc, revId, err = db.PutExistingRevWithBody(id, body, []string{"2-a", "1-a"}, false)
		require.NoError(t, err, "Couldn't create revision 2-a")
		require.Equal(t, "2-a", revId)

		// 28 = Branched(16) + Conflict(8) + Hidden(4)
		docIdFlagMap[doc.ID] = channels.Branched | channels.Conflict | channels.Hidden
		endSeq = doc.Sequence
	}

	// At this point the bucket has 50 documents
	// 30 active documents (10 created + 10 branched + 10 branched|conflict)
	// 20 Deleted documents (10 deleted + 10 branched|deleted)

	// Get changes from channel "ABC" with limit and activeOnly true
	entries, err := db.getChangesInChannelFromQuery(base.TestCtx(t), "ABC", startSeq, endSeq, 25, true)
	require.NoError(t, err, "Couldn't query active docs from channel ABC with limit")
	require.Len(t, entries, 25)
	checkFlags(entries)

	// Get changes from channel "*" with limit and activeOnly true
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "*", startSeq, endSeq, 25, true)
	require.NoError(t, err, "Couldn't query active docs from channel * with limit")
	require.Len(t, entries, 25)
	checkFlags(entries)

	// Get changes from channel "ABC" without limit and activeOnly true
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "ABC", startSeq, endSeq, 0, true)
	require.NoError(t, err, "Couldn't query active docs from channel ABC with limit")
	require.Len(t, entries, 30)
	checkFlags(entries)

	// Get changes from channel "*" without limit and activeOnly true
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "*", startSeq, endSeq, 0, true)
	require.NoError(t, err, "Couldn't query active docs from channel * with limit")
	require.Len(t, entries, 30)
	checkFlags(entries)

	// Get changes from channel "ABC" with limit and activeOnly false
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "ABC", startSeq, endSeq, 45, false)
	require.NoError(t, err, "Couldn't query active docs from channel ABC with limit")
	require.Len(t, entries, 45)
	checkFlags(entries)

	// Get changes from channel "*" with limit and activeOnly false
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "*", startSeq, endSeq, 45, false)
	require.NoError(t, err, "Couldn't query active docs from channel * with limit")
	require.Len(t, entries, 45)
	checkFlags(entries)

	// Get changes from channel "ABC" without limit and activeOnly false
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "ABC", startSeq, endSeq, 0, false)
	require.NoError(t, err, "Couldn't query active docs from channel ABC with limit")
	require.Len(t, entries, 50)
	checkFlags(entries)

	// Get changes from channel "*" without limit and activeOnly true
	entries, err = db.getChangesInChannelFromQuery(base.TestCtx(t), "*", startSeq, endSeq, 0, false)
	require.NoError(t, err, "Couldn't query active docs from channel * with limit")
	require.Len(t, entries, 50)
	checkFlags(entries)
}

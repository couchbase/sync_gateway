package base

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/couchbase/gocb"
	"github.com/couchbaselabs/go.assert"
)

var testN1qlOptions = &N1qlIndexOptions{
	NumReplica: 0,
}

func TestN1qlQuery(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Write a few docs to the bucket to query
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := bucket.AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Errorf("Error adding doc for TestN1qlQuery: %v", err)
		}
		assertTrue(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	err := bucket.CreateIndex("testIndex_value", indexExpression, "", testN1qlOptions)
	if err != nil {
		if !strings.Contains(err.Error(), "index testIndex_value already exists") {
			t.Errorf("Error creating index: %s", err)
		}
	}

	// Wait for index readiness
	onlineErr := bucket.WaitForIndexOnline("testIndex_value")
	if onlineErr != nil {
		t.Errorf("Error waiting for index to come online: %v", err)
	}

	// Check index state
	exists, state, stateErr := bucket.GetIndexMeta("testIndex_value")
	assertNoError(t, stateErr, "Error validating index state")
	assertTrue(t, state != nil, "No state returned for index")
	assert.Equals(t, state.State, "online")
	assert.Equals(t, exists, true)

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value")
		if err != nil {
			t.Errorf("Error dropping index: %s", err)
		}
	}()

	readyErr := bucket.WaitForIndexOnline("testIndex_value")
	assertNoError(t, readyErr, "Error validating index online")

	// Query the index
	queryExpression := fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", BucketQueryToken)
	params := make(map[string]interface{})
	params["minvalue"] = 2

	queryResults, queryErr := bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assertNoError(t, queryErr, "Error executing n1ql query")

	// Struct to receive the query response (META().id, val)
	var queryResult struct {
		Id  string
		Val int
	}
	var queryCloseErr error
	var count int

	// Iterate over results - validate values and count to ensure $minvalue is being applied correctly
	for {
		ok := queryResults.Next(&queryResult)
		if !ok {
			queryCloseErr = queryResults.Close()
			break
		}
		assertTrue(t, queryResult.Val > 2, "Query returned unexpected result")
		count++
	}

	// Requery the index, validate empty resultset behaviour
	params = make(map[string]interface{})
	params["minvalue"] = 10

	queryResults, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assertNoError(t, queryErr, "Error executing n1ql query")

	count = 0
	for {
		ok := queryResults.Next(&queryResult)
		if !ok {
			queryCloseErr = queryResults.Close()
			break
		}
		assertTrue(t, queryResult.Val > 10, "Query returned unexpected result")
		count++
	}

	assertNoError(t, queryCloseErr, "Unexpected error closing query results")
	assert.Equals(t, count, 0)

}

func TestN1qlFilterExpression(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Write a few docs to the bucket to query
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := bucket.AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Errorf("Error adding doc for TestIndexFilterExpression: %v", err)
		}
		assertTrue(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	filterExpression := "val < 3"
	err := bucket.CreateIndex("testIndex_filtered_value", indexExpression, filterExpression, testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Wait for index readiness
	readyErr := bucket.WaitForIndexOnline("testIndex_filtered_value")
	assertNoError(t, readyErr, "Error validating index online")

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_filtered_value")
		if err != nil {
			t.Errorf("Error dropping index: %s", err)
		}
	}()

	// Query the index
	queryExpression := fmt.Sprintf("SELECT META().id, val FROM %s WHERE %s AND META().id = 'doc1'", BucketQueryToken, filterExpression)
	queryResults, queryErr := bucket.Query(queryExpression, nil, gocb.RequestPlus, false)
	assertNoError(t, queryErr, "Error executing n1ql query")

	// Struct to receive the query response (META().id, val)
	var queryResult struct {
		Id  string
		Val int
	}
	var queryCloseErr error
	var count int

	// Iterate over results - validate values and count to ensure $minvalue is being applied correctly
	for {
		ok := queryResults.Next(&queryResult)
		if !ok {
			queryCloseErr = queryResults.Close()
			assertNoError(t, queryCloseErr, "Error closing queryResults")
			break
		}
		assertTrue(t, queryResult.Val < 3, "Query returned unexpected result")
		assert.Equals(t, queryResult.Id, "doc1")
		count++
	}
	assert.Equals(t, count, 1)

}

// Test index state retrieval
func TestIndexMeta(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Check index state pre-creation
	exists, meta, err := bucket.GetIndexMeta("testIndex_value")
	assert.Equals(t, exists, false)
	assertNoError(t, err, "Error getting meta for non-existent index")

	indexExpression := "val"
	err = bucket.CreateIndex("testIndex_value", indexExpression, "", testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex_value")
	assertNoError(t, readyErr, "Error validating index online")

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value")
		if err != nil {
			t.Errorf("Error dropping index: %s", err)
		}
	}()

	// Check index state post-creation
	exists, meta, err = bucket.GetIndexMeta("testIndex_value")
	assert.Equals(t, exists, true)
	assert.Equals(t, meta.State, "online")
	assertNoError(t, err, "Error retrieving index state")
}

// Ensure that n1ql query errors are handled and returned (and don't result in panic etc)
func TestMalformedN1qlQuery(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Write a few docs to the bucket to query
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := bucket.AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Errorf("Error adding doc for TestN1qlQuery: %v", err)
		}
		assertTrue(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	err := bucket.CreateIndex("testIndex_value_malformed", indexExpression, "", testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex_value_malformed")
	assertNoError(t, readyErr, "Error validating index online")

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value_malformed")
		if err != nil {
			t.Errorf("Error dropping index: %s", err)
		}
	}()

	// Query with syntax error
	queryExpression := "SELECT META().id, val WHERE val > $minvalue"
	params := make(map[string]interface{})
	_, queryErr := bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assertTrue(t, queryErr != nil, "Expected error for malformed n1ql query (syntax)")

	// Query against non-existing bucket
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", "badBucket")
	params = map[string]interface{}{"minvalue": 2}
	_, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assertTrue(t, queryErr != nil, "Expected error for malformed n1ql query (no bucket)")

	// Specify params for non-parameterized query (no error expected, ensure doesn't break)
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > 5", BucketQueryToken)
	params = map[string]interface{}{"minvalue": 2}
	_, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assertTrue(t, queryErr == nil, "Unexpected error for malformed n1ql query (extra params)")

	// Omit params for parameterized query
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", BucketQueryToken)
	params = make(map[string]interface{})
	_, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assertTrue(t, queryErr != nil, "Expected error for malformed n1ql query (missing params)")

}

func TestCreateAndDropIndex(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	createExpression := "_sync.sequence"
	err := bucket.CreateIndex("testIndex_sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex_sequence")
	assertNoError(t, readyErr, "Error validating index online")

	// Drop the index
	err = bucket.DropIndex("testIndex_sequence")
	if err != nil {
		t.Errorf("Error dropping index: %s", err)
	}
}

func TestCreateDuplicateIndex(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	createExpression := "_sync.sequence"
	err := bucket.CreateIndex("testIndexDuplicateSequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndexDuplicateSequence")
	assertNoError(t, readyErr, "Error validating index online")

	// Attempt to create duplicate, validate duplicate error
	duplicateErr := bucket.CreateIndex("testIndexDuplicateSequence", createExpression, "", testN1qlOptions)
	assert.Equals(t, duplicateErr, ErrIndexAlreadyExists)

	// Drop the index
	err = bucket.DropIndex("testIndexDuplicateSequence")
	if err != nil {
		t.Errorf("Error dropping index: %s", err)
	}
}

func TestCreateAndDropIndexSpecialCharacters(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	createExpression := "_sync.sequence"
	err := bucket.CreateIndex("testIndex-sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex-sequence")
	assertNoError(t, readyErr, "Error validating index online")

	// Drop the index
	err = bucket.DropIndex("testIndex-sequence")
	if err != nil {
		t.Errorf("Error dropping index: %s", err)
	}
}

func TestDeferredCreateIndex(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()

	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	indexName := "testIndexDeferred"
	assertNoError(t, tearDownTestIndex(bucket, indexName), "Error in pre-test cleanup")

	deferN1qlOptions := &N1qlIndexOptions{
		NumReplica: 0,
		DeferBuild: true,
	}

	createExpression := "_sync.sequence"
	err := bucket.CreateIndex(indexName, createExpression, "", deferN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Drop the index
	defer func() {
		err = bucket.DropIndex(indexName)
		if err != nil {
			t.Errorf("Error dropping index: %s", err)
		}
	}()

	buildErr := bucket.BuildIndexes([]string{indexName})
	assertNoError(t, buildErr, "Error building indexes")

	readyErr := bucket.WaitForIndexOnline(indexName)
	assertNoError(t, readyErr, "Error validating index online")

}

func TestBuildPendingIndexes(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()

	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	deferredIndexName := "testIndexDeferred"
	nonDeferredIndexName := "testIndexNonDeferred"
	assertNoError(t, tearDownTestIndex(bucket, deferredIndexName), "Error in pre-test cleanup")
	assertNoError(t, tearDownTestIndex(bucket, nonDeferredIndexName), "Error in pre-test cleanup")

	deferN1qlOptions := &N1qlIndexOptions{
		NumReplica: 0,
		DeferBuild: true,
	}

	// Create a deferred and a non-deferred index
	createExpression := "_sync.sequence"
	err := bucket.CreateIndex(deferredIndexName, createExpression, "", deferN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	createExpression = "_sync.rev"
	err = bucket.CreateIndex(nonDeferredIndexName, createExpression, "", &N1qlIndexOptions{NumReplica: 0})
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Drop the indexes
	defer func() {
		err = bucket.DropIndex(deferredIndexName)
		if err != nil {
			t.Errorf("Error dropping deferred index: %s", err)
		}
	}()
	defer func() {
		err = bucket.DropIndex(nonDeferredIndexName)
		if err != nil {
			t.Errorf("Error dropping non-deferred index: %s", err)
		}
	}()

	buildErr := bucket.BuildPendingIndexes([]string{deferredIndexName, nonDeferredIndexName}, 10)
	assertNoError(t, buildErr, "Error building indexes")

	readyErr := bucket.WaitForIndexOnline(deferredIndexName)
	assertNoError(t, readyErr, "Error validating index online")
	readyErr = bucket.WaitForIndexOnline(nonDeferredIndexName)
	assertNoError(t, readyErr, "Error validating index online")

	// Ensure no errors from no-op scenarios
	alreadyBuiltErr := bucket.BuildPendingIndexes([]string{deferredIndexName, nonDeferredIndexName}, 10)
	assertNoError(t, alreadyBuiltErr, "Error building already built indexes")

	emptySetErr := bucket.BuildPendingIndexes([]string{}, 10)
	assertNoError(t, emptySetErr, "Error building empty set")
}

func TestCreateAndDropIndexErrors(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Malformed expression
	createExpression := "_sync sequence"
	err := bucket.CreateIndex("testIndex_malformed", createExpression, "", testN1qlOptions)
	if err == nil {
		t.Errorf("Expected error for malformed index expression")
	}

	// Create index
	createExpression = "_sync.sequence"
	err = bucket.CreateIndex("testIndex_sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Attempt to recreate duplicate index
	err = bucket.CreateIndex("testIndex_sequence", createExpression, "", testN1qlOptions)
	if err == nil {
		t.Errorf("Expected error attempting to recreate already existing index")
	}

	// Drop non-existent index
	err = bucket.DropIndex("testIndex_not_found")
	if err == nil {
		t.Errorf("Expected error attempting to drop non-existent index")
	}

	// Drop the index
	err = bucket.DropIndex("testIndex_sequence")
	if err != nil {
		t.Errorf("Error dropping index: %s", err)
	}

	// Drop index that's already been dropped
	err = bucket.DropIndex("testIndex_sequence")
	if err == nil {
		t.Errorf("Expected error attempting to drop index twice")
	}
}

func queryResultCount(queryResults gocb.QueryResults) (count int, err error) {

	for {
		bytes := queryResults.NextBytes()
		if bytes == nil {
			return count, queryResults.Close()
		}
		log.Printf("QueryResults[%d]: %s", count, bytes)
		count++
	}
}

func tearDownTestIndex(bucket *CouchbaseBucketGoCB, indexName string) (err error) {

	exists, _, err := bucket.GetIndexMeta(indexName)
	if err != nil {
		return err
	}

	if exists {
		return bucket.DropIndex(indexName)
	} else {
		return nil
	}

}

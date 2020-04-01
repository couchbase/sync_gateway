package base

import (
	"errors"
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/gocb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testN1qlOptions = &N1qlIndexOptions{
	NumReplica: 0,
}

func TestN1qlQuery(t *testing.T) {

	// Disabled due to CBG-755:
	t.Skip("WARNING: TEST DISABLED - the testIndex_value creation is causing issues with CB 6.5.0")

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := AsGoCBBucket(testBucket)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Write a few docs to the bucket to query
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := bucket.AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Fatalf("Error adding doc for TestN1qlQuery: %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	err := bucket.CreateIndex("testIndex_value", indexExpression, "", testN1qlOptions)
	if err != nil && err != ErrIndexAlreadyExists {
		t.Errorf("Error creating index: %s", err)
	}

	// Wait for index readiness
	onlineErr := bucket.WaitForIndexOnline("testIndex_value")
	if onlineErr != nil {
		t.Fatalf("Error waiting for index to come online: %v", err)
	}

	// Check index state
	exists, state, stateErr := bucket.GetIndexMeta("testIndex_value")
	assert.NoError(t, stateErr, "Error validating index state")
	assert.True(t, state != nil, "No state returned for index")
	assert.Equal(t, "online", state.State)
	assert.True(t, exists)

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value")
		if err != nil {
			t.Fatalf("Error dropping index: %s", err)
		}
	}()

	readyErr := bucket.WaitForIndexOnline("testIndex_value")
	require.NoError(t, readyErr, "Error validating index online")

	// Query the index
	queryExpression := fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", BucketQueryToken)
	params := make(map[string]interface{})
	params["minvalue"] = 2

	queryResults, queryErr := bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	require.NoError(t, queryErr, "Error executing n1ql query")

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
		assert.True(t, queryResult.Val > 2, "Query returned unexpected result")
		count++
	}

	// Requery the index, validate empty resultset behaviour
	params = make(map[string]interface{})
	params["minvalue"] = 10

	queryResults, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assert.NoError(t, queryErr, "Error executing n1ql query")

	count = 0
	for {
		ok := queryResults.Next(&queryResult)
		if !ok {
			queryCloseErr = queryResults.Close()
			break
		}
		assert.True(t, queryResult.Val > 10, "Query returned unexpected result")
		count++
	}

	assert.NoError(t, queryCloseErr, "Unexpected error closing query results")
	assert.Equal(t, 0, count)

}

func TestN1qlFilterExpression(t *testing.T) {

	// Disabled due to CBG-755:
	t.Skip("WARNING: TEST DISABLED - the testIndex_value creation is causing issues with CB 6.5.0")

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucket(t)
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
			t.Fatalf("Error adding doc for TestIndexFilterExpression: %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	filterExpression := "val < 3"
	err := bucket.CreateIndex("testIndex_filtered_value", indexExpression, filterExpression, testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	// Wait for index readiness
	readyErr := bucket.WaitForIndexOnline("testIndex_filtered_value")
	require.NoError(t, readyErr, "Error validating index online")

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_filtered_value")
		if err != nil {
			t.Fatalf("Error dropping index: %s", err)
		}
	}()

	// Query the index
	queryExpression := fmt.Sprintf("SELECT META().id, val FROM %s WHERE %s AND META().id = 'doc1'", BucketQueryToken, filterExpression)
	queryResults, queryErr := bucket.Query(queryExpression, nil, gocb.RequestPlus, false)
	require.NoError(t, queryErr, "Error executing n1ql query")

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
			assert.NoError(t, queryCloseErr, "Error closing queryResults")
			break
		}
		assert.True(t, queryResult.Val < 3, "Query returned unexpected result")
		assert.Equal(t, "doc1", queryResult.Id)
		count++
	}
	assert.Equal(t, 1, count)

}

// Test index state retrieval
func TestIndexMeta(t *testing.T) {

	// Disabled due to CBG-755:
	t.Skip("WARNING: TEST DISABLED - the testIndex_value creation is causing issues with CB 6.5.0")

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Check index state pre-creation
	exists, meta, err := bucket.GetIndexMeta("testIndex_value")
	assert.False(t, exists)
	assert.NoError(t, err, "Error getting meta for non-existent index")

	indexExpression := "val"
	err = bucket.CreateIndex("testIndex_value", indexExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex_value")
	require.NoError(t, readyErr, "Error validating index online")

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value")
		if err != nil {
			t.Fatalf("Error dropping index: %s", err)
		}
	}()

	// Check index state post-creation
	exists, meta, err = bucket.GetIndexMeta("testIndex_value")
	assert.True(t, exists)
	assert.Equal(t, "online", meta.State)
	assert.NoError(t, err, "Error retrieving index state")
}

// Ensure that n1ql query errors are handled and returned (and don't result in panic etc)
func TestMalformedN1qlQuery(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
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
			t.Fatalf("Error adding doc for TestN1qlQuery: %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	err := bucket.CreateIndex("testIndex_value_malformed", indexExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex_value_malformed")
	assert.NoError(t, readyErr, "Error validating index online")

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value_malformed")
		if err != nil {
			t.Fatalf("Error dropping index: %s", err)
		}
	}()

	// Query with syntax error
	queryExpression := "SELECT META().id, val WHERE val > $minvalue"
	params := make(map[string]interface{})
	_, queryErr := bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assert.True(t, queryErr != nil, "Expected error for malformed n1ql query (syntax)")

	// Query against non-existing bucket
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", "badBucket")
	params = map[string]interface{}{"minvalue": 2}
	_, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assert.True(t, queryErr != nil, "Expected error for malformed n1ql query (no bucket)")

	// Specify params for non-parameterized query (no error expected, ensure doesn't break)
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > 5", BucketQueryToken)
	params = map[string]interface{}{"minvalue": 2}
	_, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assert.True(t, queryErr == nil, "Unexpected error for malformed n1ql query (extra params)")

	// Omit params for parameterized query
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", BucketQueryToken)
	params = make(map[string]interface{})
	_, queryErr = bucket.Query(queryExpression, params, gocb.RequestPlus, false)
	assert.True(t, queryErr != nil, "Expected error for malformed n1ql query (missing params)")

}

func TestCreateAndDropIndex(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	createExpression := SyncPropertyName + ".sequence"
	err := bucket.CreateIndex("testIndex_sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex_sequence")
	assert.NoError(t, readyErr, "Error validating index online")

	// Drop the index
	err = bucket.DropIndex("testIndex_sequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}
}

func TestCreateDuplicateIndex(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	createExpression := SyncPropertyName + ".sequence"
	err := bucket.CreateIndex("testIndexDuplicateSequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndexDuplicateSequence")
	assert.NoError(t, readyErr, "Error validating index online")

	// Attempt to create duplicate, validate duplicate error
	duplicateErr := bucket.CreateIndex("testIndexDuplicateSequence", createExpression, "", testN1qlOptions)
	assert.Equal(t, ErrIndexAlreadyExists, duplicateErr)

	// Drop the index
	err = bucket.DropIndex("testIndexDuplicateSequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}
}

func TestCreateAndDropIndexSpecialCharacters(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	createExpression := SyncPropertyName + ".sequence"
	err := bucket.CreateIndex("testIndex-sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := bucket.WaitForIndexOnline("testIndex-sequence")
	assert.NoError(t, readyErr, "Error validating index online")

	// Drop the index
	err = bucket.DropIndex("testIndex-sequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}
}

func TestDeferredCreateIndex(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	indexName := "testIndexDeferred"
	assert.NoError(t, tearDownTestIndex(bucket, indexName), "Error in pre-test cleanup")

	deferN1qlOptions := &N1qlIndexOptions{
		NumReplica: 0,
		DeferBuild: true,
	}

	createExpression := SyncPropertyName + ".sequence"
	err := bucket.CreateIndex(indexName, createExpression, "", deferN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	// Drop the index
	defer func() {
		err = bucket.DropIndex(indexName)
		if err != nil {
			t.Fatalf("Error dropping index: %s", err)
		}
	}()

	buildErr := bucket.buildIndexes([]string{indexName})
	assert.NoError(t, buildErr, "Error building indexes")

	readyErr := bucket.WaitForIndexOnline(indexName)
	assert.NoError(t, readyErr, "Error validating index online")

}

func TestBuildDeferredIndexes(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	deferredIndexName := "testIndexDeferred"
	nonDeferredIndexName := "testIndexNonDeferred"
	assert.NoError(t, tearDownTestIndex(bucket, deferredIndexName), "Error in pre-test cleanup")
	assert.NoError(t, tearDownTestIndex(bucket, nonDeferredIndexName), "Error in pre-test cleanup")

	deferN1qlOptions := &N1qlIndexOptions{
		NumReplica: 0,
		DeferBuild: true,
	}

	// Create a deferred and a non-deferred index
	createExpression := SyncPropertyName + ".sequence"
	err := bucket.CreateIndex(deferredIndexName, createExpression, "", deferN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	createExpression = SyncPropertyName + ".rev"
	err = bucket.CreateIndex(nonDeferredIndexName, createExpression, "", &N1qlIndexOptions{NumReplica: 0})
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	// Drop the indexes
	defer func() {
		err = bucket.DropIndex(deferredIndexName)
		if err != nil {
			t.Fatalf("Error dropping deferred index: %s", err)
		}
	}()
	defer func() {
		err = bucket.DropIndex(nonDeferredIndexName)
		if err != nil {
			t.Fatalf("Error dropping non-deferred index: %s", err)
		}
	}()

	buildErr := bucket.BuildDeferredIndexes([]string{deferredIndexName, nonDeferredIndexName})
	assert.NoError(t, buildErr, "Error building indexes")

	readyErr := bucket.WaitForIndexOnline(deferredIndexName)
	assert.NoError(t, readyErr, "Error validating index online")
	readyErr = bucket.WaitForIndexOnline(nonDeferredIndexName)
	assert.NoError(t, readyErr, "Error validating index online")

	// Ensure no errors from no-op scenarios
	alreadyBuiltErr := bucket.BuildDeferredIndexes([]string{deferredIndexName, nonDeferredIndexName})
	assert.NoError(t, alreadyBuiltErr, "Error building already built indexes")

	emptySetErr := bucket.BuildDeferredIndexes([]string{})
	assert.NoError(t, emptySetErr, "Error building empty set")
}

func TestCreateAndDropIndexErrors(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := testBucket.Bucket.(*CouchbaseBucketGoCB)
	if !ok {
		t.Fatalf("Requires gocb bucket")
	}

	// Malformed expression
	createExpression := "_sync sequence"
	err := bucket.CreateIndex("testIndex_malformed", createExpression, "", testN1qlOptions)
	if err == nil {
		t.Fatalf("Expected error for malformed index expression")
	}

	// Create index
	createExpression = "_sync.sequence"
	err = bucket.CreateIndex("testIndex_sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	// Attempt to recreate duplicate index
	err = bucket.CreateIndex("testIndex_sequence", createExpression, "", testN1qlOptions)
	if err == nil {
		t.Fatalf("Expected error attempting to recreate already existing index")
	}

	// Drop non-existent index
	err = bucket.DropIndex("testIndex_not_found")
	if err == nil {
		t.Fatalf("Expected error attempting to drop non-existent index")
	}

	// Drop the index
	err = bucket.DropIndex("testIndex_sequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}

	// Drop index that's already been dropped
	err = bucket.DropIndex("testIndex_sequence")
	if err == nil {
		t.Fatalf("Expected error attempting to drop index twice")
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

func TestWaitForBucketExistence(t *testing.T) {

	defer SetUpTestLogging(LevelDebug, KeyAll)()

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucket(t)
	defer testBucket.Close()
	bucket, ok := AsGoCBBucket(testBucket)
	require.True(t, ok, "Requires gocb bucket")

	// Create index
	const (
		indexName        = "index1"
		expression       = "_sync"
		filterExpression = ""
	)
	var options = &N1qlIndexOptions{NumReplica: 0}

	go func() {
		indexExists, _, err := bucket.getIndexMetaWithoutRetry(indexName)
		assert.NoError(t, err, "No error while trying to fetch the index metadata")

		if indexExists {
			err := bucket.DropIndex(indexName)
			assert.NoError(t, err, "Index should be removed from the bucket")
		}

		err = bucket.CreateIndex(indexName, expression, filterExpression, options)
		assert.NoError(t, err, "Index should be created in the bucket")
	}()

	assert.NoError(t, bucket.waitForBucketExistence(indexName, true))

	// Drop the index;
	err := bucket.DropIndex(indexName)
	assert.NoError(t, err, "Index should be removed from the bucket")
}

func TestIsIndexerRetryBuildError(t *testing.T) {
	var err error
	assert.False(t, IsIndexerRetryBuildError(err))
	err = errors.New("will retry building in the background")
	assert.True(t, IsIndexerRetryBuildError(err))
}

func TestIsTransientIndexerError(t *testing.T) {
	var err error
	assert.False(t, isTransientIndexerError(err))
	err = errors.New("lost heartbeat")
	assert.False(t, isTransientIndexerError(err))
	err = errors.New("Indexer rollback")
	assert.True(t, isTransientIndexerError(err))
}

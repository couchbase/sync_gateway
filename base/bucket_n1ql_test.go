package base

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/gocb"
	"github.com/couchbaselabs/go.assert"
)

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
	err := bucket.CreateIndex("testIndex_value", indexExpression, 0)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Check index state
	exists, state, stateErr := bucket.GetIndexMeta("testIndex_value")
	assertNoError(t, stateErr, "Error validating index state")
	assert.Equals(t, state, "online")
	assert.Equals(t, exists, true)

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value")
		if err != nil {
			t.Errorf("Error dropping index: %s", err)
		}
	}()

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
	err = bucket.CreateIndex("testIndex_value", indexExpression, 0)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

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
	err := bucket.CreateIndex("testIndex_value", indexExpression, 0)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Defer index teardown
	defer func() {
		// Drop the index
		err = bucket.DropIndex("testIndex_value")
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
	err := bucket.CreateIndex("testIndex_sequence", createExpression, 0)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Drop the index
	err = bucket.DropIndex("testIndex_sequence")
	if err != nil {
		t.Errorf("Error dropping index: %s", err)
	}
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
	err := bucket.CreateIndex("testIndex_malformed", createExpression, 0)
	if err == nil {
		t.Errorf("Expected error for malformed index expression")
	}

	// Create index
	createExpression = "_sync.sequence"
	err = bucket.CreateIndex("testIndex_sequence", createExpression, 0)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	// Attempt to recreate duplicate index
	err = bucket.CreateIndex("testIndex_sequence", createExpression, 0)
	if err == nil {
		t.Errorf("Expected error attempting to recreate already existing index")
	}

	// Drop non-existent index
	err = bucket.DropIndex("testIndex_not_found")
	if err == nil {
		t.Errorf("Expected error attempting to drop non-existent index", err)
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

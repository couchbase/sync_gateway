/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testN1qlOptions = &N1qlIndexOptions{
	NumReplica: 0,
}

func TestN1qlQuery(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	// Write a few docs to the bucket to query
	for i := range 5 {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := n1qlStore.(sgbucket.DataStore).AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Fatalf("Error adding doc for TestN1qlQuery: %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	err := n1qlStore.CreateIndex(ctx, "testIndex_value", indexExpression, "", testN1qlOptions)
	require.NoError(t, err)

	// Wait for index readiness
	onlineErr := n1qlStore.WaitForIndexesOnline(ctx, []string{"testIndex_value"}, WaitForIndexesDefault)
	if onlineErr != nil {
		t.Fatalf("Error waiting for index to come online: %v", err)
	}

	// Check index state
	exists, meta, stateErr := n1qlStore.GetIndexMeta(ctx, "testIndex_value")
	require.NoError(t, stateErr, "Error validating index state")
	require.NotNil(t, meta, "No state returned for index")
	assert.Equal(t, IndexStateOnline, meta.State)
	assert.True(t, exists)
	expectedTestIndexMeta := IndexMeta{
		Name:  "testIndex_value",
		State: IndexStateOnline,
	}
	metas, err := GetIndexesMeta(ctx, n1qlStore, []string{"testIndex_value"})
	require.NoError(t, err)
	require.Len(t, metas, 1)
	require.Equal(t, expectedTestIndexMeta, metas["testIndex_value"])

	metas, err = GetIndexesMeta(ctx, n1qlStore, []string{"does_not_exist"})
	require.NoError(t, err)
	require.Len(t, metas, 0)

	// Query the index
	queryExpression := fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", KeyspaceQueryToken)
	params := make(map[string]any)
	params["minvalue"] = 2

	queryResults, queryErr := n1qlStore.Query(ctx, queryExpression, params, RequestPlus, false)
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
		ok := queryResults.Next(ctx, &queryResult)
		if !ok {
			queryCloseErr = queryResults.Close()
			break
		}
		assert.True(t, queryResult.Val > 2, "Query returned unexpected result")
		count++
	}
	assert.NoError(t, queryCloseErr, "Unexpected error closing query results")

	// Requery the index, validate empty resultset behaviour
	params = make(map[string]any)
	params["minvalue"] = 10

	queryResults, queryErr = n1qlStore.Query(ctx, queryExpression, params, RequestPlus, false)
	require.NoError(t, queryErr, "Error executing n1ql query")

	count = 0
	for {
		ok := queryResults.Next(ctx, &queryResult)
		if !ok {
			queryCloseErr = queryResults.Close()
			break
		}
		assert.True(t, queryResult.Val > 10, "Query returned unexpected result")
		count++
	}

	assert.NoError(t, queryCloseErr, "Unexpected error closing query results")
	assert.Equal(t, 0, count)

	// test creation of a primary index
	primaryIdx := t.Name() + "_primary"
	require.NoError(t, n1qlStore.CreatePrimaryIndex(ctx, primaryIdx, nil))

	expectedPrimaryIndexMeta := IndexMeta{
		Name:  primaryIdx,
		State: IndexStateOnline,
	}
	metas, err = GetIndexesMeta(ctx, n1qlStore, []string{primaryIdx, "testIndex_value"})
	require.NoError(t, err)
	require.Equal(t, map[string]IndexMeta{
		primaryIdx:        expectedPrimaryIndexMeta,
		"testIndex_value": expectedTestIndexMeta,
	}, metas)

}

func TestN1qlFilterExpression(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	// Write a few docs to the bucket to query
	for i := range 5 {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := n1qlStore.(sgbucket.DataStore).AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Fatalf("Error adding doc for TestIndexFilterExpression: %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	filterExpression := "val < 3"
	err := n1qlStore.CreateIndex(ctx, "testIndex_filtered_value", indexExpression, filterExpression, testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	// Wait for index readiness
	readyErr := n1qlStore.WaitForIndexesOnline(ctx, []string{"testIndex_filtered_value"}, WaitForIndexesDefault)
	require.NoError(t, readyErr, "Error validating index online")

	// Query the index
	queryExpression := fmt.Sprintf("SELECT META().id, val FROM %s WHERE %s AND META().id = 'doc1'", KeyspaceQueryToken, filterExpression)
	queryResults, queryErr := n1qlStore.Query(ctx, queryExpression, nil, RequestPlus, false)
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
		ok := queryResults.Next(ctx, &queryResult)
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
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	// Check index state pre-creation
	exists, _, err := n1qlStore.GetIndexMeta(ctx, "testIndex_value")
	require.NoError(t, err, "Error getting meta for non-existent index")
	require.False(t, exists)

	indexExpression := "val"
	err = n1qlStore.CreateIndex(ctx, "testIndex_value", indexExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := n1qlStore.WaitForIndexesOnline(ctx, []string{"testIndex_value"}, WaitForIndexesDefault)
	require.NoError(t, readyErr, "Error validating index online")

	// Check index state post-creation
	exists, meta, err := n1qlStore.GetIndexMeta(ctx, "testIndex_value")
	require.NoError(t, err, "Error retrieving index state")
	assert.True(t, exists)
	assert.Equal(t, IndexStateOnline, meta.State)
}

// Ensure that n1ql query errors are handled and returned (and don't result in panic etc)
func TestMalformedN1qlQuery(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	// Write a few docs to the bucket to query
	for i := range 5 {
		key := fmt.Sprintf("doc%d", i)
		body := fmt.Sprintf(`{"val": %d}`, i)
		added, err := n1qlStore.(sgbucket.DataStore).AddRaw(key, 0, []byte(body))
		if err != nil {
			t.Fatalf("Error adding doc for TestN1qlQuery: %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")
	}

	indexExpression := "val"
	err := n1qlStore.CreateIndex(ctx, "testIndex_value_malformed", indexExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := n1qlStore.WaitForIndexesOnline(ctx, []string{"testIndex_value_malformed"}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")

	// Query with syntax error
	queryExpression := "SELECT META().id, val WHERE val > $minvalue"
	params := make(map[string]any)
	_, queryErr := n1qlStore.Query(ctx, queryExpression, params, RequestPlus, false)
	assert.True(t, queryErr != nil, "Expected error for malformed n1ql query (syntax)")

	// Query against non-existing bucket
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", "badBucket")
	params = map[string]any{"minvalue": 2}
	_, queryErr = n1qlStore.Query(ctx, queryExpression, params, RequestPlus, false)
	assert.True(t, queryErr != nil, "Expected error for malformed n1ql query (no bucket)")

	// Specify params for non-parameterized query (no error expected, ensure doesn't break)
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > 5", KeyspaceQueryToken)
	params = map[string]any{"minvalue": 2}
	queryResults, queryErr := n1qlStore.Query(ctx, queryExpression, params, RequestPlus, false)
	require.True(t, queryErr == nil, "Unexpected error for malformed n1ql query (extra params)")
	assert.NoError(t, queryResults.Close())

	// Omit params for parameterized query
	queryExpression = fmt.Sprintf("SELECT META().id, val FROM %s WHERE val > $minvalue", KeyspaceQueryToken)
	params = map[string]any{"othervalue": 2}
	results, queryErr := n1qlStore.Query(ctx, queryExpression, params, RequestPlus, false)
	if queryErr == nil {
		for results.NextBytes() != nil {
		}
		queryErr = results.Close()
	}
	assert.True(t, queryErr != nil, "Expected error for malformed n1ql query (missing params)")
}

func TestCreateAndDropIndex(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	createExpression := SyncPropertyName + ".`sequence`"
	err := n1qlStore.CreateIndex(ctx, "testIndex_sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}
	readyErr := n1qlStore.WaitForIndexesOnline(ctx, []string{"testIndex_sequence"}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")

	// Drop the index
	err = n1qlStore.DropIndex(ctx, "testIndex_sequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}
}

func TestCreateDuplicateIndex(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	createExpression := SyncPropertyName + ".`sequence`"
	err := n1qlStore.CreateIndex(ctx, "testIndexDuplicateSequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := n1qlStore.WaitForIndexesOnline(TestCtx(t), []string{"testIndexDuplicateSequence"}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")

	// Attempt to create duplicate, validate duplicate error
	duplicateErr := n1qlStore.CreateIndex(ctx, "testIndexDuplicateSequence", createExpression, "", testN1qlOptions)
	assert.Equal(t, ErrAlreadyExists, duplicateErr)

	// Drop the index
	err = n1qlStore.DropIndex(ctx, "testIndexDuplicateSequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}
}

func TestCreateAndDropIndexSpecialCharacters(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	createExpression := SyncPropertyName + ".`sequence`"
	err := n1qlStore.CreateIndex(ctx, "testIndex-sequence", createExpression, "", testN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	readyErr := n1qlStore.WaitForIndexesOnline(TestCtx(t), []string{"testIndex-sequence"}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")

	// Drop the index
	err = n1qlStore.DropIndex(ctx, "testIndex-sequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}
}

func TestDeferredCreateIndex(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	indexName := "testIndexDeferred"

	deferN1qlOptions := &N1qlIndexOptions{
		NumReplica: 0,
		DeferBuild: true,
	}

	createExpression := SyncPropertyName + ".`sequence`"
	err := n1qlStore.CreateIndex(ctx, indexName, createExpression, "", deferN1qlOptions)
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}
	exists, meta, err := n1qlStore.GetIndexMeta(ctx, indexName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, IndexStateDeferred, meta.State)

	buildErr := buildIndexes(ctx, n1qlStore, []string{indexName})
	assert.NoError(t, buildErr, "Error building indexes")

	readyErr := n1qlStore.WaitForIndexesOnline(ctx, []string{indexName}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")

	exists, meta, err = n1qlStore.GetIndexMeta(ctx, indexName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, IndexStateOnline, meta.State)

}

func TestBuildDeferredIndexes(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	deferredIndexName := "testIndexDeferred"
	nonDeferredIndexName := "testIndexNonDeferred"

	deferN1qlOptions := &N1qlIndexOptions{
		NumReplica: 0,
		DeferBuild: true,
	}

	// Create a deferred and a non-deferred index
	createExpression := SyncPropertyName + ".`sequence`"
	err := n1qlStore.CreateIndex(ctx, deferredIndexName, createExpression, "", deferN1qlOptions)
	if err != nil {
		t.Errorf("Error creating index: %s", err)
	}

	createExpression = SyncPropertyName + ".rev"
	err = n1qlStore.CreateIndex(ctx, nonDeferredIndexName, createExpression, "", &N1qlIndexOptions{NumReplica: 0})
	if err != nil {
		t.Fatalf("Error creating index: %s", err)
	}

	buildErr := n1qlStore.BuildDeferredIndexes(TestCtx(t), []string{deferredIndexName, nonDeferredIndexName})
	assert.NoError(t, buildErr, "Error building indexes")

	readyErr := n1qlStore.WaitForIndexesOnline(TestCtx(t), []string{deferredIndexName}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")
	readyErr = n1qlStore.WaitForIndexesOnline(TestCtx(t), []string{nonDeferredIndexName}, WaitForIndexesDefault)
	assert.NoError(t, readyErr, "Error validating index online")

	// Ensure no errors from no-op scenarios
	alreadyBuiltErr := n1qlStore.BuildDeferredIndexes(TestCtx(t), []string{deferredIndexName, nonDeferredIndexName})
	assert.NoError(t, alreadyBuiltErr, "Error building already built indexes")

	emptySetErr := n1qlStore.BuildDeferredIndexes(TestCtx(t), []string{})
	assert.NoError(t, emptySetErr, "Error building empty set")
}

func TestCreateAndDropIndexErrors(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	// Malformed expression
	createExpression := "_sync sequence"
	err := n1qlStore.CreateIndex(ctx, "testIndex_malformed", createExpression, "", testN1qlOptions)
	if err == nil {
		t.Fatalf("Expected error for malformed index expression")
	}

	// Create index
	createExpression = "_sync.`sequence`"
	require.NoError(t, n1qlStore.CreateIndex(ctx, "testIndex_sequence", createExpression, "", testN1qlOptions))

	// Attempt to recreate duplicate index
	err = n1qlStore.CreateIndex(ctx, "testIndex_sequence", createExpression, "", testN1qlOptions)
	if err == nil {
		t.Fatalf("Expected error attempting to recreate already existing index")
	}

	// Drop non-existent index
	err = n1qlStore.DropIndex(ctx, "testIndex_not_found")
	if err == nil {
		t.Fatalf("Expected error attempting to drop non-existent index")
	}

	// Drop the index
	err = n1qlStore.DropIndex(ctx, "testIndex_sequence")
	if err != nil {
		t.Fatalf("Error dropping index: %s", err)
	}

	// Drop index that's already been dropped
	err = n1qlStore.DropIndex(ctx, "testIndex_sequence")
	if err == nil {
		t.Fatalf("Expected error attempting to drop index twice")
	}
}

func TestWaitForBucketExistence(t *testing.T) {
	ctx := TestCtx(t)
	n1qlStore := getN1QLStore(t)

	// Create index
	const (
		indexName        = "index1"
		expression       = "_sync"
		filterExpression = ""
	)
	var options = &N1qlIndexOptions{NumReplica: 0}

	go func() {
		err := n1qlStore.CreateIndex(ctx, indexName, expression, filterExpression, options)
		assert.NoError(t, err, "Index should be created in the bucket")
	}()
	assert.NoError(t, n1qlStore.WaitForIndexesOnline(TestCtx(t), []string{indexName}, WaitForIndexesDefault))

	// Drop the index;
	err := n1qlStore.DropIndex(ctx, indexName)
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

// getN1QLStore returns a N1QLStore. This function will cause a test skip if GSI tests are not enabled. This function uses testing.T.Cleanup to delete all indexes before running the bucket to the bucket pool.
func getN1QLStore(t *testing.T) N1QLStore {
	if TestsDisableGSI() {
		t.Skip("This test requires N1QL on Couchbase Server")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })
	dataStore := bucket.GetSingleDataStore()

	n1qlStore, ok := AsN1QLStore(bucket.GetSingleDataStore())
	require.True(t, ok, "Requires bucket to be N1QLStore, was %T", dataStore)

	t.Cleanup(func() {
		assert.NoError(t, DropAllIndexes(ctx, n1qlStore))
	})
	return n1qlStore
}

/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"log"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestView(t *testing.T) {
	if !TestsDisableGSI() {
		t.Skip("GSI tests are not compatible with views")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()
	viewStore, ok := AsViewStore(dataStore)
	require.True(t, ok)

	ddocName := "testDDoc"
	viewName := "testView"
	// Create design doc and view
	view := sgbucket.ViewDef{
		Map: `function (doc, meta) { emit(doc.type, null); }`,
	}

	ddoc := &sgbucket.DesignDoc{
		Views: map[string]sgbucket.ViewDef{viewName: view},
	}
	err := viewStore.PutDDoc(ctx, ddocName, ddoc)
	require.NoError(t, err)

	defer func() {
		err := viewStore.DeleteDDoc(ddocName)
		if err != nil {
			log.Printf("Error removing design doc during test teardown")
		}
	}()

	// Put test docs
	err = dataStore.Set("u1", 0, nil, map[string]any{"type": "Circle"})
	assert.NoError(t, err)
	err = dataStore.Set("u2", 0, nil, map[string]any{"type": "Northern"})
	assert.NoError(t, err)
	err = dataStore.Set("u3", 0, nil, map[string]any{"type": "District"})
	assert.NoError(t, err)

	// Confirm view availability
	ddocCheck, getErr := viewStore.GetDDoc(ddocName)
	assert.NoError(t, getErr)
	assert.NotNil(t, ddocCheck)

	// wait for view readiness
	worker := func() (shouldRetry bool, err error, value any) {
		viewParams := make(map[string]any)
		_, viewErr := viewStore.View(ctx, ddocName, viewName, viewParams)
		if viewErr == nil {
			return false, nil, nil
		}
		log.Printf("Unexpected error querying view for readiness, retrying: %v", viewErr)
		return true, viewErr, nil
	}

	description := fmt.Sprintf("Wait for view readiness")
	sleeper := CreateSleeperFunc(50, 100)
	viewErr, _ := RetryLoop(ctx, description, worker, sleeper)
	require.NoError(t, viewErr)

	// stale=false
	viewParams := make(map[string]any)
	viewParams[ViewQueryParamStale] = false
	result, viewErr := viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	assert.Len(t, result.Rows, 3)

	// Limit
	viewParams[ViewQueryParamLimit] = 1
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Circle", result.Rows[0].Key)

	// Startkey, Endkey
	delete(viewParams, ViewQueryParamLimit)
	viewParams[ViewQueryParamStartKey] = "District"
	viewParams[ViewQueryParamEndKey] = "Northern"
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	require.Len(t, result.Rows, 2)
	assert.Equal(t, "District", result.Rows[0].Key)
	assert.Equal(t, "Northern", result.Rows[1].Key)

	// InclusiveEnd=false
	viewParams[ViewQueryParamInclusiveEnd] = false
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "District", result.Rows[0].Key)

	// Descending
	viewParams = make(map[string]any) // clear previous
	viewParams[ViewQueryParamDescending] = true
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	require.Len(t, result.Rows, 3)
	assert.Equal(t, "Northern", result.Rows[0].Key)

	// ...and skip (CBS only)
	if !UnitTestUrlIsWalrus() {
		viewParams[ViewQueryParamSkip] = 1
		result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		require.Len(t, result.Rows, 2)
		assert.Equal(t, "District", result.Rows[0].Key)
	}

	// Key and keys
	viewParams = make(map[string]any) // clear previous
	viewParams[ViewQueryParamKey] = "District"
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "District", result.Rows[0].Key)

	viewParams[ViewQueryParamKey] = "Central"
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	require.Len(t, result.Rows, 0)

	delete(viewParams, ViewQueryParamKey)
	viewParams[ViewQueryParamKeys] = []any{"Central", "Circle"}
	result, viewErr = viewStore.View(ctx, ddocName, viewName, viewParams)
	require.NoError(t, viewErr)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Circle", result.Rows[0].Key)

	// ViewCustom

	// ViewQuery, Next
	viewQueryParams := make(map[string]any)
	iterator, viewQueryErr := viewStore.ViewQuery(ctx, ddocName, viewName, viewQueryParams)
	require.NoError(t, viewQueryErr)
	var value any
	rowCount := 0
	for iterator.Next(ctx, &value) {
		rowCount++
	}
	assert.Equal(t, 3, rowCount)
	assert.NoError(t, iterator.Close())

	// ViewQuery, NextBytes
	bytesIterator, viewQueryErr := viewStore.ViewQuery(ctx, ddocName, viewName, viewQueryParams)
	require.NoError(t, viewQueryErr)
	rowCount = 0
	for {
		nextBytes := bytesIterator.NextBytes()
		if nextBytes == nil {
			break
		}
		rowCount++
	}
	assert.Equal(t, 3, rowCount)
	assert.NoError(t, iterator.Close())
}

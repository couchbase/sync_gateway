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
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		ddocName := "testDDoc"
		viewName := "testView"
		//Create design doc and view
		view := sgbucket.ViewDef{
			Map: `function (doc, meta) { emit(doc.type, null); }`,
		}

		ddoc := &sgbucket.DesignDoc{
			Views: map[string]sgbucket.ViewDef{viewName: view},
		}

		err := bucket.PutDDoc(ddocName, ddoc)
		require.NoError(t, err)

		defer func() {
			err := bucket.DeleteDDoc(ddocName)
			if err != nil {
				log.Printf("Error removing design doc during test teardown")
			}
		}()

		// Put test docs
		err = bucket.Set("u1", 0, nil, map[string]interface{}{"type": "Circle"})
		assert.NoError(t, err)
		err = bucket.Set("u2", 0, nil, map[string]interface{}{"type": "Northern"})
		assert.NoError(t, err)
		err = bucket.Set("u3", 0, nil, map[string]interface{}{"type": "District"})
		assert.NoError(t, err)

		// Confirm view availability
		ddocCheck, getErr := bucket.GetDDoc(ddocName)
		assert.NoError(t, getErr)
		assert.NotNil(t, ddocCheck)

		// wait for view readiness
		worker := func() (shouldRetry bool, err error, value interface{}) {
			viewParams := make(map[string]interface{})
			_, viewErr := bucket.View(ddocName, viewName, viewParams)
			if viewErr == nil {
				return false, nil, nil
			}
			log.Printf("Unexpected error querying view for readiness, retrying: %v", viewErr)
			return true, viewErr, nil
		}

		description := fmt.Sprintf("Wait for view readiness")
		sleeper := CreateSleeperFunc(50, 100)
		viewErr, _ := RetryLoop(description, worker, sleeper)
		require.NoError(t, viewErr)

		// stale=false
		viewParams := make(map[string]interface{})
		viewParams[ViewQueryParamStale] = false
		result, viewErr := bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		assert.Equal(t, 3, len(result.Rows))

		// Limit
		viewParams[ViewQueryParamLimit] = 1
		result, viewErr = bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, "Circle", result.Rows[0].Key)

		// Startkey, Endkey
		delete(viewParams, ViewQueryParamLimit)
		viewParams[ViewQueryParamStartKey] = "District"
		viewParams[ViewQueryParamEndKey] = "Northern"
		result, viewErr = bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		require.Equal(t, 2, len(result.Rows))
		assert.Equal(t, "District", result.Rows[0].Key)
		assert.Equal(t, "Northern", result.Rows[1].Key)

		// InclusiveEnd=false
		viewParams[ViewQueryParamInclusiveEnd] = false
		result, viewErr = bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		assert.Equal(t, 1, len(result.Rows))
		assert.Equal(t, "District", result.Rows[0].Key)

		// Descending (CBS only)
		if !UnitTestUrlIsWalrus() {
			viewParams = make(map[string]interface{}) // clear previous
			viewParams[ViewQueryParamDescending] = true
			result, viewErr = bucket.View(ddocName, viewName, viewParams)
			require.NoError(t, viewErr)
			require.Equal(t, 3, len(result.Rows))
			assert.Equal(t, "Northern", result.Rows[0].Key)
		}

		// ...and skip (CBS only)
		if !UnitTestUrlIsWalrus() {
			viewParams[ViewQueryParamSkip] = 1
			result, viewErr = bucket.View(ddocName, viewName, viewParams)
			require.NoError(t, viewErr)
			require.Equal(t, 2, len(result.Rows))
			assert.Equal(t, "District", result.Rows[0].Key)
		}

		// Key and keys
		viewParams = make(map[string]interface{}) // clear previous
		viewParams[ViewQueryParamKey] = "District"
		result, viewErr = bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, "District", result.Rows[0].Key)

		viewParams[ViewQueryParamKey] = "Central"
		result, viewErr = bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		require.Equal(t, 0, len(result.Rows))

		delete(viewParams, ViewQueryParamKey)
		viewParams[ViewQueryParamKeys] = []interface{}{"Central", "Circle"}
		result, viewErr = bucket.View(ddocName, viewName, viewParams)
		require.NoError(t, viewErr)
		require.Equal(t, 1, len(result.Rows))
		assert.Equal(t, "Circle", result.Rows[0].Key)

		// ViewCustom

		// ViewQuery, Next
		viewQueryParams := make(map[string]interface{})
		iterator, viewQueryErr := bucket.ViewQuery(ddocName, viewName, viewQueryParams)
		require.NoError(t, viewQueryErr)
		var value interface{}
		rowCount := 0
		for iterator.Next(&value) {
			rowCount++
		}
		assert.Equal(t, 3, rowCount)
		assert.NoError(t, iterator.Close())

		// ViewQuery, NextBytes
		bytesIterator, viewQueryErr := bucket.ViewQuery(ddocName, viewName, viewQueryParams)
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
	})
}

// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func sendGetCheckpointRequest(bt *BlipTester) {
	t := bt.restTester.TB
	rq := bt.newRequest()
	rq.SetProfile("getCheckpoint")
	require.True(t, bt.sender.Send(rq))
	errorCode, exists := rq.Response().Properties["Error-Code"]
	require.True(t, exists)
	require.Equal(t, "404", errorCode)
}

// waitForStatGreaterThan will retry for up to 20 seconds until the result of getStatFunc is equal to the expected value.
func waitForStatGreaterThan(t *testing.T, getStatFunc func() int64, expected int64) {
	workerFunc := func() (shouldRetry bool, err error, val interface{}) {
		val = getStatFunc()
		stat, ok := val.(int64)
		require.True(t, ok)
		return stat <= expected, nil, val
	}
	// wait for up to 20 seconds for the stat to meet the expected value
	err, val := base.RetryLoop("waitForStatGreaterThan retry loop", workerFunc, base.CreateSleeperFunc(200, 100))
	require.NoError(t, err)
	valInt64, ok := val.(int64)
	require.True(t, ok)
	require.Greater(t, valInt64, expected)
}

func TestBlipStatsBasic(t *testing.T) {
	bt, err := NewBlipTester(t)
	require.NoError(t, err)
	defer bt.Close()

	// make sure requests have not incremented stats.
	/// Note: there is a blip call in NewBlipTester to initialize collections
	dbStats := bt.restTester.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), dbStats.BlipBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.BlipBytesSent.Value())

	// send a request, close BlipSyncContext and make sure stats are incremented
	sendGetCheckpointRequest(bt)

	// requests shouldn't be implemented as part of handler
	require.Equal(t, int64(0), dbStats.BlipBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.BlipBytesSent.Value())

	bt.sender.Close()

	waitForStatGreaterThan(t, dbStats.BlipBytesReceived.Value, 1)
	waitForStatGreaterThan(t, dbStats.BlipBytesSent.Value, 1)

}

func TestBlipStatsFastReport(t *testing.T) {
	bt, err := NewBlipTester(t)
	require.NoError(t, err)
	defer bt.Close()
	sendRequest := func() {
		rq := bt.newRequest()
		rq.SetProfile("getCheckpoint")
		require.True(t, bt.sender.Send(rq))
		errorCode, exists := rq.Response().Properties["Error-Code"]
		require.True(t, exists)
		require.Equal(t, "404", errorCode)
	}

	dbStats := bt.restTester.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), dbStats.BlipBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.BlipBytesSent.Value())

	sendRequest()

	require.Equal(t, int64(0), dbStats.BlipBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.BlipBytesSent.Value())

	// set reporting interval to update stats immediately
	bt.restTester.GetDatabase().BlipStatsReportingInterval = 0
	sendRequest()
	require.Less(t, int64(0), dbStats.BlipBytesReceived.Value())
	require.Less(t, int64(0), dbStats.BlipBytesSent.Value())
}

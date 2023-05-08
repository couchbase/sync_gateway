package rest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlipStatsNoData(t *testing.T) {
	bt, err := NewBlipTester(t)
	require.NoError(t, err)
	defer bt.Close()

	dbStats := bt.restTester.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), dbStats.BlipBytesReceived.Value())
	require.Equal(t, int64(0), dbStats.BlipBytesSent.Value())
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

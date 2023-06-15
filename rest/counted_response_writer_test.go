// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name string
}

var testCases []testCase

func init() {
	testCases = []testCase{
		{
			name: "CountedResponseWriter",
		},
		{
			name: "EncodedResponseWriter",
		},
		{
			name: "EncodedResponseWriterNoGzip",
		},
	}
}

func getResponseWriter(t *testing.T, name string) CountableResponseWriter {
	var writer CountableResponseWriter
	switch name {
	case "CountedResponseWriter":
		writer = NewCountedResponseWriter(httptest.NewRecorder(), 0)
	case "EncodedResponseWriter":
		rq := httptest.NewRequest(http.MethodGet, "/foo/", nil)
		rq.Header.Set("Accept-Encoding", "gzip")
		writer = NewEncodedResponseWriter(httptest.NewRecorder(), rq, 0)
	case "EncodedResponseWriterNoGzip":
		rq := httptest.NewRequest(http.MethodGet, "/foo/", nil)
		rq.Header.Set("Accept-Encoding", "gzip")
		writer = NewEncodedResponseWriter(httptest.NewRecorder(), rq, 0)
		writer.Header().Set("Content-Encoding", "compress")
	default:
		t.Fatalf("Unexpected test case: %s", name)
	}
	require.NotNil(t, writer)
	return writer
}

func TestCountedResponseWriterRestTester(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	const alice = "alice"
	rt.CreateUser(alice, nil)

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/", "")
	RequireStatus(t, resp, http.StatusOK)

	stats := rt.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), stats.HTTPBytesWritten.Value())

	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/", "", alice)
	RequireStatus(t, resp, http.StatusOK)
	require.Greater(t, stats.HTTPBytesWritten.Value(), int64(0))
	stats.HTTPBytesWritten.Set(0)

	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/", "", "")
	RequireStatus(t, resp, http.StatusOK)
	require.Greater(t, stats.HTTPBytesWritten.Value(), int64(0))
}

// TestCountedResponseWriterNoDelay tests that the stats are immediately updated if there is no delay set
func TestCountedResponseWriterNoDelay(t *testing.T) {

	oneByte := []byte("1")

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			stat, err := base.NewIntStat(base.SubsystemDatabaseKey, "http_bytes_written", nil, nil, prometheus.CounterValue, 0)
			require.NoError(t, err)

			responseWriter := getResponseWriter(t, test.name)
			n, err := responseWriter.Write(oneByte)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			// add stats after writer happens
			responseWriter.setStat(stat)
			require.Equal(t, int64(0), stat.Value())
			responseWriter.reportStats(true)
			require.Equal(t, int64(1), stat.Value())

			n, err = responseWriter.Write(oneByte)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Equal(t, int64(2), stat.Value())
		})
	}
}

// TestCountedResponseWriterWithDelay tests that the stats are _not_ written immediately.
func TestCountedResponseWriterWithDelay(t *testing.T) {
	oneByte := []byte("1")
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			stat, err := base.NewIntStat(base.SubsystemDatabaseKey, "http_bytes_written", nil, nil, prometheus.CounterValue, 0)
			require.NoError(t, err)
			responseWriter := NewCountedResponseWriter(httptest.NewRecorder(), 30*time.Second)
			n, err := responseWriter.Write(oneByte)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			// add stats after writer happens
			responseWriter.setStat(stat)
			require.Equal(t, int64(0), stat.Value())
			responseWriter.reportStats(true)
			require.Equal(t, int64(1), stat.Value())

			n, err = responseWriter.Write(oneByte)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Equal(t, int64(1), stat.Value())

			responseWriter.reportStats(true)
			require.Equal(t, int64(2), stat.Value())
		})
	}

}

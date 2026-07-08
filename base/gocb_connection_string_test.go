// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/couchbase/gocbcore/v10/connstr"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestGetGoCBConnStringWithDefaults(t *testing.T) {
	testCases := []struct {
		name    string
		server  string
		connStr string
		params  *GoCBConnStringParams
	}{
		{
			name:    "default, no params",
			server:  "couchbase://127.0.0.1",
			connStr: "couchbase://127.0.0.1?idle_http_connection_timeout=90000&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "default, default params",
			server:  "couchbase://127.0.0.1",
			connStr: "couchbase://127.0.0.1?idle_http_connection_timeout=90000&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "kv_pool_size=8, no params",
			server:  "couchbase://127.0.0.1?kv_pool_size=8",
			connStr: "couchbase://127.0.0.1?idle_http_connection_timeout=90000&kv_pool_size=8&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "kv_pool_size=8, default params",
			server:  "couchbase://127.0.0.1?kv_pool_size=8",
			connStr: "couchbase://127.0.0.1?idle_http_connection_timeout=90000&kv_pool_size=8&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "kv_buffer_size=3, no params",
			server:  "couchbase://127.0.0.1?kv_buffer_size=3",
			connStr: "couchbase://127.0.0.1?idle_http_connection_timeout=90000&kv_buffer_size=3&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "kv_buffer_size=3, default params",
			server:  "couchbase://127.0.0.1?kv_buffer_size=3",
			connStr: "couchbase://127.0.0.1?idle_http_connection_timeout=90000&kv_buffer_size=3&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "dcp_buffer_size=3, no params",
			server:  "couchbase://127.0.0.1?dcp_buffer_size=3",
			connStr: "couchbase://127.0.0.1?dcp_buffer_size=3&idle_http_connection_timeout=90000&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "dcp_buffer_size=3, default params",
			server:  "couchbase://127.0.0.1?dcp_buffer_size=3",
			connStr: "couchbase://127.0.0.1?dcp_buffer_size=3&idle_http_connection_timeout=90000&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			connStr, err := GetGoCBConnStringWithDefaults(testCase.server, testCase.params)
			require.NoError(t, err)
			require.Equal(t, testCase.connStr, connStr)
		})
	}
}

func TestGetConnSpecOption(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		testCases := []struct {
			name          string
			options       map[string][]string
			expected      *int
			expectedError bool
		}{
			{
				name:    "not set",
				options: map[string][]string{},
			},
			{
				name:     "single value",
				options:  map[string][]string{"kv_pool_size": {"8"}},
				expected: Ptr(8),
			},
			{
				name:          "multiple values",
				options:       map[string][]string{"kv_pool_size": {"8", "4"}},
				expectedError: true,
			},
			{
				name:          "non-int value",
				options:       map[string][]string{"kv_pool_size": {"notanint"}},
				expectedError: true,
			},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				spec := &connstr.ConnSpec{Options: testCase.options}
				value, err := getConnSpecOption[int](spec, "kv_pool_size")
				if testCase.expectedError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, testCase.expected, value)
				}
			})
		}
	})

	t.Run("string", func(t *testing.T) {
		testCases := []struct {
			name          string
			options       map[string][]string
			expected      *string
			expectedError bool
		}{
			{
				name:    "not set",
				options: map[string][]string{},
			},
			{
				name:     "single value",
				options:  map[string][]string{networkKey: {"external"}},
				expected: Ptr("external"),
			},
			{
				name:          "multiple values",
				options:       map[string][]string{networkKey: {"external", "default"}},
				expectedError: true,
			},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				spec := &connstr.ConnSpec{Options: testCase.options}
				value, err := getConnSpecOption[string](spec, networkKey)
				if testCase.expectedError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, testCase.expected, value)
				}
			})
		}
	})
}

func TestGetIntFromConnStr(t *testing.T) {
	testCases := []struct {
		name          string
		server        string
		kvPoolSize    *int
		expectedError bool
	}{
		{
			name:   "no kv_pool_size",
			server: "couchbase://127.0.0.1",
		},
		{
			name:          "kv_pool_size=8",
			server:        "couchbase://127.0.0.1?kv_pool_size=8",
			kvPoolSize:    Ptr(8),
			expectedError: false,
		},
		{
			name:          "multiple kv_pool_size",
			server:        "couchbase://127.0.0.1?kv_pool_size=8&kv_pool_size=4",
			expectedError: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			kvPoolSize, err := getIntFromConnStr(testCase.server, kvPoolSizeKey)
			if testCase.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.kvPoolSize, kvPoolSize)
			}
		})
	}
}

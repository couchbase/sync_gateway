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

	"github.com/stretchr/testify/require"
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
			server:  "couchbase://localhost",
			connStr: "couchbase://localhost?idle_http_connection_timeout=90000&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "default, default params",
			server:  "couchbase://localhost",
			connStr: "couchbase://localhost?idle_http_connection_timeout=90000&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "default, serverless params",
			server:  "couchbase://localhost",
			connStr: "couchbase://localhost?dcp_buffer_size=1048576&idle_http_connection_timeout=90000&kv_buffer_size=1048576&kv_pool_size=1&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultServerlessGoCBConnStringParams(),
		},
		{
			name:    "kv_pool_size=8, no params",
			server:  "couchbase://localhost?kv_pool_size=8",
			connStr: "couchbase://localhost?idle_http_connection_timeout=90000&kv_pool_size=8&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "kv_pool_size=8, default params",
			server:  "couchbase://localhost?kv_pool_size=8",
			connStr: "couchbase://localhost?idle_http_connection_timeout=90000&kv_pool_size=8&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "kv_pool_size=8, serverless params",
			server:  "couchbase://localhost?kv_pool_size=8",
			connStr: "couchbase://localhost?dcp_buffer_size=1048576&idle_http_connection_timeout=90000&kv_buffer_size=1048576&kv_pool_size=8&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultServerlessGoCBConnStringParams(),
		},
		{
			name:    "kv_buffer_size=3, no params",
			server:  "couchbase://localhost?kv_buffer_size=3",
			connStr: "couchbase://localhost?idle_http_connection_timeout=90000&kv_buffer_size=3&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "kv_buffer_size=3, default params",
			server:  "couchbase://localhost?kv_buffer_size=3",
			connStr: "couchbase://localhost?idle_http_connection_timeout=90000&kv_buffer_size=3&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "kv_buffer_size=3, serverless params",
			server:  "couchbase://localhost?kv_buffer_size=3",
			connStr: "couchbase://localhost?dcp_buffer_size=1048576&idle_http_connection_timeout=90000&kv_buffer_size=3&kv_pool_size=1&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultServerlessGoCBConnStringParams(),
		},
		{
			name:    "dcp_buffer_size=3, no params",
			server:  "couchbase://localhost?dcp_buffer_size=3",
			connStr: "couchbase://localhost?dcp_buffer_size=3&idle_http_connection_timeout=90000&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name:    "dcp_buffer_size=3, default params",
			server:  "couchbase://localhost?dcp_buffer_size=3",
			connStr: "couchbase://localhost?dcp_buffer_size=3&idle_http_connection_timeout=90000&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultGoCBConnStringParams(),
		},
		{
			name:    "dcp_pool_size=3, serverless params",
			server:  "couchbase://localhost?dcp_buffer_size=3",
			connStr: "couchbase://localhost?dcp_buffer_size=3&idle_http_connection_timeout=90000&kv_buffer_size=1048576&kv_pool_size=1&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			params:  DefaultServerlessGoCBConnStringParams(),
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

func TestGetKvPoolSize(t *testing.T) {
	testCases := []struct {
		name          string
		server        string
		kvPoolSize    int
		expectedError bool
	}{
		{
			name:          "no kv_pool_size",
			server:        "couchbase://localhost",
			expectedError: true,
		},
		{
			name:          "kv_pool_size=8",
			server:        "couchbase://localhost?kv_pool_size=8",
			kvPoolSize:    8,
			expectedError: false,
		},
		{
			name:          "multiple kv_pool_size",
			server:        "couchbase://localhost?kv_pool_size=8&kv_pool_size=4",
			expectedError: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			kvPoolSize, err := GetKvPoolSize(testCase.server)
			if testCase.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.kvPoolSize, *kvPoolSize)
			}
		})
	}
}

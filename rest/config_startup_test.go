// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test merging behaviour with different types orders
func TestStartupConfigMerge(t *testing.T) {
	tests := []struct {
		name     string
		config   StartupConfig
		override StartupConfig
		expected StartupConfig
	}{
		{
			name:     "Override *ConfigDuration",
			config:   StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 5)}},
			override: StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 10)}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override empty *ConfigDuration",
			config:   StartupConfig{},
			override: StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 10)}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Keep original *ConfigDuration",
			config:   StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 10)}},
			override: StartupConfig{},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ConfigUpdateFrequency: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override string",
			config:   StartupConfig{Bootstrap: BootstrapConfig{Server: "test.com"}},
			override: StartupConfig{Bootstrap: BootstrapConfig{Server: "test.net"}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "test.net"}},
		},
		{
			name:     "Override empty string",
			config:   StartupConfig{},
			override: StartupConfig{Bootstrap: BootstrapConfig{Server: "test.net"}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "test.net"}},
		},
		{
			name:     "Keep original string",
			config:   StartupConfig{Bootstrap: BootstrapConfig{Server: "test.net"}},
			override: StartupConfig{},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "test.net"}},
		},
		{
			name:     "Keep original *bool",
			config:   StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(true)}},
			override: StartupConfig{},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(true)}},
		},
		{
			name:     "Override *bool",
			config:   StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(true)}},
			override: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(false)}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(false)}},
		},
		{
			name:     "Override unset *bool",
			config:   StartupConfig{},
			override: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(true)}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.Ptr(true)}},
		},
		{
			name:     "Keep original *ConsoleLoggerConfig",
			config:   StartupConfig{Logging: base.LoggingConfig{Console: &base.ConsoleLoggerConfig{LogKeys: []string{"HTTP", "Config", "CRUD", "DCP", "Sync"}}}},
			override: StartupConfig{Logging: base.LoggingConfig{Console: &base.ConsoleLoggerConfig{}}},
			expected: StartupConfig{Logging: base.LoggingConfig{Console: &base.ConsoleLoggerConfig{LogKeys: []string{"HTTP", "Config", "CRUD", "DCP", "Sync"}}}},
		}, {
			name:     "Override empty logging",
			config:   StartupConfig{Logging: base.LoggingConfig{Trace: &base.FileLoggerConfig{}}},
			override: StartupConfig{Logging: base.LoggingConfig{Trace: &base.FileLoggerConfig{Enabled: base.Ptr(true)}}},
			expected: StartupConfig{Logging: base.LoggingConfig{Trace: &base.FileLoggerConfig{Enabled: base.Ptr(true)}}},
		},
		{
			name:     "Keep original *CORSconfig",
			config:   StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
			override: StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{}}},
			expected: StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
		},
		{
			name:     "Keep original *auth.CORSConfig from override nil value",
			config:   StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
			override: StartupConfig{},
			expected: StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
		},
		{
			name:     "Override unset ConfigDuration",
			config:   StartupConfig{},
			override: StartupConfig{Replicator: ReplicatorConfig{MaxHeartbeat: base.NewConfigDuration(time.Second * 5)}},
			expected: StartupConfig{Replicator: ReplicatorConfig{MaxHeartbeat: base.NewConfigDuration(time.Second * 5)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Merge(&test.override)
			require.NoError(t, err)

			assert.Equal(t, test.expected, test.config)
		})
	}
}

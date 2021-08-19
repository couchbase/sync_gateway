package rest

import (
	"testing"
	"time"

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
			config:   StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(true)}},
			override: StartupConfig{},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(true)}},
		},
		{
			name:     "Override *bool",
			config:   StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(true)}},
			override: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(false)}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(false)}},
		},
		{
			name:     "Override unset *bool",
			config:   StartupConfig{},
			override: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(true)}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{ServerTLSSkipVerify: base.BoolPtr(true)}},
		},
		{
			name:     "Keep original *ConsoleLoggerConfig",
			config:   StartupConfig{Logging: base.LoggingConfig{Console: &base.ConsoleLoggerConfig{LogKeys: []string{"HTTP", "Config", "CRUD", "DCP", "Sync"}}}},
			override: StartupConfig{Logging: base.LoggingConfig{Console: &base.ConsoleLoggerConfig{}}},
			expected: StartupConfig{Logging: base.LoggingConfig{Console: &base.ConsoleLoggerConfig{LogKeys: []string{"HTTP", "Config", "CRUD", "DCP", "Sync"}}}},
		}, {
			name:     "Override empty logging",
			config:   StartupConfig{Logging: base.LoggingConfig{Trace: &base.FileLoggerConfig{}}},
			override: StartupConfig{Logging: base.LoggingConfig{Trace: &base.FileLoggerConfig{Enabled: base.BoolPtr(true)}}},
			expected: StartupConfig{Logging: base.LoggingConfig{Trace: &base.FileLoggerConfig{Enabled: base.BoolPtr(true)}}},
		},
		{
			name:     "Keep original *CORSconfig",
			config:   StartupConfig{API: APIConfig{CORS: &CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
			override: StartupConfig{API: APIConfig{CORS: &CORSConfig{}}},
			expected: StartupConfig{API: APIConfig{CORS: &CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
		},
		{
			name:     "Keep original *CORSConfig from override nil value",
			config:   StartupConfig{API: APIConfig{CORS: &CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
			override: StartupConfig{},
			expected: StartupConfig{API: APIConfig{CORS: &CORSConfig{MaxAge: 5, Origin: []string{"Test"}}}},
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

package rest

import (
	"bytes"
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadServerConfig(t *testing.T) {

	tests := []struct {
		name   string
		config string
		err    string
	}{
		{
			name:   "nil",
			config: ``,
			err:    "EOF",
		},
		{
			name:   "valid empty",
			config: `{}`,
		},
		{
			name:   "valid minimal",
			config: `{"logging": {"console": {"enabled": true}}}`,
		},
		{
			name:   "unknown field",
			config: `{"invalid": true}`,
			err:    `json: unknown field "invalid"`,
		},
		{
			name:   "incorrect type",
			config: `{"logging": true}`,
			err:    `json: cannot unmarshal bool into Go struct field ServerConfig.Logging of type base.LoggingConfig`,
		},
		{
			name:   "invalid JSON",
			config: `{true}`,
			err:    `invalid character 't' looking for beginning of object key string`,
		},
		{
			name:   "sync fn backquotes",
			config: "{\"databases\": {\"db\": {\"sync\": `function(doc, oldDoc) {channel(doc.channels)}`}}}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			buf := bytes.NewBufferString(test.config)
			_, unknownFields, err := readServerConfig(SyncGatewayRunModeNormal, buf)
			if test.err == "" {
				assert.NoError(tt, err, "unexpected error for test config")
			} else {
				if err == nil && unknownFields != nil {
					err = unknownFields
				}
				assert.EqualError(tt, err, test.err, "expecting error for test config")
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		config string
		err    string
	}{
		{
			name:   "Compact Interval too low",
			config: `{"databases": {"db":{"compact_interval_days": 0.039}}}`,
			err:    "compact_interval_days cannot be lower than 0.04",
		},
		{
			name:   "Compact Interval too high",
			config: `{"databases": {"db":{"compact_interval_days": 61}}}`,
			err:    "compact_interval_days cannot be higher than 60",
		},
		{
			name:   "Compact Interval just right",
			config: `{"databases": {"db":{"compact_interval_days": 0.04}}}`,
		},
		{
			name:   "db deprecated shadow",
			config: "{\"databases\": {\"db\": {\"shadow\": {}}}}",
			err:    "Bucket shadowing configuration has been moved to the 'deprecated' section of the config.  Please update your config and retry",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			buf := bytes.NewBufferString(test.config)
			config, _, err := readServerConfig(SyncGatewayRunModeNormal, buf)
			assert.NoError(tt, err)
			errorMessages := config.setupAndValidateDatabases()
			if test.err != "" {
				require.Len(t, errorMessages, 1)
				assert.EqualError(tt, errorMessages[0], test.err)
			} else {
				assert.Nil(t, errorMessages)
			}
		})
	}
}

// TestLoadServerConfigExamples will run LoadServerConfig for configs found under the examples directory.
func TestLoadServerConfigExamples(t *testing.T) {
	const exampleLogDirectory = "../examples/"
	const configSuffix = ".json"

	const enterpriseConfigPrefix = "ee_"
	const accelConfigPrefix = "accel_"

	err := filepath.Walk(exampleLogDirectory, func(configPath string, file os.FileInfo, err error) error {
		assert.NoError(t, err)

		runMode := SyncGatewayRunModeNormal

		// Skip directories or files that aren't configs
		if file.IsDir() || !strings.HasSuffix(file.Name(), configSuffix) {
			return nil
		}

		// Skip EE configs in CE
		if !base.IsEnterpriseEdition() && strings.HasPrefix(file.Name(), enterpriseConfigPrefix) {
			return nil
		}

		// Set the accel run mode for accel configs
		if strings.HasPrefix(file.Name(), accelConfigPrefix) {
			runMode = SyncGatewayRunModeAccel
		}

		t.Run(configPath, func(tt *testing.T) {
			_, _, err := LoadServerConfig(runMode, configPath)
			assert.NoError(tt, err, "unexpected error validating example config")
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestDeprecatedCacheConfig(t *testing.T) {
	// Create new DbConfig
	dbConfig := DbConfig{
		CacheConfig: &CacheConfig{},
	}

	// Set Deprecated Values
	dbConfig.DeprecatedRevCacheSize = base.Uint32Ptr(10)
	dbConfig.CacheConfig.DeprecatedChannelCacheAge = base.IntPtr(10)
	dbConfig.CacheConfig.DeprecatedChannelCacheMinLength = base.IntPtr(10)
	dbConfig.CacheConfig.DeprecatedChannelCacheMaxLength = base.IntPtr(10)
	dbConfig.CacheConfig.DeprecatedEnableStarChannel = base.BoolPtr(true)
	dbConfig.CacheConfig.DeprecatedCacheSkippedSeqMaxWait = base.Uint32Ptr(10)
	dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxNum = base.IntPtr(10)
	dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxWait = base.Uint32Ptr(10)

	// Run Deprecated Fallback
	warnings := dbConfig.deprecatedConfigCacheFallback()

	// Check we have 8 warnings as this is the number of deprecated values we are testing
	assert.Equal(t, 8, len(warnings))

	// Check that the deprecated values have correctly been propagated upto the new config values
	assert.Equal(t, *dbConfig.CacheConfig.RevCacheConfig.Size, uint32(10))
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds, 10)
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.MinLength, 10)
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.MaxLength, 10)
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel, true)
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped, uint32(10))
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending, 10)
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending, uint32(10))

	// Reset DBConfig
	dbConfig = DbConfig{
		CacheConfig: &CacheConfig{
			RevCacheConfig:     &RevCacheConfig{},
			ChannelCacheConfig: &ChannelCacheConfig{},
		},
	}

	// Set A Couple Deprecated Values AND Their New Counterparts
	dbConfig.DeprecatedRevCacheSize = base.Uint32Ptr(10)
	dbConfig.CacheConfig.RevCacheConfig.Size = base.Uint32Ptr(20)
	dbConfig.CacheConfig.DeprecatedEnableStarChannel = base.BoolPtr(false)
	dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel = base.BoolPtr(true)

	// Run Deprecated Fallback
	warnings = dbConfig.deprecatedConfigCacheFallback()

	// Check we have 2 warnings as this is the number of deprecated values we are testing
	assert.Equal(t, 2, len(warnings))

	// Check that the deprecated value has been ignored as the new value is the priority
	assert.Equal(t, *dbConfig.CacheConfig.RevCacheConfig.Size, uint32(20))
	assert.Equal(t, *dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel, true)
}

// Test TLS Version
func TestTLSMinimumVersionSetting(t *testing.T) {
	tests := []struct {
		name        string
		tlsString   string
		expectedTLS uint16
	}{
		{
			name:        `Set TLS 1.0`,
			tlsString:   `tlsv1`,
			expectedTLS: tls.VersionTLS10,
		},
		{
			name:        `Set TLS 1.1`,
			tlsString:   `tlsv1.1`,
			expectedTLS: tls.VersionTLS11,
		},
		{
			name:        `Set TLS 1.2`,
			tlsString:   `tlsv1.2`,
			expectedTLS: tls.VersionTLS12,
		},
		{
			name:        `No TLS set, should default to 1.0`,
			tlsString:   ``,
			expectedTLS: tls.VersionTLS10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			assert.Equal(t, test.expectedTLS, GetTLSVersionFromString(&test.tlsString))
		})
	}

}

func TestAutoImportEnabled(t *testing.T) {
	tests := []struct {
		name        string
		configValue interface{}
		expected    bool
		hasError    bool
	}{
		{
			"default",
			nil,
			base.DefaultAutoImport,
			false,
		},
		{
			"true",
			true,
			true,
			false,
		},
		{
			"false",
			false,
			false,
			false,
		},
		{
			"continuous",
			"continuous",
			true,
			false,
		},
		{
			"unknown",
			"unknown",
			false,
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbConfig := &DbConfig{AutoImport: test.configValue}

			got, err := dbConfig.AutoImportEnabled()
			assert.Equal(t, test.hasError, err != nil, "unexpected error from AutoImportEnabled")
			assert.Equal(t, test.expected, got, "unexpected value from AutoImportEnabled")
		})
	}
}

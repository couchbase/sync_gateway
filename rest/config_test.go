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
		name        string
		config      string
		errStdlib   string
		errJSONIter string
	}{
		{
			name:        "nil",
			config:      ``,
			errStdlib:   "EOF",
			errJSONIter: "EOF",
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
			name:        "unknown field",
			config:      `{"invalid": true}`,
			errStdlib:   `json: unknown field "invalid": unrecognized config value`,
			errJSONIter: `found unknown field: invalid`,
		},
		{
			name:        "incorrect type",
			config:      `{"logging": true}`,
			errStdlib:   `json: cannot unmarshal bool into Go struct field ServerConfig.Logging of type base.LoggingConfig`,
			errJSONIter: `expect { or n, but found t`,
		},
		{
			name:        "invalid JSON",
			config:      `{true}`,
			errStdlib:   `invalid character 't' looking for beginning of object key string`,
			errJSONIter: `expects " or n, but found t`,
		},
		{
			name:   "sync fn backquotes",
			config: "{\"databases\": {\"db\": {\"sync\": `function(doc, oldDoc) {channel(doc.channels)}`}}}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			buf := bytes.NewBufferString(test.config)
			_, err := readServerConfig(buf)

			// stdlib/CE specific error checking
			expectedErr := test.errStdlib
			if !base.UseStdlibJSON && base.IsEnterpriseEdition() {
				// jsoniter specific error checking
				expectedErr = test.errJSONIter
			}

			// If we expected no error, make sure we didn't get one
			if expectedErr == "" {
				require.NoError(tt, err, "unexpected error for test config")
			} else {
				// Otherwise - check the error we got matches what we expected
				require.NotNil(tt, err)
				assert.Contains(tt, err.Error(), expectedErr)
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
			err:    "valid range for compact_interval_days is: 0.04-60",
		},
		{
			name:   "Compact Interval too high",
			config: `{"databases": {"db":{"compact_interval_days": 61}}}`,
			err:    "valid range for compact_interval_days is: 0.04-60",
		},
		{
			name:   "Compact Interval just right",
			config: `{"databases": {"db":{"compact_interval_days": 0.04}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			buf := bytes.NewBufferString(test.config)
			config, err := readServerConfig(buf)
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

func TestConfigValidationDeltaSync(t *testing.T) {
	jsonConfig := `{"databases": {"db": {"delta_sync": {"enabled": true}}}}`

	buf := bytes.NewBufferString(jsonConfig)
	config, err := readServerConfig(buf)
	assert.NoError(t, err)

	errorMessages := config.setupAndValidateDatabases()
	assert.Nil(t, errorMessages)

	require.NotNil(t, config.Databases["db"])
	require.NotNil(t, config.Databases["db"].DeltaSync)
	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].DeltaSync.Enabled)
		assert.True(t, *config.Databases["db"].DeltaSync.Enabled)
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].DeltaSync.Enabled)
	}
}

func TestConfigValidationImportPartitions(t *testing.T) {
	jsonConfig := `{"databases": {"db": {"enable_shared_bucket_access":true, "import_partitions": 32}}}`

	buf := bytes.NewBufferString(jsonConfig)
	config, err := readServerConfig(buf)
	assert.NoError(t, err)

	errorMessages := config.setupAndValidateDatabases()
	assert.Nil(t, errorMessages)

	require.NotNil(t, config.Databases["db"])
	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].ImportPartitions)
		assert.Equal(t, uint16(32), *config.Databases["db"].ImportPartitions)
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].ImportPartitions)
	}
}

func TestConfigValidationCache(t *testing.T) {
	jsonConfig := `{"databases": {"db": {"cache": {"rev_cache": {"size": 0}, "channel_cache": {"max_number": 100, "compact_high_watermark_pct": 95, "compact_low_watermark_pct": 25}}}}}`

	buf := bytes.NewBufferString(jsonConfig)
	config, err := readServerConfig(buf)
	assert.NoError(t, err)

	errorMessages := config.setupAndValidateDatabases()
	assert.Nil(t, errorMessages)

	require.NotNil(t, config.Databases["db"])
	require.NotNil(t, config.Databases["db"].CacheConfig)

	require.NotNil(t, config.Databases["db"].CacheConfig.RevCacheConfig)
	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.RevCacheConfig.Size)
		assert.Equal(t, 0, int(*config.Databases["db"].CacheConfig.RevCacheConfig.Size))
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.RevCacheConfig.Size)
	}

	require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig)
	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.MaxNumber)
		assert.Equal(t, 100, int(*config.Databases["db"].CacheConfig.ChannelCacheConfig.MaxNumber))
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.MaxNumber)
	}

	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.HighWatermarkPercent)
		assert.Equal(t, 95, int(*config.Databases["db"].CacheConfig.ChannelCacheConfig.HighWatermarkPercent))
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.HighWatermarkPercent)
	}

	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.LowWatermarkPercent)
		assert.Equal(t, 25, int(*config.Databases["db"].CacheConfig.ChannelCacheConfig.LowWatermarkPercent))
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.LowWatermarkPercent)
	}
}

func TestConfigValidationImport(t *testing.T) {
	tests := []struct {
		name   string
		config string
		err    string
	}{
		{
			name:   "Import enabled, shared bucket not enabled",
			config: `{"databases": {"db": {"import_docs":true}}}`,
			err:    "Invalid configuration - import_docs enabled, but enable_shared_bucket_access not enabled",
		},
		{
			name:   "Import partitions set, shared bucket not enabled",
			config: `{"databases": {"db": {"import_partitions":32}}}`,
			err:    "Invalid configuration - import_partitions set, but enable_shared_bucket_access not enabled",
		},
		{
			name:   "Import disabled, but partitions set",
			config: `{"databases": {"db": {"enable_shared_bucket_access":true,"import_docs":false,"import_partitions":32}}}`,
			err:    "Invalid configuration - import_partitions set, but import_docs disabled",
		},
		{
			name:   "Too many partitions",
			config: `{"databases": {"db": {"enable_shared_bucket_access":true,"import_partitions":2048}}}`,
			err:    "valid range for import_partitions is: 1-1024",
		},
		{
			name:   "Not enough partitions",
			config: `{"databases": {"db": {"enable_shared_bucket_access":true,"import_partitions":0}}}`,
			err:    "valid range for import_partitions is: 1-1024",
		},
		{
			name:   "Valid partitions",
			config: `{"databases": {"db": {"enable_shared_bucket_access":true,"import_partitions":32}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			buf := bytes.NewBufferString(test.config)
			config, err := readServerConfig(buf)
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

	err := filepath.Walk(exampleLogDirectory, func(configPath string, file os.FileInfo, err error) error {
		assert.NoError(t, err)

		// Skip directories or files that aren't configs
		if file.IsDir() || !strings.HasSuffix(file.Name(), configSuffix) {
			return nil
		}

		// Skip EE configs in CE
		if !base.IsEnterpriseEdition() && strings.HasPrefix(file.Name(), enterpriseConfigPrefix) {
			return nil
		}

		t.Run(configPath, func(tt *testing.T) {
			_, err := LoadServerConfig(configPath)
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

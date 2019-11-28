package rest

import (
	"bytes"
	"crypto/tls"
	"flag"
	"io/ioutil"
	"log"
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
	jsonConfig := `{"databases": {"db": {"enable_shared_bucket_access":true, "import_docs": true, "import_partitions": 32}}}`

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

func TestConfigValidationImportPartitions(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skip("Import partitions config validation is enterprise edition only")
	}

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
				require.Len(tt, errorMessages, 1)
				assert.EqualError(tt, errorMessages[0], test.err)
			} else {
				assert.Nil(tt, errorMessages)
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

func TestMergeWith(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	defaultInterface := "4984"
	adminInterface := "127.0.0.1:4985"
	profileInterface := "127.0.0.1:4985"
	configServer := "remote.config.server:4985/db"
	deploymentID := "DeploymentID1008"
	facebookConfig := FacebookConfig{Register: true}

	corsConfig := &CORSConfig{
		Origin:      []string{"http://example.com", "*", "http://staging.example.com"},
		LoginOrigin: []string{"http://example.com"},
		Headers:     []string{},
		MaxAge:      1728000,
	}

	deprecatedLog := []string{"Admin", "Access", "Auth", "Bucket", "Cache"}

	databases := make(DbConfigMap, 2)
	databases["db3"] = &DbConfig{Name: "db3"}
	databases["db4"] = &DbConfig{Name: "db4"}

	other := &ServerConfig{
		Interface:        &defaultInterface,
		AdminInterface:   &adminInterface,
		ProfileInterface: &profileInterface,
		ConfigServer:     &configServer,
		DeploymentID:     &deploymentID,
		Facebook:         &facebookConfig,
		CORS:             corsConfig,
		DeprecatedLog:    deprecatedLog,
		Pretty:           true,
		Databases:        databases}

	databases = make(DbConfigMap, 2)
	databases["db1"] = &DbConfig{Name: "db1"}
	databases["db2"] = &DbConfig{Name: "db2"}
	self := &ServerConfig{Databases: databases}

	err := self.MergeWith(other)
	assert.NoError(t, err, "No error while merging this server config with other")
	assert.Equal(t, defaultInterface, *self.Interface)
	assert.Equal(t, adminInterface, *self.AdminInterface)
	assert.Equal(t, profileInterface, *self.ProfileInterface)
	assert.Equal(t, configServer, *self.ConfigServer)
	assert.Equal(t, deploymentID, *self.DeploymentID)
	assert.Equal(t, facebookConfig.Register, self.Facebook.Register)

	assert.Equal(t, corsConfig.Headers, self.CORS.Headers)
	assert.Equal(t, corsConfig.Origin, self.CORS.Origin)
	assert.Equal(t, corsConfig.LoginOrigin, self.CORS.LoginOrigin)
	assert.Equal(t, corsConfig.MaxAge, self.CORS.MaxAge)
	assert.Equal(t, deprecatedLog, self.DeprecatedLog)
	assert.True(t, self.Pretty)

	assert.Len(t, self.Databases, 4)
	assert.Equal(t, "db1", self.Databases["db1"].Name)
	assert.Equal(t, "db2", self.Databases["db2"].Name)
	assert.Equal(t, "db3", self.Databases["db3"].Name)
	assert.Equal(t, "db4", self.Databases["db4"].Name)

	// Merge configuration with already specified database; it should throw
	// database "db3" already specified earlier error
	databases = make(DbConfigMap, 2)
	databases["db3"] = &DbConfig{Name: "db3"}
	databases["db5"] = &DbConfig{Name: "db5"}
	err = self.MergeWith(other)
	assert.Error(t, err, "Database 'db3' already specified earlier")
}

func TestDeprecatedConfigLoggingFallback(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	logFilePath := "/var/log/sync_gateway"
	dlfPath := "/tmp/log/sync_gateway"
	logKeys := []string{"Admin", "Access", "Auth", "Bucket", "Cache"}
	deprecatedLog := []string{"Admin", "Access", "Auth", "Bucket", "Cache"}

	ddl := &base.LogAppenderConfig{LogFilePath: &logFilePath, LogKeys: logKeys, LogLevel: base.PanicLevel}
	lc := &base.LoggingConfig{DeprecatedDefaultLog: ddl}
	sc := &ServerConfig{Logging: lc, DeprecatedLogFilePath: &dlfPath, DeprecatedLog: deprecatedLog}

	warns := sc.deprecatedConfigLoggingFallback()
	assert.Equal(t, filepath.Dir(*sc.Logging.DeprecatedDefaultLog.LogFilePath), sc.Logging.LogFilePath)
	assert.Equal(t, sc.Logging.DeprecatedDefaultLog.LogKeys, sc.Logging.Console.LogKeys)
	assert.Equal(t, base.ToLogLevel(sc.Logging.DeprecatedDefaultLog.LogLevel), sc.Logging.Console.LogLevel)
	assert.Equal(t, sc.DeprecatedLog, sc.Logging.Console.LogKeys)
	assert.Len(t, warns, 3)

	// Call deprecatedConfigLoggingFallback without DeprecatedDefaultLog
	sc = &ServerConfig{Logging: &base.LoggingConfig{}, DeprecatedLogFilePath: &dlfPath, DeprecatedLog: deprecatedLog}
	warns = sc.deprecatedConfigLoggingFallback()

	assert.Equal(t, *sc.DeprecatedLogFilePath, sc.Logging.LogFilePath)
	assert.Equal(t, sc.DeprecatedLog, sc.Logging.Console.LogKeys)
	assert.Len(t, warns, 2)
}

func TestSetupAndValidateLogging(t *testing.T) {
	t.Skip("Skipping TestSetupAndValidateLogging")
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	sc := &ServerConfig{}
	warns, err := sc.SetupAndValidateLogging()
	assert.NoError(t, err, "Setup and validate logging should be successful")
	assert.NotEmpty(t, sc.Logging)
	assert.Empty(t, sc.Logging.DeprecatedDefaultLog)
	assert.Len(t, warns, 2)
}

func TestSetupAndValidateLoggingWithLoggingConfig(t *testing.T) {
	t.Skip("Skipping TestSetupAndValidateLoggingWithLoggingConfig")
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	logFilePath := "/var/log/sync_gateway"
	logKeys := []string{"Admin", "Access", "Auth", "Bucket", "Cache"}
	ddl := &base.LogAppenderConfig{LogFilePath: &logFilePath, LogKeys: logKeys, LogLevel: base.PanicLevel}
	lc := &base.LoggingConfig{DeprecatedDefaultLog: ddl, RedactionLevel: base.RedactFull}
	sc := &ServerConfig{Logging: lc}
	warns, err := sc.SetupAndValidateLogging()
	assert.NoError(t, err, "Setup and validate logging should be successful")
	assert.Len(t, warns, 5)
	assert.Equal(t, base.RedactFull, sc.Logging.RedactionLevel)
	assert.Equal(t, ddl, sc.Logging.DeprecatedDefaultLog)
}

func TestServerConfigValidate(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	// unsupported.stats_log_freq_secs
	statsLogFrequencySecs := uint(9)
	unsupported := &UnsupportedServerConfig{StatsLogFrequencySecs: &statsLogFrequencySecs}
	sc := &ServerConfig{Unsupported: unsupported}
	assert.Len(t, sc.validate(), 1)

	// Valid configuration value for StatsLogFrequencySecs
	statsLogFrequencySecs = uint(10)
	unsupported = &UnsupportedServerConfig{StatsLogFrequencySecs: &statsLogFrequencySecs}
	sc = &ServerConfig{Unsupported: unsupported}
	assert.Len(t, sc.validate(), 0)

	// Explicitly disabled
	statsLogFrequencySecs = uint(0)
	unsupported = &UnsupportedServerConfig{StatsLogFrequencySecs: &statsLogFrequencySecs}
	sc = &ServerConfig{Unsupported: unsupported}
	assert.Len(t, sc.validate(), 0)
}

func TestSetupAndValidateDatabases(t *testing.T) {
	// No error will be returned if the server config itself is nil
	var sc *ServerConfig
	errs := sc.setupAndValidateDatabases()
	assert.Nil(t, errs)

	// Simulate  invalid control character in URL while validating and setting up databases;
	server := "walrus:\n\r"
	bc := &BucketConfig{Server: &server}
	databases := make(DbConfigMap, 2)
	databases["db1"] = &DbConfig{Name: "db1", BucketConfig: *bc}

	sc = &ServerConfig{Databases: databases}
	errs = sc.setupAndValidateDatabases()
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "invalid control character in URL")
}

func TestParseCommandLine(t *testing.T) {
	var (
		adminInterface     = "127.0.0.1:4985"
		bucket             = "sync_gateway"
		cacertpath         = "/etc/ssl/certs/ca.cert"
		certpath           = "/etc/ssl/certs/client.pem"
		configServer       = "http://127.0.0.1:4981/conf"
		dbname             = "beer_sample"
		defaultLogFilePath = "/var/log/sync_gateway"
		deploymentID       = "DEPID100"
		interfaceAddress   = "4984"
		keypath            = "/etc/ssl/certs/key.pem"
		logKeys            = "Admin,Access,Auth,Bucket"
		logFilePath        = "/var/log/sync_gateway"
		pool               = "liverpool"
	)
	args := []string{
		"sync_gateway",
		"--adminInterface", adminInterface,
		"--bucket", bucket,
		"--cacertpath", cacertpath,
		"--certpath", certpath,
		"--configServer", configServer,
		"--dbname", dbname,
		"--defaultLogFilePath", defaultLogFilePath,
		"--deploymentID", deploymentID,
		"--interface", interfaceAddress,
		"--keypath", keypath,
		"--log", logKeys,
		"--logFilePath", logFilePath,
		"--pool", pool,
		"--pretty"}

	config, err := ParseCommandLine(args, flag.ContinueOnError)
	require.NoError(t, err, "Parsing commandline arguments without any config file")
	assert.Equal(t, interfaceAddress, *config.Interface)
	assert.Equal(t, adminInterface, *config.AdminInterface)
	assert.Empty(t, *config.ProfileInterface)
	assert.True(t, config.Pretty)
	log.Printf("config: %v", config)
	databases := config.Databases
	assert.Len(t, databases, 1)
	log.Printf("databases[dbname]: %v", databases[dbname])
	assert.Equal(t, dbname, databases[dbname].Name)
	assert.Equal(t, bucket, *databases[dbname].Bucket)
	assert.Equal(t, pool, *databases[dbname].Pool)
	assert.Equal(t, cacertpath, databases[dbname].CACertPath)
	assert.Equal(t, certpath, databases[dbname].CertPath)
	assert.Equal(t, keypath, databases[dbname].KeyPath)
}

func TestGetCredentialsFromDbConfig(t *testing.T) {
	bucket := "albums"
	config := BucketConfig{Bucket: &bucket, Username: "Alice", Password: "QWxpY2U="}
	dbConfig := &DbConfig{BucketConfig: config}
	username, password, bucket := dbConfig.GetCredentials()
	assert.Equal(t, config.Username, username)
	assert.Equal(t, config.Password, password)
	assert.Equal(t, *config.Bucket, bucket)
}

func TestGetCredentialsFromClusterConfig(t *testing.T) {
	bucket := "albums"
	config := BucketConfig{Bucket: &bucket, Username: "Alice", Password: "QWxpY2U="}
	heartbeatIntervalSeconds := uint16(10)
	clusterConfig := &ClusterConfig{
		BucketConfig:             config,
		DataDir:                  "/var/lib/sync_gateway/data",
		HeartbeatIntervalSeconds: &heartbeatIntervalSeconds,
	}
	username, password, bucket := clusterConfig.GetCredentials()
	assert.Equal(t, config.Username, username)
	assert.Equal(t, config.Password, password)
	assert.Equal(t, *config.Bucket, bucket)
}

func TestSetMaxFileDescriptors(t *testing.T) {
	var maxFDs *uint64
	err := SetMaxFileDescriptors(maxFDs)
	assert.NoError(t, err, "Sets file descriptor limit to default when requested soft limit is nil")

	// Set MaxFileDescriptors
	maxFDsHigher := DefaultMaxFileDescriptors + 1
	err = SetMaxFileDescriptors(&maxFDsHigher)
	assert.NoError(t, err, "Error setting MaxFileDescriptors")
}

func TestParseCommandLineWithMissingConfig(t *testing.T) {
	log.Printf("TestParseCommandLineWithMissingConfig:%v", GetConfig())
	// Parse command line options with unknown sync gateway configuration file
	args := []string{"sync_gateway", "missing-sync-gateway.conf"}
	config, err := ParseCommandLine(args, flag.ContinueOnError)
	require.Error(t, err, "Trying to read configuration file which doesn't exist")
	assert.Nil(t, config)
}

func TestParseCommandLineWithBadConfigContent(t *testing.T) {
	content := `{"adminInterface":"127.0.0.1:4985","interface":"0.0.0.0:4984",
    	"databases":{"db":{"unknown_field":"walrus:data","users":{"GUEST":{"disabled":false,
		"admin_channels":["*"]}}, "allow_conflicts":false,"revs_limit":20}}}`

	configFilePath := os.TempDir() + "sync_gateway.conf"
	err := ioutil.WriteFile(configFilePath, []byte(content), 0644)
	assert.NoError(t, err, "Writing JSON content")

	defer func() {
		err := os.Remove(configFilePath)
		assert.NoError(t, err)
	}()

	args := []string{"sync_gateway", configFilePath}
	config, err := ParseCommandLine(args, flag.ContinueOnError)
	require.Error(t, err, "Parsing configuration file with an unknown field")
	assert.NotNil(t, config)
}

func TestParseCommandLineWithConfigContent(t *testing.T) {
	content := `{"logging":{"log_file_path":"/var/tmp/sglogs","console":{"log_level":"debug","log_keys":["*"]},
		"error":{"enabled":true,"rotation":{"max_size":20,"max_age":180}},"warn":{"enabled":true,"rotation":{
        "max_size":20,"max_age":90}},"info":{"enabled":false},"debug":{"enabled":false}},"databases":{"db1":{
        "server":"couchbase://localhost","username":"username","password":"password","bucket":"default","pool":"pool1",
        "certpath":"/etc/ssl/certs/cert.pem","cacertpath":"/etc/ssl/certs/ca.cert","keypath":"/etc/ssl/certs/key.pem",
        "users":{"GUEST":{"disabled":false,"admin_channels":["*"]}},"allow_conflicts":false,"revs_limit":20}}}`

	configFilePath := os.TempDir() + "sync_gateway1.conf"
	err := ioutil.WriteFile(configFilePath, []byte(content), 0644)
	assert.NoError(t, err, "Writing JSON content")

	defer func() {
		err := os.Remove(configFilePath)
		assert.NoError(t, err)
	}()

	var (
		adminInterface     = "127.10.0.1:4985"
		profileInterface   = "127.10.0.1:8088"
		bucket             = "sync_gateway"
		cacertpath         = "/etc/ssl/certs/ca.cert"
		certpath           = "/etc/ssl/certs/client.pem"
		configServer       = "http://127.0.0.1:4981/conf"
		defaultLogFilePath = "/var/log/sync_gateway"
		deploymentID       = "DEPID100"
		interfaceAddress   = "4443"
		keypath            = "/etc/ssl/certs/key.pem"
		logKeys            = "Admin,Access,Auth,Bucket"
		logFilePath        = "/var/log/sync_gateway"
		pool               = "liverpool"
	)
	args := []string{
		"sync_gateway",
		"--adminInterface", adminInterface,
		"--bucket", bucket,
		"--cacertpath", cacertpath,
		"--certpath", certpath,
		"--configServer", configServer,
		"--defaultLogFilePath", defaultLogFilePath,
		"--deploymentID", deploymentID,
		"--interface", interfaceAddress,
		"--keypath", keypath,
		"--log", logKeys,
		"--logFilePath", logFilePath,
		"--pool", pool,
		"--pretty",
		"--verbose",
		"--profileInterface", profileInterface,
		configFilePath}

	config, err := ParseCommandLine(args, flag.ContinueOnError)
	require.NoError(t, err, "while parsing commandline options")
	assert.Equal(t, interfaceAddress, *config.Interface)
	assert.Equal(t, adminInterface, *config.AdminInterface)
	assert.Equal(t, profileInterface, *config.ProfileInterface)
	assert.Equal(t, configServer, *config.ConfigServer)
	assert.Equal(t, deploymentID, *config.DeploymentID)
	assert.Equal(t, logFilePath, config.Logging.LogFilePath)
	assert.Equal(t, []string{"Admin", "Access", "Auth", "Bucket", "HTTP+"}, config.Logging.Console.LogKeys)
	assert.True(t, config.Pretty)
	assert.Len(t, config.Databases, 1)

	db1 := config.Databases["db1"]
	assert.Equal(t, "default", *db1.Bucket)
	assert.Equal(t, "username", db1.BucketConfig.Username)
	assert.Equal(t, "password", db1.BucketConfig.Password)
	assert.Equal(t, "default", *db1.BucketConfig.Bucket)
	assert.Equal(t, "couchbase://localhost", *db1.BucketConfig.Server)
	assert.Equal(t, "pool1", *db1.BucketConfig.Pool)
	assert.Equal(t, "/etc/ssl/certs/cert.pem", db1.BucketConfig.CertPath)
	assert.Equal(t, "/etc/ssl/certs/ca.cert", db1.BucketConfig.CACertPath)
	assert.Equal(t, "/etc/ssl/certs/key.pem", db1.BucketConfig.KeyPath)

	guest := db1.Users["GUEST"]
	assert.False(t, guest.Disabled)
	assert.Equal(t, base.SetFromArray([]string{"*"}), guest.ExplicitChannels)
}

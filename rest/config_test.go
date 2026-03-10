/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"golang.org/x/crypto/bcrypt"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
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
			errStdlib:   `json: unknown field "invalid": unrecognized JSON field`,
			errJSONIter: `found unknown field: invalid`,
		},
		{
			name:        "incorrect type",
			config:      `{"logging": true}`,
			errStdlib:   `json: cannot unmarshal bool into Go struct field LegacyServerConfig.Logging of type base.LegacyLoggingConfig`,
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
			_, err := readLegacyServerConfig(base.TestCtx(t), buf)

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
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			buf := bytes.NewBufferString(test.config)
			config, err := readLegacyServerConfig(ctx, buf)
			assert.NoError(t, err)
			errorMessages := config.setupAndValidateDatabases(ctx)
			if test.err != "" {
				require.NotNil(t, errorMessages)
				multiError, ok := errorMessages.(*base.MultiError)
				require.Truef(t, ok, "expected multiError but got: %v", errorMessages)
				require.Equal(t, multiError.Len(), 1)
				assert.EqualError(t, multiError.Errors[0], test.err)
			} else {
				assert.Nil(t, errorMessages)
			}
		})
	}
}

func TestConfigValidationDeltaSync(t *testing.T) {
	jsonConfig := `{"databases": {"db": {"delta_sync": {"enabled": true}}}}`
	ctx := base.TestCtx(t)
	buf := bytes.NewBufferString(jsonConfig)
	config, err := readLegacyServerConfig(ctx, buf)
	assert.NoError(t, err)

	errorMessages := config.setupAndValidateDatabases(ctx)
	require.NoError(t, errorMessages)

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
	ctx := base.TestCtx(t)
	buf := bytes.NewBufferString(jsonConfig)
	config, err := readLegacyServerConfig(ctx, buf)
	assert.NoError(t, err)

	errorMessages := config.setupAndValidateDatabases(ctx)
	require.NoError(t, errorMessages)

	require.NotNil(t, config.Databases["db"])
	require.NotNil(t, config.Databases["db"].CacheConfig)

	require.NotNil(t, config.Databases["db"].CacheConfig.RevCacheConfig)
	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.RevCacheConfig.MaxItemCount)
		assert.Equal(t, 0, int(*config.Databases["db"].CacheConfig.RevCacheConfig.MaxItemCount))
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.RevCacheConfig.MaxItemCount)
	}

	require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig)
	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.MaxNumber)
		assert.Equal(t, 100, *config.Databases["db"].CacheConfig.ChannelCacheConfig.MaxNumber)
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.MaxNumber)
	}

	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.HighWatermarkPercent)
		assert.Equal(t, 95, *config.Databases["db"].CacheConfig.ChannelCacheConfig.HighWatermarkPercent)
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.HighWatermarkPercent)
	}

	if base.IsEnterpriseEdition() {
		require.NotNil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.LowWatermarkPercent)
		assert.Equal(t, 25, *config.Databases["db"].CacheConfig.ChannelCacheConfig.LowWatermarkPercent)
	} else {
		// CE disallowed - should be nil
		assert.Nil(t, config.Databases["db"].CacheConfig.ChannelCacheConfig.LowWatermarkPercent)
	}
}

func TestConfigValidationImport(t *testing.T) {
	jsonConfig := `{"databases": {"db": {"enable_shared_bucket_access":true, "import_docs": true, "import_partitions": 32}}}`
	ctx := base.TestCtx(t)
	buf := bytes.NewBufferString(jsonConfig)
	config, err := readLegacyServerConfig(ctx, buf)
	assert.NoError(t, err)

	errorMessages := config.setupAndValidateDatabases(ctx)
	require.NoError(t, errorMessages)
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
			name:   "Import enabled, shared bucket disabled",
			config: `{"databases": {"db": {"import_docs":true, "enable_shared_bucket_access":false}}}`,
			err:    "Invalid configuration - import_docs enabled, but enable_shared_bucket_access not enabled",
		},
		{
			name:   "Import partitions set, shared bucket disabled",
			config: `{"databases": {"db": {"import_partitions":32, "enable_shared_bucket_access":false}}}`,
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
		{
			name:   "Valid partitions, enable_shared_bucket_access default value (true)",
			config: `{"databases": {"db": {"import_partitions":32}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			buf := bytes.NewBufferString(test.config)
			config, err := readLegacyServerConfig(ctx, buf)
			assert.NoError(t, err)
			errorMessages := config.setupAndValidateDatabases(ctx)
			if test.err != "" {
				require.NotNil(t, errorMessages)
				multiError, ok := errorMessages.(*base.MultiError)
				require.True(t, ok)
				require.Equal(t, multiError.Len(), 1)
				assert.EqualError(t, multiError.Errors[0], test.err)
			} else {
				assert.NoError(t, errorMessages)
			}
		})
	}
}

func generateTestJWK(t *testing.T, alg, kid string, key any) string {
	jwk := jose.JSONWebKey{
		Algorithm: alg,
		Key:       key,
		KeyID:     kid,
	}
	keyJSON, err := jwk.MarshalJSON()
	require.NoError(t, err, "failed to marshal test key")
	return string(keyJSON)
}

func TestConfigValidationJWTAndOIDC(t *testing.T) {
	rsaPrivkey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecdsaPrivkey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	testRSA256JWK := generateTestJWK(t, "RS256", "rsa", rsaPrivkey.Public())
	testEC256JWK := generateTestJWK(t, "ES256", "ec", ecdsaPrivkey.Public())
	testRSA256JWKWithoutKID := generateTestJWK(t, "RS256", "", rsaPrivkey.Public())
	testPrivateRSA256JWK := generateTestJWK(t, "RS256", "rsa-priv", rsaPrivkey)

	cases := []struct {
		name          string
		configJSON    string
		expectedError string
		validateOIDC  bool
	}{
		{
			name:       "Local JWT: ok",
			configJSON: `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "foo", "keys": [` + testRSA256JWK + `], "algorithms": ["RS256"] }}}`,
		},
		{
			name:       "Local JWT: ok - mixed algorithms",
			configJSON: `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testRSA256JWK + "," + testEC256JWK + ` ], "algorithms": ["RS256", "ES256"] }}}`,
		},
		{
			name:          "Local JWT: no issuer",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "client_id": "foo", "keys": [] } }}`,
			expectedError: "Issuer required for Local JWT provider test",
		},
		{
			name:          "Local JWT: no client ID",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "keys": [] } }}`,
			expectedError: "Client ID required for Local JWT provider test",
		},
		{
			name:       "Local JWT: empty client ID (valid)",
			configJSON: `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testRSA256JWK + `], "algorithms": ["RS256"] } }}`,
		},
		{
			name:          "Local JWT: no keys",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "algorithms": ["RS256"] } }}`,
			expectedError: `either 'keys' or 'jwks_uri' must be specified for Local JWT provider test`,
		},
		{
			name:          "Local JWT: private key",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testPrivateRSA256JWK + `], "algorithms": ["RS256"] } }}`,
			expectedError: `key "rsa-priv" is not a public key`,
		},
		{
			name:          "Local JWT: multiple keys without KID",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testRSA256JWKWithoutKID + ", " + testRSA256JWKWithoutKID + `], "algorithms": ["RS256"] } }}`,
			expectedError: "'kid' property required on all keys when more than one key is defined",
		},
		{
			name:          "Local JWT: invalid algo",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testRSA256JWK + `], "algorithms": ["asdf"] } }}`,
			expectedError: `signing algorithm "asdf" invalid or unsupported`,
		},
		{
			name:          "Local JWT: unsupported algos (HS256)",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testRSA256JWK + `], "algorithms": ["HS256"] } }}`,
			expectedError: `signing algorithm "HS256" invalid or unsupported`,
		},
		{
			name:          "Local JWT: unsupported algos (none)",
			configJSON:    `{"name": "test", "local_jwt": { "test": { "issuer": "test", "client_id": "", "keys": [` + testRSA256JWK + `], "algorithms": ["none"] } }}`,
			expectedError: `signing algorithm "none" invalid or unsupported`,
		},
		{
			name:          "OIDC: no providers",
			configJSON:    `{"name": "test", "oidc": {"providers": {}}}`,
			expectedError: "OpenID Connect defined in config, but no valid providers specified",
		},
		{
			name:          "OIDC: no client ID",
			configJSON:    `{"name": "test", "oidc": {"providers": { "test": {"issuer": "test", "client_id": ""} }}}`,
			expectedError: "OpenID Connect defined in config, but no valid providers specified",
		},
		{
			name:          "OIDC: illegal name",
			configJSON:    `{"name": "test", "oidc": {"providers": { "invalid_test": {"issuer": "test", "client_id": "test"} }}}`,
			expectedError: "OpenID Connect provider names cannot contain underscore",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var dbConfig DatabaseConfig
			err := base.JSONUnmarshal([]byte(tc.configJSON), &dbConfig)
			if tc.expectedError == "" {
				require.NoError(t, err, "failed to unmarshal valid startupConfig")
			} else if err != nil {
				if strings.Contains(err.Error(), tc.expectedError) {
					return
				}
				t.Fatalf("received unexpected unmarshaling error: %v", err)
			}

			validateReplications := true
			err = dbConfig.validate(base.TestCtx(t), tc.validateOIDC, validateReplications)
			switch {
			case tc.expectedError == "":
				require.NoError(t, err, "failed to validate valid startupConfig")
			case err != nil:
				if strings.Contains(err.Error(), tc.expectedError) {
					return
				}
				t.Fatalf("received unexpected validation error: %v", err)
			default:
				t.Fatal("expected error, but did not get one")
			}
		})
	}
}

// TestLoadServerConfigExamples will run LoadLegacyServerConfig for configs found under the legacy examples directory.
func TestLoadServerConfigExamples(t *testing.T) {
	const exampleLogDirectory = "../examples/legacy_config"
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
			_, err := LoadLegacyServerConfig(base.TestCtx(t), configPath)
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
	dbConfig.DeprecatedRevCacheSize = base.Ptr(uint32(10))
	dbConfig.CacheConfig.DeprecatedChannelCacheAge = base.Ptr(10)
	dbConfig.CacheConfig.DeprecatedChannelCacheMinLength = base.Ptr(10)
	dbConfig.CacheConfig.DeprecatedChannelCacheMaxLength = base.Ptr(10)
	dbConfig.CacheConfig.DeprecatedEnableStarChannel = base.Ptr(true)
	dbConfig.CacheConfig.DeprecatedCacheSkippedSeqMaxWait = base.Ptr(uint32(10))
	dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxNum = base.Ptr(10)
	dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxWait = base.Ptr(uint32(10))

	// Run Deprecated Fallback
	warnings := dbConfig.deprecatedConfigCacheFallback()

	// Check we have 8 warnings as this is the number of deprecated values we are testing
	require.Len(t, warnings, 8)

	// Check that the deprecated values have correctly been propagated upto the new config values
	assert.Equal(t, *dbConfig.CacheConfig.RevCacheConfig.MaxItemCount, uint32(10))
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
	dbConfig.DeprecatedRevCacheSize = base.Ptr(uint32(10))
	dbConfig.CacheConfig.RevCacheConfig.MaxItemCount = base.Ptr(uint32(20))
	dbConfig.CacheConfig.DeprecatedEnableStarChannel = base.Ptr(false)
	dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel = base.Ptr(true)

	// Run Deprecated Fallback
	warnings = dbConfig.deprecatedConfigCacheFallback()

	// Check we have 2 warnings as this is the number of deprecated values we are testing
	require.Len(t, warnings, 2)

	// Check that the deprecated value has been ignored as the new value is the priority
	assert.Equal(t, *dbConfig.CacheConfig.RevCacheConfig.MaxItemCount, uint32(20))
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
			expectedTLS: tls.VersionTLS12,
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
		configValue any
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

			got, err := dbConfig.AutoImportEnabled(base.TestCtx(t))
			assert.Equal(t, test.hasError, err != nil, "unexpected error from AutoImportEnabled")
			assert.Equal(t, test.expected, got, "unexpected value from AutoImportEnabled")
		})
	}
}

func TestMergeWith(t *testing.T) {
	defaultInterface := "4984"
	adminInterface := "127.0.0.1:4985"
	profileInterface := "127.0.0.1:4985"
	configServer := "remote.config.server:4985/db"
	deploymentID := "DeploymentID1008"
	facebookConfig := FacebookConfigLegacy{Register: true}

	corsConfig := &CORSConfigLegacy{
		Origin:      []string{"http://example.com", "*", "http://staging.example.com"},
		LoginOrigin: []string{"http://example.com"},
		Headers:     []string{},
		MaxAge:      1728000,
	}

	deprecatedLog := []string{"Admin", "Access", "Auth", "Bucket", "Cache"}

	databases := make(DbConfigMap, 2)
	databases["db3"] = &DbConfig{Name: "db3"}
	databases["db4"] = &DbConfig{Name: "db4"}

	other := &LegacyServerConfig{
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
	self := &LegacyServerConfig{Databases: databases}

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

func TestSetupAndValidateLogging(t *testing.T) {
	t.Skip("Skipping TestSetupAndValidateLogging")
	sc := &StartupConfig{}
	err := sc.SetupAndValidateLogging(base.TestCtx(t))
	assert.NoError(t, err, "Setup and validate logging should be successful")
	assert.NotEmpty(t, sc.Logging)
}

func TestSetupAndValidateLoggingWithLoggingConfig(t *testing.T) {
	t.Skip("Skipping TestSetupAndValidateLoggingWithLoggingConfig")
	logFilePath := "/var/log/sync_gateway"
	sc := &StartupConfig{Logging: base.LoggingConfig{LogFilePath: logFilePath, RedactionLevel: base.RedactFull}}
	err := sc.SetupAndValidateLogging(base.TestCtx(t))
	assert.NoError(t, err, "Setup and validate logging should be successful")
	assert.Equal(t, base.RedactFull, sc.Logging.RedactionLevel)
}

func TestAuditLogConfigDatabaseEventInGlobal(t *testing.T) {
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()

	sc := NewEmptyStartupConfig()
	sc.Logging.LogFilePath = t.TempDir()
	sc.Logging.Audit.Enabled = base.Ptr(true)
	sc.Logging.Audit.EnabledEvents = []uint{
		uint(base.AuditIDSyncGatewayStartup),          // global, non-filterable
		uint(base.AuditIDUserCreate),                  // database, filterable
		uint(base.AuditIDSyncGatewayCollectInfoStart), // global, filterable
	}

	err := sc.SetupAndValidateLogging(base.TestCtx(t))
	assert.ErrorContains(t, err, "1 errors")
	assert.ErrorContains(t, err, "audit event ID 54100 \"Create user\" can only be configured at the database level")
}

func TestServerConfigValidate(t *testing.T) {
	// unsupported.stats_log_freq_secs
	statsLogFrequencySecs := uint(9)
	unsupported := &UnsupportedServerConfigLegacy{StatsLogFrequencySecs: &statsLogFrequencySecs}
	sc := &LegacyServerConfig{Unsupported: unsupported}
	validationErrors := sc.validate()
	require.NotNil(t, validationErrors)
	multiError, ok := validationErrors.(*base.MultiError)
	require.True(t, ok)
	require.Equal(t, multiError.Len(), 1)
	assert.Contains(t, multiError.Errors[0].Error(), "minimum value for unsupported.stats_log_freq_secs")

	// Valid configuration value for StatsLogFrequencySecs
	statsLogFrequencySecs = uint(10)
	unsupported = &UnsupportedServerConfigLegacy{StatsLogFrequencySecs: &statsLogFrequencySecs}
	sc = &LegacyServerConfig{Unsupported: unsupported}
	assert.Nil(t, sc.validate())

	// Explicitly disabled
	statsLogFrequencySecs = uint(0)
	unsupported = &UnsupportedServerConfigLegacy{StatsLogFrequencySecs: &statsLogFrequencySecs}
	sc = &LegacyServerConfig{Unsupported: unsupported}
	assert.Nil(t, sc.validate())
}

func TestSetupAndValidateDatabases(t *testing.T) {
	ctx := base.TestCtx(t)
	// No error will be returned if the server config itself is nil
	var sc *LegacyServerConfig
	errs := sc.setupAndValidateDatabases(ctx)
	assert.Nil(t, errs)

	// Simulate  invalid control character in URL while validating and setting up databases;
	server := "walrus:\n\r"
	bc := &BucketConfig{Server: &server}
	databases := make(DbConfigMap, 2)
	databases["db1"] = &DbConfig{Name: "db1", BucketConfig: *bc}

	sc = &LegacyServerConfig{Databases: databases}
	validationError := sc.setupAndValidateDatabases(ctx)
	require.NotNil(t, validationError)
	assert.Contains(t, validationError.Error(), "invalid control character in URL")
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
	)
	args := []string{
		"sync_gateway",
		"--adminInterface", adminInterface,
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
		"--pretty"}

	config, err := ParseCommandLine(base.TestCtx(t), args, flag.ContinueOnError)
	require.NoError(t, err, "Parsing commandline arguments without any config file")
	assert.Equal(t, interfaceAddress, *config.Interface)
	assert.Equal(t, adminInterface, *config.AdminInterface)

	assert.Equal(t, configServer, *config.ConfigServer)
	assert.Equal(t, logFilePath, config.Logging.LogFilePath)
	assert.Equal(t, strings.Split(logKeys, ","), config.Logging.Console.LogKeys)
	assert.Empty(t, *config.ProfileInterface)
	assert.True(t, config.Pretty)
	databases := config.Databases
	assert.Len(t, databases, 1)
	assert.Equal(t, dbname, databases[dbname].Name)
	assert.Equal(t, bucket, *databases[dbname].Bucket)
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
	clusterConfig := &ClusterConfigLegacy{
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
	err := SetMaxFileDescriptors(base.TestCtx(t), maxFDs)
	assert.NoError(t, err, "Sets file descriptor limit to default when requested soft limit is nil")

	// Set MaxFileDescriptors
	maxFDsHigher := DefaultMaxFileDescriptors + 1
	err = SetMaxFileDescriptors(base.TestCtx(t), &maxFDsHigher)
	assert.NoError(t, err, "Error setting MaxFileDescriptors")
}

func TestParseCommandLineWithMissingConfig(t *testing.T) {
	// Parse command line options with unknown sync gateway configuration file
	args := []string{"sync_gateway", "missing-sync-gateway.conf"}
	config, err := ParseCommandLine(base.TestCtx(t), args, flag.ContinueOnError)
	require.Error(t, err, "Trying to read configuration file which doesn't exist")
	assert.Nil(t, config)
}

func TestParseCommandLineWithBadConfigContent(t *testing.T) {
	content := `{"adminInterface":"127.0.0.1:4985","interface":"0.0.0.0:4984",
    	"databases":{"db":{"unknown_field":"walrus:data","users":{"GUEST":{"disabled":false,
		"admin_channels":["*"]}}, "allow_conflicts":false,"revs_limit":20}}}`

	configFile, err := os.CreateTemp("", "sync_gateway.conf")
	configFileName := configFile.Name()
	require.NoError(t, err, "Couldn't create configuration file")
	_, err = configFile.Write([]byte(content))
	assert.NoError(t, err, "Writing JSON content")

	defer func() {
		assert.NoError(t, configFile.Close(), "Couldn't close file: %v ", configFileName)
		assert.NoError(t, os.Remove(configFileName), "Couldn't remove file: %v ", configFileName)
		assert.False(t, base.FileExists(configFileName), "File %v should be removed", configFileName)
	}()

	args := []string{"sync_gateway", configFile.Name()}
	config, err := ParseCommandLine(base.TestCtx(t), args, flag.ContinueOnError)
	assert.Error(t, err, "Parsing configuration file with an unknown field")
	assert.Nil(t, config)
}

func TestParseCommandLineWithConfigContent(t *testing.T) {
	content := `{"logging":{"log_file_path":"/var/tmp/sglogs","console":{"log_level":"debug","log_keys":["*"]},
		"error":{"enabled":true,"rotation":{"max_size":20,"max_age":180}},"warn":{"enabled":true,"rotation":{
        "max_size":20,"max_age":90}},"info":{"enabled":false},"debug":{"enabled":false}},"databases":{"db1":{
        "server":"couchbase://127.0.0.1","username":"username","password":"password","bucket":"default",
        "certpath":"/etc/ssl/certs/cert.pem","cacertpath":"/etc/ssl/certs/ca.cert","keypath":"/etc/ssl/certs/key.pem",
        "users":{"GUEST":{"disabled":false,"admin_channels":["*"]}},"allow_conflicts":false,"revs_limit":20}}}`

	configFile, err := os.CreateTemp("", "sync_gateway.conf")
	configFileName := configFile.Name()
	require.NoError(t, err, "Couldn't create configuration file")
	_, err = configFile.Write([]byte(content))
	assert.NoError(t, err, "Writing JSON content")

	defer func() {
		assert.NoError(t, configFile.Close(), "Couldn't close file: %v ", configFileName)
		assert.NoError(t, os.Remove(configFileName), "Couldn't remove file: %v ", configFileName)
		assert.False(t, base.FileExists(configFileName), "File %v should be removed", configFileName)
	}()

	var (
		adminInterface     = "127.10.0.1:4985"
		profileInterface   = "127.10.0.1:8088"
		cacertpath         = "/etc/ssl/certs/ca.cert"
		certpath           = "/etc/ssl/certs/client.pem"
		configServer       = "http://127.0.0.1:4981/conf"
		defaultLogFilePath = "/var/log/sync_gateway"
		deploymentID       = "DEPID100"
		interfaceAddress   = "4443"
		keypath            = "/etc/ssl/certs/key.pem"
		logKeys            = "Admin,Access,Auth,Bucket"
		logFilePath        = "/var/log/sync_gateway"
	)
	args := []string{
		"sync_gateway",
		"--adminInterface", adminInterface,
		"--cacertpath", cacertpath,
		"--certpath", certpath,
		"--configServer", configServer,
		"--defaultLogFilePath", defaultLogFilePath,
		"--deploymentID", deploymentID,
		"--interface", interfaceAddress,
		"--keypath", keypath,
		"--log", logKeys,
		"--logFilePath", logFilePath,
		"--pretty",
		"--verbose",
		"--profileInterface", profileInterface,
		configFile.Name()}

	config, err := ParseCommandLine(base.TestCtx(t), args, flag.ContinueOnError)
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
	assert.Equal(t, "couchbase://127.0.0.1", *db1.BucketConfig.Server)
	assert.Equal(t, "/etc/ssl/certs/cert.pem", db1.BucketConfig.CertPath)
	assert.Equal(t, "/etc/ssl/certs/ca.cert", db1.BucketConfig.CACertPath)
	assert.Equal(t, "/etc/ssl/certs/key.pem", db1.BucketConfig.KeyPath)

	guest := db1.Users["GUEST"]
	require.NotNil(t, guest.Disabled)
	assert.False(t, *guest.Disabled)
	assert.Equal(t, base.SetFromArray([]string{"*"}), guest.ExplicitChannels)
}

func TestValidateServerContextSharedBuckets(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Skipping this test; requires Couchbase Bucket")
	}

	ctx := base.TestCtx(t)

	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)

	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)

	tb1User, tb1Password, _ := tb1.BucketSpec.Auth.GetCredentials()
	tb2User, tb2Password, _ := tb2.BucketSpec.Auth.GetCredentials()

	xattrs := base.TestUseXattrs()
	config := &StartupConfig{
		Bootstrap: BootstrapConfig{
			UseTLSServer:        base.Ptr(base.ServerIsTLS(base.UnitTestUrl())),
			ServerTLSSkipVerify: base.Ptr(base.TestTLSSkipVerify()),
		},
	}
	databases := DbConfigMap{
		"db1": {
			BucketConfig: BucketConfig{
				Server:   &tb1.BucketSpec.Server,
				Bucket:   &tb1.BucketSpec.BucketName,
				Username: tb1User,
				Password: tb1Password,
			},
			EnableXattrs: &xattrs,
			UseViews:     base.Ptr(base.TestsDisableGSI()),
		},
		"db2": {
			BucketConfig: BucketConfig{
				Server:   &tb1.BucketSpec.Server,
				Bucket:   &tb1.BucketSpec.BucketName,
				Username: tb1User,
				Password: tb1Password,
			},
			EnableXattrs: &xattrs,
			UseViews:     base.Ptr(base.TestsDisableGSI()),
		},
		"db3": {
			BucketConfig: BucketConfig{
				Server:   &tb2.BucketSpec.Server,
				Bucket:   &tb2.BucketSpec.BucketName,
				Username: tb2User,
				Password: tb2Password,
			},
			EnableXattrs: &xattrs,
			UseViews:     base.Ptr(base.TestsDisableGSI()),
		},
	}

	require.NoError(t, SetupAndValidateDatabases(ctx, databases), "Unexpected error while validating databases")

	sc := NewServerContext(ctx, config, false)
	defer sc.Close(ctx)
	for _, dbConfig := range databases {
		if !base.TestsDisableGSI() {
			dbConfig.Index = &IndexConfig{NumReplicas: base.Ptr(uint(0))}
		}
		_, err := sc.AddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: *dbConfig})
		require.NoError(t, err, "Couldn't add database from config")
	}

	sharedBucketErrors := sharedBucketDatabaseCheck(ctx, sc)
	require.NotNil(t, sharedBucketErrors)
	multiError, ok := sharedBucketErrors.(*base.MultiError)
	require.NotNil(t, ok)
	require.Equal(t, multiError.Len(), 1)
	var sharedBucketError *SharedBucketError
	require.True(t, errors.As(multiError.Errors[0], &sharedBucketError))
	assert.Equal(t, tb1.BucketSpec.BucketName, sharedBucketError.GetSharedBucket().bucketName)
	assert.Subset(t, []string{"db1", "db2"}, sharedBucketError.GetSharedBucket().dbNames)
}

func TestParseCommandLineWithIllegalOptionBucket(t *testing.T) {
	args := []string{
		"sync_gateway",
		"--bucket", "sync_gateway", // Bucket option has been removed
	}
	config, err := ParseCommandLine(base.TestCtx(t), args, flag.ContinueOnError)
	assert.Error(t, err, "Parsing commandline arguments without any config file")
	assert.Empty(t, config, "Couldn't parse commandline arguments")
}

func TestPutInvalidConfig(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/_config", `{"db": {"server": "walrus"}}`)
	assert.Equal(t, http.StatusBadRequest, response.Code)
}

func TestEnvDefaultExpansion(t *testing.T) {
	tests := []struct {
		name          string
		envKey        string
		envValue      string
		expectedValue string
		expectedError error
	}{
		{
			name:          "no value, no default",
			envKey:        "PASSWORD",
			envValue:      "",
			expectedValue: "",
			expectedError: ErrEnvVarUndefined{key: "PASSWORD"},
		},
		{
			name:          "value, no default",
			envKey:        "PASSWORD",
			envValue:      "pa55w0rd",
			expectedValue: "pa55w0rd",
			expectedError: nil,
		},
		{
			name:          "value, default",
			envKey:        "PASSWORD:-pa55w0rd",
			envValue:      "foobar",
			expectedValue: "foobar",
			expectedError: nil,
		},
		{
			name:          "no value, default",
			envKey:        "PASSWORD:-pa55w0rd",
			envValue:      "",
			expectedValue: "pa55w0rd",
			expectedError: nil,
		},
		{
			name:          "no value, default with special chars",
			envKey:        "PASSWORD:-pa55w:-0rd",
			envValue:      "",
			expectedValue: "pa55w:-0rd",
			expectedError: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualValue, err := envDefaultExpansion(base.TestCtx(t), test.envKey, func(s string) string { return test.envValue })
			require.Equal(t, err, test.expectedError)
			assert.Equal(t, test.expectedValue, actualValue)
		})
	}
}

func TestExpandEnv(t *testing.T) {
	makeEnvVarError := func(keys ...string) (errs *base.MultiError) {
		for _, key := range keys {
			errs = errs.Append(ErrEnvVarUndefined{key: key})
		}
		return errs
	}

	tests := []struct {
		name           string
		inputConfig    []byte
		varsEnv        map[string]string
		expectedConfig []byte
		expectedError  *base.MultiError
	}{
		{
			name: "environment variable substitution with $var and ${var} syntax",
			inputConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "$USERNAME",
				      "password": "${PASSWORD}"
				    }
				  }
				}
			`),
			varsEnv: map[string]string{
				"USERNAME": "Administrator",
				"PASSWORD": "password",
			},
			expectedConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "Administrator",
				      "password": "password"
				    }
				  }
				}
			`),
		},
		{
			name: "environment variable substitution with ${var:-default_value} syntax",
			inputConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "${BUCKET:-leaky_bucket}",
				      "server": "couchbase://127.0.0.1"
				    }
				  }
				}
			`),
			expectedConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1"
				    }
				  }
				}
			`),
		},
		{
			name: "environment variable substitution escape with $$var syntax",
			inputConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "$$USERNAME",
                      "password": "P@$$$$w0rd",
                      "sync": "function (doc, oldDoc) { if (doc.$$sdk) { channel(doc.$$sdk);}}"
				    }
				  }
				}
			`),
			varsEnv: map[string]string{
				"USERNAME": "Administrator",
			},
			expectedConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "$USERNAME",
                      "password": "P@$$w0rd",
                      "sync": "function (doc, oldDoc) { if (doc.$sdk) { channel(doc.$sdk);}}"
				    }
				  }
				}
			`),
		},
		{
			name: "error when environment variable is not set and no default value is specified",
			inputConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "$USERNAME",
				      "password": "${PASSWORD}"
				    }
				  }
				}
			`),
			expectedConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "",
				      "password": ""
				    }
				  }
				}
			`),
			expectedError: makeEnvVarError("USERNAME", "PASSWORD"),
		},
		{
			name: "error even when a single environment variable is not set and no default value is specified",
			inputConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "$USERNAME",
				      "password": "${PASSWORD}"
				    }
				  }
				}
			`),
			varsEnv: map[string]string{
				"USERNAME": "Administrator",
			},
			expectedConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "bucket": "leaky_bucket",
				      "server": "couchbase://127.0.0.1",
				      "username": "Administrator",
				      "password": ""
				    }
				  }
				}
			`),
			expectedError: makeEnvVarError("PASSWORD"),
		},
		{
			name: "concatenated envs",
			inputConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "username": "$COMPANY$USERNAME"
				    }
				  }
				}
			`),
			varsEnv: map[string]string{
				"COMPANY":  "couchbase",
				"USERNAME": "bbrks",
			},
			expectedConfig: []byte(`
				{
				  "databases": {
				    "db": {
				      "username": "couchbasebbrks"
				    }
				  }
				}
			`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set an environment variables.
			for k, v := range test.varsEnv {
				err := os.Setenv(k, v)
				require.NoError(t, err, "Error setting environment variable %q", k)
				value, ok := os.LookupEnv(k)
				require.True(t, ok, "Environment variable %q should exist", k)
				require.Equal(t, v, value, "Unexpected value set for environment variable %q", k)
			}
			// Check environment variable substitutions.
			actualConfig, err := expandEnv(base.TestCtx(t), test.inputConfig)
			if test.expectedError != nil {
				errs, ok := err.(*base.MultiError)
				require.True(t, ok)
				require.Equal(t, test.expectedError.Len(), errs.Len())
				require.Equal(t, test.expectedError, errs)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.expectedConfig, actualConfig)

			// Unset environment variables.
			for k, _ := range test.varsEnv {
				err := os.Unsetenv(k)
				require.NoError(t, err, "Error removing environment variable %q", k)
				value, ok := os.LookupEnv(k)
				assert.False(t, ok, "Environment variable %q shouldn't exist", k)
				assert.Empty(t, value, "Environment variable %q must be empty", k)
			}
		})
	}
}

// TestDbConfigEnvVarsToggle ensures that AllowDbConfigEnvVars toggles the ability to use env vars in db config.
func TestDbConfigEnvVarsToggle(t *testing.T) {
	// Set up an env var with a secret value and use it as a channel name to assert its value.
	const (
		varName    = "SECRET_SG_TEST_VAR"
		secretVal  = "secret"
		defaultVal = "substitute"
	)
	// ${SECRET_SG_TEST_VAR:-substitute}
	unexpandedVal := fmt.Sprintf("${%s:-%s}", varName, defaultVal)

	tests := []struct {
		allowDbConfigEnvVars *bool
		setEnvVar            bool
		expectedChannel      string
		unexpectedChannels   []string
	}{
		{
			allowDbConfigEnvVars: nil, // defaults to true - so use nil to check default handling
			setEnvVar:            true,
			expectedChannel:      secretVal,
			unexpectedChannels:   []string{defaultVal, unexpandedVal},
		},
		{
			allowDbConfigEnvVars: base.Ptr(true),
			setEnvVar:            false,
			expectedChannel:      defaultVal,
			unexpectedChannels:   []string{secretVal, unexpandedVal},
		},
		{
			allowDbConfigEnvVars: base.Ptr(false),
			setEnvVar:            true,
			expectedChannel:      unexpandedVal,
			unexpectedChannels:   []string{secretVal, defaultVal},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("allowDbConfigEnvVars=%v_setEnvVar=%v", test.allowDbConfigEnvVars, test.setEnvVar), func(t *testing.T) {
			rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{
				PersistentConfig:     true,
				allowDbConfigEnvVars: test.allowDbConfigEnvVars,
			})
			defer rt.Close()

			if test.setEnvVar {
				require.NoError(t, os.Setenv(varName, secretVal))
				t.Cleanup(func() {
					require.NoError(t, os.Unsetenv(varName))
				})
			}

			dbc := rt.NewDbConfig()
			dbc.Sync = base.Ptr(fmt.Sprintf(
				"function(doc) {channel('%s');}",
				unexpandedVal,
			))

			resp := rt.CreateDatabase("db", dbc)
			AssertStatus(t, resp, http.StatusCreated)

			docID := "doc"
			_ = rt.PutDoc(docID, `{"foo":"bar"}`)

			rt.WaitForPendingChanges()

			// ensure doc is in expected channel and is not in the unexpected channels
			changes := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels="+test.expectedChannel, "", true)
			changes.RequireDocIDs(t, []string{docID})

			channels := strings.Join(test.unexpectedChannels, ",")
			changes = rt.WaitForChanges(0, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels="+channels, "", true)
			changes.RequireDocIDs(t, []string{})
		})
	}
}

// createTempFile creates a temporary file with the given content.
func createTempFile(t *testing.T, content []byte) *os.File {
	file, err := os.CreateTemp("", "*-sync_gateway.conf")
	require.NoError(t, err, "Error creating temp file")
	_, err = file.Write(content)
	require.NoError(t, err, "Error writing bytes")
	return file
}

// deleteTempFile deletes the given file.
func deleteTempFile(t *testing.T, file *os.File) {
	path := file.Name()
	require.NoError(t, file.Close(), "Error closing file: %s ", path)
	require.NoError(t, os.Remove(path), "Error removing file: %s ", path)
	require.False(t, base.FileExists(path), "Deleted file %s shouldn't exist", path)
}

func TestDefaultLogging(t *testing.T) {
	base.ResetGlobalTestLogging(t)
	config := DefaultStartupConfig("")
	assert.Equal(t, base.RedactPartial, config.Logging.RedactionLevel)
	assert.Equal(t, true, base.RedactUserData)

	require.NoError(t, config.SetupAndValidateLogging(base.TestCtx(t)))
	assert.Equal(t, base.LevelNone, *base.ConsoleLogLevel())
	assert.Equal(t, []string{}, base.ConsoleLogKey().EnabledLogKeys())

	// setting just a log key should enable logging
	config.Logging.Console = &base.ConsoleLoggerConfig{LogKeys: []string{"CRUD"}}
	require.NoError(t, config.SetupAndValidateLogging(base.TestCtx(t)))
	assert.Equal(t, base.LevelInfo, *base.ConsoleLogLevel())
	assert.Equal(t, []string{"CRUD"}, base.ConsoleLogKey().EnabledLogKeys())
}

func TestSetupServerContext(t *testing.T) {
	t.Run("Create server context with a valid configuration", func(t *testing.T) {
		config := DefaultStartupConfig("")
		config.Bootstrap.Server = base.UnitTestUrl() // Valid config requires server to be explicitly defined
		config.Bootstrap.UseTLSServer = base.Ptr(base.ServerIsTLS(base.UnitTestUrl()))
		config.Bootstrap.ServerTLSSkipVerify = base.Ptr(base.TestTLSSkipVerify())
		config.Bootstrap.Username = base.TestClusterUsername()
		config.Bootstrap.Password = base.TestClusterPassword()
		ctx := base.TestCtx(t)
		sc, err := SetupServerContext(ctx, &config, false)
		require.NoError(t, err)
		defer sc.Close(ctx)
		require.NotNil(t, sc)
	})
}

// CBG-1583 - config group ID EE-only
func TestConfigGroupIDValidation(t *testing.T) {
	testCases := []struct {
		name          string
		cfgGroupID    string
		eeMode        bool
		expectedError string
	}{
		{
			name:       "No change, CE mode",
			cfgGroupID: PersistentConfigDefaultGroupID,
			eeMode:     false,
		},
		{
			name:       "No change, EE mode",
			cfgGroupID: PersistentConfigDefaultGroupID,
			eeMode:     true,
		},
		{
			name:       "Changed, EE mode",
			cfgGroupID: "testGroup",
			eeMode:     true,
		},
		{
			name:          "Changed, CE mode",
			cfgGroupID:    "testGroup",
			eeMode:        false,
			expectedError: "customization of group_id is only supported in enterprise edition",
		},
		{
			name:       "Changed, EE mode, at max length",
			cfgGroupID: strings.Repeat("a", persistentConfigGroupIDMaxLength),
			eeMode:     true,
		},
		{
			name:          "Changed, EE mode, over max length",
			cfgGroupID:    strings.Repeat("a", persistentConfigGroupIDMaxLength+1),
			eeMode:        true,
			expectedError: "group_id must be at most",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			isEnterpriseEdition := base.IsEnterpriseEdition()
			if test.eeMode && !isEnterpriseEdition {
				t.Skip("EE mode only test case")
			}
			if !test.eeMode && isEnterpriseEdition {
				t.Skip("CE mode only test case")
			}

			sc := StartupConfig{
				Bootstrap: BootstrapConfig{
					ConfigGroupID: test.cfgGroupID,
					Server:        base.UnitTestUrl(),
					UseTLSServer:  base.Ptr(base.ServerIsTLS(base.UnitTestUrl())),
				},
			}
			err := sc.Validate(base.TestCtx(t), isEnterpriseEdition)
			if test.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// CBG-1599
func TestClientTLSMissing(t *testing.T) {
	errorTLSOneMissing := "both TLS Key Path and TLS Cert Path must be provided when using client TLS. Disable client TLS by not providing either of these options"
	testCases := []struct {
		name        string
		tlsKey      bool
		tlsCert     bool
		expectError bool
	}{
		{
			name:        "No TLS",
			expectError: false,
		},
		{
			name:        "TLS Key but no cert provided",
			tlsKey:      true,
			expectError: true,
		},
		{
			name:        "TLS Cert but no key provided",
			tlsCert:     true,
			expectError: true,
		},
		{
			name:        "TLS Cert and key provided",
			tlsKey:      true,
			tlsCert:     true,
			expectError: false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			config := StartupConfig{}
			if test.tlsKey {
				config.API.HTTPS.TLSKeyPath = "test.key"
			}
			if test.tlsCert {
				config.API.HTTPS.TLSCertPath = "test.cert"
			}
			err := config.Validate(base.TestCtx(t), base.IsEnterpriseEdition())
			if test.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), errorTLSOneMissing)
			} else {
				if err != nil { // If validate fails, make sure it's not due to TLS
					assert.NotContains(t, err.Error(), errorTLSOneMissing)
				}
			}
		})
	}
}

// missingJavaScriptFilePath represents a nonexistent JavaScript file path on the disk.
// It is used to cover the negative test scenario for loading JavaScript from a file that is missing.
const missingJavaScriptFilePath = "/var/folders/lv/956l1vqx48gfln58125f_mmc0000gs/T/missing-703266776.js"

// errInternalServerError is thrown from the mocked HTTP endpoint backed by the
// httptest server. It is used to simulate the negative test scenario for loading
// JavaScript source from an external HTTP endpoint.
var errInternalServerError = &base.HTTPError{
	Status:  http.StatusInternalServerError,
	Message: "Internal Server Error",
}

// javaScriptFile returns a callable function to create a JavaScript file on the disk with the
// given JavaScript source and returns a teardown function to remove the file after its use.
func javaScriptFile(t *testing.T, js string) func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		file, err := os.CreateTemp("", "*.js")
		require.NoError(t, err, "Error creating JavaScript file")
		_, err = file.Write([]byte(js))
		require.NoError(t, err, "Error writing to JavaScript file")
		path = file.Name()
		teardownFn = func() {
			assert.NoError(t, file.Close(), "Error closing file: %s ", path)
			assert.NoError(t, os.Remove(path), "Error removing file: %s ", path)
			assert.False(t, base.FileExists(path), "File %s should be removed", path)
		}
		return path, teardownFn
	}
}

// emptyJavaScriptFile returns a callable function to create an empty file on the disk
// and returns a teardown function to remove the file after its use.
func emptyJavaScriptFile(t *testing.T) func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		file, err := os.CreateTemp("", "*.js")
		require.NoError(t, err, "Error creating JavaScript file")
		path = file.Name()
		teardownFn = func() {
			assert.NoError(t, file.Close(), "Error closing file: %s ", path)
			assert.NoError(t, os.Remove(path), "Error removing file: %s ", path)
			assert.False(t, base.FileExists(path), "File %s should be removed", path)
		}
		return path, teardownFn
	}
}

// missingJavaScriptFile returns a callable function that internally returns a missing
// JavaScript file path. The callable teardown function returned from this function is nil.
func missingJavaScriptFile() func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		path = missingJavaScriptFilePath
		return path, nil
	}
}

// inlineJavaScript returns a callable function that internally returns given JavaScript
// source as-is. The callable teardown function returned from this function is nil.
func inlineJavaScript(js string) func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		return js, nil
	}
}

// javaScriptHttpEndpoint returns a callable function to setup an HTTP endpoint that exposes
// the given JavaScript source and returns a teardown function to terminate the underlying
// httptest server instance.
func javaScriptHttpEndpoint(t *testing.T, js string) func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := fmt.Fprint(w, js)
			require.NoError(t, err)
		}))
		teardownFn = func() { ts.Close() }
		return ts.URL, teardownFn
	}
}

// javaScriptHttpErrorEndpoint returns a callable function to setup an HTTP endpoint that always
// return StatusInternalServerError and a teardown function to terminate the underlying httptest
// server instance.
func javaScriptHttpErrorEndpoint() func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		teardownFn = func() { ts.Close() }
		return ts.URL, teardownFn
	}
}

// javaScriptHttpsEndpoint returns a callable function to setup an HTTPS endpoint that exposes
// the given JavaScript source and returns a teardown function to terminate the underlying
// httptest server instance.
func javaScriptHttpsEndpoint(t *testing.T, js string) func() (path string, teardownFn func()) {
	return func() (path string, teardownFn func()) {
		ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := fmt.Fprint(w, js)
			require.NoError(t, err)
		}))
		teardownFn = func() { ts.Close() }
		return ts.URL, teardownFn
	}
}

func TestLoadJavaScript(t *testing.T) {
	const js = `function (doc, oldDoc) { if (doc.published) { channel("public"); } }`
	var tests = []struct {
		name               string
		jsInput            func() (path string, teardownFn func())
		insecureSkipVerify bool
		jsExpected         string
		errExpected        error
	}{
		{
			name:               "Load inline JavaScript",
			jsInput:            inlineJavaScript(js),
			insecureSkipVerify: false,
			jsExpected:         js,
			errExpected:        nil,
		},
		{
			name:               "Load JavaScript from an external http endpoint",
			jsInput:            javaScriptHttpEndpoint(t, js),
			insecureSkipVerify: false,
			jsExpected:         js,
			errExpected:        nil,
		},
		{
			name:               "Load JavaScript from an external https endpoint with ssl verification enabled",
			jsInput:            javaScriptHttpsEndpoint(t, js),
			insecureSkipVerify: false,
			jsExpected:         "",
			errExpected:        x509.UnknownAuthorityError{},
		},
		{
			name:               "Load JavaScript from an external https endpoint with ssl verification disabled",
			jsInput:            javaScriptHttpsEndpoint(t, js),
			insecureSkipVerify: true,
			jsExpected:         js,
			errExpected:        nil,
		},
		{
			name:               "Load JavaScript from an external endpoint that returns an error",
			jsInput:            javaScriptHttpErrorEndpoint(),
			insecureSkipVerify: false,
			jsExpected:         "",
			errExpected:        errInternalServerError,
		},
		{
			name:               "Load JavaScript from an external file",
			jsInput:            javaScriptFile(t, js),
			insecureSkipVerify: false,
			jsExpected:         js,
			errExpected:        nil,
		},
		{
			name:               "Load JavaScript from an external empty file",
			jsInput:            emptyJavaScriptFile(t),
			insecureSkipVerify: false,
			jsExpected:         "",
			errExpected:        nil,
		},
		{
			name:               "Load JavaScript from an external file that doesn't exist",
			jsInput:            missingJavaScriptFile(),
			insecureSkipVerify: false,
			jsExpected:         missingJavaScriptFilePath,
			errExpected:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputJavaScriptOrPath, teardownFn := test.jsInput()
			defer func() {
				if teardownFn != nil {
					teardownFn()
				}
			}()
			js, err := loadJavaScript(base.TestCtx(t), inputJavaScriptOrPath, test.insecureSkipVerify)
			if test.errExpected != nil {
				requireErrorWithX509UnknownAuthority(t, err, test.errExpected)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.jsExpected, js)
			}
		})
	}
}

func TestSetupDbConfigCredentials(t *testing.T) {
	const (
		expectedUsername = "alice"
		expectedPassword = "hunter2"
		expectedX509Cert = "/tmp/x509cert"
		expectedX509Key  = "/tmp/x509key"
	)
	var tests = []struct {
		name              string
		dbConfig          DbConfig
		bootstrapConfig   BootstrapConfig
		credentialsConfig *base.CredentialsConfig
		expectX509        bool
	}{
		{
			name:              "bootstrap only",
			dbConfig:          DbConfig{Name: "db"},
			bootstrapConfig:   BootstrapConfig{Server: "couchbase://example.org", Username: expectedUsername, Password: expectedPassword},
			credentialsConfig: nil,
		},
		{
			name:              "db username/password override",
			dbConfig:          DbConfig{Name: "db"},
			bootstrapConfig:   BootstrapConfig{Server: "couchbase://example.org", Username: "bob", Password: "foobar"},
			credentialsConfig: &base.CredentialsConfig{Username: expectedUsername, Password: expectedPassword},
		},
		{
			name:              "db username/password override from x509",
			dbConfig:          DbConfig{Name: "db"},
			bootstrapConfig:   BootstrapConfig{Server: "couchbase://example.org", X509CertPath: "/tmp/x509cert", X509KeyPath: "/tmp/x509key"},
			credentialsConfig: &base.CredentialsConfig{Username: expectedUsername, Password: expectedPassword},
		},
		{
			name:              "db x509 override from username/password",
			dbConfig:          DbConfig{Name: "db"},
			bootstrapConfig:   BootstrapConfig{Server: "couchbase://example.org", Username: "bob", Password: "foobar"},
			credentialsConfig: &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: expectedX509Cert, X509KeyPath: expectedX509Key}},
			expectX509:        true,
		},
		{
			name:              "db x509 override",
			dbConfig:          DbConfig{Name: "db"},
			bootstrapConfig:   BootstrapConfig{Server: "couchbase://example.org", X509CertPath: "/tmp/bs-x509cert", X509KeyPath: "/tmp/bs-x509key"},
			credentialsConfig: &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: expectedX509Cert, X509KeyPath: expectedX509Key}},
			expectX509:        true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.dbConfig.setup(base.TestCtx(t), test.dbConfig.Name, test.bootstrapConfig, test.credentialsConfig, nil, false)
			require.NoError(t, err)
			if test.expectX509 {
				assert.Equal(t, "", test.dbConfig.Username)
				assert.Equal(t, "", test.dbConfig.Password)
				assert.Equal(t, expectedX509Cert, test.dbConfig.CertPath)
				assert.Equal(t, expectedX509Key, test.dbConfig.KeyPath)
			} else {
				assert.Equal(t, expectedUsername, test.dbConfig.Username)
				assert.Equal(t, expectedPassword, test.dbConfig.Password)
				assert.Equal(t, "", test.dbConfig.CertPath)
				assert.Equal(t, "", test.dbConfig.KeyPath)
			}
		})
	}
}

func TestSetupDbConfigWithSyncFunction(t *testing.T) {
	const jsSync = `function (doc, oldDoc) { if (doc.published) { channel("public"); } }`
	var tests = []struct {
		name               string
		jsSyncInput        func() (path string, teardownFn func())
		insecureSkipVerify bool
		jsSyncFnExpected   string
		errExpected        error
	}{
		{
			name:               "Load inline sync function",
			jsSyncInput:        inlineJavaScript(jsSync),
			insecureSkipVerify: false,
			jsSyncFnExpected:   jsSync,
			errExpected:        nil,
		},
		{
			name:               "Load sync function from an external http endpoint",
			jsSyncInput:        javaScriptHttpEndpoint(t, jsSync),
			insecureSkipVerify: false,
			jsSyncFnExpected:   jsSync,
			errExpected:        nil,
		},
		{
			name:               "Load sync function from an external https endpoint with ssl verification enabled",
			jsSyncInput:        javaScriptHttpsEndpoint(t, jsSync),
			insecureSkipVerify: false,
			jsSyncFnExpected:   "",
			errExpected:        x509.UnknownAuthorityError{},
		},
		{
			name:               "Load sync function from an external https endpoint with ssl verification disabled",
			jsSyncInput:        javaScriptHttpsEndpoint(t, jsSync),
			insecureSkipVerify: true,
			jsSyncFnExpected:   jsSync,
			errExpected:        nil,
		},
		{
			name:               "Load sync function from an external endpoint that returns an error",
			jsSyncInput:        javaScriptHttpErrorEndpoint(),
			insecureSkipVerify: false,
			jsSyncFnExpected:   "",
			errExpected:        errInternalServerError,
		},
		{
			name:               "Load sync function from an external file",
			jsSyncInput:        javaScriptFile(t, jsSync),
			insecureSkipVerify: false,
			jsSyncFnExpected:   jsSync,
			errExpected:        nil,
		},
		{
			name:               "Load sync function from an external empty file",
			jsSyncInput:        emptyJavaScriptFile(t),
			insecureSkipVerify: false,
			jsSyncFnExpected:   "",
			errExpected:        nil,
		},
		{
			name:               "Load sync function from an external file that doesn't exist",
			jsSyncInput:        missingJavaScriptFile(),
			insecureSkipVerify: false,
			jsSyncFnExpected:   missingJavaScriptFilePath,
			errExpected:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sync, teardownFn := test.jsSyncInput()
			defer func() {
				if teardownFn != nil {
					teardownFn()
				}
			}()
			dbConfig := DbConfig{
				Name: "db",
				Sync: base.Ptr(sync),
			}
			if test.insecureSkipVerify {
				dbConfig.Unsupported = &db.UnsupportedOptions{
					RemoteConfigTlsSkipVerify: true,
				}
			}
			err := dbConfig.setup(base.TestCtx(t), dbConfig.Name, BootstrapConfig{}, nil, nil, false)
			if test.errExpected != nil {
				requireErrorWithX509UnknownAuthority(t, err, test.errExpected)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.jsSyncFnExpected, *dbConfig.Sync)
			}
		})
	}
}

func TestSetupDbConfigWithImportFilterFunction(t *testing.T) {
	const jsImportFilter = `function(doc) { if (doc.type != "mobile") { return false } return true }`
	var tests = []struct {
		name                   string
		jsImportFilterInput    func() (inlineFnOrPath string, teardownFn func())
		insecureSkipVerify     bool
		jsImportFilterExpected string
		errExpected            error
	}{
		{
			name:                   "Load inline import filter function",
			jsImportFilterInput:    inlineJavaScript(jsImportFilter),
			insecureSkipVerify:     false,
			jsImportFilterExpected: jsImportFilter,
			errExpected:            nil,
		},
		{
			name:                   "Load import filter function from an external HTTP endpoint",
			jsImportFilterInput:    javaScriptHttpEndpoint(t, jsImportFilter),
			insecureSkipVerify:     false,
			jsImportFilterExpected: jsImportFilter,
			errExpected:            nil,
		},
		{
			name:                   "Load import filter from an external https endpoint with ssl verification enabled",
			jsImportFilterInput:    javaScriptHttpsEndpoint(t, jsImportFilter),
			insecureSkipVerify:     false,
			jsImportFilterExpected: "",
			errExpected:            x509.UnknownAuthorityError{},
		},
		{
			name:                   "Load import filter from an external https endpoint with ssl verification disabled",
			jsImportFilterInput:    javaScriptHttpsEndpoint(t, jsImportFilter),
			insecureSkipVerify:     true,
			jsImportFilterExpected: jsImportFilter,
			errExpected:            nil,
		},
		{
			name:                   "Load import filter function from an external endpoint that returns an error",
			jsImportFilterInput:    javaScriptHttpErrorEndpoint(),
			insecureSkipVerify:     false,
			jsImportFilterExpected: "",
			errExpected:            errInternalServerError,
		},
		{
			name:                   "Load import filter function from an external file",
			jsImportFilterInput:    javaScriptFile(t, jsImportFilter),
			insecureSkipVerify:     false,
			jsImportFilterExpected: jsImportFilter,
			errExpected:            nil,
		},
		{
			name:                   "Load import filter function from an external empty file",
			jsImportFilterInput:    emptyJavaScriptFile(t),
			insecureSkipVerify:     false,
			jsImportFilterExpected: "",
			errExpected:            nil,
		},
		{
			name:                   "Load import filter function from an external file that doesn't exist",
			jsImportFilterInput:    missingJavaScriptFile(),
			insecureSkipVerify:     false,
			jsImportFilterExpected: missingJavaScriptFilePath,
			errExpected:            nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			importFilter, teardownFn := test.jsImportFilterInput()
			defer func() {
				if teardownFn != nil {
					teardownFn()
				}
			}()
			dbConfig := DbConfig{
				Name:         "db",
				ImportFilter: base.Ptr(importFilter),
			}
			if test.insecureSkipVerify {
				dbConfig.Unsupported = &db.UnsupportedOptions{
					RemoteConfigTlsSkipVerify: true,
				}
			}
			err := dbConfig.setup(base.TestCtx(t), dbConfig.Name, BootstrapConfig{}, nil, nil, false)
			if test.errExpected != nil {
				requireErrorWithX509UnknownAuthority(t, err, test.errExpected)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.jsImportFilterExpected, *dbConfig.ImportFilter)
			}
		})
	}
}

func TestSetupDbConfigWithConflictResolutionFunction(t *testing.T) {
	const jsConflictResolution = `function(conflict) {
      if (conflict.LocalDocument.type == "A4") {
        return defaultPolicy(conflict);
      } else {
        return conflict.RemoteDocument;
      }
    }`
	var tests = []struct {
		name                  string
		jsConflictResInput    func() (inlineFnOrPath string, teardownFn func())
		insecureSkipVerify    bool
		jsConflictResExpected string
		errExpected           error
	}{
		{
			name:                  "Load an inline conflict resolution function",
			jsConflictResInput:    inlineJavaScript(jsConflictResolution),
			insecureSkipVerify:    false,
			jsConflictResExpected: jsConflictResolution,
			errExpected:           nil,
		},
		{
			name:                  "Load conflict resolution function from an external http endpoint",
			jsConflictResInput:    javaScriptHttpEndpoint(t, jsConflictResolution),
			insecureSkipVerify:    false,
			jsConflictResExpected: jsConflictResolution,
			errExpected:           nil,
		},
		{
			name:                  "Load conflict resolution function from an external http endpoint with ssl verification enabled",
			jsConflictResInput:    javaScriptHttpsEndpoint(t, jsConflictResolution),
			insecureSkipVerify:    false,
			jsConflictResExpected: "",
			errExpected:           x509.UnknownAuthorityError{},
		},
		{
			name:                  "Load conflict resolution function from an external http endpoint with ssl verification disabled",
			jsConflictResInput:    javaScriptHttpsEndpoint(t, jsConflictResolution),
			insecureSkipVerify:    true,
			jsConflictResExpected: jsConflictResolution,
			errExpected:           nil,
		},
		{
			name:                  "Load conflict resolution function from an external endpoint that returns an error",
			jsConflictResInput:    javaScriptHttpErrorEndpoint(),
			insecureSkipVerify:    false,
			jsConflictResExpected: "",
			errExpected:           errInternalServerError,
		},
		{
			name:                  "Load conflict resolution function from an external file",
			jsConflictResInput:    javaScriptFile(t, jsConflictResolution),
			insecureSkipVerify:    false,
			jsConflictResExpected: jsConflictResolution,
			errExpected:           nil,
		},
		{
			name:                  "Load conflict resolution function from an external empty file",
			jsConflictResInput:    emptyJavaScriptFile(t),
			insecureSkipVerify:    false,
			jsConflictResExpected: "",
			errExpected:           nil,
		},
		{
			name:                  "Load conflict resolution function from an external file that doesn't exist",
			jsConflictResInput:    missingJavaScriptFile(),
			insecureSkipVerify:    false,
			jsConflictResExpected: missingJavaScriptFilePath,
			errExpected:           nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conflictResolutionFn, teardownFn := test.jsConflictResInput()
			defer func() {
				if teardownFn != nil {
					teardownFn()
				}
			}()
			dbConfig := DbConfig{
				Name: "db",
				Replications: map[string]*db.ReplicationConfig{
					"replication1": {
						ID:                     "replication1",
						ConflictResolutionType: db.ConflictResolverCustom,
						ConflictResolutionFn:   conflictResolutionFn,
					},
				},
			}
			if test.insecureSkipVerify {
				dbConfig.Unsupported = &db.UnsupportedOptions{
					RemoteConfigTlsSkipVerify: true,
				}
			}
			err := dbConfig.setup(base.TestCtx(t), dbConfig.Name, BootstrapConfig{}, nil, nil, false)
			if test.errExpected != nil {
				requireErrorWithX509UnknownAuthority(t, err, test.errExpected)
			} else {
				require.NoError(t, err)
				require.NotNil(t, dbConfig.Replications["replication1"])
				conflictResolutionFnActual := dbConfig.Replications["replication1"].ConflictResolutionFn
				assert.Equal(t, test.jsConflictResExpected, conflictResolutionFnActual)
			}
		})
	}
}

func TestWebhookFilterFunctionLoad(t *testing.T) {
	const jsWebhookFilter = `function(doc) { if (doc.type != "mobile") { return false } return true }`
	var tests = []struct {
		name                 string
		jsWebhookFilterInput func() (inlineFnOrPath string, teardownFn func())
		insecureSkipVerify   bool
		errExpected          error
	}{
		{
			name:                 "Load inline webhook filter function",
			jsWebhookFilterInput: inlineJavaScript(jsWebhookFilter),
			insecureSkipVerify:   false,
			errExpected:          nil,
		},
		{
			name:                 "Load webhook filter function from an external http endpoint",
			jsWebhookFilterInput: javaScriptHttpEndpoint(t, jsWebhookFilter),
			insecureSkipVerify:   false,
			errExpected:          nil,
		},
		{
			name:                 "Load webhook filter function from an external https endpoint with ssl verification enabled",
			jsWebhookFilterInput: javaScriptHttpsEndpoint(t, jsWebhookFilter),
			insecureSkipVerify:   false,
			errExpected:          x509.UnknownAuthorityError{},
		},
		{
			name:                 "Load webhook filter function from an external https endpoint with ssl verification disabled",
			jsWebhookFilterInput: javaScriptHttpsEndpoint(t, jsWebhookFilter),
			insecureSkipVerify:   true,
			errExpected:          nil,
		},
		{
			name:                 "Load webhook filter function from an external endpoint that returns an error",
			jsWebhookFilterInput: javaScriptHttpErrorEndpoint(),
			insecureSkipVerify:   false,
			errExpected:          errInternalServerError,
		},
		{
			name:                 "Load webhook filter function from an external file",
			jsWebhookFilterInput: javaScriptFile(t, jsWebhookFilter),
			insecureSkipVerify:   false,
			errExpected:          nil,
		},
		{
			name:                 "Load webhook filter function from an external empty file",
			jsWebhookFilterInput: emptyJavaScriptFile(t),
			insecureSkipVerify:   false,
			errExpected:          nil,
		},
		{
			name:                 "Load webhook filter function from an external file that doesn't exist",
			jsWebhookFilterInput: missingJavaScriptFile(),
			insecureSkipVerify:   false,
			errExpected:          nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			webhookFilter, teardownFn := test.jsWebhookFilterInput()
			defer func() {
				if teardownFn != nil {
					teardownFn()
				}
			}()
			dbConfig := DbConfig{
				Name: "db",
				EventHandlers: &EventHandlerConfig{
					MaxEventProc:   500,
					WaitForProcess: "100",
					DBStateChanged: []*EventConfig{
						{
							HandlerType: "webhook",
							Url:         "http://127.0.0.1:8080/",
							Timeout:     base.Ptr(uint64(0)),
							Filter:      webhookFilter,
						},
					},
				},
			}
			if test.insecureSkipVerify {
				dbConfig.Unsupported = &db.UnsupportedOptions{
					RemoteConfigTlsSkipVerify: true,
				}
			}
			terminator := make(chan bool)
			defer close(terminator)
			ctx := &db.DatabaseContext{EventMgr: db.NewEventManager(terminator)}
			sc := &ServerContext{}
			err := sc.initEventHandlers(base.TestCtx(t), ctx, &dbConfig)
			if test.errExpected != nil {
				requireErrorWithX509UnknownAuthority(t, err, test.errExpected)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJSLoadTypeString(t *testing.T) {
	// Ensure number of JSLoadType constants, and names match.
	assert.Equal(t, int(jsLoadTypeCount), len(jsLoadTypes))

	assert.Equal(t, "SyncFunction", SyncFunction.String())
	assert.Equal(t, "ImportFilter", ImportFilter.String())
	assert.Equal(t, "ConflictResolver", ConflictResolver.String())
	assert.Equal(t, "WebhookFilter", WebhookFilter.String())

	// Test out of bounds JSLoadType
	assert.Equal(t, "JSLoadType(4294967295)", JSLoadType(math.MaxUint32).String())
}

func TestUseXattrs(t *testing.T) {
	testCases := []struct {
		name           string
		enableXattrs   *bool
		expectedXattrs bool
	}{
		{
			name:           "Nil Xattrs",
			enableXattrs:   nil,
			expectedXattrs: true, // Expects base.DefaultUseXattrs
		},
		{
			name:           "False Xattrs",
			enableXattrs:   base.Ptr(false),
			expectedXattrs: false,
		},
		{
			name:           "True Xattrs",
			enableXattrs:   base.Ptr(true),
			expectedXattrs: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			dbc := &DbConfig{EnableXattrs: test.enableXattrs}
			result := dbc.UseXattrs()
			assert.Equal(t, test.expectedXattrs, result)
		})
	}
}

func TestInvalidJavascriptFunctions(t *testing.T) {
	testCases := []struct {
		Name               string
		SyncFunction       *string
		ImportFilter       *string
		ExpectErrorCount   int
		ExpectSyncFunction *string
		ExpectImportFilter *string
	}{
		{
			"Both nil",
			nil,
			nil,
			0,
			nil,
			nil,
		},
		{
			"Valid Sync Fn No Import",
			base.Ptr(`function(){}`),
			nil,
			0,
			base.Ptr(`function(){}`),
			nil,
		},
		{
			"Valid Import Fn No Sync",
			nil,
			base.Ptr(`function(){}`),
			0,
			nil,
			base.Ptr(`function(){}`),
		},
		{
			"Both empty",
			base.Ptr(``),
			base.Ptr(``),
			0,
			nil,
			nil,
		},
		{
			"Both blank",
			base.Ptr(` `),
			base.Ptr(` `),
			0,
			nil,
			nil,
		},
		{
			"Invalid Sync Fn No Import",
			base.Ptr(`function(){`),
			nil,
			1,
			nil,
			nil,
		},
		{
			"Invalid Sync Fn No Import 2",
			base.Ptr(`function(doc){
				if (t )){}
			}`),
			nil,
			1,
			nil,
			nil,
		},
		{
			"Invalid Import Fn No Sync",
			nil,
			base.Ptr(`function(){`),
			1,
			nil,
			nil,
		},
		{
			"Both invalid",
			base.Ptr(`function(){`),
			base.Ptr(`function(){`),
			2,
			nil,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			dbConfig := DbConfig{
				Name: SafeDatabaseName(t, testCase.Name),
			}

			if testCase.SyncFunction != nil {
				dbConfig.Sync = testCase.SyncFunction
			}

			if testCase.ImportFilter != nil {
				dbConfig.ImportFilter = testCase.ImportFilter
			}

			validateReplications := true
			err := dbConfig.validate(base.TestCtx(t), false, validateReplications)

			if testCase.ExpectErrorCount == 0 {
				assert.NoError(t, err)
				assert.Equal(t, testCase.ExpectSyncFunction, dbConfig.Sync)
				assert.Equal(t, testCase.ExpectImportFilter, dbConfig.ImportFilter)
			} else {
				assert.Error(t, err)
				errorMessages, ok := err.(*base.MultiError)
				require.True(t, ok)
				assert.Len(t, errorMessages.Errors, testCase.ExpectErrorCount)
			}

		})
	}

}

func TestStartupConfigBcryptCostValidation(t *testing.T) {
	errContains := auth.ErrInvalidBcryptCost.Error()
	testCases := []struct {
		name        string
		cost        int
		expectError bool
	}{
		{
			name:        "Valid bcrypt value",
			cost:        20,
			expectError: false,
		},
		{
			name:        "Valid edge case max bcrypt value",
			cost:        bcrypt.MaxCost,
			expectError: false,
		},
		{
			name:        "Invalid edge case max+1 bcrypt value",
			cost:        bcrypt.MaxCost + 1,
			expectError: true,
		},
		{
			name:        "Invalid edge case min-1 bcrypt value",
			cost:        auth.DefaultBcryptCost - 1,
			expectError: true,
		},
		{
			name:        "Valid edge case min bcrypt value",
			cost:        auth.DefaultBcryptCost,
			expectError: false,
		},
		{
			name:        "Valid 0 bcrypt",
			cost:        0,
			expectError: false,
		},
		{
			name:        "Valid below 0 bcrypt",
			cost:        -1,
			expectError: false,
		},
		{
			name:        "Invalid above 0, below min bcrypt",
			cost:        1,
			expectError: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			sc := StartupConfig{Auth: AuthConfig{BcryptCost: test.cost}}
			err := sc.Validate(base.TestCtx(t), base.IsEnterpriseEdition())
			if test.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), errContains)
			} else if err != nil {
				assert.NotContains(t, err.Error(), errContains)
			}
		})
	}
}

func TestStartupConfigReplicationThrottleValidation(t *testing.T) {
	errContains, err := regexp.Compile(`max_concurrent_revs: \d+ outside allowed range`)
	require.NoErrorf(t, err, "regexp.Compile failed for test assertion")

	testCases := []struct {
		name         string
		revsThrottle *int
		expectError  bool
	}{
		{
			name:         "valid-nil",
			revsThrottle: nil,
			expectError:  false,
		},
		{
			name:         "invalid-low",
			revsThrottle: base.Ptr(4),
			expectError:  true,
		},
		{
			name:         "Valid-5",
			revsThrottle: base.Ptr(5),
			expectError:  false,
		},
		{
			name:         "Valid-20",
			revsThrottle: base.Ptr(20),
			expectError:  false,
		},
		{
			name:         "Valid-200",
			revsThrottle: base.Ptr(200),
			expectError:  false,
		},
		{
			name:         "invalid-high",
			revsThrottle: base.Ptr(201),
			expectError:  true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			sc := StartupConfig{Replicator: ReplicatorConfig{MaxConcurrentRevs: test.revsThrottle}}
			err := sc.Validate(base.TestCtx(t), base.IsEnterpriseEdition())
			if test.expectError {
				require.Error(t, err)
				assert.Regexp(t, errContains, err.Error())
			} else if err != nil {
				assert.NotContains(t, err.Error(), errContains)
			}
		})
	}
}

func Test_validateJavascriptFunction(t *testing.T) {
	tests := []struct {
		name        string
		jsFunc      *string
		wantIsEmpty bool
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name:        "unset (nil)",
			jsFunc:      nil,
			wantIsEmpty: true,
			wantErr:     assert.NoError,
		},
		{
			name:        "whitespace only",
			jsFunc:      base.Ptr("   \t \n "),
			wantIsEmpty: true,
			wantErr:     assert.NoError,
		},
		{
			name:        "invalid js",
			jsFunc:      base.Ptr("  func() { console.log(\"foo\"); } "),
			wantIsEmpty: false,
			wantErr:     assert.Error,
		},
		{
			name:        "valid js",
			jsFunc:      base.Ptr(" function() { console.log(\"foo\"); }  "),
			wantIsEmpty: false,
			wantErr:     assert.NoError,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotIsEmpty, err := validateJavascriptFunction(test.jsFunc)
			if !test.wantErr(t, err, fmt.Sprintf("validateJavascriptFunction(%v)", test.jsFunc)) {
				return
			}
			assert.Equalf(t, test.wantIsEmpty, gotIsEmpty, "validateJavascriptFunction(%v)", test.jsFunc)
		})
	}
}

func TestBucketCredentialsValidation(t *testing.T) {
	bucketCredsError := "bucket_credentials cannot use both x509 and basic auth"
	bucketNoCredsServerless := "at least 1 bucket must be defined in bucket_credentials when running in serverless mode"
	testCases := []struct {
		name          string
		startupConfig StartupConfig
		expectedError *string
	}{
		{
			name:          "No credentials provided",
			startupConfig: StartupConfig{},
		},
		{
			name: "Valid bucket using x509",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert", X509KeyPath: "key"}}},
			},
		},
		{
			name: "Valid bucket using basic auth",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{Username: "uname", Password: "pass"}}},
		},
		{
			name: "Valid database using basic auth",
			startupConfig: StartupConfig{
				DatabaseCredentials: PerDatabaseCredentialsConfig{"db": &base.CredentialsConfig{Username: "uname", Password: "pword"}},
			},
		},
		{
			name: "Blank database and filled bucket creds provided",
			startupConfig: StartupConfig{
				DatabaseCredentials: PerDatabaseCredentialsConfig{},
				BucketCredentials:   base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert", X509KeyPath: "key"}}},
			},
		},
		{
			name: "Filled database and blank bucket creds provided",
			startupConfig: StartupConfig{
				BucketCredentials:   base.PerBucketCredentialsConfig{},
				DatabaseCredentials: PerDatabaseCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert", X509KeyPath: "key"}}},
			},
		},
		{
			name: "Database and bucket creds provided",
			startupConfig: StartupConfig{
				DatabaseCredentials: PerDatabaseCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert", X509KeyPath: "key"}}},
				BucketCredentials:   base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert", X509KeyPath: "key"}}},
			},
		},
		{
			name: "Bucket creds for x509 and basic auth",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert", X509KeyPath: "key"}, Username: "uname", Password: "pword"}},
			},
			expectedError: &bucketCredsError,
		},
		{
			name: "Bucket creds for x509 key and basic auth password only",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509KeyPath: "key"}, Password: "pword"}},
			},
			expectedError: &bucketCredsError,
		},
		{
			name: "Bucket creds for x509 cert and basic auth username only",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert"}, Username: "uname"}},
			},
			expectedError: &bucketCredsError,
		},
		{
			name: "Bucket creds for x509 cert and basic auth username only",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{CredentialsConfigX509: base.CredentialsConfigX509{X509CertPath: "cert"}, Username: "uname"}},
			},
			expectedError: &bucketCredsError,
		},
		{
			name: "Nil bucket credentials provided when running in serverless mode",
			startupConfig: StartupConfig{
				Unsupported: UnsupportedConfig{Serverless: ServerlessConfig{Enabled: base.Ptr(true)}},
			},
			expectedError: &bucketNoCredsServerless,
		},
		{
			name: "No bucket credentials provided when running in serverless mode",
			startupConfig: StartupConfig{
				BucketCredentials: base.PerBucketCredentialsConfig{},
				Unsupported:       UnsupportedConfig{Serverless: ServerlessConfig{Enabled: base.Ptr(true)}},
			},
			expectedError: &bucketNoCredsServerless,
		},
		{
			name: "Bucket credentials provided when running in serverless mode",
			startupConfig: StartupConfig{
				Unsupported:       UnsupportedConfig{Serverless: ServerlessConfig{Enabled: base.Ptr(true)}},
				BucketCredentials: base.PerBucketCredentialsConfig{"bucket": &base.CredentialsConfig{Username: "u", Password: "p"}},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			err := test.startupConfig.Validate(base.TestCtx(t), base.IsEnterpriseEdition())
			if test.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), *test.expectedError)

			} else if err != nil {
				assert.NotContains(t, err.Error(), bucketCredsError)
				assert.NotContains(t, err.Error(), bucketNoCredsServerless)
			}
		})
	}
}

func TestCollectionsValidation(t *testing.T) {
	testCases := []struct {
		name          string
		dbConfig      DbConfig
		expectedError *string
	}{
		{
			name: "views=true,collections=false",
			dbConfig: DbConfig{
				Name:     "db",
				UseViews: base.Ptr(true),
			},
			expectedError: nil,
		},
		{
			name: "views=true,collections=true",
			dbConfig: DbConfig{
				Name:     "db",
				UseViews: base.Ptr(true),
				Scopes: ScopesConfig{
					"fooScope": ScopeConfig{
						map[string]*CollectionConfig{
							"fooCollection:": {},
						},
					},
				},
			},
			expectedError: base.Ptr("requires GSI"),
		},
		{
			name: "views_specified=false,collections=false",
			dbConfig: DbConfig{
				Name:     "db",
				UseViews: base.Ptr(false),
			},
			expectedError: nil,
		},
		{
			name: "views_specified=false,collections=true",
			dbConfig: DbConfig{
				Name:     "db",
				UseViews: base.Ptr(false),
				Scopes: ScopesConfig{
					"fooScope": ScopeConfig{
						map[string]*CollectionConfig{
							"fooCollection:": {},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			name: "views_unspecified,collections=false",
			dbConfig: DbConfig{
				Name: "db",
			},
			expectedError: nil,
		},
		{
			name: "views_unspecified,collections=true",
			dbConfig: DbConfig{
				Name: "db",
				Scopes: ScopesConfig{
					"fooScope": ScopeConfig{
						map[string]*CollectionConfig{
							"fooCollection:": {},
						},
					},
				},
			},
			expectedError: nil,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			validateOIDCConfig := false
			validateReplications := true
			err := test.dbConfig.validate(base.TestCtx(t), validateOIDCConfig, validateReplications)
			if test.expectedError != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), *test.expectedError)
			} else {
				require.NoError(t, err)
			}

		})
	}
}

// This function allows for error checking on both x509.UnknownAuthorityError non-x509.UnknownAuthorityError types as we switch on the expected error type
// We get OS specific errors on x509.UnknownAuthorityError so we switch the expected error string if on darwin OS
func requireErrorWithX509UnknownAuthority(t testing.TB, actual, expected error) {
	expectedErrorString := expected.Error()
	if strings.Contains(actual.Error(), expectedErrorString) {
		return
	}

	// workaround for versions of go affected by https://github.com/golang/go/issues/56891
	isMacOS := runtime.GOOS == "darwin"
	_, isX509Error := expected.(x509.UnknownAuthorityError)
	if isX509Error && isMacOS {
		expectedErrorString = "certificate is not trusted"
	}

	require.ErrorContains(t, actual, expectedErrorString)
}

func TestDatabaseConfigDropScopes(t *testing.T) {
	base.RequireNumTestDataStores(t, 2)

	rt := NewRestTesterMultipleCollections(t, nil, 2)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":{}}`,
		rt.Bucket().GetName(), base.TestUseXattrs()))
	RequireStatus(t, resp, http.StatusBadRequest)
	require.Contains(t, resp.Body.String(), "cannot change scope")

}

func TestScopesConfigCollectionMap(t *testing.T) {

	testCases := []struct {
		name               string
		scopeName          string
		initialCollections []string
		updatedCollections []string
		expectedHasNew     bool
	}{
		{
			name:               "add collection",
			scopeName:          "s1",
			initialCollections: []string{"sg_test_0"},
			updatedCollections: []string{"sg_test_0", "sg_test_1"},
			expectedHasNew:     true,
		},
		{
			name:               "replace collection",
			scopeName:          "s1",
			initialCollections: []string{"sg_test_0"},
			updatedCollections: []string{"sg_test_1"},
			expectedHasNew:     true,
		},
		{
			name:               "same collection",
			scopeName:          "s1",
			initialCollections: []string{"sg_test_0"},
			updatedCollections: []string{"sg_test_0"},
			expectedHasNew:     false,
		},
		{
			name:               "remove collection",
			scopeName:          "s1",
			initialCollections: []string{"sg_test_0", "sg_test_1"},
			updatedCollections: []string{"sg_test_0"},
			expectedHasNew:     false,
		},
		{
			name:               "remove all and add collection",
			scopeName:          "s1",
			initialCollections: []string{"sg_test_0", "sg_test_1"},
			updatedCollections: []string{"sg_test_2"},
			expectedHasNew:     true,
		},
		{
			name:               "default to named",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_default"},
			updatedCollections: []string{"sg_test_0"},
			expectedHasNew:     true,
		},
		{
			name:               "default add named",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_default"},
			updatedCollections: []string{"_default", "sg_test_0"},
			expectedHasNew:     true,
		},
		{
			name:               "named to default",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"sg_test_0"},
			updatedCollections: []string{"_default"},
			expectedHasNew:     true,
		},
		{
			name:               "default remove named",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_default", "sg_test_0"},
			updatedCollections: []string{"_default"},
			expectedHasNew:     false,
		},
		{
			name:               "default to scopes default",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_default"},
			updatedCollections: []string{"_scopesdefault"}, // gets converted to scopes config for default/default
			expectedHasNew:     false,
		},
		{
			name:               "scopes default to default",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_scopesdefault"}, // gets converted to scopes config for default/default
			updatedCollections: []string{"_default"},
			expectedHasNew:     false,
		},
		{
			name:               "scopes default to named",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_scopesdefault"}, // gets converted to scopes config for default/default
			updatedCollections: []string{"sg_test_0"},
			expectedHasNew:     true,
		},
		{
			name:               "named to scopes default",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_scopesdefault"}, // gets converted to scopes config for default/default
			updatedCollections: []string{"sg_test_0"},
			expectedHasNew:     true,
		},
		{
			name:               "remove scopes default",
			scopeName:          base.DefaultScope,
			initialCollections: []string{"_scopesdefault", "sg_test_0"}, // gets converted to scopes config for default/default
			updatedCollections: []string{"sg_test_0"},
			expectedHasNew:     false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			// Create initial database
			initialScopesConfig := makeScopesConfigWithDefault(test.scopeName, test.initialCollections)
			initialCollectionsMap := initialScopesConfig.CollectionMap()

			updatedScopesConfig := makeScopesConfigWithDefault(test.scopeName, test.updatedCollections)
			require.Equal(t, test.expectedHasNew, updatedScopesConfig.HasNewCollection(initialCollectionsMap))
		})
	}
}

// makeScopesConfig creates a scopes config, with additional handling for explicitly defining the default collection in ScopesConfig
func makeScopesConfigWithDefault(scopeName string, collections []string) *ScopesConfig {

	// handling for _default._default - treat as no scopes defined
	if len(collections) == 1 && collections[0] == base.DefaultCollection {
		return nil
	}

	// handling for explicit default - ScopesConfig with _default._default only defined
	if len(collections) == 1 && collections[0] == "_scopesdefault" {
		scopesConfig := makeScopesConfig(base.DefaultScope, []string{base.DefaultCollection})
		return &scopesConfig
	}

	scopesConfig := makeScopesConfig(scopeName, collections)
	return &scopesConfig
}

// TestInvalidDbConfigNoLongerPresentInBucket:
//   - Create rest tester with large config poll interval
//   - Create valid db
//   - Alter config in bucket to make it invalid
//   - Force config poll, assert it is picked up as invalid db config
//   - Delete the invalid db config form the bucket
//   - Force config poll reload and assert the invalid db is cleared
func TestInvalidDbConfigNoLongerPresentInBucket(t *testing.T) {
	base.LongRunningTest(t)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to not run
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(10 * time.Minute)
		},
		DatabaseConfig: nil,
	})
	defer rt.Close()
	realBucketName := rt.CustomTestBucket.GetName()
	ctx := base.TestCtx(t)
	const dbName = "db1"

	// create db with correct config
	dbConfig := rt.NewDbConfig()
	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// wait for db to come online
	rt.WaitForDBOnline()

	// grab the persisted db config from the bucket
	databaseConfig := DatabaseConfig{}
	_, err := rt.ServerContext().BootstrapContext.GetConfig(rt.Context(), realBucketName, rt.ServerContext().Config.Bootstrap.ConfigGroupID, "db1", &databaseConfig)
	require.NoError(t, err)

	// update the persisted config to a fake bucket name
	newBucketName := "fakeBucket"
	_, err = rt.UpdatePersistedBucketName(&databaseConfig, &newBucketName)
	require.NoError(t, err)

	// force reload of configs from bucket
	rt.ServerContext().ForceDbConfigsReload(t, ctx)

	// assert the config is picked as invalid db config
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		invalidDatabases := rt.ServerContext().AllInvalidDatabaseNames(t)
		assert.Equal(c, 1, len(invalidDatabases))
		assert.Equal(c, 0, len(rt.ServerContext()._dbConfigs))
	}, time.Second*10, time.Millisecond*100)

	// remove the invalid config from the bucket
	rt.RemoveDbConfigFromBucket(dbName, realBucketName)

	// force reload of configs from bucket
	rt.ServerContext().ForceDbConfigsReload(t, ctx)

	// assert the config is removed from tracking
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		invalidDatabases := rt.ServerContext().AllInvalidDatabaseNames(t)
		assert.Equal(c, 0, len(invalidDatabases))
		assert.Equal(c, 0, len(rt.ServerContext()._dbConfigs))
	}, time.Second*10, time.Millisecond*100)

	// create db again, should succeed
	resp = rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
}

// TestNotFoundOnInvalidDatabase:
//   - Create rest tester with large config polling interval
//   - Insert a bad dbConfig into the bucket
//   - Manually fetch and load db from buckets
//   - Assert that the bad config is tracked as invalid config
//   - Delete the bad config manually and attempt to correct the db config through create db endpoint
//   - Assert db is removed form invalid db's and is now a running database on server context
func TestNotFoundOnInvalidDatabase(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to not run
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(100 * time.Second)
		},
		DatabaseConfig: nil,
	})
	defer rt.Close()
	realBucketName := rt.CustomTestBucket.GetName()

	// create a new invalid db config and persist to bucket
	badName := "badBucketName"
	dbConfig := rt.NewDbConfig()
	dbConfig.Name = "db1"

	version, err := GenerateDatabaseConfigVersionID(rt.Context(), "", &dbConfig)
	require.NoError(t, err)
	metadataID, metadataIDError := rt.ServerContext().BootstrapContext.ComputeMetadataIDForDbConfig(base.TestCtx(t), &dbConfig)
	require.NoError(t, metadataIDError)

	// insert the db config with bad bucket name
	dbConfig.Bucket = &badName
	persistedConfig := DatabaseConfig{
		Version:    version,
		MetadataID: metadataID,
		DbConfig:   dbConfig,
		SGVersion:  base.ProductVersion.String(),
	}
	rt.InsertDbConfigToBucket(&persistedConfig, rt.CustomTestBucket.GetName())

	// manually fetch and load db configs from bucket
	_, err = rt.ServerContext().fetchAndLoadConfigs(rt.Context(), false)
	require.NoError(t, err)

	// assert the config is picked as invalid db config
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		invalidDatabases := rt.ServerContext().AllInvalidDatabaseNames(t)
		assert.Equal(c, 1, len(invalidDatabases))
	}, time.Second*10, time.Millisecond*100)

	resp := rt.SendAdminRequest(http.MethodGet, "/db1/", "")
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// delete the invalid db config to force the not found error
	rt.RemoveDbConfigFromBucket(dbConfig.Name, realBucketName)

	// fix the bucket name and try fix corrupt db through create db endpoint
	dbConfig.Bucket = &realBucketName
	RequireStatus(t, rt.CreateDatabase(dbConfig.Name, dbConfig), http.StatusCreated)

	// assert the config is remove the invalid config and we have a running db
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		invalidDatabases := rt.ServerContext().AllInvalidDatabaseNames(t)
		assert.Equal(c, 0, len(invalidDatabases))
		assert.Equal(c, 1, len(rt.ServerContext()._dbConfigs))
	}, time.Second*10, time.Millisecond*100)
}

func TestRevCacheMemoryLimitConfig(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test is rev cache config related test, should not override the test")
	}
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	resp := rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)

	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &dbConfig))
	assert.Nil(t, dbConfig.CacheConfig)

	dbConfig.CacheConfig = &CacheConfig{}
	dbConfig.CacheConfig.RevCacheConfig = &RevCacheConfig{
		MaxItemCount:     base.Ptr(uint32(100)),
		MaxMemoryCountMB: base.Ptr(uint32(51)),
	}
	RequireStatus(t, rt.UpsertDbConfig("db1", dbConfig), http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)

	// empty db config struct
	dbConfig = DbConfig{}
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &dbConfig))
	assert.NotNil(t, dbConfig.CacheConfig)

	assert.Equal(t, uint32(100), *dbConfig.CacheConfig.RevCacheConfig.MaxItemCount)
	if base.IsEnterpriseEdition() {
		assert.Equal(t, uint32(51), *dbConfig.CacheConfig.RevCacheConfig.MaxMemoryCountMB)
	} else {
		assert.Nil(t, dbConfig.CacheConfig.RevCacheConfig.MaxMemoryCountMB)
	}

	dbConfig.CacheConfig = &CacheConfig{}
	dbConfig.CacheConfig.RevCacheConfig = &RevCacheConfig{
		MaxItemCount:     base.Ptr(uint32(100)),
		MaxMemoryCountMB: base.Ptr(uint32(4)),
	}
	resp = rt.UpsertDbConfig("db1", dbConfig)
	if base.IsEnterpriseEdition() {
		AssertHTTPErrorReason(t, resp, http.StatusInternalServerError, "Internal error: maximum rev cache memory size cannot be lower than 50 MB")
	} else {
		// CE will roll back to no memory limit as it's an EE ony feature
		RequireStatus(t, resp, http.StatusCreated)
	}

	// test turing off the memory based rev cache
	dbConfig.CacheConfig = &CacheConfig{}
	dbConfig.CacheConfig.RevCacheConfig = &RevCacheConfig{
		MaxItemCount:     base.Ptr(uint32(100)),
		MaxMemoryCountMB: base.Ptr(uint32(0)),
	}
	RequireStatus(t, rt.UpsertDbConfig("db1", dbConfig), http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusOK)

	// empty db config struct
	dbConfig = DbConfig{}
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &dbConfig))
	assert.NotNil(t, dbConfig.CacheConfig)
	assert.Equal(t, uint32(100), *dbConfig.CacheConfig.RevCacheConfig.MaxItemCount)
	assert.Equal(t, uint32(0), *dbConfig.CacheConfig.RevCacheConfig.MaxMemoryCountMB)
}

func TestTLSWithoutCerts(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.Server = strings.ReplaceAll(config.Bootstrap.Server, "couchbase://", "couchbases://")
			config.Bootstrap.ServerTLSSkipVerify = base.Ptr(true)
			config.Bootstrap.UseTLSServer = base.Ptr(true)
		},
	})
	defer rt.Close()
	dbConfig := rt.NewDbConfig()
	dbConfig.AutoImport = true
	rt.CreateDatabase("db", dbConfig)
	// ensure import feed works without TLS
	err := rt.GetSingleDataStore().Set("doc1", 0, nil, []byte(`{"foo": "bar"}`))
	require.NoError(t, err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(1), rt.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value())
	}, time.Second*10, time.Millisecond*100)

}

func TestUserUpdatedAtField(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	metaKeys := rt.GetDatabase().MetadataKeys

	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_user/", `{"name":"user1","password":"password"}`)
	RequireStatus(t, resp, http.StatusCreated)

	ds := rt.MetadataStore()
	var user map[string]any
	userKey := metaKeys.UserKey("user1")
	_, err := ds.Get(userKey, &user)
	require.NoError(t, err)

	// Check that the user has an updatedAt field
	require.NotNil(t, user["updated_at"])
	currTimeStr := user["updated_at"].(string)
	currTime, err := time.Parse(time.RFC3339, currTimeStr)
	require.NoError(t, err)
	require.NotNil(t, user["created_at"])
	currTimeCreatedStr := user["created_at"].(string)
	timeCreated, err := time.Parse(time.RFC3339, currTimeCreatedStr)
	require.NoError(t, err)

	// avoid flake where update at seems to be the same (possibly running to fast)
	time.Sleep(500 * time.Nanosecond)

	resp = rt.SendAdminRequest(http.MethodPut, "/db1/_user/user1", `{"name":"user1","password":"password1"}`)
	RequireStatus(t, resp, http.StatusOK)

	user = map[string]any{}
	_, err = ds.Get(userKey, &user)
	require.NoError(t, err)
	newTimeStr := user["updated_at"].(string)
	newTime, err := time.Parse(time.RFC3339, newTimeStr)
	require.NoError(t, err)
	newCreatedStr := user["created_at"].(string)
	newCreated, err := time.Parse(time.RFC3339, newCreatedStr)
	require.NoError(t, err)

	assert.Greater(t, newTime.UnixNano(), currTime.UnixNano())
	assert.Equal(t, timeCreated.UnixNano(), newCreated.UnixNano())
}

func TestRoleUpdatedAtField(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_role/", `{"name":"role1","admin_channels":["test"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	ds := rt.MetadataStore()
	metaKeys := rt.GetDatabase().MetadataKeys
	roleKey := metaKeys.RoleKey("role1")
	var user map[string]any
	_, err := ds.Get(roleKey, &user)
	require.NoError(t, err)

	// Check that the user has an updatedAt field
	require.NotNil(t, user["updated_at"])
	currTimeStr := user["updated_at"].(string)
	currTime, err := time.Parse(time.RFC3339, currTimeStr)
	require.NoError(t, err)
	require.NotNil(t, user["created_at"])
	currTimeCreatedStr := user["created_at"].(string)
	timeCreated, err := time.Parse(time.RFC3339, currTimeCreatedStr)
	require.NoError(t, err)

	// avoid flake where update at seems to be the same (possibly running to fast)
	time.Sleep(500 * time.Nanosecond)

	resp = rt.SendAdminRequest(http.MethodPut, "/db1/_role/role1", `{"name":"role1","admin_channels":["ABC"]}`)
	RequireStatus(t, resp, http.StatusOK)

	user = map[string]any{}
	_, err = ds.Get(roleKey, &user)
	require.NoError(t, err)
	newTimeStr := user["updated_at"].(string)
	newTime, err := time.Parse(time.RFC3339, newTimeStr)
	require.NoError(t, err)
	newCreatedStr := user["created_at"].(string)
	newCreated, err := time.Parse(time.RFC3339, newCreatedStr)
	require.NoError(t, err)

	assert.Greater(t, newTime.UnixNano(), currTime.UnixNano())
	assert.Equal(t, timeCreated.UnixNano(), newCreated.UnixNano())
}

func TestServerUUIDRuntimeServerConfig(t *testing.T) {
	testCases := []struct {
		name             string
		persistentConfig bool
	}{
		{
			name:             "Persistent config",
			persistentConfig: true,
		},
		{
			name:             "non persistent config",
			persistentConfig: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: testCase.persistentConfig})
			defer rt.Close()

			// create a db and test code pathway when we have db defined
			if testCase.persistentConfig {
				dbConfig := rt.NewDbConfig()
				RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)
			}

			resp := rt.SendAdminRequest(http.MethodGet, "/_config?include_runtime=true", "")
			RequireStatus(t, resp, http.StatusOK)

			var clusterUUID string
			config := RunTimeServerConfigResponse{}
			err := base.JSONUnmarshal(resp.Body.Bytes(), &config)
			require.NoError(t, err)
			if base.TestUseCouchbaseServer() {
				require.Len(t, config.ClusterUUID, 32)
			} else {
				require.Empty(t, config.ClusterUUID)
			}
			clusterUUID = config.ClusterUUID

			// delete db and attempt to retrieve cluster UUID again to ensure we can still retrieve it
			resp = rt.SendAdminRequest(http.MethodDelete, "/db/", "")
			RequireStatus(t, resp, http.StatusOK)

			resp = rt.SendAdminRequest(http.MethodGet, "/_config?include_runtime=true", "")
			RequireStatus(t, resp, http.StatusOK)

			config = RunTimeServerConfigResponse{}
			err = base.JSONUnmarshal(resp.Body.Bytes(), &config)
			require.NoError(t, err)
			if base.TestUseCouchbaseServer() {
				require.Len(t, config.ClusterUUID, 32)
			} else {
				require.Empty(t, config.ClusterUUID)
			}
			assert.Equal(t, clusterUUID, config.ClusterUUID)
		})
	}

}

func TestConfigUserXattrKeyValidation(t *testing.T) {
	testCases := []struct {
		name          string
		dbConfig      DbConfig
		expectedError *string
	}{
		{
			name: "userXattrKey=nil",
			dbConfig: DbConfig{
				Name:         "db",
				UserXattrKey: nil,
			},
			expectedError: nil,
		},
		{
			// this probably isn't possible to hit due to JSON unmarshalling
			name: "userXattrKey=empty str",
			dbConfig: DbConfig{
				Name:         "db",
				UserXattrKey: base.Ptr(""),
			},
			expectedError: nil,
		},
		{
			name: "userXattrKey=shortstr",
			dbConfig: DbConfig{
				Name:         "db",
				UserXattrKey: base.Ptr("shortstr"),
			},
			expectedError: nil,
		},
		{
			name: "userXattrKey=veryveryveryverylongstr",
			dbConfig: DbConfig{
				Name:         "db",
				UserXattrKey: base.Ptr("veryveryveryverylongstr"),
			},
			expectedError: base.Ptr("maximum of 15 characters"),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			validateOIDCConfig := false
			validateReplications := true
			err := test.dbConfig.validate(base.TestCtx(t), validateOIDCConfig, validateReplications)
			if test.expectedError != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), *test.expectedError)
			} else {
				require.NoError(t, err)
			}

		})
	}
}

func TestValidateUnsupportedSameSiteCookies(t *testing.T) {
	tests := []struct {
		name                string
		unsupportedSettings *db.UnsupportedOptions
		error               string
	}{
		{
			name:                "no unsupportedSettings present",
			unsupportedSettings: &db.UnsupportedOptions{},
			error:               "",
		},
		{
			name:                "no samesite, unsupportedSettings present",
			unsupportedSettings: &db.UnsupportedOptions{},
			error:               "",
		},
		{
			name:                "valid value Lax",
			unsupportedSettings: &db.UnsupportedOptions{SameSiteCookie: base.Ptr("Lax")},
			error:               "",
		},
		{
			name:                "valid value Strict",
			unsupportedSettings: &db.UnsupportedOptions{SameSiteCookie: base.Ptr("Strict")},
			error:               "",
		},
		{
			name:                "valid value None",
			unsupportedSettings: &db.UnsupportedOptions{SameSiteCookie: base.Ptr("None")},
			error:               "",
		},
		{
			name:                "invalid value",
			unsupportedSettings: &db.UnsupportedOptions{SameSiteCookie: base.Ptr("invalid value")},
			error:               "unsupported_options.same_site_cookie option",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbConfig := DbConfig{
				Name:        "db",
				Unsupported: test.unsupportedSettings,
			}

			validateReplications := false
			validateOIDC := false
			ctx := base.TestCtx(t)
			err := dbConfig.validate(ctx, validateOIDC, validateReplications)
			if test.error != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.error)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

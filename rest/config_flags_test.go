// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"flag"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test to make sure that filling all the flags correctly causes no errors or panics
func TestAllConfigFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flagMap := registerConfigFlags(&config, fs)

	flags := []string{}
	for name, flagConfig := range flagMap {
		// Skip disabled flags in this test - they will intentionally error when set
		if flagConfig.disabled {
			continue
		}
		rFlagVal := reflect.ValueOf(flagConfig.flagValue).Elem()
		switch rFlagVal.Interface().(type) {
		case string: // Test different types of strings
			rConfigVal := reflect.ValueOf(flagConfig.config).Elem()
			if rConfigVal.Kind() != reflect.Ptr && rConfigVal.CanAddr() {
				rConfigVal = rConfigVal.Addr()
			}
			val := "TestString"
			switch rConfigVal.Interface().(type) {
			case *base.ConfigDuration:
				val = "5h2m33s"
			case *base.RedactionLevel:
				val = "partial"
			case *base.LogLevel:
				val = "trace"
			case *[]uint:
				val = `123,456,789`
			case *PerDatabaseCredentialsConfig:
				val = `{"db1":{"x509_cert_path":"cert","x509_key_path":"key"}}`
			case *base.PerBucketCredentialsConfig:
				val = `{"bucket":{"x509_cert_path":"cert","x509_key_path":"key"}}`
			}
			flags = append(flags, "-"+name, val)
		case bool:
			flags = append(flags, "-"+name+"=true")
		case uint, uint64:
			flags = append(flags, "-"+name, "1234")
		case int, int64:
			flags = append(flags, "-"+name, "-5678")
		case float64:
			flags = append(flags, "-"+name, "123.456")
		default:
			assert.Failf(t, "Unknown flag type", "value type %v for flag %v", rFlagVal.Interface(), name)
		}
	}
	err := fs.Parse(flags)
	assert.NoError(t, err)
	err = fillConfigWithFlags(fs, flagMap)
	assert.NoError(t, err)
}

// Manually test different types of flags with valid values
func TestFillConfigWithFlagsValidVals(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flags := registerConfigFlags(&config, fs)

	err := fs.Parse([]string{
		"-bootstrap.server", "testServer", // String
		"-bootstrap.server_tls_skip_verify=true",   // *bool
		"-bootstrap.config_update_frequency", "2h", // *base.ConfigDuration
		"-api.max_connections", "5", // uint
		"-api.cors.origin", "*,example.com,test.net", // []string
		"-api.cors.max_age", "-5", // int
		"-logging.redaction_level", "full", // RedactionLevel
		"-logging.console.log_level", "warn", // *LogLevel
		"-replicator.max_heartbeat", "5h2m33s", // base.ConfigDuration
		"-max_file_descriptors", "12345", // uint64
		"-heap_profile_collection_threshold", "10000", // uint64
	})
	require.NoError(t, err)

	err = fillConfigWithFlags(fs, flags)
	assert.NoError(t, err)

	assert.Equal(t, "testServer", config.Bootstrap.Server)
	assert.Equal(t, base.Ptr(true), config.Bootstrap.ServerTLSSkipVerify)
	assert.Equal(t, uint(5), config.API.MaximumConnections)
	require.NotNil(t, config.Bootstrap.ConfigUpdateFrequency)
	assert.Equal(t, base.NewConfigDuration(time.Hour*2), config.Bootstrap.ConfigUpdateFrequency)
	assert.Equal(t, []string{"*", "example.com", "test.net"}, config.API.CORS.Origin)
	assert.Equal(t, -5, config.API.CORS.MaxAge)
	assert.Equal(t, "full", config.Logging.RedactionLevel.String())
	assert.Equal(t, "warn", config.Logging.Console.LogLevel.String())
	assert.Equal(t, base.NewConfigDuration(time.Hour*5+time.Minute*2+time.Second*33), config.Replicator.MaxHeartbeat)
	assert.Equal(t, uint64(12345), config.MaxFileDescriptors)
	assert.Equal(t, uint64(10000), *config.HeapProfileCollectionThreshold)

	// unset values
	assert.Equal(t, "", config.Bootstrap.X509CertPath)           // String
	assert.Nil(t, config.API.CORS.Headers)                       // []string
	assert.Nil(t, config.API.AdminInterfaceAuthentication)       // *bool
	assert.Nil(t, config.Logging.Warn.Rotation.RotationInterval) // *base.ConfigDuration
}

// Manually test different types of flags with invalid values
func TestFillConfigWithFlagsInvalidVals(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flags := registerConfigFlags(&config, fs)

	err := fs.Parse([]string{
		"-bootstrap.config_update_frequency", "time2h", // *base.ConfigDuration
		"-logging.redaction_level", "redactnone", // RedactionLevel
		"-logging.console.log_level", "wrn", // *LogLevel
		"-replicator.max_heartbeat", "time5h2m", // base.ConfigDuration
		"-bootstrap.server", "testServer", // String - filled valid so should not error
	})
	require.NoError(t, err)

	err = fillConfigWithFlags(fs, flags)
	require.Error(t, err)

	// Check each flag that has invalid value has error associated with it
	assert.Contains(t, err.Error(), "bootstrap.config_update_frequency")
	assert.Contains(t, err.Error(), "logging.redaction_level")
	assert.Contains(t, err.Error(), "logging.console.log_level")
	assert.Contains(t, err.Error(), "replicator.max_heartbeat")

	assert.NotContains(t, err.Error(), "bootstrap.server")
}

// Make sure the number of config options and number of flags match
func TestAllConfigOptionsAsFlags(t *testing.T) {
	cfg := NewEmptyStartupConfig()
	cfgFieldsNum := countFields(cfg)
	flagsNum := registerConfigFlags(&cfg, flag.NewFlagSet("test", flag.ContinueOnError))
	assert.Lenf(t, flagsNum, cfgFieldsNum, "Number of cli flags and startup config properties did not match! Did you forget to add a new config option in registerConfigFlags?")
}

func countFields(cfg interface{}) (fields int) {
	rField := reflect.ValueOf(cfg)
	if rField.Kind() == reflect.Ptr {
		rField = rField.Elem()
	}
	if rField.Kind() != reflect.Struct {
		return 1
	}
	for i := 0; i < rField.NumField(); i++ {
		jsonTag := reflect.TypeOf(rField.Interface()).Field(i).Tag.Get("json")
		if !strings.HasPrefix(jsonTag, "-") { // could be -,omitempty therefore prefix check
			fields += countFields(rField.Field(i).Interface())
		}
	}
	return fields
}

// TestDisabledFlagsErrorAndDoNotMutateConfig ensures disabled flags error and values are not wired into StartupConfig.
func TestDisabledFlagsErrorAndDoNotMutateConfig(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flags := registerConfigFlags(&config, fs)

	// Only disabled flag now is bootstrap.password
	err := fs.Parse([]string{"-bootstrap.password", "sup3rsecret"})
	require.NoError(t, err)

	err = fillConfigWithFlags(fs, flags)
	require.Error(t, err)

	// All disabled flags should be mentioned in the error
	assert.Contains(t, err.Error(), "bootstrap.password")
	assert.Contains(t, err.Error(), "Use config file to specify bootstrap password")
	assert.Contains(t, err.Error(), "use X.509 cert/key path flags instead.")

	// And none should have modified the config
	assert.Equal(t, "", config.Bootstrap.Password)
}

// Validate x509-only JSON for per-db and per-bucket flags
func TestPerCredsFlagsX509Only(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()
	flags := registerConfigFlags(&config, fs)

	// Valid X.509 only JSON should work
	err := fs.Parse([]string{
		"-database_credentials", `{"db1":{"x509_cert_path":"cert","x509_key_path":"key"}}`,
		"-bucket_credentials", `{"bucket":{"x509_cert_path":"cert","x509_key_path":"key"}}`,
	})
	require.NoError(t, err)
	require.NoError(t, fillConfigWithFlags(fs, flags))
	require.NotNil(t, config.DatabaseCredentials)
	require.NotNil(t, config.BucketCredentials)
	require.NotNil(t, config.DatabaseCredentials["db1"])
	require.NotNil(t, config.BucketCredentials["bucket"])
	assert.Equal(t, "cert", config.DatabaseCredentials["db1"].X509CertPath)
	assert.Equal(t, "key", config.DatabaseCredentials["db1"].X509KeyPath)
	assert.Equal(t, "cert", config.BucketCredentials["bucket"].X509CertPath)
	assert.Equal(t, "key", config.BucketCredentials["bucket"].X509KeyPath)

	// Username/password must be rejected by JSON decoding (unknown fields)
	fs = flag.NewFlagSet("test", flag.ContinueOnError)
	config = NewEmptyStartupConfig()
	flags = registerConfigFlags(&config, fs)
	err = fs.Parse([]string{
		"-database_credentials", `{"db1":{"username":"u","password":"p"}}`,
		"-bucket_credentials", `{"bucket":{"username":"u","password":"p"}}`,
	})
	require.NoError(t, err)
	err = fillConfigWithFlags(fs, flags)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database_credentials")
	assert.Contains(t, err.Error(), "bucket_credentials")
}

// TestPerCredsFlagsRejectBasicAuthWithHelpfulError ensures that per-db and per-bucket flags reject username/password and return clear X.509-only errors.
func TestPerCredsFlagsRejectBasicAuthWithHelpfulError(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()
	flags := registerConfigFlags(&config, fs)

	// Provide username/password in JSON for both flags
	err := fs.Parse([]string{
		"-database_credentials", `{"db1":{"username":"u","password":"p"}}`,
		"-bucket_credentials", `{"bucket":{"username":"u","password":"p"}}`,
	})
	require.NoError(t, err)

	err = fillConfigWithFlags(fs, flags)
	require.Error(t, err)
	// Check flag names are present
	assert.Contains(t, err.Error(), "database_credentials")
	assert.Contains(t, err.Error(), "bucket_credentials")
	// Check helpful X.509-only guidance present
	assert.Contains(t, err.Error(), "only X.509 cert/key paths are supported")
	assert.Contains(t, err.Error(), "username/password are not allowed")
}

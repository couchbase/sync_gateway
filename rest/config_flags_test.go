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
			case *PerDatabaseCredentialsConfig:
				val = `{"db1":{"password":"foo"}}`
			}
			flags = append(flags, "-"+name, val)
		case bool:
			flags = append(flags, "-"+name+"=true")
		case uint, uint64:
			flags = append(flags, "-"+name, "1234")
		case int:
			flags = append(flags, "-"+name, "-5678")
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
		"-max_file_descriptors", "12345", //uint64
	})
	require.NoError(t, err)

	err = fillConfigWithFlags(fs, flags)
	assert.NoError(t, err)

	assert.Equal(t, "testServer", config.Bootstrap.Server)
	assert.Equal(t, base.BoolPtr(true), config.Bootstrap.ServerTLSSkipVerify)
	assert.Equal(t, uint(5), config.API.MaximumConnections)
	require.NotNil(t, config.Bootstrap.ConfigUpdateFrequency)
	assert.Equal(t, base.NewConfigDuration(time.Hour*2), config.Bootstrap.ConfigUpdateFrequency)
	assert.Equal(t, []string{"*", "example.com", "test.net"}, config.API.CORS.Origin)
	assert.Equal(t, -5, config.API.CORS.MaxAge)
	assert.Equal(t, "full", config.Logging.RedactionLevel.String())
	assert.Equal(t, "warn", config.Logging.Console.LogLevel.String())
	assert.Equal(t, base.NewConfigDuration(time.Hour*5+time.Minute*2+time.Second*33), config.Replicator.MaxHeartbeat)
	assert.Equal(t, uint64(12345), config.MaxFileDescriptors)
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
	assert.Equalf(t, len(flagsNum), cfgFieldsNum, "Number of cli flags and startup config properties did not match! Did you forget to add a new config option in registerConfigFlags?")
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

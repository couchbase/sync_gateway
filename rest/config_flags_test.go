package rest

import (
	"flag"
	"fmt"
	"reflect"
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

	flagMap := setConfigFlags(&config, fs)

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
			}
			flags = append(flags, "-"+name, val)
		case bool:
			flags = append(flags, "-"+name+"=true")
		case uint, uint64:
			flags = append(flags, "-"+name, "1234")
		case int:
			flags = append(flags, "-"+name, "-5678")
		default:
			panic(fmt.Sprintf("Unknown flag type found %v for flag %v! Please add it to this test and config_flags.go if needed", rFlagVal.Type(), name))
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

	flags := setConfigFlags(&config, fs)

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
	assert.Equal(t, base.ConfigDuration{Duration: time.Hour * 2}, *config.Bootstrap.ConfigUpdateFrequency)
	assert.Equal(t, []string{"*", "example.com", "test.net"}, config.API.CORS.Origin)
	assert.Equal(t, -5, config.API.CORS.MaxAge)
	assert.Equal(t, "full", config.Logging.RedactionLevel.String())
	assert.Equal(t, "warn", config.Logging.Console.LogLevel.String())
	assert.Equal(t, base.ConfigDuration{Duration: time.Hour*5 + time.Minute*2 + time.Second*33}, config.Replicator.MaxHeartbeat)
	assert.Equal(t, uint64(12345), config.MaxFileDescriptors)
}

// Manually test different types of flags with invalid values
func TestFillConfigWithFlagsInvalidVals(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flags := setConfigFlags(&config, fs)

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

	assert.Contains(t, err.Error(), "bootstrap.config_update_frequency")
	assert.Contains(t, err.Error(), "logging.redaction_level")
	assert.Contains(t, err.Error(), "logging.console.log_level")
	assert.Contains(t, err.Error(), "replicator.max_heartbeat")
	assert.NotContains(t, err.Error(), "bootstrap.server")
}

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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test legacy flags using valid values
func TestLegacyFlagsValid(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flags := registerLegacyFlags(&config, fs)

	err := fs.Parse([]string{
		"-interface", "12.34.56.78",
		"-adminInterface", "admin-interface:123",
		"-profileInterface", "prof",
		"-pretty",
		"-verbose",
		"-url", "server-url.com",
		"-certpath", "cert",
		"-keypath", "key",
		"-cacertpath", "cacert",
		"-log", "HTTP,DCP,*",
		"-logFilePath", "test/file",
		// Should only log warn
		"-dbname", "dbname",
		"-deploymentID", "deployment",
	})
	require.NoError(t, err)

	err = fillConfigWithLegacyFlags(base.TestCtx(t), flags, fs, false)
	assert.NoError(t, err)

	assert.Equal(t, "12.34.56.78", config.API.PublicInterface)
	assert.Equal(t, "admin-interface:123", config.API.AdminInterface)
	assert.Equal(t, "prof", config.API.ProfileInterface)
	assert.Equal(t, base.BoolPtr(true), config.API.Pretty)
	assert.Equal(t, base.LogLevelPtr(base.LevelInfo), config.Logging.Console.LogLevel)
	assert.Equal(t, "server-url.com", config.Bootstrap.Server)
	assert.Equal(t, "cert", config.API.HTTPS.TLSCertPath)
	assert.Equal(t, "key", config.API.HTTPS.TLSKeyPath)
	assert.Equal(t, "cacert", config.Bootstrap.CACertPath)
	assert.Equal(t, []string{"HTTP", "DCP", "*"}, config.Logging.Console.LogKeys)
	assert.Equal(t, "test/file", config.Logging.LogFilePath)
}

func TestLegacyFlagsError(t *testing.T) {
	errorText := `flag "-configServer" is no longer supported and has been removed`
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	config := NewEmptyStartupConfig()

	flags := registerLegacyFlags(&config, fs)

	err := fs.Parse([]string{
		"-configServer", "1.2.3.4",
	})
	require.NoError(t, err)

	err = fillConfigWithLegacyFlags(base.TestCtx(t), flags, fs, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), errorText)
}

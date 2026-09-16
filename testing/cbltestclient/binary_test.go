// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestParseVersionMap(t *testing.T) {
	testCases := []struct {
		name        string
		value       string
		expected    map[string]string
		expectError bool
	}{
		{name: "empty", value: "", expected: map[string]string{}},
		{name: "one entry", value: "4.1.2=/path/to/testserver", expected: map[string]string{"4.1.2": "/path/to/testserver"}},
		{
			name:     "several entries with whitespace",
			value:    "4.0.0=http://127.0.0.1:8080, 4.1.2 = http://127.0.0.1:8081 ",
			expected: map[string]string{"4.0.0": "http://127.0.0.1:8080", "4.1.2": "http://127.0.0.1:8081"},
		},
		{name: "missing the version", value: "/path/to/testserver", expectError: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			parsed, err := parseVersionMap(testCase.value)
			if testCase.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.expected, parsed)
		})
	}
}

func TestNewBinaryInfoFlagDetection(t *testing.T) {
	// The flags are read out of the executable rather than asked for: the test server has no
	// --help, and a build that parses no arguments would answer a probe by starting up and binding
	// its port.
	testCases := []struct {
		name             string
		contents         string
		expectedPort     bool
		expectedFilesDir bool
	}{
		{name: "neither flag", contents: "Listening on port 8080..."},
		{name: "port only", contents: "Usage: testserver [--port <port>]", expectedPort: true},
		{
			name:             "both flags",
			contents:         "Usage: testserver [--port <port>] [--files-dir <dir>]",
			expectedPort:     true,
			expectedFilesDir: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "testserver")
			require.NoError(t, os.WriteFile(path, []byte(testCase.contents), 0o700))

			info, err := newBinaryInfo(path, "4.1.2")
			require.NoError(t, err)
			assert.Equal(t, path, info.Path)
			assert.Equal(t, "4.1.2", info.Version)
			assert.Equal(t, testCase.expectedPort, info.SupportsPortFlag)
			assert.Equal(t, testCase.expectedFilesDir, info.SupportsFilesDirFlag)
		})
	}
}

func TestResolveBinary(t *testing.T) {
	// The executable lives in a bin subdirectory because the test server resolves its assets as
	// "<executable dir>/../assets".
	root := t.TempDir()
	binDir := filepath.Join(root, "4.1.2", runtime.GOOS+"-"+runtime.GOARCH, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o700))
	installed := filepath.Join(binDir, testServerExecutable)
	require.NoError(t, os.WriteFile(installed, []byte("--port"), 0o700))
	t.Setenv(EnvTestServerDir, root)

	info, err := ResolveBinary("4.1.2")
	require.NoError(t, err)
	assert.Equal(t, installed, info.Path)
	assert.True(t, info.SupportsPortFlag)

	_, err = ResolveBinary("4.0.0")
	require.ErrorIs(t, err, ErrNoTestServer)
}

func TestResolveBinaryFromEnvironment(t *testing.T) {
	explicit := filepath.Join(t.TempDir(), testServerExecutable)
	require.NoError(t, os.WriteFile(explicit, []byte("testserver"), 0o700))
	t.Setenv(EnvTestServerBinaries, "4.1.2="+explicit)
	// An explicitly named binary wins over the cache, so a developer can test a build they have in
	// hand without installing it.
	t.Setenv(EnvTestServerDir, t.TempDir())

	info, err := ResolveBinary("4.1.2")
	require.NoError(t, err)
	assert.Equal(t, explicit, info.Path)

	// A named binary that isn't there is a mistake worth reporting, not a reason to skip.
	t.Setenv(EnvTestServerBinaries, "4.1.2="+filepath.Join(t.TempDir(), "absent"))
	_, err = ResolveBinary("4.1.2")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNoTestServer)
}

func TestExternalServerURL(t *testing.T) {
	t.Setenv(EnvTestServerURLs, "4.0.0=http://127.0.0.1:8080,4.1.2=http://127.0.0.1:8081")

	url, found := ExternalServerURL("4.1.2")
	assert.True(t, found)
	assert.Equal(t, "http://127.0.0.1:8081", url)

	_, found = ExternalServerURL("3.2.0")
	assert.False(t, found)
}

func TestDefaultVersion(t *testing.T) {
	t.Setenv(EnvCBLVersion, "")
	assert.Equal(t, DefaultCBLVersion, DefaultVersion())
	t.Setenv(EnvCBLVersion, "4.0.0")
	assert.Equal(t, "4.0.0", DefaultVersion())
}

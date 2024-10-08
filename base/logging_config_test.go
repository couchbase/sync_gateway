/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateLogFileOutput(t *testing.T) {
	logFileOutput := ""
	err := validateLogFileOutput(logFileOutput)
	assert.Error(t, err, "empty log file output")
	assert.Equal(t, "empty log file output", err.Error())

	logFileOutput = filepath.Join(os.TempDir(), "sglogfile.log")
	err = validateLogFileOutput(logFileOutput)
	assert.NoError(t, err, "log file output path should be validated")
}

func TestValidateLogFileOutputNonDirectory(t *testing.T) {
	tmpdir := t.TempDir()
	file1 := filepath.Join(tmpdir, "1.log")
	f, err := os.Create(file1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	invalidFilePath := filepath.Join(file1, "2.log")
	err = validateLogFileOutput(invalidFilePath)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid log file path: mkdir")
	if runtime.GOOS == "windows" {
		assert.ErrorContains(t, err, "The system cannot find the path specified")
	} else {
		assert.ErrorContains(t, err, "not a directory")
	}
}

// CBG-1760: Error upfront when the configured logFilePath is not writable
func TestLogFilePathWritable(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Cannot make folder inaccessible to writes or make read-only: https://github.com/golang/go/issues/35042
		t.Skip("Test not compatible with Windows")
	}

	testCases := []struct {
		name             string
		logFilePathPerms os.FileMode
		error            bool
	}{
		{
			name:             "Unwritable",
			logFilePathPerms: 0444, // Read-only perms
			error:            true,
		},
		{
			name:             "Writeable",
			logFilePathPerms: 0777, // Full perms
			error:            false,
		},
	}
	for _, test := range testCases {
		// Create tempdir here to avoid slash operator in t.Name()
		tmpPath := t.TempDir()
		t.Logf("created tmpPath: %q", tmpPath)

		t.Run(test.name, func(t *testing.T) {
			logFilePath := filepath.Join(tmpPath, "logs")
			err := os.Mkdir(logFilePath, test.logFilePathPerms)
			require.NoError(t, err)

			// The write-check is done inside validateLogFileOutput now.
			err = validateLogFileOutput(filepath.Join(logFilePath, test.name+".log"))
			if test.error {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

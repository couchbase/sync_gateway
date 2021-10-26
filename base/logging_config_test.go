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

// CBG-1760: Error upfront when the configured logFilePath is not writable
func TestLogFilePathWritable(t *testing.T) {
	testCases := []struct {
		name             string
		logFilePathPerms os.FileMode
		error            bool
	}{
		{
			name:             "Unwritable",
			logFilePathPerms: 0444, // Only reads allowed
			error:            true,
		},
		{
			name:             "Writeable",
			logFilePathPerms: 0777, // Full perms
			error:            false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			tmpPath, err := os.MkdirTemp("", "TestLogFilePathWritable*") // Cannot use t.Name() due to slash separator
			require.NoError(t, err)
			defer func() { require.NoError(t, os.RemoveAll(tmpPath)) }()

			logFilePath := tmpPath + "/logs"
			err = os.Mkdir(logFilePath, test.logFilePathPerms)
			require.NoError(t, err)

			err = validateLogFilePath(logFilePath)
			if test.error {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

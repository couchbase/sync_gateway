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

func TestHasLogFilePath(t *testing.T) {
	defaultLogFilePath := "/var/log/sync_gateway/sglogfile.log"
	emptyLogFilePath := ""
	var logFilePath *string
	assert.True(t, hasLogFilePath(logFilePath, defaultLogFilePath))
	assert.True(t, hasLogFilePath(&emptyLogFilePath, defaultLogFilePath))
	assert.False(t, hasLogFilePath(logFilePath, emptyLogFilePath))
	assert.False(t, hasLogFilePath(&emptyLogFilePath, emptyLogFilePath))
}

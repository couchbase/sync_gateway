package base

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateLogFileOutput(t *testing.T) {
	logFileOutput := ""
	err := validateLogFileOutput(logFileOutput)
	assert.Error(t, err, "empty log file output")
	assert.Equal(t, "empty log file output", err.Error())

	logFileOutput = os.TempDir() + "/sglogfile.log"
	err = validateLogFileOutput(logFileOutput)
	assert.NoError(t, err, "log file output path should be validated")
}

func TestHasLogFilePath(t *testing.T) {
	defaultLogFilePath := "/var/log/sync_gateway/sglogfile.log"
	emptyLogFilePath := ""
	var logFilePath *string
	logFilePath = nil
	assert.True(t, hasLogFilePath(logFilePath, defaultLogFilePath))
	assert.True(t, hasLogFilePath(&emptyLogFilePath, defaultLogFilePath))
	assert.False(t, hasLogFilePath(logFilePath, emptyLogFilePath))
	assert.False(t, hasLogFilePath(&emptyLogFilePath, emptyLogFilePath))
}

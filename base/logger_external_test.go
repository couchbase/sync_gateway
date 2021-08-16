package base

import (
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	gocbv1 "gopkg.in/couchbase/gocb.v1"
	gocbcorev7 "gopkg.in/couchbase/gocbcore.v7"
)

func TestGoCBLogLevelEquality(t *testing.T) {
	// Ensures all gocb and gocbcore log levels match between versions.
	// If they don't, we'll need to revisit the log wrappers to not just do direct type conversions to implement 4 loggers.
	assert.Equal(t, gocb.LogError, gocb.LogLevel(gocbcore.LogError))
	assert.Equal(t, gocb.LogError, gocb.LogLevel(gocbv1.LogError))
	assert.Equal(t, gocb.LogError, gocb.LogLevel(gocbcorev7.LogError))

	assert.Equal(t, gocb.LogWarn, gocb.LogLevel(gocbcore.LogWarn))
	assert.Equal(t, gocb.LogWarn, gocb.LogLevel(gocbv1.LogWarn))
	assert.Equal(t, gocb.LogWarn, gocb.LogLevel(gocbcorev7.LogWarn))

	assert.Equal(t, gocb.LogInfo, gocb.LogLevel(gocbcore.LogInfo))
	assert.Equal(t, gocb.LogInfo, gocb.LogLevel(gocbv1.LogInfo))
	assert.Equal(t, gocb.LogInfo, gocb.LogLevel(gocbcorev7.LogInfo))

	assert.Equal(t, gocb.LogDebug, gocb.LogLevel(gocbcore.LogDebug))
	assert.Equal(t, gocb.LogDebug, gocb.LogLevel(gocbv1.LogDebug))
	assert.Equal(t, gocb.LogDebug, gocb.LogLevel(gocbcorev7.LogDebug))

	assert.Equal(t, gocb.LogTrace, gocb.LogLevel(gocbcore.LogTrace))
	assert.Equal(t, gocb.LogTrace, gocb.LogLevel(gocbv1.LogTrace))
	assert.Equal(t, gocb.LogTrace, gocb.LogLevel(gocbcorev7.LogTrace))
}

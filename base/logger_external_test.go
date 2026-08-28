// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/couchbase/clog"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbaselabs/rosmar"
)

func TestGoCBLogLevelEquality(t *testing.T) {
	// Ensures all gocb and gocbcore log levels match between versions.
	// If they don't, we'll need to revisit the log wrappers to not just do direct type conversions to implement 4 loggers.
	assert.Equal(t, gocb.LogError, gocb.LogLevel(gocbcore.LogError))

	assert.Equal(t, gocb.LogWarn, gocb.LogLevel(gocbcore.LogWarn))

	assert.Equal(t, gocb.LogInfo, gocb.LogLevel(gocbcore.LogInfo))

	assert.Equal(t, gocb.LogDebug, gocb.LogLevel(gocbcore.LogDebug))

	assert.Equal(t, gocb.LogTrace, gocb.LogLevel(gocbcore.LogTrace))
}

// TestExternalLoggersWiredAtInit ensures the external packages' logging is redirected into SG's logs by
// init. Those packages hold their loggers in plain unsynchronised globals, so init is the only point at
// which they can safely be set - anywhere later races with whatever is already logging through them.
func TestExternalLoggersWiredAtInit(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		logFn    func()
	}{
		{
			name:     "rosmar",
			expected: "[WRN] Rosmar: external logger check",
			logFn:    func() { rosmar.LoggingCallback(rosmar.LevelWarn, "external logger check") },
		},
		{
			name:     "clog",
			expected: "[INF] DCP: external logger check",
			logFn:    func() { clog.Warnf("external logger check") },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logs := captureConsoleLogs(t, LevelDebug, []LogKey{KeyAll}, test.logFn)
			require.Contains(t, logs, test.expected)
		})
	}
}
